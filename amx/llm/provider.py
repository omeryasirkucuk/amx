"""Unified LLM interface via LiteLLM — supports OpenAI, Anthropic, Gemini, local models, etc."""

from __future__ import annotations

import logging
import math
import os
import re
import time
from dataclasses import dataclass
from types import ModuleType
from typing import Any

from amx.config import LLMConfig, normalize_llm_model
from amx.utils.logging import get_logger

log = get_logger("llm.provider")

# Lazy import: LiteLLM has had circular-import issues when the top-level package is
# pulled in during a heavy dependency graph (e.g. interactive /run). Defer import until
# first LLM use. See https://github.com/BerriAI/litellm/issues/14717
_litellm_module: ModuleType | None = None


def _configure_ssl_environment() -> None:
    """Honor ``AMX_CA_BUNDLE`` / ``AMX_INSECURE_SSL`` so corporate networks work.

    Corporate environments routinely run TLS-inspecting proxies (Zscaler,
    Netskope, etc.) that re-sign every HTTPS connection with an internal
    root CA. Out of the box, ``httpx`` / ``openai`` / ``litellm`` don't
    trust that CA → "self-signed certificate in certificate chain" and
    every LLM call fails. We expose two env vars to make this fixable
    without touching code:

    - ``AMX_CA_BUNDLE=/path/to/corp_root.pem`` — point requests / httpx /
      curl at the corporate CA bundle. Most reliable fix.
    - ``AMX_INSECURE_SSL=1`` — disable SSL verification entirely. Useful
      for a one-shot test; do NOT use in production.
    """
    ca_bundle = os.getenv("AMX_CA_BUNDLE", "").strip()
    if ca_bundle and os.path.exists(ca_bundle):
        # ``requests`` / ``httpx`` / ``urllib3`` / ``curl`` all read these.
        for var in ("REQUESTS_CA_BUNDLE", "SSL_CERT_FILE", "CURL_CA_BUNDLE"):
            os.environ.setdefault(var, ca_bundle)

    insecure = os.getenv("AMX_INSECURE_SSL", "").strip().lower()
    if insecure in ("1", "true", "yes", "on"):
        try:
            import litellm as _lm

            _lm.ssl_verify = False  # type: ignore[attr-defined]
        except Exception:
            pass
        # Also tell the underlying httpx clients used by openai/anthropic.
        os.environ.setdefault("PYTHONHTTPSVERIFY", "0")
        log.warning(
            "AMX_INSECURE_SSL=1 — SSL certificate verification is DISABLED. "
            "Use only for diagnostics; set AMX_CA_BUNDLE in production."
        )


def _litellm() -> ModuleType:
    global _litellm_module
    if _litellm_module is None:
        # Apply corp-CA / insecure-SSL overrides BEFORE the first litellm call
        # so the underlying httpx client picks them up at construction time.
        _configure_ssl_environment()

        import litellm as lm

        for logger_name in ("LiteLLM", "litellm"):
            ext_logger = logging.getLogger(logger_name)
            ext_logger.handlers.clear()
            ext_logger.addHandler(logging.NullHandler())
            ext_logger.propagate = False
            ext_logger.setLevel(logging.CRITICAL + 1)

        # Re-apply ssl_verify after import in case env-driven flag was set.
        if os.getenv("AMX_INSECURE_SSL", "").strip().lower() in ("1", "true", "yes", "on"):
            try:
                lm.ssl_verify = False  # type: ignore[attr-defined]
            except Exception:
                pass

        _litellm_module = lm
    return _litellm_module


PROVIDER_MODEL_PREFIX = {
    "openai": "openai/",
    # OpenRouter prefix is now ALWAYS applied. Older code left this empty
    # because typical OpenRouter model ids look like "openai/gpt-4o-mini" —
    # i.e. they already contain a "/" and LiteLLM happens to route them
    # correctly via the OpenAI client + api_base override. But for vendor
    # namespaces LiteLLM doesn't natively recognise (qwen/, mistralai/,
    # google/, meta-llama/, etc.) the missing "openrouter/" prefix makes
    # LiteLLM fail with "LLM Provider NOT provided". Forcing the prefix
    # makes every OpenRouter model identifiable. ``model_name`` strips the
    # prefix when it's already present so we never double-prepend.
    "openrouter": "openrouter/",
    "anthropic": "anthropic/",
    "gemini": "gemini/",
    "deepseek": "deepseek/",
    "local": "openai/",
    "kimi": "openai/",
    "ollama": "ollama/",
}

PROVIDER_ENV_KEY = {
    "openai": "OPENAI_API_KEY",
    "openrouter": "OPENROUTER_API_KEY",
    "anthropic": "ANTHROPIC_API_KEY",
    "gemini": "GEMINI_API_KEY",
    "deepseek": "DEEPSEEK_API_KEY",
}

# OpenAI "reasoning" models (gpt-5*, o-series) may spend the whole max_tokens budget on
# internal reasoning, leaving message.content empty with finish_reason=length.
# Floor output budget + optional reasoning_effort (LiteLLM passes through to the API).
_DEFAULT_REASONING_FLOOR = 16_384


@dataclass
class ToolCall:
    """A single tool/function call requested by the LLM in a chat turn."""

    id: str
    name: str
    arguments: str  # JSON-encoded argument blob — caller decodes.


@dataclass
class ChatResult:
    content: str
    usage: dict | None = None
    logprobs: list | None = None
    finish_reason: str | None = None
    confidence_score: float | None = None
    tool_calls: list[ToolCall] | None = None

    def __str__(self) -> str:  # noqa: D105
        return self.content


@dataclass
class LLMTestResult:
    ok: bool
    message: str = ""


_BOILERPLATE_TOKENS: frozenset[str] = frozenset(
    {
        "COLUMN",
        "DESCRIPTION",
        "DESCRIPTION_1",
        "DESCRIPTION_2",
        "DESCRIPTION_3",
        "DESCRIPTION_4",
        "DESCRIPTION_5",
        "TABLE_DESCRIPTION",
        "TABLE_DESCRIPTION_1",
        "TABLE_DESCRIPTION_2",
        "TABLE_DESCRIPTION_3",
        "TABLE_DESCRIPTION_4",
        "TABLE_DESCRIPTION_5",
        "CONFIDENCE",
        "REASONING",
        "BEST_DESCRIPTION",
    }
)


class LLMTruncationError(RuntimeError):
    """Raised when model output is truncated by max_tokens."""


class FatalLLMError(RuntimeError):
    """Raised for non-recoverable LLM errors that should abort the entire run.

    Examples: out-of-credits (HTTP 402), invalid API key (401), permission
    denied (403), model not found (404). Retrying these just wastes time and
    money — every queued batch fails the same way. Catching this at the
    orchestrator / analyze_flow level lets us show the user one clear,
    actionable message and exit instead of generating 200+ identical
    warnings while iterating through every table.

    The ``user_message`` should be short, specific, and tell the user what
    to do (e.g. "Your OpenRouter account is out of credits. Visit
    https://openrouter.ai/settings/credits to top up.").
    """

    def __init__(self, user_message: str, *, original_message: str = "") -> None:
        super().__init__(user_message)
        self.user_message = user_message
        self.original_message = original_message or user_message


# HTTP status codes / message patterns that map to FatalLLMError. We classify
# these once at the LLMProvider boundary so every caller (Profile/RAG/Code
# agents, search agent, etc.) sees the same fatal contract instead of having
# to special-case 402 / 401 / 404 in their own try/except.
_FATAL_HTTP_STATUS_CODES: frozenset[int] = frozenset({401, 402, 403, 404})
_FATAL_MESSAGE_PATTERNS: tuple[tuple[str, str], ...] = (
    # Pattern → user-facing summary. Provider names are intentionally stripped
    # so the same patterns work across OpenAI, OpenRouter, Anthropic, etc.
    ("more credits", "Your account is out of credits — top up to continue."),
    (
        "insufficient_quota",
        "Your account has hit its quota — increase the limit or wait for the reset.",
    ),
    (
        "insufficient quota",
        "Your account has hit its quota — increase the limit or wait for the reset.",
    ),
    ("requires more credits", "Your account is out of credits — top up to continue."),
    ("can only afford", "Your account is out of credits — top up to continue."),
    (
        "invalid api key",
        "The API key configured for this LLM profile is invalid. Run /llm to fix it.",
    ),
    (
        "invalid_api_key",
        "The API key configured for this LLM profile is invalid. Run /llm to fix it.",
    ),
    (
        "incorrect api key",
        "The API key configured for this LLM profile is invalid. Run /llm to fix it.",
    ),
    ("authentication", "LLM authentication failed — check the API key under /llm."),
    (
        "model not found",
        "The configured model does not exist for this provider. Run /llm to pick another.",
    ),
    (
        "model_not_found",
        "The configured model does not exist for this provider. Run /llm to pick another.",
    ),
    (
        "does not exist",
        "The configured model does not exist for this provider. Run /llm to pick another.",
    ),
)


# Error fragments emitted by providers that don't support logprobs at
# all (Gemini Flash, OpenAI o-series, some Anthropic via OpenRouter).
# We catch these specifically so AMX can retry the SAME call without
# the ``logprobs=True`` request flag, instead of treating the 400 as a
# fatal error and aborting the whole run.
_LOGPROBS_UNSUPPORTED_PATTERNS: tuple[str, ...] = (
    "logprobs is not enabled for this model",  # Gemini Flash exact text
    "logprobs is not supported",
    "logprobs not supported",
    "logprobs are not supported",
    "logprob is not supported",
    "does not support logprobs",
    "logprobs parameter is not supported",
)


def _is_logprobs_unsupported_error(exc: BaseException) -> bool:
    """True when the exception indicates the model rejected ``logprobs=True``.

    Detection is message-based because providers wrap the rejection
    inside provider-specific exception types (``GeminiException``,
    ``BadRequestError``, etc.) — the only reliable signal is the
    inner JSON body.
    """
    msg = str(exc).lower()
    return any(pat in msg for pat in _LOGPROBS_UNSUPPORTED_PATTERNS)


def _classify_fatal_llm_error(exc: BaseException) -> FatalLLMError | None:
    """Return ``FatalLLMError`` when ``exc`` is non-retryable, else None.

    Detection strategy: inspect the exception's ``status_code`` (LiteLLM /
    httpx attach this) AND the lowercased error message. We need both
    because some providers wrap responses with status_code=200 and put the
    real error in the body.
    """
    msg = str(exc)
    msg_lower = msg.lower()

    # Corporate SSL inspection / self-signed CA bundles cause every retry to
    # fail the same way — short-circuit so the user gets the fix-it message
    # immediately instead of three repeated "certificate verify failed"
    # warnings followed by a generic stack trace.
    if (
        "certificate verify failed" in msg_lower
        or "self-signed certificate" in msg_lower
        or "ssl: certificate" in msg_lower
    ):
        return FatalLLMError(
            "SSL certificate verification failed — your network is using a "
            "TLS-inspecting proxy whose root CA Python doesn't trust. "
            "Fix: set AMX_CA_BUNDLE=/path/to/corp_root.pem (preferred), or "
            "AMX_INSECURE_SSL=1 for diagnostics only.",
            original_message=msg,
        )

    status: int | None = None
    for attr in ("status_code", "code"):
        candidate = getattr(exc, attr, None)
        if isinstance(candidate, int):
            status = candidate
            break
        if isinstance(candidate, str) and candidate.isdigit():
            status = int(candidate)
            break
    # Many LiteLLM error strings include the HTTP code inline ("APIError: 402").
    if status is None:
        for code in _FATAL_HTTP_STATUS_CODES:
            if f"{code}" in msg and (
                f" {code} " in msg
                or f'":{code}' in msg
                or f'"code":{code}' in msg
                or f'"code": {code}' in msg
                or f"({code})" in msg
            ):
                status = code
                break
    for pattern, user_msg in _FATAL_MESSAGE_PATTERNS:
        if pattern in msg_lower:
            return FatalLLMError(user_msg, original_message=msg)
    if status in _FATAL_HTTP_STATUS_CODES:
        # Last-resort generic message when the body didn't match a pattern.
        if status == 402:
            return FatalLLMError(
                "Your LLM provider returned 402 Payment Required — usually out of credits.",
                original_message=msg,
            )
        if status == 401:
            return FatalLLMError(
                "LLM authentication failed (HTTP 401). Re-check the API key under /llm.",
                original_message=msg,
            )
        if status == 403:
            return FatalLLMError(
                "LLM access denied (HTTP 403). Your key may lack permission for this model.",
                original_message=msg,
            )
        if status == 404:
            return FatalLLMError(
                "LLM returned 404 — the configured model name is unknown to the provider.",
                original_message=msg,
            )
    return None


def _lp_token_text(token_obj: object) -> str:
    if isinstance(token_obj, dict):
        return str(token_obj.get("token", "") or "")
    return str(getattr(token_obj, "token", "") or "")


def _lp_token_logprob(token_obj: object) -> float | None:
    raw = (
        token_obj.get("logprob")
        if isinstance(token_obj, dict)
        else getattr(token_obj, "logprob", None)
    )
    if raw is None:
        return None
    try:
        return float(raw)
    except Exception:
        return None


def _is_value_token(token_text: str) -> bool:
    t = (token_text or "").strip()
    if not t:
        return False
    upper = t.upper().strip(":")
    if upper in _BOILERPLATE_TOKENS:
        return False
    if t in {"{", "}", "[", "]", ":", ",", '"', "```"}:
        return False
    return not all(ch in "-_=*#`|:;,.()[]{} " for ch in t)


def _description_value_spans(text: str) -> list[tuple[int, int]]:
    """Return spans for generated description/comment values only."""
    if not text:
        return []
    spans: list[tuple[int, int]] = []
    json_pattern = re.compile(
        r'"(?:description|comment|best_description|table_description)(?:_\d+)?"\s*:\s*"((?:\\.|[^"\\])*)"',
        re.IGNORECASE,
    )
    for match in json_pattern.finditer(text):
        spans.append((match.start(1), match.end(1)))
    label_pattern = re.compile(
        r"(?im)^(?:DESCRIPTION(?:_\d+)?|TABLE_DESCRIPTION(?:_\d+)?|BEST_DESCRIPTION|COMMENT)\s*:\s*(.+)$"
    )
    for match in label_pattern.finditer(text):
        value = match.group(1).strip()
        if not value:
            continue
        offset = match.group(1).find(value)
        start = match.start(1) + max(0, offset)
        spans.append((start, start + len(value)))
    spans.sort()
    merged: list[tuple[int, int]] = []
    for start, end in spans:
        if not merged or start > merged[-1][1]:
            merged.append((start, end))
        else:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
    return merged


def _weighted_score_for_spans(
    logprobs_content: list | None, spans: list[tuple[int, int]] | None = None
) -> float | None:
    if not logprobs_content:
        return None
    weighted_logprob_sum = 0.0
    total_weight = 0.0
    token_spans = _logprob_token_spans(logprobs_content)
    for tok_start, tok_end, token_obj in token_spans:
        if spans and not any(
            tok_end > span_start and tok_start < span_end for span_start, span_end in spans
        ):
            continue
        token_text = _lp_token_text(token_obj)
        if not _is_value_token(token_text):
            continue
        lp = _lp_token_logprob(token_obj)
        if lp is None:
            continue
        weight = max(1.0, float(len(token_text.strip())))
        weighted_logprob_sum += lp * weight
        total_weight += weight
    if total_weight <= 0:
        return None
    avg_lp = weighted_logprob_sum / total_weight
    return math.exp(avg_lp)


def logprob_confidence_score(logprobs_content: list | None) -> float | None:
    """Weighted geometric-mean confidence from generated description/comment text."""
    if not logprobs_content:
        return None
    generated_text = "".join(_lp_token_text(token_obj) for token_obj in logprobs_content)
    spans = _description_value_spans(generated_text)
    return _weighted_score_for_spans(logprobs_content, spans or None)


def _logprob_token_spans(logprobs_content: list | None) -> list[tuple[int, int, object]]:
    """Best-effort offsets for completion tokens reconstructed from token text."""
    if not logprobs_content:
        return []
    spans: list[tuple[int, int, object]] = []
    pos = 0
    for token_obj in logprobs_content:
        token_text = _lp_token_text(token_obj)
        start = pos
        pos += len(token_text)
        spans.append((start, pos, token_obj))
    return spans


def logprob_confidence_score_for_text(
    logprobs_content: list | None,
    generated_text: str,
    target_text: str,
) -> float | None:
    """Score the logprob confidence for one generated text fragment.

    Provider APIs return logprobs for completion tokens, not for parsed AMX
    suggestions. AMX reconstructs token offsets and scores only the tokens
    overlapping the selected description text, falling back to the full
    response score when the fragment cannot be located exactly.
    """
    if not logprobs_content or not generated_text or not target_text:
        return None

    start = generated_text.find(target_text)
    if start < 0:
        return None
    end = start + len(target_text)

    return _weighted_score_for_spans(logprobs_content, [(start, end)])


def confidence_from_logprobs(
    logprobs_content: list | None,
    high_threshold: float = 0.85,
    medium_threshold: float = 0.50,
) -> str | None:
    """Map weighted geometric-mean token probability to HIGH/MEDIUM/LOW."""
    score = logprob_confidence_score(logprobs_content)
    if score is None:
        return None
    log.debug("Weighted logprob confidence score=%.6f", score)
    if score >= high_threshold:
        return "HIGH"
    if score >= medium_threshold:
        return "MEDIUM"
    return "LOW"


def _openai_model_id(model: str) -> str:
    return model.split("/")[-1].strip().lower()


def _is_openai_reasoning_style_model(model: str) -> bool:
    mid = _openai_model_id(model)
    return (
        mid.startswith("gpt-5")
        or mid.startswith("o1")
        or mid.startswith("o3")
        or mid.startswith("o4")
    )


def _normalized_api_base(provider: str, api_base: str | None) -> str | None:
    """Normalize provider-specific base URLs to avoid common endpoint mismatches."""
    if not api_base:
        return api_base
    base = api_base.strip()
    if provider == "ollama":
        # LiteLLM's ollama provider expects the root endpoint (no trailing /v1).
        lower = base.lower().rstrip("/")
        if lower.endswith("/v1"):
            return base.rstrip("/")[:-3].rstrip("/")
    return base


MAX_LLM_RETRIES = 2
LLM_RETRY_BACKOFF_BASE_SEC = 1.0

# Per-request timeout (seconds). Without this, a stalled upstream connection
# leaves the call hanging indefinitely — the user reported a single profile
# batch sitting at 9m58s while sibling batches finished in 1m. LiteLLM
# forwards ``timeout=N`` to the underlying client (OpenAI / Anthropic /
# Gemini / OpenRouter); on expiry the call raises ``Timeout`` /
# ``APITimeoutError`` which our ``_is_transient_llm_error`` filter already
# recognises, so retry-with-backoff kicks in automatically.
#
# Tunable via env: ``AMX_LLM_TIMEOUT_SEC``. Default 180s is enough for a
# wide-batch profile call (50 cols ≈ 60–120s in practice) but caps the
# pathological-hang case below 3 minutes — at which point the retry loop
# starts a fresh request.
_DEFAULT_LLM_TIMEOUT_SEC = 180.0


def _llm_timeout_sec() -> float:
    raw = os.getenv("AMX_LLM_TIMEOUT_SEC", "").strip()
    if not raw:
        return _DEFAULT_LLM_TIMEOUT_SEC
    try:
        value = float(raw)
    except ValueError:
        return _DEFAULT_LLM_TIMEOUT_SEC
    return value if value > 0 else _DEFAULT_LLM_TIMEOUT_SEC


_TRANSIENT_LLM_EXCEPTION_NAMES: frozenset[str] = frozenset(
    {
        "APITimeoutError",
        "APIConnectionError",
        "InternalServerError",
        "ServiceUnavailableError",
        "RateLimitError",
        "Timeout",
        "ReadTimeout",
        "ConnectTimeout",
        "ConnectionError",
        "ConnectionResetError",
    }
)
_TRANSIENT_LLM_MESSAGE_TOKENS: tuple[str, ...] = (
    "rate limit",
    "rate_limit",
    "429",
    "timed out",
    "timeout",
    "connection reset",
    "connection aborted",
    "broken pipe",
    "service unavailable",
    "502 bad gateway",
    "503 service",
    "504 gateway",
    "temporary failure",
)


def _is_transient_llm_error(exc: BaseException) -> bool:
    """Return True for LLM errors worth retrying once with backoff.

    Covers rate-limit (HTTP 429), timeouts, connection-reset, and the common
    upstream 5xx classes. Authentication / bad-request errors are NOT
    transient and propagate to the caller after one attempt so the user
    sees a themed connector-categorised message instead of a 30-second
    silent retry storm.
    """
    if isinstance(exc, (TimeoutError, ConnectionError)):
        return True
    cls_name = exc.__class__.__name__
    if cls_name in _TRANSIENT_LLM_EXCEPTION_NAMES:
        return True
    msg = str(exc).lower()
    return any(token in msg for token in _TRANSIENT_LLM_MESSAGE_TOKENS)


class LLMProvider:
    """Thin wrapper around LiteLLM so every agent uses the same calling convention."""

    def __init__(self, cfg: LLMConfig):
        self.cfg = cfg
        normalized_model = normalize_llm_model(cfg.provider, cfg.model)
        if normalized_model and normalized_model != cfg.model:
            log.info(
                "Normalizing %s model from '%s' to '%s'",
                cfg.provider,
                cfg.model,
                normalized_model,
            )
            self.cfg.model = normalized_model
        self._configure_env()

    @property
    def supports_logprobs(self) -> bool:
        """Whether AMX should request logprobs for this profile.

        Defaults to True (force-enable), can be overridden per profile via
        ``force_logprobs`` on the LLM config, AND is auto-disabled at
        runtime when the provider rejects the request (see
        ``_logprobs_runtime_disabled`` set by ``chat()`` when it sees a
        ``Logprobs is not enabled for this model`` 400 from Gemini /
        OpenAI o-series / etc.).
        """
        if getattr(self, "_logprobs_runtime_disabled", False):
            return False
        return bool(getattr(self.cfg, "force_logprobs", True))

    @property
    def supports_batch(self) -> bool:
        """True when the configured provider has a registered batch implementation."""
        from amx.llm.batch import get_batch_provider

        return get_batch_provider(self.cfg) is not None

    def _configure_env(self) -> None:
        env_key = PROVIDER_ENV_KEY.get(self.cfg.provider)
        if env_key and self.cfg.api_key:
            os.environ[env_key] = self.cfg.api_key

        normalized_base = _normalized_api_base(self.cfg.provider, self.cfg.api_base)
        if normalized_base != self.cfg.api_base:
            log.info(
                "Normalizing %s api_base from '%s' to '%s'",
                self.cfg.provider,
                self.cfg.api_base,
                normalized_base,
            )
            self.cfg.api_base = normalized_base

        if self.cfg.provider in ("local", "kimi"):
            if self.cfg.api_base:
                os.environ["OPENAI_API_BASE"] = self.cfg.api_base
            os.environ.setdefault("OPENAI_API_KEY", self.cfg.api_key or "local")
        elif self.cfg.provider == "openrouter":
            if self.cfg.api_base:
                os.environ["OPENAI_API_BASE"] = self.cfg.api_base
            # LiteLLM OpenRouter path expects OpenAI-style key wiring.
            if self.cfg.api_key:
                os.environ["OPENROUTER_API_KEY"] = self.cfg.api_key
                os.environ["OPENAI_API_KEY"] = self.cfg.api_key
            else:
                os.environ.setdefault("OPENROUTER_API_KEY", "")
        elif self.cfg.provider == "ollama":
            if self.cfg.api_base:
                os.environ["OLLAMA_API_BASE"] = self.cfg.api_base
            # Some LiteLLM versions still check for a key even if unused
            os.environ.setdefault("OLLAMA_API_KEY", self.cfg.api_key or "ollama")

        lm = _litellm()
        lm.drop_params = True
        # Avoid LiteLLM printing raw "Give Feedback / turn_on_debug" lines into the TUI.
        if hasattr(lm, "suppress_debug_info"):
            lm.suppress_debug_info = True
        if hasattr(lm, "set_verbose"):
            lm.set_verbose = False

    @property
    def model_name(self) -> str:
        raw = normalize_llm_model(self.cfg.provider, self.cfg.model)
        if not raw:
            return raw
        prefix = PROVIDER_MODEL_PREFIX.get(self.cfg.provider, "")
        # Apply the prefix unless the user already typed it in their config.
        # Previous code used a blanket ``if "/" in raw`` early-return, which
        # bypassed the prefix for any model id that contained a vendor
        # namespace (e.g. ``qwen/qwen3.5-...`` on OpenRouter). LiteLLM then
        # saw an unknown provider in the head of the model id and raised
        # "LLM Provider NOT provided". The new check skips the prefix only
        # when ``raw`` already begins with it.
        if not prefix:
            return raw
        if raw.lower().startswith(prefix.lower()):
            return raw
        return f"{prefix}{raw}"

    def chat(
        self,
        messages: list[dict[str, str]],
        temperature: float | None = None,
        max_tokens: int | None = None,
        use_logprobs: bool = True,
        **kwargs: Any,
    ) -> ChatResult:
        model = self.model_name
        mt = max_tokens or self.cfg.max_tokens
        extra: dict[str, Any] = dict(kwargs)

        # Honor a runtime-discovered disable flag — set lower in the
        # exception path when a provider returns
        # ``Logprobs is not enabled for this model`` (Gemini Flash,
        # OpenAI o-series, some others). Without this, every subsequent
        # call in the same session would re-trigger the same 400.
        if use_logprobs and getattr(self, "_logprobs_runtime_disabled", False):
            use_logprobs = False
        if use_logprobs:
            # Force-request logprobs regardless of provider capability metadata.
            extra["logprobs"] = True
            # Keep OpenAI-compatible top-k logprob detail where supported.
            extra.setdefault("top_logprobs", 5)
            # Some local/Ollama-compatible backends require explicit integer hint.
            if self.cfg.provider == "ollama":
                extra.setdefault("num_probs", 5)

        # Reasoning models: raise floor so visible content can appear after thinking tokens.
        if self.cfg.provider == "openai" and _is_openai_reasoning_style_model(model):
            floor = int(os.getenv("AMX_LLM_MIN_MAX_TOKENS", str(_DEFAULT_REASONING_FLOOR)))
            if mt < floor:
                log.debug(
                    "Raising max_tokens %d → %d for reasoning model %s",
                    mt,
                    floor,
                    model,
                )
                mt = floor
            effort = os.getenv("AMX_REASONING_EFFORT", "low").strip().lower()
            if effort in ("none", "minimal", "low", "medium", "high"):
                extra.setdefault("reasoning_effort", effort)

        log.debug("LLM call → model=%s, max_tokens=%d", model, mt)
        call_api_base = (
            self.cfg.api_base
            if self.cfg.provider in ("local", "kimi", "ollama", "openrouter")
            else None
        )

        # Resolve timeout once per call. ``extra`` is the user-passed kwargs;
        # if a caller wants to override the default for a specific call they
        # can pass ``timeout=N``. Otherwise we fall back to the env-tunable
        # default so no LLM call can hang indefinitely.
        if "timeout" not in extra and "request_timeout" not in extra:
            extra["timeout"] = _llm_timeout_sec()

        def _do_completion(api_base_override: str | None) -> Any:
            explicit_api_key = self.cfg.api_key if self.cfg.provider == "openrouter" else None
            return _litellm().completion(
                model=model,
                messages=messages,
                temperature=temperature or self.cfg.temperature,
                max_tokens=mt,
                api_key=explicit_api_key,
                api_base=api_base_override,
                **extra,
            )

        t0 = time.perf_counter()
        resp = None
        last_exc: BaseException | None = None
        for attempt in range(MAX_LLM_RETRIES + 1):
            try:
                resp = _do_completion(call_api_base)
                last_exc = None
                break
            except Exception as exc:
                last_exc = exc

                # Logprobs-not-supported recovery: Gemini Flash, OpenAI
                # o-series, and a few OpenRouter-fronted models reject
                # ``logprobs=True`` with a 400. Strip the flag, set the
                # session-level disable so subsequent calls skip it
                # upfront, and retry the same call once.
                if (
                    extra.get("logprobs")
                    and _is_logprobs_unsupported_error(exc)
                    and not getattr(self, "_logprobs_runtime_disabled", False)
                ):
                    log.info(
                        "Provider %s/%s rejected logprobs=True. Disabling "
                        "logprobs for this session and retrying. Confidence "
                        "bands will use heuristic scoring instead of "
                        "calibrated token logprobs.",
                        self.cfg.provider,
                        self.cfg.model,
                    )
                    self._logprobs_runtime_disabled = True
                    for key in ("logprobs", "top_logprobs", "num_probs"):
                        extra.pop(key, None)
                    try:
                        resp = _do_completion(call_api_base)
                        last_exc = None
                        break
                    except Exception as logprobs_retry_exc:
                        last_exc = logprobs_retry_exc
                        # Fall through to the standard classification path
                        # so the user sees the real underlying issue if
                        # the retry also fails.

                # Fatal errors (auth / quota / payment / model-not-found) are
                # not transient — every retry will fail the same way. Raise
                # immediately so the orchestrator can abort the entire run
                # instead of producing 200+ identical warnings while
                # iterating through tables. Skip Ollama 404 — that one IS
                # recoverable (handled below).
                fatal = _classify_fatal_llm_error(exc)
                if fatal is not None and not (
                    self.cfg.provider == "ollama" and "404 page not found" in str(exc).lower()
                ):
                    log.error("Fatal LLM error (no retry): %s", fatal.user_message)
                    raise fatal from exc

                # Provider-specific recovery (only on first attempt): legacy
                # Ollama configs that still point at an OpenAI-style /v1 path
                # see 404s. Strip /v1 and retry once before falling through to
                # the generic transient-retry loop.
                if attempt == 0:
                    msg = str(exc).lower()
                    if (
                        self.cfg.provider == "ollama"
                        and "404 page not found" in msg
                        and isinstance(call_api_base, str)
                        and call_api_base.rstrip("/").lower().endswith("/v1")
                    ):
                        fallback_base = call_api_base.rstrip("/")[:-3].rstrip("/")
                        log.warning(
                            "Ollama returned 404 with api_base=%s; retrying once with %s",
                            call_api_base,
                            fallback_base,
                        )
                        try:
                            resp = _do_completion(fallback_base)
                            self.cfg.api_base = fallback_base
                            os.environ["OLLAMA_API_BASE"] = fallback_base
                            call_api_base = fallback_base
                            last_exc = None
                            break
                        except Exception as fallback_exc:
                            last_exc = fallback_exc

                # Transient retry — rate-limit, timeout, 5xx, connection reset.
                if (
                    attempt < MAX_LLM_RETRIES
                    and last_exc is not None
                    and _is_transient_llm_error(last_exc)
                ):
                    wait = LLM_RETRY_BACKOFF_BASE_SEC * (2**attempt)
                    log.warning(
                        "LLM transient failure (attempt %d/%d) — retrying in %.1fs: %s",
                        attempt + 1,
                        MAX_LLM_RETRIES + 1,
                        wait,
                        last_exc,
                    )
                    time.sleep(wait)
                    continue

                # Non-transient or out of retries → propagate.
                log.error("LLM call failed: %s", last_exc)
                raise

        if resp is None:
            # Defensive — the loop should either have populated resp or raised.
            raise (
                last_exc
                if last_exc is not None
                else RuntimeError("LLM completion failed without a recorded exception")
            )

        elapsed_sec = max(0.0, time.perf_counter() - t0)

        choice = resp.choices[0]
        content = choice.message.content or ""
        finish = getattr(choice, "finish_reason", None)

        raw_usage = getattr(resp, "usage", None)
        usage_dict: dict | None = None
        if raw_usage:
            usage_dict = {
                "prompt_tokens": getattr(raw_usage, "prompt_tokens", 0) or 0,
                "completion_tokens": getattr(raw_usage, "completion_tokens", 0) or 0,
                "total_tokens": getattr(raw_usage, "total_tokens", 0) or 0,
                "model_processing_sec": elapsed_sec,
            }
        else:
            usage_dict = {"model_processing_sec": elapsed_sec}

        logprobs_content: list | None = None
        try:
            raw_lp = getattr(choice, "logprobs", None)
            if raw_lp is not None:
                logprobs_content = getattr(raw_lp, "content", None) or None
            if use_logprobs and not logprobs_content:
                log.warning(
                    "Requested logprobs but response had none (provider=%s, model=%s).",
                    self.cfg.provider,
                    model,
                )
        except Exception as exc:
            log.warning(
                "Failed to parse logprobs from response (provider=%s, model=%s): %s",
                self.cfg.provider,
                model,
                exc,
            )
            logprobs_content = None

        log.debug(
            "LLM response: %d chars, finish_reason=%s, usage=%s, logprobs=%s",
            len(content),
            finish,
            usage_dict,
            "yes" if logprobs_content else "no",
        )
        confidence_score = logprob_confidence_score(logprobs_content)
        if confidence_score is None:
            log.debug(
                "Logprob confidence score unavailable (provider=%s, model=%s)",
                self.cfg.provider,
                model,
            )
        else:
            log.debug("Logprob confidence score=%.6f", confidence_score)

        if finish == "length":
            # Distinguish two truncation cases:
            #
            # (a) Content was produced but cut off — the user can usually
            #     fix this by raising max_tokens. Raise the soft
            #     ``LLMTruncationError`` and let the caller decide.
            # (b) ``content`` is empty AND output_tokens == max_tokens.
            #     This is the reasoning-model failure mode: the model
            #     burned every output token on internal "thinking" and
            #     returned nothing. No amount of retrying or max_tokens
            #     bumping helps if the model is the wrong tool — every
            #     batch fails the same way (the user reported this with
            #     openrouter/tencent/hy3-preview:free). Treat as fatal so
            #     the run aborts after one attempt with a clear message.
            if not content:
                raise FatalLLMError(
                    (
                        f"Model `{model}` returned 0 visible characters and used all "
                        f"{mt} output tokens — this almost always means a reasoning "
                        "model burnt the budget on internal thinking. AMX needs visible "
                        "JSON output. Try a non-reasoning model like "
                        "`openrouter/openai/gpt-4o-mini`, "
                        "`openrouter/anthropic/claude-3-5-haiku`, or "
                        "`openrouter/google/gemini-1.5-flash` under /llm. "
                        "If you must use this model, raise max_tokens dramatically "
                        "(e.g. AMX_LLM_MIN_MAX_TOKENS=32000) AND set "
                        "AMX_REASONING_EFFORT=minimal."
                    ),
                    original_message=(
                        f"finish_reason=length, content_chars=0, max_tokens={mt}, model={model}"
                    ),
                )
            raise LLMTruncationError(
                f"LLM response truncated (finish_reason=length, model={model}, max_tokens={mt}). "
                "Increase max_tokens and retry before metadata extraction."
            )

        if not content:
            # ``finish_reason=tool_calls`` means the model is asking for
            # a tool to run — an empty content body is the expected
            # OpenAI-protocol shape (the function call lives in
            # ``message.tool_calls`` instead). Don't warn on that; it's
            # normal flow and just adds log noise. Same goes for
            # ``function_call`` (legacy single-tool mode).
            #
            # Genuine "model returned nothing" cases — finish_reason in
            # {stop, content_filter, length, end_turn, ""} with no
            # accompanying tool_calls — still warrant the WARNING below.
            tool_call_finishes = {"tool_calls", "function_call"}
            if str(finish or "").lower() in tool_call_finishes:
                log.debug(
                    "LLM tool-call response (finish_reason=%s, model=%s); "
                    "empty content is expected.",
                    finish,
                    model,
                )
            else:
                log.warning(
                    "LLM returned EMPTY content (finish_reason=%s, model=%s). "
                    "Check model name, API key, and provider dashboard.",
                    finish,
                    model,
                )
        # Tool/function calls — extracted when the caller passes ``tools`` in
        # ``extra``. LiteLLM mirrors the OpenAI shape across providers, so we
        # read ``message.tool_calls[*].function.{name,arguments}`` regardless
        # of upstream backend (OpenAI / Anthropic / Gemini / OpenRouter / etc.).
        parsed_tool_calls: list[ToolCall] | None = None
        try:
            raw_calls = getattr(choice.message, "tool_calls", None) or []
            collected: list[ToolCall] = []
            for tc in raw_calls:
                fn = getattr(tc, "function", None)
                if fn is None:
                    continue
                collected.append(
                    ToolCall(
                        id=str(getattr(tc, "id", "") or ""),
                        name=str(getattr(fn, "name", "") or ""),
                        arguments=str(getattr(fn, "arguments", "") or ""),
                    )
                )
            if collected:
                parsed_tool_calls = collected
        except Exception as exc:
            log.warning("Failed to parse tool_calls from response: %s", exc)

        return ChatResult(
            content=content,
            usage=usage_dict,
            logprobs=logprobs_content,
            finish_reason=finish,
            confidence_score=confidence_score,
            tool_calls=parsed_tool_calls,
        )

    def test_result(self) -> LLMTestResult:
        try:
            result = self.chat([{"role": "user", "content": "Reply with OK"}])
            if "ok" in result.content.lower():
                return LLMTestResult(ok=True)
            return LLMTestResult(
                ok=False,
                message=(
                    "LLM responded, but the health-check reply was unexpected. "
                    "Verify that the active model accepts standard chat completions."
                ),
            )
        except Exception as exc:
            log.error("LLM test failed: %s", exc)
            return LLMTestResult(ok=False, message=str(exc))

    def test(self) -> bool:
        return self.test_result().ok
