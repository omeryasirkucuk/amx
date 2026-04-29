"""Unified LLM interface via LiteLLM — supports OpenAI, Anthropic, Gemini, local models, etc."""

from __future__ import annotations

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


def _litellm() -> ModuleType:
    global _litellm_module
    if _litellm_module is None:
        import litellm as lm

        _litellm_module = lm
    return _litellm_module


PROVIDER_MODEL_PREFIX = {
    "openai": "openai/",
    "openrouter": "",
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
class ChatResult:
    content: str
    usage: dict | None = None
    logprobs: list | None = None
    finish_reason: str | None = None
    confidence_score: float | None = None

    def __str__(self) -> str:  # noqa: D105
        return self.content


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


def _lp_token_text(token_obj: object) -> str:
    if isinstance(token_obj, dict):
        return str(token_obj.get("token", "") or "")
    return str(getattr(token_obj, "token", "") or "")


def _lp_token_logprob(token_obj: object) -> float | None:
    raw = token_obj.get("logprob") if isinstance(token_obj, dict) else getattr(token_obj, "logprob", None)
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
    if all(ch in "-_=*#`|:;,.()[]{} " for ch in t):
        return False
    return True


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


def _weighted_score_for_spans(logprobs_content: list | None, spans: list[tuple[int, int]] | None = None) -> float | None:
    if not logprobs_content:
        return None
    weighted_logprob_sum = 0.0
    total_weight = 0.0
    token_spans = _logprob_token_spans(logprobs_content)
    for tok_start, tok_end, token_obj in token_spans:
        if spans and not any(tok_end > span_start and tok_start < span_end for span_start, span_end in spans):
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
) -> "str | None":
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

        Defaults to True (force-enable), but can be overridden per profile by
        setting ``force_logprobs`` on the LLM config object.
        """
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
        if "/" in raw:
            return raw
        prefix = PROVIDER_MODEL_PREFIX.get(self.cfg.provider, "")
        return f"{prefix}{raw}" if prefix else raw

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
        call_api_base = self.cfg.api_base if self.cfg.provider in ("local", "kimi", "ollama", "openrouter") else None

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
        try:
            resp = _do_completion(call_api_base)
        except Exception as exc:
            # If a legacy config still uses an OpenAI-style Ollama base (/v1), retry once.
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
                except Exception as retry_exc:
                    log.error("LLM call failed: %s", retry_exc)
                    raise
            else:
                log.error("LLM call failed: %s", exc)
                raise

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
            raise LLMTruncationError(
                f"LLM response truncated (finish_reason=length, model={model}, max_tokens={mt}). "
                "Increase max_tokens and retry before metadata extraction."
            )

        if not content:
            if finish == "length":
                log.warning(
                    "LLM returned EMPTY content (finish_reason=length, model=%s). "
                    "For gpt-5 / o-series, output budget may be spent on reasoning only — "
                    "increase max_tokens in ~/.amx/config.yml (e.g. 32000), set env "
                    "AMX_LLM_MIN_MAX_TOKENS, and/or AMX_REASONING_EFFORT=minimal. "
                    "Or use gpt-4o for non-reasoning completions.",
                    model,
                )
            else:
                log.warning(
                    "LLM returned EMPTY content (finish_reason=%s, model=%s). "
                    "Check model name, API key, and provider dashboard.",
                    finish,
                    model,
                )
        return ChatResult(
            content=content,
            usage=usage_dict,
            logprobs=logprobs_content,
            finish_reason=finish,
            confidence_score=confidence_score,
        )

    def test(self) -> bool:
        try:
            result = self.chat([{"role": "user", "content": "Reply with OK"}])
            return "ok" in result.content.lower()
        except Exception as exc:
            log.error("LLM test failed: %s", exc)
            return False
