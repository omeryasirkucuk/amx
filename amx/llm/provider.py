"""Unified LLM interface via LiteLLM — supports OpenAI, Anthropic, Gemini, local models, etc."""

from __future__ import annotations

import logging
import os
import time
from collections.abc import Callable
from dataclasses import dataclass
from types import ModuleType
from typing import Any

from amx.config import LLMConfig, normalize_llm_model
from amx.llm._logprob_confidence import (  # noqa: PLC0414
    confidence_from_logprobs as confidence_from_logprobs,
)
from amx.llm._logprob_confidence import (
    logprob_confidence_score as logprob_confidence_score,
)
from amx.llm._logprob_confidence import (
    logprob_confidence_score_for_text as logprob_confidence_score_for_text,
)
from amx.llm._provider_config import (
    _DEFAULT_REASONING_FLOOR as _DEFAULT_REASONING_FLOOR,
)
from amx.llm._provider_config import (
    _REASONING_AUTO_RETRY_CAP as _REASONING_AUTO_RETRY_CAP,
)
from amx.llm._provider_config import (  # noqa: PLC0414
    PROVIDER_ENV_KEY as PROVIDER_ENV_KEY,
)
from amx.llm._provider_config import (
    PROVIDER_MODEL_PREFIX as PROVIDER_MODEL_PREFIX,
)
from amx.llm._provider_config import (
    _is_openai_reasoning_style_model as _is_openai_reasoning_style_model,
)
from amx.llm._provider_config import (
    _openai_model_id as _openai_model_id,
)
from amx.llm._provider_config import (
    _supports_thinking as _supports_thinking,
)
from amx.llm._provider_errors import (
    _FATAL_HTTP_STATUS_CODES as _FATAL_HTTP_STATUS_CODES,
)
from amx.llm._provider_errors import (
    _FATAL_MESSAGE_PATTERNS as _FATAL_MESSAGE_PATTERNS,
)
from amx.llm._provider_errors import (
    _LOGPROBS_UNSUPPORTED_PATTERNS as _LOGPROBS_UNSUPPORTED_PATTERNS,
)
from amx.llm._provider_errors import (  # noqa: PLC0414
    FatalLLMError as FatalLLMError,
)
from amx.llm._provider_errors import (
    LLMTruncationError as LLMTruncationError,
)
from amx.llm._provider_errors import (
    _classify_fatal_llm_error as _classify_fatal_llm_error,
)
from amx.llm._provider_errors import (
    _is_logprobs_unsupported_error as _is_logprobs_unsupported_error,
)
from amx.llm._provider_errors import (
    _is_transient_llm_error as _is_transient_llm_error,
)
from amx.utils.logging import get_logger

log = get_logger("llm.provider")

# Lazy import: LiteLLM has had circular-import issues when the top-level package is
# pulled in during a heavy dependency graph (e.g. interactive /run). Defer import until
# first LLM use. See https://github.com/BerriAI/litellm/issues/14717
_litellm_module: ModuleType | None = None


def _configure_ssl_environment() -> None:
    """Apply ``AMX_INSECURE_SSL`` to the litellm / httpx stack.

    Corporate environments routinely run TLS-inspecting proxies
    (Zscaler, Netskope, etc.) that re-sign every HTTPS connection
    with an internal root CA. The general fix lives in
    :mod:`amx.utils.network_trust`: the startup helper there injects
    ``truststore`` so Python's ``ssl`` module consults the OS trust
    store (where the corporate CA is almost always already
    installed), and fans ``AMX_CA_BUNDLE`` out to
    ``REQUESTS_CA_BUNDLE`` / ``SSL_CERT_FILE`` / ``CURL_CA_BUNDLE``
    for the third-party HTTP clients that read those env vars.

    This function is now the litellm-specific extension of that
    contract: when the user sets ``AMX_INSECURE_SSL=1`` for a
    one-shot diagnostic, AMX flips litellm's own ``ssl_verify``
    flag and the cross-library ``PYTHONHTTPSVERIFY`` env var so the
    underlying httpx clients used by openai / anthropic SDKs also
    bypass verification. ``AMX_INSECURE_SSL`` should never be set in
    production.
    """
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
            "AMX_INSECURE_SSL=1  --  SSL certificate verification is DISABLED. "
            "Use only for diagnostics; set AMX_CA_BUNDLE in production."
        )


def _litellm() -> ModuleType:
    global _litellm_module
    if _litellm_module is None:
        # Apply corp-CA / insecure-SSL overrides BEFORE the first litellm call
        # so the underlying httpx client picks them up at construction time.
        _configure_ssl_environment()

        # Silence LiteLLM's loggers BEFORE the import so the cost-map
        # fetch warning ("Failed to fetch remote model cost map …
        # SSL: CERTIFICATE_VERIFY_FAILED … falling back to local
        # backup") doesn't leak through the CLI's clean prompt. The
        # warning is informational — LiteLLM ships a local backup and
        # uses it on fetch failure — but on corporate networks with
        # MITM TLS proxies it fires every single import and clutters
        # the chat. Setting LITELLM_LOCAL_MODEL_COST_MAP=True skips
        # the network call entirely; pinning the loggers handles any
        # other start-up chatter LiteLLM emits.
        os.environ.setdefault("LITELLM_LOCAL_MODEL_COST_MAP", "True")
        for logger_name in ("LiteLLM", "litellm"):
            ext_logger = logging.getLogger(logger_name)
            ext_logger.handlers.clear()
            ext_logger.addHandler(logging.NullHandler())
            ext_logger.propagate = False
            ext_logger.setLevel(logging.CRITICAL + 1)

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

        _install_structured_content_shim(lm)
        _litellm_module = lm
    return _litellm_module


def _flatten_structured_content(content: Any) -> str | None:
    """Coerce OpenAI-Responses-style structured content into a plain string.

    Some hosted endpoints — Databricks Foundation Models (gpt-oss family),
    OpenAI's o1 / o3 over the Responses API, certain self-hosted reasoning
    models — return ``message.content`` as a list of structured items::

        [
            {"type": "reasoning", "summary": [{"type": "summary_text", ...}]},
            {"type": "text", "text": "OK"},
        ]

    LiteLLM's ``Message`` pydantic model declares ``content: str``, so the
    response normalizer rejects the payload with::

        ValidationError: 1 validation error for Message
        content: Input should be a valid string

    Without a fix the call retries 3× and ultimately surfaces a confusing
    ``InternalServerError: Invalid response object`` to the user despite
    the upstream API having returned a valid (and successful) answer.

    This helper extracts every ``text`` chunk from the list and concatenates
    them. Reasoning summaries are dropped on the floor — the LLM provider's
    streaming path already exposes reasoning content separately via
    ``on_thinking``; embedding it in ``content`` would corrupt downstream
    parsers (catalog/code agent JSON, deterministic answer extraction, …).

    Returns ``None`` when the input isn't a list — caller should leave the
    original value untouched.
    """
    if not isinstance(content, list):
        return None
    parts: list[str] = []
    for item in content:
        if not isinstance(item, dict):
            continue
        kind = str(item.get("type") or "").lower()
        if kind in {"text", "output_text"}:
            text = item.get("text")
            if isinstance(text, str) and text:
                parts.append(text)
    return "".join(parts)


def _normalize_response_dict_in_place(response_obj: Any) -> None:
    """Walk a LiteLLM response dict and flatten any structured ``content`` lists.

    Mutates in place. Safe to call on any value — non-dict / non-list inputs
    are ignored. Touches both ``message.content`` (chat completions) and
    ``delta.content`` (streaming chunks).
    """
    if not isinstance(response_obj, dict):
        return
    choices = response_obj.get("choices")
    if not isinstance(choices, list):
        return
    for choice in choices:
        if not isinstance(choice, dict):
            continue
        for slot in ("message", "delta"):
            payload = choice.get(slot)
            if not isinstance(payload, dict):
                continue
            content = payload.get("content")
            flattened = _flatten_structured_content(content)
            if flattened is not None:
                payload["content"] = flattened


def _install_structured_content_shim(lm: ModuleType) -> None:
    """Belt-and-suspenders flatten of list-shaped ``message.content``.

    Two layers because patching ``convert_to_model_response_object``
    alone leaks: callers that did ``from … convert_dict_to_response
    import convert_to_model_response_object`` at module load time hold
    a frozen reference to the original function, and our module-level
    rebind doesn't reach them. Patching the ``Message`` pydantic class
    closes that gap because every code path eventually constructs the
    same ``Message`` object — there's only one class.

    Layer 1: wrap ``convert_to_model_response_object`` so the raw
    response dict is normalised before the function ever builds a
    Message. Cheap and covers the common path.

    Layer 2: wrap ``Message.__init__`` so any direct Message(content=
    [...]) construction also flattens. This is what actually defends
    against the user-reported ``InternalServerError: Invalid response
    object`` from Databricks Foundation Models (gpt-oss family) and
    similar Responses-API-shaped endpoints — the first layer was
    silently bypassed because litellm internally imports the
    converter under a local alias.

    Both layers are idempotent so repeat ``litellm()`` lookups never
    install the wrapper twice.
    """
    try:
        from litellm.litellm_core_utils.llm_response_utils import (
            convert_dict_to_response as _conv_mod,
        )
    except Exception:  # pragma: no cover - defensive against LiteLLM internals shifting
        _conv_mod = None  # type: ignore[assignment]
    if _conv_mod is not None and not getattr(_conv_mod, "_amx_structured_content_shim", False):
        original = _conv_mod.convert_to_model_response_object

        def _patched(*args: Any, **kwargs: Any):
            response_object = kwargs.get("response_object")
            if response_object is None and args:
                response_object = args[0]
            try:
                _normalize_response_dict_in_place(response_object)
            except Exception:  # pragma: no cover - never block on a flatten error
                pass
            return original(*args, **kwargs)

        _conv_mod.convert_to_model_response_object = _patched
        _conv_mod._amx_structured_content_shim = True

    # Layer 2 — class-level Message patch. Captures every Message
    # construction across litellm regardless of which module imported
    # ``convert_to_model_response_object`` under what alias.
    try:
        from litellm.types.utils import Message as _LiteLLMMessage  # type: ignore[attr-defined]
    except Exception:  # pragma: no cover - litellm typing module relocated upstream
        return
    if getattr(_LiteLLMMessage, "_amx_structured_content_shim", False):
        return
    _orig_init = _LiteLLMMessage.__init__

    def _patched_init(self: Any, *args: Any, **kwargs: Any) -> None:
        # Flatten BEFORE pydantic validation runs. The Responses-API
        # output (reasoning + text typed parts) needs to collapse to
        # the single text string Message's content field expects;
        # reasoning is dropped here because the provider layer surfaces
        # it through ``on_thinking`` already and embedding it in the
        # content string would corrupt downstream JSON parsers.
        content = kwargs.get("content")
        if isinstance(content, list):
            flattened = _flatten_structured_content(content)
            if flattened is not None:
                kwargs["content"] = flattened
        return _orig_init(self, *args, **kwargs)

    _LiteLLMMessage.__init__ = _patched_init  # type: ignore[assignment]
    _LiteLLMMessage._amx_structured_content_shim = True  # type: ignore[attr-defined]


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
    # Visible reasoning text produced by the model when extended thinking /
    # reasoning is enabled (Anthropic ``thinking`` blocks, DeepSeek-reasoner
    # ``reasoning_content``, etc.). Empty for the default non-streaming path.
    thinking_content: str = ""
    thinking_tokens: int = 0

    def __str__(self) -> str:  # noqa: D105
        return self.content


@dataclass
class LLMTestResult:
    ok: bool
    message: str = ""


# ── Streamed-response shim ──────────────────────────────────────────────────
#
# When ``chat()`` runs in streaming mode (``on_thinking`` callback supplied
# AND model supports reasoning), we consume the LiteLLM stream inline and
# repackage the final state as a non-streamed-shape object so the rest of
# the response-parsing path (logprobs, tool_calls, finish_reason, usage)
# stays unchanged. These minimal classes mirror the attribute access
# pattern LiteLLM uses on its own response objects.


class _StreamedFunction:
    def __init__(self, name: str, arguments: str) -> None:
        self.name = name
        self.arguments = arguments


class _StreamedToolCall:
    def __init__(self, id_: str, name: str, arguments: str) -> None:
        self.id = id_
        self.function = _StreamedFunction(name, arguments)


class _StreamedMessage:
    def __init__(self, content: str, tool_calls: list[_StreamedToolCall]) -> None:
        self.content = content
        self.tool_calls = tool_calls


class _StreamedChoice:
    def __init__(
        self,
        content: str,
        tool_calls: list[_StreamedToolCall],
        finish_reason: str | None,
    ) -> None:
        self.message = _StreamedMessage(content, tool_calls)
        self.finish_reason = finish_reason
        self.logprobs = None


class _StreamedUsage:
    def __init__(self, prompt: int, completion: int, total: int, thinking: int) -> None:
        self.prompt_tokens = prompt
        self.completion_tokens = completion
        self.total_tokens = total
        self.thinking_tokens = thinking


class _StreamedResponse:
    def __init__(
        self,
        content: str,
        thinking_content: str,
        tool_calls: list[_StreamedToolCall],
        finish_reason: str | None,
        usage: _StreamedUsage | None,
    ) -> None:
        self.choices = [_StreamedChoice(content, tool_calls, finish_reason)]
        self.usage = usage
        self.thinking_content = thinking_content
        self.thinking_tokens = usage.thinking_tokens if usage else 0


def _consume_thinking_stream(
    iterator: Any,
    on_thinking: Callable[[str], None] | None = None,
    on_content: Callable[[str], None] | None = None,
    cancel_token: Any | None = None,
) -> _StreamedResponse:
    """Drain a LiteLLM stream, fire ``on_thinking`` / ``on_content`` deltas, and rebuild a response.

    Reasoning text arrives as ``delta.reasoning_content`` chunks (LiteLLM
    normalizes this across Anthropic ``thinking_blocks`` and DeepSeek/OpenAI
    ``reasoning_content``). Visible answer text arrives as ``delta.content``
    chunks; ``on_content`` (when provided) is fired per-delta so the caller can
    forward token-level streaming to its UI. Tool calls arrive split by
    ``index`` and we reassemble each by id/name/arguments-suffix. Usage
    typically arrives in a trailing chunk with no choices.

    ``on_thinking`` is fired with the CUMULATIVE thinking text so far (matches
    the historical contract); ``on_content`` is fired with the PER-CHUNK delta
    so a UI can append directly.
    """
    content_parts: list[str] = []
    thinking_parts: list[str] = []
    tool_slots: dict[int, dict[str, str]] = {}
    finish_reason: str | None = None
    prompt_tokens = 0
    completion_tokens = 0
    total_tokens = 0
    thinking_tokens = 0

    for chunk in iterator:
        # Cancellation handshake — the worker thread shares its
        # ``threading.Event`` with the JobRegistry. A Cancel POST flips
        # the event; we bail between chunks so the SSE producer can fire
        # ``job.cancelled`` and the SPA's "Cancelling…" pill stops
        # waiting on a stream that would otherwise keep draining until
        # the LLM is done. Without this, the in-flight LiteLLM HTTP
        # request is uninterruptible and the whole answer lands anyway.
        if cancel_token is not None and cancel_token.is_set():
            from amx.agents.orchestrator import RunCancelled

            raise RunCancelled("Cancelled mid-stream")
        usage = getattr(chunk, "usage", None)
        if usage is not None:
            prompt_tokens = int(getattr(usage, "prompt_tokens", 0) or prompt_tokens)
            completion_tokens = int(getattr(usage, "completion_tokens", 0) or completion_tokens)
            total_tokens = int(getattr(usage, "total_tokens", 0) or total_tokens)
            details = getattr(usage, "completion_tokens_details", None)
            if details is not None:
                thinking_tokens = int(getattr(details, "reasoning_tokens", 0) or thinking_tokens)

        choices = getattr(chunk, "choices", None) or []
        if not choices:
            continue
        choice0 = choices[0]
        delta = getattr(choice0, "delta", None)
        if delta is not None:
            rc = getattr(delta, "reasoning_content", None) or ""
            if rc:
                thinking_parts.append(rc)
                if on_thinking is not None:
                    try:
                        on_thinking("".join(thinking_parts))
                    except Exception as cb_exc:
                        log.debug("on_thinking callback raised: %s", cb_exc)

            cc = getattr(delta, "content", None) or ""
            if cc:
                content_parts.append(cc)
                if on_content is not None:
                    try:
                        on_content(cc)
                    except Exception as cb_exc:
                        log.debug("on_content callback raised: %s", cb_exc)

            raw_tcs = getattr(delta, "tool_calls", None) or []
            for tc in raw_tcs:
                idx = int(getattr(tc, "index", 0) or 0)
                slot = tool_slots.setdefault(idx, {"id": "", "name": "", "arguments": ""})
                tid = getattr(tc, "id", None)
                if tid:
                    slot["id"] = str(tid)
                fn = getattr(tc, "function", None)
                if fn is not None:
                    nm = getattr(fn, "name", None)
                    if nm:
                        slot["name"] = str(nm)
                    args_chunk = getattr(fn, "arguments", None)
                    if args_chunk:
                        slot["arguments"] += str(args_chunk)

        fr = getattr(choice0, "finish_reason", None)
        if fr:
            finish_reason = str(fr)

    tool_calls_list: list[_StreamedToolCall] = [
        _StreamedToolCall(
            slot["id"] or f"tool_stream_{idx}",
            slot["name"],
            slot["arguments"] or "{}",
        )
        for idx, slot in sorted(tool_slots.items())
        if slot["name"]
    ]

    usage_obj: _StreamedUsage | None = None
    if prompt_tokens or completion_tokens or total_tokens or thinking_tokens:
        usage_obj = _StreamedUsage(prompt_tokens, completion_tokens, total_tokens, thinking_tokens)

    return _StreamedResponse(
        content="".join(content_parts),
        thinking_content="".join(thinking_parts),
        tool_calls=tool_calls_list,
        finish_reason=finish_reason,
        usage=usage_obj,
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
    if provider == "databricks_serving":
        # Users routinely paste either:
        #   * the bare workspace host (``adb-xxxxxxxxxxxxxxxx.0.azuredatabricks.net``)
        #   * the workspace URL (``https://adb-…/``)
        #   * the full chat-completions path the wizard built (``…/serving-endpoints``)
        # Coerce all of them to the Databricks chat-completions root —
        # ``https://<host>/serving-endpoints`` — which is what LiteLLM's
        # OpenAI client appends ``/<endpoint>/invocations`` to.
        stripped = base.rstrip("/")
        scheme = ""
        host_path = stripped
        for candidate in ("https://", "http://"):
            if host_path.lower().startswith(candidate):
                scheme = candidate
                host_path = host_path[len(candidate) :]
                break
        if not scheme:
            scheme = "https://"
        # Drop any leading double-slash artefact and trim again.
        host_path = host_path.lstrip("/")
        if "/serving-endpoints" not in host_path.lower():
            host_path = host_path.split("/", 1)[0] + "/serving-endpoints"
        return f"{scheme}{host_path}".rstrip("/")
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


class LLMProvider:
    """Thin wrapper around LiteLLM so every agent uses the same calling convention."""

    def __init__(self, cfg: LLMConfig):
        self.cfg = cfg
        # When the keyring backend is unreachable (Keychain ACL miss
        # after a binary path change on macOS, gnome-keyring /
        # KWallet not running on Linux, Credential Manager access
        # denied on Windows, …), the YAML's
        # ``keyring:llm_profiles/<name>/api_key`` reference falls
        # through to ``cfg.api_key`` unresolved. Compute an effective
        # key for outgoing auth without mutating the dataclass —
        # mutating would propagate through ``cfg.save()`` and erase
        # the YAML reference for future runs that have a healthy
        # backend.
        from amx.storage.secrets import is_secret_reference

        if is_secret_reference(cfg.api_key):
            env_fallback = os.getenv("AMX_LLM_API_KEY", "")
            log.warning(
                "%s api_key is an unresolved keyring reference (backend unavailable?). %s",
                cfg.provider or "(provider)",
                "Using AMX_LLM_API_KEY env fallback."
                if env_fallback
                else "No AMX_LLM_API_KEY env fallback set; calls will fail until "
                "the keyring backend recovers or you run /llm to re-enter the key.",
            )
            self._effective_api_key = env_fallback
        else:
            self._effective_api_key = cfg.api_key or ""
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
        # ``_effective_api_key`` is the resolved key we should auth with —
        # ``cfg.api_key`` may still be a ``keyring:`` reference when the
        # backend was offline at config-load time, and we don't want to
        # leak the reference into upstream Authorization headers.
        api_key = self._effective_api_key
        env_key = PROVIDER_ENV_KEY.get(self.cfg.provider)
        if env_key and api_key:
            os.environ[env_key] = api_key

        normalized_base = _normalized_api_base(self.cfg.provider, self.cfg.api_base)
        if normalized_base != self.cfg.api_base:
            log.info(
                "Normalizing %s api_base from '%s' to '%s'",
                self.cfg.provider,
                self.cfg.api_base,
                normalized_base,
            )
            self.cfg.api_base = normalized_base

        if self.cfg.provider in ("local", "kimi", "databricks_serving"):
            if self.cfg.api_base:
                os.environ["OPENAI_API_BASE"] = self.cfg.api_base
            # Databricks Serving rejects an empty bearer; fall back to a
            # placeholder so LiteLLM doesn't strip the Authorization header,
            # but the real PAT (when supplied) wins.
            os.environ.setdefault("OPENAI_API_KEY", api_key or "local")
        elif self.cfg.provider == "openrouter":
            if self.cfg.api_base:
                os.environ["OPENAI_API_BASE"] = self.cfg.api_base
            # LiteLLM OpenRouter path expects OpenAI-style key wiring.
            if api_key:
                os.environ["OPENROUTER_API_KEY"] = api_key
                os.environ["OPENAI_API_KEY"] = api_key
            else:
                os.environ.setdefault("OPENROUTER_API_KEY", "")
        elif self.cfg.provider == "ollama":
            if self.cfg.api_base:
                os.environ["OLLAMA_API_BASE"] = self.cfg.api_base
            # Some LiteLLM versions still check for a key even if unused
            os.environ.setdefault("OLLAMA_API_KEY", api_key or "ollama")

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
        on_thinking: Callable[[str], None] | None = None,
        on_content: Callable[[str], None] | None = None,
        cancel_token: Any | None = None,
        **kwargs: Any,
    ) -> ChatResult:
        # ``_amx_rerun_attempt`` is consumed by the auto-retry path below
        # (length+empty reasoning failure). Pull it out of kwargs before
        # the dict reaches LiteLLM, which would 400 on an unknown param.
        _retry_attempt = int(kwargs.pop("_amx_rerun_attempt", 0) or 0)
        model = self.model_name
        mt = max_tokens or self.cfg.max_tokens
        extra: dict[str, Any] = dict(kwargs)
        # Engage streaming when the caller wants live visible-content deltas
        # (any model) OR live thinking on a model that emits reasoning. The
        # thinking-specific extras below only fire when the route actually
        # supports thinking; ``on_content`` callers on non-reasoning models
        # get vanilla token streaming with no provider-side reasoning knobs.
        supports_thinking = _supports_thinking(self.cfg.provider, model)
        use_streaming = on_content is not None or (on_thinking is not None and supports_thinking)
        if use_streaming and supports_thinking:
            budget = max(1024, int(getattr(self.cfg, "thinking_budget", 1024)))
            if self.cfg.provider == "anthropic":
                extra["thinking"] = {"type": "enabled", "budget_tokens": budget}
                # Anthropic requires ``max_tokens > thinking.budget_tokens``;
                # leave headroom so the model has room for visible output.
                if mt < budget + 512:
                    mt = budget + 512
                # Anthropic's extended-thinking API rejects temperature != 1
                # and is incompatible with logprobs. Override both rather
                # than letting the request fail.
                temperature = 1.0
                use_logprobs = False
                for k in ("logprobs", "top_logprobs"):
                    extra.pop(k, None)
            elif self.cfg.provider == "openrouter":
                # OpenRouter omits reasoning tokens from responses by default —
                # without an opt-in the stream contains content but no
                # ``reasoning_content`` deltas, so the thinking panel stays
                # empty even though everything else is wired up.
                #
                # The OpenRouter API enforces ``effort`` XOR ``max_tokens`` and
                # 400s if both are sent ("Only one of reasoning.effort and
                # reasoning.max_tokens can be specified"). We pick ``effort``
                # because it works across every reasoning route OpenRouter
                # currently fronts (o-series, gpt-5, kimi-k2-thinking, Claude
                # extended-thinking, deepseek-reasoner) — the downstream
                # provider gets the right knob set for it. Users wanting
                # explicit Anthropic budget control should use the Anthropic
                # provider directly.
                effort = os.getenv("AMX_REASONING_EFFORT", "low").strip().lower()
                if effort not in ("low", "medium", "high"):
                    effort = "low"
                extra.setdefault("reasoning", {"effort": effort})
                # Reasoning models burn output tokens on internal thinking
                # before producing visible content — without a generous
                # ``max_tokens`` floor the model exhausts its budget mid-
                # thought and returns empty content with finish_reason=length,
                # which the existing guard surfaces as a fatal "0 visible
                # characters" error. Match the OpenAI-direct behaviour at
                # the bottom of this method so OpenRouter routes get the
                # same headroom.
                floor = int(os.getenv("AMX_LLM_MIN_MAX_TOKENS", str(_DEFAULT_REASONING_FLOOR)))
                if mt < floor:
                    log.debug(
                        "Raising max_tokens %d -> %d for OpenRouter reasoning model %s",
                        mt,
                        floor,
                        model,
                    )
                    mt = floor
                # logprobs are unsupported on most reasoning routes; drop to
                # avoid 400s before the streamed call goes out.
                use_logprobs = False
                for k in ("logprobs", "top_logprobs"):
                    extra.pop(k, None)
            elif self.cfg.provider == "deepseek":
                # DeepSeek-reasoner returns reasoning_content natively in the
                # stream; just drop logprobs which the route doesn't accept.
                use_logprobs = False
                for k in ("logprobs", "top_logprobs"):
                    extra.pop(k, None)
        if use_streaming:
            # Ask the provider to emit a final usage chunk so we can capture
            # reasoning_tokens (when applicable) for telemetry. Applies to
            # plain content-streaming routes too so usage accounting stays
            # consistent with the non-streaming path.
            extra.setdefault("stream_options", {"include_usage": True})

        # Honor a runtime-discovered disable flag — set lower in the
        # exception path when a provider returns
        # ``Logprobs is not enabled for this model`` (Gemini Flash,
        # OpenAI o-series, some others). Without this, every subsequent
        # call in the same session would re-trigger the same 400.
        if use_logprobs and getattr(self, "_logprobs_runtime_disabled", False):
            use_logprobs = False
        # Providers that NEVER accept ``logprobs`` / ``top_logprobs`` —
        # disabled unconditionally (streaming + non-streaming) so calls
        # that don't go through the use_streaming branch above
        # (``LLMProvider.test_result``, the profile / RAG / code agents
        # in CHAT mode) don't trip the same 400. Databricks Foundation
        # Model Serving's OpenAI shim rejects both kwargs outright on
        # its Anthropic-backed Claude endpoints; the literal error is
        # ``top_logprobs: Extra inputs are not permitted``.
        if self.cfg.provider == "databricks_serving":
            use_logprobs = False
            for k in ("logprobs", "top_logprobs"):
                extra.pop(k, None)
        if use_logprobs:
            # Force-request logprobs regardless of provider capability metadata.
            extra["logprobs"] = True
            # Keep OpenAI-compatible top-k logprob detail where supported.
            extra.setdefault("top_logprobs", 5)
            # Some local/Ollama-compatible backends require explicit integer hint.
            if self.cfg.provider == "ollama":
                extra.setdefault("num_probs", 5)

        # Reasoning models: raise floor so visible content can appear after thinking tokens.
        # Applies to every provider/model the streaming path treats as a reasoning route
        # (OpenRouter's Kimi K2.x family, generic *-thinking / *-reasoner / *-reasoning
        # routes, Anthropic extended-thinking, deepseek-reasoner, OpenAI o-series / gpt-5).
        # Without this, non-streamed callers (profile / code / rag agents in CHAT mode)
        # keep the LLMConfig.max_tokens budget and routinely truncate with
        # finish_reason=length once the model has spent it on internal thinking.
        if _supports_thinking(self.cfg.provider, model):
            floor = int(os.getenv("AMX_LLM_MIN_MAX_TOKENS", str(_DEFAULT_REASONING_FLOOR)))
            if mt < floor:
                log.debug(
                    "Raising max_tokens %d -> %d for reasoning model %s",
                    mt,
                    floor,
                    model,
                )
                mt = floor
            # ``reasoning_effort`` is OpenAI's parameter shape; OpenRouter uses
            # ``reasoning: {effort: ...}`` (set in the streaming branch above).
            # Other providers manage it via their own thinking budgets, so only
            # forward this kwarg for OpenAI direct.
            if self.cfg.provider == "openai":
                effort = os.getenv("AMX_REASONING_EFFORT", "low").strip().lower()
                if effort in ("none", "minimal", "low", "medium", "high"):
                    extra.setdefault("reasoning_effort", effort)

        log.debug("LLM call -> model=%s, max_tokens=%d", model, mt)
        call_api_base = (
            self.cfg.api_base
            if self.cfg.provider
            in (
                "local",
                "kimi",
                "ollama",
                "openrouter",
                "databricks_serving",
            )
            else None
        )

        # Resolve timeout once per call. ``extra`` is the user-passed kwargs;
        # if a caller wants to override the default for a specific call they
        # can pass ``timeout=N``. Otherwise we fall back to the env-tunable
        # default so no LLM call can hang indefinitely.
        if "timeout" not in extra and "request_timeout" not in extra:
            extra["timeout"] = _llm_timeout_sec()

        def _do_completion(api_base_override: str | None) -> Any:
            explicit_api_key = (
                self._effective_api_key if self.cfg.provider == "openrouter" else None
            )
            kwargs_for_call = dict(extra)
            if use_streaming:
                kwargs_for_call["stream"] = True
            raw = _litellm().completion(
                model=model,
                messages=messages,
                temperature=temperature or self.cfg.temperature,
                max_tokens=mt,
                api_key=explicit_api_key,
                api_base=api_base_override,
                **kwargs_for_call,
            )
            if use_streaming:
                # Consume the stream inside the retry context: any mid-stream
                # error surfaces as the same exception class as a non-streamed
                # failure, so the existing transient-retry / fatal-error
                # classification keeps working. ``cancel_token`` lets the
                # consumer bail between chunks the moment the Studio Cancel
                # button is clicked, instead of draining the full answer.
                return _consume_thinking_stream(
                    raw,
                    on_thinking=on_thinking,
                    on_content=on_content,
                    cancel_token=cancel_token,
                )
            return raw

        t0 = time.perf_counter()
        resp = None
        last_exc: BaseException | None = None
        for attempt in range(MAX_LLM_RETRIES + 1):
            # Cancel short-circuit. The orchestrator binds the active
            # job's ``cancel_token`` via ``bind_cancel_token`` in the
            # run worker; checking here means a Studio Cancel click
            # bails out of the NEXT LLM call instead of waiting for
            # the current chain of retries to complete naturally. The
            # in-flight HTTP request can't be interrupted mid-call
            # (litellm is synchronous), but every retry budget the
            # token saves is many seconds the user no longer waits.
            from amx.utils.cancel import raise_if_cancelled

            raise_if_cancelled(phase=f"LLM call (attempt {attempt + 1})")
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
                        "LLM transient failure (attempt %d/%d)  --  retrying in %.1fs: %s",
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
            tt = int(getattr(raw_usage, "thinking_tokens", 0) or 0)
            if tt:
                usage_dict["thinking_tokens"] = tt
        else:
            usage_dict = {"model_processing_sec": elapsed_sec}

        logprobs_content: list | None = None
        try:
            raw_lp = getattr(choice, "logprobs", None)
            if raw_lp is not None:
                logprobs_content = getattr(raw_lp, "content", None) or None
            if use_logprobs and not logprobs_content:
                # Some providers silently return no logprobs even when the
                # request flag was accepted (no 400). The fallback to
                # heuristic confidence is automatic, so demote this to
                # DEBUG — surfacing it as WARNING fires during every
                # spinner render and confuses users into thinking
                # something failed when nothing did.
                log.debug(
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
                # Auto-recovery path: a reasoning route burned every token
                # on internal thinking. Before surfacing this as a fatal
                # error to the user, retry the call once with a much larger
                # ``max_tokens`` budget — modern reasoning models often
                # need 32-64k tokens of headroom before emitting visible
                # output, and the user shouldn't have to set an env var
                # for every newly-released route. Bounded by
                # ``_REASONING_AUTO_RETRY_CAP`` so a runaway model can't
                # drain a credit balance.
                if _retry_attempt == 0 and mt < _REASONING_AUTO_RETRY_CAP:
                    bumped = min(_REASONING_AUTO_RETRY_CAP, max(mt * 4, mt + 32_768))
                    log.warning(
                        "LLM `%s` returned 0 visible chars at max_tokens=%d "
                        "(reasoning budget exhausted). Auto-retrying once with "
                        "max_tokens=%d before failing.",
                        model,
                        mt,
                        bumped,
                    )
                    return self.chat(
                        messages,
                        temperature=temperature,
                        max_tokens=bumped,
                        use_logprobs=use_logprobs,
                        on_thinking=on_thinking,
                        _amx_rerun_attempt=_retry_attempt + 1,
                        **kwargs,
                    )
                raise FatalLLMError(
                    (
                        f"Model `{model}` returned 0 visible characters and used all "
                        f"{mt} output tokens even after AMX auto-retried with a "
                        "larger budget — the model is spending its entire output "
                        "allowance on internal reasoning and never gets to visible "
                        "JSON. Pick a non-reasoning model under /llm, e.g.: "
                        "`openrouter/anthropic/claude-haiku-4.5`, "
                        "`openrouter/google/gemini-2.5-flash`, "
                        "`openrouter/openai/gpt-5.4-mini`, or "
                        "`openrouter/deepseek/deepseek-chat-v4`. "
                        "If you must keep this model, raise max_tokens via "
                        "/max-tokens (CLI) or Settings → LLM (Studio), or set "
                        "AMX_LLM_MIN_MAX_TOKENS=65536 AMX_REASONING_EFFORT=minimal."
                    ),
                    original_message=(
                        f"finish_reason=length, content_chars=0, max_tokens={mt}, "
                        f"model={model}, retry_attempt={_retry_attempt}"
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
            thinking_content=str(getattr(resp, "thinking_content", "") or ""),
            thinking_tokens=int(getattr(resp, "thinking_tokens", 0) or 0),
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
