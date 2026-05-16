"""LiteLLM bootstrap + structured-content compatibility shims.

Extracted from :mod:`amx.llm.provider`. Holds three concerns that
historically lived as module-level helpers in ``provider.py``:

* Lazy LiteLLM import (:func:`_litellm`) — defers the heavy
  ``import litellm`` until the first chat call, sets the cost-map env
  hint, silences LiteLLM's loggers, and re-applies the SSL override
  once the module is loaded.
* Corporate-network SSL handling (:func:`_configure_ssl_environment`) —
  honours ``AMX_INSECURE_SSL=1`` by flipping both ``litellm.ssl_verify``
  and ``PYTHONHTTPSVERIFY`` so the underlying httpx clients used by
  the openai / anthropic SDKs also bypass verification.
* Structured-content shim (:func:`_install_structured_content_shim`,
  :func:`_normalize_response_dict_in_place`,
  :func:`_flatten_structured_content`) — flattens list-shaped
  ``message.content`` returned by OpenAI Responses-API-shaped
  endpoints (Databricks Foundation Models, certain reasoning routes)
  so LiteLLM's strict ``content: str`` validator stops rejecting valid
  responses.

The single ``_litellm_module`` global lives here too because it is
the cache the shim install uses to know whether the wrappers were
already applied this session. ``provider.py`` re-exports the public
names (``_litellm``, ``_install_structured_content_shim``) so any
historical patch-site still works.
"""

from __future__ import annotations

import logging
import os
from types import ModuleType
from typing import Any

from amx.utils.logging import get_logger

log = get_logger("llm.provider.shims")

# Lazy import: LiteLLM has had circular-import issues when the top-level package is
# pulled in during a heavy dependency graph (e.g. interactive /run). Defer import
# until first LLM use. See https://github.com/BerriAI/litellm/issues/14717
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
    _LiteLLMMessage._amx_structured_content_shim = True
