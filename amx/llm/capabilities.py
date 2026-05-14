"""Knob-applicability checks shared by Studio's capability-gating UI
and the backend's runtime guardrails.

The Studio's *Advanced LLM settings* panel grays out / hides knobs that
the selected ``(provider, model)`` doesn't honour — e.g. ``thinking_budget``
on a non-reasoning model, ``logprob_high`` / ``logprob_medium`` on
Gemini Flash / OpenAI o-series. Without a central table the heuristics
would drift between three places (frontend, backend picker, runtime
provider check), so this module owns them.

Wraps :func:`amx.llm.provider._supports_thinking` (Anthropic Sonnet 3.7+
/ Opus 4+, DeepSeek-reasoner, OpenAI o-series, OpenRouter reasoning
routes) and adds a static "logprobs unsupported" model substring table
mirroring the runtime error patterns in ``_LOGPROBS_UNSUPPORTED_PATTERNS``.
The runtime fallback in :class:`LLMProvider.chat` still catches anything
this static table misses — e.g. a brand-new reasoning route — so the
static gate is purely a UX hint.
"""

from __future__ import annotations

from amx.llm.provider import _supports_thinking

# Provider/model substrings whose logprobs request is rejected with one
# of the ``_LOGPROBS_UNSUPPORTED_PATTERNS`` 400 responses. Anything not
# matched here is *assumed* to support logprobs and the runtime
# fallback handles the few that don't.
_LOGPROBS_UNSUPPORTED_MODEL_TOKENS: tuple[tuple[str, tuple[str, ...]], ...] = (
    # Anthropic native API never exposed logprobs.
    ("anthropic", ()),
    # Gemini Flash family rejects with "Logprobs is not enabled for this model".
    ("gemini", ("flash",)),
    # OpenAI reasoning models (o1, o3, o4, gpt-5) reject the parameter.
    (
        "openai",
        (
            "o1",
            "o3",
            "o4",
            "gpt-5",
        ),
    ),
    # OpenRouter routes the same Anthropic + Gemini + OpenAI reasoning models
    # — sniff by model substring.
    (
        "openrouter",
        (
            "anthropic/",
            "claude-",
            "/flash",
            "-flash",
            "/o1",
            "/o3",
            "/o4",
            "gpt-5",
            "thinking",
            "reasoner",
            "reasoning",
        ),
    ),
    # Databricks Foundation Model Serving's OpenAI shim rejects logprobs on
    # its Anthropic-backed Claude endpoints with "top_logprobs: extra inputs
    # are not permitted". See :data:`amx.llm.provider._LOGPROBS_UNSUPPORTED_PATTERNS`.
    ("databricks_serving", ()),
)


def supports_thinking(provider: str, model: str) -> bool:
    """Whether the model emits a reasoning / extended-thinking trace.

    Thin wrapper around :func:`amx.llm.provider._supports_thinking` so
    callers (CLI picker, Studio gate, backend runtime) read the same
    table. True ⇒ ``thinking_budget`` is honoured; False ⇒ the knob
    has no effect and the UI should disable it.
    """
    return _supports_thinking(provider, model)


def supports_logprobs(provider: str, model: str) -> bool:
    """Whether the provider/model accepts ``logprobs=True`` requests.

    Static table sourced from observed 400-rejection providers (see
    :data:`amx.llm.provider._LOGPROBS_UNSUPPORTED_PATTERNS`). Used for
    pre-call UI gating only — the runtime path in
    :meth:`LLMProvider.chat` still catches anything this list misses
    via the error-substring sniffer and falls back to a no-logprobs
    retry.
    """
    p = (provider or "").lower()
    m = (model or "").lower()
    for prov, tokens in _LOGPROBS_UNSUPPORTED_MODEL_TOKENS:
        if p != prov:
            continue
        if not tokens:
            return False
        if any(tok in m for tok in tokens):
            return False
    return True


__all__ = ["supports_thinking", "supports_logprobs"]
