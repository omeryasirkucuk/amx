"""Provider routing config + reasoning-model detection for :class:`LLMProvider`.

Extracted from :mod:`amx.llm.provider` so the provider-keyed lookup
tables (model-id prefix, env-var name) and the small reasoning-model
sniffers live in one focused module. The reasoning helpers stay
data-driven — they read the model identifier and ask "is this a
reasoning route?" without touching LLM call state — so they are pure
and trivially testable.

``provider.py`` re-exports the public names so historical imports
(``from amx.llm.provider import PROVIDER_MODEL_PREFIX``) and
patched-symbol references continue to work unchanged.
"""

from __future__ import annotations

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
    # Databricks Foundation Model Serving + custom serving endpoints expose
    # an OpenAI-compatible REST surface at
    # ``<workspace>/serving-endpoints/<endpoint>/invocations`` (and the
    # chat-completions alias). LiteLLM routes those via the openai client,
    # so we use the same prefix as ``local`` / ``kimi``. The wizard adds the
    # ``serving-endpoints`` suffix to the api_base so users only paste the
    # workspace host.
    "databricks_serving": "openai/",
}


PROVIDER_ENV_KEY = {
    "openai": "OPENAI_API_KEY",
    "openrouter": "OPENROUTER_API_KEY",
    "anthropic": "ANTHROPIC_API_KEY",
    "gemini": "GEMINI_API_KEY",
    "deepseek": "DEEPSEEK_API_KEY",
    "databricks_serving": "DATABRICKS_TOKEN",
}


_DEFAULT_REASONING_FLOOR = 32_768


_REASONING_AUTO_RETRY_CAP = 131_072


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


def _supports_thinking(provider: str, model: str) -> bool:
    """Whether this provider/model emits a stream of reasoning content.

    True for Anthropic Claude with extended thinking (Sonnet 3.7+, Sonnet/Opus
    4+), DeepSeek-reasoner, and OpenAI reasoning models. OpenRouter routes
    these too, so we sniff the model substring there as well.
    """
    p = (provider or "").lower()
    m = (model or "").lower()
    if p == "anthropic":
        return any(
            tag in m
            for tag in (
                "claude-sonnet-4",
                "claude-opus-4",
                "claude-3-7-sonnet",
                "claude-3.7-sonnet",
            )
        )
    if p == "deepseek":
        return "reasoner" in m
    if p == "openai":
        return _is_openai_reasoning_style_model(model)
    if p == "openrouter":
        # Reuse the OpenAI sniffer for o-series / gpt-5 routes (covers o1,
        # o3, o4, gpt-5 in any vendor-prefixed form). Then add named
        # routes for non-OpenAI reasoning models OpenRouter fronts.
        if _is_openai_reasoning_style_model(model):
            return True
        # Generic "thinking" / "reasoner" / "reasoning" substring match
        # so newly-launched reasoning routes are caught automatically
        # without code changes — every major lab now uses one of these
        # tokens in the route name (kimi-thinking, qwen3-thinking,
        # glm-4.6-thinking, deepseek-reasoner, …). False positives on
        # non-reasoning models that happen to contain the substring are
        # harmless: the only effect is a higher max_tokens floor.
        if any(tok in m for tok in ("thinking", "reasoner", "reasoning")):
            return True
        return any(
            tag in m
            for tag in (
                "claude-sonnet-4",
                "claude-opus-4",
                "claude-3-7-sonnet",
                # Kimi K2.x — every 2.x point release ships a thinking
                # mode by default (k2.6, k2.7, …). Match the family
                # rather than each version so we don't have to keep up.
                "kimi-k2",
                "kimi-2",
            )
        )
    return False
