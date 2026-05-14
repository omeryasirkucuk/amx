"""LLM capability lookup endpoint for Studio's gating UI.

The Studio's *Advanced LLM settings* panel disables knobs the selected
provider/model can't honour (``thinking_budget`` on non-reasoning
models, ``logprob_high`` / ``logprob_medium`` on Gemini Flash / OpenAI
o-series, etc.). To avoid the heuristics living in two places, the
frontend asks the backend instead — :mod:`amx.llm.capabilities` owns
the table.

The response is intentionally tiny and never-changing for a given
``(provider, model)`` pair, which is why the SPA caches it with
``staleTime: Infinity`` and only invalidates on profile-save mutations.
"""

from __future__ import annotations

from fastapi import APIRouter, Query

from amx.llm.capabilities import supports_logprobs, supports_thinking

router = APIRouter(prefix="/api/llm", tags=["llm-capabilities"])


@router.get("/capabilities")
def get_capabilities(
    provider: str = Query(..., description="LLM provider identifier (openai, anthropic, …)."),
    model: str = Query(..., description="LLM model identifier."),
) -> dict[str, bool | str]:
    """Return capability flags for one ``(provider, model)`` pair.

    Used by the Studio's Advanced LLM settings panel to gray-out /
    hide knobs the selected profile can't honour. Cheap, pure
    table-lookup; safe to call on every modal mount.
    """
    return {
        "provider": provider,
        "model": model,
        "supports_thinking": supports_thinking(provider, model),
        "supports_logprobs": supports_logprobs(provider, model),
    }


__all__ = ["router"]
