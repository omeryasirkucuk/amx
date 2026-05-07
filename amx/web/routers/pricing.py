"""Studio pricing endpoints — model lookup + cache refresh + status.

The Settings → LLM editor calls :func:`lookup_model` as the user types
to render an "OpenRouter says $0.30/$2.50" hint next to the custom-cost
inputs. The TopBar pricing-freshness badge reads :func:`cache_info`.
The "↻" refresh button on that badge hits :func:`refresh`.

All endpoints are read-mostly. Refresh is idempotent — safe to spam.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query, status
from pydantic import BaseModel, Field

from amx.config import AMXConfig
from amx.llm import pricing
from amx.utils.logging import get_logger
from amx.web.deps import get_cfg

router = APIRouter(prefix="/api/pricing", tags=["pricing"])
log = get_logger("web.pricing")


class ModelPriceResponse(BaseModel):
    input_per_mtok: float = Field(..., description="USD per 1M input tokens.")
    output_per_mtok: float = Field(..., description="USD per 1M output tokens.")
    source: str = Field(
        ...,
        description=(
            "Where the price came from: 'user_override', 'litellm', "
            "'openrouter', 'fallback', or 'unknown'."
        ),
    )
    fetched_at: float | None = Field(
        default=None,
        description="Unix epoch when the source data was fetched (None for overrides + bundled).",
    )


class CacheInfoResponse(BaseModel):
    fetched_at: float | None
    age_seconds: float | None
    ttl_seconds: float
    is_stale: bool
    litellm_count: int
    openrouter_count: int
    fallback_count: int


class RefreshResponse(BaseModel):
    litellm: int
    openrouter: int
    errors: list[str]
    skipped: bool = False


@router.get("/model", response_model=ModelPriceResponse)
def lookup_model(
    provider: str = Query(...),
    model: str = Query(...),
    profile_name: str | None = Query(default=None),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Resolve a price for one (provider, model) pair via the same path
    the rest of AMX uses. ``profile_name`` lets the SPA preview a custom
    override before saving (the editor shows the effective rate live as
    the user types into the cost inputs).
    """
    price = pricing.lookup_price(cfg, provider=provider, model=model, profile_name=profile_name)
    return {
        "input_per_mtok": price.input_per_mtok,
        "output_per_mtok": price.output_per_mtok,
        "source": price.source,
        "fetched_at": price.fetched_at,
    }


@router.get("/cache-info", response_model=CacheInfoResponse)
def cache_info() -> dict[str, Any]:
    return pricing.cache_info()


@router.post("/refresh", response_model=RefreshResponse, status_code=status.HTTP_200_OK)
def refresh() -> dict[str, Any]:
    """Force a fetch from both network sources. Returns counts + errors.

    Errors do not surface as 5xx — the SPA can render "Updated 312 + 0
    (openrouter: timeout)" so the user sees both what worked and what
    did not, in one round-trip, instead of the request failing entirely.
    """
    result = pricing.refresh_prices(force=True)
    return {
        "litellm": int(result.get("litellm") or 0),
        "openrouter": int(result.get("openrouter") or 0),
        "errors": list(result.get("errors") or []),
        "skipped": bool(result.get("skipped") or False),
    }


__all__ = ["router"]
