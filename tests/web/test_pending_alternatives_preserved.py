"""Regression: pending serialiser must always replace the cached
alternatives list with the fresh ``run_results.alternatives_json`` so
the Run Detail carousel keeps every alternative clickable after the
user picks one.

Without the fix, after the first ``pickAlternative`` the pending JSON
file caches ``alternatives=[chosen]``; ``_serialize`` had a guard
``if ext_alts and not alternatives`` that kept the cached single-entry
list and dropped the original A / B / C carousel from the UI.
"""

from __future__ import annotations

from amx.agents.base import Confidence
from amx.agents.orchestrator import ReviewResult
from amx.web.routers.pending import _serialize


def _review_with_cached_chosen(chosen: str) -> ReviewResult:
    return ReviewResult(
        schema="public",
        table="lists",
        column="list_cover_image_url",
        final_description=chosen,
        confidence=Confidence.HIGH,
        source="db_profile",
        applied=False,
        asset_kind="column",
        result_id=42,
        # Simulate the post-pick pending file: only the chosen alt was
        # serialised into the on-disk JSON when the user clicked it.
        alternatives=[chosen],
        logprob_score=0.78,
    )


def test_serialize_replaces_cached_alternatives_with_fresh_run_results():
    rr = _review_with_cached_chosen("URL to the cover image displayed for a user-curated list.")
    fresh = [
        "URL to the cover image displayed for a user-curated list.",
        "Primary thumbnail URL representing the list on browse pages.",
        "Promotional image URL hosted on the Mubi CDN with versioning timestamps.",
    ]
    enrichment = {42: {"alternatives_json": fresh, "logprob_score": 0.78}}

    payload = _serialize(idx=0, rr=rr, enrichment=enrichment)

    # All three alternatives must come back so the user can still click
    # B or C even after picking A.
    assert payload["alternatives"] == fresh


def test_serialize_keeps_cached_alternatives_when_enrichment_is_missing():
    """If we have no fresh run_results row (e.g. db unavailable, or the
    row was deleted), we must still surface what's cached in the
    pending file so the SPA does not blank the carousel completely."""
    rr = _review_with_cached_chosen("A single fallback description.")

    payload = _serialize(idx=0, rr=rr, enrichment=None)

    assert payload["alternatives"] == ["A single fallback description."]
