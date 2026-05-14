"""Regression: ``_ensure_complete_table_coverage`` honors n_alternatives.

Before this fix, the fallback path injected a single-item
``suggestions=[desc]`` list when the model returned no table-level
or no column-level row — regardless of ``cfg.llm.n_alternatives``.
On a profile with ``n_alternatives=3`` the user saw a TABLE row
with only an ``A`` slot labeled FALLBACK while the same run's
column rows had A/B/C as expected. The asymmetry was confusing and
made the ✨ Variations flow unreachable on the table row (only 1
alternative → ✨ trigger hidden).

Fix: read ``cfg.llm.n_alternatives`` once at the top of the method
and multiply the fallback string list. The N entries are identical
placeholders all marked ``source='fallback'`` — a reviewer picks
one and continues, then uses ✨ Variations on the chosen row to
regenerate real alternatives in one click.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from amx.agents.base import Confidence, MetadataSuggestion
from amx.db.connector import AssetKind, ColumnProfile, TableProfile


@dataclass
class _StubLLMCfg:
    n_alternatives: int = 3


@dataclass
class _StubLLM:
    """Shape-matches the ``LLMProvider`` carrying ``cfg`` — that's
    where the orchestrator reads ``n_alternatives`` from."""

    cfg: _StubLLMCfg = field(default_factory=_StubLLMCfg)


class _StubOrchestrator:
    """Minimal stand-in carrying ``llm.cfg`` so the unbound method
    works. Bringing in the full ``Orchestrator`` constructor would
    drag the whole DB/LLM/RAG stack into a test that only exercises
    a pure-Python helper."""

    def __init__(self, n_alternatives: int = 3) -> None:
        self.llm = _StubLLM(cfg=_StubLLMCfg(n_alternatives=n_alternatives))


def _profile(*, columns: list[str]) -> TableProfile:
    return TableProfile(
        schema="public",
        name="orders",
        asset_kind=AssetKind.TABLE,
        columns=[ColumnProfile(name=c, dtype="TEXT", nullable=True) for c in columns],
    )


def _ensure(orch: _StubOrchestrator, profile: TableProfile, merged: list[Any]) -> list[Any]:
    """Call ``_ensure_complete_table_coverage`` as an unbound method
    bound to the stub. The method body only reads ``self.cfg``."""
    from amx.agents.orchestrator import Orchestrator

    return Orchestrator._ensure_complete_table_coverage(orch, profile, merged)


class TestFallbackHonorsNAlternatives:
    def test_table_level_fallback_pads_to_n(self) -> None:
        """Model returned NO table-level row; the injected fallback
        must carry ``n_alternatives`` suggestions, not just one."""
        orch = _StubOrchestrator(n_alternatives=3)
        profile = _profile(columns=["country"])
        existing_col_only = [
            MetadataSuggestion(
                schema="public",
                table="orders",
                column="country",
                suggestions=["a1", "a2", "a3"],
                confidence=Confidence.MEDIUM,
                reasoning="seeded",
                source="llm",
            )
        ]
        out = _ensure(orch, profile, existing_col_only)
        table_row = next((s for s in out if s.column is None), None)
        assert table_row is not None, "Expected the fallback to inject a table-level row."
        assert len(table_row.suggestions) == 3, (
            f"Table-level fallback emitted {len(table_row.suggestions)} suggestion(s); "
            f"expected 3 to match n_alternatives. This is the production bug — "
            f"the TABLE row showed only one 'A' slot labeled FALLBACK."
        )
        assert table_row.source == "fallback"

    def test_column_level_fallback_pads_to_n(self) -> None:
        """A column the model missed must also receive ``n_alternatives``
        fallback slots — symmetric with the table-level path."""
        orch = _StubOrchestrator(n_alternatives=3)
        profile = _profile(columns=["country", "amount"])
        existing = [
            MetadataSuggestion(
                schema="public",
                table="orders",
                column=None,
                suggestions=["table-desc-1", "table-desc-2", "table-desc-3"],
                confidence=Confidence.HIGH,
                reasoning="seeded",
                source="llm",
            ),
            MetadataSuggestion(
                schema="public",
                table="orders",
                column="amount",
                suggestions=["a1", "a2", "a3"],
                confidence=Confidence.MEDIUM,
                reasoning="seeded",
                source="llm",
            ),
        ]
        out = _ensure(orch, profile, existing)
        country_row = next(
            (s for s in out if s.column == "country" and s.source == "fallback"),
            None,
        )
        assert country_row is not None
        assert len(country_row.suggestions) == 3

    def test_fallback_n_equals_1_still_works(self) -> None:
        """Single-answer profiles (n_alternatives=1) must continue to
        emit one fallback slot — back-compat for the original shape."""
        orch = _StubOrchestrator(n_alternatives=1)
        profile = _profile(columns=["country"])
        out = _ensure(orch, profile, [])
        table_row = next((s for s in out if s.column is None), None)
        col_row = next((s for s in out if s.column == "country"), None)
        assert table_row is not None and len(table_row.suggestions) == 1
        assert col_row is not None and len(col_row.suggestions) == 1

    def test_fallback_reasoning_carries_n(self) -> None:
        """The reasoning string explains why the fallback fired and
        how many slots were requested so the persisted row tells the
        operator what to investigate."""
        orch = _StubOrchestrator(n_alternatives=3)
        profile = _profile(columns=[])
        out = _ensure(orch, profile, [])
        table_row = next(s for s in out if s.column is None)
        assert "3" in (table_row.reasoning or "")
        assert "fallback" in (table_row.reasoning or "").lower()

    def test_no_fallback_when_model_succeeded(self) -> None:
        """The fallback path must NOT fire when the model returned a
        table-level row + every column. Pin this so a future refactor
        doesn't over-pad real model output."""
        orch = _StubOrchestrator(n_alternatives=3)
        profile = _profile(columns=["country"])
        existing = [
            MetadataSuggestion(
                schema="public",
                table="orders",
                column=None,
                suggestions=["t1", "t2", "t3"],
                confidence=Confidence.HIGH,
                reasoning="seeded",
                source="llm",
            ),
            MetadataSuggestion(
                schema="public",
                table="orders",
                column="country",
                suggestions=["c1", "c2", "c3"],
                confidence=Confidence.HIGH,
                reasoning="seeded",
                source="llm",
            ),
        ]
        out = _ensure(orch, profile, existing)
        # No fallback rows introduced.
        assert all(s.source != "fallback" for s in out)
        assert len(out) == 2
