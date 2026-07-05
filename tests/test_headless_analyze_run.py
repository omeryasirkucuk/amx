"""Headless / non-interactive `/analyze run` behaviour.

Covers the seams that make a piped / CI / scripted run first-class:

* the auto-apply branch honours ``--no-apply`` (never writes to the live
  DB when ``apply=False``) — the regression the CLI warning always
  claimed but the code contradicted;
* headless scope resolution never prompts — a ``--schema`` with no
  ``--table`` expands to all assets, and a run with neither errors;
* the post-loop summary skips the interactive ``batch_review`` in
  headless mode, marks the generated suggestions accepted (so they land
  in the pending queue), and never applies under ``--no-apply``.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from amx.agents._orchestrator.table_processor import TableProcessor
from amx.agents.base import Confidence


def _suggestion(column: str, text: str) -> SimpleNamespace:
    """A minimal stand-in for a merged MetadataSuggestion."""
    return SimpleNamespace(
        schema="s",
        table="t",
        column=column,
        suggestions=[text],
        confidence=Confidence.HIGH,
        source="profile",
        logprob_score=-0.1,
        citations=[],
    )


def _table_processor(apply: bool) -> TableProcessor:
    orch = SimpleNamespace(
        results=[],
        db=object(),
        _record_applied_state=lambda r: None,
    )
    return TableProcessor(orch, "s", "t", apply=apply)


@pytest.fixture
def _stub_catalog_and_history(monkeypatch: pytest.MonkeyPatch) -> None:
    """Neutralise the catalog / history side-effects so the branch under
    test only exercises the DB-write decision."""
    from amx.search import catalog as catalog_mod
    from amx.storage import sqlite_store

    monkeypatch.setattr(catalog_mod.SearchCatalog, "from_history_store", staticmethod(lambda: None))
    monkeypatch.setattr(sqlite_store, "history_store", lambda: None)


def test_auto_apply_branch_no_apply_skips_db_write(
    monkeypatch: pytest.MonkeyPatch, _stub_catalog_and_history: None
) -> None:
    """`--no-apply` (apply=False) must NOT write COMMENTs even under
    review_strategy=auto-apply."""
    import amx.agents.orchestrator as orch_mod

    writeback = Mock(return_value=0)
    monkeypatch.setattr(orch_mod, "apply_review_results_to_db", writeback)

    tp = _table_processor(apply=False)
    results = tp._auto_apply_branch([_suggestion("c1", "desc one")], {"c1": 1}, "table")

    writeback.assert_not_called()
    # The suggestion is still accepted (applied=True) so the summary /
    # pending queue capture it; it just isn't written to the DB.
    assert len(results) == 1
    assert results[0].applied is True
    assert results[0].final_description == "desc one"


def test_auto_apply_branch_with_apply_writes_db(
    monkeypatch: pytest.MonkeyPatch, _stub_catalog_and_history: None
) -> None:
    """With apply=True the auto-apply branch writes to the live DB."""
    import amx.agents.orchestrator as orch_mod

    writeback = Mock(return_value=1)
    monkeypatch.setattr(orch_mod, "apply_review_results_to_db", writeback)

    tp = _table_processor(apply=True)
    tp._auto_apply_branch([_suggestion("c1", "desc one")], {"c1": 1}, "table")

    writeback.assert_called_once()


def test_process_table_forwards_apply_flag() -> None:
    """Orchestrator.process_table threads ``apply`` into TableProcessor."""
    import amx.agents.orchestrator as orch_mod
    from amx.agents.orchestrator import Orchestrator

    captured: dict[str, object] = {}

    class _FakeTP:
        def __init__(self, *args: object, **kwargs: object) -> None:
            captured.update(kwargs)

        def run(self) -> list:
            return []

    # process_table imports TableProcessor from amx.agents._orchestrator.
    import amx.agents._orchestrator as pkg

    orig = pkg.TableProcessor
    pkg.TableProcessor = _FakeTP  # type: ignore[misc, assignment]
    try:
        orch = Orchestrator.__new__(Orchestrator)  # skip heavy __init__
        orch.process_table("s", "t", auto_apply=True, apply=False)
    finally:
        pkg.TableProcessor = orig  # type: ignore[misc]

    assert captured.get("apply") is False
    assert captured.get("auto_apply") is True
    del orch_mod  # silence unused import lint if any


# ── Headless scope resolution ──────────────────────────────────────────


def test_resolve_run_scope_headless_lists_all_assets(monkeypatch: pytest.MonkeyPatch) -> None:
    """Headless + --schema with no --table expands to all business assets,
    without ever calling the interactive pickers."""
    from amx.services import analyze_scope

    monkeypatch.setattr(
        analyze_scope, "asset_display_list", lambda db, schema: ["orders  [table]", "customers"]
    )
    ask_choice = Mock()
    ask_multi = Mock()

    scope = analyze_scope.resolve_run_scope(
        cfg=SimpleNamespace(),
        db=object(),
        schema="public",
        table_args=[],
        ask_choice=ask_choice,
        ask_multi_choice=ask_multi,
        warn=lambda *a, **k: None,
        headless=True,
    )

    assert scope == {"public": ["orders", "customers"]}
    ask_choice.assert_not_called()
    ask_multi.assert_not_called()


def test_resolve_run_scope_headless_requires_scope() -> None:
    """Headless with neither --schema nor --table raises instead of
    dropping into the interactive scope picker."""
    from amx.services import analyze_scope

    ask_choice = Mock()
    with pytest.raises(ValueError, match="Headless run needs an explicit scope"):
        analyze_scope.resolve_run_scope(
            cfg=SimpleNamespace(),
            db=object(),
            schema=None,
            table_args=[],
            ask_choice=ask_choice,
            ask_multi_choice=Mock(),
            warn=lambda *a, **k: None,
            headless=True,
        )
    ask_choice.assert_not_called()


# ── Headless post-loop summary ─────────────────────────────────────────


def test_render_summary_headless_skips_review_and_no_apply(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """In headless mode the summary skips batch_review, marks the
    suggestions accepted (pending), and — under --no-apply — never
    applies to the DB."""
    from amx.agents.orchestrator import ReviewResult
    from amx.cli_support.commands._analyze import run_summary

    saved: list[list] = []
    monkeypatch.setattr(
        "amx.pending_review.save_pending", lambda approved: saved.append(list(approved))
    )
    # Avoid touching the rich rendering path (field access on results).
    monkeypatch.setattr(run_summary, "_render_results_table", lambda **kwargs: None)

    result = ReviewResult(
        schema="s",
        table="t",
        column="c1",
        final_description="a description",
        confidence=Confidence.HIGH,
        source="profile",
        applied=False,
    )
    orch = Mock()

    approved, skipped = run_summary.render_summary_and_apply(
        all_results=[result],
        orch=orch,
        review_strategy="deferred",
        apply=False,
        rag_store=object(),  # non-None so token_tracker.drop_steps is skipped
        dedup_outcome=None,
        run_id=None,
        history_store_fn=lambda: None,
        headless=True,
    )

    orch.batch_review.assert_not_called()
    orch.apply_results.assert_not_called()
    assert result.applied is True
    assert approved == [result]
    assert saved == [[result]]
