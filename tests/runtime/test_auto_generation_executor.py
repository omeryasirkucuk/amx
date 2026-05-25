"""Auto-generation knobs in ``production_run_executor``.

A scheduled analyze run becomes "auto-generation" when:
  * scope.missing_only → the Orchestrator only describes columns that
    lack a description,
  * scope.deep_first → a deep sync runs before generation so newly
    added columns are discovered,
  * review_strategy='auto' → results are applied to the DB after
    generation (vs 'manual' which leaves them in pending review).

These tests drive the executor past its setup with stubs and assert
each knob is honoured.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

import pytest


class _FakeOrchestrator:
    last: _FakeOrchestrator | None = None

    def __init__(self, *a: Any, **k: Any) -> None:
        self.missing_only = k.get("missing_only")
        self.process_calls: list[tuple[str, str]] = []
        self.applied = 0
        _FakeOrchestrator.last = self

    def process_table(self, schema: str, table: str, **_k: Any) -> None:
        self.process_calls.append((schema, table))

    def apply_results(self, results: Any = None) -> int:
        self.applied += 1
        return 3


def _patch_common(monkeypatch: pytest.MonkeyPatch) -> None:
    import amx.agents.orchestrator as orch_mod
    import amx.config as config_mod
    import amx.db.connector as connector_mod
    import amx.llm.provider as llm_mod
    import amx.runtime.worker as worker

    cfg = SimpleNamespace(
        db_profiles={"prof": SimpleNamespace(backend="postgresql")},
        llm_profiles={"llm": object()},
    )
    monkeypatch.setattr(config_mod.AMXConfig, "load", staticmethod(lambda *a, **k: cfg))
    monkeypatch.setattr(connector_mod, "DatabaseConnector", lambda *a, **k: object())
    monkeypatch.setattr(llm_mod, "LLMProvider", lambda *a, **k: object())
    monkeypatch.setattr(orch_mod, "Orchestrator", _FakeOrchestrator)
    # One table in scope so process_table fires once.
    monkeypatch.setattr(worker, "_resolve_live_scope", lambda *a, **k: {"s": ["t"]})


def _payload(**scope_extra: Any) -> dict[str, Any]:
    review = scope_extra.pop("review_strategy", "manual")
    return {
        "id": 1,
        "db_profile": "prof",
        "llm_profile": "llm",
        "review_strategy": review,
        "scope_json": json.dumps({"mode": "all", **scope_extra}),
    }


def test_missing_only_flows_to_orchestrator(monkeypatch: pytest.MonkeyPatch) -> None:
    from amx.runtime.worker import production_run_executor

    _patch_common(monkeypatch)
    production_run_executor(1, _payload(missing_only=True))

    assert _FakeOrchestrator.last is not None
    assert _FakeOrchestrator.last.missing_only is True


def test_auto_strategy_applies_results(monkeypatch: pytest.MonkeyPatch) -> None:
    from amx.runtime.worker import production_run_executor

    _patch_common(monkeypatch)
    production_run_executor(1, _payload(missing_only=True, review_strategy="auto"))

    assert _FakeOrchestrator.last.applied == 1  # apply_results called once


def test_manual_strategy_does_not_apply(monkeypatch: pytest.MonkeyPatch) -> None:
    from amx.runtime.worker import production_run_executor

    _patch_common(monkeypatch)
    production_run_executor(1, _payload(review_strategy="manual"))

    assert _FakeOrchestrator.last.applied == 0  # left in pending review


def test_deep_first_runs_deep_sync_before_generation(monkeypatch: pytest.MonkeyPatch) -> None:
    import amx.search.catalog as catalog_mod
    import amx.search.drift as drift_mod
    from amx.runtime.worker import production_run_executor

    _patch_common(monkeypatch)
    monkeypatch.setattr(
        catalog_mod.SearchCatalog, "from_history_store", classmethod(lambda cls: object())
    )
    deep_calls = {"n": 0}
    monkeypatch.setattr(
        drift_mod, "deep_sync_profile", lambda *a, **k: deep_calls.__setitem__("n", 1)
    )

    production_run_executor(1, _payload(deep_first=True))

    assert deep_calls["n"] == 1


def test_no_deep_first_skips_deep_sync(monkeypatch: pytest.MonkeyPatch) -> None:
    import amx.search.drift as drift_mod
    from amx.runtime.worker import production_run_executor

    _patch_common(monkeypatch)
    deep_calls = {"n": 0}
    monkeypatch.setattr(
        drift_mod, "deep_sync_profile", lambda *a, **k: deep_calls.__setitem__("n", 1)
    )

    production_run_executor(1, _payload(missing_only=True))

    assert deep_calls["n"] == 0
