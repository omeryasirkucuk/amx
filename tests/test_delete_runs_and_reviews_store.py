"""Storage + service tests for run deletion and table-review clearing.

Covers the local ``SQLiteHistoryStore`` delete/reset helpers, the
``pending_review.clear_pending_for_table`` queue helper, and the
``table_reviews.clear_table_reviews`` orchestrator that ties them
together for the CLI and Studio.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from amx.storage.sqlite_store import SQLiteHistoryStore


def _store(tmp_path: Path) -> SQLiteHistoryStore:
    s = SQLiteHistoryStore(tmp_path / "history.db")
    s.init()
    return s


def _seed_run(
    s: SQLiteHistoryStore, *, schema: str = "s", table: str = "t"
) -> tuple[int, list[int]]:
    rid = s.create_run(
        command="analyze.run",
        mode="x",
        db_backend="sqlite",
        db_profile="p",
        llm_provider="lp",
        llm_model="m",
        scope={schema: [table]},
    )
    ids = s.save_run_results(
        rid,
        [
            {
                "schema": schema,
                "table": table,
                "column": None,
                "asset_kind": "table",
                "source": "llm",
                "confidence": "high",
                "alternatives": [{"text": "a"}],
            },
            {
                "schema": schema,
                "table": table,
                "column": "c1",
                "asset_kind": "column",
                "source": "llm",
                "confidence": "low",
                "alternatives": [{"text": "b"}],
            },
        ],
    )
    return rid, ids


def test_delete_run_removes_run_and_results_keeps_audit(tmp_path: Path) -> None:
    s = _store(tmp_path)
    rid, ids = _seed_run(s)
    s.record_apply_event(
        schema_name="s", table_name="t", new_comment="d", run_id=rid, result_id=ids[0]
    )

    counts = s.delete_run(rid)

    assert counts == {"runs": 1, "results": 2}
    assert s.get_run(rid) is None
    assert s.get_run_results(rid) == []
    # The apply_events audit trail is deliberately untouched by run delete.
    assert len(s.list_apply_events()) == 1


def test_delete_run_missing_is_idempotent(tmp_path: Path) -> None:
    s = _store(tmp_path)
    assert s.delete_run(999999) == {"runs": 0, "results": 0}


def test_delete_runs_bulk(tmp_path: Path) -> None:
    s = _store(tmp_path)
    r1, _ = _seed_run(s)
    r2, _ = _seed_run(s)
    counts = s.delete_runs([r1, r2])
    assert counts["runs"] == 2
    assert counts["results"] == 4
    assert s.list_recent_runs(command_filter=None) == []


def test_delete_runs_empty_is_noop(tmp_path: Path) -> None:
    s = _store(tmp_path)
    assert s.delete_runs([]) == {"runs": 0, "results": 0}


def test_delete_runs_matching_requires_filter(tmp_path: Path) -> None:
    s = _store(tmp_path)
    _seed_run(s)
    with pytest.raises(ValueError):
        s.delete_runs_matching()  # empty filter would wipe everything


def test_delete_runs_matching_by_command(tmp_path: Path) -> None:
    s = _store(tmp_path)
    _seed_run(s)
    _seed_run(s)
    counts = s.delete_runs_matching(command_filter="analyze.run")
    assert counts["runs"] == 2
    assert s.list_recent_runs(command_filter=None) == []


def test_reset_review_state_for_table(tmp_path: Path) -> None:
    s = _store(tmp_path)
    rid, ids = _seed_run(s)
    s.record_evaluation(ids[0], chosen_description="d", evaluation="accepted")
    s.record_applied(ids[0], chosen_description="d")

    n = s.reset_review_state_for_table("s", "t")

    assert n == 2  # both rows for the table
    rows = {r["id"]: r for r in s.get_run_results(rid)}
    r0 = rows[ids[0]]
    assert r0.get("evaluation") in (None, "")
    assert not r0.get("chosen_description")
    assert r0.get("db_applied_status") in (None, "")
    # The generated alternatives survive the reset.
    assert r0.get("alternatives_json")


def test_reset_review_state_scoped_to_table(tmp_path: Path) -> None:
    s = _store(tmp_path)
    rid_a, ids_a = _seed_run(s, schema="s", table="t")
    rid_b, ids_b = _seed_run(s, schema="s", table="other")
    s.record_evaluation(ids_a[0], chosen_description="d", evaluation="accepted")
    s.record_evaluation(ids_b[0], chosen_description="d", evaluation="accepted")

    s.reset_review_state_for_table("s", "t")

    other = {r["id"]: r for r in s.get_run_results(rid_b)}
    assert other[ids_b[0]].get("evaluation") == "accepted"  # untouched


def test_delete_apply_events_for_table(tmp_path: Path) -> None:
    s = _store(tmp_path)
    rid, ids = _seed_run(s)
    s.record_apply_event(
        schema_name="s", table_name="t", new_comment="d", run_id=rid, result_id=ids[0]
    )
    s.record_apply_event(
        schema_name="s", table_name="other", new_comment="d", run_id=rid, result_id=ids[1]
    )

    n = s.delete_apply_events_for_table("s", "t")

    assert n == 1
    remaining = s.list_apply_events()
    assert len(remaining) == 1
    assert remaining[0]["table_name"] == "other"


def test_clear_pending_for_table(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import amx.pending_review as pr

    pending_file = tmp_path / "pending_metadata.json"
    monkeypatch.setattr(pr, "PENDING_FILE", pending_file)
    pending_file.write_text(
        json.dumps(
            [
                {"schema": "s", "table": "t", "column": None, "final_description": "td"},
                {"schema": "s", "table": "t", "column": "c1", "final_description": "cd"},
                {"schema": "s", "table": "other", "column": None, "final_description": "od"},
            ]
        ),
        encoding="utf-8",
    )

    removed = pr.clear_pending_for_table("s", "t")

    assert removed == 2
    left = json.loads(pending_file.read_text(encoding="utf-8"))
    assert len(left) == 1
    assert left[0]["table"] == "other"


def test_clear_pending_for_table_missing_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import amx.pending_review as pr

    monkeypatch.setattr(pr, "PENDING_FILE", tmp_path / "nope.json")
    assert pr.clear_pending_for_table("s", "t") == 0


def test_clear_table_reviews_orchestrator(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import amx.pending_review as pr
    from amx.table_reviews import clear_table_reviews

    pending_file = tmp_path / "pending_metadata.json"
    monkeypatch.setattr(pr, "PENDING_FILE", pending_file)
    pending_file.write_text(
        json.dumps([{"schema": "s", "table": "t", "column": None, "final_description": "td"}]),
        encoding="utf-8",
    )
    s = _store(tmp_path)
    rid, ids = _seed_run(s)
    s.record_evaluation(ids[0], chosen_description="d", evaluation="accepted")
    s.record_apply_event(
        schema_name="s", table_name="t", new_comment="d", run_id=rid, result_id=ids[0]
    )

    counts = clear_table_reviews(s, "s", "t")

    assert counts == {"pending": 1, "review_state": 2, "audit": 1}
    assert not pending_file.exists()  # dropped when the queue empties
    assert s.list_apply_events() == []


def test_clear_table_reviews_respects_flags(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import amx.pending_review as pr
    from amx.table_reviews import clear_table_reviews

    monkeypatch.setattr(pr, "PENDING_FILE", tmp_path / "pending.json")
    s = _store(tmp_path)
    rid, ids = _seed_run(s)
    s.record_apply_event(
        schema_name="s", table_name="t", new_comment="d", run_id=rid, result_id=ids[0]
    )

    counts = clear_table_reviews(s, "s", "t", pending=False, review_state=False, audit=True)

    assert counts == {"pending": 0, "review_state": 0, "audit": 1}
    assert s.list_apply_events() == []
