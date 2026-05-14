"""Descendant tree fetcher backing GET /api/runs/{id}/results?include_descendants=true.

Both the Studio's inline nested display and the CLI's ``/history show``
tree render call into :meth:`SQLiteHistoryStore.get_descendant_runs`,
so a single test pins the tree shape + depth caps. Variations recurse
up to three levels; Re-Run descend one level only.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from amx.storage.sqlite_store import SQLiteHistoryStore


@pytest.fixture
def store(tmp_path: Path) -> SQLiteHistoryStore:
    s = SQLiteHistoryStore(tmp_path / "history.db")
    s.init()
    return s


def _seed_original(s: SQLiteHistoryStore) -> tuple[int, int]:
    run_id = s.create_run(
        command="analyze.run",
        mode="chat",
        db_backend="postgresql",
        db_profile="local",
        llm_provider="openai",
        llm_model="gpt-x",
        scope={"public": ["orders"]},
    )
    [rid] = s.save_run_results(
        run_id,
        [
            {
                "schema": "public",
                "table": "orders",
                "column": "status",
                "asset_kind": "column",
                "source": "llm",
                "confidence": "medium",
                "alternatives": ["a1", "a2", "a3"],
                "alternatives_mode": "semantic",
                "model": "gpt-x",
                "provider": "openai",
            }
        ],
    )
    return run_id, rid


def _seed_variation_run(
    s: SQLiteHistoryStore,
    *,
    parent_run_id: int,
    parent_result_id: int,
    alt_index: int,
    seed_text: str,
    mode: str = "semantic",
) -> int:
    child_run_id = s.create_run(
        command="rerun",
        mode="single",
        db_backend="postgresql",
        db_profile="local",
        llm_provider="openai",
        llm_model="gpt-x",
        scope={"public": ["orders"]},
        settings={"trigger": "variations", "parent_run_id": parent_run_id},
    )
    s.save_run_results(
        child_run_id,
        [
            {
                "schema": "public",
                "table": "orders",
                "column": "status",
                "asset_kind": "column",
                "source": "rerun",
                "confidence": "high",
                "alternatives": [f"{seed_text}-v1", f"{seed_text}-v2"],
                "alternatives_mode": mode,
                "model": "gpt-x",
                "provider": "openai",
                "seed_alternative_id": f"{parent_result_id}:{alt_index}",
                "seed_alternative_text": seed_text,
                "parent_run_id": parent_run_id,
            }
        ],
    )
    return child_run_id


def _seed_rerun_run(s: SQLiteHistoryStore, *, parent_run_id: int) -> int:
    child_run_id = s.create_run(
        command="rerun",
        mode="single",
        db_backend="postgresql",
        db_profile="local",
        llm_provider="openai",
        llm_model="gpt-x",
        scope={"public": ["orders"]},
        settings={"trigger": "rerun", "parent_run_id": parent_run_id},
    )
    s.save_run_results(
        child_run_id,
        [
            {
                "schema": "public",
                "table": "orders",
                "column": "status",
                "asset_kind": "column",
                "source": "rerun",
                "confidence": "medium",
                "alternatives": ["rerun-alt-a", "rerun-alt-b"],
                "alternatives_mode": "lexical",
                "model": "claude-opus-4",
                "provider": "anthropic",
            }
        ],
    )
    return child_run_id


class TestGetDescendantRuns:
    def test_empty_when_no_descendants(self, store: SQLiteHistoryStore) -> None:
        run_id, _ = _seed_original(store)
        assert store.get_descendant_runs(run_id) == []

    def test_one_variation_descendant(self, store: SQLiteHistoryStore) -> None:
        run_id, rid = _seed_original(store)
        child_run_id = _seed_variation_run(
            store,
            parent_run_id=run_id,
            parent_result_id=rid,
            alt_index=1,
            seed_text="seed-B",
        )
        tree = store.get_descendant_runs(run_id)
        assert len(tree) == 1
        entry = tree[0]
        assert entry["kind"] == "variations"
        assert entry["run_id"] == child_run_id
        assert entry["seed_alternative_id"] == f"{rid}:1"
        assert entry["mode"] == "semantic"
        assert entry["depth"] == 1
        assert entry["over_max_depth"] is False
        assert len(entry["rows"]) == 1

    def test_one_rerun_descendant_with_model_override(self, store: SQLiteHistoryStore) -> None:
        run_id, _ = _seed_original(store)
        child = _seed_rerun_run(store, parent_run_id=run_id)
        tree = store.get_descendant_runs(run_id)
        assert len(tree) == 1
        entry = tree[0]
        assert entry["kind"] == "rerun"
        assert entry["run_id"] == child
        assert entry["mode"] == "lexical"
        assert entry["model"] == "claude-opus-4"
        assert entry["provider"] == "anthropic"

    def test_variations_recurse_three_deep(self, store: SQLiteHistoryStore) -> None:
        run1, rid1 = _seed_original(store)
        run2 = _seed_variation_run(
            store, parent_run_id=run1, parent_result_id=rid1, alt_index=1, seed_text="L1"
        )
        rows2 = store.get_run_results(run2)
        rid2 = int(rows2[0]["id"])
        run3 = _seed_variation_run(
            store, parent_run_id=run2, parent_result_id=rid2, alt_index=0, seed_text="L2"
        )
        rows3 = store.get_run_results(run3)
        rid3 = int(rows3[0]["id"])
        run4 = _seed_variation_run(
            store, parent_run_id=run3, parent_result_id=rid3, alt_index=0, seed_text="L3"
        )

        tree = store.get_descendant_runs(run1, variations_depth_cap=3)
        assert {e["run_id"] for e in tree} == {run2, run3, run4}
        depth_by_run = {e["run_id"]: e["depth"] for e in tree}
        assert depth_by_run[run2] == 1
        assert depth_by_run[run3] == 2
        assert depth_by_run[run4] == 3
        # None over the cap at depth=3.
        assert all(e["over_max_depth"] is False for e in tree)

    def test_variations_depth_cap_truncates_recursion(self, store: SQLiteHistoryStore) -> None:
        run1, rid1 = _seed_original(store)
        run2 = _seed_variation_run(
            store, parent_run_id=run1, parent_result_id=rid1, alt_index=1, seed_text="L1"
        )
        rows2 = store.get_run_results(run2)
        rid2 = int(rows2[0]["id"])
        # Seed a second-level variation under run2 so that with cap=1 it
        # MUST be excluded from the tree returned for run1.
        _seed_variation_run(
            store, parent_run_id=run2, parent_result_id=rid2, alt_index=0, seed_text="L2"
        )

        # With variations_depth_cap=1 we collect only run2.
        tree = store.get_descendant_runs(run1, variations_depth_cap=1)
        assert {e["run_id"] for e in tree} == {run2}

    def test_rerun_does_not_recurse(self, store: SQLiteHistoryStore) -> None:
        run1, _ = _seed_original(store)
        run2 = _seed_rerun_run(store, parent_run_id=run1)
        # A re-run of a re-run — should not surface under run1's descendants.
        _seed_rerun_run(store, parent_run_id=run2)

        tree = store.get_descendant_runs(run1)
        # Only run2 (the direct re-run of run1) appears at depth 1; run3
        # is logically a re-run of run2 and doesn't bubble up to run1's
        # descendants.
        rerun_runs = [e for e in tree if e["kind"] == "rerun"]
        assert {e["run_id"] for e in rerun_runs} == {run2}

    def test_mixed_variations_and_rerun_under_same_parent(self, store: SQLiteHistoryStore) -> None:
        run1, rid1 = _seed_original(store)
        var_run = _seed_variation_run(
            store, parent_run_id=run1, parent_result_id=rid1, alt_index=1, seed_text="seed"
        )
        rerun_run = _seed_rerun_run(store, parent_run_id=run1)

        tree = store.get_descendant_runs(run1)
        kinds = {e["run_id"]: e["kind"] for e in tree}
        assert kinds == {var_run: "variations", rerun_run: "rerun"}

    def test_alternatives_json_parsed_in_rows(self, store: SQLiteHistoryStore) -> None:
        """Sanity: descendant rows come back with alternatives_json parsed,
        matching the contract of ``get_run_results``."""
        run1, rid1 = _seed_original(store)
        _seed_variation_run(
            store, parent_run_id=run1, parent_result_id=rid1, alt_index=0, seed_text="x"
        )
        tree = store.get_descendant_runs(run1)
        alt_payload = tree[0]["rows"][0]["alternatives_json"]
        # Parsed into a list (the JSON form would be a string).
        assert not isinstance(alt_payload, str)
        # And the stored payload is the original list — payload may be
        # a list[str] or list[dict] depending on alternative_scores.
        assert isinstance(alt_payload, list)
