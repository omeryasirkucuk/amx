"""Compare pivot must surface Variations / Re-Run descendants.

A run that has variations descendants for one of its assets must
show v2 / v3 entries in the ``per_column`` long-format payload --
otherwise the Studio Compare page renders only v1 cells and the
user has no way to see how the agent's alternative descriptions
diverged from the seed they picked. This test module pins the
``_collect_per_column_long`` contract: parent's v1 first, then
chronologically-ordered v2..vN entries each tagged with
``version_label``, ``parent_run_id``, ``descendant_kind``,
``descendant_run_id``, ``seed_alternative_text``, and
``alternatives_mode``.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from amx.cli_support.commands.compare import compare_runs
from amx.storage.sqlite_store import SQLiteHistoryStore


@pytest.fixture
def store(tmp_path: Path) -> SQLiteHistoryStore:
    s = SQLiteHistoryStore(tmp_path / "history.db")
    s.init()
    return s


def _seed_parent_run(s: SQLiteHistoryStore) -> tuple[int, int]:
    """Create one parent run with a single ``country`` column row.

    Returns ``(parent_run_id, parent_result_id)``.
    """
    parent_id = s.create_run(
        command="analyze.run",
        mode="batch",
        db_backend="postgresql",
        db_profile="local",
        llm_provider="openai",
        llm_model="gpt-x",
        scope={"public": ["country"]},
        settings={"alternatives_mode": "semantic"},
    )
    [rid] = s.save_run_results(
        parent_id,
        [
            {
                "schema": "public",
                "table": "country",
                "column": "abbreviation",
                "asset_kind": "column",
                "source": "llm",
                "confidence": "medium",
                "alternatives": [
                    "Two-letter ISO code identifying the country.",
                    "Short alpha code for the country (e.g. US, TR).",
                ],
                "alternatives_mode": "semantic",
                "model": "gpt-x",
                "provider": "openai",
            }
        ],
    )
    return parent_id, rid


def _seed_variations_descendant(
    s: SQLiteHistoryStore,
    *,
    parent_run_id: int,
    parent_result_id: int,
    seed_text: str,
    new_alts: list[str],
    mode: str = "lexical",
) -> int:
    """Create a child run linked back to ``parent_run_id`` via the
    ``parent_run_id`` field on its row, mirroring the production
    Variations executor's write path."""
    child_id = s.create_run(
        command="rerun",
        mode="single",
        db_backend="postgresql",
        db_profile="local",
        llm_provider="openai",
        llm_model="gpt-x",
        scope={"public": ["country"]},
        settings={
            "trigger": "variations",
            "parent_run_id": parent_run_id,
            "alternatives_mode": mode,
        },
    )
    s.save_run_results(
        child_id,
        [
            {
                "schema": "public",
                "table": "country",
                "column": "abbreviation",
                "asset_kind": "column",
                "source": "rerun",
                "confidence": "medium",
                "alternatives": new_alts,
                "alternatives_mode": mode,
                "model": "gpt-x",
                "provider": "openai",
                "parent_result_id": parent_result_id,
                "rerun_seq": s.next_rerun_seq(parent_result_id),
                "seed_alternative_id": f"{parent_result_id}:0",
                "seed_alternative_text": seed_text,
                "parent_run_id": parent_run_id,
            }
        ],
    )
    return child_id


class TestPerColumnDescendants:
    def test_per_column_includes_variations(self, store: SQLiteHistoryStore) -> None:
        parent_id, parent_rid = _seed_parent_run(store)
        seed = "Two-letter ISO code identifying the country."
        _seed_variations_descendant(
            store,
            parent_run_id=parent_id,
            parent_result_id=parent_rid,
            seed_text=seed,
            new_alts=[
                "Three-letter alpha-3 code identifying the country.",
                "Numeric ISO 3166-1 country code.",
            ],
            mode="lexical",
        )
        with patch("amx.cli_support.commands.compare.history_store", return_value=store):
            payload = compare_runs([parent_id])

        per_col = payload["per_column"]
        # 1 v1 + 1 v2 for this single asset.
        assert len(per_col) == 2
        v1, v2 = per_col[0], per_col[1]
        assert v1["version_label"] == "v1"
        assert v1["parent_run_id"] is None
        assert v1["alternatives_mode"] == "semantic"
        assert v2["version_label"] == "v2"
        assert v2["parent_run_id"] == parent_id
        assert v2["descendant_kind"] == "variations"
        assert v2["descendant_run_id"] is not None
        assert v2["descendant_run_id"] != parent_id
        assert v2["seed_alternative_text"] == seed
        assert v2["alternatives_mode"] == "lexical"
        # Top alternative of v2 is the first of the two seeded.
        assert v2["description"].startswith("Three-letter")

    def test_per_column_orders_versions_within_cell(self, store: SQLiteHistoryStore) -> None:
        parent_id, parent_rid = _seed_parent_run(store)
        first_child = _seed_variations_descendant(
            store,
            parent_run_id=parent_id,
            parent_result_id=parent_rid,
            seed_text="seed",
            new_alts=["v2 alt A", "v2 alt B"],
            mode="lexical",
        )
        second_child = _seed_variations_descendant(
            store,
            parent_run_id=parent_id,
            parent_result_id=parent_rid,
            seed_text="seed",
            new_alts=["v3 alt A", "v3 alt B"],
            mode="semantic",
        )
        with patch("amx.cli_support.commands.compare.history_store", return_value=store):
            payload = compare_runs([parent_id])

        per_col = payload["per_column"]
        # v1 + v2 + v3.
        assert [r["version_label"] for r in per_col] == ["v1", "v2", "v3"]
        # v2 corresponds to the earlier child id; v3 to the later.
        assert per_col[1]["descendant_run_id"] == first_child
        assert per_col[2]["descendant_run_id"] == second_child
        # Mode chip per-version surfaces the row's own mode, NOT the
        # parent's -- the user needs to see lexical vs semantic per row.
        assert per_col[1]["alternatives_mode"] == "lexical"
        assert per_col[2]["alternatives_mode"] == "semantic"

    def test_per_column_v1_only_when_no_descendants(self, store: SQLiteHistoryStore) -> None:
        parent_id, _ = _seed_parent_run(store)
        with patch("amx.cli_support.commands.compare.history_store", return_value=store):
            payload = compare_runs([parent_id])

        per_col = payload["per_column"]
        assert len(per_col) == 1
        row = per_col[0]
        assert row["version_label"] == "v1"
        assert row["parent_run_id"] is None
        assert row["descendant_kind"] is None
        assert row["descendant_run_id"] is None
        assert row["seed_alternative_text"] is None
