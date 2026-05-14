"""Tests for ``_tool_find_joinable_tables`` strategy variants.

Covers the v0.14 changes:

* ``strategy`` parameter validates and selects per-tier behavior.
* ``inference_source`` is ``None`` (not stuck on ``foreign_key``) when
  every metadata tier returns empty — the bug observed in the SAP
  ``adrc`` session.
* ``strategies_tried`` reports every tier the call attempted.
* ``name_overlap`` falls through to a live ``information_schema``
  lookup when the catalog has no column rows for the target.
* ``value_overlap`` samples distinct values from both sides of each
  name-overlap candidate and scores by Jaccard intersection.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from amx.config import AMXConfig, DBConfig
from amx.search.agent_tools import ToolBox, _ToolError
from amx.search.catalog import SearchCatalog
from amx.storage.sqlite_store import SQLiteHistoryStore


@pytest.fixture()
def cfg_sap() -> AMXConfig:
    cfg = AMXConfig()
    cfg.db_profiles = {
        "sap": DBConfig(backend="postgresql", host="sap.local", database="sap"),
    }
    cfg.active_db_profile = "sap"
    cfg.active_db_profiles = ["sap"]
    cfg.db = cfg.db_profiles["sap"]
    return cfg


def _seed_columns(store: SQLiteHistoryStore, rows: list[tuple]) -> None:
    """Insert (db_profile, schema, table, column, kind, dtype, pk, fk)
    rows into ``catalog_entities``. Skips the ``index`` upsert so tests
    don't need a real ``SearchIndex``."""
    with store._lock, store._connect() as conn:
        for row in rows:
            conn.execute(
                """
                INSERT INTO catalog_entities
                    (db_profile, schema_name, table_name, column_name,
                     entity_kind, dtype, pk_flag, fk_flag, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)
                """,
                row,
            )


def _make_box(cfg: AMXConfig, store: SQLiteHistoryStore) -> ToolBox:
    catalog = SearchCatalog(store.db_path)
    return ToolBox(cfg, catalog, db_profiles=["sap"])


def test_strategy_validation_rejects_unknown(cfg_sap, tmp_path) -> None:
    store = SQLiteHistoryStore(tmp_path / "history.db")
    store.init()
    box = _make_box(cfg_sap, store)
    with pytest.raises(_ToolError):
        box._tool_find_joinable_tables(table="sap_s6p.adrc", strategy="cosmic_rays")


def test_inference_source_null_when_all_metadata_tiers_empty(cfg_sap, tmp_path) -> None:
    """The SAP ``adrc`` failure mode: catalog knows the table exists
    (a single table row + a single non-matching column) but FK,
    name_overlap, and semantic all come back empty. The pre-fix code
    returned ``inference_source='foreign_key'`` even with zero rows,
    which misled the LLM into stating an FK-verified empty list."""
    store = SQLiteHistoryStore(tmp_path / "history.db")
    store.init()
    _seed_columns(
        store,
        [
            ("sap", "sap_s6p", "adrc", None, "table", "", 0, 0),
            # One column on a column name no peer shares — name_overlap
            # has columns to scan but finds no peers.
            ("sap", "sap_s6p", "adrc", "very_unique_col", "column", "TEXT", 0, 0),
        ],
    )
    box = _make_box(cfg_sap, store)
    result = box._tool_find_joinable_tables(table="sap_s6p.adrc", strategy="auto")
    assert result["found"] is True
    assert result["joinable_tables"] == []
    assert result["count"] == 0
    assert result["inference_source"] is None
    assert set(result["strategies_tried"]) == {
        "foreign_key",
        "name_overlap",
        "semantic_similarity",
    }


def test_name_overlap_wins_when_fk_empty(cfg_sap, tmp_path) -> None:
    store = SQLiteHistoryStore(tmp_path / "history.db")
    store.init()
    _seed_columns(
        store,
        [
            ("sap", "sap_s6p", "adrc", None, "table", "", 0, 0),
            ("sap", "sap_s6p", "adrc", "addrnumber", "column", "TEXT", 1, 0),
            ("sap", "sap_s6p", "kna1", None, "table", "", 0, 0),
            ("sap", "sap_s6p", "kna1", "addrnumber", "column", "TEXT", 0, 0),
        ],
    )
    box = _make_box(cfg_sap, store)
    result = box._tool_find_joinable_tables(table="sap_s6p.adrc")
    assert result["inference_source"] == "name_overlap"
    assert any(r["target_table"] == "kna1" for r in result["joinable_tables"])
    assert "foreign_key" in result["strategies_tried"]
    assert "name_overlap" in result["strategies_tried"]


def test_live_rescue_fires_when_catalog_has_no_columns(cfg_sap, tmp_path) -> None:
    """When ``catalog_entities`` has no column rows for the target
    (stale catalog — exactly the SAP adrc case), the tool falls back
    to a live ``get_columns`` lookup and feeds the column list into
    name_overlap via ``base_cols_override``. We mock the live fetch
    so the test doesn't need a real Postgres."""
    store = SQLiteHistoryStore(tmp_path / "history.db")
    store.init()
    _seed_columns(
        store,
        [
            # Only the peer table is in the catalog; adrc has zero
            # column rows — simulates "synced kna1 but not adrc".
            ("sap", "sap_s6p", "kna1", None, "table", "", 0, 0),
            ("sap", "sap_s6p", "kna1", "addrnumber", "column", "TEXT", 0, 0),
            ("sap", "sap_s6p", "lfa1", None, "table", "", 0, 0),
            ("sap", "sap_s6p", "lfa1", "addrnumber", "column", "TEXT", 0, 0),
        ],
    )
    box = _make_box(cfg_sap, store)
    with patch.object(
        ToolBox,
        "_fetch_live_column_names",
        return_value=["addrnumber", "name1", "city1"],
    ):
        result = box._tool_find_joinable_tables(table="sap_s6p.adrc", strategy="name_overlap")
    assert result["inference_source"] == "name_overlap"
    assert result["source_was_live"] is True
    targets = {r["target_table"] for r in result["joinable_tables"]}
    assert "kna1" in targets
    assert "lfa1" in targets


def test_value_overlap_uses_real_value_intersection(cfg_sap, tmp_path) -> None:
    """The data-touching strategy: name_overlap seeds the candidates,
    then for each one we sample distinct values from both sides and
    keep only candidates with a real intersection. Mock the sampler
    so the test runs without a live DB."""
    store = SQLiteHistoryStore(tmp_path / "history.db")
    store.init()
    _seed_columns(
        store,
        [
            ("sap", "sap_s6p", "adrc", None, "table", "", 0, 0),
            ("sap", "sap_s6p", "adrc", "addrnumber", "column", "TEXT", 1, 0),
            ("sap", "sap_s6p", "kna1", None, "table", "", 0, 0),
            ("sap", "sap_s6p", "kna1", "addrnumber", "column", "TEXT", 0, 0),
            # Coincidental name match but no value overlap — should be
            # filtered out by min_intersection.
            ("sap", "sap_s6p", "noise", None, "table", "", 0, 0),
            ("sap", "sap_s6p", "noise", "addrnumber", "column", "TEXT", 0, 0),
        ],
    )
    box = _make_box(cfg_sap, store)

    samples_by_table = {
        ("sap_s6p", "adrc", "addrnumber"): (
            ["0000000001", "0000000002", "0000000003", "0000000004", "0000000005"],
            5,
        ),
        ("sap_s6p", "kna1", "addrnumber"): (
            ["0000000001", "0000000002", "0000000003", "0000000099"],
            4,
        ),
        ("sap_s6p", "noise", "addrnumber"): (
            ["zzz1", "zzz2", "zzz3"],
            3,
        ),
    }

    def _fake_sampler(db, schema, table, column, limit):
        return samples_by_table.get(
            (schema, table, column),
            ([], None),
        )

    with (
        patch.object(ToolBox, "_connector_for_profile", return_value=object()),
        patch("amx.search.agent_tools._sample_distinct_values", side_effect=_fake_sampler),
    ):
        result = box._tool_find_joinable_tables(table="sap_s6p.adrc", strategy="value_overlap")
    assert result["inference_source"] == "value_overlap"
    targets = {r["target_table"]: r for r in result["joinable_tables"]}
    assert "kna1" in targets
    assert targets["kna1"]["overlap_count"] >= 3
    assert 0.0 < targets["kna1"]["overlap_ratio"] <= 1.0
    # ``noise`` shares the column name but no values — filtered out by
    # min_intersection=3.
    assert "noise" not in targets


def test_strategy_name_overlap_skips_fk_and_semantic(cfg_sap, tmp_path) -> None:
    store = SQLiteHistoryStore(tmp_path / "history.db")
    store.init()
    _seed_columns(
        store,
        [
            ("sap", "sap_s6p", "adrc", None, "table", "", 0, 0),
            ("sap", "sap_s6p", "adrc", "addrnumber", "column", "TEXT", 0, 0),
            ("sap", "sap_s6p", "kna1", None, "table", "", 0, 0),
            ("sap", "sap_s6p", "kna1", "addrnumber", "column", "TEXT", 0, 0),
        ],
    )
    box = _make_box(cfg_sap, store)
    result = box._tool_find_joinable_tables(table="sap_s6p.adrc", strategy="name_overlap")
    assert result["strategies_tried"] == ["name_overlap"]
    assert result["inference_source"] == "name_overlap"
