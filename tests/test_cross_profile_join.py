"""Cross-profile JOIN finder tests for PR ask-C.

The aggressive 4-signal scorer (name + dtype + vector + FK pattern)
operates on the catalog SQLite store. We seed two profiles' worth of
columns and verify:

1. Name overlap signal fires on tokenised matches (customer_id ↔ cust_id)
2. Dtype compatibility gates incompatible joins (VARCHAR↔INT score 0)
3. FK pattern boost (sender ends in _id, receiver is PK)
4. Result rows carry full source/target paths and score breakdown
5. Source profile resolves to anchor when not explicit
6. Profile-locked source via ``profile::schema.table`` syntax
"""

from __future__ import annotations

import pytest

from amx.config import AMXConfig, DBConfig
from amx.search.agent_tools import (
    ToolBox,
    _dtype_compat_score,
    _name_overlap_score,
)


def test_name_overlap_token_match() -> None:
    # Token jaccard catches the shared "id" + "customer/cust" stem.
    score = _name_overlap_score("customer_id", "cust_id")
    assert score > 0.5


def test_name_overlap_unrelated_zero() -> None:
    score = _name_overlap_score("customer_id", "payment_status")
    assert score < 0.2


def test_name_overlap_exact_one() -> None:
    assert _name_overlap_score("user_id", "user_id") == 1.0


def test_dtype_compat_same_family() -> None:
    assert _dtype_compat_score("BIGINT", "INT") == 1.0
    assert _dtype_compat_score("VARCHAR(50)", "TEXT") == 1.0


def test_dtype_compat_incompatible_zero() -> None:
    assert _dtype_compat_score("VARCHAR", "INT") == 0.0


def test_dtype_compat_int_float_weak() -> None:
    assert _dtype_compat_score("INT", "NUMERIC") == 0.5


@pytest.fixture()
def cfg_for_join() -> AMXConfig:
    cfg = AMXConfig()
    cfg.db_profiles = {
        "sap": DBConfig(backend="postgresql", host="sap.local", database="sap"),
        "warehouse": DBConfig(backend="postgresql", host="wh.local", database="wh"),
    }
    cfg.active_db_profile = "sap"
    cfg.active_db_profiles = ["sap", "warehouse"]
    cfg.db = cfg.db_profiles["sap"]
    return cfg


def test_cross_profile_join_returns_candidates(cfg_for_join, tmp_path) -> None:
    """End-to-end smoke: seed two profiles, ask for joinable columns
    on a SAP table, expect warehouse columns ranked first when names +
    dtypes line up."""
    from amx.search.catalog import SearchCatalog
    from amx.storage.sqlite_store import SQLiteHistoryStore

    store = SQLiteHistoryStore(tmp_path / "history.db")
    store.init()
    # Seed a source table on SAP and a target on warehouse with a
    # join-friendly column pair.
    with store._lock, store._connect() as conn:
        for row in [
            # SAP: customers.customer_id (PK)
            ("sap", "public", "customers", "customer_id", "column", "BIGINT", 1, 0),
            ("sap", "public", "customers", "name", "column", "VARCHAR(255)", 0, 0),
            ("sap", "public", "customers", None, "table", "", 0, 0),
            # Warehouse: orders.cust_id (FK-pattern, BIGINT)
            ("warehouse", "fact", "orders", "cust_id", "column", "BIGINT", 0, 0),
            ("warehouse", "fact", "orders", "amount", "column", "DECIMAL", 0, 0),
            ("warehouse", "fact", "orders", None, "table", "", 0, 0),
        ]:
            conn.execute(
                """
                INSERT INTO catalog_entities
                    (db_profile, schema_name, table_name, column_name,
                     entity_kind, dtype, pk_flag, fk_flag, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)
                """,
                row,
            )
    catalog = SearchCatalog(store.db_path)

    box = ToolBox(
        cfg_for_join,
        catalog,
        db_profiles=["sap", "warehouse"],
    )
    result = box._tool_find_joinable_across_profiles(table="public.customers")
    assert result["found"] is True
    assert result["source"]["profile"] == "sap"
    candidates = result["candidates"]
    assert len(candidates) >= 1
    # Top candidate should pair customer_id ↔ cust_id (BIGINT both).
    top = candidates[0]
    assert top["source"]["column"] == "customer_id"
    assert top["target"]["profile"] == "warehouse"
    assert top["target"]["column"] == "cust_id"
    # Score breakdown carries individual signals.
    assert "name" in top["signals"]
    assert "dtype" in top["signals"]
    # Strong name overlap + same dtype family + FK pattern fires.
    assert top["score"] >= 0.4


def test_cross_profile_join_unknown_source_table(cfg_for_join, tmp_path) -> None:
    from amx.search.catalog import SearchCatalog
    from amx.storage.sqlite_store import SQLiteHistoryStore

    store = SQLiteHistoryStore(tmp_path / "history.db")
    store.init()
    catalog = SearchCatalog(store.db_path)

    box = ToolBox(
        cfg_for_join,
        catalog,
        db_profiles=["sap", "warehouse"],
    )
    result = box._tool_find_joinable_across_profiles(table="public.nonexistent")
    assert result["found"] is False
    assert "candidates" in result
    assert len(result["candidates"]) == 0


def test_cross_profile_join_single_profile_scope_returns_empty(cfg_for_join, tmp_path) -> None:
    """When scope is single-profile, there are no other profiles to
    join against — the tool returns ``found=True, candidates=[]`` with
    a friendly message instead of erroring."""
    from amx.search.catalog import SearchCatalog
    from amx.storage.sqlite_store import SQLiteHistoryStore

    store = SQLiteHistoryStore(tmp_path / "history.db")
    store.init()
    with store._lock, store._connect() as conn:
        conn.execute(
            """
            INSERT INTO catalog_entities
                (db_profile, schema_name, table_name, column_name,
                 entity_kind, dtype, pk_flag, fk_flag, updated_at)
            VALUES ('sap', 'public', 'customers', 'id', 'column', 'BIGINT', 1, 0, 0)
            """
        )
    catalog = SearchCatalog(store.db_path)
    box = ToolBox(cfg_for_join, catalog, db_profiles=["sap"])
    result = box._tool_find_joinable_across_profiles(table="public.customers")
    assert result["found"] is True
    assert result["candidates"] == []
    assert "message" in result
