"""Live-DB tools must refuse-with-hint on unpinned 2-level profiles.

Reported (CLI + Studio screenshots): with profile `postgre`
(PostgreSQL, no `database` pinned) in scope, asking
"which tables can we reach" replied with either a blocking
"please /use-db <db> first" picker (CLI) or a wrong "0 tables in
public" answer (Studio — the connector silently fell through to the
`postgres` bootstrap maintenance database).

Root cause: ``_list_schemas_on_profile`` and ``_list_tables_on_profile``
opened a connection against the empty database, which the driver
substituted for the bootstrap DB. The LLM then dutifully reported
that wrong scope as if it were the truth.

Fix: detect the 2-level + unpinned state at metadata level and
return ``unpinned: True`` with a hint telling the LLM to call
``list_databases(with_counts=True)`` (which fans out across every
reachable database) or pass ``database=`` explicitly. This matches
Studio's multi-profile browse model: an unpinned profile is normal,
the user spans the server.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from amx.config import AMXConfig, DBConfig
from amx.search.agent_tools import ToolBox


@pytest.fixture()
def cfg_unpinned_postgres() -> AMXConfig:
    cfg = AMXConfig()
    cfg.db_profiles = {
        "postgre": DBConfig(
            backend="postgresql",
            host="pg.local",
            database="",  # unpinned — the bug-report scenario
        ),
    }
    cfg.active_db_profile = "postgre"
    cfg.active_db_profiles = ["postgre"]
    cfg.db = cfg.db_profiles["postgre"]
    return cfg


def _stub_connector_should_not_run() -> MagicMock:
    """A connector whose live methods would explode if reached.

    The point is to verify the unpinned-2-level branch refuses BEFORE
    opening a live connection; if these methods get called the test
    surfaces a clear "should not have been reached" failure.
    """
    conn = MagicMock()
    conn.list_schemas = MagicMock(side_effect=AssertionError("must not run for unpinned 2-level"))
    conn.list_tables = MagicMock(side_effect=AssertionError("must not run for unpinned 2-level"))
    conn.list_assets = MagicMock(side_effect=AssertionError("must not run for unpinned 2-level"))
    return conn


def test_list_schemas_on_unpinned_2level_profile_refuses_with_hint(
    cfg_unpinned_postgres,
) -> None:
    """``_list_schemas_on_profile`` on an unpinned PostgreSQL profile
    returns ``unpinned: True`` with a hint, NOT a silent fall-through
    to the bootstrap database."""
    box = ToolBox(
        cfg_unpinned_postgres,
        MagicMock(),
        db_profiles=["postgre"],
        db_connectors={"postgre": _stub_connector_should_not_run()},
    )
    result = box._list_schemas_on_profile("postgre")
    assert result["unpinned"] is True
    assert result["pinned_database"] is None
    assert result["schemas"] == []
    assert result["count"] == 0
    assert "list_databases" in result["error"]
    assert "with_counts" in result["error"] or "database=" in result["error"]


def test_list_tables_on_unpinned_2level_profile_refuses_with_hint(
    cfg_unpinned_postgres,
) -> None:
    """Same defence for ``_list_tables_on_profile`` — refuses to
    enumerate against the bootstrap DB."""
    box = ToolBox(
        cfg_unpinned_postgres,
        MagicMock(),
        db_profiles=["postgre"],
        db_connectors={"postgre": _stub_connector_should_not_run()},
    )
    result = box._list_tables_on_profile("postgre", "public")
    assert result["unpinned"] is True
    assert result["found"] is False
    assert result["pinned_database"] is None
    assert result["tables"] == []
    assert result["count"] == 0
    assert "list_databases" in result["error"]


def test_pinned_2level_profile_runs_live_query() -> None:
    """A pinned 2-level profile is NOT refused — the existing live
    path runs as before. The unpinned guard must not regress the
    common pinned case."""
    cfg = AMXConfig()
    cfg.db_profiles = {
        "pinned-pg": DBConfig(
            backend="postgresql",
            host="pg.local",
            database="bird_train",
        ),
    }
    cfg.active_db_profile = "pinned-pg"
    cfg.active_db_profiles = ["pinned-pg"]
    cfg.db = cfg.db_profiles["pinned-pg"]

    conn = MagicMock()
    conn.list_schemas = MagicMock(return_value=["public", "sales"])
    conn.cfg = cfg.db_profiles["pinned-pg"]

    box = ToolBox(
        cfg,
        MagicMock(),
        db_profiles=["pinned-pg"],
        db_connectors={"pinned-pg": conn},
    )
    result = box._list_schemas_on_profile("pinned-pg")
    assert "unpinned" not in result
    assert result["schemas"] == ["public", "sales"]
    assert result["pinned_database"] == "bird_train"


def test_unpinned_3level_profile_is_NOT_refused() -> None:
    """3-level backends (Databricks UC, BigQuery) have their own
    auto-pick path for unpinned catalogs (``_resolve_catalog_or_autopick``).
    The 2-level unpinned guard must not catch them by mistake."""
    cfg = AMXConfig()
    cfg.db_profiles = {
        "dbr": DBConfig(
            backend="databricks",
            host="dbc-xxx.databricks.com",
            catalog="",  # unpinned 3-level
        ),
    }
    cfg.active_db_profile = "dbr"
    cfg.active_db_profiles = ["dbr"]
    cfg.db = cfg.db_profiles["dbr"]

    conn = MagicMock()
    conn.list_schemas = MagicMock(return_value=["default"])
    conn.cfg = cfg.db_profiles["dbr"]

    box = ToolBox(
        cfg,
        MagicMock(),
        db_profiles=["dbr"],
        db_connectors={"dbr": conn},
    )
    result = box._list_schemas_on_profile("dbr")
    # 3-level path stays as-is — no unpinned refusal.
    assert "unpinned" not in result
    assert result["schemas"] == ["default"]


def test_per_profile_payload_carries_pinned_fields_for_anti_hallucination() -> None:
    """Every per-profile payload must carry ``pinned_database`` and
    ``pinned_catalog`` so the LLM sees each profile's pinned scope
    independently. This is the defence against the Studio bug
    'postgre (amx_test.public): 0 tables' — `amx_test` is `dbr`'s
    catalog, not `postgre`'s, and the tool result must make that
    boundary obvious."""
    cfg = AMXConfig()
    cfg.db_profiles = {
        "pinned-pg": DBConfig(
            backend="postgresql",
            host="pg.local",
            database="bird_train",
        ),
    }
    cfg.active_db_profile = "pinned-pg"
    cfg.active_db_profiles = ["pinned-pg"]
    cfg.db = cfg.db_profiles["pinned-pg"]

    conn = MagicMock()
    conn.list_schemas = MagicMock(return_value=["public"])
    conn.list_tables = MagicMock(return_value=["t1", "t2"])
    conn.list_assets = MagicMock(side_effect=AttributeError)
    conn.cfg = cfg.db_profiles["pinned-pg"]

    box = ToolBox(
        cfg,
        MagicMock(),
        db_profiles=["pinned-pg"],
        db_connectors={"pinned-pg": conn},
    )
    schemas_result = box._list_schemas_on_profile("pinned-pg")
    assert schemas_result["pinned_database"] == "bird_train"
    assert schemas_result["pinned_catalog"] is None

    tables_result = box._list_tables_on_profile("pinned-pg", "public")
    assert tables_result["pinned_database"] == "bird_train"
    assert tables_result["pinned_catalog"] is None
