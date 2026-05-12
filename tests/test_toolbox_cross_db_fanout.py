"""Cross-database fanout for unpinned 2-level backends.

A user reported that ``/ask`` couldn't find a table on an unpinned
PostgreSQL profile (``local-postgre`` with ``cfg.db.database == ''``)
even though the table existed under a different database on the same
server. The agent's own diagnostic was accurate: ``find_table_by_name``
and ``describe_table`` were issuing live-DB queries against whatever
database the JDBC URL happened to default to, never sweeping the rest
of the server.

These tests pin the fix: for an unpinned 2-level backend, the
``find_table_by_name`` walk iterates over every database the server
exposes, and the response surfaces ``resolved_databases`` so the LLM
can pass ``database=…`` straight to ``describe_table``. The
``describe_table`` tool itself fans out the same way as a safety net
when the LLM forgets.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from amx.search.agent_tools import ToolBox


class _StubAdapter:
    """Pretend to be a postgres-style 2-level adapter."""

    def supports_catalogs(self) -> bool:
        return False


def _stub_connector(database: str, schemas: dict[str, list[str]]):
    """Return a fake DatabaseConnector that responds to the live-DB
    methods the cross-DB fanout exercises."""
    cfg = SimpleNamespace(
        database=database,
        catalog="",
        backend="postgresql",
        project="",
    )
    conn = MagicMock(name=f"connector[{database}]")
    conn.cfg = cfg
    conn.supports_catalogs.return_value = False
    conn.list_schemas.return_value = list(schemas.keys())
    conn.list_assets.side_effect = lambda sch: [(t, "table") for t in schemas.get(sch, [])]
    conn.list_assets_bulk = None  # force the per-schema path
    return conn


def _bare_toolbox(
    *,
    db_database: str,
    db_profile: str = "local-postgre",
):
    """Build a ToolBox via ``__new__`` and wire just enough state so
    ``_tool_find_table_by_name`` runs end-to-end without touching real
    Chroma / Postgres."""
    cfg_db = SimpleNamespace(database=db_database, catalog="", backend="postgresql", project="")
    cfg = SimpleNamespace(
        db=cfg_db,
        db_profiles={db_profile: cfg_db},
        doc_profiles={},
        code_profiles={},
        doc_profile_linked_dbs={},
        code_profile_linked_dbs={},
    )
    catalog = MagicMock()
    catalog.find_tables_by_exact_name.return_value = []
    catalog._connect.return_value.__enter__.return_value.execute.return_value.fetchall.return_value = []
    catalog._connect.return_value.__exit__.return_value = False

    tb = ToolBox.__new__(ToolBox)
    tb.cfg = cfg
    tb.catalog = catalog
    tb.db_profile = db_profile
    tb.db_profiles = [db_profile]
    tb._connectors = {}
    tb._owned_connectors = set()
    tb._db = None
    # ``_live_db`` will fall back to this factory if no connector is
    # primed via ``tb._db = …``. Tests that exercise the database
    # fanout set ``_db`` directly; tests that don't get an anchor
    # MagicMock from the factory so the call doesn't AttributeError.
    tb._db_factory = lambda: MagicMock()
    return tb


def test_find_table_by_name_sweeps_all_databases_when_unpinned() -> None:
    """Regression: unpinned PostgreSQL profile must walk every visible
    database when looking for an exact-match table.

    Pre-fix behaviour: ``self._live_db()`` returned a connector bound
    to the connection-time default database; ``list_schemas`` only
    saw that database's schemas → ``find_table_by_name("vbrk")`` came
    back empty even when ``SAP.sap_s6p.vbrk`` existed.

    Post-fix: the walk iterates over ``list_databases()`` and ends up
    finding the table in the right database. The response surfaces
    ``resolved_databases[path] = "SAP"`` so the LLM can target
    ``describe_table`` correctly on the next call.
    """
    tb = _bare_toolbox(db_database="")  # no pinned database

    public = _stub_connector("public", {"public": ["users", "orders"]})
    public.list_databases.return_value = ["public", "SAP"]
    sap = _stub_connector("SAP", {"sap_s6p": ["vbrk", "kna1"]})

    def _connector_for_database(database):
        if not database or database == "public":
            return public
        if database == "SAP":
            return sap
        raise AssertionError(f"unexpected database: {database!r}")

    tb._db = public
    tb._connector_for_database = _connector_for_database  # type: ignore[method-assign]

    out = tb._tool_find_table_by_name("vbrk")

    assert "sap_s6p.vbrk" in out["from_live_db"]
    assert "sap_s6p.vbrk" in out["matches"]
    # The resolved-database map lets the LLM route describe_table
    # without re-running the sweep.
    assert out["resolved_databases"].get("sap_s6p.vbrk") == "SAP"


def test_find_table_by_name_keeps_legacy_path_when_database_pinned() -> None:
    """Pinned profiles must NOT trigger the fanout — the legacy single-
    database walk is the right behaviour and the cheap one. The test
    confirms ``list_databases`` is never consulted when a database is
    pinned and that the walk only sees the pinned database's schemas."""
    tb = _bare_toolbox(db_database="warehouse")

    warehouse = _stub_connector("warehouse", {"public": ["orders"]})
    warehouse.list_databases.return_value = ["warehouse", "ANOTHER"]
    tb._db = warehouse

    out = tb._tool_find_table_by_name("orders")

    assert "public.orders" in out["from_live_db"]
    warehouse.list_databases.assert_not_called()


def test_describe_table_database_kwarg_routes_directly() -> None:
    """When the LLM passes ``database="SAP"`` based on a prior
    ``find_table_by_name`` result, ``describe_table`` must build a
    connector against THAT database and not pay for the cross-DB
    sweep again."""
    tb = _bare_toolbox(db_database="")

    sap = _stub_connector("SAP", {"sap_s6p": ["vbrk"]})
    sap_profile = MagicMock(name="profile[SAP]")
    sap_profile.columns = []
    sap_profile.existing_comment = ""
    sap_profile.row_count = 0
    sap_profile.analytics = None
    sap.profile_table.return_value = sap_profile

    seen_databases: list[str | None] = []

    def _connector_for_database(database):
        seen_databases.append(database)
        if database == "SAP":
            return sap
        raise AssertionError(f"unexpected database: {database!r}")

    tb._connector_for_database = _connector_for_database  # type: ignore[method-assign]
    # ``_resolve_catalog_or_autopick`` only matters for 3-level
    # backends; the postgres stub returns no catalog, so short-circuit.
    tb._resolve_catalog_or_autopick = lambda _db, _explicit: ("", [], [])  # type: ignore[method-assign]
    tb._scoped_catalog = lambda _db, _cat: _NullCtx()  # type: ignore[method-assign]

    out = tb._tool_describe_table(
        schema="sap_s6p",
        table="vbrk",
        database="SAP",
    )

    assert out["found"] is True
    assert out["resolved_database"] == "SAP"
    # Only one connector was built — the explicit ``database=`` lookup
    # short-circuits before ``_databases_to_sweep`` runs.
    assert seen_databases == ["SAP"]


class _NullCtx:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False
