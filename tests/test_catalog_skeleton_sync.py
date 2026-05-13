"""Skeleton sync + completeness gate regression coverage.

The catalog cache-first read path (sidebar, schedule/run scope pickers,
Ask agent tools) gates on ``SearchCatalog.is_profile_fully_synced`` —
which only flips True after a successful skeleton sync. These tests pin
both halves of that contract so a future refactor can't silently
re-introduce the "partial catalog presented as complete" bug the user
reported on PR #415.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import pytest

from amx.search.catalog import SearchCatalog
from amx.search.drift import sync_profile_skeleton
from amx.storage import sqlite_store as ss
from amx.storage.sqlite_store import SQLiteHistoryStore


class _StubConnector:
    """Minimal connector good enough for ``sync_profile_skeleton``.

    Returns the schemas + assets passed in the constructor; raises on
    every other connector call so a regression that starts asking the
    skeleton path for row counts (``profile_table``) gets caught.
    """

    def __init__(self, *, schemas: list[str], assets: dict[str, list[tuple[str, str]]]):
        self._schemas = schemas
        self._assets = assets

    def list_schemas(self) -> list[str]:
        return list(self._schemas)

    def list_assets(self, schema: str) -> list[tuple[str, str]]:
        return list(self._assets.get(schema, []))


class _StubCfg:
    """Stand-in for ``AMXConfig`` carrying just the fields the
    skeleton sync reads (``cfg.db.backend`` / ``database`` / ``catalog`` /
    ``project``)."""

    class _DB:
        backend = "postgresql"
        database = "appdb"
        catalog = ""
        project = ""

    db = _DB()


@pytest.fixture()
def fresh_catalog(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> SearchCatalog:
    db_path = tmp_path / "history.db"
    SQLiteHistoryStore(db_path).init()
    # Bind the freshly-initialised store as the module-level singleton
    # so SearchCatalog.from_history_store() resolves consistently for
    # both the skeleton sync helper and the cache-first readers.
    ss._store = SQLiteHistoryStore(db_path)  # noqa: SLF001
    monkeypatch.setattr(ss, "_store", ss._store, raising=False)  # noqa: SLF001
    cat = SearchCatalog(db_path)
    yield cat
    ss._store = None  # noqa: SLF001


def _stub_build_connector(monkeypatch: pytest.MonkeyPatch, connector: Any) -> None:
    """Replace the drift module's ``_build_connector`` so the skeleton
    sync gets a deterministic stand-in instead of opening a real
    SQLAlchemy engine."""

    def _fake(cfg: Any, profile: str):
        return connector

    import amx.search.drift as drift

    monkeypatch.setattr(drift, "_build_connector", _fake)


def test_state_transitions_none_syncing_done(
    fresh_catalog: SearchCatalog, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A clean skeleton sync runs every catalog_entities row in and
    leaves ``catalog_profile_state.state='done'``."""
    connector = _StubConnector(
        schemas=["public", "analytics"],
        assets={
            "public": [("users", "table"), ("orders", "table")],
            "analytics": [("daily_metrics", "view")],
        },
    )
    _stub_build_connector(monkeypatch, connector)

    # Before sync — state is "none".
    state = fresh_catalog.get_profile_state("prof-a")
    assert state["state"] == "none"
    assert fresh_catalog.is_profile_fully_synced("prof-a") is False

    summary = sync_profile_skeleton(_StubCfg(), "prof-a", fresh_catalog)
    assert summary["state"] == "done"
    assert summary["total"] == 3
    assert summary["processed"] == 3

    state = fresh_catalog.get_profile_state("prof-a")
    assert state["state"] == "done"
    assert state["total_tables"] == 3
    assert state["processed_tables"] == 3
    assert state["finished_at"] is not None
    assert state["last_full_sync_at"] is not None
    assert state["last_error"] == ""
    assert fresh_catalog.is_profile_fully_synced("prof-a") is True


def test_every_table_lands_in_catalog_entities(
    fresh_catalog: SearchCatalog, monkeypatch: pytest.MonkeyPatch
) -> None:
    connector = _StubConnector(
        schemas=["s1", "s2"],
        assets={
            "s1": [(f"t{i}", "table") for i in range(50)],
            "s2": [(f"t{i}", "table") for i in range(50)],
        },
    )
    _stub_build_connector(monkeypatch, connector)

    sync_profile_skeleton(_StubCfg(), "prof-a", fresh_catalog)

    with fresh_catalog._connect() as conn:  # noqa: SLF001
        rows = conn.execute(
            "SELECT COUNT(*) AS n FROM catalog_entities WHERE db_profile = ?",
            ("prof-a",),
        ).fetchone()
    assert int(rows["n"]) == 100


def test_failure_path_marks_state_failed(
    fresh_catalog: SearchCatalog, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A connector that crashes on ``list_schemas`` leaves the
    profile state at ``failed`` with the error text on the row — the
    pill's Retry surface reads this."""

    class _BadConnector:
        def list_schemas(self) -> list[str]:
            raise RuntimeError("permission denied")

    _stub_build_connector(monkeypatch, _BadConnector())

    summary = sync_profile_skeleton(_StubCfg(), "prof-a", fresh_catalog)
    assert summary["state"] == "failed"
    assert "permission denied" in summary["error"]

    state = fresh_catalog.get_profile_state("prof-a")
    assert state["state"] == "failed"
    assert "permission denied" in state["last_error"]
    assert fresh_catalog.is_profile_fully_synced("prof-a") is False


def test_empty_schemas_marks_state_failed(
    fresh_catalog: SearchCatalog, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``list_schemas`` returning ``[]`` without raising is treated as
    a failure, not a silent done. The user-reported scenario: a
    Databricks profile without a catalog pinned succeeds at connect
    but enumerates zero schemas. The previous behaviour flipped the
    pill to ``never · stale`` with no Retry signal; this branch is
    why we now mark the state as failed with a backend-specific
    actionable message."""
    connector = _StubConnector(schemas=[], assets={})
    _stub_build_connector(monkeypatch, connector)

    summary = sync_profile_skeleton(_StubCfg(), "prof-a", fresh_catalog)
    assert summary["state"] == "failed"
    assert "no schemas" in summary["error"].lower()

    state = fresh_catalog.get_profile_state("prof-a")
    assert state["state"] == "failed"
    assert "no schemas" in state["last_error"].lower()
    assert fresh_catalog.is_profile_fully_synced("prof-a") is False


def test_empty_schemas_databricks_message(
    fresh_catalog: SearchCatalog, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Databricks-backed profiles get a tailored error mentioning the
    catalog pin — the #1 reason ``list_schemas`` returns ``[]`` on UC
    setups."""
    connector = _StubConnector(schemas=[], assets={})
    _stub_build_connector(monkeypatch, connector)

    class _DatabricksCfg:
        class _DB:
            backend = "databricks"
            database = ""
            catalog = ""
            project = ""

        db = _DB()

    summary = sync_profile_skeleton(_DatabricksCfg(), "prof-dbr", fresh_catalog)
    assert summary["state"] == "failed"
    assert "catalog" in summary["error"].lower()


def test_backend_read_from_target_profile_not_active(
    fresh_catalog: SearchCatalog, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When the user clicks Sync All from an active 2-level profile,
    the empty-schemas error for a *different* 3-level profile in the
    batch must use the 3-level wording. Regression for the prod bug
    where dbr-oyk failed with the postgres-shaped 'check ``USAGE`` on
    one schema' message because the sync read backend from
    ``cfg.db`` (the active profile) instead of
    ``cfg.db_profiles[profile]`` (the target)."""
    connector = _StubConnector(schemas=[], assets={})
    _stub_build_connector(monkeypatch, connector)

    class _ActivePostgresDB:
        backend = "postgresql"
        database = "appdb"
        catalog = ""
        project = ""

    class _DatabricksDB:
        backend = "databricks"
        database = ""
        catalog = ""
        project = ""

    class _MixedCfg:
        db = _ActivePostgresDB()
        db_profiles = {"prof-dbr": _DatabricksDB()}

    summary = sync_profile_skeleton(_MixedCfg(), "prof-dbr", fresh_catalog)
    assert summary["state"] == "failed"
    assert "catalog" in summary["error"].lower()
    # And the postgres-only 2-level wording must NOT appear.
    assert "USAGE" not in summary["error"]


def test_completeness_gate_window(
    fresh_catalog: SearchCatalog, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A profile synced more than 7 days ago is no longer considered
    fully synced — the gate forces a refresh."""
    connector = _StubConnector(schemas=["public"], assets={"public": [("t", "table")]})
    _stub_build_connector(monkeypatch, connector)
    sync_profile_skeleton(_StubCfg(), "prof-a", fresh_catalog)
    assert fresh_catalog.is_profile_fully_synced("prof-a") is True

    # Force the stamp into the past.
    eight_days_ago = time.time() - 8 * 24 * 60 * 60
    with fresh_catalog._connect() as conn:  # noqa: SLF001
        conn.execute(
            "UPDATE catalog_profile_state SET last_full_sync_at = ? WHERE db_profile = ?",
            (eight_days_ago, "prof-a"),
        )

    assert fresh_catalog.is_profile_fully_synced("prof-a") is False


class _MultiDBConnector:
    """Connector stub that exposes ``list_databases`` so the skeleton
    sync enumerates and walks every database. Each database carries
    its own schemas + assets — the regression guard is that all rows
    land in ``catalog_entities`` with the correct ``database_name``
    stamp.

    The skeleton-sync's per-container loop re-builds a connector via
    ``replace(cfg.db_profiles[profile], database=name)``. The test's
    cfg has no ``db_profiles``, so :func:`_scoped_connector` falls back
    to ``_build_connector`` for every container — the monkeypatch
    points that at this same stub. We therefore track the *current*
    requested database via a class attribute that the test sets
    before each ``list_schemas`` call would happen; instead, we use a
    simpler pattern: this stub returns the union of every database's
    contents on the default connector enumeration, then a per-database
    factory closure returns the right one. The factory is what the
    monkeypatch installs.
    """

    def __init__(self, databases: dict[str, dict[str, list[tuple[str, str]]]]):
        self._databases = databases

    def list_databases(self) -> list[str]:
        return list(self._databases.keys())

    def list_catalogs(self) -> list[str]:
        return []


class _PerDBConnector:
    """Connector that knows about exactly one database's content —
    used as the per-container scoped connector handed to the skeleton
    sync's pass-1 / pass-2 loop."""

    def __init__(self, database: str, schemas: dict[str, list[tuple[str, str]]]):
        self.database = database
        self._schemas = schemas

    def list_schemas(self) -> list[str]:
        return list(self._schemas.keys())

    def list_assets(self, schema: str) -> list[tuple[str, str]]:
        return list(self._schemas.get(schema, []))


def test_sync_walks_every_database(
    fresh_catalog: SearchCatalog, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The skeleton sync enumerates every database under a profile and
    upserts each table with its own ``database_name`` stamp.

    Regression guard for the user-reported screenshot where
    ``LOCAL-POSTGRE`` showed identical schemas under SAP, bird_train,
    and bird_train_desc because the sync only walked the pinned
    database AND the unique index didn't include ``database_name``.
    """
    databases_payload: dict[str, dict[str, list[tuple[str, str]]]] = {
        "SAP": {"public": [("sap_users", "table"), ("sap_orders", "table")]},
        "bird_train": {"aviary": [("birds", "table"), ("species", "table")]},
        "bird_train_desc": {"meta": [("notes", "table")]},
    }

    enumerator = _MultiDBConnector(databases_payload)
    per_db = {name: _PerDBConnector(name, schemas) for name, schemas in databases_payload.items()}

    # Track the order ``_build_connector`` is called. First call is the
    # default enumerator; subsequent calls are per-container scoped
    # connectors. Because the test's ``_StubCfg`` has no ``db_profiles``,
    # ``_scoped_connector`` always defers to ``_build_connector`` —
    # which we hijack here to route by requested-database via a
    # mutable "current" pointer.
    call_log: list[str] = []
    current: dict[str, str] = {"db": ""}

    def _fake_build(cfg, profile):
        # On first call (no current set) return the enumerator.
        if not call_log:
            call_log.append("__enumerator__")
            return enumerator
        # Subsequent calls are per-database. The drift module's
        # _scoped_connector falls back to _build_connector for stubs
        # without a real db_profiles map, so we resolve the right
        # connector via the current pointer the loop drives.
        db = current["db"] or next(iter(databases_payload))
        call_log.append(db)
        return per_db[db]

    import amx.search.drift as drift

    monkeypatch.setattr(drift, "_build_connector", _fake_build)

    # Patch _scoped_connector so we can thread the current container
    # name through; the production helper takes the container from
    # cfg overlay, but the stub bypass needs an explicit signal.
    original_scoped = drift._scoped_connector

    def _fake_scoped(cfg, profile, container, is_three_level):
        if container:
            current["db"] = container
        else:
            current["db"] = ""
        return original_scoped(cfg, profile, container, is_three_level)

    monkeypatch.setattr(drift, "_scoped_connector", _fake_scoped)

    summary = sync_profile_skeleton(_StubCfg(), "local-postgre", fresh_catalog)
    assert summary["state"] == "done", summary
    assert summary["total"] == 5
    assert summary["processed"] == 5
    assert set(summary["containers"]) == {"SAP", "bird_train", "bird_train_desc"}

    # Every database has its own rows with the correct database_name.
    with fresh_catalog._connect() as conn:  # noqa: SLF001
        rows = conn.execute(
            """
            SELECT database_name, schema_name, table_name
            FROM catalog_entities
            WHERE db_profile = ?
            ORDER BY database_name, schema_name, table_name
            """,
            ("local-postgre",),
        ).fetchall()
    by_db: dict[str, set[tuple[str, str]]] = {}
    for r in rows:
        by_db.setdefault(r["database_name"], set()).add((r["schema_name"], r["table_name"]))
    assert by_db == {
        "SAP": {("public", "sap_users"), ("public", "sap_orders")},
        "bird_train": {("aviary", "birds"), ("aviary", "species")},
        "bird_train_desc": {("meta", "notes")},
    }


def test_explicit_databases_kwarg_skips_enumeration(
    fresh_catalog: SearchCatalog, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When ``databases=[...]`` is passed (the per-database refresh
    button in the sidebar) the sync walks exactly that list and skips
    ``list_databases``."""
    per_db = {"SAP": _PerDBConnector("SAP", {"public": [("sap_users", "table")]})}

    current: dict[str, str] = {"db": "SAP"}
    enumeration_called = {"hit": False}

    class _Probe:
        def list_databases(self) -> list[str]:
            enumeration_called["hit"] = True
            return ["SAP", "bird_train", "bird_train_desc"]

        def list_catalogs(self) -> list[str]:
            return []

    probe = _Probe()

    def _fake_build(cfg, profile):
        return per_db[current["db"]] if current["db"] in per_db else probe

    import amx.search.drift as drift

    monkeypatch.setattr(drift, "_build_connector", _fake_build)

    original_scoped = drift._scoped_connector

    def _fake_scoped(cfg, profile, container, is_three_level):
        current["db"] = container or "SAP"
        return original_scoped(cfg, profile, container, is_three_level)

    monkeypatch.setattr(drift, "_scoped_connector", _fake_scoped)

    summary = sync_profile_skeleton(_StubCfg(), "local-postgre", fresh_catalog, databases=["SAP"])
    assert summary["state"] == "done"
    assert summary["containers"] == ["SAP"]
    assert enumeration_called["hit"] is False


def test_cache_helpers_fall_through_on_incomplete_sync(
    fresh_catalog: SearchCatalog, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``_cached_schemas_for_profile`` and ``_cached_assets_for_profile_schema``
    return None until the skeleton sync flips ``state='done'`` — the
    sidebar / schedule / run pickers then fall through to the live DB.
    """
    import amx.web.routers.live_db as live_db

    # No state row yet — gate returns None.
    assert live_db._cached_schemas_for_profile("prof-a") is None
    assert live_db._cached_assets_for_profile_schema("prof-a", "public") is None

    # Run skeleton sync; gate flips True; helpers now return rows.
    connector = _StubConnector(schemas=["public"], assets={"public": [("users", "table")]})
    _stub_build_connector(monkeypatch, connector)
    sync_profile_skeleton(_StubCfg(), "prof-a", fresh_catalog)

    schemas = live_db._cached_schemas_for_profile("prof-a")
    assert schemas is not None
    assert {s["name"] for s in schemas} == {"public"}

    assets = live_db._cached_assets_for_profile_schema("prof-a", "public")
    assert assets is not None
    assert {a["name"] for a in assets} == {"users"}
