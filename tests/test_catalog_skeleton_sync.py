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

    def _populate_schema_metadata_cache(
        self, schema: str, *, ttl_seconds: float | None = None
    ) -> bool:
        """No-op stand-in: the real connector bulk-fills
        ``column_comments_cache`` here. The stub just signals "warm
        pass completed" so the skeleton sync stamps
        ``last_columns_sync_at`` and ``is_profile_fully_synced``
        returns True under the post-tightening contract.

        Records ``ttl_seconds`` because the skeleton sync now stamps the
        durable comment-cache TTL on this warm pass."""
        self.last_warm_ttl = ttl_seconds
        return True

    def _populate_catalogs_cache(
        self, catalog: str = "", *, ttl_seconds: float | None = None
    ) -> bool:
        """No-op stand-in for the durable ``schemas_cache`` re-stamp the
        warm pass now issues per container. The real connector re-runs
        ``bulk_catalog_metadata`` and writes the rows with the durable
        TTL; the stub just records ``ttl_seconds`` so the durable-schemas
        regression test can assert the sync stamps schemas durably (the
        fix for the "Catalog freshness failed on every open" bug)."""
        self.last_schemas_ttl = ttl_seconds
        return True


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
    """The cache never auto-expires. A profile synced two weeks ago
    is still ``fully_synced=True`` (the staleness warning is a
    separate UI-only signal driven by ``get_profile_state``).

    Pre-PR the helper rejected snapshots older than 7 days, which
    forced every sidebar / Ask expand of a week-old profile to fall
    through to the live DB. The user's contract now: cache lives
    forever, warn but never invalidate."""
    connector = _StubConnector(schemas=["public"], assets={"public": [("t", "table")]})
    _stub_build_connector(monkeypatch, connector)
    sync_profile_skeleton(_StubCfg(), "prof-a", fresh_catalog)
    assert fresh_catalog.is_profile_fully_synced("prof-a") is True

    # Force the stamp two weeks into the past.
    fourteen_days_ago = time.time() - 14 * 24 * 60 * 60
    with fresh_catalog._connect() as conn:  # noqa: SLF001
        conn.execute(
            "UPDATE catalog_profile_state SET last_full_sync_at = ? WHERE db_profile = ?",
            (fourteen_days_ago, "prof-a"),
        )

    # Still fully synced — no 7-day cliff anymore.
    assert fresh_catalog.is_profile_fully_synced("prof-a") is True

    # But the age signal is still available for the UI staleness pill.
    state = fresh_catalog.get_profile_state("prof-a")
    assert state["state"] == "done"
    assert state["last_full_sync_at"] is not None
    assert time.time() - state["last_full_sync_at"] > 7 * 24 * 60 * 60


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

    def _populate_schema_metadata_cache(self, schema: str) -> bool:
        return True


class _UnpinnedStubCfg:
    """Variant of :class:`_StubCfg` with no pinned default database.

    Used by the multi-database enumeration test below: with no
    ``database`` pinned on the profile, the skeleton sync falls back
    to ``list_databases()`` enumeration and walks every reachable
    database. The hard-limit short-circuit added for pinned profiles
    is intentionally bypassed.
    """

    class _DB:
        backend = "postgresql"
        database = ""
        catalog = ""
        project = ""

    db = _DB()


def test_sync_walks_every_database(
    fresh_catalog: SearchCatalog, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When the profile pins no default database, the skeleton sync
    enumerates every database under it and upserts each table with
    its own ``database_name`` stamp.

    Regression guard for the user-reported screenshot where
    ``LOCAL-POSTGRE`` showed identical schemas under SAP, bird_train,
    and bird_train_desc because the sync only walked one database
    AND the unique index didn't include ``database_name``. The
    pinned-default case is now hard-limited; see
    ``tests/test_skeleton_sync_pinned_default.py`` for that path.
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

    summary = sync_profile_skeleton(_UnpinnedStubCfg(), "local-postgre", fresh_catalog)
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


def test_skeleton_sync_warms_comments_with_durable_ttl(
    fresh_catalog: SearchCatalog, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The warm pass stamps the durable comment-cache TTL so a sync's
    imported comments don't evaporate after the 1-hour browse window
    (which would leave the cache-only read gate returning empty)."""
    from amx.db.connector import DURABLE_COMMENT_CACHE_TTL_SECONDS

    connector = _StubConnector(schemas=["public"], assets={"public": [("users", "table")]})
    _stub_build_connector(monkeypatch, connector)

    sync_profile_skeleton(_StubCfg(), "prof-a", fresh_catalog)

    assert getattr(connector, "last_warm_ttl", None) == DURABLE_COMMENT_CACHE_TTL_SECONDS


def test_skeleton_sync_restamps_schemas_with_durable_ttl(
    fresh_catalog: SearchCatalog, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The warm pass re-stamps ``schemas_cache`` with the durable TTL,
    not the 1-hour browse TTL ``list_schemas`` writes during enumeration.

    Regression guard for the reported bug: schemas_cache rows carried
    only a 1h TTL, so the startup ``gc_schemas_cache`` sweep emptied the
    table within the hour while the never-expiring
    ``is_profile_fully_synced`` markers stayed set — driving
    ``list_schemas``'s cache-only gate to serve an empty schema list and
    flip the freshness pill to a false "no schemas were visible" failure
    on every open."""
    from amx.db.connector import DURABLE_COMMENT_CACHE_TTL_SECONDS

    connector = _StubConnector(schemas=["public"], assets={"public": [("users", "table")]})
    _stub_build_connector(monkeypatch, connector)

    sync_profile_skeleton(_StubCfg(), "prof-a", fresh_catalog)

    assert getattr(connector, "last_schemas_ttl", None) == DURABLE_COMMENT_CACHE_TTL_SECONDS


def test_durable_schemas_cache_survives_startup_gc_sweep(
    fresh_catalog: SearchCatalog,
) -> None:
    """A schemas_cache row stamped with the durable sync TTL survives the
    startup ``gc_schemas_cache`` sweep, while a row left on the 1-hour
    browse TTL (here pre-expired) is purged.

    This pins the other half of the fix: the durable TTL chosen by the
    warm pass actually defeats the gc sweep that caused the bug, so a
    fully-synced profile's schemas stay cached until the next sync."""
    from amx.db.connector import DURABLE_COMMENT_CACHE_TTL_SECONDS

    store = ss._store  # noqa: SLF001 — the fixture-bound singleton

    # Durable row (what the warm pass now writes) and a browse-TTL row
    # that has already expired (what the pre-fix sync left behind).
    store.save_schemas_cache(
        db_profile="prof-durable",
        database="appdb",
        catalog="",
        entries={"public": None, "analytics": None},
        bulk_filled=True,
        ttl_seconds=DURABLE_COMMENT_CACHE_TTL_SECONDS,
    )
    store.save_schemas_cache(
        db_profile="prof-expired",
        database="appdb",
        catalog="",
        entries={"stale": None},
        bulk_filled=True,
        ttl_seconds=-1.0,  # already expired — mimics a 1h row past its window
    )

    swept = store.gc_schemas_cache()
    assert swept == 1  # only the expired row

    with fresh_catalog._connect() as conn:  # noqa: SLF001
        durable = conn.execute(
            "SELECT schema_name FROM schemas_cache WHERE db_profile = ? ORDER BY schema_name",
            ("prof-durable",),
        ).fetchall()
        expired = conn.execute(
            "SELECT schema_name FROM schemas_cache WHERE db_profile = ?",
            ("prof-expired",),
        ).fetchall()
    assert [r["schema_name"] for r in durable] == ["analytics", "public"]
    assert expired == []


def _first_synced(cat: SearchCatalog, profile: str, table: str) -> float | None:
    with cat._connect() as conn:  # noqa: SLF001
        row = conn.execute(
            "SELECT first_synced_at FROM catalog_entities "
            "WHERE db_profile = ? AND table_name = ? AND entity_kind = 'table'",
            (profile, table),
        ).fetchone()
    return None if row is None else row["first_synced_at"]


def test_first_synced_at_set_on_insert_preserved_on_resync(
    fresh_catalog: SearchCatalog, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``first_synced_at`` is the change-trigger signal: stamped when an
    asset first appears, never bumped on a re-sync (unlike
    ``last_synced_at``). A newly-appeared table carries a strictly later
    ``first_synced_at`` so a watermark diff can isolate it."""
    connector = _StubConnector(schemas=["public"], assets={"public": [("users", "table")]})
    _stub_build_connector(monkeypatch, connector)

    sync_profile_skeleton(_StubCfg(), "prof-a", fresh_catalog)
    first = _first_synced(fresh_catalog, "prof-a", "users")
    assert first is not None and first > 0

    # Re-sync the SAME table — first_synced_at must not move; last_synced_at must.
    time.sleep(0.01)
    sync_profile_skeleton(_StubCfg(), "prof-a", fresh_catalog)
    assert _first_synced(fresh_catalog, "prof-a", "users") == first

    # A brand-new table appears — its first_synced_at is strictly later,
    # so "what is new since I last looked" is a simple watermark compare.
    time.sleep(0.01)
    connector._assets["public"] = [("users", "table"), ("orders", "table")]  # noqa: SLF001
    sync_profile_skeleton(_StubCfg(), "prof-a", fresh_catalog)
    new_first = _first_synced(fresh_catalog, "prof-a", "orders")
    assert new_first is not None and new_first > first
    # The pre-existing table's stamp is still untouched.
    assert _first_synced(fresh_catalog, "prof-a", "users") == first
