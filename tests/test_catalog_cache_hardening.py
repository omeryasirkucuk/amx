"""Regression coverage for the catalog-cache hardening pass.

Three orthogonal pieces ship in this PR; each gets a focused test
here so the next refactor catches a break early:

* FTS5 mirror — every ``_upsert_entity`` + description write must
  push a row into ``catalog_entities_fts`` so concept search MATCH
  queries return the row on the next call.
* ``record_applied_description`` — Studio's apply worker calls this
  so the catalog reflects the just-applied COMMENT ON live text the
  moment the worker emits ``job.done``.
* ``catalog_freshness`` endpoint shape — the Studio top-bar pill
  reads the returned shape; tests pin the keys so a payload reshuffle
  surfaces as a red test, not a silent UI regression.
"""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path

import pytest

from amx.search.catalog import SearchCatalog
from amx.storage.sqlite_store import SQLiteHistoryStore


@pytest.fixture()
def fresh_catalog(tmp_path: Path) -> SearchCatalog:
    db = tmp_path / "history.db"
    store = SQLiteHistoryStore(db)
    store.init()
    return SearchCatalog(db)


def _entity_ids(catalog: SearchCatalog) -> list[int]:
    with catalog._connect() as conn:  # noqa: SLF001
        return [int(r["id"]) for r in conn.execute("SELECT id FROM catalog_entities")]


def _fts_count(catalog: SearchCatalog) -> int:
    with catalog._connect() as conn:  # noqa: SLF001
        row = conn.execute("SELECT COUNT(*) AS n FROM catalog_entities_fts").fetchone()
        return int(row["n"] or 0)


def test_record_applied_description_writes_reviewed_row(
    fresh_catalog: SearchCatalog,
) -> None:
    fresh_catalog.record_applied_description(
        db_profile="prod-pg",
        db_backend="postgresql",
        database_name="analytics",
        schema_name="sales",
        table_name="orders",
        column_name="vehicle_plate",
        entity_kind="column",
        asset_kind="table",
        description="License plate of the vehicle that fulfilled the order.",
    )
    ids = _entity_ids(fresh_catalog)
    assert ids, "entity was not upserted"
    with fresh_catalog._connect() as conn:  # noqa: SLF001
        row = conn.execute(
            """
            SELECT ce.effective_status, ce.effective_source_kind,
                   cd.description_text, cd.source_kind, cd.applied_to_db
            FROM catalog_entities ce
            LEFT JOIN catalog_descriptions cd ON cd.id = ce.effective_description_id
            WHERE ce.id = ?
            """,
            (ids[0],),
        ).fetchone()
    assert row["source_kind"] == "reviewed"
    assert row["applied_to_db"] == 1
    assert "License plate" in row["description_text"]
    assert row["effective_source_kind"] == "reviewed"


def test_fts_mirror_is_populated_after_sync(fresh_catalog: SearchCatalog) -> None:
    fresh_catalog.record_applied_description(
        db_profile="prod-pg",
        db_backend="postgresql",
        database_name="analytics",
        schema_name="sales",
        table_name="vehicles",
        column_name="plate_number",
        entity_kind="column",
        asset_kind="table",
        description="License plate identifier for the vehicle row.",
    )
    assert _fts_count(fresh_catalog) >= 1
    # MATCH retrieves the row by the description blob.
    with fresh_catalog._connect() as conn:  # noqa: SLF001
        rows = conn.execute(
            "SELECT rowid FROM catalog_entities_fts WHERE catalog_entities_fts MATCH ?",
            ("plate",),
        ).fetchall()
    assert len(rows) >= 1


def test_fts_backfill_on_existing_catalog(tmp_path: Path) -> None:
    """A catalog written by an older AMX version (no FTS rows) must
    backfill the FTS table on the next ``init()`` so concept search
    works on the first ``/ask`` after upgrade."""
    db_path = tmp_path / "legacy.db"
    legacy = SQLiteHistoryStore(db_path)
    legacy.init()
    # Drop the FTS table to simulate a pre-FTS install.
    with sqlite3.connect(db_path) as conn:
        conn.execute("DROP TABLE catalog_entities_fts")
        conn.execute(
            """
            INSERT INTO catalog_entities (
                db_profile, schema_name, table_name, column_name, entity_kind,
                search_text
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                "legacy",
                "public",
                "vehicles",
                "plate",
                "column",
                "path=legacy.public.vehicles.plate\nlicense plate identifier",
            ),
        )
    # Re-init — the new FTS bootstrap runs and backfills.
    SQLiteHistoryStore(db_path).init()
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT COUNT(*) AS n FROM catalog_entities_fts").fetchone()
    assert int(row["n"]) >= 1


def test_upsert_entity_bumps_last_synced_at_on_update(tmp_path: Path) -> None:
    """``Sync all`` over an unchanged catalog must still move the
    ``last_synced_at`` timestamp forward — that's what the freshness
    pill reads. The legacy UPDATE clause only touched ``updated_at``
    and the pill stayed stuck on the prior value."""
    db_path = tmp_path / "history.db"
    SQLiteHistoryStore(db_path).init()
    catalog = SearchCatalog(db_path)

    with catalog._connect() as conn:  # noqa: SLF001
        catalog._upsert_entity(  # noqa: SLF001
            conn,
            db_profile="prof",
            db_backend="postgresql",
            database_name="db",
            schema_name="public",
            table_name="t",
            column_name=None,
            entity_kind="table",
            asset_kind="table",
        )
        first = conn.execute("SELECT last_synced_at FROM catalog_entities LIMIT 1").fetchone()
    first_ts = float(first["last_synced_at"])
    assert first_ts > 0

    # Sleep a hair so the time.time() return value moves forward
    # measurably even on fast machines, then re-upsert the same row.
    time.sleep(0.01)
    with catalog._connect() as conn:  # noqa: SLF001
        catalog._upsert_entity(  # noqa: SLF001
            conn,
            db_profile="prof",
            db_backend="postgresql",
            database_name="db",
            schema_name="public",
            table_name="t",
            column_name=None,
            entity_kind="table",
            asset_kind="table",
        )
        second = conn.execute("SELECT last_synced_at FROM catalog_entities LIMIT 1").fetchone()
    assert float(second["last_synced_at"]) > first_ts


def test_drift_probe_force_bypasses_cooldown(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """force=True must spawn the worker even when the per-profile
    cooldown is warm. Without this, a user who fires Sync all within
    60s of the auto-probe gets a silent no-op."""
    monkeypatch.delenv("AMX_SKIP_DRIFT_PROBE", raising=False)
    from amx.search import drift

    db_path = tmp_path / "history.db"
    SQLiteHistoryStore(db_path).init()
    # Stub history_store so fire_drift_probe gets past its gate.
    import amx.storage.sqlite_store as ss

    ss._store = SQLiteHistoryStore(db_path)  # noqa: SLF001

    drift._LAST_PROBE.clear()
    drift._LAST_PROBE["prof-a"] = time.time()  # cooldown is warm

    spawned: list[str] = []
    real_thread = drift.threading.Thread

    def _capture_thread(*args, **kwargs):
        spawned.append(kwargs.get("name") or "")
        return real_thread(target=lambda: None, name="capture", daemon=True)

    monkeypatch.setattr(drift.threading, "Thread", _capture_thread)
    try:
        drift.fire_drift_probe(None, ["prof-a"], force=True)
    finally:
        ss._store = None  # noqa: SLF001

    assert "amx-drift-probe" in spawned, "force=True did not spawn the worker"


def test_drift_probe_skipped_when_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AMX_SKIP_DRIFT_PROBE", "1")
    from amx.search.drift import fire_drift_probe

    # No connector, no exception, no thread spawn — the env var
    # short-circuits the function before any DB call.
    fire_drift_probe(None, ["any-profile"])


def test_drift_probe_cooldown_blocks_back_to_back_calls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("AMX_SKIP_DRIFT_PROBE", raising=False)
    from amx.search import drift

    drift._LAST_PROBE.clear()
    now = 1_000_000.0
    assert drift._cooldown_blocks("prof-a", now) is False
    # Same profile, +1s later — still cooling down.
    assert drift._cooldown_blocks("prof-a", now + 1) is True
    # Different profile is unaffected.
    assert drift._cooldown_blocks("prof-b", now + 1) is False


def test_catalog_freshness_endpoint_shape(tmp_path: Path) -> None:
    """The Studio top-bar pill reads this exact key set."""
    db_path = tmp_path / "history.db"
    SQLiteHistoryStore(db_path).init()
    now = time.time()
    fresh_row_at = now - 60
    # PR #501 bumped the staleness threshold from 24 hours to 7
    # days so the UI pill aligns with the new "cache never auto-
    # expires; nudge once a week" contract. Push the stale fixture
    # past two weeks so it still trips the warning regardless of
    # future threshold tweaks within reason.
    stale_row_at = now - (14 * 24 * 60 * 60)
    with sqlite3.connect(db_path) as conn:
        for db_profile, last in (("fresh-p", fresh_row_at), ("stale-p", stale_row_at)):
            conn.execute(
                """
                INSERT INTO catalog_entities (
                    db_profile, schema_name, table_name, entity_kind, last_synced_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (db_profile, "public", "t", "table", last),
            )

    # Stub history_store() to return our fresh handle.
    import amx.web.routers.catalog as catalog_router
    from amx.storage import sqlite_store as ss

    store = SQLiteHistoryStore(db_path)
    ss._store = store  # noqa: SLF001 — module-level singleton
    try:
        # cfg=None disables the ghost-profile filter so the legacy
        # behaviour (every profile surfaces) is still covered.
        payload = catalog_router.catalog_freshness(cfg=None)
    finally:
        ss._store = None  # noqa: SLF001

    assert payload["stale_profile_count"] == 1
    profiles = {p["profile"]: p for p in payload["profiles"]}
    assert profiles["fresh-p"]["stale"] is False
    assert profiles["stale-p"]["stale"] is True
    # Top-bar pill depends on these exact keys; pin them so a payload
    # reshuffle surfaces as a red test, not a silent UI regression.
    expected_keys = {
        "profile",
        "entity_count",
        "last_synced_at",
        "age_seconds",
        "stale",
    }
    assert expected_keys.issubset(profiles["fresh-p"].keys())


def test_catalog_freshness_filters_ghost_profiles(tmp_path: Path) -> None:
    """Profiles with catalog rows but not in ``cfg.db_profiles`` (the
    user-reported ``default`` tombstone) are dropped from the pill.
    They remain in ``catalog_entities`` on disk in case the user
    re-adds a profile with the same name, but they don't clutter the
    dropdown."""
    db_path = tmp_path / "history.db"
    SQLiteHistoryStore(db_path).init()
    now = time.time()
    with sqlite3.connect(db_path) as conn:
        # One ghost row (legacy ``default`` profile that's no longer
        # in the user's config) + one real row.
        for db_profile in ("default", "local-postgre"):
            conn.execute(
                """
                INSERT INTO catalog_entities (
                    db_profile, schema_name, table_name, entity_kind, last_synced_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (db_profile, "public", "t", "table", now - 60),
            )

    import amx.web.routers.catalog as catalog_router
    from amx.storage import sqlite_store as ss

    store = SQLiteHistoryStore(db_path)
    ss._store = store  # noqa: SLF001

    class _Cfg:
        # Only ``local-postgre`` is in the active config.
        db_profiles = {"local-postgre": object()}

    try:
        payload = catalog_router.catalog_freshness(cfg=_Cfg())
    finally:
        ss._store = None  # noqa: SLF001

    names = {p["profile"] for p in payload["profiles"]}
    assert names == {"local-postgre"}
    # Ghost stays on disk — re-adding ``default`` later should pick
    # those rows back up.
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM catalog_entities WHERE db_profile = 'default'"
        ).fetchone()
    assert int(row[0]) == 1
