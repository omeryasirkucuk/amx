"""Tests for the in-process wizard memo + the drift-probe cache-age gate.

Two cache leaks the user reported as "tables aren't cached, the DB
still gets a query even without /refresh":

1. The wizard's ``list_catalogs`` / ``list_databases`` had no cache at
   all. Every picker invocation fired a fresh ``SHOW CATALOGS`` /
   ``SHOW DATABASES`` even when the same connector had just answered
   the same question. ``DatabaseConnector`` now memos those listings
   in-process for ``_listing_memo_ttl_seconds`` (default 5 minutes)
   and clears the memo on ``reconnect()``.

2. ``fire_drift_probe`` was rate-limited only by a 60-second per-profile
   cooldown. Past the cooldown, every ``/ask`` handshake re-issued the
   probe even when ``schemas_cache`` was minutes old and would give
   the same answer. The probe now also skips when the persistent
   schema cache for the profile was refreshed within
   ``_resolve_min_age_seconds()`` (default 5 minutes).

These tests pin both behaviors at the connector / drift module level
without standing up a live database — the adapter is stubbed.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from amx.config import DBConfig
from amx.storage.sqlite_store import SQLiteHistoryStore


@dataclass
class _StubAdapter:
    """Records every ``list_catalogs`` / ``list_databases`` call.

    Returns canned data so the connector code under test never opens
    a real engine. The connector's ``self.engine`` property is itself
    monkey-patched to skip the SQLAlchemy round-trip.
    """

    name: str = "stub"
    catalog_calls: int = 0
    database_calls: int = 0
    catalogs: list[str] | None = None
    databases: list[str] | None = None

    def list_catalogs(self, _engine: Any) -> list[str]:
        self.catalog_calls += 1
        return list(self.catalogs or ["main", "dev"])

    def list_databases(self, _engine: Any) -> list[str]:
        self.database_calls += 1
        return list(self.databases or ["analytics", "raw"])


def _stub_connector(monkeypatch: pytest.MonkeyPatch) -> Any:
    """Build a ``DatabaseConnector`` with stubbed adapter + engine.

    The connector lazy-imports ``ensure_backend_driver`` and
    ``get_adapter`` inside ``__init__``; patch them at their source
    modules so the real driver-install + adapter wiring is skipped.
    """
    from amx.db import connector as connector_mod

    monkeypatch.setattr("amx.db.drivers.ensure_backend_driver", lambda _: None)
    monkeypatch.setattr("amx.db.adapters.get_adapter", lambda _cfg: _StubAdapter())

    db = connector_mod.DatabaseConnector(
        DBConfig(backend="postgresql"),
        profile_name="stub-profile",
    )
    # Bypass real engine construction — list_* never touches the engine
    # except to pass it through to the adapter, and the stub ignores it.
    db._engine = MagicMock(name="engine")
    return db


def test_list_catalogs_serves_repeat_calls_from_memo(monkeypatch: pytest.MonkeyPatch) -> None:
    """Two back-to-back wizard calls must hit the adapter once."""
    from amx.db import connector as connector_mod

    # Reset process-wide counters so prior tests don't pollute the count.
    connector_mod._LISTING_MEMO_COUNTERS["catalogs"] = {"hit": 0, "miss": 0}

    db = _stub_connector(monkeypatch)
    first = db.list_catalogs()
    second = db.list_catalogs()
    assert first == second == ["main", "dev"]
    assert db._adapter.catalog_calls == 1, "second call hit the adapter again"
    snap = connector_mod.get_listing_memo_counters()
    assert snap["catalogs"]["miss"] == 1
    assert snap["catalogs"]["hit"] == 1


def test_list_databases_serves_repeat_calls_from_memo(monkeypatch: pytest.MonkeyPatch) -> None:
    """Same contract as catalogs — wizard re-prompts don't re-list."""
    from amx.db import connector as connector_mod

    connector_mod._LISTING_MEMO_COUNTERS["databases"] = {"hit": 0, "miss": 0}

    db = _stub_connector(monkeypatch)
    db.list_databases()
    db.list_databases()
    db.list_databases()
    assert db._adapter.database_calls == 1
    snap = connector_mod.get_listing_memo_counters()
    assert snap["databases"]["miss"] == 1
    assert snap["databases"]["hit"] == 2


def test_listing_memo_respects_ttl(monkeypatch: pytest.MonkeyPatch) -> None:
    """Past the TTL the memo expires and the adapter is hit again."""
    db = _stub_connector(monkeypatch)
    db._listing_memo_ttl_seconds = 0.01

    db.list_catalogs()
    time.sleep(0.02)
    db.list_catalogs()
    assert db._adapter.catalog_calls == 2


def test_reconnect_clears_listing_memo(monkeypatch: pytest.MonkeyPatch) -> None:
    """Re-pinning the database (the picker mutates ``cfg`` and calls
    reconnect) must drop the memo — the listing for the new scope can
    differ from the old one."""
    db = _stub_connector(monkeypatch)
    db.list_databases()
    assert db._adapter.database_calls == 1

    # ``reconnect`` disposes the engine; supply a mock that allows it.
    db._engine = MagicMock()
    db.reconnect()
    db._engine = MagicMock(name="engine-after-reconnect")

    db.list_databases()
    assert db._adapter.database_calls == 2, "memo survived reconnect()"


def test_invalidate_listing_memo_drops_both_kinds(monkeypatch: pytest.MonkeyPatch) -> None:
    db = _stub_connector(monkeypatch)
    db.list_catalogs()
    db.list_databases()
    db.invalidate_listing_memo()
    db.list_catalogs()
    db.list_databases()
    assert db._adapter.catalog_calls == 2
    assert db._adapter.database_calls == 2


def test_list_catalogs_import_error_propagates(monkeypatch: pytest.MonkeyPatch) -> None:
    """``ImportError`` from a missing optional driver must still reach the
    catalog picker as the actionable hint — the memo path doesn't
    swallow it."""
    db = _stub_connector(monkeypatch)

    def _raises(_engine: Any) -> list[str]:
        raise ImportError("install the [databricks] extra")

    db._adapter.list_catalogs = _raises  # type: ignore[assignment]

    with pytest.raises(ImportError):
        db.list_catalogs()


# ── Drift probe cache-age gate ─────────────────────────────────────────


def _make_history_store(tmp_path: Path) -> SQLiteHistoryStore:
    store = SQLiteHistoryStore(tmp_path / "history.db")
    store.init()
    return store


def test_schemas_cache_recently_refreshed_true_when_fresh(tmp_path: Path) -> None:
    from amx.search import drift

    store = _make_history_store(tmp_path)
    # Stamp a row "now" so the freshness check passes.
    store.save_schemas_cache(
        db_profile="prof-a",
        database="db1",
        catalog="",
        entries={"public": None},
        bulk_filled=True,
    )
    now = time.time()
    assert drift._schemas_cache_recently_refreshed(store, "prof-a", 300.0, now) is True


def test_schemas_cache_recently_refreshed_false_when_stale(tmp_path: Path) -> None:
    from amx.search import drift

    store = _make_history_store(tmp_path)
    store.save_schemas_cache(
        db_profile="prof-a",
        database="db1",
        catalog="",
        entries={"public": None},
        bulk_filled=True,
    )
    # Pretend ten minutes elapsed since the stamp.
    assert (
        drift._schemas_cache_recently_refreshed(store, "prof-a", 60.0, time.time() + 600.0) is False
    )


def test_schemas_cache_recently_refreshed_false_when_missing(tmp_path: Path) -> None:
    from amx.search import drift

    store = _make_history_store(tmp_path)
    assert (
        drift._schemas_cache_recently_refreshed(store, "no-such-profile", 300.0, time.time())
        is False
    )


def test_drift_probe_skipped_when_schemas_cache_fresh(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Past the 60s cooldown the cache-age gate is the new short-circuit.

    Stamp a schemas_cache row "now" for the profile, then call
    ``fire_drift_probe`` — the worker thread must not spawn and the
    skipped counter must increment.
    """
    monkeypatch.delenv("AMX_SKIP_DRIFT_PROBE", raising=False)
    monkeypatch.delenv("AMX_DRIFT_PROBE_MIN_AGE_SEC", raising=False)
    from amx.search import drift

    store = _make_history_store(tmp_path)
    store.save_schemas_cache(
        db_profile="prof-a",
        database="db1",
        catalog="",
        entries={"public": None},
        bulk_filled=True,
    )
    import amx.storage.sqlite_store as ss

    monkeypatch.setattr(ss, "_store", store, raising=False)
    monkeypatch.setattr(ss, "history_store", lambda: store)

    drift._LAST_PROBE.clear()
    drift._DRIFT_PROBE_COUNTERS["skipped_cache_fresh"] = 0
    drift._DRIFT_PROBE_COUNTERS["ran"] = 0

    spawned: list[str] = []

    def _capture_thread(*args, **kwargs):
        spawned.append(kwargs.get("name") or "")
        return MagicMock()

    monkeypatch.setattr(drift.threading, "Thread", _capture_thread)
    drift.fire_drift_probe(None, ["prof-a"])

    assert spawned == [], "probe ran despite a fresh schemas_cache row"
    assert drift._DRIFT_PROBE_COUNTERS["skipped_cache_fresh"] == 1


def test_drift_probe_runs_when_schemas_cache_stale(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """When the cache is older than the gate window the probe must fire."""
    monkeypatch.delenv("AMX_SKIP_DRIFT_PROBE", raising=False)
    monkeypatch.setenv("AMX_DRIFT_PROBE_MIN_AGE_SEC", "1")
    from amx.search import drift

    store = _make_history_store(tmp_path)
    store.save_schemas_cache(
        db_profile="prof-b",
        database="db1",
        catalog="",
        entries={"public": None},
        bulk_filled=True,
    )
    import amx.storage.sqlite_store as ss

    monkeypatch.setattr(ss, "history_store", lambda: store)

    drift._LAST_PROBE.clear()
    drift._DRIFT_PROBE_COUNTERS["skipped_cache_fresh"] = 0
    drift._DRIFT_PROBE_COUNTERS["ran"] = 0

    # Wait past the 1-second window so the cache is "stale" per the gate.
    time.sleep(1.2)

    spawned: list[str] = []

    def _capture_thread(*args, **kwargs):
        spawned.append(kwargs.get("name") or "")
        m = MagicMock()
        # Capture the worker so we can run it inline and observe the
        # ran counter without spawning a real thread.
        target = kwargs.get("target")
        if callable(target):
            target()
        return m

    monkeypatch.setattr(drift.threading, "Thread", _capture_thread)

    # Stub the probe internals so the worker doesn't actually open
    # a connector — we only care that the gate let it through.
    monkeypatch.setattr(
        drift, "_probe_one", lambda *a, **kw: drift.DriftResult("prof-b", 0, 0, False)
    )
    monkeypatch.setattr(drift, "_enqueue_sync", lambda *a, **kw: None)

    drift.fire_drift_probe(None, ["prof-b"])

    assert spawned, "probe was skipped despite a stale schemas_cache row"
    assert drift._DRIFT_PROBE_COUNTERS["ran"] == 1
    assert drift._DRIFT_PROBE_COUNTERS["skipped_cache_fresh"] == 0


def test_resolve_min_age_seconds_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    from amx.search import drift

    monkeypatch.setenv("AMX_DRIFT_PROBE_MIN_AGE_SEC", "42")
    assert drift._resolve_min_age_seconds() == 42.0

    monkeypatch.setenv("AMX_DRIFT_PROBE_MIN_AGE_SEC", "not-a-number")
    assert drift._resolve_min_age_seconds() == drift._SCHEMAS_CACHE_FRESH_SEC

    monkeypatch.setenv("AMX_DRIFT_PROBE_MIN_AGE_SEC", "-5")
    assert drift._resolve_min_age_seconds() == drift._SCHEMAS_CACHE_FRESH_SEC

    monkeypatch.delenv("AMX_DRIFT_PROBE_MIN_AGE_SEC", raising=False)
    assert drift._resolve_min_age_seconds() == drift._SCHEMAS_CACHE_FRESH_SEC


def test_cache_runtime_counters_aggregates_both_sources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``cache_runtime_counters`` is what ``/db cache stats`` reads."""
    from amx.db import connector as connector_mod
    from amx.search import drift
    from amx.storage.cache_ops import cache_runtime_counters

    connector_mod._LISTING_MEMO_COUNTERS["catalogs"] = {"hit": 3, "miss": 1}
    connector_mod._LISTING_MEMO_COUNTERS["databases"] = {"hit": 0, "miss": 2}
    drift._DRIFT_PROBE_COUNTERS["skipped_cache_fresh"] = 5
    drift._DRIFT_PROBE_COUNTERS["ran"] = 7

    snap = cache_runtime_counters()
    assert snap["listing_memo"]["catalogs"] == {"hit": 3, "miss": 1}
    assert snap["listing_memo"]["databases"] == {"hit": 0, "miss": 2}
    assert snap["drift_probe"] == {"skipped_cache_fresh": 5, "ran": 7}
