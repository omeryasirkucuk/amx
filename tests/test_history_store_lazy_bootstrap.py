"""Pin the lazy-bootstrap contract for the shared history store.

Symptom that motivated this: ``amx`` CLI startup was taking 3-4
seconds because :func:`init_history_store` was synchronously building
the SQLAlchemy engine + running ``CREATE SCHEMA`` and
``MetaData.create_all`` against the team backend on every invocation.
Most ``amx`` invocations never write history (the user types /help,
/db-profiles, etc.), so blocking the welcome banner on that work was
pure user-perceived latency.

After the fix:
- Local SQLite still inits eagerly (it's cheap).
- When ``history_store_enabled`` is True, ``init_history_store``
  returns a :class:`_LazyDualWriteStore` that has NOT yet built the
  shared backend.
- The first method call (``pending_count``, ``log_event``, accessing
  ``shared``, …) bootstraps it once.
- Bootstrap failure transparently falls back to local-only.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from amx.config import AMXConfig
from amx.storage.factory import _LazyDualWriteStore, init_history_store
from amx.storage.sqlite_store import SQLiteHistoryStore


def _local_only_cfg(td: str) -> AMXConfig:
    cfg = AMXConfig()
    cfg.CONFIG_DIR = td
    return cfg


def _shared_mode_cfg_pointing_at_missing_profile(td: str) -> AMXConfig:
    cfg = AMXConfig()
    cfg.CONFIG_DIR = td
    cfg.history_store_enabled = True
    # Profile name that intentionally does NOT exist in db_profiles —
    # _build_shared_store will short-circuit when the lazy build runs.
    cfg.history_store_profile = "definitely-not-a-real-profile"
    return cfg


def test_init_returns_plain_sqlite_when_shared_mode_disabled() -> None:
    with tempfile.TemporaryDirectory() as td:
        cfg = _local_only_cfg(td)
        store = init_history_store(cfg)
        assert isinstance(store, SQLiteHistoryStore)
        # Sanity: the returned store is initialised and queryable.
        assert isinstance(store.db_path, Path)


def test_init_returns_lazy_wrapper_when_shared_mode_enabled() -> None:
    with tempfile.TemporaryDirectory() as td:
        cfg = _shared_mode_cfg_pointing_at_missing_profile(td)
        store = init_history_store(cfg)
        assert isinstance(store, _LazyDualWriteStore)
        # The whole point of this PR: at startup the wrapper has NOT
        # built its shared target yet. If this assertion ever flips,
        # the welcome banner is paying the bootstrap cost again.
        assert store._wrapped is None


def test_lazy_wrapper_builds_target_on_first_method_call() -> None:
    with tempfile.TemporaryDirectory() as td:
        cfg = _shared_mode_cfg_pointing_at_missing_profile(td)
        store = init_history_store(cfg)
        assert store._wrapped is None

        # pending_count() goes through the wrapper; it must trigger a
        # build. With a bogus profile the build falls back to local
        # SQLite, so pending_count() returns 0 (the local store has
        # no outbox table).
        depth = store.pending_count()
        assert depth == 0
        assert store._wrapped is not None
        # Falls back to the local store on bogus-profile bootstrap.
        assert isinstance(store._wrapped, SQLiteHistoryStore)


def test_lazy_wrapper_does_not_rebuild_on_subsequent_calls() -> None:
    with tempfile.TemporaryDirectory() as td:
        cfg = _shared_mode_cfg_pointing_at_missing_profile(td)
        store = init_history_store(cfg)
        store.pending_count()  # triggers first build
        first_target = store._wrapped
        store.pending_count()
        store.pending_count()
        store.flush_pending()
        # Identity preserved — no spurious rebuilds.
        assert store._wrapped is first_target


def test_lazy_wrapper_exposes_db_path_without_bootstrap() -> None:
    """``db_path`` is read by IHistoryStore consumers and by tests; it
    must work without forcing a network round-trip. The wrapper mirrors
    it from the local SQLite store at construction time."""
    with tempfile.TemporaryDirectory() as td:
        cfg = _shared_mode_cfg_pointing_at_missing_profile(td)
        store = init_history_store(cfg)
        assert store._wrapped is None
        path = store.db_path
        assert isinstance(path, Path)
        # Reading db_path must NOT have triggered a build — that was
        # the whole point of mirroring the attribute.
        assert store._wrapped is None


def test_lazy_wrapper_local_property_is_zero_cost() -> None:
    """Reading ``.local`` must not trigger a build either."""
    with tempfile.TemporaryDirectory() as td:
        cfg = _shared_mode_cfg_pointing_at_missing_profile(td)
        store = init_history_store(cfg)
        assert store._wrapped is None
        local = store.local
        assert isinstance(local, SQLiteHistoryStore)
        assert store._wrapped is None


def test_lazy_wrapper_falls_back_to_local_on_bootstrap_failure() -> None:
    """Bootstrap failure (bogus profile) must NOT raise to the caller —
    it falls back to local-only and the wrapper from then on proxies
    every call to the local store. This preserves the historical
    behaviour where a misconfigured shared backend never broke the
    user's session, just disabled team visibility."""
    with tempfile.TemporaryDirectory() as td:
        cfg = _shared_mode_cfg_pointing_at_missing_profile(td)
        store = init_history_store(cfg)
        # First call triggers build → fallback → wrapped == local.
        store.pending_count()
        assert isinstance(store._wrapped, SQLiteHistoryStore)
        # Subsequent IHistoryStore calls go straight through.
        run_id = store.create_run(
            command="test",
            mode="chat",
            db_backend="postgresql",
            db_profile="default",
            llm_provider="unit",
            llm_model="unit-model",
            scope={},
        )
        assert isinstance(run_id, int)
