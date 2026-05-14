"""Regression: DualWriteHistoryStore forwards local-only methods to
the inner SQLite store.

Production deploys can run in shared-history mode where
``history_store()`` returns a :class:`DualWriteHistoryStore` instead
of the bare :class:`SQLiteHistoryStore` used by every unit test
fixture. The wrapper hand-forwards every dual-write method but
historically did NOT forward the local-only rerun snapshot lifecycle
+ context-cache helpers — so every Re-Run / Variations submit failed
with ``AttributeError: 'DualWriteHistoryStore' object has no
attribute 'lookup_run_context_cache'`` at the first call from
:func:`build_context_snapshot`. The new ``__getattr__`` fallback
proxies undefined attributes to the local store.

Pin every local-only method the Re-Run / Variations executors
depend on so this regression cannot silently re-introduce itself.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from amx.storage.dual_write import DualWriteHistoryStore
from amx.storage.sqlite_store import SQLiteHistoryStore


@pytest.fixture
def dual_store(tmp_path: Path) -> DualWriteHistoryStore:
    local = SQLiteHistoryStore(tmp_path / "local.db")
    local.init()
    # The shared half is mocked — we're testing the wrapper's
    # local-fallback, not the shared write path.
    shared = MagicMock()
    return DualWriteHistoryStore(local=local, shared=shared)


class TestDualWriteFallbackForwarding:
    """Every method the Re-Run / Variations executors touch on the
    history store must be reachable through the dual-write wrapper."""

    # Names sourced from a code search for ``hs.<method>(...)`` /
    # ``history_store().<method>(...)`` inside ``rerun.py``,
    # ``variations.py``, and ``rerun_context.py``.
    LOCAL_ONLY_METHODS = (
        "lookup_run_context_cache",
        "save_rerun_snapshot",
        "read_rerun_snapshot",
        "delete_rerun_snapshots_for_job",
        "next_rerun_seq",
        "get_result_chain",
        "get_descendant_runs",
    )

    @pytest.mark.parametrize("method_name", LOCAL_ONLY_METHODS)
    def test_method_resolves_via_fallback(
        self, dual_store: DualWriteHistoryStore, method_name: str
    ) -> None:
        """Each method must be reachable on the wrapper without
        raising AttributeError. We don't care what it returns here —
        only that the attribute is bound to the local store's bound
        method."""
        bound = getattr(dual_store, method_name)
        assert callable(bound), (
            f"{method_name!r} must be callable on the dual-write wrapper. "
            "If a method was added to SQLiteHistoryStore and "
            "Re-Run/Variations started using it, the __getattr__ "
            "fallback should pick it up automatically."
        )
        # The fallback delegates to the local instance's bound method.
        assert bound == getattr(dual_store.local, method_name)

    def test_dunder_attrs_still_raise_attribute_error(
        self, dual_store: DualWriteHistoryStore
    ) -> None:
        """The fallback intentionally excludes underscored names so
        pickling / copying / framework introspection still get a
        clean AttributeError instead of silently proxying to the
        local store (which would mask real bugs)."""
        with pytest.raises(AttributeError):
            _ = dual_store.__nonexistent_dunder__

    def test_unknown_method_raises_normal_attribute_error(
        self, dual_store: DualWriteHistoryStore
    ) -> None:
        """A truly unknown method must still raise AttributeError so
        typos surface fast — the fallback only saves us when the
        method exists on the local store."""
        with pytest.raises(AttributeError):
            _ = dual_store.this_method_does_not_exist_anywhere

    def test_lookup_run_context_cache_actually_works(
        self, dual_store: DualWriteHistoryStore
    ) -> None:
        """End-to-end smoke: a real call lands on the local store and
        returns a real result (or None when there's no cache hit)."""
        # No cache row → returns None, not AttributeError.
        result = dual_store.lookup_run_context_cache(
            db_profile="x", database="y", schema="z", table="t"
        )
        assert result is None or isinstance(result, dict)
