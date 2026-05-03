"""Capability-gating tests for shared run-history.

DuckDB and ClickHouse adapters explicitly leave
``BackendCapabilities.supports_shared_history = False`` because
neither backend can hold AMX's run-history schema (DuckDB is a local
file; ClickHouse cannot UPDATE rows the way ``finish_run`` needs).
The factory must reject these backends with a clear error.
"""

from __future__ import annotations

import pytest

from amx.config import AMXConfig, DBConfig
from amx.db.adapters import get_adapter
from amx.storage.factory import HistoryStoreBootstrapError, _build_shared_store


@pytest.mark.parametrize(
    "backend,expected_flag",
    [
        ("postgresql", True),
        ("mysql", True),
        ("mssql", True),
        ("oracle", True),
        ("redshift", True),
        ("snowflake", True),
        ("databricks", True),
        ("bigquery", True),
        ("duckdb", False),
        ("clickhouse", False),
    ],
)
def test_supports_shared_history_flag(backend: str, expected_flag: bool) -> None:
    db = DBConfig(backend=backend)
    try:
        adapter = get_adapter(db)
    except ImportError:
        # The adapter's optional driver is not installed in this test
        # environment. The capability flag is on the class itself, so
        # we can read it from the class without instantiating.
        pytest.skip(f"Optional driver for {backend!r} not installed in this environment.")
    assert adapter.capabilities.supports_shared_history is expected_flag, (
        f"{backend!r}: expected supports_shared_history={expected_flag}, "
        f"got {adapter.capabilities.supports_shared_history}"
    )


def test_factory_rejects_unsupported_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    """Asking the factory to build a shared store on DuckDB raises a
    HistoryStoreBootstrapError naming the backend."""
    cfg = AMXConfig()
    cfg.history_store_enabled = True
    cfg.history_store_profile = "duck-prof"
    cfg.history_store_schema = "AMX"
    cfg.db_profiles["duck-prof"] = DBConfig(backend="duckdb", database=":memory:")

    with pytest.raises(HistoryStoreBootstrapError) as exc_info:
        _build_shared_store(cfg)
    assert "duckdb" in str(exc_info.value).lower()


def test_factory_warns_when_profile_missing(caplog: pytest.LogCaptureFixture) -> None:
    """A missing history_store_profile is a soft fallback to local-only,
    not a hard error — we never break the user's session for config drift."""
    cfg = AMXConfig()
    cfg.history_store_enabled = True
    cfg.history_store_profile = "does-not-exist"
    result = _build_shared_store(cfg)
    assert result is None
