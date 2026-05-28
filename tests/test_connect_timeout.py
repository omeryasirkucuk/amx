"""Connectivity probes are bounded by ``DB_CONNECT_TIMEOUT_SEC``.

An unreachable host (off-VPN, wrong host/port, firewalled SYN drop) used
to hang for the OS TCP default (~75-130s), doubled by the retry layer,
with Ctrl-C unable to break the native driver connect. The probe now runs
in a daemon thread bounded by the timeout, so ``test_connection_result``
returns promptly with an actionable host/network message — uniformly
across every backend, not just Databricks.
"""

from __future__ import annotations

import time

import pytest

import amx.db.connector as connector_mod
from amx.config import DBConfig
from amx.db.connector import DatabaseConnector


def _duckdb_connector() -> DatabaseConnector:
    # DuckDB is the only driver shipped by default, so it is always
    # importable in CI; we monkeypatch the adapter probe to simulate the
    # network behaviours we cannot reproduce against a real remote host.
    return DatabaseConnector(DBConfig(backend="duckdb", database=":memory:"))


def test_probe_times_out_and_returns_actionable_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(connector_mod, "DB_CONNECT_TIMEOUT_SEC", 0.2)
    conn = _duckdb_connector()

    def _hang(engine: object = None) -> None:
        time.sleep(5.0)  # simulate an unreachable host

    monkeypatch.setattr(conn._adapter, "test_connection", _hang)

    started = time.monotonic()
    result = conn.test_connection_result()
    elapsed = time.monotonic() - started

    assert result.ok is False
    assert "timed out" in (result.message or "").lower()
    # Must return at ~the timeout, NOT wait out the full 5s hang.
    assert elapsed < 2.0


def test_probe_success_passes_through(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(connector_mod, "DB_CONNECT_TIMEOUT_SEC", 5.0)
    conn = _duckdb_connector()
    monkeypatch.setattr(conn._adapter, "test_connection", lambda engine=None: None)
    assert conn.test_connection_result().ok is True


def test_probe_reraises_real_connect_error(monkeypatch: pytest.MonkeyPatch) -> None:
    # A genuine (non-timeout) connect error still flows through the
    # existing actionable-message classification, not the timeout branch.
    monkeypatch.setattr(connector_mod, "DB_CONNECT_TIMEOUT_SEC", 5.0)
    conn = _duckdb_connector()

    def _boom(engine: object = None) -> None:
        raise RuntimeError("password authentication failed")

    monkeypatch.setattr(conn._adapter, "test_connection", _boom)
    result = conn.test_connection_result()
    assert result.ok is False
    assert "timed out" not in (result.message or "").lower()
