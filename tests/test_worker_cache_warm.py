"""Pre-generation comment warming in ``cache_refresh_executor``.

A RUN is a deliberate user action, so the generation agents must see
the descriptions that already exist in the DB — generating without
them yields wrong or duplicate results. The warm pass therefore pulls
the existing table + column comments through the bulk metadata path
(which bypasses the cache-only read gate) and stamps the durable TTL
so the gated reads the agent context relies on return real values.
"""

from __future__ import annotations

import json

import pytest

import amx.config as config_mod
import amx.db.connector as connector_mod
import amx.runtime.worker as worker
from amx.db.connector import DURABLE_COMMENT_CACHE_TTL_SECONDS


class _FakeConnector:
    def __init__(self, *a, **k) -> None:
        self.bulk_warm_calls: list[tuple[str, float | None]] = []

    def invalidate_column_comments_cache(self, **_k) -> None:
        pass

    def _populate_schema_metadata_cache(self, schema, *, ttl_seconds=None) -> bool:
        self.bulk_warm_calls.append((schema, ttl_seconds))
        return True


class _FakeCfg:
    db_profiles = {"prof": object()}


def _patch(monkeypatch: pytest.MonkeyPatch) -> list[_FakeConnector]:
    built: list[_FakeConnector] = []

    def _make(*_a, **_k) -> _FakeConnector:
        inst = _FakeConnector()
        built.append(inst)
        return inst

    monkeypatch.setattr(connector_mod, "DatabaseConnector", _make)
    monkeypatch.setattr(config_mod.AMXConfig, "load", staticmethod(lambda *a, **k: _FakeCfg()))
    return built


def test_run_warm_uses_bulk_path_with_durable_ttl(monkeypatch: pytest.MonkeyPatch) -> None:
    built = _patch(monkeypatch)
    payload = {
        "id": 1,
        "db_profile": "prof",
        "scope_json": json.dumps(
            {"mode": "tables", "tables": [{"schema": "sap_s6p", "table": "cskt"}]}
        ),
    }

    worker.cache_refresh_executor(1, payload)

    assert built, "connector was never built"
    calls = built[0].bulk_warm_calls
    # The target schema is warmed through the gate-bypassing bulk path
    # with the durable TTL so the agent sees existing comments.
    assert ("sap_s6p", DURABLE_COMMENT_CACHE_TTL_SECONDS) in calls


def test_run_warm_columns_mode_collapses_to_table(monkeypatch: pytest.MonkeyPatch) -> None:
    built = _patch(monkeypatch)
    payload = {
        "id": 2,
        "db_profile": "prof",
        "scope_json": json.dumps(
            {
                "mode": "columns",
                "columns": [
                    {"schema": "sap_s6p", "table": "cskt", "column": "mandt"},
                    {"schema": "sap_s6p", "table": "cskt", "column": "spras"},
                ],
            }
        ),
    }

    worker.cache_refresh_executor(2, payload)

    calls = built[0].bulk_warm_calls
    # Two column picks on the same table collapse to a single schema warm.
    assert calls.count(("sap_s6p", DURABLE_COMMENT_CACHE_TTL_SECONDS)) == 1
