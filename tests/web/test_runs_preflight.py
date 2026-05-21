"""Reachability pre-flight contract for the bulk-run path.

``POST /api/runs/preflight`` probes every ``(schema, table)`` in the
scope against the live DB via the cheap metadata-only
:meth:`DatabaseConnector.list_column_profiles` and splits them into
``blocked_assets`` (no live columns visible) and ``reachable_assets``.
Studio uses the split to ask the user whether to substitute the catalog
cache before submitting the actual run.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from amx.config import DBConfig
from amx.web.routers import live_db

PROFILE = "test-profile"


@pytest.fixture(autouse=True)
def _wipe_connector_cache() -> None:
    live_db._CONNECTOR_CACHE.clear()
    yield
    live_db._CONNECTOR_CACHE.clear()


@pytest.fixture(autouse=True)
def _register_profile(cfg) -> None:
    cfg.db_profiles[PROFILE] = DBConfig(
        backend="postgresql",
        host="pg.test",
        user="amx",
        database="appdb",
    )


def _post(client, headers, body):
    return client.post("/api/runs/preflight", headers=headers, json=body)


def test_preflight_returns_reachable_for_visible_table(client, auth_headers, monkeypatch) -> None:
    """``list_column_profiles`` returning a non-empty list means the
    table is reachable on the live DB. The preflight surfaces it under
    ``reachable_assets`` and leaves ``blocked_assets`` empty."""
    fake = MagicMock(
        list_column_profiles=MagicMock(return_value=[MagicMock(name="id"), MagicMock(name="ts")]),
    )
    monkeypatch.setattr(live_db, "DatabaseConnector", lambda *_a, **_kw: fake)

    resp = _post(
        client,
        auth_headers,
        {
            "scope": {"sales": ["orders"]},
            "db_profile": PROFILE,
            "database": "appdb",
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["blocked_assets"] == []
    assert body["reachable_assets"] == [{"schema": "sales", "table": "orders"}]


def test_preflight_flags_blocked_when_columns_empty(client, auth_headers, monkeypatch) -> None:
    """``list_column_profiles`` returning ``[]`` is the cheap signal
    for "table doesn't exist in the live DB or can't be reflected".
    The preflight surfaces it as blocked so Studio can ask the user."""
    fake = MagicMock(list_column_profiles=MagicMock(return_value=[]))
    monkeypatch.setattr(live_db, "DatabaseConnector", lambda *_a, **_kw: fake)

    resp = _post(
        client,
        auth_headers,
        {
            "scope": {"sap_s6p": ["adrt"]},
            "db_profile": PROFILE,
            "database": "appdb",
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["reachable_assets"] == []
    assert body["blocked_assets"] == [
        {"schema": "sap_s6p", "table": "adrt", "reason": "not_in_live_db"}
    ]


def test_preflight_splits_mixed_scope(client, auth_headers, monkeypatch) -> None:
    """Tables that probe-OK end up reachable; tables that probe to ``[]``
    end up blocked. Same scope, single response, no extra round-trips."""

    def fake_probe(schema: str, table: str):
        # Only ``sales.orders`` returns columns; everything else is blocked.
        if schema == "sales" and table == "orders":
            return [MagicMock(name="id")]
        return []

    fake = MagicMock(list_column_profiles=MagicMock(side_effect=fake_probe))
    monkeypatch.setattr(live_db, "DatabaseConnector", lambda *_a, **_kw: fake)

    resp = _post(
        client,
        auth_headers,
        {
            "scope": {
                "sales": ["orders", "customers"],
                "sap_s6p": ["adrt"],
            },
            "db_profile": PROFILE,
            "database": "appdb",
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["reachable_assets"] == [{"schema": "sales", "table": "orders"}]
    blocked_keys = {(b["schema"], b["table"]) for b in body["blocked_assets"]}
    assert blocked_keys == {("sales", "customers"), ("sap_s6p", "adrt")}
    assert all(b["reason"] == "not_in_live_db" for b in body["blocked_assets"])


def test_preflight_requires_db_profile(cfg, client, auth_headers) -> None:
    """Studio always sends the profile (the URL encodes it); CLI direct
    callers without an explicit profile and no active default get a 400
    instead of a silent fallback that could probe the wrong server."""
    # Suppress the active-profile fallback so the empty body actually
    # reaches the "no profile" guard rather than resolving to a default
    # profile name that doesn't exist in ``cfg.db_profiles``.
    cfg.active_db_profile = ""
    resp = _post(
        client,
        auth_headers,
        {
            "scope": {"sales": ["orders"]},
            "db_profile": "",
        },
    )
    assert resp.status_code == 400
    assert "db_profile" in resp.json()["detail"].lower()


def test_preflight_swallows_probe_exception_as_blocked(client, auth_headers, monkeypatch) -> None:
    """A connector-side exception during probing is treated as "can't
    reach this table" — we report it as blocked rather than 500ing the
    whole preflight."""
    fake = MagicMock(list_column_profiles=MagicMock(side_effect=RuntimeError("oops")))
    monkeypatch.setattr(live_db, "DatabaseConnector", lambda *_a, **_kw: fake)

    resp = _post(
        client,
        auth_headers,
        {
            "scope": {"sales": ["orders"]},
            "db_profile": PROFILE,
            "database": "appdb",
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["blocked_assets"] == [
        {"schema": "sales", "table": "orders", "reason": "not_in_live_db"}
    ]
