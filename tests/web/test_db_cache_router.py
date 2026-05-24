"""HTTP coverage for the /api/db/cache router.

The router is a thin pass-through over :mod:`amx.storage.cache_ops`
(unit-tested separately in ``test_db_cache_ops.py``). Here we pin the
HTTP-layer contract: param wiring, JSON shape, and the global-flush
safety gate that requires ``force=true`` when neither profile nor
database is set.
"""

from __future__ import annotations

import pytest

from amx.web.routers import db_cache as db_cache_router


@pytest.fixture()
def stub_ops(monkeypatch):
    """Replace the storage helpers with deterministic stand-ins so the
    router test exercises wiring, not SQLite state."""
    from dataclasses import dataclass

    @dataclass
    class _Row:
        profile: str
        database: str
        schemas_rows: int
        columns_rows: int
        catalog_rows: int
        last_fetch: float | None

    @dataclass
    class _Stat:
        table: str
        total_rows: int
        distinct_profiles: int
        distinct_databases: int
        oldest_fetch: float | None
        newest_fetch: float | None
        expired_rows: int
        ttl_aware: bool

    @dataclass
    class _Report:
        deleted: dict
        types: list
        scope: dict

        @property
        def total(self) -> int:
            return sum(self.deleted.values())

    state: dict = {
        "inventory_args": None,
        "stats_called": False,
        "clear_args": None,
    }

    def fake_inventory(*, profile=None, database=None):
        state["inventory_args"] = (profile, database)
        return [_Row("prof-a", "db1", 2, 1, 4, 1700000000.0)]

    def fake_stats(*, valid_profiles=None):
        # ``valid_profiles`` is the configured profile set the
        # /api/db/cache/stats endpoint forwards from cfg.db_profiles so
        # tombstones (rows for deleted profiles) never inflate the
        # headline counts. Tests don't exercise the filter — they just
        # accept the kwarg so the router can pass it through.
        state["stats_called"] = True
        state["stats_valid_profiles"] = valid_profiles
        return {
            "schemas": _Stat("schemas_cache", 4, 1, 2, 1.0, 2.0, 0, True),
        }

    def fake_clear(*, profile=None, database=None, types=None):
        state["clear_args"] = (profile, database, list(types or []))
        return _Report(
            deleted={"schemas": 2, "columns": 1, "catalog": 4},
            types=types or ["schemas", "columns", "catalog"],
            scope={"profile": profile, "database": database},
        )

    monkeypatch.setattr(db_cache_router, "cache_inventory", fake_inventory)
    monkeypatch.setattr(db_cache_router, "cache_stats", fake_stats)
    monkeypatch.setattr(db_cache_router, "cache_clear", fake_clear)
    return state


def test_show_threads_profile_and_database(client, auth_headers, stub_ops) -> None:
    response = client.get(
        "/api/db/cache/show?profile=prof-a&database=db1",
        headers=auth_headers,
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["rows"][0] == {
        "profile": "prof-a",
        "database": "db1",
        "schemas_rows": 2,
        "columns_rows": 1,
        "catalog_rows": 4,
        "last_fetch": 1700000000.0,
    }
    assert stub_ops["inventory_args"] == ("prof-a", "db1")


def test_stats_returns_per_cache_payload(client, auth_headers, stub_ops) -> None:
    response = client.get("/api/db/cache/stats", headers=auth_headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["schemas"]["table"] == "schemas_cache"
    assert payload["schemas"]["ttl_aware"] is True
    assert stub_ops["stats_called"] is True


def test_clear_scoped_to_profile(client, auth_headers, stub_ops) -> None:
    response = client.post(
        "/api/db/cache/clear",
        headers=auth_headers,
        json={"profile": "prof-a"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 7
    assert stub_ops["clear_args"][0] == "prof-a"


def test_clear_global_without_force_rejected(client, auth_headers, stub_ops) -> None:
    response = client.post(
        "/api/db/cache/clear",
        headers=auth_headers,
        json={},
    )
    assert response.status_code == 400
    assert "force=true" in response.json()["detail"]
    # Must not have reached cache_clear.
    assert stub_ops["clear_args"] is None


def test_clear_global_with_force_allowed(client, auth_headers, stub_ops) -> None:
    response = client.post(
        "/api/db/cache/clear",
        headers=auth_headers,
        json={"force": True},
    )
    assert response.status_code == 200
    assert stub_ops["clear_args"] == (None, None, [])


def test_clear_unknown_type_returns_400(client, auth_headers, monkeypatch) -> None:
    # No stub_ops here — we want the real cache_clear to raise the
    # ValueError that the router converts to a 400.
    def bad_clear(*, profile=None, database=None, types=None):
        raise ValueError("Unknown cache types: ['foo']")

    monkeypatch.setattr(db_cache_router, "cache_clear", bad_clear)
    response = client.post(
        "/api/db/cache/clear",
        headers=auth_headers,
        json={"profile": "prof-a", "types": ["foo"]},
    )
    assert response.status_code == 400
    assert "Unknown cache types" in response.json()["detail"]
