"""Thread-safety of the Studio live-DB connector cache.

FastAPI runs sync route handlers in a threadpool, so ``_connector_for_scope``,
``evict_connector_cache`` and ``_evict_oldest`` can hit the module-level
``_CONNECTOR_CACHE`` from several threads at once. These tests exercise that
the cache stays correct under contention: concurrent builds of the same key
dedupe to a single connector (the losers are closed), and a mix of inserts and
evictions never leaks past the cap or raises.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass

import pytest

from amx.config import AMXConfig, DBConfig
from amx.web.routers import live_db


@dataclass
class _FakeConnector:
    db_cfg: object
    profile_name: str | None = None
    closed: bool = False

    def close(self) -> None:
        self.closed = True


@pytest.fixture(autouse=True)
def _clear_cache():
    live_db._CONNECTOR_CACHE.clear()
    yield
    live_db._CONNECTOR_CACHE.clear()


def _cfg() -> AMXConfig:
    cfg = AMXConfig()
    cfg.upsert_db_profile("prod", DBConfig(backend="duckdb", database="main"))
    return cfg


def test_concurrent_same_key_dedupes_to_one_connector(monkeypatch) -> None:
    cfg = _cfg()
    parties = 8
    barrier = threading.Barrier(parties)
    created: list[_FakeConnector] = []
    created_lock = threading.Lock()

    def _factory(db_cfg, profile_name=None):
        # Release all builders together so every thread races on one key.
        barrier.wait()
        conn = _FakeConnector(db_cfg, profile_name)
        with created_lock:
            created.append(conn)
        return conn

    monkeypatch.setattr(live_db, "DatabaseConnector", _factory)

    results: list[_FakeConnector] = []
    results_lock = threading.Lock()

    def _worker():
        conn = live_db._connector_for_scope(cfg, "prod", database="main")
        with results_lock:
            results.append(conn)

    threads = [threading.Thread(target=_worker) for _ in range(parties)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Every caller gets the same single cached connector.
    assert len({id(c) for c in results}) == 1
    cached = list(live_db._CONNECTOR_CACHE.values())
    assert len(cached) == 1
    winner = cached[0]
    assert not winner.closed
    # The duplicates that lost the race were built then closed.
    losers = [c for c in created if c is not winner]
    assert losers, "expected the race to build more than one connector"
    assert all(c.closed for c in losers)


def test_concurrent_inserts_and_evictions_stay_bounded(monkeypatch) -> None:
    cfg = _cfg()
    monkeypatch.setattr(
        live_db,
        "DatabaseConnector",
        lambda db_cfg, profile_name=None: _FakeConnector(db_cfg, profile_name),
    )
    errors: list[BaseException] = []

    def _insert(worker: int):
        try:
            for j in range(40):
                live_db._connector_for_scope(cfg, "prod", database=f"db{(worker * 40 + j) % 100}")
        except BaseException as exc:  # noqa: BLE001 - captured for the assertion
            errors.append(exc)

    def _evict():
        try:
            for _ in range(40):
                live_db.evict_connector_cache("prod")
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=_insert, args=(i,)) for i in range(8)]
    threads += [threading.Thread(target=_evict) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, f"cache raised under concurrency: {errors[:3]}"
    assert len(live_db._CONNECTOR_CACHE) <= live_db._CONNECTOR_CACHE_MAX
