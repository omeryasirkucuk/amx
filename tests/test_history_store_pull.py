"""Pull-from-shared tests.

Cover the v0.12.x reverse migration that surfaces a teammate's runs in
the local user's ``/history list`` after they connect to a shared
store another team member already populated.

Two sets of behaviour:

1. ``count_runs_by_other_hosts`` — the query that powers the
   "this shared store already has runs from <X>" detection prompt
   triggered by ``/history-store enable`` after bootstrap. Verifies
   per-host bucketing, hostname-exclusion, and an empty result when
   only the current machine has runs.
2. ``pull_shared_to_local`` — the reverse migration that ferries
   teammates' runs DOWN into local SQLite. Verifies idempotency
   (re-pull copies zero), FK rewiring (run_results land under the
   correct local INT id), and attribution preservation (created_by /
   hostname / shared_uuid populated locally).
"""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import create_engine

from amx.storage.migration import pull_shared_to_local
from amx.storage.shared_schema import build_metadata
from amx.storage.sqlalchemy_store import SQLAlchemyHistoryStore
from amx.storage.sqlite_store import SQLiteHistoryStore


def _make_shared(tmp_path: Path, *, hostname: str = "test-host") -> SQLAlchemyHistoryStore:
    db_path = tmp_path / f"shared-{hostname}.db"
    engine = create_engine(f"sqlite:///{db_path}")
    md = build_metadata(schema="main")
    md.create_all(engine)
    s = SQLAlchemyHistoryStore.__new__(SQLAlchemyHistoryStore)
    s.engine = engine
    s.schema = "main"
    s._md = md
    s._t_runs = md.tables["main.analysis_runs"]
    s._t_results = md.tables["main.run_results"]
    s._t_events = md.tables["main.app_events"]
    s._t_session = md.tables["main.session_state"]
    s._t_meta = md.tables["main.schema_meta"]
    s._hostname = hostname
    s._username = f"user-{hostname}"
    s._client_version = "0.12.0-test"
    return s


@pytest.fixture
def shared(tmp_path: Path) -> SQLAlchemyHistoryStore:
    return _make_shared(tmp_path, hostname="machine-A")


@pytest.fixture
def local(tmp_path: Path) -> SQLiteHistoryStore:
    s = SQLiteHistoryStore(tmp_path / "local.db")
    s.init()
    return s


def _seed(
    store: SQLAlchemyHistoryStore,
    *,
    hostname: str,
    user: str,
    scope: dict[str, list[str]],
    db_profile: str = "prod_pg",
) -> str:
    store._hostname = hostname
    store._username = user
    rid = store.create_run(
        command="analyze.run",
        mode="auto",
        db_backend="postgresql",
        db_profile=db_profile,
        llm_provider="openai",
        llm_model="gpt-5",
        scope=scope,
    )
    store.save_run_results(
        rid,
        [
            {
                "schema": list(scope.keys())[0],
                "table": list(scope.values())[0][0],
                "column": "id",
                "asset_kind": "column",
                "source": "llm",
                "confidence": "high",
                "alternatives": ["row id"],
            }
        ],
    )
    store.finish_run(rid, status="success", metrics={}, tokens={}, results={"applied": 0})
    return rid


def test_count_runs_by_other_hosts_buckets_per_host(
    shared: SQLAlchemyHistoryStore,
) -> None:
    _seed(shared, hostname="machine-B", user="bob", scope={"sales": ["orders"]})
    _seed(shared, hostname="machine-B", user="bob", scope={"sales": ["customers"]})
    _seed(shared, hostname="machine-C", user="carol", scope={"hr": ["employees"]})
    summary = shared.count_runs_by_other_hosts(exclude_hostname="machine-A")
    assert set(summary.keys()) == {"machine-B", "machine-C"}
    assert summary["machine-B"]["count"] == 2
    assert "bob" in summary["machine-B"]["users"]
    assert summary["machine-C"]["count"] == 1
    assert summary["machine-C"]["users"] == ["carol"]


def test_count_runs_by_other_hosts_excludes_self(
    shared: SQLAlchemyHistoryStore,
) -> None:
    _seed(shared, hostname="machine-A", user="alice", scope={"x": ["y"]})
    summary = shared.count_runs_by_other_hosts(exclude_hostname="machine-A")
    assert summary == {}


def test_pull_copies_other_hosts_runs(
    shared: SQLAlchemyHistoryStore, local: SQLiteHistoryStore
) -> None:
    _seed(shared, hostname="machine-B", user="bob", scope={"sales": ["orders"]})
    _seed(shared, hostname="machine-C", user="carol", scope={"hr": ["employees"]})
    # The pull uses the shared store's ``_hostname`` to filter, so set
    # it to this machine (machine-A) — the seeded runs were on B and C.
    shared._hostname = "machine-A"
    stats = pull_shared_to_local(local=local, shared=shared)
    assert stats["analysis_runs"] == 2
    assert stats["run_results"] == 2

    # Local rows now exist with their attribution preserved.
    rows = local.list_recent_runs(limit=10)
    creators = {r.get("created_by") for r in rows}
    assert {"bob", "carol"}.issubset(creators)
    hosts = {r.get("hostname") for r in rows}
    assert {"machine-B", "machine-C"}.issubset(hosts)


def test_pull_is_idempotent(shared: SQLAlchemyHistoryStore, local: SQLiteHistoryStore) -> None:
    _seed(shared, hostname="machine-B", user="bob", scope={"sales": ["orders"]})
    shared._hostname = "machine-A"
    first = pull_shared_to_local(local=local, shared=shared)
    second = pull_shared_to_local(local=local, shared=shared)
    assert first["analysis_runs"] == 1
    assert first["run_results"] == 1
    assert second["analysis_runs"] == 0  # nothing new to copy
    assert second["run_results"] == 0


def test_pull_skips_own_hostname(shared: SQLAlchemyHistoryStore, local: SQLiteHistoryStore) -> None:
    """A run we wrote ourselves on this machine must not be pulled
    back down — local already has the canonical row."""
    _seed(shared, hostname="machine-A", user="alice", scope={"x": ["y"]})
    shared._hostname = "machine-A"
    stats = pull_shared_to_local(local=local, shared=shared)
    assert stats["analysis_runs"] == 0


def test_pull_preserves_fk_link(shared: SQLAlchemyHistoryStore, local: SQLiteHistoryStore) -> None:
    """run_results pulled down must reference the parent run's NEW
    local INT id (not the shared UUID)."""
    _seed(shared, hostname="machine-B", user="bob", scope={"sales": ["orders"]})
    shared._hostname = "machine-A"
    pull_shared_to_local(local=local, shared=shared)

    runs = local.list_recent_runs(limit=10)
    assert len(runs) == 1
    parent_local_id = int(runs[0]["id"])  # local INT id

    results = local.get_run_results(parent_local_id)
    assert len(results) == 1
    # FK is by INT id locally, not by shared UUID
    assert results[0]["run_id"] == parent_local_id
    assert results[0]["shared_uuid"] is not None
