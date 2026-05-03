"""Collaboration-guard tests.

Cover the two new safety behaviours added on top of the v0.12.0
shared-history feature:

* ``find_prior_runs_by_others`` — the query that powers the pre-run
  "this scope was already analysed by X" warning. Verifies overlap
  detection (any common (schema, table) tuple matches) and the
  hostname-exclusion that hides the user's own prior runs from the
  warning list.
* ``_action_disable`` outbox guard — bouncing the disable when the
  outbox is non-empty so queued shared writes are never silently
  stranded.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine

from amx.storage.shared_schema import build_metadata
from amx.storage.sqlalchemy_store import SQLAlchemyHistoryStore


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


def _seed_run(
    store: SQLAlchemyHistoryStore,
    *,
    db_profile: str,
    scope: dict[str, list[str]],
    hostname: str | None = None,
    created_by: str | None = None,
) -> str:
    # Override the per-row attribution so we can simulate "another user".
    if hostname is not None:
        store._hostname = hostname
    if created_by is not None:
        store._username = created_by
    return store.create_run(
        command="analyze.run",
        mode="auto",
        db_backend="postgresql",
        db_profile=db_profile,
        llm_provider="openai",
        llm_model="gpt-5",
        scope=scope,
    )


def test_find_prior_runs_by_others_overlap(shared: SQLAlchemyHistoryStore) -> None:
    # Seed one run from machine-B against (sales.orders, sales.customers)
    _seed_run(
        shared,
        db_profile="prod_pg",
        scope={"sales": ["orders", "customers"]},
        hostname="machine-B",
        created_by="bob",
    )
    # Reset the store's identity back to machine-A so the query
    # excludes machine-A's own future runs.
    shared._hostname = "machine-A"
    shared._username = "alice"

    prior = shared.find_prior_runs_by_others(
        db_profile="prod_pg",
        scope={"sales": ["orders"]},  # subset of B's scope
        exclude_hostname="machine-A",
    )
    assert len(prior) == 1
    assert prior[0]["created_by"] == "bob"
    assert prior[0]["hostname"] == "machine-B"
    assert ("sales", "orders") in prior[0]["overlap_assets"]


def test_find_prior_runs_excludes_own_hostname(
    shared: SQLAlchemyHistoryStore,
) -> None:
    # Same machine ran once — the query must hide it from the warning.
    _seed_run(
        shared,
        db_profile="prod_pg",
        scope={"hr": ["employees"]},
        hostname="machine-A",
        created_by="alice",
    )
    prior = shared.find_prior_runs_by_others(
        db_profile="prod_pg",
        scope={"hr": ["employees"]},
        exclude_hostname="machine-A",
    )
    assert prior == []


def test_find_prior_runs_no_overlap(shared: SQLAlchemyHistoryStore) -> None:
    _seed_run(
        shared,
        db_profile="prod_pg",
        scope={"finance": ["ledger"]},
        hostname="machine-B",
        created_by="bob",
    )
    shared._hostname = "machine-A"
    prior = shared.find_prior_runs_by_others(
        db_profile="prod_pg",
        scope={"hr": ["employees"]},  # disjoint
        exclude_hostname="machine-A",
    )
    assert prior == []


def test_find_prior_runs_filters_by_profile(shared: SQLAlchemyHistoryStore) -> None:
    """A run on a different profile must not surface in the warning."""
    _seed_run(
        shared,
        db_profile="staging_pg",
        scope={"sales": ["orders"]},
        hostname="machine-B",
        created_by="bob",
    )
    shared._hostname = "machine-A"
    prior = shared.find_prior_runs_by_others(
        db_profile="prod_pg",  # different profile
        scope={"sales": ["orders"]},
        exclude_hostname="machine-A",
    )
    assert prior == []


def test_disable_action_blocks_when_outbox_has_pending() -> None:
    """The Disable action must not silently strand pending shared writes."""
    from amx.cli_support.commands import history_store as hs_module

    store = MagicMock()
    store.shared = MagicMock()
    store.flush_pending = MagicMock(return_value=(0, 0))
    store.pending_count = MagicMock(return_value=3)

    cfg = MagicMock()
    cfg.history_store_enabled = True
    cfg.history_store_profile = "prod_pg"
    cfg.history_store_schema = "AMX"

    log_event = MagicMock()
    # Stub the dual-store resolver to return our mock without touching
    # the global singleton.
    original = hs_module._resolve_history_dual_store
    hs_module._resolve_history_dual_store = lambda: store
    # Stub the second confirm() prompt to deny so the action aborts.
    original_confirm = hs_module.confirm
    hs_module.confirm = MagicMock(return_value=False)
    try:
        hs_module._action_disable(cfg, log_event=log_event)
    finally:
        hs_module._resolve_history_dual_store = original
        hs_module.confirm = original_confirm

    # cfg.transaction must NOT have been entered — disable should have
    # bounced because the outbox warning was declined.
    cfg.transaction.assert_not_called()
