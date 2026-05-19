"""Tests for the /admin CLI namespace commands.

Uses a real SQLite store (via tmp_path) seeded with the shared schema so
every admin function can execute against a real database without network
access.  The CLI commands are called directly via Click's
``CliRunner`` — exactly the same pattern used by
``tests/lineage/test_cli.py`` and ``tests/cli/test_schedule_cli.py``.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner
from sqlalchemy import create_engine

from amx.storage.admin import (
    current_role,
    register_session,
)
from amx.storage.shared_schema import build_metadata
from amx.storage.sqlalchemy_store import SQLAlchemyHistoryStore

SCHEMA = "main"


# ── Fixtures ──────────────────────────────────────────────────────────────────


def _make_store(db_path: Path) -> SQLAlchemyHistoryStore:
    engine = create_engine(f"sqlite:///{db_path}")
    md = build_metadata(schema=SCHEMA)
    md.create_all(engine)
    store = SQLAlchemyHistoryStore.__new__(SQLAlchemyHistoryStore)
    store.engine = engine
    store.schema = SCHEMA
    store._md = md
    store._t_runs = md.tables[f"{SCHEMA}.analysis_runs"]
    store._t_results = md.tables[f"{SCHEMA}.run_results"]
    store._t_events = md.tables[f"{SCHEMA}.app_events"]
    store._t_session = md.tables[f"{SCHEMA}.session_state"]
    store._t_meta = md.tables[f"{SCHEMA}.schema_meta"]
    store._hostname = "test-host"
    store._username = "test-user"
    store._client_version = "0.14.0-test"
    return store


@pytest.fixture()
def shared(tmp_path: Path) -> SQLAlchemyHistoryStore:
    return _make_store(tmp_path / "admin_cli_test.db")


def _seed_admin_and_viewer(shared):
    admin_rec = register_session(
        shared,
        username="alice",
        hostname="box1",
        client_version="0.14.0",
        db_profiles_seen=[],
    )
    viewer_rec = register_session(
        shared,
        username="bob",
        hostname="box2",
        client_version="0.14.0",
        db_profiles_seen=[],
    )
    return admin_rec, viewer_rec


# ── Helper: build a minimal Click group from the admin module ─────────────────


def _build_group(shared):
    """Return a Click group with admin sub-commands wired against ``shared``."""
    import click

    from amx.cli_support.commands.admin import register_admin_commands

    @click.group()
    def main():
        pass

    def noop_pass_config(fn):
        return fn

    def noop_log_event(**kwargs):
        pass

    register_admin_commands(main, pass_config=noop_pass_config, log_event=noop_log_event)
    return main


# ── Tests ─────────────────────────────────────────────────────────────────────


def test_promote_happy_path(shared):
    """Admin can promote a viewer to admin via the CLI command."""
    admin_rec, viewer_rec = _seed_admin_and_viewer(shared)

    main = _build_group(shared)

    with (
        patch("amx.cli_support.commands.admin._get_shared_store", return_value=shared),
        patch(
            "amx.cli_support.commands.admin._current_identity",
            return_value=("alice", "box1"),
        ),
    ):
        runner = CliRunner()
        result = runner.invoke(main, ["admin", "promote", "bob"])

    assert result.exit_code == 0, result.output
    assert current_role(shared, username="bob", hostname="box2") == "admin"


def test_demote_happy_path(shared):
    """Admin can demote another admin to viewer."""
    admin_rec, viewer_rec = _seed_admin_and_viewer(shared)

    # Promote bob to admin first so demoting him doesn't break the invariant.
    from amx.storage.admin import promote_to_admin

    promote_to_admin(shared, actor_user_id=admin_rec.id, target_user_id=viewer_rec.id)

    main = _build_group(shared)

    with (
        patch("amx.cli_support.commands.admin._get_shared_store", return_value=shared),
        patch(
            "amx.cli_support.commands.admin._current_identity",
            return_value=("alice", "box1"),
        ),
    ):
        runner = CliRunner()
        result = runner.invoke(main, ["admin", "demote", "bob"])

    assert result.exit_code == 0, result.output
    assert current_role(shared, username="bob", hostname="box2") == "viewer"


def test_revoke_happy_path(shared):
    """Admin can revoke a viewer."""
    admin_rec, viewer_rec = _seed_admin_and_viewer(shared)

    main = _build_group(shared)

    with (
        patch("amx.cli_support.commands.admin._get_shared_store", return_value=shared),
        patch(
            "amx.cli_support.commands.admin._current_identity",
            return_value=("alice", "box1"),
        ),
    ):
        runner = CliRunner()
        result = runner.invoke(main, ["admin", "revoke", "bob"])

    assert result.exit_code == 0, result.output

    # Verify bob is now revoked.
    from sqlalchemy import select

    t_users = shared._md.tables[f"{SCHEMA}._amx_users"]
    with shared.engine.connect() as conn:
        row = conn.execute(select(t_users).where(t_users.c.username == "bob")).fetchone()
    assert row.revoked_at is not None


def test_non_admin_denial(shared):
    """A viewer invoking a write command gets a permission denied error."""
    _seed_admin_and_viewer(shared)  # alice=admin, bob=viewer

    main = _build_group(shared)

    # Bob (viewer) tries to promote himself.
    with (
        patch("amx.cli_support.commands.admin._get_shared_store", return_value=shared),
        patch(
            "amx.cli_support.commands.admin._current_identity",
            return_value=("bob", "box2"),
        ),
    ):
        runner = CliRunner()
        result = runner.invoke(main, ["admin", "promote", "alice"])

    # The command should not crash (exit_code 0 from Click's perspective) but
    # must print the permission error and NOT change alice's role.
    assert "admin role required" in result.output.lower() or "required" in result.output
    # alice should still be admin, not double-promoted (role unchanged).
    assert current_role(shared, username="alice", hostname="box1") == "admin"


def test_demote_last_admin_invariant_error(shared):
    """Demoting the last admin shows a friendly error; role is unchanged."""
    admin_rec, _viewer = _seed_admin_and_viewer(shared)

    main = _build_group(shared)

    with (
        patch("amx.cli_support.commands.admin._get_shared_store", return_value=shared),
        patch(
            "amx.cli_support.commands.admin._current_identity",
            return_value=("alice", "box1"),
        ),
    ):
        runner = CliRunner()
        result = runner.invoke(main, ["admin", "demote", "alice"])

    # Must mention the invariant constraint without crashing.
    assert "cannot demote" in result.output.lower() or "last" in result.output.lower()
    # alice must still be admin.
    assert current_role(shared, username="alice", hostname="box1") == "admin"
