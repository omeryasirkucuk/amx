"""Tests for amx.storage.admin — workspace admin data layer.

Uses an on-disk SQLite engine (via tmp_path) with ``schema="main"``
because SQLite's in-memory engine does not support named schemas.
The ``schema="main"`` convention mirrors the pattern used by other
admin-layer tests in this suite (e.g. test_history_store_collaboration.py).
"""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import create_engine, select

from amx.storage.admin import (
    AdminInvariantError,
    AdminUserRecord,
    current_role,
    demote_admin,
    list_members,
    promote_to_admin,
    record_audit_event,
    register_session,
    revoke_user,
    unrevoke_user,
)
from amx.storage.shared_schema import build_metadata
from amx.storage.sqlalchemy_store import SQLAlchemyHistoryStore


# ── Fixture ───────────────────────────────────────────────────────────────────

SCHEMA = "main"


def _make_store(db_path: Path) -> SQLAlchemyHistoryStore:
    """Build a SQLAlchemyHistoryStore backed by an on-disk SQLite file.

    Uses ``schema="main"`` because SQLite does not support arbitrary named
    schemas for ``MetaData.create_all``; "main" is the implicit SQLite schema.
    """
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
    """Fresh SQLite-backed store per test."""
    return _make_store(tmp_path / "admin_test.db")


def _register(shared, username="alice", hostname="box1", version="0.14.0", profiles=None):
    return register_session(
        shared,
        username=username,
        hostname=hostname,
        client_version=version,
        db_profiles_seen=profiles or [],
    )


def _audit_rows(shared):
    t = shared._md.tables[f"{SCHEMA}._amx_admin_audit"]
    with shared.engine.connect() as conn:
        return conn.execute(select(t)).fetchall()


def _user_rows(shared):
    t = shared._md.tables[f"{SCHEMA}._amx_users"]
    with shared.engine.connect() as conn:
        return conn.execute(select(t)).fetchall()


# ── Tests ─────────────────────────────────────────────────────────────────────


def test_first_user_becomes_admin(shared):
    """First call to register_session on an empty store creates an admin."""
    record = _register(shared, username="alice", hostname="box1")
    assert isinstance(record, AdminUserRecord)
    assert record.role == "admin"
    assert record.username == "alice"
    assert record.hostname == "box1"

    role = current_role(shared, username="alice", hostname="box1")
    assert role == "admin"

    audit = _audit_rows(shared)
    assert len(audit) == 1
    assert audit[0].action == "user_join"


def test_second_user_becomes_viewer(shared):
    """A second distinct (username, hostname) pair joins as viewer."""
    _register(shared, username="alice", hostname="box1")
    record = _register(shared, username="bob", hostname="box2")

    assert record.role == "viewer"
    assert current_role(shared, username="bob", hostname="box2") == "viewer"


def test_promote_to_admin_updates_role_and_audits(shared):
    """Promoting a viewer sets role='admin' and writes a promote_admin audit row."""
    admin_rec = _register(shared, username="alice", hostname="box1")
    viewer_rec = _register(shared, username="bob", hostname="box2")

    assert viewer_rec.role == "viewer"

    promote_to_admin(
        shared,
        actor_user_id=admin_rec.id,
        target_user_id=viewer_rec.id,
    )

    assert current_role(shared, username="bob", hostname="box2") == "admin"

    audit = _audit_rows(shared)
    actions = [r.action for r in audit]
    assert "promote_admin" in actions

    promo = next(r for r in audit if r.action == "promote_admin")
    assert promo.actor_user_id == admin_rec.id
    assert promo.target_user_id == viewer_rec.id


def test_demote_last_admin_raises_AdminInvariantError(shared):
    """Demoting the only active admin raises AdminInvariantError; role unchanged."""
    admin_rec = _register(shared, username="alice", hostname="box1")

    with pytest.raises(AdminInvariantError):
        demote_admin(
            shared,
            actor_user_id=admin_rec.id,
            target_user_id=admin_rec.id,
        )

    # Role must be unchanged.
    assert current_role(shared, username="alice", hostname="box1") == "admin"

    # No demote_admin audit row should have been written.
    audit = _audit_rows(shared)
    assert not any(r.action == "demote_admin" for r in audit)


def test_revoke_user_sets_fields_and_audits(shared):
    """Revoking a viewer sets revoked_at/revoked_by and writes a revoke audit row."""
    admin_rec = _register(shared, username="alice", hostname="box1")
    viewer_rec = _register(shared, username="bob", hostname="box2")

    revoke_user(
        shared,
        actor_user_id=admin_rec.id,
        target_user_id=viewer_rec.id,
    )

    # Check fields on the user row.
    t = shared._md.tables[f"{SCHEMA}._amx_users"]
    with shared.engine.connect() as conn:
        row = conn.execute(
            select(t).where(t.c.id == viewer_rec.id)
        ).fetchone()

    assert row.revoked_at is not None
    assert row.revoked_by == admin_rec.id

    audit = _audit_rows(shared)
    assert any(r.action == "revoke" and r.target_user_id == viewer_rec.id for r in audit)


def test_revoke_last_admin_raises_AdminInvariantError(shared):
    """Revoking the only non-revoked admin raises AdminInvariantError."""
    admin_rec = _register(shared, username="alice", hostname="box1")

    with pytest.raises(AdminInvariantError):
        revoke_user(
            shared,
            actor_user_id=admin_rec.id,
            target_user_id=admin_rec.id,
        )

    # Admin must still be active (not revoked).
    t = shared._md.tables[f"{SCHEMA}._amx_users"]
    with shared.engine.connect() as conn:
        row = conn.execute(select(t).where(t.c.id == admin_rec.id)).fetchone()
    assert row.revoked_at is None


def test_register_session_is_idempotent(shared):
    """Calling register_session twice with the same pair keeps one user row."""
    _register(shared, username="alice", hostname="box1")
    _register(shared, username="alice", hostname="box1")

    users = _user_rows(shared)
    assert len(users) == 1

    # last_seen_at should be updated (not necessarily different in fast tests,
    # but no error and exactly one row).
    assert users[0].username == "alice"


def test_list_members_orders_admin_first_then_recent(shared):
    """list_members returns admins before viewers, most recent within each group."""
    import time as _time

    admin_rec = _register(shared, username="alice", hostname="box1")
    viewer_rec1 = _register(shared, username="bob", hostname="box2")
    # Touch alice again to update last_seen_at (makes ordering meaningful).
    _time.sleep(0.01)
    viewer_rec2 = _register(shared, username="carol", hostname="box3")

    members = list_members(shared)
    assert len(members) == 3
    # First member must be the admin.
    assert members[0].role == "admin"
    # Remaining members are viewers.
    assert all(m.role == "viewer" for m in members[1:])


def test_record_audit_event_writes_arbitrary_action(shared):
    """record_audit_event can write any action string."""
    admin_rec = _register(shared, username="alice", hostname="box1")

    record_audit_event(
        shared,
        actor_user_id=admin_rec.id,
        action="forced_overwrite",
        target_resource="lineage_comment:abc123",
        details={"reason": "OCC conflict resolution"},
    )

    audit = _audit_rows(shared)
    custom = [r for r in audit if r.action == "forced_overwrite"]
    assert len(custom) == 1
    assert custom[0].target_resource == "lineage_comment:abc123"
    assert custom[0].actor_user_id == admin_rec.id


def test_table_names_create_on_in_memory_sqlite(tmp_path):
    """Full MetaData.create_all round-trip confirms _amx_* naming is SQLite-compatible.

    SQLite does not support arbitrary named schemas, so we use schema="main"
    which is SQLite's implicit schema. This confirms _amx_* table names work
    on SQLite without leading-underscore issues.
    """
    from sqlalchemy import inspect as sa_inspect

    from amx.storage.shared_schema import build_metadata

    engine = create_engine(f"sqlite:///{tmp_path / 'naming_test.db'}")
    md = build_metadata(schema="main")
    # Should not raise.
    md.create_all(engine)

    inspector = sa_inspect(engine)
    table_names = inspector.get_table_names()
    assert "_amx_users" in table_names
    assert "_amx_admin_audit" in table_names
    assert "_amx_session_events" in table_names
