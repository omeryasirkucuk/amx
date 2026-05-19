"""Admin operations for the AMX shared history store.

Owns the workspace member registry and all permission management:
registering sessions, querying roles, promoting/demoting/revoking
users, and writing audit events.

Follows the module-level-functions pattern of :mod:`amx.lineage.store`:
every public function takes the ``SQLAlchemyHistoryStore`` as its first
argument and uses its ``engine`` and ``_md`` directly. No classes.

Thread safety: each function opens a fresh ``engine.begin()`` context
so concurrent callers do not share connections.

Custom exception
----------------
:class:`AdminInvariantError` — raised when an operation would leave
the workspace with zero active admins (e.g. demoting or revoking the
last non-revoked admin). Callers should surface this to the user
rather than silently completing the operation.
"""

from __future__ import annotations

import logging
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Literal

from sqlalchemy import and_, func, insert, select, update

if TYPE_CHECKING:
    from amx.storage.sqlalchemy_store import SQLAlchemyHistoryStore

log = logging.getLogger(__name__)


# ── Helpers ───────────────────────────────────────────────────────────────────


def _new_uuid() -> str:
    return str(uuid.uuid4())


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _os_platform() -> str:
    return sys.platform


# ── Public exception ──────────────────────────────────────────────────────────


class AdminInvariantError(Exception):
    """Raised when an operation would leave the workspace with no active admins.

    This protects the workspace from becoming permanently locked out:
    at least one non-revoked admin must always remain so future members
    can be promoted or the configuration can be changed.
    """


# ── Data record ───────────────────────────────────────────────────────────────


@dataclass
class AdminUserRecord:
    """In-memory representation of one row from ``_amx_users``.

    Fields mirror the table columns exactly so callers can rely on
    attribute access without juggling row indices.
    """

    id: str
    username: str
    hostname: str
    display_name: str | None
    email: str | None
    role: str
    first_seen_at: datetime
    last_seen_at: datetime
    client_version: str | None
    created_by: str | None
    revoked_at: datetime | None
    revoked_by: str | None


# ── Table accessors ───────────────────────────────────────────────────────────


def _t_users(shared: SQLAlchemyHistoryStore):  # type: ignore[return]
    return shared._md.tables[f"{shared.schema}._amx_users"]


def _t_audit(shared: SQLAlchemyHistoryStore):  # type: ignore[return]
    return shared._md.tables[f"{shared.schema}._amx_admin_audit"]


def _t_sessions(shared: SQLAlchemyHistoryStore):  # type: ignore[return]
    return shared._md.tables[f"{shared.schema}._amx_session_events"]


# ── Internal helpers ──────────────────────────────────────────────────────────


def _count_active_admins(conn, t_users) -> int:
    """Count non-revoked admins in a transaction already open by the caller."""
    row = conn.execute(
        select(func.count()).where(
            and_(
                t_users.c.role == "admin",
                t_users.c.revoked_at.is_(None),
            )
        )
    ).scalar()
    return int(row or 0)


def _row_to_record(row) -> AdminUserRecord:
    return AdminUserRecord(
        id=row.id,
        username=row.username,
        hostname=row.hostname,
        display_name=row.display_name,
        email=row.email,
        role=row.role,
        first_seen_at=row.first_seen_at,
        last_seen_at=row.last_seen_at,
        client_version=row.client_version,
        created_by=row.created_by,
        revoked_at=row.revoked_at,
        revoked_by=row.revoked_by,
    )


# ── Public API ────────────────────────────────────────────────────────────────


def register_session(
    shared: SQLAlchemyHistoryStore,
    *,
    username: str,
    hostname: str,
    client_version: str,
    db_profiles_seen: list[str],
) -> AdminUserRecord:
    """Register a client session and return the caller's ``AdminUserRecord``.

    Behavior
    --------
    * If ``_amx_users`` is empty the caller becomes the workspace **admin**
      (bootstrap path). A ``first_seen`` session event and a ``user_join``
      audit event are written.
    * If a row for ``(username, hostname)`` already exists its
      ``last_seen_at`` and ``client_version`` are updated and a ``connect``
      session event is appended. No audit row is written for repeat logins.
    * If ``(username, hostname)`` is new but the table already has rows the
      caller joins as a **viewer**. A ``first_seen`` session event and a
      ``user_join`` audit event are written.

    This function is idempotent with respect to the user row: calling it
    twice with the same ``(username, hostname)`` produces exactly one row
    in ``_amx_users`` and one new row in ``_amx_session_events`` per call.
    """
    t_users = _t_users(shared)
    t_audit = _t_audit(shared)
    t_sessions = _t_sessions(shared)
    now = _utcnow()
    platform = _os_platform()

    with shared.engine.begin() as conn:
        # Check if the table is empty (bootstrap scenario).
        total_count = conn.execute(select(func.count()).select_from(t_users)).scalar()
        is_first_user = int(total_count or 0) == 0

        # Look up existing row.
        existing = conn.execute(
            select(t_users).where(
                and_(
                    t_users.c.username == username,
                    t_users.c.hostname == hostname,
                )
            )
        ).fetchone()

        if existing is None:
            # New user.
            role = "admin" if is_first_user else "viewer"
            user_id = _new_uuid()
            conn.execute(
                insert(t_users).values(
                    id=user_id,
                    username=username,
                    hostname=hostname,
                    display_name=None,
                    email=None,
                    role=role,
                    first_seen_at=now,
                    last_seen_at=now,
                    client_version=client_version,
                    created_by=None,
                    revoked_at=None,
                    revoked_by=None,
                )
            )
            event_kind = "first_seen"
            # Write user_join audit event.
            conn.execute(
                insert(t_audit).values(
                    id=_new_uuid(),
                    event_at=now,
                    actor_user_id=user_id,
                    actor_username=username,
                    actor_hostname=hostname,
                    action="user_join",
                    target_user_id=None,
                    target_resource=None,
                    details_json={"role": role, "bootstrap": is_first_user},
                )
            )
        else:
            user_id = existing.id
            conn.execute(
                update(t_users)
                .where(t_users.c.id == user_id)
                .values(
                    last_seen_at=now,
                    client_version=client_version,
                )
            )
            event_kind = "connect"

        # Write session event.
        conn.execute(
            insert(t_sessions).values(
                id=_new_uuid(),
                event_at=now,
                user_id=user_id,
                username=username,
                hostname=hostname,
                event_kind=event_kind,
                client_version=client_version,
                os_platform=platform,
                db_profiles_seen=db_profiles_seen,
            )
        )

        # Re-fetch the final row to return a complete record.
        row = conn.execute(select(t_users).where(t_users.c.id == user_id)).fetchone()

    return _row_to_record(row)


def current_role(
    shared: SQLAlchemyHistoryStore,
    *,
    username: str,
    hostname: str,
) -> Literal["admin", "viewer"] | None:
    """Return the role for a known ``(username, hostname)`` pair.

    Returns ``None`` if the user has never been registered.
    """
    t_users = _t_users(shared)
    with shared.engine.connect() as conn:
        row = conn.execute(
            select(t_users.c.role).where(
                and_(
                    t_users.c.username == username,
                    t_users.c.hostname == hostname,
                )
            )
        ).fetchone()
    if row is None:
        return None
    return row.role  # type: ignore[return-value]


def list_members(shared: SQLAlchemyHistoryStore) -> list[AdminUserRecord]:
    """Return all ``_amx_users`` rows ordered by role then ``last_seen_at`` descending.

    Admins appear before viewers. Within each role group the most recently
    active member appears first.
    """
    t_users = _t_users(shared)
    with shared.engine.connect() as conn:
        rows = conn.execute(
            select(t_users).order_by(t_users.c.role, t_users.c.last_seen_at.desc())
        ).fetchall()
    return [_row_to_record(r) for r in rows]


def promote_to_admin(
    shared: SQLAlchemyHistoryStore,
    *,
    actor_user_id: str,
    target_user_id: str,
) -> None:
    """Promote *target_user_id* to ``admin`` role and write an audit event.

    The actor must exist in ``_amx_users``. No role-guard here (any admin
    can promote — the caller is responsible for checking that the actor is
    themselves an admin if that constraint is needed at the CLI layer).
    """
    t_users = _t_users(shared)
    t_audit = _t_audit(shared)
    now = _utcnow()

    with shared.engine.begin() as conn:
        # Fetch actor details for denormalization.
        actor = conn.execute(
            select(t_users.c.username, t_users.c.hostname).where(t_users.c.id == actor_user_id)
        ).fetchone()
        actor_username = actor.username if actor else ""
        actor_hostname = actor.hostname if actor else ""

        conn.execute(update(t_users).where(t_users.c.id == target_user_id).values(role="admin"))
        conn.execute(
            insert(t_audit).values(
                id=_new_uuid(),
                event_at=now,
                actor_user_id=actor_user_id,
                actor_username=actor_username,
                actor_hostname=actor_hostname,
                action="promote_admin",
                target_user_id=target_user_id,
                target_resource=None,
                details_json={"new_role": "admin"},
            )
        )


def demote_admin(
    shared: SQLAlchemyHistoryStore,
    *,
    actor_user_id: str,
    target_user_id: str,
) -> None:
    """Demote *target_user_id* from ``admin`` to ``viewer``.

    Raises :class:`AdminInvariantError` if doing so would leave the
    workspace with zero non-revoked admins. In that case neither the
    role update nor the audit event is written.
    """
    t_users = _t_users(shared)
    t_audit = _t_audit(shared)
    now = _utcnow()

    with shared.engine.begin() as conn:
        active_admins = _count_active_admins(conn, t_users)
        # If the target is an admin and the only one, the demotion
        # would leave zero admins — reject.
        target_row = conn.execute(
            select(t_users.c.role, t_users.c.revoked_at).where(t_users.c.id == target_user_id)
        ).fetchone()
        if (
            target_row is not None
            and target_row.role == "admin"
            and target_row.revoked_at is None
            and active_admins <= 1
        ):
            raise AdminInvariantError(
                "Cannot demote the last active admin. Promote another user first."
            )

        actor = conn.execute(
            select(t_users.c.username, t_users.c.hostname).where(t_users.c.id == actor_user_id)
        ).fetchone()
        actor_username = actor.username if actor else ""
        actor_hostname = actor.hostname if actor else ""

        conn.execute(update(t_users).where(t_users.c.id == target_user_id).values(role="viewer"))
        conn.execute(
            insert(t_audit).values(
                id=_new_uuid(),
                event_at=now,
                actor_user_id=actor_user_id,
                actor_username=actor_username,
                actor_hostname=actor_hostname,
                action="demote_admin",
                target_user_id=target_user_id,
                target_resource=None,
                details_json={"new_role": "viewer"},
            )
        )


def revoke_user(
    shared: SQLAlchemyHistoryStore,
    *,
    actor_user_id: str,
    target_user_id: str,
) -> None:
    """Revoke *target_user_id*, blocking future connections.

    Raises :class:`AdminInvariantError` if revoking this user would
    leave the workspace with zero non-revoked admins.

    Sets ``revoked_at`` and ``revoked_by`` on the target row and
    writes an audit event.
    """
    t_users = _t_users(shared)
    t_audit = _t_audit(shared)
    now = _utcnow()

    with shared.engine.begin() as conn:
        target_row = conn.execute(
            select(t_users.c.role, t_users.c.revoked_at).where(t_users.c.id == target_user_id)
        ).fetchone()
        if target_row is not None and target_row.role == "admin" and target_row.revoked_at is None:
            active_admins = _count_active_admins(conn, t_users)
            if active_admins <= 1:
                raise AdminInvariantError(
                    "Cannot revoke the last active admin. Promote another user first."
                )

        actor = conn.execute(
            select(t_users.c.username, t_users.c.hostname).where(t_users.c.id == actor_user_id)
        ).fetchone()
        actor_username = actor.username if actor else ""
        actor_hostname = actor.hostname if actor else ""

        conn.execute(
            update(t_users)
            .where(t_users.c.id == target_user_id)
            .values(revoked_at=now, revoked_by=actor_user_id)
        )
        conn.execute(
            insert(t_audit).values(
                id=_new_uuid(),
                event_at=now,
                actor_user_id=actor_user_id,
                actor_username=actor_username,
                actor_hostname=actor_hostname,
                action="revoke",
                target_user_id=target_user_id,
                target_resource=None,
                details_json=None,
            )
        )


def unrevoke_user(
    shared: SQLAlchemyHistoryStore,
    *,
    actor_user_id: str,
    target_user_id: str,
) -> None:
    """Reinstate a previously revoked user, clearing ``revoked_at`` and ``revoked_by``.

    Writes an ``unrevoke`` audit event.
    """
    t_users = _t_users(shared)
    t_audit = _t_audit(shared)
    now = _utcnow()

    with shared.engine.begin() as conn:
        actor = conn.execute(
            select(t_users.c.username, t_users.c.hostname).where(t_users.c.id == actor_user_id)
        ).fetchone()
        actor_username = actor.username if actor else ""
        actor_hostname = actor.hostname if actor else ""

        conn.execute(
            update(t_users)
            .where(t_users.c.id == target_user_id)
            .values(revoked_at=None, revoked_by=None)
        )
        conn.execute(
            insert(t_audit).values(
                id=_new_uuid(),
                event_at=now,
                actor_user_id=actor_user_id,
                actor_username=actor_username,
                actor_hostname=actor_hostname,
                action="unrevoke",
                target_user_id=target_user_id,
                target_resource=None,
                details_json=None,
            )
        )


def list_audit_events(
    shared: SQLAlchemyHistoryStore,
    *,
    limit: int = 50,
    actor_username: str | None = None,
    action: str | None = None,
) -> list[dict]:
    """Return recent audit events from ``_amx_admin_audit``, newest first.

    Optional ``actor_username`` and ``action`` filters narrow the result.
    """
    t_audit = _t_audit(shared)
    stmt = select(t_audit).order_by(t_audit.c.event_at.desc()).limit(limit)
    if actor_username:
        stmt = stmt.where(t_audit.c.actor_username == actor_username)
    if action:
        stmt = stmt.where(t_audit.c.action == action)
    with shared.engine.connect() as conn:
        rows = conn.execute(stmt).fetchall()
    return [
        {
            "id": r.id,
            "event_at": r.event_at.isoformat() if r.event_at else None,
            "actor_user_id": r.actor_user_id,
            "actor_username": r.actor_username,
            "actor_hostname": r.actor_hostname,
            "action": r.action,
            "target_user_id": r.target_user_id,
            "target_resource": r.target_resource,
            "details": r.details_json,
        }
        for r in rows
    ]


def list_session_events(
    shared: SQLAlchemyHistoryStore,
    *,
    since: datetime | None = None,
    limit: int = 50,
) -> list[dict]:
    """Return recent session events from ``_amx_session_events``, newest first.

    Optional ``since`` (UTC datetime) restricts to events after that timestamp.
    """
    t_sessions = _t_sessions(shared)
    stmt = select(t_sessions).order_by(t_sessions.c.event_at.desc()).limit(limit)
    if since is not None:
        stmt = stmt.where(t_sessions.c.event_at >= since)
    with shared.engine.connect() as conn:
        rows = conn.execute(stmt).fetchall()
    return [
        {
            "id": r.id,
            "event_at": r.event_at.isoformat() if r.event_at else None,
            "user_id": r.user_id,
            "username": r.username,
            "hostname": r.hostname,
            "event_kind": r.event_kind,
            "client_version": r.client_version,
            "os_platform": r.os_platform,
            "db_profiles_seen": r.db_profiles_seen,
        }
        for r in rows
    ]


def resolve_user_by_username(
    shared: SQLAlchemyHistoryStore,
    username: str,
) -> AdminUserRecord | None:
    """Return the most-recently-seen ``AdminUserRecord`` for ``username``.

    Resolves the ``username`` column of ``_amx_users``.  Returns ``None``
    when no matching row exists.
    """
    t_users = _t_users(shared)
    with shared.engine.connect() as conn:
        row = conn.execute(
            select(t_users)
            .where(t_users.c.username == username)
            .order_by(t_users.c.last_seen_at.desc())
            .limit(1)
        ).fetchone()
    if row is None:
        return None
    return _row_to_record(row)


def list_active_admins(shared: SQLAlchemyHistoryStore) -> list[str]:
    """Return usernames of all non-revoked admins."""
    t_users = _t_users(shared)
    with shared.engine.connect() as conn:
        rows = conn.execute(
            select(t_users.c.username).where(
                and_(
                    t_users.c.role == "admin",
                    t_users.c.revoked_at.is_(None),
                )
            )
        ).fetchall()
    return [r.username for r in rows]


def record_audit_event(
    shared: SQLAlchemyHistoryStore,
    *,
    actor_user_id: str | None,
    action: str,
    target_user_id: str | None = None,
    target_resource: str | None = None,
    details: Any | None = None,
) -> None:
    """Append an arbitrary audit event to ``_amx_admin_audit``.

    General-purpose entry point usable from other modules (e.g. the
    OCC forced-overwrite path in PR-3) so they don't have to import
    table internals directly.

    Parameters
    ----------
    shared
        The active ``SQLAlchemyHistoryStore`` instance.
    actor_user_id
        UUID of the ``_amx_users`` row for the acting user. ``None`` for
        system-generated events.
    action
        Free-text discriminator for the event (e.g. ``"forced_overwrite"``).
    target_user_id
        UUID of the target ``_amx_users`` row, if applicable.
    target_resource
        Opaque string referencing a non-user resource, if applicable.
    details
        Any JSON-serializable payload with event-specific context.
        Stored in ``details_json``.
    """
    t_users = _t_users(shared)
    t_audit = _t_audit(shared)
    now = _utcnow()

    with shared.engine.begin() as conn:
        if actor_user_id is not None:
            actor = conn.execute(
                select(t_users.c.username, t_users.c.hostname).where(t_users.c.id == actor_user_id)
            ).fetchone()
            actor_username = actor.username if actor else ""
            actor_hostname = actor.hostname if actor else ""
        else:
            actor_username = ""
            actor_hostname = ""

        conn.execute(
            insert(t_audit).values(
                id=_new_uuid(),
                event_at=now,
                actor_user_id=actor_user_id,
                actor_username=actor_username,
                actor_hostname=actor_hostname,
                action=action,
                target_user_id=target_user_id,
                target_resource=target_resource,
                details_json=details,
            )
        )


__all__ = [
    "AdminInvariantError",
    "AdminUserRecord",
    "current_role",
    "demote_admin",
    "list_active_admins",
    "list_audit_events",
    "list_members",
    "list_session_events",
    "promote_to_admin",
    "record_audit_event",
    "register_session",
    "resolve_user_by_username",
    "revoke_user",
    "unrevoke_user",
]
