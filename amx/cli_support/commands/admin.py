"""``/admin`` namespace commands for AMX workspace administration.

Surfaces the admin data layer (``amx.storage.admin``) through the CLI.
Every write command (promote, demote, revoke, unrevoke) requires the
invoking user to hold the ``admin`` role in the shared history store.
Read commands (members, audit, sessions) are open to any role.

The shared store is resolved via
:func:`amx.storage.factory.history_store`; commands that need the
shared admin API call :func:`amx.storage.admin` module functions
directly.

Wizard-first invocation: bare ``/admin promote`` (no args) launches a
user picker then confirmation. Supplying the ``<username>`` argument
skips the wizard (power-user shortcut).
"""

from __future__ import annotations

import getpass
import socket
from collections.abc import Callable
from typing import Any

import click

from amx.utils.console import ask_choice, error, info, render_table, success, warn

LogEvent = Callable[..., None]


def _get_shared_store():
    """Return the active history store or None if unavailable."""
    try:
        from amx.storage.factory import history_store

        return history_store()
    except Exception:
        return None


def _current_identity() -> tuple[str, str]:
    """Return (username, hostname) for the invoking user.

    Uses ``getpass.getuser()`` + ``socket.gethostname()`` — the same
    pair that ``SQLAlchemyHistoryStore.__init__`` records when the
    shared store is initialised.  Cross-platform: no POSIX assumptions.
    """
    try:
        username = getpass.getuser()
    except Exception:
        username = "unknown"
    try:
        hostname = socket.gethostname()
    except Exception:
        hostname = "unknown"
    return username, hostname


def _check_admin_role(shared) -> bool:
    """Return True if the invoking user holds the admin role.

    Prints an error message with the list of active admins when not.
    """
    from amx.storage import admin as _admin

    username, hostname = _current_identity()
    role = _admin.current_role(shared, username=username, hostname=hostname)
    if role == "admin":
        return True
    active = _admin.list_active_admins(shared)
    admin_list = ", ".join(active) if active else "(none)"
    error(f"Workspace admin role required. Ask one of: {admin_list}")
    return False


def _require_shared(cmd_name: str) -> Any | None:
    """Return the shared store or print a clear error.

    The shared history store must be enabled (``/history-store enable``)
    before admin commands can work.
    """
    shared = _get_shared_store()
    if shared is None:
        error(
            f"/admin {cmd_name} requires a shared history store. Run /history-store enable first."
        )
        return None
    # Only SQLAlchemyHistoryStore has the _amx_users table.
    if not hasattr(shared, "engine") or not hasattr(shared, "_md"):
        error(
            f"/admin {cmd_name} requires shared mode to be active. Run /history-store enable first."
        )
        return None
    return shared


def _pick_username(shared, prompt: str) -> str | None:
    """Wizard: pick a username from the current member list."""
    from amx.storage import admin as _admin

    members = _admin.list_members(shared)
    if not members:
        warn("No members found in the workspace.")
        return None
    choices = [f"{m.username}@{m.hostname}  [{m.role}]" for m in members]
    choice = ask_choice(prompt, choices)
    if not choice:
        return None
    idx = choices.index(choice)
    return members[idx].username


def register_admin_commands(
    main: click.Group,
    *,
    pass_config: Callable[[Callable[..., Any]], Callable[..., Any]],
    log_event: LogEvent,
) -> click.Group:
    """Attach ``/admin`` namespace commands to the main Click group.

    Returns the inner ``admin`` Click group so the caller can extend it.
    """

    @main.group()
    def admin() -> None:
        """Workspace admin: members, roles, audit log, sessions."""

    # ── /admin members ────────────────────────────────────────────────────

    @admin.command("members")
    def admin_members() -> None:
        """List all workspace members and their roles."""
        shared = _require_shared("members")
        if shared is None:
            return
        from amx.storage import admin as _admin

        members = _admin.list_members(shared)
        if not members:
            info("No members registered yet.")
            return
        rows = []
        for m in members:
            revoked = "yes" if m.revoked_at else "no"
            last_seen = m.last_seen_at.strftime("%Y-%m-%d %H:%M") if m.last_seen_at else "-"
            rows.append(
                [
                    m.username,
                    m.hostname,
                    m.role,
                    last_seen,
                    revoked,
                    m.client_version or "-",
                ]
            )
        render_table(
            "Workspace members",
            ["Username", "Hostname", "Role", "Last seen", "Revoked", "Version"],
            rows,
        )

    # ── /admin promote ────────────────────────────────────────────────────

    @admin.command("promote")
    @click.argument("username", required=False)
    def admin_promote(username: str | None) -> None:
        """Promote USERNAME to admin role.

        Bare ``/admin promote`` opens a wizard to pick the target user.
        """
        shared = _require_shared("promote")
        if shared is None:
            return
        if not _check_admin_role(shared):
            return

        from amx.storage import admin as _admin

        # Wizard when no username supplied.
        if not username:
            username = _pick_username(shared, "Which user to promote to admin?")
            if not username:
                info("Cancelled.")
                return

        target = _admin.resolve_user_by_username(shared, username)
        if target is None:
            error(f"No user found with username '{username}'.")
            return

        actor_username, actor_hostname = _current_identity()
        actor = _admin.resolve_user_by_username(shared, actor_username)
        if actor is None:
            error("Could not resolve your own user record. Are you registered?")
            return

        _admin.promote_to_admin(
            shared,
            actor_user_id=actor.id,
            target_user_id=target.id,
        )
        success(f"Promoted {username} to admin.")
        log_event(
            event_type="admin_promote",
            status="success",
            command="admin.promote",
            details={"target": username},
        )

    # ── /admin demote ─────────────────────────────────────────────────────

    @admin.command("demote")
    @click.argument("username", required=False)
    def admin_demote(username: str | None) -> None:
        """Demote USERNAME from admin to viewer.

        Bare ``/admin demote`` opens a wizard to pick the target user.
        """
        shared = _require_shared("demote")
        if shared is None:
            return
        if not _check_admin_role(shared):
            return

        from amx.storage import admin as _admin
        from amx.storage.admin import AdminInvariantError

        if not username:
            username = _pick_username(shared, "Which user to demote from admin?")
            if not username:
                info("Cancelled.")
                return

        target = _admin.resolve_user_by_username(shared, username)
        if target is None:
            error(f"No user found with username '{username}'.")
            return

        actor_username, _ = _current_identity()
        actor = _admin.resolve_user_by_username(shared, actor_username)
        if actor is None:
            error("Could not resolve your own user record. Are you registered?")
            return

        try:
            _admin.demote_admin(
                shared,
                actor_user_id=actor.id,
                target_user_id=target.id,
            )
        except AdminInvariantError as exc:
            error(f"Cannot demote: {exc}")
            return

        success(f"Demoted {username} to viewer.")
        log_event(
            event_type="admin_demote",
            status="success",
            command="admin.demote",
            details={"target": username},
        )

    # ── /admin revoke ─────────────────────────────────────────────────────

    @admin.command("revoke")
    @click.argument("username", required=False)
    def admin_revoke(username: str | None) -> None:
        """Revoke USERNAME, blocking future connections.

        Bare ``/admin revoke`` opens a wizard to pick the target user.
        """
        shared = _require_shared("revoke")
        if shared is None:
            return
        if not _check_admin_role(shared):
            return

        from amx.storage import admin as _admin
        from amx.storage.admin import AdminInvariantError

        if not username:
            username = _pick_username(shared, "Which user to revoke?")
            if not username:
                info("Cancelled.")
                return

        target = _admin.resolve_user_by_username(shared, username)
        if target is None:
            error(f"No user found with username '{username}'.")
            return

        actor_username, _ = _current_identity()
        actor = _admin.resolve_user_by_username(shared, actor_username)
        if actor is None:
            error("Could not resolve your own user record. Are you registered?")
            return

        try:
            _admin.revoke_user(
                shared,
                actor_user_id=actor.id,
                target_user_id=target.id,
            )
        except AdminInvariantError as exc:
            error(f"Cannot revoke: {exc}")
            return

        success(f"Revoked {username}.")
        log_event(
            event_type="admin_revoke",
            status="success",
            command="admin.revoke",
            details={"target": username},
        )

    # ── /admin unrevoke ───────────────────────────────────────────────────

    @admin.command("unrevoke")
    @click.argument("username", required=False)
    def admin_unrevoke(username: str | None) -> None:
        """Reinstate a previously revoked USERNAME.

        Bare ``/admin unrevoke`` opens a wizard to pick the target user.
        """
        shared = _require_shared("unrevoke")
        if shared is None:
            return
        if not _check_admin_role(shared):
            return

        from amx.storage import admin as _admin

        if not username:
            username = _pick_username(shared, "Which user to unrevoke?")
            if not username:
                info("Cancelled.")
                return

        target = _admin.resolve_user_by_username(shared, username)
        if target is None:
            error(f"No user found with username '{username}'.")
            return

        actor_username, _ = _current_identity()
        actor = _admin.resolve_user_by_username(shared, actor_username)
        if actor is None:
            error("Could not resolve your own user record. Are you registered?")
            return

        _admin.unrevoke_user(
            shared,
            actor_user_id=actor.id,
            target_user_id=target.id,
        )
        success(f"Unrevoked {username}.")
        log_event(
            event_type="admin_unrevoke",
            status="success",
            command="admin.unrevoke",
            details={"target": username},
        )

    # ── /admin audit ──────────────────────────────────────────────────────

    @admin.command("audit")
    @click.option("-n", "--limit", type=int, default=20, help="Number of rows to show.")
    @click.option("--actor", default=None, help="Filter by actor username.")
    @click.option("--action", default=None, help="Filter by action name.")
    def admin_audit(limit: int, actor: str | None, action: str | None) -> None:
        """Show recent admin audit log entries, newest first."""
        shared = _require_shared("audit")
        if shared is None:
            return
        from amx.storage import admin as _admin

        events = _admin.list_audit_events(shared, limit=limit, actor_username=actor, action=action)
        if not events:
            info("No audit events found.")
            return
        rows = []
        for ev in events:
            rows.append(
                [
                    str(ev.get("event_at") or "-")[:19],
                    str(ev.get("actor_username") or "-"),
                    str(ev.get("action") or "-"),
                    str(ev.get("target_user_id") or "-"),
                    str(ev.get("target_resource") or "-"),
                ]
            )
        render_table(
            "Admin audit log",
            ["Time", "Actor", "Action", "Target user ID", "Target resource"],
            rows,
        )

    # ── /admin sessions ───────────────────────────────────────────────────

    @admin.command("sessions")
    @click.option(
        "--since",
        default=None,
        help="ISO datetime (UTC) — only show events after this time.",
    )
    @click.option("-n", "--limit", type=int, default=20, help="Number of rows to show.")
    def admin_sessions(since: str | None, limit: int) -> None:
        """Show recent session connection events, newest first."""
        shared = _require_shared("sessions")
        if shared is None:
            return
        from amx.storage import admin as _admin

        since_dt = None
        if since:
            from datetime import datetime, timezone

            try:
                since_dt = datetime.fromisoformat(since)
                if since_dt.tzinfo is None:
                    since_dt = since_dt.replace(tzinfo=timezone.utc)
            except ValueError:
                error(
                    f"Could not parse --since value {since!r}. Use ISO format e.g. 2026-01-01T00:00:00."
                )
                return

        events = _admin.list_session_events(shared, since=since_dt, limit=limit)
        if not events:
            info("No session events found.")
            return
        rows = []
        for ev in events:
            rows.append(
                [
                    str(ev.get("event_at") or "-")[:19],
                    str(ev.get("username") or "-"),
                    str(ev.get("hostname") or "-"),
                    str(ev.get("event_kind") or "-"),
                    str(ev.get("client_version") or "-"),
                    str(ev.get("os_platform") or "-"),
                ]
            )
        render_table(
            "Session events",
            ["Time", "Username", "Hostname", "Event", "Version", "Platform"],
            rows,
        )

    return admin
