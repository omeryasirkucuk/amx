"""Admin routes for AMX Studio — member registry, role management, and audit log.

Every write endpoint (promote, demote, revoke, unrevoke) is protected by
:func:`require_admin_role`, a FastAPI dependency that resolves the calling
user from the shared history store and rejects non-admins with 403.

Read endpoints (members, audit, sessions) are accessible to any authenticated
Studio user.

Identity resolution follows the same pattern as the CLI: ``getpass.getuser()``
+ ``socket.gethostname()`` to identify the caller.  When no shared store is
available a ``503`` is returned with a setup hint.
"""

from __future__ import annotations

import getpass
import socket
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel

router = APIRouter(prefix="/api/admin", tags=["admin"])


# ── Identity helpers ──────────────────────────────────────────────────────────


def _caller_identity(request: Request) -> tuple[str, str]:
    """Return ``(username, hostname)`` for the incoming request.

    Falls back to the OS identity (``getpass.getuser()`` + ``socket.gethostname()``)
    because AMX Studio has no separate login mechanism — the session token in
    ``app.state.token`` already gates every ``/api/*`` call, so the identity
    of the caller is the OS user running the ``/studio`` session.

    Cross-platform: uses only stdlib functions available on macOS, Windows,
    and Linux.
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


# ── Shared-store dependency ───────────────────────────────────────────────────


def _get_shared_store():
    """Return the active SQLAlchemyHistoryStore or raise 503."""
    try:
        from amx.storage.factory import history_store

        store = history_store()
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"History store unavailable: {exc}",
        ) from exc

    if store is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "Shared history store is not initialised. "
                "Run /history-store enable from the AMX CLI first."
            ),
        )
    if not hasattr(store, "engine") or not hasattr(store, "_md"):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "Admin API requires shared mode. Run /history-store enable from the AMX CLI first."
            ),
        )
    return store


# ── Permission dependency ─────────────────────────────────────────────────────


def require_admin_role(request: Request):
    """FastAPI dependency: raise 403 when the caller is not an admin.

    Used by every write endpoint (promote, demote, revoke, unrevoke).
    """
    from amx.storage import admin as _admin

    shared = _get_shared_store()
    username, hostname = _caller_identity(request)
    role = _admin.current_role(shared, username=username, hostname=hostname)
    if role != "admin":
        active_admins = _admin.list_active_admins(shared)
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "error": "permission_denied",
                "required_role": "admin",
                "active_admins": active_admins,
            },
        )
    return shared


# ── Pydantic request bodies ───────────────────────────────────────────────────


class UserTargetIn(BaseModel):
    username: str
    hostname: str | None = None


# ── Endpoints ─────────────────────────────────────────────────────────────────


@router.get("/members")
def list_members(request: Request) -> dict[str, Any]:
    """Return all workspace members ordered by role then last activity."""
    from amx.storage import admin as _admin

    shared = _get_shared_store()
    members = _admin.list_members(shared)
    return {
        "members": [
            {
                "id": m.id,
                "username": m.username,
                "hostname": m.hostname,
                "display_name": m.display_name,
                "email": m.email,
                "role": m.role,
                "first_seen_at": m.first_seen_at.isoformat() if m.first_seen_at else None,
                "last_seen_at": m.last_seen_at.isoformat() if m.last_seen_at else None,
                "client_version": m.client_version,
                "revoked_at": m.revoked_at.isoformat() if m.revoked_at else None,
            }
            for m in members
        ],
        "count": len(members),
    }


@router.post("/promote", status_code=status.HTTP_200_OK)
def promote_member(
    body: UserTargetIn,
    request: Request,
    shared=Depends(require_admin_role),
) -> dict[str, Any]:
    """Promote a user to the admin role."""
    from amx.storage import admin as _admin

    target = _admin.resolve_user_by_username(shared, body.username)
    if target is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"User '{body.username}' not found.",
        )

    actor_username, _ = _caller_identity(request)
    actor = _admin.resolve_user_by_username(shared, actor_username)
    if actor is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Could not resolve actor identity.",
        )

    _admin.promote_to_admin(
        shared,
        actor_user_id=actor.id,
        target_user_id=target.id,
    )
    return {"ok": True, "username": body.username, "new_role": "admin"}


@router.post("/demote", status_code=status.HTTP_200_OK)
def demote_member(
    body: UserTargetIn,
    request: Request,
    shared=Depends(require_admin_role),
) -> dict[str, Any]:
    """Demote an admin to the viewer role."""
    from amx.storage import admin as _admin
    from amx.storage.admin import AdminInvariantError

    target = _admin.resolve_user_by_username(shared, body.username)
    if target is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"User '{body.username}' not found.",
        )

    actor_username, _ = _caller_identity(request)
    actor = _admin.resolve_user_by_username(shared, actor_username)
    if actor is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Could not resolve actor identity.",
        )

    try:
        _admin.demote_admin(
            shared,
            actor_user_id=actor.id,
            target_user_id=target.id,
        )
    except AdminInvariantError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"error": "invariant_violation", "message": str(exc)},
        ) from exc

    return {"ok": True, "username": body.username, "new_role": "viewer"}


@router.post("/revoke", status_code=status.HTTP_200_OK)
def revoke_member(
    body: UserTargetIn,
    request: Request,
    shared=Depends(require_admin_role),
) -> dict[str, Any]:
    """Revoke a user, blocking future connections."""
    from amx.storage import admin as _admin
    from amx.storage.admin import AdminInvariantError

    target = _admin.resolve_user_by_username(shared, body.username)
    if target is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"User '{body.username}' not found.",
        )

    actor_username, _ = _caller_identity(request)
    actor = _admin.resolve_user_by_username(shared, actor_username)
    if actor is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Could not resolve actor identity.",
        )

    try:
        _admin.revoke_user(
            shared,
            actor_user_id=actor.id,
            target_user_id=target.id,
        )
    except AdminInvariantError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"error": "invariant_violation", "message": str(exc)},
        ) from exc

    return {"ok": True, "username": body.username, "revoked": True}


@router.post("/unrevoke", status_code=status.HTTP_200_OK)
def unrevoke_member(
    body: UserTargetIn,
    request: Request,
    shared=Depends(require_admin_role),
) -> dict[str, Any]:
    """Reinstate a previously revoked user."""
    from amx.storage import admin as _admin

    target = _admin.resolve_user_by_username(shared, body.username)
    if target is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"User '{body.username}' not found.",
        )

    actor_username, _ = _caller_identity(request)
    actor = _admin.resolve_user_by_username(shared, actor_username)
    if actor is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Could not resolve actor identity.",
        )

    _admin.unrevoke_user(
        shared,
        actor_user_id=actor.id,
        target_user_id=target.id,
    )
    return {"ok": True, "username": body.username, "revoked": False}


@router.get("/audit")
def list_audit(
    limit: int = Query(default=20, ge=1, le=500),
    actor: str | None = Query(default=None),
    action: str | None = Query(default=None),
) -> dict[str, Any]:
    """Return recent admin audit log entries, newest first."""
    from amx.storage import admin as _admin

    shared = _get_shared_store()
    events = _admin.list_audit_events(shared, limit=limit, actor_username=actor, action=action)
    return {"events": events, "count": len(events)}


@router.get("/sessions")
def list_sessions(
    since: str | None = Query(default=None, description="ISO datetime (UTC)"),
    limit: int = Query(default=20, ge=1, le=500),
) -> dict[str, Any]:
    """Return recent session connection events, newest first."""
    from amx.storage import admin as _admin

    shared = _get_shared_store()
    since_dt: datetime | None = None
    if since:
        try:
            since_dt = datetime.fromisoformat(since)
            if since_dt.tzinfo is None:
                since_dt = since_dt.replace(tzinfo=timezone.utc)
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid since value: {exc}",
            ) from exc

    events = _admin.list_session_events(shared, since=since_dt, limit=limit)
    return {"events": events, "count": len(events)}
