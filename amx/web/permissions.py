"""Role-based permission dependencies for AMX Studio write endpoints.

Shared storage mutations (lineage comments, documentation pages) should
be restricted to non-viewer roles so a workspace member who joined as a
viewer (the default for new joiners in shared mode) cannot silently
overwrite team metadata.

Roles
-----
* ``admin``  — full access; may read and write.
* ``writer`` — may read and write (reserved for future use; treated
  identically to ``admin`` by this module).
* ``viewer`` — read-only; write attempts → 403.

When the shared history store is not available (local-only mode) the
dependency is a no-op: no ``_amx_users`` table exists so every caller is
implicitly unrestricted.  This keeps local Studio usage unchanged.

Identity
--------
Uses the same ``getpass.getuser()`` + ``socket.gethostname()`` pair that
the admin bootstrap, the CLI admin commands, and the admin API router all
use.  Cross-platform: no POSIX-only assumptions.
"""

from __future__ import annotations

import getpass
import socket

from fastapi import HTTPException, Request, status


def _caller_identity() -> tuple[str, str]:
    try:
        username = getpass.getuser()
    except Exception:
        username = "unknown"
    try:
        hostname = socket.gethostname()
    except Exception:
        hostname = "unknown"
    return username, hostname


def require_writer_role(request: Request) -> None:
    """FastAPI dependency that blocks viewers from write endpoints.

    When the shared history store is active and the caller's role is
    ``"viewer"``, raise 403 with a ``permission_denied`` JSON body.
    In local-only mode (no shared store or no ``_amx_users`` table)
    the check is skipped so the endpoint proceeds normally.
    """
    try:
        from amx.storage.factory import history_store

        store = history_store()
    except Exception:
        # If we can't resolve the store at all, don't block the call.
        return

    if store is None:
        return  # Local-only mode — no restriction.

    if not hasattr(store, "engine") or not hasattr(store, "_md"):
        return  # Not a SQLAlchemyHistoryStore — no restriction.

    try:
        from amx.storage import admin as _admin

        username, hostname = _caller_identity()
        role = _admin.current_role(store, username=username, hostname=hostname)
    except Exception:
        # If the role lookup fails (table not present, network error, …)
        # don't block the call — fail open for resilience.
        return

    if role == "viewer":
        try:
            active_admins = _admin.list_active_admins(store)
        except Exception:
            active_admins = []
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "error": "permission_denied",
                "required_role": "writer",
                "your_role": "viewer",
                "active_admins": active_admins,
            },
        )
