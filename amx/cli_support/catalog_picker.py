"""Shared 'pick a catalog' helper for backends with a 3-level
catalog → schema → table hierarchy (Databricks Unity Catalog,
BigQuery projects, etc.).

Lives at the package level rather than inside ``commands/manual.py``
so every flow that lists schemas / tables before knowing the
catalog (``/connect``, ``/run``, ``/run-apply``, ``/search sync``,
``/edit``) can call the same helper. Per the v0.10.13 UX rule the
picker is always shown when the backend supports catalogs — the
existing ``cfg.catalog`` seeds the default so a user happy with
their previous pick just presses Enter, and a user who wants to
switch catalogs picks a different one.

The helper is intentionally a pure function with no side effects
beyond updating ``db.cfg.catalog`` for the active in-memory
session. Persisting the pick across sessions (writing to disk) is
out of scope; the user can use ``/db`` profile editing for that.
"""

from __future__ import annotations

from typing import Any

from amx.utils.console import info, warn
from amx.utils.logging import get_logger

log = get_logger("cli.catalog_picker")


def ensure_catalog_selected(
    db: Any,
    *,
    silent_when_set: bool = False,
) -> str:
    """Prompt the user for a catalog, defaulting to the current pick.

    Args:
        db: A live :class:`DatabaseConnector`. The helper checks
            ``db.supports_catalogs()`` and short-circuits when False
            so PG / Snowflake / BigQuery (without project switching)
            connections are unaffected.
        silent_when_set: When True, skip the prompt entirely if
            ``cfg.catalog`` is already populated and the catalog
            list is non-empty. Useful for non-interactive flows
            where the user has already pinned a catalog and any
            extra prompt would be noise. Default False — the picker
            shows every time, mirroring the v0.10.13 UX rule.

    Returns:
        The selected catalog name (or the existing one when the
        user pressed Enter to keep it). Returns "" when the
        backend doesn't support catalogs or when the user cancelled.
    """
    # Lazy import to avoid coupling cli_support → commands.manual at
    # import time. The text-prompt helpers come from utils.console
    # via prompt_toolkit; importing them at module load would yank
    # prompt_toolkit into every test that touches the catalog picker.
    from amx.cli_support.commands.manual import (
        _ask_choice_or_cancel,
    )
    from amx.utils.console import step_spinner

    if not bool(getattr(db, "supports_catalogs", lambda: False)()):
        return ""
    existing_catalog = str(
        getattr(db, "cfg", None) and getattr(db.cfg, "catalog", "") or ""
    ).strip()

    try:
        with step_spinner("Listing catalogs"):
            catalogs = list(db.list_catalogs())  # type: ignore[attr-defined]
    except ImportError as exc:
        # Missing optional driver — surface the actionable install hint
        # the adapter already attached to the ImportError instead of
        # falling through to the misleading "SHOW CATALOGS returned
        # nothing" path.
        warn(str(exc))
        return ""
    except Exception as exc:
        log.debug("list_catalogs failed: %s", exc)
        catalogs = []

    if not catalogs:
        if existing_catalog:
            return existing_catalog
        warn(
            "Backend supports catalogs but `SHOW CATALOGS` returned "
            "nothing. Falling back to the legacy schema inspector — "
            "if your Databricks workspace uses Unity Catalog, set "
            "`/db` profile's catalog explicitly."
        )
        return ""

    if silent_when_set and existing_catalog and existing_catalog in catalogs:
        return existing_catalog

    if existing_catalog:
        info(
            f"Current catalog: '{existing_catalog}'. Press Enter to "
            "keep it, or pick a different one."
        )
    else:
        info(
            "Databricks Unity Catalog detected. Pick a catalog before the schema and table picker."
        )

    default_choice = (
        existing_catalog
        if existing_catalog and existing_catalog in catalogs
        else (catalogs[0] if catalogs else "")
    )
    chosen = _ask_choice_or_cancel(
        "Select catalog",
        catalogs,
        default=default_choice,
    )
    if chosen and chosen.strip():
        try:
            db.cfg.catalog = chosen.strip()  # type: ignore[attr-defined]
        except Exception:
            pass
        return chosen.strip()
    return existing_catalog


def warn_when_database_unpinned(db: Any) -> None:
    """One-line hint when a 2-level backend has no database pinned AND
    no runtime picker is wired up for it.

    For PostgreSQL the runtime picker (:func:`ensure_database_selected`)
    now resolves the unpinned case interactively, so this warning only
    fires for backends we haven't built a picker for yet (Snowflake at
    time of writing). When a picker IS available the caller should
    invoke :func:`ensure_database_selected` first; if the user picks
    one, this warning becomes a silent no-op.
    """
    if bool(getattr(db, "supports_catalogs", lambda: False)()):
        # 3-level backend — catalog picker already covered the case.
        return
    cfg = getattr(db, "cfg", None)
    if cfg is None:
        return
    backend = str(getattr(cfg, "backend", "") or "")
    database = str(getattr(cfg, "database", "") or "")
    if database:
        return
    if backend not in {"postgresql", "snowflake"}:
        return
    # Postgres now has the runtime picker. If we still got here without
    # a database it means the picker was offered and the user
    # cancelled, OR we're on a backend (snowflake) where the picker
    # isn't wired yet. Either way the user needs to pin one.
    warn(f"No {backend} database selected — listings will be empty. Use /edit to pin one.")


def ensure_database_selected(db: Any) -> str:
    """Prompt the user to pick a database when a 2-level backend has none pinned.

    Mirrors :func:`ensure_catalog_selected` but for the
    PostgreSQL / Snowflake server-with-multiple-databases case (the
    user-reported "system can't see my databases" flow on 2026-05-02).
    The flow is:

    1. Connect to whatever database the profile resolved to (the
       ``postgres`` system fallback when blank).
    2. ``SELECT datname FROM pg_database WHERE datistemplate = false``
       (or the snowflake equivalent — adapter decides).
    3. Render a numbered picker; the user picks one.
    4. Update ``db.cfg.database`` in-memory and call
       ``db.reconnect()`` so subsequent listing queries target the
       chosen database.

    The pick is **not** persisted to ``~/.amx/config.yml`` — it's
    session-scoped, exactly like the catalog picker. Run ``/edit`` to
    pin a default permanently.

    Returns the chosen database name, or ``""`` when the backend
    doesn't support the picker, no databases were enumerated, or the
    user cancelled.
    """
    from amx.cli_support.commands.manual import _ask_choice_or_cancel
    from amx.utils.console import step_spinner

    cfg = getattr(db, "cfg", None)
    if cfg is None:
        return ""
    backend = str(getattr(cfg, "backend", "") or "")
    if backend not in {"postgresql", "snowflake"}:
        return ""
    existing = str(getattr(cfg, "database", "") or "").strip()
    if existing:
        # Already pinned. The picker is for the unpinned case only —
        # don't pester users who already chose a database.
        return existing
    if bool(getattr(db, "supports_catalogs", lambda: False)()):
        # 3-level backend — catalog picker handles this.
        return ""

    try:
        with step_spinner("Listing databases on this server"):
            databases = list(db.list_databases())  # type: ignore[attr-defined]
    except Exception as exc:
        log.debug("list_databases failed: %s", exc)
        databases = []

    # Filter out template databases that some servers may still surface
    # (the adapter does this too, defence-in-depth).
    databases = [d for d in databases if d and not d.startswith("template")]

    if not databases:
        warn(
            "No databases visible on this server. Either the role lacks "
            "CONNECT on every database, or the server is empty. Use /edit "
            "to pin a database name explicitly."
        )
        return ""

    info(
        f"Profile has no {backend} database pinned. Pick one for this "
        "session (use /edit to pin permanently)."
    )
    chosen = _ask_choice_or_cancel(
        "Select database",
        databases,
        default=databases[0],
    )
    if not chosen or not chosen.strip():
        return ""
    chosen = chosen.strip()
    try:
        db.cfg.database = chosen  # type: ignore[attr-defined]
        db.reconnect()
    except Exception as exc:
        log.debug("Failed to apply chosen database: %s", exc)
        return ""
    return chosen


__all__ = [
    "ensure_catalog_selected",
    "ensure_database_selected",
    "warn_when_database_unpinned",
]
