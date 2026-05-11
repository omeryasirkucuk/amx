"""Shared 'pick a catalog' helper for backends with a 3-level
catalog → schema → table hierarchy (Databricks Unity Catalog,
BigQuery projects, etc.).

Lives at the package level rather than inside ``commands/manual.py``
so every flow that lists schemas / tables before knowing the
catalog (``/connect``, ``/run``, ``/run-apply``, ``/search sync``,
``/edit``) can call the same helper.

UX rule (revised post-v0.13): when the profile already has a
``cfg.catalog`` pinned, use it silently — same as the 2-level
``ensure_database_selected`` already does for ``cfg.database``.
Prompting on every run for a value the user has already chosen at
profile-creation time was noise; the original "always prompt with
Enter to keep" rule was rolled back. To explicitly change the pin,
the user re-runs ``/db`` and edits the profile.

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
    silent_when_set: bool = True,
) -> str:
    """Prompt the user for a catalog only when nothing is pinned.

    Args:
        db: A live :class:`DatabaseConnector`. The helper checks
            ``db.supports_catalogs()`` and short-circuits when False
            so PG / Snowflake / BigQuery (without project switching)
            connections are unaffected.
        silent_when_set: When True (the default since v0.13), skip
            the prompt entirely if ``cfg.catalog`` is already pinned
            in the profile AND visible in ``list_catalogs()``. This
            mirrors what :func:`ensure_database_selected` already
            does for 2-level backends — pinned == use directly, no
            re-ask on every run. Pass False to force the picker
            (e.g. an explicit "switch catalog" sub-command).

    Returns:
        The selected catalog name (the pinned value when silent,
        the user's pick otherwise). Returns "" when the backend
        doesn't support catalogs or when the user cancelled.
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
    the runtime picker found nothing to offer.

    Since 0.12.3 :func:`ensure_database_selected` runs for every 2-level
    backend, so this warning is now a "the picker tried, the picker
    couldn't help" terminal hint rather than a "we never built the
    picker" hint. Stays as a safety net for the case where the user
    cancelled the picker, or no databases are visible to the role.
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
    warn(f"No {backend} database selected — listings will be empty. Use /edit to pin one.")


def ensure_database_selected(db: Any) -> str:
    """Prompt the user to pick a database when a 2-level backend has none pinned.

    Generalised in 0.12.3: the previous version short-circuited for any
    backend outside ``{postgresql, snowflake}``, which left MySQL,
    Oracle, MSSQL, Redshift, and ClickHouse users staring at empty
    listings with no clear way to pick a database at runtime. The new
    rule is duck-typed: any backend whose adapter overrides
    :meth:`DatabaseAdapter.list_databases` to return ≥1 entry gets the
    picker. Backends without a ``list_databases`` override (DuckDB,
    BigQuery — those use a different concept) silently return ``""``
    so flows that call this unconditionally don't pay any cost.

    Flow:

    1. Skip when the active profile already has ``cfg.database`` set,
       or when the backend is 3-level (``supports_catalogs() == True``
       — that's what :func:`ensure_catalog_selected` covers).
    2. Call ``db.list_databases()``; surface ``ImportError`` (missing
       optional driver) as an actionable hint instead of swallowing it.
    3. Render a numbered picker; the user picks one.
    4. Update ``db.cfg.database`` in-memory and call ``db.reconnect()``
       so subsequent listing queries target the chosen database.

    The pick is **not** persisted to ``~/.amx/config.yml`` — it's
    session-scoped, exactly like the catalog picker. Run ``/edit`` to
    pin a default permanently.

    Returns the chosen database name, or ``""`` when the backend
    doesn't expose multiple databases, no databases were enumerated, or
    the user cancelled.
    """
    from amx.cli_support.commands.manual import _ask_choice_or_cancel
    from amx.utils.console import step_spinner

    cfg = getattr(db, "cfg", None)
    if cfg is None:
        return ""
    backend = str(getattr(cfg, "backend", "") or "")
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
    except ImportError as exc:
        # Missing optional driver — surface the actionable extras hint
        # the adapter attached to the ImportError instead of pretending
        # the server has no databases.
        warn(str(exc))
        return ""
    except Exception as exc:
        log.debug("list_databases failed: %s", exc)
        databases = []

    # Filter out template databases that some servers may still surface
    # (the adapter does this too, defence-in-depth).
    databases = [d for d in databases if d and not d.startswith("template")]

    if not databases:
        # No multi-database server (e.g. MSSQL with one user DB only,
        # MySQL with a pinned default), or the role has no privileges.
        # Silent return so flows that always call this helper don't
        # spam the user with an irrelevant warning. The 2-level
        # ``warn_when_database_unpinned`` safety net surfaces only when
        # the rest of the flow actually tries to list and finds nothing.
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


def ensure_hierarchy_resolved(db: Any) -> str:
    """Single entry point that picks the right hierarchy helper per backend.

    3-level backends (Databricks Unity Catalog) get the catalog picker;
    2-level backends (Postgres, Snowflake, MySQL, Oracle, MSSQL,
    Redshift, ClickHouse) get the database picker. Callers in
    ``/edit``, ``/run``, ``/sync``, ``/connect`` invoke this helper
    instead of branching themselves — that way new backends only need
    to override the right ``list_*`` method on the adapter and they're
    automatically covered.

    Returns the chosen value (catalog name or database name) for
    callers that want it; most callers just want the side-effect of
    populating ``cfg.catalog`` / ``cfg.database`` so they can ignore
    the return value.
    """
    if bool(getattr(db, "supports_catalogs", lambda: False)()):
        return ensure_catalog_selected(db)
    return ensure_database_selected(db)


__all__ = [
    "ensure_catalog_selected",
    "ensure_database_selected",
    "ensure_hierarchy_resolved",
    "warn_when_database_unpinned",
]
