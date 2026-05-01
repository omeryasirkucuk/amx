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
    from amx.utils.console import step_spinner

    # Lazy import to avoid coupling cli_support → commands.manual at
    # import time. The text-prompt helpers come from utils.console
    # via prompt_toolkit; importing them at module load would yank
    # prompt_toolkit into every test that touches the catalog picker.
    from amx.cli_support.commands.manual import (
        _ask_choice_or_cancel,
    )

    if not bool(getattr(db, "supports_catalogs", lambda: False)()):
        return ""
    existing_catalog = str(
        getattr(db, "cfg", None) and getattr(db.cfg, "catalog", "") or ""
    ).strip()

    try:
        with step_spinner("Listing catalogs"):
            catalogs = list(db.list_catalogs())  # type: ignore[attr-defined]
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
            "Databricks Unity Catalog detected. Pick a catalog before "
            "the schema and table picker."
        )

    default_choice = (
        existing_catalog
        if existing_catalog and existing_catalog in catalogs
        else (catalogs[0] if catalogs else "")
    )
    chosen = _ask_choice_or_cancel(
        "Select catalog", catalogs, default=default_choice,
    )
    if chosen and chosen.strip():
        try:
            db.cfg.catalog = chosen.strip()  # type: ignore[attr-defined]
        except Exception:
            pass
        return chosen.strip()
    return existing_catalog


__all__ = ["ensure_catalog_selected"]
