"""Database metadata editing and monitoring commands for AMX."""

from __future__ import annotations

from collections.abc import Callable

import click

from amx.config import AMXConfig, DBConfig
from amx.db.connector import AssetKind
from amx.services.manual_metadata import (
    ManualEditTarget,
    ManualTargetKind,
    build_inspect_rows,
    build_monitor_rows,
    resolve_path_target,
    resolve_manual_target as _resolve_manual_target,
    split_metadata_path,
)
from amx.utils.console import ask, confirm, console, error, info, render_table, success, warn

LogEvent = Callable[..., None]


def _summarize_db_exception(exc: Exception) -> str:
    detail = str(exc).strip()
    lower = detail.lower()
    if "connection refused" in lower:
        return "Database connection refused."
    if "timeout" in lower or "timed out" in lower:
        return "Database connection timed out."
    if "authentication" in lower or "password" in lower or "permission denied" in lower:
        return "Database authentication failed."
    first_line = next((line.strip() for line in detail.splitlines() if line.strip()), "")
    return first_line[:220]


def _report_manual_db_error(action: str, exc: Exception) -> None:
    error(f"Could not {action} because AMX cannot reach the active database.")
    warn("Check the active DB profile and run /db then /connect.")
    summary = _summarize_db_exception(exc)
    if summary:
        warn(f"Cause: {summary}")


def _is_cancel(value: str | None) -> bool:
    return (value or "").strip().lower() in {"exit", "quit", "q", "cancel"}


def _cancel_manual_edit() -> None:
    warn("Manual edit cancelled.")


def _ask_text_or_cancel(question: str, default: str = "") -> str | None:
    try:
        value = ask(question, default=default)
    except (EOFError, KeyboardInterrupt):
        _cancel_manual_edit()
        return None
    if _is_cancel(value):
        _cancel_manual_edit()
        return None
    return value


def _ask_choice_or_cancel(
    question: str,
    choices: list[str],
    *,
    default: str = "",
    descriptions: dict[str, str] | None = None,
) -> str | None:
    if not choices:
        return default or None
    while True:
        console.print(f"  [info]{question}[/info]")
        for i, choice in enumerate(choices, 1):
            marker = " [dim](default)[/dim]" if default and choice == default else ""
            desc = f" [dim]{descriptions[choice]}[/dim]" if descriptions and choice in descriptions else ""
            console.print(f"    {i}. [bold]{choice}[/bold]{desc}{marker}")
        value = _ask_text_or_cancel("Select", default="")
        if value is None:
            return None
        if not value and default:
            return default
        if value.isdigit() and 1 <= int(value) <= len(choices):
            return choices[int(value) - 1]
        if value in choices:
            return value
        lower_matches = [choice for choice in choices if choice.lower() == value.lower()]
        if len(lower_matches) == 1:
            return lower_matches[0]
        prefix_matches = [choice for choice in choices if choice.lower().startswith(value.lower())]
        if len(prefix_matches) == 1:
            return prefix_matches[0]
        warn(f"No option matched {value!r}. Type a number, name, or exit.")


def _connector_for_profile(profile_cfg: DBConfig):
    from amx.db.connector import DatabaseConnector

    return DatabaseConnector(profile_cfg)


def _profile_for_path(cfg: AMXConfig, path: str) -> tuple[str, DBConfig] | None:
    parts = split_metadata_path(path)
    if not parts:
        return None
    requested = parts[0]
    if requested in cfg.db_profiles:
        return requested, cfg.db_profiles[requested]
    active = cfg.active_db_profile or "default"
    if requested == active or requested == cfg.db.database:
        return active, cfg.db
    return None


def _target_from_legacy_scope(
    cfg: AMXConfig,
    db: object,
    scope: str,
    names: list[str],
) -> ManualEditTarget | None:
    target = _resolve_manual_target(cfg, db, scope, names, error=warn)
    if target is None:
        return None
    label, writer = target
    kind = ManualTargetKind.DATABASE if scope in {"database", "db"} else ManualTargetKind(scope)
    return ManualEditTarget(
        profile=cfg.active_db_profile or "default",
        kind=kind,
        label=label,
        writer=writer,
    )


def _resolve_explicit_edit_target(
    cfg: AMXConfig,
    target_parts: tuple[str, ...],
) -> ManualEditTarget | None:
    if not target_parts:
        return None

    head = target_parts[0].lower()
    if head in {"schema", "table", "column"} and len(target_parts) == 1:
        return None

    if head in {"database", "db", "schema", "table", "column"}:
        db = _connector_for_profile(cfg.db)
        return _target_from_legacy_scope(cfg, db, head, list(target_parts[1:]))

    if len(target_parts) != 1:
        return None

    profile = _profile_for_path(cfg, target_parts[0])
    if profile is None:
        warn("Use /edit <db>, <db>.<schema>, <db>.<schema>.<table>, or <db>.<schema>.<table>.<column>.")
        warn("Tip: run /edit with no target for the guided wizard.")
        return None

    profile_name, profile_cfg = profile
    db = _connector_for_profile(profile_cfg)
    return resolve_path_target(cfg, db, profile_name, target_parts[0], error=warn)


def _sync_manual_comment_to_search_catalog(
    cfg: AMXConfig,
    target: ManualEditTarget,
    value: str,
) -> None:
    try:
        from amx.search.catalog import SearchCatalog
    except Exception:
        return
    catalog = SearchCatalog.from_history_store()
    if catalog is None:
        return
    profile_cfg = cfg.db_profiles.get(target.profile, cfg.db)
    database_name = profile_cfg.database or profile_cfg.catalog or profile_cfg.project or ""
    path = target.label.split()[-1]
    parts = split_metadata_path(path)
    schema_name = ""
    table_name = ""
    column_name: str | None = None
    entity_kind = "table"
    asset_kind = AssetKind.TABLE.value
    if target.kind == ManualTargetKind.DATABASE:
        entity_kind = "database"
        asset_kind = AssetKind.DATABASE.value
    elif target.kind == ManualTargetKind.SCHEMA:
        entity_kind = "schema"
        asset_kind = AssetKind.SCHEMA.value
        if parts:
            schema_name = parts[-1]
    elif target.kind == ManualTargetKind.TABLE:
        entity_kind = "table"
        if len(parts) >= 2:
            schema_name = parts[-2]
            table_name = parts[-1]
    elif target.kind == ManualTargetKind.COLUMN:
        entity_kind = "column"
        if len(parts) >= 3:
            schema_name = parts[-3]
            table_name = parts[-2]
            column_name = parts[-1]
    catalog.record_manual_description(
        db_profile=target.profile,
        db_backend=profile_cfg.backend,
        database_name=database_name,
        schema_name=schema_name,
        table_name=table_name,
        column_name=column_name,
        entity_kind=entity_kind,
        asset_kind=asset_kind,
        description=value,
    )


def _select_db_profile_for_wizard(cfg: AMXConfig) -> tuple[str, DBConfig] | None:
    active = cfg.active_db_profile or "default"
    current = cfg.db_profiles.get(active, cfg.db)
    answer = _ask_text_or_cancel(f"Use current active database profile '{active}'? (y/n)", default="y")
    if answer is None:
        return None
    if answer.strip().lower() in {"y", "yes"}:
        return active, current
    if answer.strip().lower() not in {"n", "no"}:
        warn("Please answer y or n.")
        return _select_db_profile_for_wizard(cfg)

    names = sorted(cfg.db_profiles.keys())
    descriptions = {name: f"[{db.backend}] {db.display_summary}" for name, db in cfg.db_profiles.items()}
    selected = _ask_choice_or_cancel(
        "Select database profile",
        names,
        default=active if active in names else (names[0] if names else ""),
        descriptions=descriptions,
    )
    if selected is None:
        return None
    return selected, cfg.db_profiles[selected]


def _select_catalog_for_wizard(db: object) -> str:
    """Shim that delegates to the shared catalog picker.

    The actual logic lives in ``amx.cli_support.catalog_picker``
    so every flow (``/edit``, ``/run``, ``/run-apply``, ``/connect``,
    ``/search sync``) can call the same helper. Kept as a wrapper
    for backwards compatibility with existing call sites in this
    module.
    """
    from amx.cli_support.catalog_picker import ensure_catalog_selected

    return ensure_catalog_selected(db)


def _select_schema_for_wizard(db: object, default: str = "") -> str | None:
    from amx.utils.console import step_spinner

    # Catalog picker for backends with a 3-level hierarchy. No-op
    # when the active connection isn't Unity-Catalog-shaped.
    _select_catalog_for_wizard(db)

    try:
        with step_spinner("Listing schemas for manual edit"):
            schemas = list(db.list_schemas())  # type: ignore[attr-defined]
    except Exception:
        schemas = []
    if schemas:
        return _ask_choice_or_cancel("Select schema", schemas, default=default if default in schemas else "")
    return _ask_text_or_cancel("Schema", default=default)


def _select_table_for_wizard(db: object, schema: str, default: str = "") -> str | None:
    from amx.utils.console import step_spinner

    try:
        with step_spinner(f"Listing assets in {schema}"):
            assets = list(db.list_assets(schema))  # type: ignore[attr-defined]
    except Exception:
        assets = []
    names = [name for name, _kind in assets]
    if names:
        descriptions = {name: kind.label for name, kind in assets}
        return _ask_choice_or_cancel("Select table/view", names, default=default if default in names else "", descriptions=descriptions)
    return _ask_text_or_cancel("Table/view", default=default)


def _select_column_for_wizard(db: object, schema: str, table: str) -> str | None:
    from amx.utils.console import step_spinner

    try:
        with step_spinner(f"Listing columns for {schema}.{table}"):
            profiles = list(db.list_column_profiles(schema, table))  # type: ignore[attr-defined]
    except Exception:
        profiles = []
    names = [profile.name for profile in profiles]
    if names:
        descriptions = {profile.name: profile.dtype for profile in profiles}
        return _ask_choice_or_cancel("Select column", names, descriptions=descriptions)
    return _ask_text_or_cancel("Column")


def _resolve_bulk_target_name(cfg: AMXConfig, bulk_pick_mode: str) -> str | None:
    """Resolve the bare entity name for a bulk-edit run.

    There are three paths:
    * ``Pick a column`` — drill DB → schema → table → column. Uses the
      picked column's NAME (not the fully qualified path) so that
      ``_run_bulk_edit_by_name`` can fan out to every other table/schema
      that has a column with the same name.
    * ``Pick a table`` — drill DB → schema → table. Uses the table NAME
      so the bulk-edit picks up the same table name across every schema.
    * ``Type a name manually`` — keeps the legacy text-entry path for
      power users who already know the name.

    Returns ``None`` if the user cancels at any step.
    """
    mode = (bulk_pick_mode or "").lower()
    if mode.startswith("type"):
        return _ask_text_or_cancel(
            "Entity name to bulk-edit (column or table; AMX finds every match)",
            default="",
        )

    selected = _select_db_profile_for_wizard(cfg)
    if selected is None:
        return None
    _, profile_cfg = selected
    db = _connector_for_profile(profile_cfg)

    schema = _select_schema_for_wizard(db, default=cfg.current_schema)
    if schema is None:
        return None
    table = _select_table_for_wizard(db, schema, default=cfg.current_table)
    if table is None:
        return None

    if mode.startswith("pick a column"):
        column = _select_column_for_wizard(db, schema, table)
        if column is None:
            return None
        info(
            f"  Using column name '{column}' (from {schema}.{table}) as bulk target — "
            "AMX will find every other column that shares this name."
        )
        return column

    # "Pick a table" path
    info(
        f"  Using table name '{table}' (from schema {schema}) as bulk target — "
        "AMX will find every other table that shares this name."
    )
    return table


def _run_edit_wizard(cfg: AMXConfig) -> ManualEditTarget | None:
    # First question — let the user decide how MANY entities they want to
    # touch, before they walk into the per-asset wizard. Without this, a
    # user that wanted to bulk-edit ``customer_id`` across 50 tables had
    # to either type the bare name on the command line or step through
    # one (database → schema → table → column) cycle per occurrence.
    edit_mode = _ask_choice_or_cancel(
        "How would you like to edit?",
        [
            "Single entity",
            "Bulk by name (column or table across many schemas)",
        ],
        default="Single entity",
    )
    if edit_mode is None:
        return None
    if edit_mode.startswith("Bulk"):
        # Pick how the user wants to identify the entity to bulk-edit.
        # The drill-down options reuse the existing wizard pickers so the
        # user doesn't have to remember the exact spelling — they pick a
        # concrete table/column from the live DB and AMX uses that
        # asset's NAME to find every other asset that shares it.
        bulk_pick_mode = _ask_choice_or_cancel(
            "Bulk-edit by what?",
            [
                "Pick a column from the catalog",
                "Pick a table from the catalog",
                "Type a name manually",
            ],
            default="Pick a column from the catalog",
        )
        if bulk_pick_mode is None:
            return None
        bare_name = _resolve_bulk_target_name(cfg, bulk_pick_mode)
        if bare_name is None or not bare_name.strip():
            warn("Bulk edit cancelled.")
            return None
        # The bulk flow runs to completion on its own (writes to DB,
        # syncs catalog) and returns no ManualEditTarget. Returning
        # ``None`` here is the documented signal that the caller (the
        # command's outer try/except wrapper) shouldn't try to apply
        # another edit on top.
        _run_bulk_edit_by_name(
            cfg,
            bare_name=bare_name.strip(),
            comment=None,
            skip_confirm=False,
            log_event=None,
            preselected_mode="bulk",
        )
        return None

    selected = _select_db_profile_for_wizard(cfg)
    if selected is None:
        return None
    profile_name, profile_cfg = selected
    db = _connector_for_profile(profile_cfg)

    granularity = _ask_choice_or_cancel(
        "What do you want to edit?",
        ["Database", "Schema", "Table", "Column"],
        default="Table",
    )
    if granularity is None:
        return None

    kind = granularity.lower()
    if kind == "database":
        return ManualEditTarget(
            profile=profile_name,
            kind=ManualTargetKind.DATABASE,
            label=f"database {profile_name}",
            writer=lambda comment: db.set_database_comment(comment),  # type: ignore[attr-defined]
        )

    schema = _select_schema_for_wizard(db, default=cfg.current_schema)
    if schema is None:
        return None
    if kind == "schema":
        return ManualEditTarget(
            profile=profile_name,
            kind=ManualTargetKind.SCHEMA,
            label=f"schema {profile_name}.{schema}",
            writer=lambda comment: db.set_schema_comment(schema, comment),  # type: ignore[attr-defined]
        )

    table = _select_table_for_wizard(db, schema, default=cfg.current_table)
    if table is None:
        return None
    if kind == "table":
        asset_kind = db.resolve_asset_kind(schema, table)  # type: ignore[attr-defined]
        if not isinstance(asset_kind, AssetKind):
            asset_kind = AssetKind.TABLE
        return ManualEditTarget(
            profile=profile_name,
            kind=ManualTargetKind.TABLE,
            label=f"{asset_kind.label} {profile_name}.{schema}.{table}",
            writer=lambda comment: db.set_table_comment(schema, table, comment, asset_kind=asset_kind),  # type: ignore[attr-defined]
        )

    column = _select_column_for_wizard(db, schema, table)
    if column is None:
        return None
    return ManualEditTarget(
        profile=profile_name,
        kind=ManualTargetKind.COLUMN,
        label=f"column {profile_name}.{schema}.{table}.{column}",
        writer=lambda comment: db.set_column_comment(schema, table, column, comment),  # type: ignore[attr-defined]
    )


def _prompt_for_comment(target_label: str, comment: str | None) -> str | None:
    if comment is not None:
        return comment
    console.print(f"  [heading]Editing: {target_label}[/heading]")
    return _ask_text_or_cancel("New comment", default="")


def _parse_multiselect(raw: str, total: int) -> list[int]:
    """Parse '1,3,5' / 'all' / '1-4' into 0-based indices.

    Returns an empty list when input is empty / 'cancel' / nothing valid.
    """
    raw = (raw or "").strip().lower()
    if not raw or raw in {"cancel", "exit", "q", "quit"}:
        return []
    if raw == "all":
        return list(range(total))
    indices: set[int] = set()
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        if "-" in token:
            try:
                lo_s, hi_s = token.split("-", 1)
                lo = int(lo_s)
                hi = int(hi_s)
            except ValueError:
                continue
            for i in range(min(lo, hi), max(lo, hi) + 1):
                if 1 <= i <= total:
                    indices.add(i - 1)
            continue
        try:
            i = int(token)
        except ValueError:
            continue
        if 1 <= i <= total:
            indices.add(i - 1)
    return sorted(indices)


def _run_bulk_edit_by_name(
    cfg: AMXConfig,
    *,
    bare_name: str,
    comment: str | None,
    skip_confirm: bool,
    log_event: LogEvent | None,
    preselected_mode: str | None = None,
) -> None:
    """Bulk-edit comment by bare entity name.

    ``preselected_mode`` lets a caller (e.g. the wizard, when the user
    already picked "Bulk by name" at the very first step) skip the
    bulk-vs-individual question. Accepted values: ``"bulk"`` ,
    ``"individual"`` , or ``None`` (ask the user as usual).

    Searches the catalog for tables and columns matching the name, then
    offers a multi-select picker. The same comment text is applied to
    every selected entity via the live DB ``COMMENT ON …`` SQL. Catalog
    state is then refreshed so the new comments are immediately visible
    to ``/ask``.
    """
    from amx.db.connector import DatabaseConnector
    from amx.search.catalog import SearchCatalog

    db_profile = cfg.active_db_profile or "default"
    catalog = SearchCatalog.from_history_store()
    if catalog is None:
        error("Catalog is unavailable; bulk-edit by name needs an indexed catalog. Run /search /sync first.")
        return

    try:
        table_rows = catalog.find_tables_by_exact_name(db_profile, bare_name, limit=200)
    except Exception as exc:
        error(f"Catalog lookup for tables failed: {exc}")
        table_rows = []
    try:
        column_rows = catalog.find_columns_by_exact_name(db_profile, bare_name, limit=500)
    except Exception as exc:
        error(f"Catalog lookup for columns failed: {exc}")
        column_rows = []

    if not table_rows and not column_rows:
        error(
            f"No tables or columns named '{bare_name}' in the catalog for profile "
            f"'{db_profile}'. If you just sync'd /run-apply or recently added a table, "
            "run `/search sync` to refresh the catalog index, then retry."
        )
        return

    # Build a unified entity list: each row is one (kind, schema, table, column).
    entries: list[dict[str, str]] = []
    for r in table_rows:
        entries.append({
            "kind": "table",
            "schema": str(r.get("schema_name") or ""),
            "table": str(r.get("table_name") or ""),
            "column": "",
            "dtype": "",
            "existing": str(r.get("effective_description") or ""),
        })
    for r in column_rows:
        entries.append({
            "kind": "column",
            "schema": str(r.get("schema_name") or ""),
            "table": str(r.get("table_name") or ""),
            "column": str(r.get("column_name") or ""),
            "dtype": str(r.get("dtype") or ""),
            "existing": str(r.get("effective_description") or ""),
        })

    # Bulk-update analysis header — explicit summary of what AMX is about
    # to do, so the user can see the impact at a glance before any
    # selection. Counts how many tables vs columns matched and how many
    # schemas are involved.
    table_match_count = sum(1 for e in entries if e["kind"] == "table")
    column_match_count = sum(1 for e in entries if e["kind"] == "column")
    distinct_schemas = sorted({e["schema"] for e in entries if e["schema"]})
    console.print(
        f"\n  [heading]Bulk-update analysis for '{bare_name}'[/heading]"
    )
    summary_bits: list[str] = []
    if table_match_count:
        summary_bits.append(f"{table_match_count} table(s)")
    if column_match_count:
        summary_bits.append(f"{column_match_count} column(s)")
    schemas_text = (
        f"{len(distinct_schemas)} schema(s): {', '.join(distinct_schemas[:5])}"
        + ("…" if len(distinct_schemas) > 5 else "")
    )
    info(
        f"  Found {' + '.join(summary_bits) or 'no matches'} across {schemas_text}."
    )
    info(
        "  Whatever you select below will be updated TOGETHER with the same comment."
    )
    rows_for_render = []
    for idx, e in enumerate(entries, start=1):
        if e["kind"] == "table":
            label = f"{e['schema']}.{e['table']}"
            kind_str = "TABLE"
            dtype = "—"
        else:
            label = f"{e['schema']}.{e['table']}.{e['column']}"
            kind_str = "COL"
            dtype = e["dtype"] or "—"
        existing = (e["existing"] or "").strip()
        existing_display = (existing[:50] + "…") if len(existing) > 50 else existing or "(none)"
        rows_for_render.append([str(idx), kind_str, label, dtype, existing_display])
    render_table(
        f"Matches for '{bare_name}'",
        ["#", "Kind", "Schema.Table[.Column]", "Type", "Existing comment"],
        rows_for_render,
    )

    # Single-match short-circuit: no point asking bulk-vs-individual when
    # there's only one row. Fall through to the existing single-target
    # path so the user gets the regular edit flow.
    if len(entries) == 1:
        only = entries[0]
        info("Only one match — switching to single-target edit.")
        target_path = (
            f"{cfg.active_db_profile or 'default'}.{only['schema']}.{only['table']}"
            + (f".{only['column']}" if only["column"] else "")
        )
        info(f"Path: {target_path}")
        # Fall back to the existing edit flow by emitting the path as if
        # the user had typed it. Keeps the legacy behavior for trivial
        # cases instead of duplicating it here.
        return

    # If the caller already locked the mode (e.g. the wizard's first
    # prompt picked "Bulk by name"), skip the second-level question.
    if preselected_mode in {"bulk", "individual"}:
        mode_norm = preselected_mode
    else:
        # Ask the user how they want to handle multiple matches BEFORE
        # locking them into bulk mode. Some entities just happen to share
        # a name and need different descriptions; the user wants the choice.
        mode = _ask_text_or_cancel(
            "How to handle these matches? "
            "[bulk] one comment for selected rows  |  "
            "[individual] walk through each row separately  |  "
            "[cancel]",
            default="bulk",
        )
        if mode is None:
            warn("Edit cancelled.")
            return
        mode_norm = mode.strip().lower()
    if mode_norm in {"cancel", "exit", "q", "quit"}:
        warn("Edit cancelled.")
        return
    if mode_norm in {"individual", "i", "one-by-one", "each", "ind"}:
        _run_individual_edits(
            cfg,
            entries=entries,
            db_profile=db_profile,
            skip_confirm=skip_confirm,
            log_event=log_event,
        )
        return
    if mode_norm not in {"bulk", "b", "batch", ""}:
        warn(f"Unknown mode {mode_norm!r}. Use 'bulk', 'individual', or 'cancel'.")
        return

    selection_raw = _ask_text_or_cancel(
        "Pick rows (e.g. 1,3,5 or 1-4 or all; empty/cancel to abort)",
        default="all",
    )
    if selection_raw is None:
        warn("Bulk edit cancelled.")
        return
    indices = _parse_multiselect(selection_raw, total=len(entries))
    if not indices:
        warn("No rows selected — bulk edit cancelled.")
        return

    selected = [entries[i] for i in indices]
    info(f"{len(selected)} entity(ies) will receive the same comment.")
    new_comment = _prompt_for_comment(
        f"{len(selected)} entity(ies) named '{bare_name}'",
        comment,
    )
    if new_comment is None:
        return
    if not new_comment.strip():
        warn("Empty comment — bulk edit cancelled. Use a non-empty value.")
        return

    if not skip_confirm:
        try:
            confirmed = confirm(
                f"Write the same comment to {len(selected)} entity(ies)?",
                default=True,
            )
        except (EOFError, KeyboardInterrupt):
            warn("Bulk edit cancelled.")
            return
        if not confirmed:
            warn("Bulk edit cancelled.")
            return

    db = DatabaseConnector(cfg.db)
    try:
        applied = 0
        failed: list[tuple[str, str]] = []
        for e in selected:
            label = (
                f"{e['schema']}.{e['table']}.{e['column']}"
                if e["kind"] == "column"
                else f"{e['schema']}.{e['table']}"
            )
            try:
                # Resolve the actual live-DB asset kind for the
                # (schema, table) pair so VIEWs and MATERIALIZED VIEWs
                # get the right COMMENT ON keyword. Column-level edits
                # bypass this — they always use COMMENT ON COLUMN.
                ak = AssetKind.TABLE
                if not e["column"]:
                    try:
                        ak = db.resolve_asset_kind(e["schema"], e["table"])
                    except Exception:
                        ak = AssetKind.TABLE
                db.apply_comment(
                    schema=e["schema"],
                    table=e["table"],
                    column=e["column"] or None,
                    comment=new_comment,
                    asset_kind=ak,
                )
                applied += 1
            except Exception as exc:
                failed.append((label, str(exc)))
        success(f"Applied comment to {applied}/{len(selected)} entity(ies).")
        if failed:
            warn(f"{len(failed)} write(s) failed:")
            for label, msg in failed[:5]:
                warn(f"  {label}: {msg[:120]}")
            if len(failed) > 5:
                warn(f"  … and {len(failed) - 5} more — see ~/.amx/logs/amx.log")

        # Sync the new comments back to the catalog so /ask and the search
        # tools see them immediately. Without this step, ``find_columns_by_
        # exact_name`` would still show stale descriptions on the next
        # invocation.
        if applied:
            try:
                from amx.search.catalog import SearchCatalog as _Catalog
                cat = _Catalog.from_history_store()
                if cat is not None:
                    db_name = cfg.db.database or cfg.db.catalog or cfg.db.project or ""
                    for e in selected:
                        is_col = bool(e["column"])
                        cat.record_manual_description(
                            db_profile=db_profile,
                            db_backend=cfg.db.backend or "",
                            database_name=db_name,
                            schema_name=e["schema"],
                            table_name=e["table"],
                            column_name=e["column"] or None,
                            entity_kind="column" if is_col else "table",
                            asset_kind="column" if is_col else "table",
                            description=new_comment,
                        )
            except Exception as exc:
                log_event_if_present(
                    log_event,
                    "manual_metadata_bulk_catalog_sync_failed",
                    "warning",
                    {"error": str(exc)},
                )

        if log_event is not None:
            log_event(
                event_type="manual_metadata_bulk_edit",
                status="success" if not failed else "partial",
                command="manual edit (bulk-by-name)",
                details={
                    "name": bare_name,
                    "applied": applied,
                    "failed": len(failed),
                    "db_profile": db_profile,
                },
            )
    finally:
        try:
            db.close()
        except Exception:
            pass


def log_event_if_present(log_event: LogEvent | None, name: str, status: str, details: dict) -> None:
    if log_event is None:
        return
    try:
        log_event(event_type=name, status=status, command="manual edit", details=details)
    except Exception:
        pass


def _run_individual_edits(
    cfg: AMXConfig,
    *,
    entries: list[dict[str, str]],
    db_profile: str,
    skip_confirm: bool,
    log_event: LogEvent | None,
) -> None:
    """Walk each match in turn and prompt for a per-row comment.

    The opposite of bulk mode: the user explicitly opted out of "one
    comment for many" because the entities, despite sharing a name,
    are semantically different (e.g. ``code`` in ``country.code`` vs
    ``currency.code``). For each entry we print its full path + dtype
    + existing comment, ask for a NEW comment (Enter to skip), and
    write per-entity. Skipping leaves the existing comment untouched.
    """
    from amx.db.connector import DatabaseConnector
    from amx.search.catalog import SearchCatalog

    if not entries:
        return
    db = DatabaseConnector(cfg.db)
    catalog: SearchCatalog | None = None
    try:
        catalog = SearchCatalog.from_history_store()
    except Exception:
        catalog = None
    db_name = cfg.db.database or cfg.db.catalog or cfg.db.project or ""

    applied = 0
    skipped = 0
    failed: list[tuple[str, str]] = []
    try:
        for idx, e in enumerate(entries, start=1):
            is_col = bool(e["column"])
            label = (
                f"{e['schema']}.{e['table']}.{e['column']}"
                if is_col
                else f"{e['schema']}.{e['table']}"
            )
            kind_str = "COLUMN" if is_col else "TABLE"
            existing = (e["existing"] or "").strip() or "(none)"
            console.print(
                f"\n  [heading]({idx}/{len(entries)}) {kind_str}: {label}[/heading]"
            )
            console.print(f"  [dim]Type: {e['dtype'] or '—'} · existing: {existing}[/dim]")
            new_text = _ask_text_or_cancel(
                "New comment (Enter = skip, 'cancel' = stop the loop)",
                default="",
            )
            if new_text is None:
                warn("Stopped by user.")
                break
            new_text = new_text.strip()
            if not new_text:
                skipped += 1
                continue
            try:
                ak = AssetKind.TABLE
                if not is_col:
                    try:
                        ak = db.resolve_asset_kind(e["schema"], e["table"])
                    except Exception:
                        ak = AssetKind.TABLE
                db.apply_comment(
                    schema=e["schema"],
                    table=e["table"],
                    column=e["column"] or None,
                    comment=new_text,
                    asset_kind=ak,
                )
                applied += 1
                if catalog is not None:
                    try:
                        catalog.record_manual_description(
                            db_profile=db_profile,
                            db_backend=cfg.db.backend or "",
                            database_name=db_name,
                            schema_name=e["schema"],
                            table_name=e["table"],
                            column_name=e["column"] or None,
                            entity_kind="column" if is_col else "table",
                            asset_kind="column" if is_col else "table",
                            description=new_text,
                        )
                    except Exception as exc:
                        log_event_if_present(
                            log_event,
                            "manual_metadata_individual_catalog_sync_failed",
                            "warning",
                            {"path": label, "error": str(exc)},
                        )
            except Exception as exc:
                failed.append((label, str(exc)))
        success(
            f"Individual edits done. Applied {applied}, skipped {skipped}, failed {len(failed)}."
        )
        if failed:
            warn(f"{len(failed)} write(s) failed:")
            for label, msg in failed[:5]:
                warn(f"  {label}: {msg[:120]}")
            if len(failed) > 5:
                warn(f"  … and {len(failed) - 5} more — see ~/.amx/logs/amx.log")
        if log_event is not None:
            log_event(
                event_type="manual_metadata_individual_edits",
                status="success" if not failed else "partial",
                command="manual edit (individual loop)",
                details={
                    "applied": applied,
                    "skipped": skipped,
                    "failed": len(failed),
                    "db_profile": db_profile,
                },
            )
    finally:
        try:
            db.close()
        except Exception:
            pass


def register_manual_commands(
    main: click.Group,
    *,
    pass_config: Callable[..., object],
    log_event: LogEvent | None = None,
) -> click.Group:
    """Attach database metadata editing commands to the main Click group."""

    @main.group("metadata")
    def manual() -> None:
        """Inspect, edit, and monitor database metadata."""

    @manual.command("inspect")
    @click.argument("schema", required=False)
    @click.argument("table", required=False)
    @pass_config
    def manual_inspect(cfg: AMXConfig, schema: str | None, table: str | None) -> None:
        """Inspect current database/schema/table/column comments."""
        from amx.db.connector import DatabaseConnector

        db = DatabaseConnector(cfg.db)
        try:
            result = build_inspect_rows(cfg, db, schema, table, error=error)
        except Exception as exc:
            _report_manual_db_error("inspect metadata", exc)
            return
        if result is None:
            return
        title, rows = result
        render_table(title, ["Scope", "Name", "Comment"], rows)

    @manual.command("edit")
    @click.argument("target_parts", nargs=-1)
    @click.option("--comment", "-c", default=None, help="Comment text. If omitted, AMX prompts interactively.")
    @click.option("--yes", "-y", is_flag=True, help="Write without confirmation.")
    @pass_config
    def manual_edit(
        cfg: AMXConfig,
        target_parts: tuple[str, ...],
        comment: str | None,
        yes: bool,
    ) -> None:
        """Edit one database/schema/table/column comment manually.

        Bulk mode: when ``target_parts`` is a single bare name (no dots,
        no scope keyword), AMX searches the catalog for ALL tables and
        columns matching that name across every schema, then offers a
        multi-select picker so you can apply one comment to many
        (schema, table) or (schema, table, column) pairs at once. The
        most common case: a column like ``customer_id`` appears in 50
        tables and you want the same description on all of them.
        """
        # Bulk-by-name mode: bare single token, no dots, not a scope keyword.
        if (
            len(target_parts) == 1
            and "." not in target_parts[0]
            and target_parts[0].lower() not in {"database", "db", "schema", "table", "column"}
        ):
            _run_bulk_edit_by_name(
                cfg,
                bare_name=target_parts[0],
                comment=comment,
                skip_confirm=yes,
                log_event=log_event,
            )
            return
        try:
            target = _resolve_explicit_edit_target(cfg, target_parts)
            if target is None:
                target = _run_edit_wizard(cfg)
        except Exception as exc:
            _report_manual_db_error("resolve the manual edit target", exc)
            return
        if target is None:
            return
        target_label = target.label
        value = _prompt_for_comment(target_label, comment)
        if value is None:
            return
        if not yes:
            try:
                confirmed = confirm(f"Write comment to {target_label}?", default=True)
            except (EOFError, KeyboardInterrupt):
                warn("Manual edit cancelled.")
                return
            if not confirmed:
                warn("Manual edit cancelled.")
                return
        try:
            target.writer(value)
        except Exception as exc:
            _report_manual_db_error("write the manual comment", exc)
            if log_event is not None:
                log_event(
                    event_type="manual_metadata_edit",
                    status="failed",
                    command="manual edit",
                    details={"target": target_label, "error": str(exc)},
                )
            return
        _sync_manual_comment_to_search_catalog(cfg, target, value)
        success(f"Updated {target_label}.")
        if log_event is not None:
            log_event(
                event_type="manual_metadata_edit",
                status="success",
                command="manual edit",
                details={"target": target_label, "scope": target.kind.value, "db_profile": target.profile},
            )

    @manual.command("monitor")
    @click.argument("schema", required=False)
    @pass_config
    def manual_monitor(cfg: AMXConfig, schema: str | None) -> None:
        """Show comment coverage for one schema or all user schemas."""
        from amx.db.connector import DatabaseConnector

        db = DatabaseConnector(cfg.db)
        try:
            rows = build_monitor_rows(cfg, db, schema)
        except Exception as exc:
            _report_manual_db_error("monitor metadata coverage", exc)
            return
        render_table(
            "Manual metadata coverage",
            ["Schema", "Asset comments", "Asset %", "Column comments", "Column %"],
            rows,
        )

    main.add_command(manual, "manual")
    return manual
