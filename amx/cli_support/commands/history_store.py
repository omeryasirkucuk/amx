"""``/history-store`` command — configure shared run-history collaboration.

Lives under the ``/db`` namespace (next to ``/db-profiles`` and
``/use-db``) because it manages a database resource. Bare
``/history-store`` opens an interactive picker — Status first, then
the actions you might want next based on whether shared mode is on.
Each menu entry maps to a Click subcommand so power users / scripts
can still invoke them directly (``amx db history-store status``,
``amx db history-store enable``, etc.).

Subcommands (also available individually, but the picker is the
primary user-facing surface):

* ``status`` — what's enabled, which profile, schema name, outbox depth.
* ``enable`` — interactive: pick a profile, run schema bootstrap, offer
  to migrate local history.
* ``disable`` — flip back to local-only. Does NOT delete shared rows.
* ``migrate-from-local`` — idempotent one-shot copy of local SQLite
  rows into the shared schema (only useful right after enabling).
* ``flush-pending`` — drain the dual-write outbox (replay queued
  shared writes that failed at write time).
* ``dump-ddl`` — print the schema-bootstrap DDL so a DBA can run it.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import click

from amx.config import AMXConfig
from amx.storage.factory import HistoryStoreBootstrapError, history_store
from amx.utils.console import (
    ask_choice,
    ask_multi_choice,
    confirm,
    error,
    heading,
    info,
    render_table,
    success,
    warn,
)
from amx.utils.logging import get_logger

log = get_logger("cli.history_store")

LogEvent = Callable[..., None]


# ── Action labels (also the picker entries) ───────────────────────────────
# Status comes first per UX request — the user almost always wants to
# know "what's currently configured?" before deciding the next action.
ACTION_STATUS = "Status — show shared-mode state and outbox depth"
ACTION_ENABLE = "Enable — bootstrap an AMX schema on a saved DB profile"
ACTION_DISABLE = "Disable — stop dual-writing (existing shared rows are kept)"
ACTION_PROFILES = "Profiles — pick which DB profiles participate in the shared store"
ACTION_MIGRATE = "Migrate from local — copy local SQLite history into the shared store"
ACTION_PULL = "Pull from shared — sync teammates' runs into your local SQLite cache"
ACTION_FLUSH = "Flush pending — retry queued shared writes that failed at write time"
ACTION_DUMP_DDL = "Dump DDL — print bootstrap SQL for a DBA to run by hand"
ACTION_CANCEL = "Cancel — exit without doing anything"


def _resolve_history_dual_store() -> Any | None:
    """Return the active store iff it is a DualWriteHistoryStore."""
    store = history_store()
    if store is None:
        return None
    # Avoid a hard import on dual_write at module load — most sessions
    # don't enable shared mode and importing the SQLAlchemy stack on
    # the cold path would slow startup. The duck-type check below
    # matches DualWriteHistoryStore exactly.
    if hasattr(store, "shared") and hasattr(store, "flush_pending"):
        return store
    return None


# ── Action implementations (shared between picker and subcommands) ────────


def _action_status(cfg: AMXConfig) -> None:
    heading("Shared run-history status")
    if not getattr(cfg, "history_store_enabled", False):
        info("Shared mode: [bold]disabled[/bold] (local SQLite only)")
        info("Run /history-store and pick Enable to turn it on.")
        return
    profile = cfg.history_store_profile or "(none)"
    schema = cfg.history_store_schema or "AMX"
    database = cfg.history_store_database or "(profile default)"
    # Backend for the chosen profile drives the label — "Catalog" for
    # Databricks, "Project" for BigQuery, otherwise "Database".
    db_cfg = cfg.db_profiles.get(profile) if profile in cfg.db_profiles else None
    backend = getattr(db_cfg, "backend", "") if db_cfg else ""
    if backend == "databricks":
        target_label = "Catalog"
    elif backend == "bigquery":
        target_label = "Project"
    else:
        target_label = "Database"
    info("Shared mode: [bold green]enabled[/bold green]")
    info(f"  Profile:  {profile}")
    info(f"  {target_label}: {database}")
    info(f"  Schema:   {schema}")
    store = _resolve_history_dual_store()
    if store is not None:
        try:
            depth = store.pending_count()
        except Exception:
            depth = -1
        if depth < 0:
            warn("  Outbox: (could not query)")
        elif depth == 0:
            info("  Outbox: empty (everything is in sync)")
        else:
            warn(f"  Outbox: {depth} pending shared writes — pick 'Flush pending' to retry them.")
    else:
        warn(
            "  Active store: local-only fallback. Bootstrap probably "
            "failed; check the connection / re-run Enable."
        )


def _action_enable(
    cfg: AMXConfig,
    *,
    log_event: LogEvent,
    profile_name: str | None = None,
    schema_name: str | None = None,
    database_name: str | None = None,
) -> None:
    if not cfg.db_profiles:
        error(
            "No DB profiles saved. Configure one with /db-profiles or "
            "/setup before enabling shared history."
        )
        return

    if profile_name is None:
        profile_name = ask_choice(
            "Pick a DB profile to host the AMX schema",
            sorted(cfg.db_profiles.keys()),
            default=cfg.active_db_profile or next(iter(cfg.db_profiles)),
        )
    # Empty string here means the user pressed Esc (or Enter on a
    # picker without a valid default) — treat as a clean wizard
    # cancel rather than erroring on "Unknown DB profile: ''".
    if not profile_name:
        return
    if profile_name not in cfg.db_profiles:
        error(f"Unknown DB profile: {profile_name!r}")
        return

    db_cfg = cfg.db_profiles[profile_name]
    # Local imports so this command does not pay the SQLAlchemy adapter
    # cost on the cold path.
    from amx.db.adapters import get_adapter
    from amx.storage.factory import apply_history_db_override
    from amx.storage.sqlalchemy_store import SQLAlchemyHistoryStore

    adapter = get_adapter(db_cfg)
    if not getattr(adapter.capabilities, "supports_shared_history", False):
        error(
            f"The {db_cfg.backend!r} backend does not support shared "
            "run-history. Supported backends: PostgreSQL, MySQL, MSSQL, "
            "Oracle, Redshift, Snowflake, Databricks, BigQuery."
        )
        return

    # Pick the database / catalog / project that will host the AMX
    # schema. A single profile (e.g. ``prod_pg``) often points at
    # multiple databases — the user may want AMX in a "tools" DB
    # rather than alongside production data. We always offer the
    # picker for backends where it applies; the helper returns the
    # picked override (empty string = use the profile's default).
    if database_name is None:
        database_name = _pick_history_database(cfg, db_cfg, adapter)

    # Apply the override on a copy of the DBConfig so the user's saved
    # profile is never mutated. The factory does the same at every
    # session start so subsequent runs reconnect to the same DB.
    target_cfg = apply_history_db_override(db_cfg, database_name) if database_name else db_cfg

    if schema_name is None or not schema_name.strip():
        schema_name = (cfg.history_store_schema or "AMX").strip() or "AMX"

    target_label = _format_db_target(target_cfg, database_name)
    info(
        f"Bootstrapping schema {schema_name!r} on profile {profile_name!r} "
        f"({db_cfg.backend}) → {target_label}…"
    )
    engine = get_adapter(target_cfg).create_engine()
    try:
        adapter.create_history_schema(engine, schema_name)
    except Exception as exc:
        error(f"Could not create schema {schema_name!r}: {exc}")
        ddl = adapter.create_history_schema_ddl(schema_name)
        warn("Hand the following DDL to a DBA and re-run Enable:")
        click.echo(ddl)
        return

    store = SQLAlchemyHistoryStore(engine=engine, schema=schema_name)
    try:
        store.init()
    except Exception as exc:
        error(f"Could not create AMX tables under {schema_name!r}: {exc}")
        return

    with cfg.transaction():
        cfg.history_store_enabled = True
        cfg.history_store_profile = profile_name
        cfg.history_store_schema = schema_name
        cfg.history_store_database = database_name or ""

    success(
        f"Shared run-history enabled. Profile: {profile_name!r} "
        f"({db_cfg.backend}). Target: {target_label}. Schema: {schema_name!r}."
    )
    info(
        "All future runs will be dual-written to local SQLite AND the "
        "shared backend. Reads still come from local SQLite."
    )

    # Detection: did a teammate already populate this shared store?
    # When yes, surface a compact summary of who wrote what and offer
    # to pull those runs down into the local cache so /history list
    # immediately shows team activity, not just this machine's.
    _maybe_offer_pull_on_enable(store)

    if confirm(
        "Migrate existing local runs UP into the shared store now? (Idempotent — safe to skip.)",
        default=True,
    ):
        _run_migration(cfg, store)
    log_event(event_type="history_store.enable", status="ok", command="/history-store enable")


def _maybe_offer_pull_on_enable(shared_store) -> None:
    """If the shared store already has rows from other hosts, prompt
    the user to pull them into local SQLite.

    Best-effort: any error walking the shared store is logged and the
    enable flow continues — we never block the user on this nicety.
    """
    import socket

    try:
        summary = shared_store.count_runs_by_other_hosts(exclude_hostname=socket.gethostname())
    except Exception as exc:
        log.debug("count_runs_by_other_hosts failed in enable detection: %s", exc)
        return
    if not summary:
        return

    info("")
    warn("This shared store already has runs from other team members:")
    rows: list[list[str]] = []
    for host, bucket in sorted(summary.items(), key=lambda kv: -kv[1]["count"]):
        users = ", ".join(bucket["users"]) if bucket["users"] else "?"
        last = bucket["last_run"]
        last_str = "?"
        if last is not None:
            try:
                ts = last
                if hasattr(ts, "tzinfo") and ts.tzinfo is None:
                    from datetime import timezone

                    ts = ts.replace(tzinfo=timezone.utc)
                last_str = ts.astimezone().strftime("%Y-%m-%d %H:%M")  # type: ignore[union-attr]
            except Exception:
                last_str = str(last)
        rows.append([host, users, str(bucket["count"]), last_str])
    render_table(
        "Existing runs in shared store",
        ["Host", "Users", "Runs", "Last activity"],
        rows,
    )
    if confirm(
        "Pull these runs into your local SQLite cache so /history list shows "
        "team activity? (Idempotent — safe to skip and re-run later.)",
        default=True,
    ):
        _run_pull_from_shared(shared_store)


def _run_pull_from_shared(shared_store) -> None:
    """Execute pull_shared_to_local with progress + result table."""
    from amx.storage.migration import pull_shared_to_local
    from amx.storage.sqlite_store import SQLiteHistoryStore

    store = history_store()
    if store is None:
        error("History store not initialised — cannot pull.")
        return
    local_path = (
        store.local.db_path  # type: ignore[union-attr]
        if hasattr(store, "local")
        else store.db_path  # type: ignore[union-attr]
    )
    local = SQLiteHistoryStore(local_path)
    info("Pulling teammates' runs into local SQLite cache…")
    try:
        stats = pull_shared_to_local(local=local, shared=shared_store)
    except Exception as exc:
        error(f"Pull failed: {exc}")
        return
    rows = [[table, str(count)] for table, count in stats.items()]
    render_table("Pull result", ["Table", "Rows pulled"], rows)
    if any(stats.values()):
        success("Pull complete. /history list will now include teammates' runs alongside your own.")
    else:
        info("Nothing new to pull (already in sync).")

    # Shared structural catalog: backfill local deep-sync results UP
    # (covers rows produced while the store was off), then pull the
    # team's catalog DOWN so columns + row counts arrive without a local
    # COUNT(*) pass. Best-effort — never blocks the enable/pull flow.
    try:
        from amx.storage.migration import (
            pull_catalog_to_local,
            push_catalog_to_shared,
        )

        pushed = push_catalog_to_shared(local, shared_store)
        pulled = pull_catalog_to_local(local, shared_store)
        if pushed or pulled:
            info(
                f"Catalog sync: pushed {pushed} local row(s) up, pulled {pulled} team row(s) down."
            )
    except Exception as exc:
        info(f"Catalog sync skipped (runs still synced): {exc}")


def _pick_history_database(cfg: AMXConfig, db_cfg, adapter) -> str:
    """Ask the user which database / catalog should host the AMX schema.

    The picker semantics differ per backend:

    * Databricks Unity Catalog → catalogs (``catalog.schema.table``).
      Listed via ``adapter.list_catalogs(engine)``.
    * BigQuery → projects. The project is already pinned on the
      profile (it has to be set before the engine works), so the
      picker is skipped and the empty string is returned (which means
      "use whatever is on the profile").
    * MySQL → schemas == databases. The schema name itself IS the
      database, so a separate database picker would be redundant.
      Skipped.
    * PostgreSQL / MSSQL / Oracle / Redshift / Snowflake → databases
      listed via ``adapter.list_databases(engine)``.

    Returns the picked override as a string (empty string to mean
    "no override"). On any discovery error we surface a warning and
    let the user proceed with the profile's default — never block.
    """
    backend = db_cfg.backend
    if backend in {"bigquery", "mysql"}:
        return ""

    is_databricks = backend == "databricks"
    label_kind = "catalog" if is_databricks else "database"
    current = (
        getattr(db_cfg, "catalog", "") if is_databricks else getattr(db_cfg, "database", "")
    ) or ""

    info(f"Discovering available {label_kind}s on profile (read-only)…")
    try:
        engine = adapter.create_engine()
        choices = adapter.list_catalogs(engine) if is_databricks else adapter.list_databases(engine)
    except Exception as exc:
        warn(
            f"Could not list {label_kind}s ({exc}). Using the profile's "
            f"current {label_kind}: {current or '(none)'}."
        )
        return current

    choices = sorted({str(c) for c in (choices or []) if c})
    if not choices:
        warn(
            f"No {label_kind}s reported by the server. Using the profile's "
            f"current {label_kind}: {current or '(none)'}."
        )
        return current

    default = current if current in choices else choices[0]
    # Annotate well-known maintenance/system targets so a user does not
    # accidentally pick an empty system DB on a fresh server. Today only
    # PostgreSQL surfaces such a target (``postgres``) — its adapter
    # already drops it when other DBs exist, so this label only fires
    # in the fresh-install fallback.
    descriptions: dict[str, str] = {}
    if backend == "postgresql" and "postgres" in choices:
        descriptions["postgres"] = "(maintenance DB — empty by default)"
    picked = ask_choice(
        f"Where should the AMX schema live? Pick a {label_kind}",
        choices,
        default=default,
        descriptions=descriptions or None,
    )
    return picked or default


def _format_db_target(target_cfg, database_name: str) -> str:
    """Render the user-facing label for the chosen database/catalog."""
    if not database_name:
        if target_cfg.backend == "databricks":
            return target_cfg.catalog or "(profile default)"
        if target_cfg.backend == "bigquery":
            return target_cfg.project or "(profile default)"
        return target_cfg.database or "(profile default)"
    return database_name


def _action_disable(cfg: AMXConfig, *, log_event: LogEvent) -> None:
    if not getattr(cfg, "history_store_enabled", False):
        info("Shared mode is already disabled.")
        return
    profile = cfg.history_store_profile or "(unknown)"
    schema = cfg.history_store_schema or "AMX"
    # Block disable when the outbox is non-empty so the user does not
    # silently strand work. Local rows already exist; the queued ops
    # are *shared* writes that haven't reached the team backend yet.
    store = _resolve_history_dual_store()
    if store is not None:
        try:
            depth = store.pending_count()
        except Exception:
            depth = 0
        if depth > 0:
            warn(
                f"Outbox has {depth} pending shared write(s) that have NOT "
                "reached the team backend yet."
            )
            if not confirm(
                "Disable anyway? The pending writes stay queued locally; you "
                "can re-enable and run 'Flush pending' later to deliver them.",
                default=False,
            ):
                info("Disable cancelled. Pick 'Flush pending' to drain first.")
                return
    info(
        "Disconnecting only flips this machine to local-only mode. The "
        f"shared schema {schema!r} on profile {profile!r} stays untouched — "
        "every existing shared row remains visible to your teammates, and "
        "you can re-enable from this same machine later."
    )
    if not confirm(
        f"Disable shared run-history (profile {profile!r}, schema {schema!r})? "
        "Existing shared rows will NOT be deleted.",
        default=False,
    ):
        return
    with cfg.transaction():
        cfg.history_store_enabled = False
    success("Shared run-history disabled (local-only on this machine).")
    info(
        "Shared rows untouched. To wipe the shared schema permanently you must "
        f"DROP SCHEMA {schema} on profile {profile!r} yourself — AMX never "
        "drops a team-shared schema automatically."
    )
    log_event(event_type="history_store.disable", status="ok", command="/history-store disable")


def _action_profiles(cfg: AMXConfig) -> None:
    """Multi-select wizard for ``history_store_profiles``.

    The primary profile (singular ``history_store_profile``) is always
    in scope; this picker manages the extras that get dual-written
    alongside it. Useful for users running AMX against several
    backends who want every profile's runs to land in one shared
    catalog.
    """
    heading("Shared history-store profiles")
    if not cfg.db_profiles:
        error("No DB profiles saved. Configure one with /db-profiles first.")
        return
    primary = (cfg.history_store_profile or "").strip()
    candidates = sorted(p for p in cfg.db_profiles if p and p != primary)
    if not candidates:
        info(
            "No additional profiles available — the only saved profile "
            "is already the primary history-store profile."
        )
        return
    current = sorted(
        p for p in (cfg.history_store_profiles or []) if p and p in cfg.db_profiles and p != primary
    )
    if primary:
        info(f"Primary profile (always included): {primary}")
    if current:
        info(f"Currently included extras: {', '.join(current)}")
    else:
        info("No extra profiles included yet.")
    picked = ask_multi_choice(
        "Pick the extra profiles to include (Enter cancels)",
        candidates,
    )
    if not picked:
        info("No change.")
        return
    deduped: list[str] = []
    seen: set[str] = set()
    for name in picked:
        if name in seen or name == primary:
            continue
        seen.add(name)
        deduped.append(name)
    with cfg.transaction():
        cfg.history_store_profiles = deduped
    success(f"Updated extra profiles: {', '.join(deduped) if deduped else '(none)'}")


def _action_migrate(cfg: AMXConfig) -> None:
    if not getattr(cfg, "history_store_enabled", False):
        error("Shared mode is disabled. Pick Enable first.")
        return
    store = _resolve_history_dual_store()
    if store is None:
        error("Shared store is not active. Pick Enable first.")
        return
    _run_migration(cfg, store.shared)


def _action_pull(cfg: AMXConfig) -> None:
    """Pull teammates' runs from the shared store into local SQLite.

    The mirror of _action_migrate (which pushes UP). Reads come from
    local SQLite in v0.12, so pulling is what makes ``/history list``
    surface team activity. Idempotent — re-running only inserts rows
    whose ``shared_uuid`` is not already present locally.
    """
    if not getattr(cfg, "history_store_enabled", False):
        error("Shared mode is disabled. Pick Enable first.")
        return
    store = _resolve_history_dual_store()
    if store is None:
        error("Shared store is not active. Pick Enable first.")
        return
    _run_pull_from_shared(store.shared)


def _action_flush(cfg: AMXConfig) -> None:
    store = _resolve_history_dual_store()
    if store is None:
        warn("Shared mode is disabled — nothing to flush.")
        return
    succeeded, remaining = store.flush_pending()
    if succeeded == 0 and remaining == 0:
        info("Outbox is empty.")
    else:
        success(
            f"Flushed {succeeded} pending writes. "
            f"{remaining} remaining (will retry next time you pick Flush pending)."
        )


def _action_dump_ddl(
    cfg: AMXConfig,
    *,
    profile_name: str | None = None,
    schema_name: str | None = None,
) -> None:
    target = profile_name or cfg.history_store_profile or cfg.active_db_profile or ""
    if not target or target not in cfg.db_profiles:
        error("No DB profile resolved. Specify --profile or configure one via /db-profiles.")
        return
    from amx.db.adapters import get_adapter

    db_cfg = cfg.db_profiles[target]
    adapter = get_adapter(db_cfg)
    schema = (schema_name or cfg.history_store_schema or "AMX").strip() or "AMX"
    click.echo(f"-- Backend: {db_cfg.backend}")
    click.echo(f"-- Schema:  {schema}")
    click.echo(adapter.create_history_schema_ddl(schema) + ";")
    schema_comment_ddl = adapter.history_schema_comment_ddl(schema)
    if schema_comment_ddl:
        click.echo(schema_comment_ddl + ";")
    click.echo("")
    click.echo("-- Tables (CREATE TABLE + COMMENT ON + CREATE INDEX)")
    try:
        click.echo(adapter.create_history_tables_ddl(schema))
    except Exception as exc:
        click.echo(f"-- (table DDL render failed: {exc})")


# ── Picker (the main user-facing surface) ─────────────────────────────────


def _run_picker(ctx: click.Context, cfg: AMXConfig, *, log_event: LogEvent) -> None:
    """Show a numbered menu of next actions; run the chosen one.

    The menu is short on purpose — six actions plus Cancel. Status is
    listed first so picking it (or pressing Enter on the default) is
    the safe answer when the user doesn't know what they want.
    Disable / Migrate / Flush only show when shared mode is on; Enable
    only shows when it's off — so the picker never offers an option
    that would error.

    Every menu choice runs its corresponding action and produces
    visible output (including Status, which always reprints the
    status panel) so picking a number never feels like a no-op.
    """
    enabled = bool(getattr(cfg, "history_store_enabled", False))
    # Picker ordering follows user-task frequency, with destructive
    # actions placed late so they're not adjacent to the default:
    #   1. Status (default — most-used, informational)
    #   2. Pull / Migrate / Flush (sync — the daily team workflow)
    #   3. Dump DDL (DBA helper — infrequent but read-only)
    #   4. Disable (administrative — moved away from #2 so it cannot
    #      be fat-fingered)
    #   5. Cancel
    # When shared mode is OFF the only meaningful next step is Enable
    # (also placed first after Status because the user opened the
    # picker to do exactly that).
    options: list[str] = [ACTION_STATUS]
    if enabled:
        options.append(ACTION_PROFILES)
        options.append(ACTION_PULL)
        options.append(ACTION_MIGRATE)
        options.append(ACTION_FLUSH)
        options.append(ACTION_DUMP_DDL)
        options.append(ACTION_DISABLE)
    else:
        options.append(ACTION_ENABLE)
        options.append(ACTION_DUMP_DDL)
    options.append(ACTION_CANCEL)

    picked = ask_choice(
        "What would you like to do?",
        options,
        default=ACTION_STATUS,
    )
    if picked == ACTION_STATUS:
        _action_status(cfg)
        return
    if picked == ACTION_ENABLE:
        _action_enable(cfg, log_event=log_event)
        return
    if picked == ACTION_DISABLE:
        _action_disable(cfg, log_event=log_event)
        return
    if picked == ACTION_PROFILES:
        _action_profiles(cfg)
        return
    if picked == ACTION_PULL:
        _action_pull(cfg)
        return
    if picked == ACTION_MIGRATE:
        _action_migrate(cfg)
        return
    if picked == ACTION_FLUSH:
        _action_flush(cfg)
        return
    if picked == ACTION_DUMP_DDL:
        _action_dump_ddl(cfg)
        return
    # ACTION_CANCEL or anything else — exit silently.


# ── Click registration ────────────────────────────────────────────────────


def register_history_store_commands(
    parent: click.Group,
    *,
    pass_config: Callable[[Callable[..., Any]], Callable[..., Any]],
    log_event: LogEvent,
) -> click.Group:
    """Attach ``/history-store`` to a parent Click group.

    Pass the ``/db`` group so the command lives at
    ``amx db history-store …`` and shows under the visual ``/db`` tab.
    """

    @parent.group("history-store", invoke_without_command=True)
    @click.pass_context
    def history_store_grp(ctx: click.Context) -> None:
        """Configure shared run-history (team collaboration)."""
        if ctx.invoked_subcommand is not None:
            return
        cfg = ctx.find_object(AMXConfig)
        if cfg is None:
            error("Config not loaded — run /setup or restart AMX.")
            return
        _run_picker(ctx, cfg, log_event=log_event)

    @history_store_grp.command("status")
    @pass_config
    def hs_status(cfg: AMXConfig) -> None:
        """Show whether shared mode is on and which profile hosts it."""
        _action_status(cfg)

    @history_store_grp.command("enable")
    @click.option(
        "--profile",
        "profile_name",
        default=None,
        help="DB profile that should host the AMX schema. Defaults to an interactive picker.",
    )
    @click.option(
        "--schema",
        "schema_name",
        default=None,
        help="Schema/database name where AMX tables live. Default 'AMX'.",
    )
    @click.option(
        "--database",
        "database_name",
        default=None,
        help=(
            "Override which database/catalog inside the profile hosts the "
            "AMX schema. Defaults to an interactive picker for backends "
            "that support multi-database listing (PG, MSSQL, Oracle, "
            "Redshift, Snowflake, Databricks). Skipped on MySQL "
            "(schema == database) and BigQuery (project pinned on profile)."
        ),
    )
    @pass_config
    def hs_enable(
        cfg: AMXConfig,
        profile_name: str | None,
        schema_name: str | None,
        database_name: str | None,
    ) -> None:
        """Bootstrap the AMX schema in a saved DB profile and enable shared mode."""
        _action_enable(
            cfg,
            log_event=log_event,
            profile_name=profile_name,
            schema_name=schema_name,
            database_name=database_name,
        )

    @history_store_grp.command("disable")
    @pass_config
    def hs_disable(cfg: AMXConfig) -> None:
        """Stop dual-writing to the shared store. Local SQLite continues normally."""
        _action_disable(cfg, log_event=log_event)

    @history_store_grp.command("profiles")
    @pass_config
    def hs_profiles(cfg: AMXConfig) -> None:
        """Pick which DB profiles participate in the shared history store.

        The "primary" profile (the one chosen at /history-store enable
        time) always participates; this picker manages the extra
        profiles whose runs are also dual-written.
        """
        _action_profiles(cfg)

    @history_store_grp.command("migrate-from-local")
    @pass_config
    def hs_migrate(cfg: AMXConfig) -> None:
        """Copy local SQLite history into the shared store (idempotent)."""
        _action_migrate(cfg)

    @history_store_grp.command("pull-from-shared")
    @pass_config
    def hs_pull(cfg: AMXConfig) -> None:
        """Pull teammates' runs from the shared store into local SQLite (idempotent)."""
        _action_pull(cfg)

    @history_store_grp.command("flush-pending")
    @pass_config
    def hs_flush(cfg: AMXConfig) -> None:
        """Replay queued shared-write retries from the local outbox."""
        _action_flush(cfg)

    @history_store_grp.command("dump-ddl")
    @click.option(
        "--profile",
        "profile_name",
        default=None,
        help="DB profile to template DDL for (defaults to the configured "
        "history-store profile, or the active analysis profile).",
    )
    @click.option(
        "--schema",
        "schema_name",
        default=None,
        help="Schema name to use in the DDL (default 'AMX').",
    )
    @pass_config
    def hs_dump_ddl(cfg: AMXConfig, profile_name: str | None, schema_name: str | None) -> None:
        """Print the schema-bootstrap DDL so a DBA can run it by hand."""
        _action_dump_ddl(cfg, profile_name=profile_name, schema_name=schema_name)

    return history_store_grp


def _run_migration(cfg: AMXConfig, shared) -> None:
    """Execute the local→shared migration with progress + result table."""
    from amx.storage.migration import migrate_local_to_shared
    from amx.storage.sqlite_store import SQLiteHistoryStore

    # Build a thin local handle bound to the same db_path the running
    # CLI uses. We do not reuse the global singleton because the
    # singleton may already be wrapped in DualWriteHistoryStore — the
    # migration takes the raw SQLite store directly.
    local_path = (
        history_store().local.db_path  # type: ignore[union-attr]
        if hasattr(history_store(), "local")
        else history_store().db_path  # type: ignore[union-attr]
    )
    local = SQLiteHistoryStore(local_path)

    info(f"Migrating local history → shared (schema {shared.schema!r})…")
    try:
        stats = migrate_local_to_shared(local=local, shared=shared)
    except HistoryStoreBootstrapError as exc:
        error(f"Migration failed: {exc}")
        return
    except Exception as exc:
        error(f"Migration failed: {exc}")
        return

    rows = [[table, str(count)] for table, count in stats.items()]
    render_table("Migration result", ["Table", "Rows copied"], rows)
    success("Migration complete (idempotent — safe to re-run).")
