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
    confirm,
    error,
    heading,
    info,
    render_table,
    success,
    warn,
)

LogEvent = Callable[..., None]


# ── Action labels (also the picker entries) ───────────────────────────────
# Status comes first per UX request — the user almost always wants to
# know "what's currently configured?" before deciding the next action.
ACTION_STATUS = "Status — show shared-mode state and outbox depth"
ACTION_ENABLE = "Enable — bootstrap an AMX schema on a saved DB profile"
ACTION_DISABLE = "Disable — stop dual-writing (existing shared rows are kept)"
ACTION_MIGRATE = "Migrate from local — copy local SQLite history into the shared store"
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
    info("Shared mode: [bold green]enabled[/bold green]")
    info(f"  Profile: {profile}")
    info(f"  Schema:  {schema}")
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
    if profile_name not in cfg.db_profiles:
        error(f"Unknown DB profile: {profile_name!r}")
        return

    db_cfg = cfg.db_profiles[profile_name]
    # Local imports so this command does not pay the SQLAlchemy adapter
    # cost on the cold path.
    from amx.db.adapters import get_adapter
    from amx.storage.sqlalchemy_store import SQLAlchemyHistoryStore

    adapter = get_adapter(db_cfg)
    if not getattr(adapter.capabilities, "supports_shared_history", False):
        error(
            f"The {db_cfg.backend!r} backend does not support shared "
            "run-history. Supported backends: PostgreSQL, MySQL, MSSQL, "
            "Oracle, Redshift, Snowflake, Databricks, BigQuery."
        )
        return

    if schema_name is None or not schema_name.strip():
        schema_name = (cfg.history_store_schema or "AMX").strip() or "AMX"

    info(f"Bootstrapping schema {schema_name!r} on profile {profile_name!r} ({db_cfg.backend})…")
    engine = adapter.create_engine()
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

    success(
        f"Shared run-history enabled. Profile: {profile_name!r} "
        f"({db_cfg.backend}). Schema: {schema_name!r}."
    )
    info(
        "All future runs will be dual-written to local SQLite AND the "
        "shared backend. Reads still come from local SQLite."
    )
    if confirm(
        "Migrate existing local runs into the shared store now? (Idempotent — safe to skip.)",
        default=True,
    ):
        _run_migration(cfg, store)
    log_event(event_type="history_store.enable", status="ok", command="/history-store enable")


def _action_disable(cfg: AMXConfig, *, log_event: LogEvent) -> None:
    if not getattr(cfg, "history_store_enabled", False):
        info("Shared mode is already disabled.")
        return
    if not confirm(
        "Disable shared run-history? Existing shared rows are NOT deleted; "
        "you can re-enable later from this same machine.",
        default=False,
    ):
        return
    with cfg.transaction():
        cfg.history_store_enabled = False
    success("Shared run-history disabled.")
    info("Restart AMX (or run any command) for the change to take effect.")
    log_event(event_type="history_store.disable", status="ok", command="/history-store disable")


def _action_migrate(cfg: AMXConfig) -> None:
    if not getattr(cfg, "history_store_enabled", False):
        error("Shared mode is disabled. Pick Enable first.")
        return
    store = _resolve_history_dual_store()
    if store is None:
        error("Shared store is not active. Pick Enable first.")
        return
    _run_migration(cfg, store.shared)


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
    click.echo(adapter.create_history_schema_ddl(schema))


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
    options: list[str] = [ACTION_STATUS]
    if enabled:
        options.append(ACTION_DISABLE)
        options.append(ACTION_MIGRATE)
        options.append(ACTION_FLUSH)
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
    @pass_config
    def hs_enable(cfg: AMXConfig, profile_name: str | None, schema_name: str | None) -> None:
        """Bootstrap the AMX schema in a saved DB profile and enable shared mode."""
        _action_enable(
            cfg,
            log_event=log_event,
            profile_name=profile_name,
            schema_name=schema_name,
        )

    @history_store_grp.command("disable")
    @pass_config
    def hs_disable(cfg: AMXConfig) -> None:
        """Stop dual-writing to the shared store. Local SQLite continues normally."""
        _action_disable(cfg, log_event=log_event)

    @history_store_grp.command("migrate-from-local")
    @pass_config
    def hs_migrate(cfg: AMXConfig) -> None:
        """Copy local SQLite history into the shared store (idempotent)."""
        _action_migrate(cfg)

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
