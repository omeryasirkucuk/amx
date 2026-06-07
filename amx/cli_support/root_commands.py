"""Top-level setup, database, and config command registration."""

from __future__ import annotations

import sys
from collections.abc import Callable
from dataclasses import replace

import click

from amx.config import DISABLED_PROFILE, AMXConfig
from amx.utils.console import (
    ask,
    confirm,
    error,
    heading,
    info,
    render_table,
    step_spinner,
    success,
    warn,
)
from amx.utils.live_commands import command_display

InteractiveDbBlock = Callable[[object], object]
InteractiveLlmBlock = Callable[[object], object]


def _catalog_distinct_schemas(profile: str, database: str | None) -> list[str] | None:
    """Cache-first read for ``/db schemas``. Returns ``None`` when the
    catalog has nothing recorded for the profile so the caller can
    fall back to the live connector. Matches the Studio sidebar's
    cache-first contract in ``amx/web/routers/live_db.py``."""
    try:
        from amx.search.catalog import SearchCatalog

        cat = SearchCatalog.from_history_store()
    except Exception:
        return None
    if cat is None:
        return None
    try:
        rows = cat.fetch_distinct_schemas(profile, database_name=database)
    except Exception:
        return None
    if not rows:
        return None
    return [str(r.get("name") or "") for r in rows if r.get("name")]


def _catalog_distinct_tables(profile: str, schema: str, database: str | None) -> list[str] | None:
    """Cache-first read for ``/db tables <schema>``. Returns ``None``
    on cache miss so the caller falls back to the live connector."""
    try:
        from amx.search.catalog import SearchCatalog

        cat = SearchCatalog.from_history_store()
    except Exception:
        return None
    if cat is None:
        return None
    try:
        rows = cat.fetch_distinct_tables_in_schema(profile, schema, database_name=database)
    except Exception:
        return None
    if not rows:
        return None
    return [str(r.get("name") or "") for r in rows if r.get("name")]


def register_root_commands(
    main: click.Group,
    *,
    interactive_db_block: InteractiveDbBlock,
    interactive_llm_block: InteractiveLlmBlock,
) -> None:
    """Attach setup, db, and config commands to the main Click group."""

    @main.command()
    @click.pass_obj
    def setup(cfg: AMXConfig) -> None:
        """Interactive first-time setup wizard."""
        heading("AMX Setup Wizard")

        info("Step 1/3 — Database Connection")
        cfg.db = interactive_db_block(cfg.db)

        if not cfg.active_db_profile:
            cfg.active_db_profile = "default"
        cfg.upsert_db_profile(cfg.active_db_profile, cfg.db)
        cfg.apply_active_db_profile()

        from amx.db.connector import DatabaseConnector

        db = DatabaseConnector(cfg.db)
        with command_display(mode="setup-db", provider=cfg.llm.provider, model=cfg.llm.model):
            with step_spinner("Testing database connection..."):
                result = db.test_connection_result()
        if result.ok:
            success(f"Database connection successful! (backend: {cfg.db.backend})")
        else:
            # Surface the categorised cause (wrong password / SSL / timeout /
            # missing driver / unknown database) instead of a fixed
            # credentials-only hint — the LLM step below already shows its
            # real error via test_result().
            if result.message:
                error(f"Database connection failed: {result.message}")
            else:
                error("Database connection failed. Check credentials and try again.")
            if not confirm("Continue anyway?", default=False):
                sys.exit(1)

        info("Step 2/3 — AI Model Configuration")
        cfg.llm = interactive_llm_block(cfg.llm)
        cfg.active_llm_profile = cfg.active_llm_profile or "default"
        cfg.upsert_llm_profile(cfg.active_llm_profile, replace(cfg.llm))
        cfg.apply_active_llm_profile()

        from amx.llm.provider import LLMProvider

        llm = LLMProvider(cfg.llm)
        with command_display(mode="setup-llm", provider=cfg.llm.provider, model=cfg.llm.model):
            with step_spinner("Testing LLM connection..."):
                llm_result = llm.test_result()
        if llm_result.ok:
            success("LLM connection successful!")
        else:
            warn("LLM test failed — reconfigure under `/llm` or rerun `/setup`.")
            if llm_result.message:
                info(f"Cause: {llm_result.message}")

        info("Step 3/3 — Optional Data Sources (named profiles)")
        if confirm("Add a document profile for RAG?", default=False):
            from amx.docs.scanner import test_source_reachable

            name = ask("Profile name", default="default")
            existing = list(cfg.doc_profiles.get(name, []))
            new_paths: list[str] = []
            while True:
                prompt = "Document path" if not new_paths else "Another path (empty to finish)"
                path = ask(prompt, default="")
                if not path:
                    break
                if path in existing or path in new_paths:
                    duplicate = (
                        f"This path is already in profile {name!r}: {path}. Add duplicate anyway?"
                    )
                    if not confirm(duplicate, default=False):
                        continue
                try:
                    test_source_reachable(path)
                    success(f"Source reachable: {path}")
                    new_paths.append(path)
                except Exception as exc:
                    error(f"Source not reachable: {path}")
                    warn(str(exc))
                if not confirm("Add another path?", default=False):
                    break
            if new_paths:
                cfg.upsert_doc_profile(name, existing + new_paths)
                cfg.active_doc_profile = name
            else:
                warn("Skipping document profile — no valid sources were provided.")

        if confirm("Add a codebase profile?", default=False):
            from amx.codebase.analyzer import test_codebase_path_reachable

            name = ask("Profile name", default="default")
            path = ask("Codebase path (local dir or Git URL)", default="")
            if path:
                try:
                    test_codebase_path_reachable(path)
                    success(f"Codebase reachable: {path}")
                    cfg.upsert_code_profile(name, path)
                    cfg.active_code_profile = name
                except Exception as exc:
                    error(f"Codebase not reachable: {path}")
                    warn(str(exc))

        saved = cfg.save()
        success(f"Configuration saved to {saved}")

    @main.command()
    @click.option(
        "--port",
        type=int,
        default=None,
        help="Listen port (defaults to 47821, falls back to a free ephemeral port).",
    )
    @click.option(
        "--no-open",
        "no_open",
        is_flag=True,
        default=False,
        help="Skip auto-opening the browser (useful in headless environments).",
    )
    @click.option(
        "--embedded",
        "embedded",
        is_flag=True,
        default=False,
        # Deliberately visible in --help: IDE integrations probe
        # `amx studio --help` for this flag to detect whether the
        # installed AMX supports the embedded host mode before
        # passing it (older versions reject unknown options).
        help=(
            "Relax framing headers so an IDE host can render Studio "
            "inside a webview iframe. Set by IDE integrations, not users."
        ),
    )
    @click.pass_obj
    def studio(cfg: AMXConfig, port: int | None, no_open: bool, embedded: bool) -> None:
        """Launch AMX Studio (local web UI) and open it in your browser."""
        try:
            from amx.web import launch_studio
        except ImportError as exc:  # pragma: no cover - belt-and-braces; web extras are core
            error(
                "FastAPI / uvicorn aren't available. "
                "Run `pip install --upgrade amx-cli` to pull in the AMX Studio dependencies. "
                f"Underlying import error: {exc}"
            )
            return
        launch_studio(cfg, port=port, open_browser=not no_open, embedded=embedded)

    @main.group()
    def db() -> None:
        """Database inspection, profiling, and shared run-history commands."""

    # Cached on the wrapper so the caller in cli.py can attach more
    # subcommands (like /history-store) under the same /db group.
    register_root_commands._db_group = db  # type: ignore[attr-defined]

    @db.command("connect")
    @click.pass_obj
    def db_connect(cfg: AMXConfig) -> None:
        """Test database connectivity."""
        from amx.cli_support.commands.db import databricks_connect_with_recovery
        from amx.db.connector import DatabaseConnector

        info(f"Testing [{cfg.db.backend}] connection to {cfg.db.display_summary} ...")
        if cfg.db.backend == "databricks":

            def _attempt(db_cfg):
                db_conn = DatabaseConnector(db_cfg)
                result = db_conn.test_connection_result()
                return result.ok, result.message

            with command_display(mode="db-connect", provider=cfg.llm.provider, model=cfg.llm.model):
                with step_spinner("Testing database connection..."):
                    connected, attempts = databricks_connect_with_recovery(cfg, _attempt)
            for attempt in attempts:
                if attempt.ok:
                    success(f"Connect stage passed: {attempt.label}")
                else:
                    warn(f"Connect stage failed: {attempt.label}")
                    if attempt.detail:
                        info(f"Cause: {attempt.detail}")
            if connected:
                active_ca = str(getattr(cfg.db, "tls_trusted_ca_file", "") or "").strip()
                if active_ca:
                    info(f"Active Databricks trusted CA bundle: {active_ca}")
                # Catalog picker — Databricks Unity Catalog needs the
                # user to pin a catalog before any list_schemas /
                # list_tables runs, otherwise downstream queries hit
                # the SQLAlchemy fallback path and fail with
                # ``SHOW TABLES FROM None.<schema>``. Hoisting the
                # picker here means /connect is the canonical place
                # to lock the catalog for the rest of the session;
                # /run, /ask, /edit and friends inherit it.
                try:
                    from amx.cli_support.catalog_picker import ensure_hierarchy_resolved

                    db_for_pick = DatabaseConnector(cfg.db)
                    ensure_hierarchy_resolved(db_for_pick)
                except Exception as _exc:
                    pass
                if getattr(cfg.db, "tls_no_verify", False):
                    warn(
                        "Active Databricks profile now uses TLS no-verify. Replace this with a trusted CA bundle when possible."
                    )
                success(f"Connected to [{cfg.db.backend}] {cfg.db.display_summary}")
                return
            error("Connection failed.")
            sys.exit(1)

        db_conn = DatabaseConnector(cfg.db)
        with command_display(mode="db-connect", provider=cfg.llm.provider, model=cfg.llm.model):
            with step_spinner("Testing database connection..."):
                result = db_conn.test_connection_result()
        if result.ok:
            success(f"Connected to [{cfg.db.backend}] {cfg.db.display_summary}")
        else:
            # Surface the categorised, actionable cause (wrong password /
            # SSL / timeout / missing driver / unknown database) that
            # test_connection_result already built. The bool
            # test_connection() path threw it away and printed a bare
            # "Connection failed.", so every non-Databricks backend hit a
            # dead end while Databricks showed the cause.
            error(
                f"Connection failed: {result.message}" if result.message else "Connection failed."
            )
            sys.exit(1)

    @db.command("tls")
    @click.argument("mode", required=False)
    @click.argument("ca_path", required=False)
    @click.pass_obj
    def db_tls(cfg: AMXConfig, mode: str | None, ca_path: str | None) -> None:
        """Show or update Databricks TLS settings for the active profile."""
        from amx.cli_support.commands.db import cmd_tls

        rest = [value for value in (mode, ca_path) if value is not None]
        cmd_tls(cfg, rest)

    @db.command("schemas")
    @click.option(
        "--live",
        is_flag=True,
        default=False,
        help="Bypass the catalog cache and re-list against the live database.",
    )
    @click.pass_obj
    def db_schemas(cfg: AMXConfig, live: bool) -> None:
        """List available schemas (cache-first, ``--live`` to force fresh)."""
        active_profile = (cfg.active_db_profile or "default").strip() or "default"
        scope = (cfg.db.database or "").strip() or (cfg.db.catalog or "").strip() or None
        if not live:
            cached = _catalog_distinct_schemas(active_profile, scope)
            if cached:
                with command_display(
                    mode="db-schemas",
                    provider=cfg.llm.provider,
                    model=cfg.llm.model,
                ):
                    render_table(
                        f"Schemas (catalog cache · profile {active_profile})",
                        ["Schema Name"],
                        [[s] for s in cached],
                    )
                return
        from amx.db.connector import DatabaseConnector

        db_conn = DatabaseConnector(cfg.db)
        with command_display(mode="db-schemas", provider=cfg.llm.provider, model=cfg.llm.model):
            with step_spinner("Listing schemas (live DB)"):
                schemas = db_conn.list_schemas()
        render_table("Schemas (live)", ["Schema Name"], [[s] for s in schemas])

    @db.command("tables")
    @click.argument("schema")
    @click.option(
        "--live",
        is_flag=True,
        default=False,
        help="Bypass the catalog cache and re-list against the live database.",
    )
    @click.pass_obj
    def db_tables(cfg: AMXConfig, schema: str, live: bool) -> None:
        """List assets in a schema (cache-first, ``--live`` to force fresh)."""
        active_profile = (cfg.active_db_profile or "default").strip() or "default"
        scope = (cfg.db.database or "").strip() or (cfg.db.catalog or "").strip() or None
        if not live:
            cached = _catalog_distinct_tables(active_profile, schema, scope)
            if cached:
                with command_display(
                    schema=schema,
                    mode="db-tables",
                    provider=cfg.llm.provider,
                    model=cfg.llm.model,
                ):
                    render_table(
                        f"Assets in {schema} (catalog cache · profile {active_profile})",
                        ["Name", "Type"],
                        [[name, "table"] for name in cached],
                    )
                return
        from amx.db.connector import DatabaseConnector

        db_conn = DatabaseConnector(cfg.db)
        with command_display(
            schema=schema, mode="db-tables", provider=cfg.llm.provider, model=cfg.llm.model
        ):
            with step_spinner(f"Listing assets in {schema} (live DB)"):
                assets = db_conn.list_assets(schema)
        render_table(
            f"Assets in {schema} (live)",
            ["Name", "Type"],
            [[name, kind.label] for name, kind in assets],
        )

    @db.command("profile")
    @click.argument("schema")
    @click.argument("table")
    @click.pass_obj
    def db_profile(cfg: AMXConfig, schema: str, table: str) -> None:
        """Profile a specific table (stats, types, samples)."""
        from amx.db.connector import DatabaseConnector

        db_conn = DatabaseConnector(cfg.db)
        with command_display(
            schema=schema,
            table=table,
            mode="db-profile",
            provider=cfg.llm.provider,
            model=cfg.llm.model,
        ):
            with step_spinner(f"Profiling {schema}.{table}"):
                profile = db_conn.profile_table(schema, table)
        rows = [
            [
                col.name,
                col.dtype,
                str(col.null_count),
                str(col.distinct_count),
                str(col.min_val)[:30],
                str(col.max_val)[:30],
                ", ".join(str(sample)[:20] for sample in col.samples[:3]),
            ]
            for col in profile.columns
        ]
        render_table(
            f"{schema}.{table} ({profile.row_count} rows)",
            ["Column", "Type", "Nulls", "Distinct", "Min", "Max", "Samples"],
            rows,
        )

    @main.command("config")
    @click.pass_obj
    def show_config(cfg: AMXConfig) -> None:
        """Display current configuration."""
        info(
            f"Active DB profile: {cfg.active_db_profile} → "
            f"[{cfg.db.backend}] {cfg.db.display_summary}"
        )
        if cfg.db_profiles:
            info("DB profiles: " + ", ".join(sorted(cfg.db_profiles.keys())))
        max_rows = int(getattr(cfg.db, "profiling_max_rows", 1_000_000) or 0)
        max_label = "off" if max_rows <= 0 else f"{max_rows:,}"
        info(
            f"Profiling: mode={cfg.db.profiling_mode}, "
            f"max_full_scan_rows={max_label}, sample_size={cfg.db.profiling_sample_size}"
        )
        info(
            f"Session context: schema={cfg.current_schema or '-'} table={cfg.current_table or '-'}"
        )
        info(
            f"Active LLM profile: {cfg.active_llm_profile} → "
            f"{cfg.llm.provider}/{cfg.llm.model} [{cfg.llm.language or 'english'} metadata]"
        )
        if cfg.llm_profiles:
            info("LLM profiles: " + ", ".join(sorted(cfg.llm_profiles.keys())))
        doc_prof = (
            "(none)"
            if cfg.active_doc_profile == DISABLED_PROFILE
            else (cfg.active_doc_profile or "-")
        )
        info(f"Active document profile: {doc_prof}")
        info(f"Document paths (active): {cfg.effective_doc_paths() or 'none'}")
        code_prof = (
            "(none)"
            if cfg.active_code_profile == DISABLED_PROFILE
            else (cfg.active_code_profile or "-")
        )
        info(f"Active codebase profile: {code_prof}")
        info(f"Codebase paths (active): {cfg.effective_code_paths() or 'none'}")
        info(f"Selected schemas: {cfg.selected_schemas or 'all'}")
