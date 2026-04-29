"""Top-level setup, database, and config command registration."""

from __future__ import annotations

import sys
from collections.abc import Callable
from dataclasses import replace

import click

from amx.config import AMXConfig, DISABLED_PROFILE
from amx.utils.console import ask, confirm, error, heading, info, render_table, step_spinner, success, warn
from amx.utils.live_commands import command_display

InteractiveDbBlock = Callable[[object], object]
InteractiveLlmBlock = Callable[[object], object]


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
                connected = db.test_connection()
        if connected:
            success(f"Database connection successful! (backend: {cfg.db.backend})")
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
            warn("LLM test failed — you can reconfigure later with `amx setup`.")
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
                    duplicate = f"This path is already in profile {name!r}: {path}. Add duplicate anyway?"
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

    @main.group()
    def db() -> None:
        """Database inspection and profiling commands."""

    @db.command("connect")
    @click.pass_obj
    def db_connect(cfg: AMXConfig) -> None:
        """Test database connectivity."""
        from amx.db.connector import DatabaseConnector
        from amx.cli_support.commands.db import databricks_connect_with_recovery

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
                if getattr(cfg.db, "tls_no_verify", False):
                    warn("Active Databricks profile now uses TLS no-verify. Replace this with a trusted CA bundle when possible.")
                success(f"Connected to [{cfg.db.backend}] {cfg.db.display_summary}")
                return
            error("Connection failed.")
            sys.exit(1)

        db_conn = DatabaseConnector(cfg.db)
        with command_display(mode="db-connect", provider=cfg.llm.provider, model=cfg.llm.model):
            with step_spinner("Testing database connection..."):
                connected = db_conn.test_connection()
        if connected:
            success(f"Connected to [{cfg.db.backend}] {cfg.db.display_summary}")
        else:
            error("Connection failed.")
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
    @click.pass_obj
    def db_schemas(cfg: AMXConfig) -> None:
        """List available schemas."""
        from amx.db.connector import DatabaseConnector

        db_conn = DatabaseConnector(cfg.db)
        with command_display(mode="db-schemas", provider=cfg.llm.provider, model=cfg.llm.model):
            with step_spinner("Listing schemas"):
                schemas = db_conn.list_schemas()
        render_table("Schemas", ["Schema Name"], [[s] for s in schemas])

    @db.command("tables")
    @click.argument("schema")
    @click.pass_obj
    def db_tables(cfg: AMXConfig, schema: str) -> None:
        """List all assets (tables, views, materialized views) in a schema."""
        from amx.db.connector import DatabaseConnector

        db_conn = DatabaseConnector(cfg.db)
        with command_display(schema=schema, mode="db-tables", provider=cfg.llm.provider, model=cfg.llm.model):
            with step_spinner(f"Listing assets in {schema}"):
                assets = db_conn.list_assets(schema)
        render_table(
            f"Assets in {schema}",
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
        with command_display(schema=schema, table=table, mode="db-profile", provider=cfg.llm.provider, model=cfg.llm.model):
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
        info(f"Session context: schema={cfg.current_schema or '-'} table={cfg.current_table or '-'}")
        info(
            f"Active LLM profile: {cfg.active_llm_profile} → "
            f"{cfg.llm.provider}/{cfg.llm.model} [{cfg.llm.language or 'english'} metadata]"
        )
        if cfg.llm_profiles:
            info("LLM profiles: " + ", ".join(sorted(cfg.llm_profiles.keys())))
        doc_prof = "(none)" if cfg.active_doc_profile == DISABLED_PROFILE else (cfg.active_doc_profile or "-")
        info(f"Active document profile: {doc_prof}")
        info(f"Document paths (active): {cfg.effective_doc_paths() or 'none'}")
        code_prof = "(none)" if cfg.active_code_profile == DISABLED_PROFILE else (cfg.active_code_profile or "-")
        info(f"Active codebase profile: {code_prof}")
        info(f"Codebase paths (active): {cfg.effective_code_paths() or 'none'}")
        info(f"Selected schemas: {cfg.selected_schemas or 'all'}")
