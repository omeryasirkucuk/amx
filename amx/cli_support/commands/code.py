"""Code namespace commands for the AMX interactive CLI."""

from __future__ import annotations

import datetime
import json
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import click

from amx.config import AMXConfig
from amx.utils.console import (
    console,
    error,
    heading,
    info,
    render_table,
    render_token_summary,
    step_spinner,
    success,
    warn,
)
from amx.utils.token_tracker import tracker as token_tracker

if TYPE_CHECKING:
    from amx.codebase.analyzer import CodebaseReport


def _build_scan_progress() -> tuple[dict[str, object | None], callable]:
    progress_state: dict[str, object | None] = {"obj": None, "task": None}

    def _scan_cb(action: str, value: object) -> None:
        from rich.progress import BarColumn, MofNCompleteColumn, Progress, TextColumn, TimeElapsedColumn

        if action == "__total__":
            progress = Progress(
                TextColumn("[info]{task.description}[/info]"),
                BarColumn(),
                MofNCompleteColumn(),
                TimeElapsedColumn(),
                console=console,
                transient=True,
            )
            progress.start()
            progress_state["obj"] = progress
            progress_state["task"] = progress.add_task("Scanning files", total=int(value))  # type: ignore[arg-type]
        elif action == "__advance__" and progress_state["obj"] is not None:
            progress = progress_state["obj"]
            task = progress_state["task"]
            progress.update(task, description=f"Scanning: {value}")
            progress.advance(task)

    return progress_state, _scan_cb


def _render_code_report_summary(report: CodebaseReport) -> None:
    ref_count = sum(len(v) for v in report.references.values())
    ext_count = sum(len(v) for v in report.external_mentions.values())
    info(
        f"Scanned {report.scanned_files}/{report.total_files} files — "
        f"{ref_count} catalog hits, {ext_count} external-style mentions"
    )

    if report.total_files == 0:
        warn(
            "No source files matched (.py, .sql, .java, .ts, …). "
            "Check the folder/repo root contains files with those extensions."
        )
    if report.references:
        rows = [
            [asset, str(len(refs)), refs[0].file if refs else ""]
            for asset, refs in sorted(report.references.items())
        ]
        render_table("Asset references found", ["Asset", "Ref Count", "Example File"], rows[:30])
    else:
        warn("No catalog-style references found.")
    if report.external_mentions:
        rows = [
            [asset, str(len(refs)), refs[0].file if refs else ""]
            for asset, refs in sorted(report.external_mentions.items())[:20]
        ]
        render_table(
            "Other identifiers (not in DB table list)",
            ["Token", "Ref Count", "Example File"],
            rows,
        )


def register_code_commands(
    main: click.Group,
    *,
    finalize_scope,
) -> None:
    """Attach `/code` namespace commands to the main Click group."""

    @main.group()
    def code() -> None:
        """Codebase scanning, analysis, and code profile management."""

    @code.command("scan")
    @click.argument("path", required=False, default=None)
    @click.option(
        "--schema",
        "-s",
        default=None,
        help="Schema to match against (defaults to session current_schema from config).",
    )
    @click.option(
        "--code-profile",
        default=None,
        help="Use this named codebase profile path when no path argument is given.",
    )
    @click.pass_obj
    def code_scan_cmd(
        cfg: AMXConfig,
        path: str | None,
        schema: str | None,
        code_profile: str | None,
    ) -> None:
        """Scan a codebase for table/column references, save results to cache, and build the semantic code index."""
        from amx.codebase.analyzer import analyze_codebase
        from amx.codebase.cache import save_cached_report
        from amx.db.connector import DatabaseConnector

        schema_name = schema or cfg.current_schema
        if not schema_name:
            error(
                "Missing schema: use --schema sap_s6p or set context with `/db` then `/schema …` in session."
            )
            sys.exit(1)

        try:
            resolved = cfg.resolve_code_path((code_profile or "").strip() or None, (path or "").strip() or None)
        except KeyError as exc:
            error(str(exc))
            sys.exit(1)
        if not resolved:
            error(
                "No codebase path given and no matching profile. "
                "Run `/code` then `/add-code-profile`, or `/code-scan --code-profile NAME`, or pass a path."
            )
            sys.exit(1)
        if not (path or "").strip():
            if (code_profile or "").strip():
                info(f"Using codebase profile {(code_profile or '').strip()!r}: {resolved}")
            else:
                info(f"Using active codebase profile path: {resolved}")

        profile_nm = ((code_profile or "").strip() or cfg.active_code_profile or "default").strip() or "default"

        db = DatabaseConnector(cfg.db)
        all_assets = db.list_assets(schema_name)
        tables = [name for name, _ in all_assets]
        catalog = frozenset(t.lower() for t in tables)

        column_names: list[str] = []
        seen_col: set[str] = set()
        with step_spinner(f"Collecting column names from {len(tables)} asset(s)"):
            for table_name in tables:
                for column in db.list_column_profiles(schema_name, table_name):
                    key = column.name.lower()
                    if key not in seen_col:
                        seen_col.add(key)
                        column_names.append(column.name)
                    if len(column_names) >= 400:
                        break
                if len(column_names) >= 400:
                    break

        info(
            f"Scanning {resolved} for references to {len(tables)} tables and "
            f"{len(column_names)} columns..."
        )
        progress_state, scan_cb = _build_scan_progress()
        try:
            report = analyze_codebase(
                resolved,
                tables,
                column_names=column_names,
                known_catalog_tables=catalog,
                index_semantic=True,
                progress_callback=scan_cb,
            )
        except Exception as exc:
            error(str(exc))
            sys.exit(1)
        finally:
            if progress_state["obj"] is not None:
                progress_state["obj"].stop()

        try:
            save_cached_report(
                profile_name=profile_nm,
                source_path=resolved,
                schema=schema_name,
                tables=tables,
                column_names=column_names,
                report=report,
            )
            success(f"Saved scan results to cache (profile {profile_nm!r})")
        except Exception as exc:
            warn(f"Could not save cache: {exc}")

        _render_code_report_summary(report)
        info("Results saved. Next `/run` will use them from cache (use `/code-refresh` to clear).")

    @code.command("refresh")
    @click.option(
        "--code-profile",
        default=None,
        help="Invalidate cache for this profile's path (default: active profile).",
    )
    @click.pass_obj
    def code_refresh_cmd(cfg: AMXConfig, code_profile: str | None) -> None:
        """Clear persisted codebase scan cache and the semantic ``amx_code`` Chroma index."""
        from amx.codebase.cache import invalidate_cache
        from amx.codebase.code_rag import delete_code_collection

        try:
            code_path = cfg.resolve_code_path((code_profile or "").strip() or None, None)
        except KeyError as exc:
            error(str(exc))
            sys.exit(1)
        if not code_path:
            error("No codebase path configured.")
            sys.exit(1)
        profile_nm = ((code_profile or "").strip() or cfg.active_code_profile or "default").strip() or "default"
        invalidate_cache(profile_nm, code_path)
        delete_code_collection(source_filters=[code_path])
        success(
            f"Cleared codebase cache for profile {profile_nm!r} and reset semantic code index (`amx_code`)."
        )

    @code.command("results")
    @click.option(
        "--code-profile",
        default=None,
        help="Show cached results for this profile (default: active profile).",
    )
    @click.pass_obj
    def code_results_cmd(cfg: AMXConfig, code_profile: str | None) -> None:
        """Display the last cached code-scan results for a profile."""
        from amx.codebase.cache import load_latest_cached_report

        try:
            code_path = cfg.resolve_code_path((code_profile or "").strip() or None, None)
        except KeyError as exc:
            error(str(exc))
            return
        if not code_path:
            error("No codebase path configured.")
            return
        profile_nm = ((code_profile or "").strip() or cfg.active_code_profile or "default").strip() or "default"

        manifest, report = load_latest_cached_report(profile_nm, code_path)
        if report is None or manifest is None:
            error(f"No cached code-scan for profile {profile_nm!r}. Run `/code-scan` first.")
            return

        scanned_ts = manifest.get("scanned_at", 0)
        scanned_str = (
            datetime.datetime.fromtimestamp(scanned_ts).strftime("%Y-%m-%d %H:%M:%S")
            if scanned_ts
            else "unknown"
        )
        schema_name = manifest.get("schema", "?")
        table_count = len(manifest.get("tables", []))

        heading(f"Code-scan results — profile {profile_nm!r}")
        info(f"Source: {code_path}")
        info(f"Schema: {schema_name} ({table_count} tables)")
        info(f"Scanned: {scanned_str}")
        info(f"Files: {report.scanned_files}/{report.total_files}")

        ref_count = sum(len(v) for v in report.references.values())
        ext_count = sum(len(v) for v in report.external_mentions.values())
        info(f"Catalog hits: {ref_count}  |  External mentions: {ext_count}")

        if report.references:
            rows = [
                [asset, str(len(refs)), refs[0].file if refs else ""]
                for asset, refs in sorted(report.references.items())
            ]
            render_table("Asset references", ["Asset", "Ref Count", "Example File"], rows[:40])
        else:
            warn("No catalog references in cached report.")

        if report.external_mentions:
            rows = [
                [asset, str(len(refs)), refs[0].file if refs else ""]
                for asset, refs in sorted(report.external_mentions.items())[:20]
            ]
            render_table(
                "External identifiers (not in DB table list)",
                ["Token", "Ref Count", "Example File"],
                rows,
            )

    @code.command("export-report")
    @click.argument("output_file", required=False, default=None)
    @click.option(
        "--code-profile",
        default=None,
        help="Export results for this profile (default: active profile).",
    )
    @click.pass_obj
    def code_export_report_cmd(cfg: AMXConfig, output_file: str | None, code_profile: str | None) -> None:
        """Export the cached code-scan results to a markdown file."""
        from amx.codebase.cache import load_latest_cached_report

        try:
            code_path = cfg.resolve_code_path((code_profile or "").strip() or None, None)
        except KeyError as exc:
            error(str(exc))
            return
        if not code_path:
            error("No codebase path configured.")
            return
        profile_nm = ((code_profile or "").strip() or cfg.active_code_profile or "default").strip() or "default"

        manifest, report = load_latest_cached_report(profile_nm, code_path)
        if report is None or manifest is None:
            error(f"No cached code-scan for profile {profile_nm!r}. Run `/code-scan` first.")
            return

        scanned_ts = manifest.get("scanned_at", 0)
        scanned_str = (
            datetime.datetime.fromtimestamp(scanned_ts).strftime("%Y-%m-%d %H:%M:%S")
            if scanned_ts
            else "unknown"
        )
        schema_name = manifest.get("schema", "?")
        table_count = len(manifest.get("tables", []))
        out = output_file or f"code_report_{profile_nm}_{schema_name}.md"

        lines: list[str] = [
            f"# Code-scan report — profile `{profile_nm}`",
            "",
            f"- **Source:** `{code_path}`",
            f"- **Schema:** `{schema_name}` ({table_count} tables)",
            f"- **Scanned:** {scanned_str}",
            f"- **Files:** {report.scanned_files}/{report.total_files}",
            "",
        ]

        ref_count = sum(len(v) for v in report.references.values())
        ext_count = sum(len(v) for v in report.external_mentions.values())
        lines.append(f"**Catalog hits:** {ref_count}  |  **External mentions:** {ext_count}")
        lines.append("")

        if report.references:
            lines.extend(
                [
                    "## Catalog references",
                    "",
                    "| Asset | Ref Count | Example File |",
                    "|-------|-----------|--------------|",
                ]
            )
            for asset, refs in sorted(report.references.items()):
                example = refs[0].file if refs else ""
                lines.append(f"| {asset} | {len(refs)} | {example} |")
            lines.append("")

        if report.external_mentions:
            lines.extend(
                [
                    "## External identifiers",
                    "",
                    "| Token | Ref Count | Example File |",
                    "|-------|-----------|--------------|",
                ]
            )
            for asset, refs in sorted(report.external_mentions.items()):
                example = refs[0].file if refs else ""
                lines.append(f"| {asset} | {len(refs)} | {example} |")
            lines.append("")

        if report.references:
            lines.extend(["## Detailed references", ""])
            for asset, refs in sorted(report.references.items()):
                lines.append(f"### `{asset}` ({len(refs)} hit{'s' if len(refs) != 1 else ''})")
                lines.append("")
                for ref in refs[:5]:
                    lines.append(f"**{ref.file}:{ref.line_no}**")
                    lines.append("```")
                    lines.append(ref.context)
                    lines.append("```")
                    lines.append("")
                if len(refs) > 5:
                    lines.append(f"*… and {len(refs) - 5} more*")
                    lines.append("")

        Path(out).write_text("\n".join(lines), encoding="utf-8")
        success(f"Exported code-scan report to {out}")

    @code.command("analyze")
    @click.argument("tables_pos", nargs=-1, metavar="[TABLE ...]")
    @click.option("--schema", "-s", help="Schema context.")
    @click.option("--table", "-t", multiple=True, help="Specific table(s).")
    @click.option("--code-profile", default=None, help="Use this codebase profile.")
    @click.pass_obj
    def code_analyze_cmd(
        cfg: AMXConfig,
        tables_pos: tuple[str, ...],
        schema: str | None,
        table: tuple[str, ...],
        code_profile: str | None,
    ) -> None:
        """Run the Code Agent standalone against the cached code-scan for the given tables.

        Pass table names on the command line to skip the long interactive list, e.g.
        ``amx code analyze vbrk`` or ``amx code analyze vbrk vbrp --schema sap_s6p``.

        Results are saved to ~/.amx/code_agent_results.json and reused by the next /run.
        """
        from amx.agents.base import AgentContext
        from amx.agents.code_agent import CodeAgent
        from amx.codebase.cache import load_latest_cached_report
        from amx.db.connector import DatabaseConnector
        from amx.llm.provider import LLMProvider

        if not cfg.llm.provider or not cfg.llm.model:
            error("LLM not configured. Run `amx setup` first.")
            sys.exit(1)

        try:
            code_path = cfg.resolve_code_path((code_profile or "").strip() or None, None)
        except KeyError as exc:
            error(str(exc))
            return
        if not code_path:
            error("No codebase path configured. Run `/code` then `/add-code-profile` first.")
            return
        profile_nm = ((code_profile or "").strip() or cfg.active_code_profile or "default").strip() or "default"

        _, code_report = load_latest_cached_report(profile_nm, code_path)
        if code_report is None:
            error(f"No cached code-scan for profile {profile_nm!r}. Run `/code-scan` first.")
            return

        token_tracker.reset()

        llm = LLMProvider(cfg.llm)
        db = DatabaseConnector(cfg.db)
        if not db.test_connection():
            error("Cannot connect to database.")
            sys.exit(1)

        tables_arg = list(tables_pos) + list(table)
        scope = finalize_scope(cfg, db, schema or cfg.current_schema, tables_arg)
        if scope is None:
            return
        schema_name = next(iter(scope))
        tables = scope[schema_name]

        agent = CodeAgent(llm, code_report)
        all_suggestions = []
        for table_name in tables:
            with step_spinner(f"Reading columns for {schema_name}.{table_name}"):
                columns = db.list_column_profiles(schema_name, table_name)
            ctx = AgentContext(
                schema=schema_name,
                table=table_name,
                db_profile={
                    "row_count": 0,
                    "columns": [{"name": c.name, "dtype": c.dtype} for c in columns],
                },
                existing_metadata={},
            )
            info(f"Code Agent: {schema_name}.{table_name} ({len(columns)} columns)")
            suggestions = agent.run(ctx)
            all_suggestions.extend(suggestions)
            info(f"  -> {len(suggestions)} suggestions")

        if not all_suggestions:
            warn("Code Agent produced no suggestions.")
            render_token_summary(token_tracker)
            return

        rows = [
            [s.column or s.table, s.suggestions[0][:60] if s.suggestions else "", s.confidence.value]
            for s in all_suggestions
        ]
        render_table("Code Agent suggestions", ["Asset", "Suggestion", "Confidence"], rows[:40])

        cache_path = Path.home() / ".amx" / "code_agent_results.json"
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        payload = [
            {
                "schema": s.schema,
                "table": s.table,
                "column": s.column,
                "suggestions": s.suggestions,
                "confidence": s.confidence.value,
                "reasoning": s.reasoning,
                "source": s.source,
            }
            for s in all_suggestions
        ]
        cache_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        success(f"Saved {len(all_suggestions)} Code Agent suggestions to {cache_path}")
        info("These will be available as pre-computed input for the next `/run`.")
        render_token_summary(token_tracker)
