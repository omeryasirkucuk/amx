"""Code namespace commands for the AMX interactive CLI."""

from __future__ import annotations

import datetime
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
    step_spinner,
    success,
    warn,
)
from amx.utils.live_commands import command_display

if TYPE_CHECKING:
    from amx.codebase.analyzer import CodebaseReport


def _build_scan_progress() -> tuple[dict[str, object | None], callable]:
    progress_state: dict[str, object | None] = {"obj": None, "task": None}

    def _scan_cb(action: str, value: object) -> None:
        from rich.progress import (
            BarColumn,
            MofNCompleteColumn,
            Progress,
            TextColumn,
            TimeElapsedColumn,
        )

        from amx.utils.live_display import get_display

        display = get_display()
        if display.is_active:
            if action == "__total__":
                progress_state["obj"] = "live"
                progress_state["task"] = display.add_activity(f"Scanning files 0/{int(value)}")
                display.begin_activity(progress_state["task"])  # type: ignore[arg-type]
            elif action == "__advance__" and progress_state["task"] is not None:
                idx = progress_state.setdefault("count", 0)
                next_count = int(idx or 0) + 1
                progress_state["count"] = next_count
                total = int(progress_state.get("total") or 0)
                if not total:
                    total = next_count
                display.update_activity(
                    progress_state["task"],  # type: ignore[arg-type]
                    label=f"Scanning files {next_count}/{total}: {value}",
                )
            if action == "__total__":
                progress_state["total"] = int(value)
            return

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

    @code.command("index")
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
    def code_index_cmd(
        cfg: AMXConfig,
        path: str | None,
        schema: str | None,
        code_profile: str | None,
    ) -> None:
        """Build / refresh the code index: scan the codebase for table +
        column references and (re)build the semantic code index under the
        active embedding. Replaces ``scan`` / ``refresh`` (columns always
        included)."""
        from amx.codebase.analyzer import analyze_codebase
        from amx.codebase.cache import save_cached_report
        from amx.db.connector import DatabaseConnector

        schema_name = schema or cfg.current_schema
        if not schema_name:
            error("Missing schema: pass --schema sap_s6p (or use /run's interactive picker).")
            sys.exit(1)

        try:
            resolved = cfg.resolve_code_path(
                (code_profile or "").strip() or None, (path or "").strip() or None
            )
        except KeyError as exc:
            error(str(exc))
            sys.exit(1)
        if not resolved:
            error(
                "No codebase path given and no matching profile. "
                "Run `/code` then `/add-code-profile`, or `/code-index --code-profile NAME`, or pass a path."
            )
            sys.exit(1)
        if not (path or "").strip():
            if (code_profile or "").strip():
                info(f"Using codebase profile {(code_profile or '').strip()!r}: {resolved}")
            else:
                info(f"Using active codebase profile path: {resolved}")

        profile_nm = (
            (code_profile or "").strip() or cfg.active_code_profile or "default"
        ).strip() or "default"

        with command_display(
            schema=schema_name, mode="code-scan", provider=cfg.llm.provider, model=cfg.llm.model
        ):
            db = DatabaseConnector(cfg.db)
            with step_spinner(f"Listing assets in {schema_name}"):
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
            scan_failed = False

            def _run_scan():
                return analyze_codebase(
                    resolved,
                    tables,
                    column_names=column_names,
                    known_catalog_tables=catalog,
                    index_semantic=True,
                    progress_callback=scan_cb,
                )

            try:
                from amx.codebase.code_rag import CodeEmbeddingMismatch

                try:
                    report = _run_scan()
                except CodeEmbeddingMismatch:
                    # The code embedding model changed, so the stamped
                    # amx_code identity no longer matches the active config.
                    # Drop the whole collection and re-index under the active
                    # model (the index equivalent of the old /code-refresh
                    # recovery); a per-path delete would leave the identity).
                    from amx.codebase.code_rag import delete_code_collection

                    info("Embedding model changed — rebuilding the code index from scratch.")
                    delete_code_collection(source_filters=None)
                    report = _run_scan()
            except Exception as exc:
                scan_failed = True
                error(str(exc))
                sys.exit(1)
            finally:
                if progress_state["obj"] == "live" and progress_state["task"] is not None:
                    from amx.utils.live_display import get_display

                    if scan_failed:
                        get_display().fail_activity(
                            progress_state["task"],  # type: ignore[arg-type]
                            "Code scan failed",
                        )
                    else:
                        get_display().complete_activity(
                            progress_state["task"],  # type: ignore[arg-type]
                            f"Scanned {report.scanned_files if 'report' in locals() else int(progress_state.get('count') or 0)} file(s)",
                        )
                elif progress_state["obj"] is not None:
                    progress_state["obj"].stop()

        try:
            with command_display(
                schema=schema_name, mode="code-scan", provider=cfg.llm.provider, model=cfg.llm.model
            ):
                with step_spinner("Saving code scan cache"):
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
        try:
            from amx.search.catalog import SearchCatalog

            catalog_store = SearchCatalog.from_history_store()
            if catalog_store is not None:
                with command_display(
                    schema=schema_name,
                    mode="code-sync",
                    provider=cfg.llm.provider,
                    model=cfg.llm.model,
                ):
                    with step_spinner("Refreshing /search code evidence"):
                        catalog_store.sync_code_report(
                            db_profile=cfg.active_db_profile or "default",
                            db_backend=cfg.db.backend,
                            database_name=cfg.db.database or cfg.db.catalog or cfg.db.project or "",
                            schema_name=schema_name,
                            source_path=resolved,
                            report=report,
                        )
        except Exception as exc:
            warn(f"Could not sync code evidence into /search catalog: {exc}")

        _render_code_report_summary(report)
        info("Results saved. Next `/run` will use them from cache (re-run `/code-index` to refresh).")

    @code.command("search")
    @click.argument("question")
    @click.option("-n", "--results", default=5, help="Number of results.")
    @click.option(
        "--code-profile",
        default=None,
        help="Scope hits to this code profile's path (default: every indexed snippet).",
    )
    @click.pass_obj
    def code_search_cmd(
        cfg: AMXConfig, question: str, results: int, code_profile: str | None
    ) -> None:
        """Embedding-only similarity search over the ``amx_code`` index.

        Mirrors the Studio Code → Search box. No LLM call; cheap. Pass
        ``--code-profile`` to gate to one profile's source paths so a
        repo without ETL code doesn't surface in a "where is X written?"
        question. The output prints file:line + snippet so the user can
        jump directly into their editor.

        💡 In Studio, the ``Search`` box under Settings → Code shows the
        same results as interactive cards.
        """
        from amx.cli_support.hints import studio_hint
        from amx.codebase.code_rag import code_collection_count, query_code_snippets

        prof = (code_profile or "").strip()
        source_filters: list[str] | None = None
        if prof:
            if prof not in cfg.code_profiles:
                error(f"Unknown code profile: {prof}")
                sys.exit(1)
            path = (cfg.code_profiles.get(prof) or "").strip()
            source_filters = [path] if path else None

        if code_collection_count(source_filters=source_filters) == 0:
            error(
                f"No indexed code{' for profile ' + prof if prof else ''}. Run `/code-index` first."
            )
            return

        n = max(1, min(int(results), 25))
        hits = query_code_snippets(question, n_results=n, source_filters=source_filters)
        if not hits:
            info("No matches.")
            return
        for i, hit in enumerate(hits, 1):
            meta = hit.get("metadata") or {}
            source = meta.get("source") or meta.get("rel_path") or "unknown"
            symbol = meta.get("symbol") or meta.get("kind") or ""
            console.print(
                f"\n[heading]Match {i}[/heading] (distance: "
                f"{float(hit.get('distance') or 0.0):.3f})"
            )
            console.print(f"  Source: {source}{' · ' + symbol if symbol else ''}")
            text = str(hit.get("text") or "")
            console.print(f"  {text[:400]}{'…' if len(text) > 400 else ''}")
        studio_hint("code-search")

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
        profile_nm = (
            (code_profile or "").strip() or cfg.active_code_profile or "default"
        ).strip() or "default"

        manifest, report = load_latest_cached_report(profile_nm, code_path)
        if report is None or manifest is None:
            error(f"No cached code-scan for profile {profile_nm!r}. Run `/code-index` first.")
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
    def code_export_report_cmd(
        cfg: AMXConfig, output_file: str | None, code_profile: str | None
    ) -> None:
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
        profile_nm = (
            (code_profile or "").strip() or cfg.active_code_profile or "default"
        ).strip() or "default"

        manifest, report = load_latest_cached_report(profile_nm, code_path)
        if report is None or manifest is None:
            error(f"No cached code-scan for profile {profile_nm!r}. Run `/code-index` first.")
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
