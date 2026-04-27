"""Document namespace commands for the AMX interactive CLI."""

from __future__ import annotations

import json
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import click

from amx.config import AMXConfig
from amx.utils.console import (
    confirm,
    console,
    error,
    info,
    render_table,
    render_token_summary,
    step_spinner,
    success,
    warn,
)
from amx.utils.token_tracker import tracker as token_tracker

FinalizeScope = Callable[[AMXConfig, object, str | None, list[str]], dict[str, list[str]] | None]
WarnNoPaths = Callable[..., None]


def _run_docs_semantic_search(question: str, results: int) -> None:
    """Chroma embedding similarity only; no generative LLM."""
    from amx.docs.rag import RAGStore

    store = RAGStore()
    if store.doc_count == 0:
        error("RAG store is empty. Run /ingest (after /add-doc-profile) first.")
        return

    hits = store.query(question, n_results=results)
    for i, hit in enumerate(hits, 1):
        console.print(f"\n[heading]Match {i}[/heading] (distance: {hit['distance']:.3f})")
        console.print(f"  Source: {hit['metadata'].get('source', 'unknown')}")
        console.print(f"  {hit['text'][:300]}...")


def register_docs_commands(
    main: click.Group,
    *,
    finalize_scope: FinalizeScope,
    warn_no_doc_paths_for_scan_or_ingest: WarnNoPaths,
) -> None:
    """Attach `/docs` namespace commands to the main Click group."""

    @main.group()
    def docs() -> None:
        """Document scanning and RAG commands."""

    @docs.command("scan")
    @click.argument("paths", nargs=-1)
    @click.option(
        "--doc-profile",
        "doc_profile",
        default=None,
        help="Use paths from this named document profile when no paths are given.",
    )
    @click.pass_obj
    def docs_scan(cfg: AMXConfig, paths: tuple[str, ...], doc_profile: str | None) -> None:
        """Scan document sources and show what would be ingested."""
        from amx.docs.scanner import scan_all_sources, total_size_mb

        try:
            all_paths = list(paths) if paths else cfg.resolve_doc_paths(doc_profile, [])
        except KeyError as exc:
            error(str(exc))
            return
        if not all_paths:
            warn_no_doc_paths_for_scan_or_ingest(cfg, cmd="scan")
            return

        documents = scan_all_sources(all_paths)
        size = total_size_mb(documents)

        render_table(
            f"Found {len(documents)} documents ({size:.1f} MB)",
            ["File", "Size (KB)", "Type", "Source"],
            [[d.path, f"{d.size_bytes / 1024:.1f}", d.extension, d.source_type] for d in documents[:50]],
        )

        if len(documents) > 50:
            info(f"... and {len(documents) - 50} more files")

        if size > 100:
            warn(f"Total size is {size:.1f} MB — ingestion may take a while.")
            if not confirm("Proceed with ingestion?"):
                return

        if confirm("Ingest these documents into the RAG store?"):
            from amx.docs.rag import RAGStore

            store = RAGStore()
            chunks = store.ingest(documents, refresh=False)
            success(f"Ingested {chunks} chunks from {len(documents)} documents")

    @docs.command("ingest")
    @click.argument("paths", nargs=-1)
    @click.option(
        "--doc-profile",
        "doc_profile",
        default=None,
        help="Use paths from this named document profile when no paths are given.",
    )
    @click.option(
        "--refresh/--no-refresh",
        default=False,
        help="Delete existing Chroma chunks for the same source paths before upserting.",
    )
    @click.pass_obj
    def docs_ingest(
        cfg: AMXConfig,
        paths: tuple[str, ...],
        doc_profile: str | None,
        refresh: bool,
    ) -> None:
        """Ingest documents directly into the RAG store."""
        from amx.docs.rag import RAGStore
        from amx.docs.scanner import scan_all_sources, total_size_mb

        try:
            all_paths = list(paths) if paths else cfg.resolve_doc_paths(doc_profile, [])
        except KeyError as exc:
            error(str(exc))
            return
        if not all_paths:
            warn_no_doc_paths_for_scan_or_ingest(cfg, cmd="ingest")
            return

        documents = scan_all_sources(all_paths)
        size = total_size_mb(documents)

        info(f"Found {len(documents)} documents ({size:.1f} MB)")

        if size > 100:
            warn(f"Large document set ({size:.1f} MB). This will take some time.")
            if not confirm("Continue?"):
                return

        store = RAGStore()
        chunks = store.ingest(documents, refresh=refresh)
        if refresh:
            info("Refreshed: removed prior chunks for the same source paths before ingest.")
        success(f"Ingested {chunks} chunks into RAG store ({store.doc_count} total chunks)")

    @docs.command("search-docs")
    @click.argument("question")
    @click.option("-n", "--results", default=5, help="Number of results.")
    def docs_search_docs(question: str, results: int) -> None:
        """Semantic similarity search over ingested documents (vector store only; no LLM reply)."""
        _run_docs_semantic_search(question, results)

    @docs.command("export-report")
    @click.argument("output_file", required=False, default=None)
    @click.option(
        "--doc-profile",
        default=None,
        help="Use this document profile (default: active profile).",
    )
    @click.pass_obj
    def docs_export_report(cfg: AMXConfig, output_file: str | None, doc_profile: str | None) -> None:
        """Export a summary of the RAG document store to a markdown file."""
        from amx.docs.rag import RAGStore

        store = RAGStore()
        if store.doc_count == 0:
            error("RAG store is empty. Run `/ingest` first.")
            return

        try:
            doc_paths = cfg.resolve_doc_paths((doc_profile or "").strip() or None, [])
        except KeyError as exc:
            error(str(exc))
            return

        profile_nm = (doc_profile or "").strip() or cfg.active_doc_profile or "default"

        all_meta = store.collection.get(include=["metadatas"])
        metadatas = all_meta.get("metadatas") or []

        source_counts: dict[str, int] = {}
        source_types: dict[str, str] = {}
        for metadata in metadatas:
            src = metadata.get("source", "unknown")
            source_counts[src] = source_counts.get(src, 0) + 1
            stype = metadata.get("source_type", "")
            if stype:
                source_types[src] = stype

        out = output_file or f"doc_report_{profile_nm}.md"

        lines: list[str] = [
            f"# Document RAG report — profile `{profile_nm}`",
            "",
            f"- **Total chunks:** {store.doc_count}",
            f"- **Configured paths:** {', '.join(doc_paths) if doc_paths else 'none'}",
            f"- **Distinct sources:** {len(source_counts)}",
            "",
            "## Sources by chunk count",
            "",
            "| Source | Chunks | Type |",
            "|--------|--------|------|",
        ]
        for src, cnt in sorted(source_counts.items(), key=lambda item: -item[1]):
            stype = source_types.get(src, "")
            lines.append(f"| {src} | {cnt} | {stype} |")
        lines.append("")

        Path(out).write_text("\n".join(lines), encoding="utf-8")
        success(f"Exported document RAG report to {out}")

    @docs.command("analyze")
    @click.argument("tables_pos", nargs=-1, metavar="[TABLE ...]")
    @click.option("--schema", "-s", help="Schema context.")
    @click.option("--table", "-t", multiple=True, help="Specific table(s).")
    @click.pass_obj
    def docs_analyze(
        cfg: AMXConfig,
        tables_pos: tuple[str, ...],
        schema: str | None,
        table: tuple[str, ...],
    ) -> None:
        """Run the RAG Agent standalone against ingested documents for the given tables.

        Results are saved to ~/.amx/doc_agent_results.json and reused by the next /run.
        """
        from amx.agents.base import AgentContext
        from amx.agents.rag_agent import RAGAgent
        from amx.db.connector import DatabaseConnector
        from amx.docs.rag import RAGStore
        from amx.llm.provider import LLMProvider

        if not cfg.llm.provider or not cfg.llm.model:
            error("LLM not configured. Run `amx setup` first.")
            sys.exit(1)

        store = RAGStore()
        if store.doc_count == 0:
            error("RAG store is empty. Run `/ingest` first.")
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

        agent = RAGAgent(llm, store)
        all_suggestions = []
        for table_name in tables:
            with step_spinner(f"Profiling {schema_name}.{table_name}"):
                table_profile = db.profile_table(schema_name, table_name)
            ctx = AgentContext(
                schema=schema_name,
                table=table_name,
                db_profile={
                    "row_count": table_profile.row_count,
                    "columns": [{"name": c.name, "dtype": c.dtype} for c in table_profile.columns],
                },
                existing_metadata={},
            )
            info(f"RAG Agent: {schema_name}.{table_name} ({len(table_profile.columns)} columns)")
            suggestions = agent.run(ctx)
            all_suggestions.extend(suggestions)
            info(f"  -> {len(suggestions)} suggestions")

        if not all_suggestions:
            warn("RAG Agent produced no suggestions.")
            render_token_summary(token_tracker)
            return

        rows = [
            [s.column or s.table, s.suggestions[0][:60] if s.suggestions else "", s.confidence.value]
            for s in all_suggestions
        ]
        render_table("RAG Agent suggestions", ["Asset", "Suggestion", "Confidence"], rows[:40])

        cache_path = Path.home() / ".amx" / "doc_agent_results.json"
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
        success(f"Saved {len(all_suggestions)} RAG Agent suggestions to {cache_path}")
        info("These will be available as pre-computed input for the next `/run`.")
        render_token_summary(token_tracker)
