"""Document namespace commands for the AMX interactive CLI."""

from __future__ import annotations

import json
import sys
from collections.abc import Callable
from pathlib import Path

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
from amx.utils.live_commands import command_display
from amx.utils.token_tracker import tracker as token_tracker

FinalizeScope = Callable[[AMXConfig, object, str | None, list[str]], dict[str, list[str]] | None]
WarnNoPaths = Callable[..., None]


def _render_scan_failures(scan_outcome: object) -> None:
    """Print ``Failed to scan: <source> — <reason>`` for each entry on
    a :class:`amx.docs.scanner.ScanResult`. Silently no-ops if the
    caller passed a bare ``list[DocInfo]`` (legacy stubbed return)."""
    failures = list(getattr(scan_outcome, "failures", []) or [])
    if not failures:
        return
    for src, reason in failures:
        error(f"  failed to scan: {src} — {reason}")


def _render_ingest_summary(summary: object, *, total_files: int) -> None:
    """Print the per-file ingest outcome line + any failure detail.

    ``summary`` is duck-typed against :class:`amx.docs.rag.IngestSummary`
    (the import lives inside the command bodies to keep this module
    cheap to import). When ``failed`` is non-empty, list each failed
    file path + the short error reason so the user can act on the
    specific files instead of grepping ``~/.amx/logs/amx.log``.
    """
    succeeded = list(getattr(summary, "succeeded", []) or [])
    failed = list(getattr(summary, "failed", []) or [])
    chunk_count = int(getattr(summary, "chunk_count", 0) or 0)
    info(
        f"Ingested {chunk_count} chunks from {len(succeeded)} of {total_files} files "
        f"({len(failed)} failed)"
    )
    for path, reason in failed:
        error(f"  failed: {path} — {reason}")


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

    @docs.command("add")
    @click.argument("profile")
    @click.argument("paths", nargs=-1, required=True)
    @click.option(
        "--no-ingest",
        is_flag=True,
        default=False,
        help="Save files only; skip the immediate ingest. Run /ingest later.",
    )
    @click.pass_obj
    def docs_add_cmd(
        cfg: AMXConfig,
        profile: str,
        paths: tuple[str, ...],
        no_ingest: bool,
    ) -> None:
        """`/doc-add <profile> <file>...` — drag-drop equivalent for the CLI.

        Copies the listed local files into ``~/.amx/uploads/<profile>/``
        (content-hashed so duplicates are detected), wires the upload
        directory into the doc profile, and unless ``--no-ingest`` is
        set, immediately runs the same ingest pipeline ``/ingest`` uses.

        💡 Tip: in Studio, Settings → Docs lets you add files via
        **drag-and-drop**.
        """
        from amx.cli_support.hints import studio_hint
        from amx.docs.rag import RAGStore
        from amx.docs.scanner import (
            cleanup_scan_artifacts,
            scan_all_sources,
            total_size_mb,
        )
        from amx.docs.uploads import UploadError, save_uploaded_file

        prof = (profile or "").strip()
        if not prof:
            error("profile name is required")
            return

        results = []
        try:
            for raw in paths:
                p = Path(raw).expanduser()
                if not p.exists():
                    error(f"Not found: {p}")
                    continue
                if p.is_dir():
                    error(
                        f"{p} is a directory — point at individual files, "
                        "or use /add-doc-profile <name> with the directory path."
                    )
                    continue
                try:
                    payload = p.read_bytes()
                    res = save_uploaded_file(cfg, prof, p.name, payload)
                except UploadError as exc:
                    error(f"{p}: {exc}")
                    continue
                tag = " (duplicate, skipped write)" if res.duplicate else ""
                success(f"Saved {p.name} → {res.saved_path}{tag}")
                results.append(res)
        finally:
            studio_hint("doc-add")

        if not results:
            return

        if no_ingest:
            info("Skipping immediate ingest — run `/ingest` to index later.")
            return

        upload_root = str(Path(results[0].saved_path).parent)
        documents = []
        try:
            with command_display(
                mode="docs-upload-ingest",
                provider=cfg.llm.provider,
                model=cfg.llm.model,
            ):
                with step_spinner(f"Scanning {upload_root}"):
                    documents = scan_all_sources([upload_root])
                _render_scan_failures(documents)
                size = total_size_mb(documents)
                info(f"Found {len(documents)} document(s) ({size:.1f} MB)")
                store = RAGStore()
                with step_spinner("Ingesting into RAG store"):
                    summary = store.ingest(documents, refresh=False)
                _render_ingest_summary(summary, total_files=len(documents))
                success(
                    f"Ingested {summary.chunk_count} chunks into RAG store "
                    f"({store.doc_count} total chunks)"
                )
        finally:
            cleanup_scan_artifacts(documents)

    @docs.command("index")
    @click.option(
        "--doc-profile",
        "doc_profile",
        default=None,
        help="Use paths from this named document profile when no paths are given.",
    )
    @click.argument("paths", nargs=-1)
    @click.pass_obj
    def docs_index(cfg: AMXConfig, paths: tuple[str, ...], doc_profile: str | None) -> None:
        """Build / refresh the docs RAG index under the active embedding.

        One smart, idempotent operation that replaces ``scan`` / ``ingest``
        / ``reindex``: it ingests new and changed files incrementally, and
        — when the embedding model has changed (a stale collection
        identity) — drops and rebuilds the collection so it is re-stamped
        with the active provider/model. Run it after changing the docs
        embedding in ``/embeddings``.
        """
        from pathlib import Path

        from amx.docs.rag import EmbeddingProviderMismatch, RAGStore
        from amx.docs.scanner import cleanup_scan_artifacts, scan_all_sources, total_size_mb

        try:
            all_paths = list(paths) if paths else cfg.resolve_doc_paths(doc_profile, [])
        except KeyError as exc:
            error(str(exc))
            return
        if not all_paths:
            warn_no_doc_paths_for_scan_or_ingest(cfg, cmd="index")
            return

        documents = []
        try:
            with command_display(
                mode="docs-ingest", provider=cfg.llm.provider, model=cfg.llm.model
            ):
                with step_spinner("Scanning document sources"):
                    documents = scan_all_sources(all_paths)
                _render_scan_failures(documents)
                size = total_size_mb(documents)
                info(f"Found {len(documents)} documents ({size:.1f} MB)")
                if size > 100:
                    warn(f"Large document set ({size:.1f} MB). This will take some time.")
                    if not confirm("Continue?"):
                        return

                # Open the store, recovering from a stale embedding identity:
                # on a model change the collection is force-dropped + rebuilt;
                # otherwise we ingest incrementally into the existing one.
                mismatch = False
                try:
                    store = RAGStore()
                except EmbeddingProviderMismatch:
                    mismatch = True
                    import chromadb

                    client = chromadb.PersistentClient(
                        path=str(Path.home() / ".amx" / "chroma_db")
                    )
                    try:
                        client.delete_collection(name="amx_docs")
                    except Exception:  # noqa: BLE001 - already absent is fine
                        pass
                    store = RAGStore()
                if mismatch:
                    store.reset_collection()

                label = (
                    "Rebuilding index (embedding model changed)"
                    if mismatch
                    else "Indexing documents into RAG store"
                )
                with step_spinner(label):
                    summary = store.ingest(documents, refresh=False)
                _render_ingest_summary(summary, total_files=len(documents))
                success(
                    f"Indexed {summary.chunk_count} chunks under "
                    f"{store.embedding_provider}/{store.embedding_model} "
                    f"({store.doc_count} total chunks)"
                )
        finally:
            cleanup_scan_artifacts(documents)

    @docs.command("search-docs")
    @click.argument("question")
    @click.option("-n", "--results", default=5, help="Number of results.")
    def docs_search_docs(question: str, results: int) -> None:
        """Semantic similarity search over ingested documents (vector store only; no LLM reply)."""
        with command_display(mode="docs-search", provider="", model=""):
            with step_spinner("Searching ingested documents"):
                _run_docs_semantic_search(question, results)

    @docs.command("export-report")
    @click.argument("output_file", required=False, default=None)
    @click.option(
        "--doc-profile",
        default=None,
        help="Use this document profile (default: active profile).",
    )
    @click.pass_obj
    def docs_export_report(
        cfg: AMXConfig, output_file: str | None, doc_profile: str | None
    ) -> None:
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
            error(
                "No active LLM profile is configured. "
                "Use `/llm` then `/add-llm-profile`, or run `/setup`."
            )
            sys.exit(1)

        store = RAGStore()
        if store.doc_count == 0:
            error("RAG store is empty. Run `/ingest` first.")
            return

        token_tracker.reset()

        llm = LLMProvider(cfg.llm)
        # Honour /use-rag-llm: build a separate provider for the RAG
        # agent when the user has pinned a different LLM profile to
        # retrieval. Identity check on cfg.llm avoids redundant work
        # when no override is set.
        rag_cfg = cfg.effective_rag_llm()
        rag_llm = LLMProvider(rag_cfg) if rag_cfg is not cfg.llm else llm
        db = DatabaseConnector(cfg.db)
        with command_display(
            schema=schema or cfg.current_schema or "",
            mode="docs-analyze",
            provider=cfg.llm.provider,
            model=cfg.llm.model,
        ):
            with step_spinner("Testing database connection..."):
                connected = db.test_connection()
            if not connected:
                error("Cannot connect to database.")
                sys.exit(1)

            tables_arg = list(tables_pos) + list(table)
            scope = finalize_scope(cfg, db, schema or cfg.current_schema, tables_arg)
            if scope is None:
                return
            schema_name = next(iter(scope))
            tables = scope[schema_name]

            agent = RAGAgent(rag_llm, store)
            all_suggestions = []
            for table_name in tables:
                with step_spinner(f"Profiling {schema_name}.{table_name}"):
                    table_profile = db.profile_table(schema_name, table_name)
                ctx = AgentContext(
                    schema=schema_name,
                    table=table_name,
                    db_profile={
                        "row_count": table_profile.row_count,
                        "columns": [
                            {"name": c.name, "dtype": c.dtype} for c in table_profile.columns
                        ],
                    },
                    existing_metadata={},
                )
                info(
                    f"RAG Agent: {schema_name}.{table_name} ({len(table_profile.columns)} columns)"
                )
                suggestions = agent.run(ctx)
                all_suggestions.extend(suggestions)
                info(f"  -> {len(suggestions)} suggestions")

        if not all_suggestions:
            warn("RAG Agent produced no suggestions.")
            render_token_summary(token_tracker)
            return

        rows = [
            [
                s.column or s.table,
                s.suggestions[0][:60] if s.suggestions else "",
                s.confidence.value,
            ]
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
