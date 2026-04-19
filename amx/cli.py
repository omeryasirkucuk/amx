"""AMX CLI — Agentic Metadata Extractor command-line interface."""

from __future__ import annotations

import sys

import click

from amx import __version__
from amx.config import AMXConfig, DBConfig, LLMConfig
from amx.utils.console import (
    ask,
    ask_choice,
    ask_multi_choice,
    ask_password,
    confirm,
    console,
    error,
    heading,
    info,
    render_table,
    success,
    warn,
)
from amx.utils.logging import get_logger

log = get_logger("cli")

pass_config = click.make_pass_decorator(AMXConfig, ensure=True)


@click.group()
@click.version_option(__version__, prog_name="amx")
@click.option("--config", "cfg_path", default=None, help="Path to config YAML file.")
@click.pass_context
def main(ctx: click.Context, cfg_path: str | None) -> None:
    """AMX — Agentic Metadata Extractor.

    AI-powered CLI to infer, review, and apply database metadata
    using database profiling, document RAG, and codebase analysis.
    """
    ctx.ensure_object(dict)
    ctx.obj = AMXConfig.load(cfg_path)


# ── Setup Commands ──────────────────────────────────────────────────────────


@main.command()
@click.pass_obj
def setup(cfg: AMXConfig) -> None:
    """Interactive first-time setup wizard."""
    heading("AMX Setup Wizard")

    # Database
    info("Step 1/3 — Database Connection")
    cfg.db.host = ask("PostgreSQL host", cfg.db.host)
    cfg.db.port = int(ask("Port", str(cfg.db.port)))
    cfg.db.user = ask("Username", cfg.db.user)
    cfg.db.password = ask_password("Password") or cfg.db.password
    cfg.db.database = ask("Database name", cfg.db.database)

    from amx.db.connector import DatabaseConnector

    db = DatabaseConnector(cfg.db)
    if db.test_connection():
        success("Database connection successful!")
    else:
        error("Database connection failed. Check credentials and try again.")
        if not confirm("Continue anyway?", default=False):
            sys.exit(1)

    # LLM
    info("Step 2/3 — AI Model Configuration")
    cfg.llm.provider = ask_choice(
        "Select AI provider",
        ["openai", "anthropic", "gemini", "deepseek", "local", "kimi", "ollama"],
        default=cfg.llm.provider or "openai",
    )
    cfg.llm.model = ask("Model name", cfg.llm.model or _default_model(cfg.llm.provider))

    if cfg.llm.provider in ("local", "ollama", "kimi"):
        cfg.llm.api_base = ask("API base URL", cfg.llm.api_base or "http://localhost:11434/v1")
    cfg.llm.api_key = ask_password("API key") or cfg.llm.api_key

    from amx.llm.provider import LLMProvider

    llm = LLMProvider(cfg.llm)
    if llm.test():
        success("LLM connection successful!")
    else:
        warn("LLM test failed — you can reconfigure later with `amx setup`.")

    # Data sources
    info("Step 3/3 — Optional Data Sources")
    if confirm("Add document paths for RAG?", default=False):
        while True:
            p = ask("Document path (local dir, s3://, github URL, or empty to stop)")
            if not p:
                break
            cfg.doc_paths.append(p)

    if confirm("Add codebase paths for analysis?", default=False):
        while True:
            p = ask("Codebase path (local dir, github URL, or empty to stop)")
            if not p:
                break
            cfg.code_paths.append(p)

    saved = cfg.save()
    success(f"Configuration saved to {saved}")


def _default_model(provider: str) -> str:
    return {
        "openai": "gpt-4o",
        "anthropic": "claude-sonnet-4-20250514",
        "gemini": "gemini-2.0-flash",
        "deepseek": "deepseek-chat",
        "local": "llama3",
        "kimi": "kimi",
        "ollama": "llama3",
    }.get(provider, "gpt-4o")


# ── Database Commands ───────────────────────────────────────────────────────


@main.group()
def db() -> None:
    """Database inspection and data loading commands."""


@db.command("connect")
@click.pass_obj
def db_connect(cfg: AMXConfig) -> None:
    """Test database connectivity."""
    from amx.db.connector import DatabaseConnector

    db = DatabaseConnector(cfg.db)
    if db.test_connection():
        success(f"Connected to {cfg.db.database} at {cfg.db.host}:{cfg.db.port}")
    else:
        error("Connection failed.")
        sys.exit(1)


@db.command("schemas")
@click.pass_obj
def db_schemas(cfg: AMXConfig) -> None:
    """List available schemas."""
    from amx.db.connector import DatabaseConnector

    db = DatabaseConnector(cfg.db)
    schemas = db.list_schemas()
    render_table("Schemas", ["Schema Name"], [[s] for s in schemas])


@db.command("tables")
@click.argument("schema")
@click.pass_obj
def db_tables(cfg: AMXConfig, schema: str) -> None:
    """List tables in a schema."""
    from amx.db.connector import DatabaseConnector

    db = DatabaseConnector(cfg.db)
    tables = db.list_tables(schema)
    render_table(f"Tables in {schema}", ["Table Name"], [[t] for t in tables])


@db.command("profile")
@click.argument("schema")
@click.argument("table")
@click.pass_obj
def db_profile(cfg: AMXConfig, schema: str, table: str) -> None:
    """Profile a specific table (stats, types, samples)."""
    from amx.db.connector import DatabaseConnector

    db = DatabaseConnector(cfg.db)
    profile = db.profile_table(schema, table)
    rows = [
        [
            c.name, c.dtype, str(c.null_count), str(c.distinct_count),
            str(c.min_val)[:30], str(c.max_val)[:30],
            ", ".join(str(s)[:20] for s in c.samples[:3]),
        ]
        for c in profile.columns
    ]
    render_table(
        f"{schema}.{table} ({profile.row_count} rows)",
        ["Column", "Type", "Nulls", "Distinct", "Min", "Max", "Samples"],
        rows,
    )


@db.command("load")
@click.argument("csv_dir")
@click.option("--schema", default="sap_s6p", help="Target schema name.")
@click.pass_obj
def db_load(cfg: AMXConfig, csv_dir: str, schema: str) -> None:
    """Bulk-load CSV files into a PostgreSQL schema."""
    from amx.db.connector import DatabaseConnector
    from amx.db.loader import load_csvs_to_schema

    db = DatabaseConnector(cfg.db)
    if not db.test_connection():
        error("Cannot connect to database.")
        sys.exit(1)

    loaded = load_csvs_to_schema(db, csv_dir, schema=schema)
    success(f"Loaded {len(loaded)} tables into {schema}")


# ── Document Commands ───────────────────────────────────────────────────────


@main.group()
def docs() -> None:
    """Document scanning and RAG commands."""


@docs.command("scan")
@click.argument("paths", nargs=-1)
@click.pass_obj
def docs_scan(cfg: AMXConfig, paths: tuple[str, ...]) -> None:
    """Scan document sources and show what would be ingested."""
    from amx.docs.scanner import scan_all_sources, total_size_mb

    all_paths = list(paths) or cfg.doc_paths
    if not all_paths:
        error("No document paths provided. Use `amx setup` or pass paths as arguments.")
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
        chunks = store.ingest(documents)
        success(f"Ingested {chunks} chunks from {len(documents)} documents")


@docs.command("ingest")
@click.argument("paths", nargs=-1)
@click.pass_obj
def docs_ingest(cfg: AMXConfig, paths: tuple[str, ...]) -> None:
    """Ingest documents directly into the RAG store."""
    from amx.docs.rag import RAGStore
    from amx.docs.scanner import scan_all_sources, total_size_mb

    all_paths = list(paths) or cfg.doc_paths
    if not all_paths:
        error("No document paths provided.")
        return

    documents = scan_all_sources(all_paths)
    size = total_size_mb(documents)

    info(f"Found {len(documents)} documents ({size:.1f} MB)")

    if size > 100:
        warn(f"Large document set ({size:.1f} MB). This will take some time.")
        if not confirm("Continue?"):
            return

    store = RAGStore()
    chunks = store.ingest(documents)
    success(f"Ingested {chunks} chunks into RAG store ({store.doc_count} total chunks)")


@docs.command("query")
@click.argument("question")
@click.option("-n", "--results", default=5, help="Number of results.")
def docs_query(question: str, results: int) -> None:
    """Query the RAG document store."""
    from amx.docs.rag import RAGStore

    store = RAGStore()
    if store.doc_count == 0:
        error("RAG store is empty. Run `amx docs ingest` first.")
        return

    hits = store.query(question, n_results=results)
    for i, hit in enumerate(hits, 1):
        console.print(f"\n[heading]Result {i}[/heading] (distance: {hit['distance']:.3f})")
        console.print(f"  Source: {hit['metadata'].get('source', 'unknown')}")
        console.print(f"  {hit['text'][:300]}...")


# ── Analysis Commands ───────────────────────────────────────────────────────


@main.group()
def analyze() -> None:
    """Run metadata inference agents."""


@analyze.command("run")
@click.option("--schema", "-s", help="Schema to analyze.")
@click.option("--table", "-t", multiple=True, help="Specific table(s). Omit for interactive selection.")
@click.option("--apply/--no-apply", default=False, help="Apply approved metadata to the database.")
@click.pass_obj
def analyze_run(cfg: AMXConfig, schema: str | None, table: tuple[str, ...], apply: bool) -> None:
    """Run all agents to infer metadata for selected tables."""
    from amx.agents.orchestrator import Orchestrator
    from amx.codebase.analyzer import analyze_codebase
    from amx.db.connector import DatabaseConnector
    from amx.docs.rag import RAGStore
    from amx.llm.provider import LLMProvider

    # Validate LLM
    if not cfg.llm.provider or not cfg.llm.model:
        error("LLM not configured. Run `amx setup` first.")
        sys.exit(1)

    llm = LLMProvider(cfg.llm)
    db = DatabaseConnector(cfg.db)

    if not db.test_connection():
        error("Cannot connect to database.")
        sys.exit(1)

    # Schema selection
    if not schema:
        schemas = db.list_schemas()
        schema = ask_choice("Select schema to analyze", schemas)

    # Table selection
    tables = list(table)
    if not tables:
        available = db.list_tables(schema)
        tables = ask_multi_choice("Select tables to analyze", available)

    # RAG store
    rag_store = None
    try:
        store = RAGStore()
        if store.doc_count > 0:
            rag_store = store
            info(f"RAG store has {store.doc_count} chunks available")
    except Exception:
        pass

    # Codebase analysis
    code_report = None
    if cfg.code_paths:
        info("Analyzing codebase references...")
        all_table_names = tables
        for cp in cfg.code_paths:
            try:
                code_report = analyze_codebase(cp, all_table_names)
                info(f"Found {sum(len(v) for v in code_report.references.values())} code references")
            except Exception as exc:
                warn(f"Codebase analysis failed for {cp}: {exc}")

    # Run orchestrator
    orch = Orchestrator(db, llm, rag_store=rag_store, code_report=code_report)

    all_results = []
    for t in tables:
        results = orch.process_table(schema, t)
        all_results.extend(results)

    # Summary
    heading("Summary")
    approved = [r for r in all_results if r.applied]
    skipped = [r for r in all_results if not r.applied]
    info(f"Approved: {len(approved)}  |  Skipped: {len(skipped)}")

    if approved:
        render_table(
            "Approved metadata",
            ["Asset", "Description", "Confidence", "Source"],
            [
                [
                    f"{r.table}.{r.column}" if r.column else r.table,
                    r.final_description[:60],
                    r.confidence.value,
                    r.source,
                ]
                for r in approved
            ],
        )

    if apply and approved:
        if confirm("Apply these metadata comments to the database?"):
            orch.apply_results(approved)
    elif approved:
        info("Run with --apply flag or use `amx apply` to write metadata to the database.")


@analyze.command("codebase")
@click.argument("path")
@click.option("--schema", "-s", required=True, help="Schema to match against.")
@click.pass_obj
def analyze_codebase_cmd(cfg: AMXConfig, path: str, schema: str) -> None:
    """Analyze a codebase for database asset references."""
    from amx.codebase.analyzer import analyze_codebase
    from amx.db.connector import DatabaseConnector

    db = DatabaseConnector(cfg.db)
    tables = db.list_tables(schema)

    info(f"Scanning {path} for references to {len(tables)} tables...")
    report = analyze_codebase(path, tables)

    info(f"Scanned {report.scanned_files}/{report.total_files} files")
    if report.references:
        rows = [
            [asset, str(len(refs)), refs[0].file if refs else ""]
            for asset, refs in sorted(report.references.items())
        ]
        render_table("Asset references found", ["Asset", "Ref Count", "Example File"], rows[:30])
    else:
        warn("No references found.")


# ── Config Commands ─────────────────────────────────────────────────────────


@main.command("config")
@click.pass_obj
def show_config(cfg: AMXConfig) -> None:
    """Display current configuration."""
    info(f"Database: {cfg.db.user}@{cfg.db.host}:{cfg.db.port}/{cfg.db.database}")
    info(f"LLM: {cfg.llm.provider}/{cfg.llm.model}")
    info(f"Document paths: {cfg.doc_paths or 'none'}")
    info(f"Codebase paths: {cfg.code_paths or 'none'}")
    info(f"Selected schemas: {cfg.selected_schemas or 'all'}")


if __name__ == "__main__":
    main()
