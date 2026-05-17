"""Console-side print helpers for the AMX REPL session.

Extracted from :mod:`amx.cli_support.session` so the namespace tab bar,
the per-namespace hint, and the (very long) per-namespace ``/help``
renderer live in one focused module. None of them touches REPL state
beyond reading config; they only emit Rich-styled lines.

``session.py`` re-exports each name so internal call sites and the
test that monkeypatches ``_print_tab_bar`` keep working unchanged.
"""

from __future__ import annotations

from collections.abc import Callable

from amx.config import SUPPORTED_BACKENDS, AMXConfig
from amx.utils.console import console, heading, info

PrintDbHint = Callable[[], None]


def _canonical_namespace(namespace: str) -> str:
    """Mirror of :func:`amx.cli_support.session._canonical_namespace`."""
    return "metadata" if namespace == "manual" else namespace


_TAB_ORDER = [
    "root",
    "db",
    "metadata",
    "docs",
    "llm",
    "code",
    "analyze",
    "search",
    "history",
    "lineage",
]


def _print_tab_bar(namespace: str) -> None:
    """Render the namespace tab strip as console output (not as part of the
    prompt). Keeping it above the variable-length hint anchors it to a
    stable line right after the banner — otherwise the bar visibly jumps
    each time the hint above it changes height between namespaces.
    """
    curr = namespace or "root"
    parts: list[str] = []
    for tab in _TAB_ORDER:
        if tab == curr:
            parts.append(f"[bold cyan][ {tab.upper()} ][/bold cyan]")
        else:
            parts.append(f"[grey50]{tab}[/grey50]")
    console.print("  ".join(parts))


def _print_namespace_hint(
    namespace: str,
    cfg: AMXConfig,
    *,
    version: str,
    print_interactive_startup_summary: Callable[[AMXConfig], None],
    print_db_namespace_hint: PrintDbHint,
) -> None:
    _print_tab_bar(namespace)
    if not namespace:
        heading("AMX Interactive Session")
        print_interactive_startup_summary(cfg)
        info("Type /help for commands, /back to return, /exit to quit (from any namespace).")
    elif namespace == "db":
        print_db_namespace_hint()
    elif _canonical_namespace(namespace) == "metadata":
        info("Inspect, edit, and monitor database metadata. Document sources are under /docs.")
    elif namespace == "docs":
        info("Manage RAG document paths for schema context. Use /add-doc-profile to map paths.")
    elif namespace == "llm":
        info(
            "Manage LLM profiles, metadata generation language, and cost settings. Search answers follow the user's question language."
        )
    elif namespace == "code":
        info("Scan your codebase to find how tables are used. Run /code-scan after adding a path.")
    elif namespace == "analyze":
        info("Run the AMX pipeline (/run) to generate metadata, or (/apply) to push to your DB.")
    elif namespace == "search":
        info("Search generated/manual metadata, join candidates, and code usage evidence.")
    elif namespace == "history":
        info("View past metadata extractions. Use /review to inspect results.")
    elif namespace == "lineage":
        info(
            "Render column-level lineage diagrams from cached metadata. "
            "Each subcommand walks a picker — /create starts a guided wizard."
        )


def _print_session_help(*, namespace: str, cfg: AMXConfig) -> None:
    active = cfg.active_db_profile or "default"
    ctx_schema = cfg.current_schema or "(not set)"
    ctx_table = cfg.current_table or "(not set)"
    out = console

    if namespace == "db":
        engines = ", ".join(SUPPORTED_BACKENDS)
        out.print(
            f"""
[heading]Help — /db namespace[/heading]
Engines: [info]{engines}[/info] — each profile stores one backend. /add-db-profile asks which engine first.

Context:
  Active DB profile: [info]{active}[/info]
  Current schema: [info]{ctx_schema}[/info]
  Current table:  [info]{ctx_table}[/info]

Commands (in order):
  1) /back                         Return to root namespace

  Profile management:
  2) /db-profiles                  List DB profiles (Backend + connection summary)
  3) /use-db [name]                Switch profile (interactive list shows [engine] per profile)
  4) /add-db-profile [name]        Create a new profile — pick engine, then connection details
  5) /edit-db-profile [name]       Edit an existing profile — current values prefilled,
                                   catalog/database validated against the live backend
  6) /remove-db-profile <name>     Remove a DB profile (cannot remove last)

  Profile settings:
  7) /profiling [mode] [max] [N]   Show/set profiling guardrails
  8) /tls [on|off] [ca|clear]      Show/set Databricks TLS settings

  Connection & inspection:
  9) /connect                      Test DB connectivity
 10) /schemas                      List schemas
 11) /tables [schema]              List tables in <schema>
 12) /profile [schema] [table]     Profile a table — pass schema + table positionally
 13) /inspect [profile]            Diagnose a profile: backend, capabilities, connection
                                   test, visible schemas, table counts. Read-only.

  Team collaboration:
 14) /history-store                Configure shared run-history. Bare command opens an
                                   interactive picker — Status, Enable, Disable,
                                   Migrate from local, Flush pending, Dump DDL — based
                                   on whether shared mode is on. Power-user shortcuts
                                   also accept a subcommand directly (e.g.
                                   /history-store status).

Navigation:
  Esc (empty line)                 Go back to root namespace
  Esc (inside any prompt)          Soft-cancel the prompt (returns no answer);
                                   does not kill the session like Ctrl-C does
"""
        )
        return

    if namespace == "docs":
        out.print(
            """
[heading]Help — /docs namespace[/heading]
Commands (in order):
  1) /back                         Return to root namespace

  Profile management:
  2) /doc-profiles                 List document profiles (named path lists)
  3) /use-doc <name>               Switch active document profile
  4) /add-doc-profile [name]       Add/update document roots (interactive)
  5) /remove-doc-profile <name>    Remove a document profile

  Ingestion:
  6) /scan [paths...]              Scan (preview); optional `--doc-profile NAME`; else active profile or paths
  7) /ingest [paths...]            Ingest into RAG; `--doc-profile NAME`; `--refresh` replaces chunks for those sources

  Search & analysis:
  8) /search-docs <text>           Vector similarity over ingested docs (Chroma; no LLM answer)
  9) /doc-analyze [TABLE …]        Run RAG Agent standalone; results saved for next /run

  Export:
 10) /export-doc-report [FILE]     Export document RAG summary to a markdown file

Tip: configure sources first (steps 2–5), then scan/ingest, then /search-docs.

Navigation:
  Esc (empty line)                 Go back to root namespace
"""
        )
        return

    if _canonical_namespace(namespace) == "metadata":
        out.print(
            """
[heading]Help — /metadata namespace[/heading]
Database metadata commands. This namespace edits database/schema/table/column
comments. Document profiles and document search are under /docs.

Commands:
  1) /back                         Return to root namespace

  Inspection (read-only):
  2) /inspect [schema] [table]     Show current database/schema/table/column comments
  3) /monitor [schema]             Show table/view and column comment coverage

  Editing:
  4) /edit                         Interactive edit wizard. FIRST asks
                                   "Single entity" or "Bulk by name". If bulk,
                                   prompts for the entity name and walks the
                                   bulk-update flow (analysis → multi-select →
                                   one comment to all). If single, walks
                                   database → schema → table → column step by step.
  5) /edit <db>                    Edit a database/profile comment
  6) /edit <db>.<schema>           Edit a schema comment
  7) /edit <db>.<schema>.<table>   Edit a table/view comment
  8) /edit <db>.<schema>.<table>.<column>
                                  Edit a column comment
  9) /edit <name>                  Bare-name shortcut: AMX searches every table
                                   AND column matching <name> across all schemas,
                                   prints a bulk-update analysis (counts + match
                                   table), then offers bulk-vs-individual mode.
                                     * bulk       — pick rows (1,3,5 or 1-4 or all),
                                                   ONE comment written to every
                                                   selected entity.
                                     * individual — walk through each match one at
                                                   a time, different comment per row.
                                     * cancel     — abort.

Options:
  /edit ... --comment "text"       Provide the new comment non-interactively
  /edit ... --yes                  Skip confirmation

Compatibility:
  /manual                          Alias for /metadata

Navigation:
  Esc (empty line)                 Go back to root namespace
"""
        )
        return

    if namespace == "llm":
        out.print(
            """
[heading]Help — /llm namespace[/heading]
Commands (in order):
  1) /back                              Return to root namespace

  Profile management:
  2) /llm-profiles                      List LLM profiles
  3) /use-llm <name>                    Switch active LLM profile
  4) /add-llm-profile [name]            Add/update an LLM profile (interactive)
  5) /remove-llm-profile <name>         Remove an LLM profile

  Prompt control (input shape):
  6) /prompt-detail [level]             Show or set the prompt detail level
                                          Levels: minimal | standard | detailed | full
                                          Controls which DB fields are included in the LLM prompt.
                                          Run without args to show the current level + what each
                                          preset includes.
  7) /description-verbosity [level]     Show or set the OUTPUT description length
                                          Levels: brief (default) | detailed | comprehensive | exhaustive
                                          brief         = 1-2 sentences per column
                                          detailed      = 2-4 sentences with purpose, typical values,
                                                          and relationships when supported by evidence
                                          comprehensive = 1-2 short paragraphs (~5-8 sentences) adding
                                                          usage patterns and caveats
                                          exhaustive    = multi-paragraph reference-style entry; best
                                                          for documentation, not interactive runs
  8) /n-alternatives [N]                Show or set number of description alternatives per column
  9) /temperature [0.0-2.0]             Show or set LLM sampling temperature (default 0.2)

  Batching (throughput):
 10) /llm-batch-size [N]                Show or set number of columns processed in one LLM call
                                          Range: 1 – 5  (default: 3)
 11) /batch-context-columns [off|all|N] Show or set how many non-batch column names are added
                                         as context in every profile batch prompt

  Alternatives diversity:
 12a) /alternatives-mode [mode]         Show or set alternatives diversity mode for the active profile
                                          semantic = SAME meaning, different wording — paraphrase DESCRIPTION_1 (default)
                                          lexical  = SHARED vocabulary, meaning may shift through added nuances

  Confidence:
 12) /logprob-thresholds [high] [med]   Show or set logprob confidence thresholds used to bucket
                                         per-column results into high / medium / low

Model examples (what to type in "Model name"):
  - openai      -> gpt-4o
  - openrouter  -> openai/gpt-4o-mini
                 -> anthropic/claude-3.5-sonnet
                 -> qwen/qwen3.6-plus
  - anthropic   -> claude-sonnet-4-20250514
  - gemini      -> gemini-2.0-flash
  - deepseek    -> deepseek-chat
  - ollama      -> llama3

Navigation:
  Esc (empty line)                      Go back to root namespace
"""
        )
        return

    if namespace == "code":
        out.print(
            """
[heading]Help — /code namespace[/heading]
Commands (in order):
  1) /back                         Return to root namespace

  Profile management:
  2) /code-profiles                List codebase profiles
  3) /use-code <name>              Switch active codebase profile
  4) /add-code-profile [name]      Add/update a codebase path (interactive)
  5) /remove-code-profile <name>   Remove a codebase profile

  Scanning:
  6) /code-scan [path] [--schema …] [--code-profile NAME]   Scan codebase, save results + semantic index
  7) /code-refresh [--code-profile NAME]                    Clear cache + semantic index
  8) /code-results [--code-profile NAME]                    Show last cached scan results

  Analysis:
  9) /code-analyze [TABLE …] [--schema …]  Run Code Agent standalone; results saved for next /run

  Export:
 10) /export-code-report [FILE]    Export scan results to markdown

Navigation:
  Esc (empty line)                 Go back to root namespace
"""
        )
        return

    if namespace == "analyze":
        out.print(
            """
[heading]Help — /analyze namespace[/heading]
Commands (in order):
  1) /back                         Return to root namespace
  2) /run [ASSET …] [--schema …] [--table …] [--apply] [--code-refresh] [--code-profile NAME]
                                   Run all agents with scope picker:
                                     Database — all schemas, all assets
                                     Schema   — select schema(s), all assets
                                     Asset    — specific tables/views
                                     Default  — interactive picker if no scope flags
  3) /run-apply [ASSET …] [--schema …] [--table …]   Same as /run --apply
  4) /apply                        Write pending comments to the database

Tip: scan code and docs first (`/code-scan`, `/doc-analyze`, `/code-analyze`), then `/run`.

Navigation:
  Esc (empty line)                 Go back to root namespace
"""
        )
        return

    if namespace == "search":
        out.print(
            """
[heading]Help — /search namespace[/heading]
Commands:
  1) /back                                     Return to root namespace

  Ask:
  2) /ask [--actions] <question>               Ask a metadata question; --actions prompts before running follow-up actions
  3) <question>                                In /search, plain text is treated like /ask

  Status:
  4) /status                                   Catalog health, LLM readiness, and last sync jobs
  5) /sources                                  Enabled sources, settings, and evidence coverage

  Configure:
  6) /config [key] [value]                     Show or update search settings
  7) /embeddings [kind] [model]                Show or change the search-index embedding provider
                                               (MiniLM default, OpenAI-compatible, or local sentence-transformers).
                                               Run /rebuild after switching to re-embed the catalog.

  Sync & rebuild:
  8) /sync [--schema …] [--table …]            Sync DB structure/comments + cached code evidence
  9) /rebuild                                  Rebuild effective search state and vector index
"""
        )
        return

    if namespace == "history":
        out.print(
            """
[heading]Help — /history namespace[/heading]
Commands:
  1) /back                                    Return to root namespace

  Browse:
  2) /list [-n N]                             Show recent analyze runs from SQLite
  3) /show <run_id>                           Show full JSON payload for one run
  4) /stats                                   Aggregate run/event stats
  5) /events [-n N]                           Recent app events

  Audit & replay:
  6) /results <run_id>                        Show all saved LLM alternatives for a run
  7) /review <run_id> [--unevaluated-only]    Re-evaluate alternatives for a past run
                         [--apply]            Write approved descriptions to the database
  8) /compare [run_ids…|--last N|--schema …|--table …|--by KEY]
                                              Pivot past runs of the same assets to compare
                                              models, doc/code profiles, prompt-detail levels,
                                              or batch sizes side-by-side (descriptions,
                                              confidence, logprob_score, timing, tokens).

SQLite file:
  ~/.amx/history.db
"""
        )
        return

    if namespace == "lineage":
        out.print(
            """
[heading]Help — /lineage namespace[/heading]
Cache-first, wizard-driven column-level lineage diagrams. Every subcommand
walks an ask_choice/ask picker chain when invoked bare — no flag is
required. Power-user flags exist but are optional.

Commands:
  1) /back                       Return to root namespace
  2) /create                     Wizard: profile → schema → table → (column)
                                 → format → depth → cache strategy → render
  3) /list                       Show all rendered artifacts in a table
  4) /open                       Picker over existing artifacts; opens the file
  5) /refresh                    Picker over artifacts; re-extracts + re-renders
  6) /delete                     Picker over artifacts; removes row + file
  7) /show                       Wizard for anchor; text-mode tree (no image)

Cache strategies (asked at the end of /create and /refresh):
  • cache-only           — never call the DB, use only what's cached (default)
  • ask if needed        — mid-run prompt if a cache miss occurs
  • fetch from DB        — auto-confirm cache fill before rendering
  • force fresh          — invalidate view-DDL cache and re-pull from DB

Artifacts:
  ~/.amx/lineage/<slug>.<format>     (default output location)
"""
        )
        return

    out.print(
        f"""
[heading]Help — root[/heading]
Context:
  Active DB profile: [info]{active}[/info]
  Current schema: [info]{ctx_schema}[/info]
  Current table:  [info]{ctx_table}[/info]

Getting started (in order):
  1) /setup                        First-time wizard (DB + LLM + sources)
  2) /config                       Show current configuration

  Setup:
  3) /db                           Database introspection + DB profiles
  4) /llm                          LLM profile management

  Evidence:
  5) /docs                         Document roots + RAG (scan/ingest/search-docs)
  6) /code                         Codebase profile management
  7) /metadata                     Database metadata editing and coverage monitoring

  Run analysis:
  8) /analyze                      Metadata inference (/run, /apply, …)

  Review:
  9) /search                       LLM-backed metadata discussion grounded on the search catalog
 10) /history                      Local SQLite history (/list, /show, /stats, /events)

Inside namespaces (examples):
  [bright_white]/db[/bright_white]   → /db-profiles, /add-db-profile, /edit-db-profile, /connect, /history-store, …
  [bright_white]/llm[/bright_white]   → /llm-profiles, /add-llm-profile, …
  [bright_white]/docs[/bright_white] → /doc-profiles, /add-doc-profile, /ingest, …
  [bright_white]/code[/bright_white] → /code-profiles, /add-code-profile, …
  [bright_white]/metadata[/bright_white] → /inspect, /edit, /monitor
  [bright_white]/search[/bright_white] → /ask, /status, /sync, or just type a metadata question

Global shortcuts (work anywhere):
  /save                            Persist ~/.amx/config.yml
  /clear                           Clear terminal output (keep session running)

Navigation:
  Esc (empty line)                 Go back one level (namespace → root)

Examples:
  [bright_white]/db[/bright_white]
  [bright_white]/connect[/bright_white]
  [bright_white]/schemas[/bright_white]
  [bright_white]/tables sap_s6p[/bright_white]
  [bright_white]/profile sap_s6p t001[/bright_white]
  [bright_white]/analyze[/bright_white]
  [bright_white]/code-scan https://github.com/org/repo --schema sap_s6p[/bright_white]
  [bright_white]/code-analyze vbrk vbrp[/bright_white]  (run Code Agent standalone on specific tables)
  [bright_white]/doc-analyze vbrk[/bright_white]  (run RAG Agent standalone)
"""
    )
