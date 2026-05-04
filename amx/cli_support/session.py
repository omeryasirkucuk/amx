"""Interactive session helpers for the AMX CLI."""

from __future__ import annotations

import os
import shlex
import signal
from collections.abc import Callable

import click
from prompt_toolkit.completion import Completer, Completion
from prompt_toolkit.filters import Condition
from prompt_toolkit.formatted_text import HTML
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.shortcuts import CompleteStyle, PromptSession
from prompt_toolkit.styles import Style

from amx.cli_support.commands.db import (
    cmd_add_profile as _cmd_add_profile,
)
from amx.cli_support.commands.db import (
    cmd_cleanup_placeholders as _cmd_cleanup_placeholders,
)
from amx.cli_support.commands.db import (
    cmd_inspect as _cmd_inspect,
)
from amx.cli_support.commands.db import (
    cmd_profiles as _cmd_profiles,
)
from amx.cli_support.commands.db import (
    cmd_profiling as _cmd_profiling,
)
from amx.cli_support.commands.db import (
    cmd_remove_profile as _cmd_remove_profile,
)
from amx.cli_support.commands.db import (
    cmd_use as _cmd_use,
)
from amx.cli_support.commands.embeddings import cmd_embeddings as _cmd_embeddings
from amx.cli_support.commands.profiles import (
    cmd_add_code_profile as _cmd_add_code_profile,
)
from amx.cli_support.commands.profiles import (
    cmd_add_doc_profile as _cmd_add_doc_profile,
)
from amx.cli_support.commands.profiles import (
    cmd_add_llm_profile as _cmd_add_llm_profile,
)
from amx.cli_support.commands.profiles import (
    cmd_batch_context_columns as _cmd_batch_context_columns,
)
from amx.cli_support.commands.profiles import (
    cmd_code_profiles as _cmd_code_profiles,
)
from amx.cli_support.commands.profiles import (
    cmd_description_verbosity as _cmd_description_verbosity,
)
from amx.cli_support.commands.profiles import (
    cmd_doc_profiles as _cmd_doc_profiles,
)
from amx.cli_support.commands.profiles import (
    cmd_llm_batch_size as _cmd_llm_batch_size,
)
from amx.cli_support.commands.profiles import (
    cmd_llm_profiles as _cmd_llm_profiles,
)
from amx.cli_support.commands.profiles import (
    cmd_logprob_thresholds as _cmd_logprob_thresholds,
)
from amx.cli_support.commands.profiles import (
    cmd_n_alternatives as _cmd_n_alternatives,
)
from amx.cli_support.commands.profiles import (
    cmd_prompt_detail as _cmd_prompt_detail,
)
from amx.cli_support.commands.profiles import (
    cmd_remove_code_profile as _cmd_remove_code_profile,
)
from amx.cli_support.commands.profiles import (
    cmd_remove_doc_profile as _cmd_remove_doc_profile,
)
from amx.cli_support.commands.profiles import (
    cmd_remove_llm_profile as _cmd_remove_llm_profile,
)
from amx.cli_support.commands.profiles import (
    cmd_temperature as _cmd_temperature,
)
from amx.cli_support.commands.profiles import (
    cmd_use_code as _cmd_use_code,
)
from amx.cli_support.commands.profiles import (
    cmd_use_doc as _cmd_use_doc,
)
from amx.cli_support.commands.profiles import (
    cmd_use_llm as _cmd_use_llm,
)
from amx.cli_support.commands.profiles import (
    cmd_use_rag_llm as _cmd_use_rag_llm,
)
from amx.cli_support.commands.usage import cmd_usage as _cmd_usage
from amx.cli_support.slash_commands import (
    cmd_heads_for_namespace as _registry_cmd_heads,
)
from amx.cli_support.slash_commands import (
    commands_for_namespace as _registry_commands_for_namespace,
)
from amx.config import SUPPORTED_BACKENDS, AMXConfig
from amx.utils.console import console, error, heading, info, success, warn

LogEvent = Callable[..., None]
WarnNoPaths = Callable[..., None]
NormalizeArgs = Callable[[list[str], AMXConfig], list[str]]
PrintDbHint = Callable[[], None]

_NS_STATE: dict[str, str] = {"namespace": ""}


def _canonical_namespace(namespace: str) -> str:
    return "metadata" if namespace == "manual" else namespace


def _handle_manual_usage_shortcuts(namespace: str, parts: list[str]) -> bool:
    """Show guided metadata-edit workflow for incomplete edit commands."""
    return False


def _format_session_click_error(cmdline: str, exc: click.ClickException) -> str:
    """Render slash-session-friendly Click errors."""
    if isinstance(exc, click.UsageError) and "No such command" in str(exc):
        return f"Unknown command: /{cmdline}. Type /help."
    return str(exc)


def _kb_escape_namespace() -> KeyBindings:
    kb = KeyBindings()

    @Condition
    def _is_buffer_empty() -> bool:
        from prompt_toolkit.application.current import get_app

        return len(get_app().current_buffer.text) == 0

    tabs = ["", "db", "metadata", "docs", "llm", "code", "analyze", "search", "history"]

    @kb.add("escape")
    def _(event) -> None:  # type: ignore[no-untyped-def]
        buf = event.app.current_buffer
        if buf.text:
            buf.reset()
            return
        namespace = _NS_STATE.get("namespace", "")
        if namespace:
            _NS_STATE["namespace"] = ""
            event.app.exit(result="__amx_esc_back__")
        else:
            event.app.exit(result="__amx_esc_root__")

    @kb.add("right", filter=_is_buffer_empty)
    def _(event) -> None:  # type: ignore[no-untyped-def]
        curr = _NS_STATE.get("namespace", "")
        idx = tabs.index(curr) if curr in tabs else 0
        event.app.exit(result=f"__amx_switch_ns__:{tabs[(idx + 1) % len(tabs)]}")

    @kb.add("left", filter=_is_buffer_empty)
    def _(event) -> None:  # type: ignore[no-untyped-def]
        curr = _NS_STATE.get("namespace", "")
        idx = tabs.index(curr) if curr in tabs else 0
        event.app.exit(result=f"__amx_switch_ns__:{tabs[(idx - 1) % len(tabs)]}")

    return kb


def _print_namespace_hint(
    namespace: str,
    cfg: AMXConfig,
    *,
    version: str,
    print_interactive_startup_summary: Callable[[AMXConfig], None],
    print_db_namespace_hint: PrintDbHint,
) -> None:
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
Engines: [cyan]{engines}[/cyan] — each profile stores one backend. /add-db-profile asks which engine first.

Context:
  Active DB profile: [cyan]{active}[/cyan]
  Current schema: [cyan]{ctx_schema}[/cyan]
  Current table:  [cyan]{ctx_table}[/cyan]

Commands (in order):
  1) /back                         Return to root namespace

  Profile management:
  2) /db-profiles                  List DB profiles (Backend + connection summary)
  3) /use-db [name]                Switch profile (interactive list shows [engine] per profile)
  4) /add-db-profile [name]        Create/update profile — pick PostgreSQL, Snowflake, Databricks, or BigQuery
  5) /remove-db-profile <name>     Remove a DB profile (cannot remove last)

  Profile settings:
  6) /profiling [mode] [max] [N]   Show/set profiling guardrails
  7) /tls [on|off] [ca|clear]      Show/set Databricks TLS settings

  Active context:
  8) /schema <name>                Set current schema context (used by /tables)
  9) /table <name>                 Set current table context (used by /profile)

  Connection & inspection:
 10) /connect                      Test DB connectivity
 11) /schemas                      List schemas
 12) /tables [schema]              List tables (defaults to current schema)
 13) /profile [schema] [table]     Profile a table (defaults to current context)
 14) /inspect [profile]            Diagnose a profile: backend, capabilities, connection
                                   test, visible schemas, table counts. Read-only.

  Team collaboration:
 15) /history-store                Configure shared run-history. Bare command opens an
                                   interactive picker — Status, Enable, Disable,
                                   Migrate from local, Flush pending, Dump DDL — based
                                   on whether shared mode is on. Power-user shortcuts
                                   also accept a subcommand directly (e.g.
                                   /history-store status).

  Maintenance:
 16) /cleanup-placeholders [schema]
                                   Remove auto-inference fallback placeholder strings
                                   ("Auto-inference missed a reliable description; please
                                   review manually.") from the live DB. Use this once when
                                   upgrading from pre-0.6.3 — newer versions never write
                                   these in the first place.

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
                                          Levels: brief (default) | detailed
                                          brief = 1 sentence per column
                                          detailed = 2-4 sentences with purpose, typical values,
                                          and relationships when supported by evidence.
  8) /n-alternatives [N]                Show or set number of description alternatives per column
  9) /temperature [0.0-2.0]             Show or set LLM sampling temperature (default 0.2)

  Batching (throughput):
 10) /llm-batch-size [N]                Show or set number of columns processed in one LLM call
                                          Range: 1 – 5  (default: 3)
 11) /batch-context-columns [off|all|N] Show or set how many non-batch column names are added
                                         as context in every profile batch prompt

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
                                     Default  — current /db schema and optional /table
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

    out.print(
        f"""
[heading]Help — root[/heading]
Context:
  Active DB profile: [cyan]{active}[/cyan]
  Current schema: [cyan]{ctx_schema}[/cyan]
  Current table:  [cyan]{ctx_table}[/cyan]

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
  [bright_white]/db[/bright_white]   → /db-profiles, /schema, /table, /connect, /history-store, …
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
  [bright_white]/schema sap_s6p[/bright_white]
  [bright_white]/tables[/bright_white]
  [bright_white]/table t001[/bright_white]
  [bright_white]/profile[/bright_white]
  [bright_white]/analyze[/bright_white]
  [bright_white]/code-scan[/bright_white]  (after /schema …, uses active /code profile path)
  [bright_white]/code-scan https://github.com/org/repo --schema sap_s6p[/bright_white]
  [bright_white]/code-analyze vbrk vbrp[/bright_white]  (run Code Agent standalone on specific tables)
  [bright_white]/doc-analyze vbrk[/bright_white]  (run RAG Agent standalone)
"""
    )


class _SlashCompleter(Completer):
    def __init__(self, namespace_cb: Callable[[], str], cfg: AMXConfig):
        self._namespace_cb = namespace_cb
        self._cfg = cfg

    def get_completions(self, document, complete_event):
        text = document.text_before_cursor
        if not text.startswith("/"):
            return

        namespace = self._namespace_cb()
        partial = text[1:]
        for cmd, meta in _slash_command_catalog(namespace, self._cfg):
            if cmd[1:].startswith(partial):
                yield Completion(cmd, start_position=-len(text), display_meta=meta)


def _slash_command_catalog(namespace: str, cfg: AMXConfig) -> list[tuple[str, str]]:
    """Return ``(slash_command, short_description)`` pairs for autocomplete.

    Pre-v0.9.3 this function carried hand-maintained lists for every
    namespace — the same data was also duplicated in the dispatch
    chain's ``*_cmd_heads`` frozensets, in ``_print_session_help``
    blocks, and in ``run_interactive_session``. The
    ``amx.cli_support.slash_commands`` registry is now the single
    source of truth; this function is just an adapter that converts
    :class:`SlashCommand` records into the ``(slash, desc)`` tuples
    the autocompleter expects.
    """
    canonical = _canonical_namespace(namespace) if namespace else ""
    cmds = _registry_commands_for_namespace(canonical or namespace)
    return [(c.command, c.short_desc) for c in cmds]


def _require_namespace(cmd: str, namespace: str, expected: str, replacement: str) -> bool:
    """Allow cross-namespace slash commands.

    Slash commands carry their own namespace in the name (e.g. ``/llm-profiles``,
    ``/db-profiles``). Refusing to execute them just because the user happens to
    be in a different tab is friction without value — every handler operates on
    ``cfg`` and doesn't care about the current namespace. We still emit a one-
    line note when the command is dispatched cross-namespace, so the user can
    learn the canonical home if they didn't already know it.
    """
    if namespace and namespace != expected:
        info(f"Running /{cmd} from /{namespace} (canonical home: /{expected}).")
    return True


def _run_ask_repl(
    cfg: AMXConfig,
    *,
    main_command: click.Group,
    log_event: LogEvent,
) -> None:
    """Drop into a sticky ``ask>`` REPL when ``/ask`` is typed alone.

    Each non-empty line is dispatched as a ``/search ask <line>`` invocation,
    re-using the same conversational session pointer (``cfg.active_chat_session_id``)
    so follow-up turns ("any others?", "what about its columns?") are linked.

    Exits on ``/exit``, ``/quit``, ``/back``, an empty line + Ctrl-D, or Ctrl-C.
    Any other line that begins with ``/`` is rejected with a hint — REPL mode is
    deliberately question-only so users don't accidentally run unrelated CLI
    commands while mid-conversation.
    """
    sid = cfg.active_chat_session_id
    sid_label = f"#{sid}" if sid else "new"
    heading(f"Ask mode (session {sid_label})")
    info("Type a question, press Enter. /exit (or Ctrl-D on an empty line) to leave.")
    # Mirror the active session id into the environment BEFORE the first
    # ``main_command.main()`` so ``AMXConfig.load`` picks it up. Without this
    # bridge each invocation re-opens a fresh chat session and follow-ups
    # lose prior context. We clear the variable when the user explicitly
    # /resume's a different session — handled inside the agent on next set.
    if sid:
        os.environ["AMX_CHAT_SESSION_ID"] = str(int(sid))
    else:
        # Brand-new REPL — drop any stale id so a fresh session is created
        # on the first question.
        os.environ.pop("AMX_CHAT_SESSION_ID", None)

    inner = PromptSession(
        message=HTML("<ansicyan><b>ask&gt;</b></ansicyan> "),
        mouse_support=False,
    )
    while True:
        try:
            line = inner.prompt().strip()
        except EOFError:
            console.print()
            success("Left ask mode.")
            return
        except KeyboardInterrupt:
            console.print()
            success("Left ask mode.")
            return

        if not line:
            continue
        # Allow the user to escape the REPL with familiar slash verbs without
        # needing to remember "press Ctrl-D on an empty line".
        if line in {"/exit", "/quit", "/q", "/back", "exit", "quit", "q", "back"}:
            success("Left ask mode.")
            return
        if line.startswith("/"):
            warn(
                "Inside /ask only questions are accepted. /exit to leave, "
                "then run any slash command from the main prompt."
            )
            continue

        previous = os.environ.get("AMX_SESSION_CHILD")
        os.environ["AMX_SESSION_CHILD"] = "1"
        try:
            main_command.main(args=["search", "ask", line], prog_name="amx", standalone_mode=False)
        except click.ClickException as exc:
            error(_format_session_click_error(f"ask {line}", exc))
        except SystemExit:
            pass
        except Exception as exc:  # pragma: no cover
            error(f"Ask failed: {exc}")
        finally:
            if previous is None:
                os.environ.pop("AMX_SESSION_CHILD", None)
            else:
                os.environ["AMX_SESSION_CHILD"] = previous


def _handle_session_builtin(
    cfg: AMXConfig,
    namespace: str,
    parts: list[str],
    *,
    log_event: LogEvent,
) -> bool | str:
    head = parts[0]
    if head in {"profiles", "use", "add-profile", "remove-profile"}:
        error(
            f"/{head} was renamed — use /db (then /db-profiles, /use-db, /add-db-profile, /remove-db-profile)."
        )
        return True
    if head == "llm-profiles":
        if not _require_namespace(head, namespace, "llm", "llm-profiles"):
            return True
        _cmd_llm_profiles(cfg)
        return True
    if head == "use-llm":
        if not _require_namespace(head, namespace, "llm", "use-llm"):
            return True
        _cmd_use_llm(cfg, parts[1:])
        return True
    if head == "use-rag-llm":
        if not _require_namespace(head, namespace, "llm", "use-rag-llm"):
            return True
        _cmd_use_rag_llm(cfg, parts[1:])
        return True
    if head == "add-llm-profile":
        if not _require_namespace(head, namespace, "llm", "add-llm-profile"):
            return True
        _cmd_add_llm_profile(cfg, parts[1:])
        return True
    if head == "remove-llm-profile":
        if not _require_namespace(head, namespace, "llm", "remove-llm-profile"):
            return True
        _cmd_remove_llm_profile(cfg, parts[1:])
        return True
    if head == "prompt-detail":
        if not _require_namespace(head, namespace, "llm", "prompt-detail"):
            return True
        _cmd_prompt_detail(cfg, parts[1:])
        return True
    if head == "description-verbosity":
        if not _require_namespace(head, namespace, "llm", "description-verbosity"):
            return True
        _cmd_description_verbosity(cfg, parts[1:])
        return True
    if head == "n-alternatives":
        if not _require_namespace(head, namespace, "llm", "n-alternatives"):
            return True
        _cmd_n_alternatives(cfg, parts[1:])
        return True
    if head == "llm-batch-size":
        if not _require_namespace(head, namespace, "llm", "llm-batch-size"):
            return True
        _cmd_llm_batch_size(cfg, parts[1:])
        return True
    if head == "batch-context-columns":
        if not _require_namespace(head, namespace, "llm", "batch-context-columns"):
            return True
        _cmd_batch_context_columns(cfg, parts[1:])
        return True
    if head == "logprob-thresholds":
        if not _require_namespace(head, namespace, "llm", "logprob-thresholds"):
            return True
        _cmd_logprob_thresholds(cfg, parts[1:])
        return True
    if head == "temperature":
        if not _require_namespace(head, namespace, "llm", "temperature"):
            return True
        _cmd_temperature(cfg, parts[1:])
        return True
    if head == "doc-profiles":
        if not _require_namespace(head, namespace, "docs", "doc-profiles"):
            return True
        _cmd_doc_profiles(cfg)
        return True
    if head == "use-doc":
        if not _require_namespace(head, namespace, "docs", "use-doc"):
            return True
        _cmd_use_doc(cfg, parts[1:])
        return True
    if head == "add-doc-profile":
        if not _require_namespace(head, namespace, "docs", "add-doc-profile"):
            return True
        _cmd_add_doc_profile(cfg, parts[1:])
        return True
    if head == "remove-doc-profile":
        if not _require_namespace(head, namespace, "docs", "remove-doc-profile"):
            return True
        _cmd_remove_doc_profile(cfg, parts[1:])
        return True
    if head == "code-profiles":
        if not _require_namespace(head, namespace, "code", "code-profiles"):
            return True
        _cmd_code_profiles(cfg)
        return True
    if head == "use-code":
        if not _require_namespace(head, namespace, "code", "use-code"):
            return True
        _cmd_use_code(cfg, parts[1:])
        return True
    if head == "add-code-profile":
        if not _require_namespace(head, namespace, "code", "add-code-profile"):
            return True
        _cmd_add_code_profile(cfg, parts[1:])
        return True
    if head == "remove-code-profile":
        if not _require_namespace(head, namespace, "code", "remove-code-profile"):
            return True
        _cmd_remove_code_profile(cfg, parts[1:])
        return True
    if head == "db-profiles":
        if not _require_namespace(head, namespace, "db", "db-profiles"):
            return True
        _cmd_profiles(cfg)
        return True
    if head == "use-db":
        if not _require_namespace(head, namespace, "db", "use-db"):
            return True
        _cmd_use(cfg, parts[1:], log_event=log_event)
        return True
    if head == "add-db-profile":
        if not _require_namespace(head, namespace, "db", "add-db-profile"):
            return True
        _cmd_add_profile(cfg, parts[1:], log_event=log_event)
        return True
    if head == "remove-db-profile":
        if not _require_namespace(head, namespace, "db", "remove-db-profile"):
            return True
        _cmd_remove_profile(cfg, parts[1:])
        return True
    if head == "profiling":
        if not _require_namespace(head, namespace, "db", "profiling"):
            return True
        _cmd_profiling(cfg, parts[1:])
        return True
    if head == "tls":
        if not _require_namespace(head, namespace, "db", "tls"):
            return True
        from amx.cli_support.commands.db import cmd_tls as _cmd_tls

        _cmd_tls(cfg, parts[1:])
        return True
    if head == "inspect":
        # Lives under /db so it shares the namespace of the other DB
        # commands. /inspect [profile] dumps backend, connection summary,
        # capabilities, connection-test result, and per-schema table counts
        # so users can self-diagnose connector and permission problems.
        if not _require_namespace(head, namespace, "db", "inspect"):
            return True
        _cmd_inspect(cfg, parts[1:])
        return True
    if head == "cleanup-placeholders":
        # /db cleanup-placeholders [schema] — one-shot cleanup of legacy
        # ``Auto-inference missed a reliable description; please review
        # manually.`` placeholder strings written to the live DB by older
        # /run-apply runs. v0.6.3+ no longer writes them in the first place.
        if not _require_namespace(head, namespace, "db", "cleanup-placeholders"):
            return True
        _cmd_cleanup_placeholders(cfg, parts[1:])
        return True
    if head == "save":
        path = cfg.save()
        success(f"Saved configuration to {path}")
        return True
    if head == "usage":
        # Top-level: /usage summarises LLM cost from local history without
        # requiring the user to enter any namespace first. No network call.
        _cmd_usage(cfg, parts[1:])
        return True
    if head in {"embeddings", "embedding"}:
        # Lives under /search since switching the embedding provider only
        # affects the search index. ``embedding`` (singular) is accepted as
        # a typo-friendly alias. When typed at the root tab, the auto-
        # namespace logic (search_cmd_heads) shifts the user into /search
        # and prints "Assumed /search namespace for this command." — the
        # same UX as /add-db-profile.
        if not _require_namespace(head, namespace, "search", head):
            return True
        _cmd_embeddings(cfg, parts[1:])
        return True
    if head == "schema":
        if not _require_namespace(head, namespace, "db", "schema"):
            return True
        if len(parts) < 2:
            error("Usage: /schema <name> (inside /db)")
            return True
        cfg.current_schema = parts[1]
        cfg.save()
        info(f"Current schema set to: {cfg.current_schema}")
        return True
    if head == "table":
        if not _require_namespace(head, namespace, "db", "table"):
            return True
        if len(parts) < 2:
            error("Usage: /table <name> (inside /db)")
            return True
        cfg.current_table = parts[1]
        cfg.save()
        info(f"Current table set to: {cfg.current_table}")
        return True
    return False


def session_to_click_args(namespace: str, parts: list[str]) -> list[str] | None:
    head = parts[0]
    shortcut_map = {
        "connect": ["db", "connect"],
        "schemas": ["db", "schemas"],
        "tables": ["db", "tables"],
        "profile": ["db", "profile"],
        "scan": ["docs", "scan"],
        "ingest": ["docs", "ingest"],
        "search-docs": ["docs", "search-docs"],
        "doc-analyze": ["docs", "analyze"],
        "export-doc-report": ["docs", "export-report"],
        "inspect": ["metadata", "inspect"],
        "edit": ["metadata", "edit"],
        "monitor": ["metadata", "monitor"],
        "run": ["analyze", "run"],
        "run-apply": ["analyze", "run", "--apply"],
        "apply": ["analyze", "apply"],
        "code-scan": ["code", "scan"],
        "code-refresh": ["code", "refresh"],
        "code-results": ["code", "results"],
        "code-analyze": ["code", "analyze"],
        "export-code-report": ["code", "export-report"],
        "ask": ["search", "ask"],
        "status": ["search", "status"],
        "sources": ["search", "sources"],
        "sync": ["search", "sync"],
        "rebuild": ["search", "rebuild"],
        "setup": ["setup"],
        "config": ["config"],
        "help": ["--help"],
        # /doctor is cross-namespace: registered as a top-level Click
        # subcommand and listed in _ROOT_BUILTINS with cross_namespace=True.
        # Without this entry, typing `/doctor` from /llm, /db, etc. would
        # fall through to `[namespace, "doctor"]` which Click rejects, and
        # from /search it would fall through to `["search", "ask",
        # "doctor"]` — sending the literal string "doctor" to the search
        # agent as a question, which silently "looks like it worked".
        "doctor": ["doctor"],
        # /compare lives under /history (audit operation, not search).
        # Same dispatch story as /doctor: from /search it'd otherwise
        # be swallowed as a question, from /db etc. it'd hit Click as
        # an unknown subcommand. The shortcut maps it to the correct
        # namespace from anywhere.
        "compare": ["history", "compare"],
        # /history-store lives under /db (it manages a database
        # resource — a saved DB profile that hosts the AMX schema).
        # When typed from outside /db this shortcut routes it; when
        # typed from inside /db the namespace+head fallthrough below
        # already produces ["db", "history-store", ...].
        "history-store": ["db", "history-store"],
    }
    if head == "search" and len(parts) > 1:
        if parts[1] in {
            "ask",
            "status",
            "sources",
            "config",
            "sync",
            "rebuild",
            "find-columns",
            "join-candidates",
            "explain",
            "explain-table",
        }:
            return parts
        return ["search", "ask"] + parts[1:]
    if namespace == "search":
        if head in {"ask", "status", "sources", "config", "sync", "rebuild"}:
            return ["search"] + parts
        if head in {"find-columns", "join-candidates", "explain", "explain-table"}:
            return ["search"] + parts
        # Before swallowing the line as `/search ask <head>`, see if the
        # command is a known cross-namespace shortcut (e.g. /run, /apply,
        # /llm-profiles). If so, route it to the correct namespace instead
        # of asking the LLM to "interpret" it as a question.
        if head in shortcut_map:
            return shortcut_map[head] + parts[1:]
        if head in {"db", "metadata", "manual", "docs", "llm", "code", "analyze", "history"}:
            if head == "manual":
                return ["metadata"] + parts[1:]
            return parts
        # Unknown slash command typed inside /search. Bare-text questions are
        # already rewritten to ``/ask <text>`` upstream (see the input loop's
        # ``not raw.startswith("/")`` branch), so anything still landing here
        # has an explicit leading slash from the user — i.e. they meant a
        # command, not a question. Return ``None`` so the caller surfaces
        # "Unknown command: /<x>" instead of silently routing the typo into
        # the search agent (e.g. ``/asl`` → ``search ask asl``).
        return None
    if head in {
        "db",
        "metadata",
        "manual",
        "docs",
        "llm",
        "code",
        "analyze",
        "search",
        "history",
        "session",
        "setup",
        "config",
    }:
        if head == "manual":
            return ["metadata"] + parts[1:]
        return parts
    if namespace and head in shortcut_map:
        return shortcut_map[head] + parts[1:]
    if head in shortcut_map:
        return shortcut_map[head] + parts[1:]
    if namespace:
        return [namespace] + parts
    return None


def inject_session_defaults(cfg: AMXConfig, namespace: str, args: list[str]) -> list[str]:
    if not args:
        return args
    if args[:2] == ["db", "tables"] and len(args) == 2 and cfg.current_schema:
        return ["db", "tables", cfg.current_schema]
    if args[:2] == ["db", "profile"]:
        if len(args) == 2 and cfg.current_schema and cfg.current_table:
            return ["db", "profile", cfg.current_schema, cfg.current_table]
        if len(args) == 3 and cfg.current_table:
            return ["db", "profile", args[2], cfg.current_table]
    if (
        len(args) >= 2
        and args[0] == "code"
        and args[1] == "scan"
        and "--schema" not in args
        and "-s" not in args
        and cfg.current_schema
    ):
        return args + ["--schema", cfg.current_schema]
    return args


def run_interactive_session(
    cfg: AMXConfig,
    *,
    version: str,
    main_command: click.Group,
    normalize_click_argv: NormalizeArgs,
    warn_no_doc_paths_for_scan_or_ingest: WarnNoPaths,
    print_interactive_startup_summary: Callable[[AMXConfig], None],
    print_db_namespace_hint: PrintDbHint,
    log_event: LogEvent,
    show_banner: Callable[..., None],
) -> None:
    _print_namespace_hint(
        "",
        cfg,
        version=version,
        print_interactive_startup_summary=print_interactive_startup_summary,
        print_db_namespace_hint=print_db_namespace_hint,
    )
    namespace = ""

    # Pre-v0.9.3 each cmd_heads frozenset was hand-maintained here AND
    # in ``_slash_command_catalog`` AND in ``_print_session_help`` —
    # drift between those caused the v0.6.1/0.6.2 description-verbosity
    # regressions. The ``amx.cli_support.slash_commands`` registry is
    # now the single source of truth; we just look up the head set per
    # namespace. The ``embeddings``/``embedding`` heads are not in the
    # registry as primary commands but still routed through /search.
    db_cmd_heads = _registry_cmd_heads("db")
    metadata_cmd_heads = _registry_cmd_heads("metadata")
    docs_cmd_heads = _registry_cmd_heads("docs")
    llm_cmd_heads = _registry_cmd_heads("llm")
    code_cmd_heads = _registry_cmd_heads("code")
    analyze_cmd_heads = _registry_cmd_heads("analyze")
    search_cmd_heads = _registry_cmd_heads("search") | frozenset({"embeddings", "embedding"})
    history_cmd_heads = _registry_cmd_heads("history")

    # SIGWINCH (terminal resize) is POSIX-only — Windows raises
    # AttributeError on signal.SIGWINCH. Guard so the interactive session
    # starts on Windows; the save/restore is purely defensive against
    # prompt_toolkit installing its own handler.
    _sigwinch = getattr(signal, "SIGWINCH", None)
    prev_sigwinch = signal.getsignal(_sigwinch) if _sigwinch is not None else None

    def _toolbar() -> HTML:
        ns = namespace or "root"
        schema_ctx = cfg.current_schema or "—"
        table_ctx = cfg.current_table or "—"
        llm_short = f"{cfg.llm.provider}/{cfg.llm.model}" if cfg.llm.model else "—"
        return HTML(
            f"<b>AMX v{version}</b> │ "
            f"ns:<b>{ns}</b> │ "
            f"schema:<b>{schema_ctx}</b> table:<b>{table_ctx}</b> │ "
            f"llm:<b>{llm_short}</b> │ "
            "<b>↑↓</b> navigate · <b>Esc</b> back · <b>Ctrl+C</b> exit"
        )

    session = PromptSession(
        completer=_SlashCompleter(lambda: namespace, cfg),
        key_bindings=_kb_escape_namespace(),
        mouse_support=False,
        bottom_toolbar=_toolbar,
        complete_while_typing=True,
        complete_style=CompleteStyle.COLUMN,
        style=Style.from_dict(
            {
                "completion-menu": "bg:#1f1f1f",
                "completion-menu.completion": "fg:#ffffff bg:#2b2b2b",
                "completion-menu.completion.current": "fg:#ffffff bold bg:#0b5fff",
                "completion-menu.meta.completion": "fg:#e6e6e6 bg:#2b2b2b",
                "completion-menu.meta.completion.current": "fg:#ffffff bold bg:#0b5fff",
            }
        ),
    )

    def _build_prompt_message(ns: str) -> HTML:
        tabs = ["root", "db", "metadata", "docs", "llm", "code", "analyze", "search", "history"]
        curr = ns or "root"
        parts = []
        for tab in tabs:
            if tab == curr:
                parts.append(f"<ansicyan><b>[ {tab.upper()} ]</b></ansicyan>")
            else:
                parts.append(f"<style fg='gray'>{tab}</style>")
        return HTML(f"{'  '.join(parts)}\n<b>&gt;</b> ")

    try:
        while True:
            _NS_STATE["namespace"] = namespace
            try:
                raw = session.prompt(_build_prompt_message(namespace)).strip()
            except (EOFError, KeyboardInterrupt):
                console.print()
                success("Session closed.")
                return

            if raw == "__amx_esc_back__":
                namespace = ""
                console.clear()
                show_banner(force=True)
                _print_namespace_hint(
                    namespace,
                    cfg,
                    version=version,
                    print_interactive_startup_summary=print_interactive_startup_summary,
                    print_db_namespace_hint=print_db_namespace_hint,
                )
                continue
            if raw == "__amx_esc_root__":
                continue
            if raw.startswith("__amx_switch_ns__:"):
                namespace = raw.split(":", 1)[1]
                console.clear()
                show_banner(force=True)
                _print_namespace_hint(
                    namespace,
                    cfg,
                    version=version,
                    print_interactive_startup_summary=print_interactive_startup_summary,
                    print_db_namespace_hint=print_db_namespace_hint,
                )
                continue
            if not raw:
                continue
            if not raw.startswith("/"):
                if namespace == "search":
                    raw = f"/ask {raw}"
                else:
                    warn("Use slash commands (example: /db, /connect, /run --schema sap_s6p)")
                    continue

            cmdline = raw[1:].strip()
            if not cmdline:
                continue
            if cmdline in {"exit", "quit", "q"}:
                success("Session closed.")
                return
            if cmdline == "clear":
                console.clear()
                show_banner(force=True)
                _print_namespace_hint(
                    namespace,
                    cfg,
                    version=version,
                    print_interactive_startup_summary=print_interactive_startup_summary,
                    print_db_namespace_hint=print_db_namespace_hint,
                )
                continue
            if cmdline in {"help", "?"}:
                _print_session_help(namespace=namespace, cfg=cfg)
                continue
            if cmdline == "back":
                namespace = ""
                console.clear()
                show_banner(force=True)
                _print_namespace_hint(
                    namespace,
                    cfg,
                    version=version,
                    print_interactive_startup_summary=print_interactive_startup_summary,
                    print_db_namespace_hint=print_db_namespace_hint,
                )
                continue
            if cmdline in {
                "db",
                "metadata",
                "manual",
                "docs",
                "llm",
                "code",
                "analyze",
                "search",
                "history",
            }:
                namespace = "metadata" if cmdline == "manual" else cmdline
                console.clear()
                show_banner(force=True)
                _print_namespace_hint(
                    namespace,
                    cfg,
                    version=version,
                    print_interactive_startup_summary=print_interactive_startup_summary,
                    print_db_namespace_hint=print_db_namespace_hint,
                )
                continue

            try:
                parts = shlex.split(cmdline)
            except ValueError as exc:
                error(f"Invalid command syntax: {exc}")
                continue
            if not parts:
                continue

            if not namespace:
                head = parts[0]
                if head in db_cmd_heads:
                    namespace = "db"
                    info("Assumed /db namespace for this command.")
                elif head in metadata_cmd_heads:
                    namespace = "metadata"
                    info("Assumed /metadata namespace for this command.")
                elif head in docs_cmd_heads:
                    namespace = "docs"
                    info("Assumed /docs namespace for this command.")
                elif head in llm_cmd_heads:
                    namespace = "llm"
                    info("Assumed /llm namespace for this command.")
                elif head in code_cmd_heads:
                    namespace = "code"
                    info("Assumed /code namespace for this command.")
                elif head in analyze_cmd_heads:
                    namespace = "analyze"
                    info("Assumed /analyze namespace for this command.")
                elif head in search_cmd_heads or head in {
                    "find-columns",
                    "join-candidates",
                    "explain",
                    "explain-table",
                }:
                    namespace = "search"
                    info("Assumed /search namespace for this command.")
                elif head in history_cmd_heads:
                    namespace = "history"
                    info("Assumed /history namespace for this command.")

            if namespace == "docs":
                if parts[0] == "search-docs" and len(parts) == 1:
                    error("Usage: /search-docs <text>")
                    info('Example: /search-docs What does field "BUKRS" mean in our docs?')
                    continue
                if (
                    parts[0] in {"ingest", "scan"}
                    and len(parts) == 1
                    and not cfg.effective_doc_paths()
                ):
                    warn_no_doc_paths_for_scan_or_ingest(cfg, cmd=parts[0])
                    continue
            if _handle_manual_usage_shortcuts(namespace, parts):
                continue

            # Special-case: bare "/ask" (no question) drops the user into a
            # sticky ask>-prompt REPL. We want this BEFORE the builtin/click
            # routing because Click would error out with
            # "Usage: /search ask <question>" otherwise.
            if parts == ["ask"]:
                _run_ask_repl(cfg, main_command=main_command, log_event=log_event)
                continue

            handled = _handle_session_builtin(cfg, namespace, parts, log_event=log_event)
            if handled == "exit":
                success("Session closed.")
                return
            if handled:
                continue

            args = session_to_click_args(namespace, parts)
            if args is None:
                error(f"Unknown command: /{cmdline}. Type /help.")
                continue

            args = normalize_click_argv(args, cfg)
            args = inject_session_defaults(cfg, namespace, args)

            previous = os.environ.get("AMX_SESSION_CHILD")
            os.environ["AMX_SESSION_CHILD"] = "1"
            try:
                main_command.main(args=args, prog_name="amx", standalone_mode=False)
            except click.ClickException as exc:
                error(_format_session_click_error(cmdline, exc))
            except SystemExit:
                pass
            except Exception as exc:  # pragma: no cover
                error(f"Command failed: {exc}")
            finally:
                if previous is None:
                    os.environ.pop("AMX_SESSION_CHILD", None)
                else:
                    os.environ["AMX_SESSION_CHILD"] = previous
    finally:
        if _sigwinch is not None and prev_sigwinch is not None:
            signal.signal(_sigwinch, prev_sigwinch)
