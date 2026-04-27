"""AMX CLI — Agentic Metadata Extractor command-line interface."""

from __future__ import annotations

import os
import shlex
import signal
import sys
from dataclasses import replace

import click
from prompt_toolkit.completion import Completer, Completion
from prompt_toolkit.filters import Condition
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.formatted_text import HTML
from prompt_toolkit.styles import Style
from prompt_toolkit.shortcuts import CompleteStyle, PromptSession

from amx import __version__
from amx.config import AMXConfig, SUPPORTED_BACKENDS, DISABLED_PROFILE
from amx.cli_analyze_flow import register_analyze_run_command
from amx.cli_code import register_code_commands
from amx.cli_db import (
    cmd_add_profile as _cmd_add_profile,
    cmd_profiles as _cmd_profiles,
    cmd_profiling as _cmd_profiling,
    cmd_remove_profile as _cmd_remove_profile,
    cmd_use as _cmd_use,
    interactive_db_block as _interactive_db_block,
    print_db_namespace_hint as _print_db_namespace_hint,
)
from amx.cli_docs import register_docs_commands
from amx.cli_history import register_history_commands
from amx.cli_profiles import (
    cmd_add_code_profile as _cmd_add_code_profile,
    cmd_add_doc_profile as _cmd_add_doc_profile,
    cmd_add_llm_profile as _cmd_add_llm_profile,
    cmd_batch_context_columns as _cmd_batch_context_columns,
    cmd_code_profiles as _cmd_code_profiles,
    cmd_doc_profiles as _cmd_doc_profiles,
    cmd_llm_batch_size as _cmd_llm_batch_size,
    cmd_llm_profiles as _cmd_llm_profiles,
    cmd_logprob_thresholds as _cmd_logprob_thresholds,
    cmd_n_alternatives as _cmd_n_alternatives,
    cmd_prompt_detail as _cmd_prompt_detail,
    cmd_remove_code_profile as _cmd_remove_code_profile,
    cmd_remove_doc_profile as _cmd_remove_doc_profile,
    cmd_remove_llm_profile as _cmd_remove_llm_profile,
    cmd_use_code as _cmd_use_code,
    cmd_use_doc as _cmd_use_doc,
    cmd_use_llm as _cmd_use_llm,
    interactive_llm_block as _interactive_llm_block,
    warn_no_doc_paths_for_scan_or_ingest as _warn_no_doc_paths_for_scan_or_ingest,
)
from amx.cli_run import (
    _asset_display_list,
    _finalize_scope,
    _pick_assets,
    _resolve_codebase_for_run,
    _resolve_run_scope,
    _validate_assets_in_schema,
    register_analyze_commands,
)
from amx.utils.console import (
    ask,
    ask_multi_choice,
    confirm,
    console,
    error,
    heading,
    info,
    render_table,
    show_banner,
    success,
    warn,
)
from amx.utils.logging import get_logger
from amx.storage.sqlite_store import history_store, init_history_store

log = get_logger("cli")

pass_config = click.make_pass_decorator(AMXConfig, ensure=True)

_NS_STATE: dict[str, str] = {"namespace": ""}


def _print_interactive_startup_summary(cfg: AMXConfig) -> None:
    """Show a concise startup summary."""
    info(f"Version {__version__}")
    info(
        f"Database: profile '{cfg.active_db_profile}' → "
        f"[{cfg.db.backend}] {cfg.db.display_summary}"
    )
    llm_line = (
        f"{cfg.llm.provider or '(unset)'}/{cfg.llm.model or '(unset)'}"
        if cfg.llm.model or cfg.llm.provider
        else "(not configured — run /setup)"
    )
    info(f"LLM: profile '{cfg.active_llm_profile}' → {llm_line}")
    if cfg.current_schema or cfg.current_table:
        info(f"Context: schema={cfg.current_schema or '—'} · table={cfg.current_table or '—'}")


def _fix_codebase_cli_tail(tokens: list[str]) -> list[str]:
    """Turn mistaken flags like `--sap_s6p` into `--schema sap_s6p` for `analyze codebase`."""
    known = {"--schema", "-s", "--help", "-h"}
    out: list[str] = []
    k = 0
    while k < len(tokens):
        t = tokens[k]
        if t in ("--schema", "-s"):
            out.append(t)
            k += 1
            if k < len(tokens) and not tokens[k].startswith("-"):
                out.append(tokens[k])
                k += 1
            continue
        if t.startswith("--") and "=" not in t and t not in ("--help",):
            name = t[2:]
            if name and name != "schema":
                out.extend(["--schema", name])
                k += 1
                continue
        out.append(t)
        k += 1
    return out


def _normalize_click_argv(args: list[str], cfg: AMXConfig) -> list[str]:
    if len(args) >= 3 and args[0] == "code" and args[1] == "scan":
        return ["code", "scan", args[2]] + _fix_codebase_cli_tail(args[3:])
    return args


def _rewrite_sys_argv_for_codebase(argv: list[str]) -> None:
    """In-place fix for `amx code scan …` when launched from a real shell."""
    for i in range(len(argv) - 2):
        if argv[i] == "code" and argv[i + 1] == "scan":
            head = argv[: i + 3]
            tail = argv[i + 3 :]
            argv[:] = head + _fix_codebase_cli_tail(tail)
            return


def run_cli() -> None:
    """Entry point for the `amx` console script (argv normalization + Click)."""
    if len(sys.argv) >= 4:
        _rewrite_sys_argv_for_codebase(sys.argv)
    main()


def _log_app_event(
    *,
    event_type: str,
    status: str,
    command: str,
    details: dict[str, object] | None = None,
) -> None:
    hs = history_store()
    if hs is None:
        return
    try:
        hs.log_event(
            event_type=event_type,
            status=status,
            command=command,
            details=details or {},
        )
    except Exception as exc:
        log.debug("Could not persist app event: %s", exc)


def _kb_escape_namespace() -> KeyBindings:
    kb = KeyBindings()

    @Condition
    def _is_buffer_empty() -> bool:
        from prompt_toolkit.application.current import get_app
        return len(get_app().current_buffer.text) == 0

    _TABS = ["", "db", "docs", "llm", "code", "analyze", "history"]

    @kb.add("escape")
    def _(event) -> None:  # type: ignore[no-untyped-def]
        buf = event.app.current_buffer
        if buf.text:
            buf.reset()
            return
        ns = _NS_STATE.get("namespace", "")
        if ns:
            _NS_STATE["namespace"] = ""
            event.app.exit(result="__amx_esc_back__")
        else:
            event.app.exit(result="__amx_esc_root__")

    @kb.add("right", filter=_is_buffer_empty)
    def _(event) -> None:  # type: ignore[no-untyped-def]
        curr = _NS_STATE.get("namespace", "")
        idx = _TABS.index(curr) if curr in _TABS else 0
        new_ns = _TABS[(idx + 1) % len(_TABS)]
        event.app.exit(result=f"__amx_switch_ns__:{new_ns}")

    @kb.add("left", filter=_is_buffer_empty)
    def _(event) -> None:  # type: ignore[no-untyped-def]
        curr = _NS_STATE.get("namespace", "")
        idx = _TABS.index(curr) if curr in _TABS else 0
        new_ns = _TABS[(idx - 1) % len(_TABS)]
        event.app.exit(result=f"__amx_switch_ns__:{new_ns}")

    return kb


@click.group(invoke_without_command=True)
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
    init_history_store(ctx.obj.CONFIG_DIR)
    is_session_child = os.getenv("AMX_SESSION_CHILD") == "1"
    if not is_session_child:
        show_banner()
    if ctx.invoked_subcommand is None:
        _interactive_session(cfg=ctx.obj)
        return

    # Enforce interactive-only command execution from the terminal.
    # Subcommands are still allowed when dispatched internally from the session.
    if not is_session_child:
        error(
            "Direct subcommands are disabled. Start with `amx`, then run slash commands "
            "inside the session (for example: /db, /connect, /run, /run-apply)."
        )
        raise click.ClickException("Use interactive mode only")


register_history_commands(main, pass_config=pass_config, log_event=_log_app_event)
analyze = register_analyze_commands(main, pass_config=pass_config, log_event=_log_app_event)
register_analyze_run_command(
    analyze,
    finalize_scope=_finalize_scope,
    resolve_codebase_for_run=_resolve_codebase_for_run,
    log_event=_log_app_event,
)
register_docs_commands(
    main,
    finalize_scope=_finalize_scope,
    warn_no_doc_paths_for_scan_or_ingest=_warn_no_doc_paths_for_scan_or_ingest,
)
register_code_commands(main, finalize_scope=_finalize_scope)


def _print_namespace_hint(namespace: str, cfg: AMXConfig) -> None:
    if not namespace:
        heading("AMX Interactive Session")
        _print_interactive_startup_summary(cfg)
        info("Type /help for commands, /back to return, /exit to quit (from any namespace).")
    elif namespace == "db":
        _print_db_namespace_hint()
    elif namespace == "docs":
        info("Manage RAG document paths for schema context. Use /add-doc-profile to map paths.")
    elif namespace == "llm":
        info("Manage LLM profiles and cost settings. Use /prompt-detail to adjust context sizes.")
    elif namespace == "code":
        info("Scan your codebase to find how tables are used. Run /code-scan after adding a path.")
    elif namespace == "analyze":
        info("Run the AMX pipeline (/run) to generate metadata, or (/apply) to push to your DB.")
    elif namespace == "history":
        info("View past metadata extractions. Use /review to inspect results.")


def _interactive_session(cfg: AMXConfig) -> None:
    """Start AMX interactive slash-command shell.

    Design: all Rich output happens *between* PromptSession.prompt() calls,
    never concurrently. This avoids patch_stdout() entirely, which prevents:
      - Raw ANSI leaking as ?[1;35m… in Terminal.app
      - Ghost 'amx>' lines on terminal resize
    """
    _print_namespace_hint("", cfg)
    namespace = ""

    _db_cmd_heads = frozenset(
        {
            "db-profiles",
            "use-db",
            "add-db-profile",
            "remove-db-profile",
            "profiling",
            "connect",
            "schema",
            "table",
            "schemas",
            "tables",
            "profile",
        }
    )
    _docs_cmd_heads = frozenset(
        {
            "doc-profiles",
            "use-doc",
            "add-doc-profile",
            "remove-doc-profile",
            "scan",
            "ingest",
            "search-docs",
            "doc-analyze",
            "export-doc-report",
        }
    )
    _llm_cmd_heads = frozenset(
        {"llm-profiles", "use-llm", "add-llm-profile", "remove-llm-profile",
         "prompt-detail", "n-alternatives", "llm-batch-size", "batch-context-columns", "logprob-thresholds"}
    )
    _code_cmd_heads = frozenset({
        "code-profiles", "use-code", "add-code-profile", "remove-code-profile",
        "code-scan", "code-refresh", "code-results", "code-analyze",
        "export-code-report",
    })
    _analyze_cmd_heads = frozenset({
        "run", "run-apply", "apply",
    })
    _history_cmd_heads = frozenset({"list", "show", "stats", "events", "results", "review"})

    prev_sigwinch = signal.getsignal(signal.SIGWINCH)

    def _toolbar() -> HTML:
        ns = namespace or "root"
        schema_ctx = cfg.current_schema or "—"
        table_ctx = cfg.current_table or "—"
        llm_short = f"{cfg.llm.provider}/{cfg.llm.model}" if cfg.llm.model else "—"
        return HTML(
            f"<b>AMX v{__version__}</b> │ "
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
        tabs = ["root", "db", "docs", "llm", "code", "analyze", "history"]
        curr = ns or "root"
        parts = []
        for t in tabs:
            if t == curr:
                parts.append(f"<ansicyan><b>[ {t.upper()} ]</b></ansicyan>")
            else:
                parts.append(f"<style fg='gray'>{t}</style>")
        tab_line = "  ".join(parts)
        return HTML(f"{tab_line}\n<b>&gt;</b> ")

    try:
        while True:
            _NS_STATE["namespace"] = namespace
            prompt_msg = _build_prompt_message(namespace)
            try:
                raw = session.prompt(prompt_msg).strip()
            except (EOFError, KeyboardInterrupt):
                console.print()
                success("Session closed.")
                return

            if raw == "__amx_esc_back__":
                namespace = ""
                console.clear()
                show_banner(force=True)
                _print_namespace_hint(namespace, cfg)
                continue
            if raw == "__amx_esc_root__":
                continue
            if raw.startswith("__amx_switch_ns__:"):
                new_ns = raw.split(":", 1)[1]
                namespace = new_ns
                console.clear()
                show_banner(force=True)
                _print_namespace_hint(namespace, cfg)
                continue

            if not raw:
                continue
            if not raw.startswith("/"):
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
                _print_namespace_hint(namespace, cfg)
                continue
            if cmdline in {"help", "?"}:
                _print_session_help(namespace=namespace, cfg=cfg)
                continue
            if cmdline == "back":
                namespace = ""
                console.clear()
                show_banner(force=True)
                _print_namespace_hint(namespace, cfg)
                continue
            if cmdline in {"db", "docs", "llm", "code", "analyze", "history"}:
                namespace = cmdline
                console.clear()
                show_banner(force=True)
                _print_namespace_hint(namespace, cfg)
                continue

            try:
                parts = shlex.split(cmdline)
            except ValueError as exc:
                error(f"Invalid command syntax: {exc}")
                continue

            if not parts:
                continue

            if not namespace:
                h = parts[0]
                if h in _db_cmd_heads:
                    namespace = "db"
                    info("Assumed /db namespace for this command.")
                elif h in _docs_cmd_heads:
                    namespace = "docs"
                    info("Assumed /docs namespace for this command.")
                elif h in _llm_cmd_heads:
                    namespace = "llm"
                    info("Assumed /llm namespace for this command.")
                elif h in _code_cmd_heads:
                    namespace = "code"
                    info("Assumed /code namespace for this command.")
                elif h in _analyze_cmd_heads:
                    namespace = "analyze"
                    info("Assumed /analyze namespace for this command.")
                elif h in _history_cmd_heads:
                    namespace = "history"
                    info("Assumed /history namespace for this command.")

            if namespace == "docs":
                if parts[0] == "search-docs" and len(parts) == 1:
                    error("Usage: /search-docs <text>")
                    info('Example: /search-docs What does field "BUKRS" mean in our docs?')
                    continue
                if parts[0] in {"ingest", "scan"} and len(parts) == 1:
                    if not cfg.effective_doc_paths():
                        _warn_no_doc_paths_for_scan_or_ingest(cfg, cmd=parts[0])
                        continue

            handled = _handle_session_builtin(cfg, namespace, parts)
            if handled == "exit":
                success("Session closed.")
                return
            if handled:
                continue

            args = _session_to_click_args(namespace, parts)
            if args is None:
                error(f"Unknown command: /{cmdline}. Type /help.")
                continue

            args = _normalize_click_argv(args, cfg)
            args = _inject_session_defaults(cfg, namespace, args)

            previous = os.environ.get("AMX_SESSION_CHILD")
            os.environ["AMX_SESSION_CHILD"] = "1"
            try:
                main.main(args=args, prog_name="amx", standalone_mode=False)
            except click.ClickException as exc:
                # Keep interactive UX slash-native; avoid Click's "Usage: amx ..."
                # blocks inside namespaces.
                if isinstance(exc, click.UsageError):
                    error(f"Unknown command: /{cmdline}. Type /help.")
                else:
                    error(str(exc))
            except SystemExit:
                pass
            except Exception as exc:  # pragma: no cover - defensive
                error(f"Command failed: {exc}")
            finally:
                if previous is None:
                    os.environ.pop("AMX_SESSION_CHILD", None)
                else:
                    os.environ["AMX_SESSION_CHILD"] = previous
    finally:
        signal.signal(signal.SIGWINCH, prev_sigwinch)


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
  2) /db-profiles                  List DB profiles (Backend + connection summary)
  3) /use-db [name]                Switch profile (interactive list shows [engine] per profile)
  4) /add-db-profile [name]        Create/update profile — pick PostgreSQL, Snowflake, Databricks, or BigQuery
  5) /remove-db-profile <name>     Remove a DB profile (cannot remove last)
  6) /profiling [mode] [max] [N]   Show/set profiling guardrails
  7) /save                         Persist config to disk (~/.amx/config.yml)
  8) /schema <name>                Set current schema context (used by /tables)
  9) /table <name>                 Set current table context (used by /profile)
 10) /connect                      Test DB connectivity
 11) /schemas                      List schemas
 12) /tables [schema]             List tables (defaults to current schema)
 13) /profile [schema] [table]    Profile a table (defaults to current context)

Navigation:
  Esc (empty line)                 Go back to root namespace
"""
        )
        return

    if namespace == "docs":
        out.print(
            """
[heading]Help — /docs namespace[/heading]
Commands (in order):
  1) /back                         Return to root namespace
  2) /doc-profiles                 List document profiles (named path lists)
  3) /use-doc <name>               Switch active document profile
  4) /add-doc-profile [name]       Add/update document roots (interactive)
  5) /remove-doc-profile <name>    Remove a document profile
  6) /scan [paths...]              Scan (preview); optional `--doc-profile NAME`; else active profile or paths
  7) /ingest [paths...]            Ingest into RAG; `--doc-profile NAME`; `--refresh` replaces chunks for those sources
  8) /search-docs <text>           Vector similarity over ingested docs (Chroma; no LLM answer)
  9) /doc-analyze [TABLE …]       Run RAG Agent standalone; results saved for next /run
 10) /export-doc-report [FILE]    Export document RAG summary to a markdown file

Tip: configure sources first (steps 2–5), then scan/ingest, then /search-docs.

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
  2) /llm-profiles                      List LLM profiles
  3) /use-llm <name>                    Switch active LLM profile
  4) /add-llm-profile [name]            Add/update an LLM profile (interactive)
  5) /remove-llm-profile <name>         Remove an LLM profile
  6) /prompt-detail [level]             Show or set the prompt detail level
                                          Levels: minimal | standard | detailed | full
                                          Controls which DB fields are included in the LLM prompt.
                                          Run without args to show the current level + what each
                                          preset includes.
  7) /n-alternatives [N]                Show or set number of description alternatives per column
  8) /llm-batch-size [N]                Show or set number of columns processed in one LLM call
                                          Range: 1 – 5  (default: 3)
  9) /batch-context-columns [off|all|N] Show or set how many non-batch column names are added
                                         as context in every profile batch prompt

Model examples (what to type in "Model name"):
  - openai      -> gpt-4o
  - openrouter  -> openrouter/openai/gpt-4o-mini
                 -> openrouter/anthropic/claude-3.5-sonnet
  - anthropic   -> claude-3-5-sonnet-20241022
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
Profiles:
  1) /back                         Return to root namespace
  2) /code-profiles                List codebase profiles
  3) /use-code <name>              Switch active codebase profile
  4) /add-code-profile [name]      Add/update a codebase path (interactive)
  5) /remove-code-profile <name>   Remove a codebase profile

Scanning and analysis:
  6) /code-scan [path] [--schema …] [--code-profile NAME]   Scan codebase, save results + semantic index
  7) /code-refresh [--code-profile NAME]   Clear cache + semantic index
  8) /code-results [--code-profile NAME]   Show last cached scan results
  9) /code-analyze [TABLE …] [--schema …]  Run Code Agent standalone; results saved for next /run
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
  4) /apply                        Write pending approved comments to the database

Tip: scan code and docs first (`/code-scan`, `/doc-analyze`, `/code-analyze`), then `/run`.

Navigation:
  Esc (empty line)                 Go back to root namespace
"""
        )
        return

    if namespace == "history":
        out.print(
            """
[heading]Help — /history namespace[/heading]
Commands:
  1) /back                                    Return to root namespace
  2) /list [-n N]                             Show recent analyze runs from SQLite
  3) /show <run_id>                           Show full JSON payload for one run
  4) /stats                                   Aggregate run/event stats
  5) /events [-n N]                           Recent app events
  6) /results <run_id>                        Show all saved LLM alternatives for a run
  7) /review <run_id> [--unevaluated-only]    Re-evaluate alternatives for a past run
                         [--apply]            Write approved descriptions to the database

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
  3) /db                           Database introspection + DB profiles
  4) /docs                         Document roots + RAG (scan/ingest/search-docs)
  5) /llm                          LLM profile management
  6) /code                         Codebase profile management
  7) /analyze                      Metadata inference (/run, /apply, …)
  8) /history                      Local SQLite history (/list, /show, /stats, /events)

Inside namespaces (examples):
  [bright_white]/db[/bright_white]   → /db-profiles, /schema, /table, /connect, …
  [bright_white]/docs[/bright_white] → /doc-profiles, /add-doc-profile, /ingest, …
  [bright_white]/llm[/bright_white]   → /llm-profiles, /add-llm-profile, …
  [bright_white]/code[/bright_white] → /code-profiles, /add-code-profile, …

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
    def __init__(self, namespace_cb, cfg: AMXConfig):
        self._namespace_cb = namespace_cb
        self._cfg = cfg

    def get_completions(self, document, complete_event):
        text = document.text_before_cursor
        if not text.startswith("/"):
            return

        ns = self._namespace_cb()
        partial = text[1:]

        for cmd, meta in _slash_command_catalog(ns, self._cfg):
            if cmd[1:].startswith(partial):
                yield Completion(
                    cmd,
                    start_position=-len(text),
                    display_meta=meta,
                )


def _slash_command_catalog(namespace: str, cfg: AMXConfig) -> list[tuple[str, str]]:
    """Ordered (command, description) pairs for autocomplete + help."""
    root: list[tuple[str, str]] = [
        ("/help", "Contextual help"),
        ("/exit", "Exit session"),
        ("/clear", "Clear terminal output"),
        ("/setup", "Run setup wizard"),
        ("/config", "Show configuration"),
        ("/db", "Enter /db namespace"),
        ("/docs", "Enter /docs namespace"),
        ("/llm", "Enter /llm namespace"),
        ("/code", "Enter /code namespace"),
        ("/analyze", "Enter /analyze namespace"),
        ("/history", "Enter /history namespace"),
        ("/save", "Save config to disk"),
    ]

    db_cmds: list[tuple[str, str]] = [
        ("/back", "Return to root namespace"),
        ("/clear", "Clear terminal output"),
        ("/db-profiles", "List DB profiles"),
        ("/use-db", "Switch DB profile (lists PostgreSQL, BigQuery, … per profile)"),
        ("/add-db-profile", "Add profile — choose engine then connection details"),
        ("/remove-db-profile", "Remove DB profile (/remove-db-profile <name>)"),
        ("/profiling", "Show/set profiling guardrails (/profiling [full|sampled|metadata] [max_rows|off] [sample_size])"),
        ("/save", "Save config to disk"),
        ("/schema", "Set current schema (/schema <name>)"),
        ("/table", "Set current table (/table <name>)"),
        ("/connect", "Test DB connectivity"),
        ("/schemas", "List schemas"),
        ("/tables", "List tables (/tables [schema])"),
        ("/profile", "Profile table (/profile [schema] [table])"),
    ]

    docs_cmds: list[tuple[str, str]] = [
        ("/back", "Return to root namespace"),
        ("/clear", "Clear terminal output"),
        ("/doc-profiles", "List document profiles"),
        ("/use-doc", "Switch document profile (/use-doc <name>)"),
        ("/add-doc-profile", "Add/update document profile"),
        ("/remove-doc-profile", "Remove document profile (/remove-doc-profile <name>)"),
        ("/scan", "Scan documents (/scan [--doc-profile NAME] [paths...])"),
        ("/ingest", "Ingest (/ingest [--doc-profile NAME] [--refresh] [paths...])"),
        ("/search-docs", "Similarity search (/search-docs <text>, no LLM)"),
        ("/doc-analyze", "Run RAG Agent standalone (/doc-analyze [TABLE …])"),
        ("/export-doc-report", "Export doc RAG summary (/export-doc-report [FILE])"),
    ]

    llm_cmds: list[tuple[str, str]] = [
        ("/back", "Return to root namespace"),
        ("/clear", "Clear terminal output"),
        ("/llm-profiles", "List LLM profiles"),
        ("/use-llm", "Switch LLM profile (/use-llm <name>)"),
        ("/add-llm-profile", "Add/update LLM profile"),
        ("/remove-llm-profile", "Remove LLM profile (/remove-llm-profile <name>)"),
        ("/prompt-detail", "Show/set prompt detail level (/prompt-detail [minimal|standard|detailed|full])"),
        ("/n-alternatives", "Show/set number of alternatives per column (/n-alternatives [1-5])"),
        ("/llm-batch-size", "Show/set number of columns per LLM call (/llm-batch-size [N])"),
        ("/batch-context-columns", "Show/set extra non-batch column names in each batch (/batch-context-columns [off|all|N])"),
        ("/logprob-thresholds", "Show/set confidence thresholds (/logprob-thresholds [high] [med])"),
    ]

    code_cmds: list[tuple[str, str]] = [
        ("/back", "Return to root namespace"),
        ("/clear", "Clear terminal output"),
        ("/code-profiles", "List codebase profiles"),
        ("/use-code", "Switch codebase profile (/use-code <name>)"),
        ("/add-code-profile", "Add/update codebase profile"),
        ("/remove-code-profile", "Remove codebase profile (/remove-code-profile <name>)"),
        ("/code-scan", "Scan codebase + save (/code-scan [path] [--code-profile NAME])"),
        ("/code-refresh", "Clear cache + semantic code index"),
        ("/code-results", "Show last cached scan results"),
        ("/code-analyze", "Run Code Agent standalone (/code-analyze [TABLE …])"),
        ("/export-code-report", "Export scan to markdown (/export-code-report [FILE])"),
    ]

    analyze_cmds: list[tuple[str, str]] = [
        ("/back", "Return to root namespace"),
        ("/clear", "Clear terminal output"),
        ("/run", "Run all agents — scope: database / schema / asset (/run [ASSET …] [--schema …] [--apply])"),
        ("/run-apply", "Run + apply (/run-apply [ASSET …] [--schema …] [--table …])"),
        ("/apply", "Write pending comments to the database"),
    ]
    history_cmds: list[tuple[str, str]] = [
        ("/back", "Return to root namespace"),
        ("/clear", "Clear terminal output"),
        ("/list", "Show recent runs (/list -n 20)"),
        ("/show", "Show one run payload (/show <run_id>)"),
        ("/stats", "Aggregate run/event metrics"),
        ("/events", "Recent app events (/events -n 30)"),
        ("/results", "Show saved LLM alternatives (/results <run_id>)"),
        ("/review", "Re-evaluate alternatives (/review <run_id> [--unevaluated-only] [--apply])"),
    ]

    if namespace == "db":
        return db_cmds
    if namespace == "docs":
        return docs_cmds
    if namespace == "llm":
        return llm_cmds
    if namespace == "code":
        return code_cmds
    if namespace == "analyze":
        return analyze_cmds
    if namespace == "history":
        return history_cmds
    return root


def _require_namespace(cmd: str, namespace: str, expected: str, replacement: str) -> bool:
    if namespace == expected:
        return True
    error(f"/{cmd} belongs in /{expected}. Example: `/{expected}` then `/{replacement}`.")
    return False


def _handle_session_builtin(cfg: AMXConfig, namespace: str, parts: list[str]) -> bool | str:
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
        _cmd_use(cfg, parts[1:], log_event=_log_app_event)
        return True
    if head == "add-db-profile":
        if not _require_namespace(head, namespace, "db", "add-db-profile"):
            return True
        _cmd_add_profile(cfg, parts[1:], log_event=_log_app_event)
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
    if head == "save":
        path = cfg.save()
        success(f"Saved configuration to {path}")
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


def _session_to_click_args(namespace: str, parts: list[str]) -> list[str] | None:
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
        "run": ["analyze", "run"],
        "run-apply": ["analyze", "run", "--apply"],
        "apply": ["analyze", "apply"],
        "code-scan": ["code", "scan"],
        "code-refresh": ["code", "refresh"],
        "code-results": ["code", "results"],
        "code-analyze": ["code", "analyze"],
        "export-code-report": ["code", "export-report"],
        "setup": ["setup"],
        "config": ["config"],
        "help": ["--help"],
    }

    if head in {"db", "docs", "llm", "code", "analyze", "history", "setup", "config"}:
        return parts

    if namespace and head in shortcut_map:
        return shortcut_map[head] + parts[1:]

    if head in shortcut_map:
        return shortcut_map[head] + parts[1:]

    if namespace:
        return [namespace] + parts

    return None


def _inject_session_defaults(cfg: AMXConfig, namespace: str, args: list[str]) -> list[str]:
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


# ── Setup Commands ──────────────────────────────────────────────────────────


@main.command()
@click.pass_obj
def setup(cfg: AMXConfig) -> None:
    """Interactive first-time setup wizard."""
    heading("AMX Setup Wizard")

    # Database
    info("Step 1/3 — Database Connection")
    cfg.db = _interactive_db_block(cfg.db)

    if not cfg.active_db_profile:
        cfg.active_db_profile = "default"
    cfg.upsert_db_profile(cfg.active_db_profile, cfg.db)
    cfg.apply_active_db_profile()

    from amx.db.connector import DatabaseConnector

    db = DatabaseConnector(cfg.db)
    if db.test_connection():
        success(f"Database connection successful! (backend: {cfg.db.backend})")
    else:
        error("Database connection failed. Check credentials and try again.")
        if not confirm("Continue anyway?", default=False):
            sys.exit(1)

    # LLM
    info("Step 2/3 — AI Model Configuration")
    cfg.llm = _interactive_llm_block(cfg.llm)
    cfg.active_llm_profile = cfg.active_llm_profile or "default"
    cfg.upsert_llm_profile(cfg.active_llm_profile, replace(cfg.llm))
    cfg.apply_active_llm_profile()

    from amx.llm.provider import LLMProvider

    llm = LLMProvider(cfg.llm)
    if llm.test():
        success("LLM connection successful!")
    else:
        warn("LLM test failed — you can reconfigure later with `amx setup`.")

    # Data sources
    info("Step 3/3 — Optional Data Sources (named profiles)")
    if confirm("Add a document profile for RAG?", default=False):
        from amx.docs.scanner import test_source_reachable

        name = ask("Profile name", default="default")
        existing = list(cfg.doc_profiles.get(name, []))
        new_paths: list[str] = []
        while True:
            p = ask("Document path" if not new_paths else "Another path (empty to finish)", default="")
            if not p:
                break
            if p in existing or p in new_paths:
                if not confirm(f"This path is already in profile {name!r}: {p}. Add duplicate anyway?", default=False):
                    continue
            try:
                test_source_reachable(p)
                success(f"Source reachable: {p}")
                new_paths.append(p)
            except Exception as exc:
                error(f"Source not reachable: {p}")
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
        p = ask("Codebase path (local dir or Git URL)", default="")
        if p:
            try:
                test_codebase_path_reachable(p)
                success(f"Codebase reachable: {p}")
                cfg.upsert_code_profile(name, p)
                cfg.active_code_profile = name
            except Exception as exc:
                error(f"Codebase not reachable: {p}")
                warn(str(exc))

    saved = cfg.save()
    success(f"Configuration saved to {saved}")
# ── Database Commands ───────────────────────────────────────────────────────


@main.group()
def db() -> None:
    """Database inspection and profiling commands."""


@db.command("connect")
@click.pass_obj
def db_connect(cfg: AMXConfig) -> None:
    """Test database connectivity."""
    from amx.db.connector import DatabaseConnector

    db = DatabaseConnector(cfg.db)
    if db.test_connection():
        success(f"Connected to [{cfg.db.backend}] {cfg.db.display_summary}")
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
    """List all assets (tables, views, materialized views) in a schema."""
    from amx.db.connector import DatabaseConnector

    db_conn = DatabaseConnector(cfg.db)
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


# ── Config Commands ─────────────────────────────────────────────────────────


@main.command("config")
@click.pass_obj
def show_config(cfg: AMXConfig) -> None:
    """Display current configuration."""
    info(
        f"Active DB profile: {cfg.active_db_profile} → "
        f"[{cfg.db.backend}] {cfg.db.display_summary}"
    )
    if cfg.db_profiles:
        names = ", ".join(sorted(cfg.db_profiles.keys()))
        info(f"DB profiles: {names}")
    max_rows = int(getattr(cfg.db, "profiling_max_rows", 1_000_000) or 0)
    max_label = "off" if max_rows <= 0 else f"{max_rows:,}"
    info(
        f"Profiling: mode={cfg.db.profiling_mode}, "
        f"max_full_scan_rows={max_label}, sample_size={cfg.db.profiling_sample_size}"
    )
    info(f"Session context: schema={cfg.current_schema or '-'} table={cfg.current_table or '-'}")
    info(
        f"Active LLM profile: {cfg.active_llm_profile} → "
        f"{cfg.llm.provider}/{cfg.llm.model}"
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


if __name__ == "__main__":
    run_cli()
