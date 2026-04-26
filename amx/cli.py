"""AMX CLI — Agentic Metadata Extractor command-line interface."""

from __future__ import annotations

import os
import shlex
import signal
import sys
import time
import json
from dataclasses import replace
from pathlib import Path

import click
from prompt_toolkit.completion import Completer, Completion
from prompt_toolkit.filters import Condition
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.formatted_text import HTML
from prompt_toolkit.styles import Style
from prompt_toolkit.shortcuts import CompleteStyle, PromptSession

from amx import __version__
from amx.config import AMXConfig, LLMConfig, SUPPORTED_BACKENDS, DISABLED_PROFILE
from amx.cli_db import (
    cmd_add_profile as _cmd_add_profile,
    cmd_profiles as _cmd_profiles,
    cmd_profiling as _cmd_profiling,
    cmd_remove_profile as _cmd_remove_profile,
    cmd_use as _cmd_use,
    interactive_db_block as _interactive_db_block,
    print_db_namespace_hint as _print_db_namespace_hint,
)
from amx.cli_history import register_history_commands
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
    ask_choice,
    ask_multi_choice,
    ask_password,
    confirm,
    console,
    error,
    heading,
    info,
    render_table,
    render_token_summary,
    show_banner,
    step_spinner,
    success,
    warn,
)
from amx.utils.logging import get_logger
from amx.storage.sqlite_store import history_store, init_history_store
from amx.utils.token_tracker import tracker as token_tracker

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


def _interactive_llm_block(defaults: LLMConfig) -> LLMConfig:
    provider = ask_choice(
        "Select AI provider",
        ["openai", "openrouter", "anthropic", "gemini", "deepseek", "local", "kimi", "ollama"],
        default=defaults.provider or "openai",
    )
    info(
        "Model: use a short id (e.g. gpt-4o) or LiteLLM form openai/gpt-4o — "
        "see https://docs.litellm.ai/docs/providers"
    )
    if provider == "openrouter":
        info("OpenRouter model examples: openrouter/openai/gpt-4o-mini, openrouter/anthropic/claude-3.5-sonnet")
    elif provider == "openai":
        info("OpenAI model example: gpt-4o")
    elif provider == "anthropic":
        info("Anthropic model example: claude-3-5-sonnet-20241022")
    elif provider == "ollama":
        info("Ollama model example: llama3")
    model = ask("Model name", defaults.model or _default_model(provider))
    api_base = defaults.api_base
    if provider in ("local", "ollama", "kimi", "openrouter"):
        default_api_base = "http://localhost:11434" if provider == "ollama" else "http://localhost:11434/v1"
        if provider == "openrouter":
            default_api_base = "https://openrouter.ai/api/v1"
        api_base = ask("API base URL", api_base or default_api_base)

    if provider in ("local", "ollama"):
        api_key = ask("API key (optional)", defaults.api_key or "")
    else:
        api_key = ask_password("API key") or defaults.api_key

    info("Generation settings:")
    n_alt = ask("  Alternatives (1\u20135)", default=str(getattr(defaults, "n_alternatives", 3)))
    batch = ask("  Column batch size", default=str(getattr(defaults, "column_batch_size", 10)))

    info("Confidence thresholds (token probability 0.0\u20131.0):")
    high = ask("  High threshold", default=str(getattr(defaults, "logprob_high", 0.85)))
    med = ask("  Medium threshold", default=str(getattr(defaults, "logprob_medium", 0.50)))

    return LLMConfig(
        provider=provider,
        model=model.strip(),
        api_key=api_key,
        api_base=api_base,
        temperature=defaults.temperature,
        max_tokens=defaults.max_tokens,
        n_alternatives=int(n_alt),
        column_batch_size=int(batch),
        batch_context_column_names=int(getattr(defaults, "batch_context_column_names", 0)),
        logprob_high=float(high),
        logprob_medium=float(med),
    )


def _cmd_logprob_thresholds(cfg: AMXConfig, rest: list[str]) -> None:
    """Show or set logprob confidence thresholds for the active LLM profile."""
    if not rest:
        high = getattr(cfg.llm, "logprob_high", 0.85)
        med = getattr(cfg.llm, "logprob_medium", 0.50)
        info(f"Current logprob thresholds: [bold]HIGH[/] >= {high:.2f} | [bold]MEDIUM[/] >= {med:.2f}")
        info("Run [cyan]/logprob-thresholds <high> <med>[/cyan] to change (e.g. 0.9 0.6).")
        return

    try:
        if len(rest) < 2:
            error("Usage: /logprob-thresholds <high> <medium>")
            return
        h = float(rest[0])
        m = float(rest[1])
    except ValueError:
        error(f"Expected numeric values, got: {rest}")
        return

    if not (0.0 <= m < h <= 1.0):
        error("Thresholds must be 0.0\u20131.0, and high must be greater than medium.")
        return

    cfg.llm.logprob_high = h
    cfg.llm.logprob_medium = m
    if cfg.active_llm_profile and cfg.active_llm_profile in cfg.llm_profiles:
        prof = cfg.llm_profiles[cfg.active_llm_profile]
        prof.logprob_high = h
        prof.logprob_medium = m
    cfg.save()
    success(
        f"Logprob thresholds saved for LLM profile '{cfg.active_llm_profile}': "
        f"HIGH >= {h:.2f}, MEDIUM >= {m:.2f}"
    )


def _cmd_llm_profiles(cfg: AMXConfig) -> None:
    rows = []
    for name, llm in sorted(cfg.llm_profiles.items(), key=lambda x: x[0]):
        mark = "*" if name == cfg.active_llm_profile else " "
        rows.append([f"{mark} {name}", llm.provider, llm.model])
    render_table("LLM profiles (* = active)", ["Profile", "Provider", "Model"], rows)


def _cmd_use_llm(cfg: AMXConfig, rest: list[str]) -> None:
    if len(rest) >= 1:
        name = rest[0]
    else:
        names = sorted(cfg.llm_profiles.keys())
        if not names:
            error("No LLM profiles configured.")
            return
        name = ask_choice("Select LLM profile", names, default=cfg.active_llm_profile)
    try:
        cfg.set_active_llm_profile(name)
        cfg.save()
        success(f"Switched active LLM profile to: {name}")
    except Exception as exc:
        error(str(exc))


def _cmd_add_llm_profile(cfg: AMXConfig, rest: list[str]) -> None:
    if len(rest) >= 1:
        name = rest[0]
    else:
        name = ask("LLM profile name", default="work")
    base = cfg.llm_profiles.get(name, cfg.llm)
    info(f"Creating/updating LLM profile: {name}")
    llm = _interactive_llm_block(replace(base))
    cfg.upsert_llm_profile(name, llm)
    if confirm(f"Activate profile {name} now?", default=True):
        cfg.set_active_llm_profile(name)
    cfg.save()
    success(f"LLM profile saved: {name} (active: {cfg.active_llm_profile})")


def _cmd_remove_llm_profile(cfg: AMXConfig, rest: list[str]) -> None:
    if len(rest) < 1:
        error("Usage: /remove-llm-profile <name>")
        return
    name = rest[0]
    try:
        cfg.remove_llm_profile(name)
        cfg.save()
        success(f"Removed LLM profile: {name} (active: {cfg.active_llm_profile})")
    except Exception as exc:
        error(str(exc))


def _cmd_prompt_detail(cfg: AMXConfig, rest: list[str]) -> None:
    """Show or set the prompt detail level for the active LLM profile."""
    from amx.config import PROMPT_DETAIL_LEVELS, prompt_detail_for

    if not rest:
        # Show current level + comparison table
        current = cfg.llm.prompt_detail or "standard"
        heading(f"Prompt detail level: {current}")
        rows = []
        flags = [
            ("samples", "include_samples"),
            ("null counts", "include_null_counts"),
            ("min / max", "include_min_max"),
            ("cardinality ratio", "include_cardinality"),
            ("col. comment", "include_existing_col_comment"),
            ("PK / FK keys", "include_pk_fk"),
            ("unique+check constraints", "include_unique_check"),
            ("usage stats (pg_stat)", "include_usage_stats"),
            ("schema+db comments", "include_schema_db_comments"),
            ("FK neighbour comments", "include_related_comments"),
            ("query-log analysis", "include_query_log_analysis"),
            ("RAG table hits", "rag_table_hits"),
            ("RAG col hits", "rag_col_hits"),
            ("RAG max chunks", "rag_max_chunks"),
        ]
        for label, attr in flags:
            row = [label]
            for lv in PROMPT_DETAIL_LEVELS:
                pd = prompt_detail_for(lv)
                val = getattr(pd, attr)
                if isinstance(val, bool):
                    mark = "✓" if val else "—"
                else:
                    mark = str(val)
                row.append(f"[{'success' if val else 'dim'}]{mark}[/]" if isinstance(val, bool) else mark)
            rows.append(row)
        render_table(
            "Preset comparison",
            ["Field", *PROMPT_DETAIL_LEVELS],
            rows,
        )
        info(
            f"Current level: [cyan]{current}[/cyan]  "
            f"(n_alternatives={cfg.llm.n_alternatives})  "
            "— run [cyan]/prompt-detail <level>[/cyan] to change."
        )
        return

    level = rest[0].lower().strip()
    if level not in PROMPT_DETAIL_LEVELS:
        error(
            f"Unknown level: {level!r}. "
            f"Valid levels: {', '.join(PROMPT_DETAIL_LEVELS)}"
        )
        return

    cfg.llm.prompt_detail = level
    # Persist to the active profile so it survives session
    if cfg.active_llm_profile and cfg.active_llm_profile in cfg.llm_profiles:
        cfg.llm_profiles[cfg.active_llm_profile].prompt_detail = level
    cfg.save()
    success(
        f"Prompt detail set to [cyan]{level}[/cyan] and saved "
        f"for LLM profile '{cfg.active_llm_profile}'."
    )
    pd = prompt_detail_for(level)
    info(
        f"  samples={pd.include_samples}(max={pd.max_samples})  "
        f"null_counts={pd.include_null_counts}  "
        f"min_max={pd.include_min_max}  "
        f"cardinality={pd.include_cardinality}  "
        f"pk_fk={pd.include_pk_fk}  "
        f"usage_stats={pd.include_usage_stats}  "
        f"query_log_analysis={pd.include_query_log_analysis}  "
        f"rag_chunks={pd.rag_max_chunks}"
    )


def _cmd_n_alternatives(cfg: AMXConfig, rest: list[str]) -> None:
    """Show or set number of description alternatives per column."""
    if not rest:
        current = getattr(cfg.llm, "n_alternatives", 3)
        info(
            f"Current n_alternatives: [cyan]{current}[/cyan]  "
            "(1 = cheapest, 5 = maximum alternatives)  "
            "— run [cyan]/n-alternatives <N>[/cyan] to change."
        )
        return

    try:
        n = int(rest[0])
    except ValueError:
        error(f"Expected an integer 1–5, got: {rest[0]!r}")
        return

    if not 1 <= n <= 5:
        error("n_alternatives must be between 1 and 5.")
        return

    cfg.llm.n_alternatives = n
    if cfg.active_llm_profile and cfg.active_llm_profile in cfg.llm_profiles:
        cfg.llm_profiles[cfg.active_llm_profile].n_alternatives = n
    cfg.save()
    cost_note = {1: "cheapest — 1 option shown at review", 2: "lean", 3: "balanced (default)",
                 4: "rich", 5: "maximum context, highest cost"}.get(n, "")
    success(
        f"n_alternatives set to [cyan]{n}[/cyan] ({cost_note}) and saved "
        f"for LLM profile '{cfg.active_llm_profile}'."
    )


def _cmd_llm_batch_size(cfg: AMXConfig, rest: list[str]) -> None:
    """Show or set the number of columns processed in a single LLM call."""
    if not rest:
        current = getattr(cfg.llm, "column_batch_size", 10)
        info(
            f"Current LLM batch size: [cyan]{current}[/cyan] columns  "
            "(Small = safer/more precise, Large = faster/cheaper)  "
            "— run [cyan]/llm-batch-size <N>[/cyan] to change."
        )
        return

    try:
        n = int(rest[0])
    except ValueError:
        error(f"Expected an integer, got: {rest[0]!r}")
        return

    if n < 1:
        error("Batch size must be at least 1.")
        return

    cfg.llm.column_batch_size = n
    if cfg.active_llm_profile and cfg.active_llm_profile in cfg.llm_profiles:
        cfg.llm_profiles[cfg.active_llm_profile].column_batch_size = n
    cfg.save()
    success(f"LLM batch size set to {n} columns and saved for LLM profile '{cfg.active_llm_profile}'.")


def _cmd_batch_context_columns(cfg: AMXConfig, rest: list[str]) -> None:
    """Show or set how many remaining column names are sent with each batch."""
    if not rest:
        current = int(getattr(cfg.llm, "batch_context_column_names", 0))
        if current == -1:
            current_label = "all remaining names"
        elif current == 0:
            current_label = "off"
        else:
            current_label = f"{current} names"
        info(
            f"Current batch context columns: [cyan]{current_label}[/cyan]. "
            "Use [cyan]/batch-context-columns off[/cyan], [cyan]all[/cyan], "
            "or [cyan]/batch-context-columns <N>[/cyan]."
        )
        return

    raw = (rest[0] or "").strip().lower()
    if raw in {"off", "none", "0"}:
        value = 0
    elif raw == "all":
        value = -1
    else:
        try:
            value = int(raw)
        except ValueError:
            error(f"Expected off, all, or an integer; got: {rest[0]!r}")
            return
        if value < 0:
            error("Value must be >= 0, or use 'all'.")
            return

    cfg.llm.batch_context_column_names = value
    if cfg.active_llm_profile and cfg.active_llm_profile in cfg.llm_profiles:
        cfg.llm_profiles[cfg.active_llm_profile].batch_context_column_names = value
    cfg.save()
    if value == -1:
        success(f"Batch context columns set to all remaining names and saved for LLM profile '{cfg.active_llm_profile}'.")
    elif value == 0:
        success(f"Batch context columns disabled and saved for LLM profile '{cfg.active_llm_profile}'.")
    else:
        success(f"Batch context columns set to {value} and saved for LLM profile '{cfg.active_llm_profile}'.")


def _cmd_doc_profiles(cfg: AMXConfig) -> None:
    if not cfg.doc_profiles:
        info("No document profiles. Use /add-doc-profile <name>")
        return
    rows = []
    if cfg.active_doc_profile == DISABLED_PROFILE:
        rows.append(["* (none)", "0", "profiles disabled"])
    for name, paths in sorted(cfg.doc_profiles.items(), key=lambda x: x[0]):
        mark = "*" if name == cfg.active_doc_profile else " "
        preview = "; ".join(paths[:2]) + (" …" if len(paths) > 2 else "")
        rows.append([f"{mark} {name}", str(len(paths)), preview])
    render_table("Document profiles (* = active)", ["Profile", "# paths", "Preview"], rows)


def _cmd_use_doc(cfg: AMXConfig, rest: list[str]) -> None:
    if len(rest) >= 1:
        raw = rest[0].strip().lower()
        name = DISABLED_PROFILE if raw in {"none", "(none)", "off", "disable"} else rest[0]
    else:
        names = sorted(cfg.doc_profiles.keys())
        if not names:
            error("No document profiles.")
            return
        choices = ["(none)"] + names
        default_choice = "(none)" if cfg.active_doc_profile == DISABLED_PROFILE else cfg.active_doc_profile
        picked = ask_choice("Select document profile", choices, default=default_choice)
        name = DISABLED_PROFILE if picked == "(none)" else picked
    if name != DISABLED_PROFILE and name not in cfg.doc_profiles:
        error(f"Unknown document profile: {name}")
        return
    cfg.active_doc_profile = name
    cfg.save()
    if name == DISABLED_PROFILE:
        success("Document profiles disabled for this session/config.")
    else:
        success(f"Active document profile: {name}")


def _cmd_add_doc_profile(cfg: AMXConfig, rest: list[str]) -> None:
    if len(rest) >= 1:
        name = rest[0]
    else:
        name = ask("Document profile name", default="default")
    from amx.docs.scanner import test_source_reachable

    existing = list(cfg.doc_profiles.get(name, []))
    new_paths: list[str] = []
    info(
        "Enter document roots (local dir, s3://, GitHub URL, Google Drive, SharePoint/OneDrive). "
        "Each path is checked for reachability only (no full scan)."
    )
    while True:
        p = ask("Path (empty to finish)" if new_paths else "Path", default="")
        if not p:
            if new_paths:
                break
            error("No paths added.")
            return
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
    if not new_paths:
        error("No valid document sources to save.")
        return
    merged = existing + new_paths
    cfg.upsert_doc_profile(name, merged)
    if not cfg.active_doc_profile or confirm(f"Switch active document profile to {name}?", default=True):
        cfg.active_doc_profile = name
    cfg.save()
    success(f"Document profile saved: {name} ({len(merged)} path(s))")



def _warn_no_doc_paths_for_scan_or_ingest(cfg: AMXConfig, *, cmd: str) -> None:
    """User-friendly hint when /scan or /ingest has no paths and no configured profile."""
    error(f"No document paths to {cmd}.")
    if not cfg.doc_profiles and not cfg.doc_paths:
        info("Add a document profile first: /add-doc-profile (or run /setup).")
    elif cfg.doc_profiles and not cfg.effective_doc_paths():
        info("Your document profiles look empty. Run /add-doc-profile to add paths.")
    else:
        info("Pass paths on the command (e.g. /ingest /path/to/docs) or set an active profile with /use-doc.")


def _cmd_remove_doc_profile(cfg: AMXConfig, rest: list[str]) -> None:
    if len(rest) < 1:
        error("Usage: /remove-doc-profile <name>")
        return
    try:
        cfg.remove_doc_profile(rest[0])
        cfg.save()
        success(f"Removed document profile: {rest[0]}")
    except Exception as exc:
        error(str(exc))


def _cmd_code_profiles(cfg: AMXConfig) -> None:
    if not cfg.code_profiles:
        info("No codebase profiles. Use /add-code-profile <name>")
        return
    rows = []
    if cfg.active_code_profile == DISABLED_PROFILE:
        rows.append(["* (none)", "disabled"])
    for name, path in sorted(cfg.code_profiles.items(), key=lambda x: x[0]):
        mark = "*" if name == cfg.active_code_profile else " "
        rows.append([f"{mark} {name}", path])
    render_table("Codebase profiles (* = active)", ["Profile", "Path / URL"], rows)


def _cmd_use_code(cfg: AMXConfig, rest: list[str]) -> None:
    if len(rest) >= 1:
        raw = rest[0].strip().lower()
        name = DISABLED_PROFILE if raw in {"none", "(none)", "off", "disable"} else rest[0]
    else:
        names = sorted(cfg.code_profiles.keys())
        if not names:
            error("No codebase profiles.")
            return
        choices = ["(none)"] + names
        default_choice = "(none)" if cfg.active_code_profile == DISABLED_PROFILE else cfg.active_code_profile
        picked = ask_choice("Select codebase profile", choices, default=default_choice)
        name = DISABLED_PROFILE if picked == "(none)" else picked
    if name != DISABLED_PROFILE and name not in cfg.code_profiles:
        error(f"Unknown codebase profile: {name}")
        return
    cfg.active_code_profile = name
    cfg.save()
    if name == DISABLED_PROFILE:
        success("Codebase profiles disabled for this session/config.")
    else:
        success(f"Active codebase profile: {name}")


def _cmd_add_code_profile(cfg: AMXConfig, rest: list[str]) -> None:
    if len(rest) >= 1:
        name = rest[0]
        path = " ".join(rest[1:]).strip() if len(rest) > 1 else ""
    else:
        name = ask("Codebase profile name", default="default")
        path = ""
    if not path:
        path = ask("Codebase path (local dir or Git URL)", default="")
    if not path:
        error("Path required.")
        return
    from amx.codebase.analyzer import test_codebase_path_reachable

    prev = cfg.code_profiles.get(name)
    if prev == path:
        success(f"Codebase profile {name!r} already points to this path — nothing to change.")
        return
    others = [n for n, p in cfg.code_profiles.items() if p == path and n != name]
    if others:
        olist = ", ".join(sorted(others))
        if not confirm(
            f"This path is already used by codebase profile(s): {olist}. Point {name!r} here too?",
            default=True,
        ):
            return
    try:
        test_codebase_path_reachable(path)
        success(f"Codebase reachable: {path}")
    except Exception as exc:
        error(f"Codebase not reachable: {path}")
        warn(str(exc))
        return
    cfg.upsert_code_profile(name, path)
    if not cfg.active_code_profile or confirm(f"Switch active codebase profile to {name}?", default=True):
        cfg.active_code_profile = name
    cfg.save()
    success(f"Codebase profile saved: {name}")


def _cmd_remove_code_profile(cfg: AMXConfig, rest: list[str]) -> None:
    if len(rest) < 1:
        error("Usage: /remove-code-profile <name>")
        return
    try:
        cfg.remove_code_profile(rest[0])
        cfg.save()
        success(f"Removed codebase profile: {rest[0]}")
    except Exception as exc:
        error(str(exc))


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


def _default_model(provider: str) -> str:
    return {
        "openai": "gpt-4o",
        "openrouter": "openai/gpt-4o-mini",
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


# ── Document Commands ───────────────────────────────────────────────────────


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
        _warn_no_doc_paths_for_scan_or_ingest(cfg, cmd="scan")
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
def docs_ingest(cfg: AMXConfig, paths: tuple[str, ...], doc_profile: str | None, refresh: bool) -> None:
    """Ingest documents directly into the RAG store."""
    from amx.docs.rag import RAGStore
    from amx.docs.scanner import scan_all_sources, total_size_mb

    try:
        all_paths = list(paths) if paths else cfg.resolve_doc_paths(doc_profile, [])
    except KeyError as exc:
        error(str(exc))
        return
    if not all_paths:
        _warn_no_doc_paths_for_scan_or_ingest(cfg, cmd="ingest")
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


def _run_docs_semantic_search(question: str, results: int) -> None:
    """Chroma embedding similarity only — no generative LLM."""
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
    for m in metadatas:
        src = m.get("source", "unknown")
        source_counts[src] = source_counts.get(src, 0) + 1
        st = m.get("source_type", "")
        if st:
            source_types[src] = st

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
    for src, cnt in sorted(source_counts.items(), key=lambda x: -x[1]):
        stype = source_types.get(src, "")
        lines.append(f"| {src} | {cnt} | {stype} |")
    lines.append("")

    from pathlib import Path

    Path(out).write_text("\n".join(lines), encoding="utf-8")
    success(f"Exported document RAG report to {out}")


@docs.command("analyze")
@click.argument("tables_pos", nargs=-1, metavar="[TABLE ...]")
@click.option("--schema", "-s", help="Schema context.")
@click.option("--table", "-t", multiple=True, help="Specific table(s).")
@click.pass_obj
def docs_analyze(cfg: AMXConfig, tables_pos: tuple[str, ...], schema: str | None, table: tuple[str, ...]) -> None:
    """Run the RAG Agent standalone against ingested documents for the given tables.

    Results are saved to ~/.amx/doc_agent_results.json and reused by the next /run.
    """
    import json
    from pathlib import Path

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
    scope = _finalize_scope(cfg, db, schema or cfg.current_schema, tables_arg)
    if scope is None:
        return
    schema = next(iter(scope))
    tables = scope[schema]

    agent = RAGAgent(llm, store)
    all_suggestions = []
    for t in tables:
        with step_spinner(f"Profiling {schema}.{t}"):
            tp = db.profile_table(schema, t)
        ctx = AgentContext(
            schema=schema,
            table=t,
            db_profile={
                "row_count": tp.row_count,
                "columns": [{"name": c.name, "dtype": c.dtype} for c in tp.columns],
            },
            existing_metadata={},
        )
        info(f"RAG Agent: {schema}.{t} ({len(tp.columns)} columns)")
        sug = agent.run(ctx)
        all_suggestions.extend(sug)
        info(f"  -> {len(sug)} suggestions")

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


# ── Codebase Commands ───────────────────────────────────────────────────────


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
    cfg: AMXConfig, path: str | None, schema: str | None, code_profile: str | None
) -> None:
    """Scan a codebase for table/column references, save results to cache, and build the semantic code index."""
    from amx.codebase.analyzer import analyze_codebase
    from amx.codebase.cache import save_cached_report
    from amx.db.connector import DatabaseConnector

    schema = schema or cfg.current_schema
    if not schema:
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
    all_assets = db.list_assets(schema)
    tables = [name for name, _ in all_assets]
    catalog = frozenset(t.lower() for t in tables)

    column_names: list[str] = []
    seen_col: set[str] = set()
    with step_spinner(f"Collecting column names from {len(tables)} asset(s)"):
        for t in tables:
            for c in db.list_column_profiles(schema, t):
                k = c.name.lower()
                if k not in seen_col:
                    seen_col.add(k)
                    column_names.append(c.name)
                if len(column_names) >= 400:
                    break
            if len(column_names) >= 400:
                break

    info(f"Scanning {resolved} for references to {len(tables)} tables and {len(column_names)} columns...")
    try:
        _scan_progress = {"obj": None, "task": None}

        def _scan_cb(action: str, value: object) -> None:
            from rich.progress import BarColumn, MofNCompleteColumn, Progress, TextColumn, TimeElapsedColumn
            if action == "__total__":
                p = Progress(
                    TextColumn("[info]{task.description}[/info]"),
                    BarColumn(),
                    MofNCompleteColumn(),
                    TimeElapsedColumn(),
                    console=console,
                    transient=True,
                )
                p.start()
                _scan_progress["obj"] = p
                _scan_progress["task"] = p.add_task("Scanning files", total=int(value))  # type: ignore[arg-type]
            elif action == "__advance__" and _scan_progress["obj"]:
                p = _scan_progress["obj"]
                t = _scan_progress["task"]
                p.update(t, description=f"Scanning: {value}")
                p.advance(t)

        report = analyze_codebase(
            resolved,
            tables,
            column_names=column_names,
            known_catalog_tables=catalog,
            index_semantic=True,
            progress_callback=_scan_cb,
        )
        if _scan_progress["obj"]:
            _scan_progress["obj"].stop()
    except Exception as exc:
        error(str(exc))
        sys.exit(1)

    try:
        save_cached_report(
            profile_name=profile_nm,
            source_path=resolved,
            schema=schema,
            tables=tables,
            column_names=column_names,
            report=report,
        )
        success(f"Saved scan results to cache (profile {profile_nm!r})")
    except Exception as exc:
        warn(f"Could not save cache: {exc}")

    ref_count = sum(len(v) for v in report.references.values())
    ext_count = sum(len(v) for v in report.external_mentions.values())
    info(f"Scanned {report.scanned_files}/{report.total_files} files — {ref_count} catalog hits, {ext_count} external-style mentions")

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
        erows = [
            [asset, str(len(refs)), refs[0].file if refs else ""]
            for asset, refs in sorted(report.external_mentions.items())[:20]
        ]
        render_table(
            "Other identifiers (not in DB table list)",
            ["Token", "Ref Count", "Example File"],
            erows,
        )
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
        cp = cfg.resolve_code_path((code_profile or "").strip() or None, None)
    except KeyError as exc:
        error(str(exc))
        sys.exit(1)
    if not cp:
        error("No codebase path configured.")
        sys.exit(1)
    nm = ((code_profile or "").strip() or cfg.active_code_profile or "default").strip() or "default"
    invalidate_cache(nm, cp)
    delete_code_collection(source_filters=[cp])
    success(f"Cleared codebase cache for profile {nm!r} and reset semantic code index (`amx_code`).")


@code.command("results")
@click.option(
    "--code-profile",
    default=None,
    help="Show cached results for this profile (default: active profile).",
)
@click.pass_obj
def code_results_cmd(cfg: AMXConfig, code_profile: str | None) -> None:
    """Display the last cached code-scan results for a profile."""
    import datetime

    from amx.codebase.cache import load_latest_cached_report

    try:
        cp = cfg.resolve_code_path((code_profile or "").strip() or None, None)
    except KeyError as exc:
        error(str(exc))
        return
    if not cp:
        error("No codebase path configured.")
        return
    nm = ((code_profile or "").strip() or cfg.active_code_profile or "default").strip() or "default"

    manifest, report = load_latest_cached_report(nm, cp)
    if report is None or manifest is None:
        error(f"No cached code-scan for profile {nm!r}. Run `/code-scan` first.")
        return

    scanned_ts = manifest.get("scanned_at", 0)
    scanned_str = datetime.datetime.fromtimestamp(scanned_ts).strftime("%Y-%m-%d %H:%M:%S") if scanned_ts else "unknown"
    schema_str = manifest.get("schema", "?")
    table_count = len(manifest.get("tables", []))

    heading(f"Code-scan results — profile {nm!r}")
    info(f"Source: {cp}")
    info(f"Schema: {schema_str} ({table_count} tables)")
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
        erows = [
            [asset, str(len(refs)), refs[0].file if refs else ""]
            for asset, refs in sorted(report.external_mentions.items())[:20]
        ]
        render_table(
            "External identifiers (not in DB table list)",
            ["Token", "Ref Count", "Example File"],
            erows,
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
    import datetime

    from amx.codebase.cache import load_latest_cached_report

    try:
        cp = cfg.resolve_code_path((code_profile or "").strip() or None, None)
    except KeyError as exc:
        error(str(exc))
        return
    if not cp:
        error("No codebase path configured.")
        return
    nm = ((code_profile or "").strip() or cfg.active_code_profile or "default").strip() or "default"

    manifest, report = load_latest_cached_report(nm, cp)
    if report is None or manifest is None:
        error(f"No cached code-scan for profile {nm!r}. Run `/code-scan` first.")
        return

    scanned_ts = manifest.get("scanned_at", 0)
    scanned_str = datetime.datetime.fromtimestamp(scanned_ts).strftime("%Y-%m-%d %H:%M:%S") if scanned_ts else "unknown"
    schema_str = manifest.get("schema", "?")
    table_count = len(manifest.get("tables", []))

    out = output_file or f"code_report_{nm}_{schema_str}.md"

    lines: list[str] = [
        f"# Code-scan report — profile `{nm}`",
        "",
        f"- **Source:** `{cp}`",
        f"- **Schema:** `{schema_str}` ({table_count} tables)",
        f"- **Scanned:** {scanned_str}",
        f"- **Files:** {report.scanned_files}/{report.total_files}",
        "",
    ]

    ref_count = sum(len(v) for v in report.references.values())
    ext_count = sum(len(v) for v in report.external_mentions.values())
    lines.append(f"**Catalog hits:** {ref_count}  |  **External mentions:** {ext_count}")
    lines.append("")

    if report.references:
        lines.append("## Catalog references")
        lines.append("")
        lines.append("| Asset | Ref Count | Example File |")
        lines.append("|-------|-----------|--------------|")
        for asset, refs in sorted(report.references.items()):
            example = refs[0].file if refs else ""
            lines.append(f"| {asset} | {len(refs)} | {example} |")
        lines.append("")

    if report.external_mentions:
        lines.append("## External identifiers")
        lines.append("")
        lines.append("| Token | Ref Count | Example File |")
        lines.append("|-------|-----------|--------------|")
        for asset, refs in sorted(report.external_mentions.items()):
            example = refs[0].file if refs else ""
            lines.append(f"| {asset} | {len(refs)} | {example} |")
        lines.append("")

    if report.references:
        lines.append("## Detailed references")
        lines.append("")
        for asset, refs in sorted(report.references.items()):
            lines.append(f"### `{asset}` ({len(refs)} hit{'s' if len(refs) != 1 else ''})")
            lines.append("")
            for r in refs[:5]:
                lines.append(f"**{r.file}:{r.line_no}**")
                lines.append("```")
                lines.append(r.context)
                lines.append("```")
                lines.append("")
            if len(refs) > 5:
                lines.append(f"*… and {len(refs) - 5} more*")
                lines.append("")

    from pathlib import Path

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
    import json
    from pathlib import Path

    from amx.agents.base import AgentContext
    from amx.agents.code_agent import CodeAgent
    from amx.codebase.cache import load_latest_cached_report
    from amx.db.connector import DatabaseConnector
    from amx.llm.provider import LLMProvider

    if not cfg.llm.provider or not cfg.llm.model:
        error("LLM not configured. Run `amx setup` first.")
        sys.exit(1)

    try:
        cp = cfg.resolve_code_path((code_profile or "").strip() or None, None)
    except KeyError as exc:
        error(str(exc))
        return
    if not cp:
        error("No codebase path configured. Run `/code` then `/add-code-profile` first.")
        return
    nm = ((code_profile or "").strip() or cfg.active_code_profile or "default").strip() or "default"

    _, code_report = load_latest_cached_report(nm, cp)
    if code_report is None:
        error(f"No cached code-scan for profile {nm!r}. Run `/code-scan` first.")
        return

    token_tracker.reset()

    llm = LLMProvider(cfg.llm)
    db = DatabaseConnector(cfg.db)
    if not db.test_connection():
        error("Cannot connect to database.")
        sys.exit(1)

    tables_arg = list(tables_pos) + list(table)
    scope = _finalize_scope(cfg, db, schema or cfg.current_schema, tables_arg)
    if scope is None:
        return
    schema = next(iter(scope))
    tables = scope[schema]

    agent = CodeAgent(llm, code_report)
    all_suggestions = []
    for t in tables:
        with step_spinner(f"Reading columns for {schema}.{t}"):
            columns = db.list_column_profiles(schema, t)
        ctx = AgentContext(
            schema=schema,
            table=t,
            db_profile={
                "row_count": 0,
                "columns": [{"name": c.name, "dtype": c.dtype} for c in columns],
            },
            existing_metadata={},
        )
        info(f"Code Agent: {schema}.{t} ({len(columns)} columns)")
        sug = agent.run(ctx)
        all_suggestions.extend(sug)
        info(f"  -> {len(sug)} suggestions")

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


@analyze.command("run")
@click.argument("tables_pos", nargs=-1, metavar="[ASSET ...]")
@click.option("--schema", "-s", help="Schema to analyze.")
@click.option("--table", "-t", multiple=True, help="Specific asset(s). Omit for interactive selection.")
@click.option("--apply/--no-apply", default=False, help="Apply approved metadata to the database.")
@click.option(
    "--code-refresh",
    is_flag=True,
    default=False,
    help="Invalidate codebase disk cache and rebuild semantic code index on this run.",
)
@click.option(
    "--code-profile",
    default=None,
    help="Use this named codebase profile path (otherwise active profile).",
)
@click.option(
    "--mode",
    type=click.Choice(["chat", "batch"], case_sensitive=False),
    default=None,
    help=(
        "Completion mode: 'chat' = Chat Completions (real-time, full price); "
        "'batch' = Batch API (async, ~50 %% cheaper)."
    ),
)
@click.pass_obj
def analyze_run(
    cfg: AMXConfig,
    tables_pos: tuple[str, ...],
    schema: str | None,
    table: tuple[str, ...],
    apply: bool,
    code_refresh: bool,
    code_profile: str | None,
    mode: str | None,
) -> None:
    """Run all agents to infer metadata for selected assets (tables, views, etc.).

    Assets can be passed as positional arguments (e.g. /run vbrk vbrp) or via --table.
    Scope levels: Database (all schemas) → Schema (all assets) → Asset (specific picks).
    """
    from amx.db.connector import DatabaseConnector
    from amx.utils.live_display import get_display

    try:
        # ── 1. Initial connection test (IMMEDIATE FEEDBACK) ───────────────────
        db_init = DatabaseConnector(cfg.db)
        display = get_display()
        display.start(
            schema=schema or cfg.current_schema or "",
            table=(table[0] if table else (tables_pos[0] if tables_pos else cfg.current_table or "")),
            mode="setup",
            provider=cfg.llm.provider,
            model=cfg.llm.model,
        )
        try:
            with step_spinner("Testing database connection..."):
                if not db_init.test_connection():
                    error("Cannot connect to database.")
                    sys.exit(1)
        finally:
            display.stop()

        # ── 2. Run the actual logic ───────────────────────────────────────────
        _analyze_run_logic(cfg, schema, table, apply, mode, tables_pos, db_init)
    except KeyboardInterrupt:
        warn("User interrupted process.")
        return
    except Exception as exc:
        raise click.ClickException(str(exc))

def _analyze_run_logic(
    cfg: AMXConfig,
    schema: str | None,
    table: tuple[str, ...],
    apply: bool,
    mode: str | None,
    tables_pos: tuple[str, ...],
    db: DatabaseConnector,  # Passed from the early connection test
) -> None:
    from amx.agents.orchestrator import Orchestrator
    from amx.db.connector import DatabaseConnector
    from amx.docs.rag import RAGStore
    from amx.llm.batch import supported_providers as batch_supported_providers
    from amx.llm.provider import LLMProvider
    from amx.config import DISABLED_PROFILE

    # Safe defaults used by interrupt/failure handlers before run setup completes.
    use_batch = False
    all_results: list = []
    run_id: int | None = None
    run_started = time.monotonic()
    total_assets = 0
    total_schemas = 0
    approved: list = []
    skipped: list = []
    final_status: str | None = None
    final_error_text = ""

    try:
        token_tracker.reset()

        if not cfg.llm.provider or not cfg.llm.model:
            error("LLM not configured. Run `amx setup` first.")
            sys.exit(1)

        llm = LLMProvider(cfg.llm)

        if not apply:
            warn(
                "Without --apply, approved metadata is not written to the database. "
                "Use `/analyze` then `/apply`, or `/run-apply`, to persist comments."
            )

        # ── 2. Profile selection ───────────────────────────────────────────────
        if confirm("Do you want to modify profiles before run?", default=False):
            # 1. DB Profile
            db_names = list(cfg.db_profiles.keys())
            if db_names:
                db_choice = ask_choice("Select DB profile", db_names, default=cfg.active_db_profile)
                cfg.set_active_db_profile(db_choice)
                info(f"Active DB: [bold cyan]{db_choice}[/]")
                # Re-test if profile changed
                db = DatabaseConnector(cfg.db)
                with step_spinner("Testing new database connection..."):
                    if not db.test_connection():
                        error(f"Cannot connect to database using profile '{db_choice}'.")
                        sys.exit(1)

            # 2. LLM Profile
            llm_names = list(cfg.llm_profiles.keys())
            if llm_names:
                llm_choice = ask_choice("Select LLM profile", llm_names, default=cfg.active_llm_profile)
                cfg.set_active_llm_profile(llm_choice)
                info(f"Active LLM: [bold cyan]{llm_choice}[/]")
                llm = LLMProvider(cfg.llm)

            # 3. Document Profile
            doc_names = list(cfg.doc_profiles.keys())
            if doc_names:
                options = doc_names + [DISABLED_PROFILE]
                doc_choice = ask_choice("Select Document profile", options, default=cfg.active_doc_profile or DISABLED_PROFILE)
                cfg.active_doc_profile = doc_choice
                info(f"Active Docs: [bold cyan]{doc_choice}[/]")

            # 4. Codebase Profile
            code_names = list(cfg.code_profiles.keys())
            if code_names:
                options = code_names + [DISABLED_PROFILE]
                code_choice = ask_choice("Select Codebase profile", options, default=cfg.active_code_profile or DISABLED_PROFILE)
                cfg.active_code_profile = code_choice
                info(f"Active Code: [bold cyan]{code_choice}[/]")

            # Persist wizard profile selections immediately to avoid in-memory-only state.
            cfg.save()
            info("Profile selections saved to config.yml.")
            console.print()

        # ── Mode selection ────────────────────────────────────────────────────────
        batch_capable = llm.supports_batch
        batch_providers_list = batch_supported_providers()

        if mode is None:
            cfg_mode = (cfg.llm.completion_mode or "chat_completions").lower()
            default_mode_label = "batch" if cfg_mode == "batch" else "chat"

            from amx.utils.console import ask_choice as _ask_choice
            batch_note = (
                " (50 % cheaper, async)"
                if batch_capable
                else f" (requires {', '.join(batch_providers_list)})"
            )
            mode = _ask_choice(
                "Select completion mode",
                ["chat", "batch"],
                default=default_mode_label,
                descriptions={
                    "chat": "Chat Completions — real-time, live spinners, full price",
                    "batch": f"Batch API{batch_note} — submit all at once, results in minutes–hours",
                },
            )

        use_batch = mode == "batch"

        if use_batch and not batch_capable:
            warn(
                f"Provider '{cfg.llm.provider}' does not support batch mode. "
                f"Supported providers: {', '.join(batch_providers_list)}. "
                "Falling back to Chat Completions."
            )
            use_batch = False

        if use_batch:
            from rich.panel import Panel
            from amx.utils.console import console as _console
            _console.print(Panel(
                "[bold]Batch API selected.[/bold]\n"
                "All LLM requests will be submitted as a single batch job.\n"
                "Typical turnaround: [bold]2–30 minutes[/bold]  |  Cost: [bold green]~50 % lower[/bold green]\n"
                "[dim]Live polling status will appear below.[/dim]",
                title="[cyan]Mode: Batch[/cyan]", border_style="cyan",
            ))
        else:
            info("Mode: [bold]Chat Completions[/bold] (real-time)")

        # ── Scope resolution ──────────────────────────────────────────────────────

        tables_arg = list(tables_pos) + list(table)
        scope = _finalize_scope(cfg, db, schema, tables_arg)
        if scope is None:
            return

        total_assets = sum(len(v) for v in scope.values())

        # ── Review strategy ───────────────────────────────────────────────────────
        review_strategy = "individual"
        if not use_batch and total_assets > 1:
            review_strategy = ask_choice(
                "Review strategy",
                ["individual", "deferred"],
                default="individual",
                descriptions={
                    "individual": "Assess each asset (table) as it becomes ready",
                    "deferred": "Process everything first, then review all together at the end",
                }
            )
        hs = history_store()
        if hs is not None:
            try:
                run_id = hs.create_run(
                    command="analyze.run",
                    mode=("batch" if use_batch else "chat"),
                    db_backend=cfg.db.backend,
                    db_profile=cfg.active_db_profile,
                    llm_provider=cfg.llm.provider,
                    llm_model=cfg.llm.model,
                    scope=scope,
                )
            except Exception as exc:
                warn(f"History persistence disabled for this run: {exc}")

        total_schemas = len(scope)
        approved = []
        skipped = []
        all_results: list = []  # tracks all ReviewResult objects; used for cancel vs fail detection
        processed_assets: list[str] = []
        try:
            scope_summary = (
                f"{total_assets} asset(s) across {total_schemas} schema(s)"
                if total_schemas > 1
                else f"{total_assets} asset(s) in {next(iter(scope))}"
            )
            info(f"Scope: {scope_summary}")

            rag_store = None
            try:
                if cfg.active_doc_profile == DISABLED_PROFILE:
                    info("RAG Agent disabled (document profile: none).")
                else:
                    doc_filters = cfg.effective_doc_paths()
                    store = RAGStore(source_filters=doc_filters)
                    visible_chunks = store.doc_count
                    if visible_chunks > 0:
                        rag_store = store
                        if doc_filters:
                            info(
                                f"RAG store has {visible_chunks} chunks available "
                                f"for active doc profile '{cfg.active_doc_profile or 'default'}'"
                            )
                        else:
                            info(f"RAG store has {visible_chunks} chunks available")
                    elif doc_filters:
                        info(
                            f"RAG store has 0 chunks for active doc profile "
                            f"'{cfg.active_doc_profile or 'default'}'"
                        )
            except Exception:
                pass

            code_profile = cfg.active_code_profile
            code_refresh = False # default
            code_report = _resolve_codebase_for_run(cfg, db, scope, code_profile, code_refresh)

            # Re-baseline token accounting right before actual agent execution.
            # This avoids cross-command contamination in long interactive sessions.
            token_tracker.reset()

            from amx.utils.live_display import get_display
            display = get_display()

            all_results: list = []

            for schema_name, assets in scope.items():
                asset_kinds = {name: db.resolve_asset_kind(schema_name, name) for name in assets}

                orch = Orchestrator(db, llm, rag_store=rag_store, code_report=code_report, run_id=run_id)

                display_label = (
                    ", ".join(assets) if len(assets) <= 3
                    else f"{len(assets)} assets"
                )
                display.start(
                    schema=schema_name,
                    table=display_label,
                    mode="batch" if use_batch else "chat",
                    provider=cfg.llm.provider,
                    model=cfg.llm.model,
                )

                try:
                    if use_batch:
                        results = orch.process_tables_batch_mode(
                            schema_name, list(assets), asset_kinds=asset_kinds,
                        )
                        all_results.extend(results)
                        processed_assets.extend([f"{schema_name}.{asset_name}" for asset_name in assets])
                    else:
                        for asset_name in assets:
                            display.set_context(table=asset_name)
                            results = orch.process_table(
                                schema_name, asset_name,
                                asset_kind=asset_kinds.get(asset_name),
                                interactive_review=(review_strategy == "individual"),
                            )
                            all_results.extend(results)
                            processed_assets.append(f"{schema_name}.{asset_name}")

                        # Point 1: Schema-level meta analysis
                        if len(assets) > 1 or total_schemas > 1:
                            schema_meta = orch.process_schema_meta(schema_name, all_results)
                            all_results.extend(schema_meta)
                finally:
                    display.stop()

            # Point 1: Database-level meta analysis
            if total_schemas > 1:
                db_meta = orch.process_database_meta(all_results)
                all_results.extend(db_meta)

            # Review any pending items (deferred table items, plus schema/database meta items).
            all_results = orch.batch_review(all_results)

            # Defensive consistency: if docs are disabled, ignore any accidental rag token records.
            if rag_store is None:
                token_tracker.drop_steps({"rag_agent", "rag_agent(batch)"})

            heading("Summary")
            render_token_summary(token_tracker)
            approved = [r for r in all_results if r.applied]
            skipped = [r for r in all_results if not r.applied]
            info(f"Approved: {len(approved)}  |  Skipped: {len(skipped)}")

            if approved:
                def _asset_label(r):
                    if r.asset_kind == "database": return "[bold cyan]DATABASE[/]"
                    if r.asset_kind == "schema": return f"[cyan]SCHEMA: {r.schema}[/]"
                    asset = f"{r.table}.{r.column}" if r.column else r.table
                    return asset

                render_table(
                    "Approved metadata",
                    ["Asset", "Description", "Confidence", "Source"],
                    [
                        [
                            f"{r.schema}.{r.table}.{r.column}" if r.column else (f"{r.schema}.{r.table}" if r.table else r.schema),
                            (r.final_description or "")[:60],
                            r.confidence.value,
                            r.source,
                        ]
                        for r in approved
                    ],
                )

            if approved:
                from amx.pending_review import save_pending

                save_pending(approved)
                if not apply:
                    info(
                        f"Saved {len(approved)} approved description(s) as pending. "
                        "Run `/analyze` then `/apply` (or `/run-apply` next time) to write them to the database."
                    )

            if apply and approved:
                if confirm("Apply these metadata comments to the database?"):
                    from amx.pending_review import clear_pending

                    orch.apply_results(approved)
                    clear_pending()
        except Exception:
            raise
        final_status = "success"
    except KeyboardInterrupt:
        # Preserve partial work on interrupt (completed assets/results so far).
        approved = [r for r in all_results if getattr(r, "applied", False)]
        skipped = [r for r in all_results if not getattr(r, "applied", False)]
        if approved:
            try:
                from amx.pending_review import save_pending
                save_pending(approved)
            except Exception:
                pass

        # User pressed Ctrl+C. If any suggestions/results were produced, keep run reviewable.
        has_reviewable_results = bool(all_results)
        hs = history_store()
        if not has_reviewable_results and run_id is not None and hs is not None:
            try:
                has_reviewable_results = bool(hs.get_run_results(run_id))
            except Exception:
                pass
        if not has_reviewable_results:
            has_reviewable_results = bool(token_tracker.total_tokens)

        kb_status = "ready_for_review" if has_reviewable_results else "cancelled"
        final_status = kb_status
        final_error_text = "Interrupted by user"
        _log_app_event(
            event_type="analyze_run",
            status=kb_status,
            command="analyze.run",
            details={
                "mode": ("batch" if use_batch else "chat"),
                "error": "KeyboardInterrupt",
                "results_ready": has_reviewable_results,
            },
        )
        warn("User interrupted process.")
        return
    except Exception as exc:
        final_status = "failed"
        final_error_text = str(exc)
        _log_app_event(
            event_type="analyze_run",
            status="failed",
            command="analyze.run",
            details={"error": str(exc), "mode": ("batch" if use_batch else "chat")},
        )
        raise
    finally:
        # Always finalize the run to avoid stale 'running' rows in /history list.
        if run_id is not None:
            hs = history_store()
            if hs is not None:
                try:
                    status = final_status or "success"
                    hs.finish_run(
                        run_id,
                        status=status,
                        metrics={
                            "duration_sec": round(time.monotonic() - run_started, 3),
                            "model_processing_sec": round(token_tracker.total_model_processing_sec, 3),
                            "total_assets": total_assets,
                            "total_schemas": total_schemas,
                            "processed_assets_count": len(processed_assets),
                            "processed_assets": processed_assets,
                            "approved_count": len(approved),
                            "skipped_count": len(skipped),
                            "applied_flag": bool(apply),
                        },
                        tokens={
                            "total_tokens": token_tracker.total_tokens,
                            "summary": token_tracker.summary(),
                            "records": token_tracker.records(),
                        },
                        results={
                            "all_results": [
                                {
                                    "schema": r.schema,
                                    "table": r.table,
                                    "column": r.column,
                                    "description": r.final_description,
                                    "confidence": r.confidence.value,
                                    "source": r.source,
                                    "asset_kind": r.asset_kind,
                                    "applied": bool(r.applied),
                                }
                                for r in all_results
                            ],
                            "approved": [
                                {
                                    "schema": r.schema,
                                    "table": r.table,
                                    "column": r.column,
                                    "description": r.final_description,
                                    "confidence": r.confidence.value,
                                    "source": r.source,
                                    "asset_kind": r.asset_kind,
                                }
                                for r in approved
                            ],
                            "skipped": [
                                {
                                    "schema": r.schema,
                                    "table": r.table,
                                    "column": r.column,
                                    "confidence": r.confidence.value,
                                    "source": r.source,
                                    "asset_kind": r.asset_kind,
                                }
                                for r in skipped
                            ],
                        },
                        error_text=final_error_text,
                    )
                except Exception as exc:
                    warn(f"Could not persist run history finalization: {exc}")
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
