"""AMX CLI — Agentic Metadata Extractor command-line interface."""

from __future__ import annotations

import os
import shlex
import signal
import sys
from dataclasses import replace
from pathlib import Path

import click
from prompt_toolkit.completion import Completer, Completion
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.formatted_text import HTML
from prompt_toolkit.styles import Style
from prompt_toolkit.shortcuts import CompleteStyle, PromptSession

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
    render_token_summary,
    show_banner,
    step_spinner,
    success,
    warn,
)
from amx.utils.logging import LAST_PROFILE_RESPONSE_FILE, LOG_DIR, get_logger
from amx.utils.token_tracker import tracker as token_tracker

log = get_logger("cli")

pass_config = click.make_pass_decorator(AMXConfig, ensure=True)

_NS_STATE: dict[str, str] = {"namespace": ""}


def _print_interactive_startup_summary(cfg: AMXConfig) -> None:
    """Show version, config location, and active profiles when the session starts."""
    cfg_path = Path(cfg.CONFIG_DIR) / "config.yml"
    info(f"Version {__version__} · Config file: {cfg_path}")
    info(f"Logs directory: {LOG_DIR} (amx.log · on LLM parse errors: {LAST_PROFILE_RESPONSE_FILE.name})")
    info(
        f"Database: profile '{cfg.active_db_profile}' → "
        f"{cfg.db.database} @ {cfg.db.host}:{cfg.db.port} (user {cfg.db.user})"
    )
    llm_line = (
        f"{cfg.llm.provider or '(unset)'}/{cfg.llm.model or '(unset)'}"
        if cfg.llm.model or cfg.llm.provider
        else "(not configured — run /setup)"
    )
    info(f"LLM: profile '{cfg.active_llm_profile}' → {llm_line}")
    info(
        f"Context: schema={cfg.current_schema or '—'} · table={cfg.current_table or '—'} "
        f"(set in /db with /schema, /table)"
    )
    info(
        "Approved descriptions are written to the database as COMMENT ON TABLE / COLUMN."
    )


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


def _kb_escape_namespace() -> KeyBindings:
    kb = KeyBindings()

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


def _interactive_session(cfg: AMXConfig) -> None:
    """Start AMX interactive slash-command shell.

    Design: all Rich output happens *between* PromptSession.prompt() calls,
    never concurrently. This avoids patch_stdout() entirely, which prevents:
      - Raw ANSI leaking as ?[1;35m… in Terminal.app
      - Ghost 'amx>' lines on terminal resize
    """
    heading("AMX Interactive Session")
    _print_interactive_startup_summary(cfg)
    info("Type /help for commands, /exit to quit.")
    info("Namespaces: /db, /docs, /llm, /code, /analyze (use /back or Esc to return).")
    info("Tip: start typing / and use ↑/↓ to pick a command.")
    namespace = ""

    _db_cmd_heads = frozenset(
        {
            "db-profiles",
            "use-db",
            "add-db-profile",
            "remove-db-profile",
            "connect",
            "c",
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
        {"llm-profiles", "use-llm", "add-llm-profile", "remove-llm-profile"}
    )
    _code_cmd_heads = frozenset({
        "code-profiles", "use-code", "add-code-profile", "remove-code-profile",
        "code-scan", "code-refresh", "code-results", "code-analyze",
        "export-code-report",
    })
    _analyze_cmd_heads = frozenset({
        "run", "run-apply", "apply",
    })

    prev_sigwinch = signal.getsignal(signal.SIGWINCH)

    session = PromptSession(
        completer=_SlashCompleter(lambda: namespace, cfg),
        key_bindings=_kb_escape_namespace(),
        mouse_support=False,
        bottom_toolbar=HTML(
            "<b>↑↓</b> navigate · <b>Enter</b> select · "
            "<b>Esc</b> go back · <b>Ctrl+C</b> exit"
        ),
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

    try:
        while True:
            _NS_STATE["namespace"] = namespace
            prefix = f"amx/{namespace}" if namespace else "amx"
            try:
                raw = session.prompt(f"{prefix}> ").strip()
            except (EOFError, KeyboardInterrupt):
                console.print()
                success("Session closed.")
                return

            if raw == "__amx_esc_back__":
                namespace = ""
                info("Back to root namespace (Esc).")
                continue
            if raw == "__amx_esc_root__":
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
            if cmdline in {"help", "?"}:
                _print_session_help(namespace=namespace, cfg=cfg)
                continue
            if cmdline == "back":
                namespace = ""
                info("Back to root namespace.")
                continue
            if cmdline in {"db", "docs", "llm", "code", "analyze"}:
                namespace = cmdline
                info(f"Entered /{namespace} namespace.")
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
                exc.show()
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
        out.print(
            f"""
[heading]Help — /db namespace[/heading]
Context:
  Active DB profile: [cyan]{active}[/cyan]
  Current schema: [cyan]{ctx_schema}[/cyan]
  Current table:  [cyan]{ctx_table}[/cyan]

Commands (in order):
  1) /back                         Return to root namespace
  2) /db-profiles                  List DB connection profiles
  3) /use-db <name>                Switch active DB profile
  4) /add-db-profile [name]        Create/update a DB profile (interactive)
  5) /remove-db-profile <name>     Remove a DB profile (cannot remove last)
  6) /save                         Persist config to disk (~/.amx/config.yml)
  7) /schema <name>                Set current schema context (used by /tables)
  8) /table <name>                 Set current table context (used by /profile)
  9) /connect                      Test DB connectivity
 10) /schemas                      List schemas
 11) /tables [schema]             List tables (defaults to current schema)
 12) /profile [schema] [table]    Profile a table (defaults to current context)

Aliases:
  /c == /connect

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
  1) /back                         Return to root namespace
  2) /llm-profiles                 List LLM profiles
  3) /use-llm <name>               Switch active LLM profile
  4) /add-llm-profile [name]       Add/update an LLM profile (interactive)
  5) /remove-llm-profile <name>    Remove an LLM profile

Navigation:
  Esc (empty line)                 Go back to root namespace
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
  2) /run [TABLE …] [--schema …] [--table …] [--apply] [--code-refresh] [--code-profile NAME]
                                   Run all agents; tables as args or picked interactively
  3) /run-apply [TABLE …] [--schema …] [--table …]   Same as /run --apply
  4) /apply                        Write pending approved comments to the database

Tip: scan code and docs first (`/code-scan`, `/doc-analyze`, `/code-analyze`), then `/run`.

Navigation:
  Esc (empty line)                 Go back to root namespace
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

Inside namespaces (examples):
  [bright_white]/db[/bright_white]   → /db-profiles, /schema, /table, /connect, …
  [bright_white]/docs[/bright_white] → /doc-profiles, /add-doc-profile, /ingest, …
  [bright_white]/llm[/bright_white]   → /llm-profiles, /add-llm-profile, …
  [bright_white]/code[/bright_white] → /code-profiles, /add-code-profile, …

Global shortcuts (work anywhere):
  /save                            Persist ~/.amx/config.yml

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
        ("/setup", "Run setup wizard"),
        ("/config", "Show configuration"),
        ("/db", "Enter /db namespace"),
        ("/docs", "Enter /docs namespace"),
        ("/llm", "Enter /llm namespace"),
        ("/code", "Enter /code namespace"),
        ("/analyze", "Enter /analyze namespace"),
        ("/save", "Save config to disk"),
    ]

    db_cmds: list[tuple[str, str]] = [
        ("/back", "Return to root namespace"),
        ("/db-profiles", "List DB profiles"),
        ("/use-db", "Switch DB profile (/use-db <name>)"),
        ("/add-db-profile", "Create/update DB profile"),
        ("/remove-db-profile", "Remove DB profile (/remove-db-profile <name>)"),
        ("/save", "Save config to disk"),
        ("/schema", "Set current schema (/schema <name>)"),
        ("/table", "Set current table (/table <name>)"),
        ("/connect", "Test DB connectivity"),
        ("/c", "Alias of /connect"),
        ("/schemas", "List schemas"),
        ("/tables", "List tables (/tables [schema])"),
        ("/profile", "Profile table (/profile [schema] [table])"),
    ]

    docs_cmds: list[tuple[str, str]] = [
        ("/back", "Return to root namespace"),
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
        ("/llm-profiles", "List LLM profiles"),
        ("/use-llm", "Switch LLM profile (/use-llm <name>)"),
        ("/add-llm-profile", "Add/update LLM profile"),
        ("/remove-llm-profile", "Remove LLM profile (/remove-llm-profile <name>)"),
    ]

    code_cmds: list[tuple[str, str]] = [
        ("/back", "Return to root namespace"),
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
        ("/run", "Run all agents (/run [TABLE …] [--schema …] [--table …] [--apply])"),
        ("/run-apply", "Run + apply (/run-apply [TABLE …] [--schema …] [--table …])"),
        ("/apply", "Write pending comments to the database"),
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
        _cmd_use(cfg, parts[1:])
        return True
    if head == "add-db-profile":
        if not _require_namespace(head, namespace, "db", "add-db-profile"):
            return True
        _cmd_add_profile(cfg, parts[1:])
        return True
    if head == "remove-db-profile":
        if not _require_namespace(head, namespace, "db", "remove-db-profile"):
            return True
        _cmd_remove_profile(cfg, parts[1:])
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


def _cmd_profiles(cfg: AMXConfig) -> None:
    rows = []
    for name, db in sorted(cfg.db_profiles.items(), key=lambda x: x[0]):
        mark = "*" if name == cfg.active_db_profile else " "
        rows.append([f"{mark} {name}", db.host, str(db.port), db.user, db.database])
    render_table(
        "DB profiles (* = active)",
        ["Profile", "Host", "Port", "User", "Database"],
        rows,
    )


def _cmd_use(cfg: AMXConfig, rest: list[str]) -> None:
    if len(rest) >= 1:
        name = rest[0]
    else:
        names = sorted(cfg.db_profiles.keys())
        if not names:
            error("No profiles configured.")
            return
        name = ask_choice("Select profile", names, default=cfg.active_db_profile)
    try:
        cfg.set_active_db_profile(name)
        cfg.save()
        success(f"Switched active DB profile to: {name}")
    except Exception as exc:
        error(str(exc))


def _cmd_add_profile(cfg: AMXConfig, rest: list[str]) -> None:
    if len(rest) >= 1:
        name = rest[0]
    else:
        name = ask("Profile name", default="local")
    info(f"Creating/updating profile: {name}")
    host = ask("Database host", cfg.db.host)
    port = int(ask("Port", str(cfg.db.port)))
    user = ask("Username", cfg.db.user)
    password = ask_password("Password")
    database = ask("Database name", cfg.db.database)
    db = DBConfig(host=host, port=port, user=user, password=password or "", database=database)
    cfg.upsert_db_profile(name, db)
    cfg.set_active_db_profile(name)
    cfg.save()
    success(f"Profile saved and activated: {name}")


def _cmd_remove_profile(cfg: AMXConfig, rest: list[str]) -> None:
    if len(rest) < 1:
        error("Usage: /remove-db-profile <name>")
        return
    name = rest[0]
    try:
        cfg.remove_db_profile(name)
        cfg.save()
        success(f"Removed profile: {name} (active: {cfg.active_db_profile})")
    except Exception as exc:
        error(str(exc))


def _interactive_llm_block(defaults: LLMConfig) -> LLMConfig:
    provider = ask_choice(
        "Select AI provider",
        ["openai", "anthropic", "gemini", "deepseek", "local", "kimi", "ollama"],
        default=defaults.provider or "openai",
    )
    info(
        "Model: use a short id (e.g. gpt-4o) or LiteLLM form openai/gpt-4o — "
        "see https://docs.litellm.ai/docs/providers"
    )
    model = ask("Model name", defaults.model or _default_model(provider))
    api_base = defaults.api_base
    if provider in ("local", "ollama", "kimi"):
        api_base = ask("API base URL", api_base or "http://localhost:11434/v1")
    api_key = ask_password("API key") or defaults.api_key
    return LLMConfig(
        provider=provider,
        model=model.strip(),
        api_key=api_key,
        api_base=api_base,
        temperature=defaults.temperature,
        max_tokens=defaults.max_tokens,
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


def _cmd_doc_profiles(cfg: AMXConfig) -> None:
    if not cfg.doc_profiles:
        info("No document profiles. Use /add-doc-profile <name>")
        return
    rows = []
    for name, paths in sorted(cfg.doc_profiles.items(), key=lambda x: x[0]):
        mark = "*" if name == cfg.active_doc_profile else " "
        preview = "; ".join(paths[:2]) + (" …" if len(paths) > 2 else "")
        rows.append([f"{mark} {name}", str(len(paths)), preview])
    render_table("Document profiles (* = active)", ["Profile", "# paths", "Preview"], rows)


def _cmd_use_doc(cfg: AMXConfig, rest: list[str]) -> None:
    if len(rest) >= 1:
        name = rest[0]
    else:
        names = sorted(cfg.doc_profiles.keys())
        if not names:
            error("No document profiles.")
            return
        name = ask_choice("Select document profile", names, default=cfg.active_doc_profile)
    if name not in cfg.doc_profiles:
        error(f"Unknown document profile: {name}")
        return
    cfg.active_doc_profile = name
    cfg.save()
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
    for name, path in sorted(cfg.code_profiles.items(), key=lambda x: x[0]):
        mark = "*" if name == cfg.active_code_profile else " "
        rows.append([f"{mark} {name}", path])
    render_table("Codebase profiles (* = active)", ["Profile", "Path / URL"], rows)


def _cmd_use_code(cfg: AMXConfig, rest: list[str]) -> None:
    if len(rest) >= 1:
        name = rest[0]
    else:
        names = sorted(cfg.code_profiles.keys())
        if not names:
            error("No codebase profiles.")
            return
        name = ask_choice("Select codebase profile", names, default=cfg.active_code_profile)
    if name not in cfg.code_profiles:
        error(f"Unknown codebase profile: {name}")
        return
    cfg.active_code_profile = name
    cfg.save()
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
        "c": ["db", "connect"],
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

    if head in {"db", "docs", "llm", "code", "analyze", "setup", "config"}:
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
    host = ask("Database host (e.g. localhost)")
    while not host:
        warn("Host is required.")
        host = ask("Database host (e.g. localhost)")
    cfg.db.host = host

    port_raw = ask("Port (e.g. 5432)")
    while not port_raw.isdigit():
        warn("Port must be a number (e.g. 5432).")
        port_raw = ask("Port (e.g. 5432)")
    cfg.db.port = int(port_raw)

    user = ask("Username")
    while not user:
        warn("Username is required.")
        user = ask("Username")
    cfg.db.user = user

    cfg.db.password = ask_password("Password")

    database = ask("Database name (e.g. postgres)")
    while not database:
        warn("Database name is required.")
        database = ask("Database name (e.g. postgres)")
    cfg.db.database = database

    # Persist DB credentials into the active profile (multi-connection support).
    if not cfg.active_db_profile:
        cfg.active_db_profile = "default"
    cfg.upsert_db_profile(cfg.active_db_profile, cfg.db)
    cfg.apply_active_db_profile()

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
    ft = _finalize_table_scope(cfg, db, schema or cfg.current_schema, tables_arg)
    if ft is None:
        return
    schema, tables = ft

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
    tables = db.list_tables(schema)
    catalog = frozenset(t.lower() for t in tables)

    column_names: list[str] = []
    seen_col: set[str] = set()
    with step_spinner(f"Collecting column names from {len(tables)} tables"):
        for t in tables:
            tp = db.profile_table(schema, t)
            for c in tp.columns:
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
    delete_code_collection()
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
    ft = _finalize_table_scope(cfg, db, schema or cfg.current_schema, tables_arg)
    if ft is None:
        return
    schema, tables = ft

    agent = CodeAgent(llm, code_report)
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
        info(f"Code Agent: {schema}.{t} ({len(tp.columns)} columns)")
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


# ── Analysis Commands ───────────────────────────────────────────────────────


@main.group()
def analyze() -> None:
    """Run metadata inference agents."""


def _validate_tables_in_schema(db: object, schema: str, tables: list[str]) -> list[str]:
    """Map user input to real table names (case-insensitive). Raise ValueError if any name is unknown."""
    from difflib import get_close_matches

    if not tables:
        raise ValueError("No tables selected.")
    avail = db.list_tables(schema)
    avail_set = set(avail)
    by_lower = {t.lower(): t for t in avail}
    resolved: list[str] = []
    missing: list[str] = []
    for t in tables:
        if t in avail_set:
            resolved.append(t)
        elif t.lower() in by_lower:
            resolved.append(by_lower[t.lower()])
        else:
            missing.append(t)
    if not missing:
        return resolved
    parts: list[str] = []
    for m in missing:
        close = get_close_matches(m, avail, n=5, cutoff=0.35)
        parts.append(f"{m!r}" + (f" (similar: {close})" if close else ""))
    raise ValueError(f"Unknown table(s) in schema {schema!r}: " + ", ".join(parts))


def _finalize_table_scope(
    cfg: AMXConfig,
    db: object,
    schema: str | None,
    table_args: list[str],
) -> tuple[str, list[str]] | None:
    """Resolve interactive / CLI table selection and validate names against the database."""
    schema, tables = _resolve_run_scope(cfg, db, schema, table_args)
    if not tables:
        error(
            "No tables selected. At the prompt, use numbers from the list, exact table names, "
            "comma-separated lists, or `all`. Enter alone cancels."
        )
        return None
    try:
        return schema, _validate_tables_in_schema(db, schema, tables)
    except ValueError as exc:
        error(str(exc))
        return None


def _resolve_run_scope(
    cfg: AMXConfig,
    db: object,
    schema: str | None,
    table_args: list[str],
) -> tuple[str, list[str]]:
    """Shared schema/table resolution for /run and /run-apply."""
    if schema is None and not table_args:
        if cfg.current_schema:
            scope = ask_choice(
                "What should we analyze?",
                [
                    "Use session defaults (/db /schema and optional /db /table)",
                    "Pick schema and table(s) interactively",
                ],
                default="Use session defaults (/db /schema and optional /db /table)",
            )
        else:
            scope = "Pick schema and table(s) interactively"
            info("No schema context yet — set /db /schema (and optional /db /table), or pick below.")

        if scope.startswith("Use session") and cfg.current_schema:
            schema = cfg.current_schema
            if cfg.current_table:
                tables = [cfg.current_table]
            else:
                available = db.list_tables(schema)
                tables = ask_multi_choice("Select table(s) to analyze", available)
        else:
            schemas = db.list_schemas()
            schema = ask_choice("Select schema to analyze", schemas)
            available = db.list_tables(schema)
            tables = ask_multi_choice("Select table(s) to analyze", available)
    else:
        if not schema:
            schemas = db.list_schemas()
            schema = ask_choice("Select schema to analyze", schemas)
        tables = list(table_args)
        if not tables:
            available = db.list_tables(schema)
            tables = ask_multi_choice("Select table(s) to analyze", available)
    return schema, tables


def _resolve_codebase_for_run(
    cfg: AMXConfig,
    db: object,
    schema: str,
    tables: list[str],
    code_profile: str | None,
    code_refresh: bool,
) -> object | None:
    """Load or build codebase report for /run and /run-apply (returns CodebaseReport or None)."""
    from amx.codebase.analyzer import analyze_codebase, merge_codebase_reports
    from amx.codebase.cache import invalidate_cache, load_cached_report, save_cached_report
    from amx.codebase.code_rag import delete_code_collection

    cp_name = (code_profile or "").strip() or None
    if cp_name:
        if cp_name not in cfg.code_profiles:
            error(f"Unknown codebase profile: {cp_name}")
            sys.exit(1)
        code_paths = [cfg.code_profiles[cp_name]]
        profile_nm = cp_name
    else:
        code_paths = cfg.effective_code_paths()
        profile_nm = (cfg.active_code_profile or "default").strip() or "default"

    if not code_paths:
        return None

    if code_refresh:
        delete_code_collection()
    column_names: list[str] = []
    seen_col: set[str] = set()
    with step_spinner(f"Collecting column names from {len(tables)} tables"):
        for t in tables:
            tp = db.profile_table(schema, t)
            for c in tp.columns:
                k = c.name.lower()
                if k in seen_col:
                    continue
                seen_col.add(k)
                column_names.append(c.name)
                if len(column_names) >= 400:
                    break
            if len(column_names) >= 400:
                break

    all_schema_tables = db.list_tables(schema)
    catalog = frozenset(x.lower() for x in all_schema_tables)

    info("Analyzing codebase references...")
    merged_report = None
    for cp in code_paths:
        if code_refresh:
            invalidate_cache(profile_nm, cp)
        try:
            cached = (
                None
                if code_refresh
                else load_cached_report(
                    profile_name=profile_nm,
                    source_path=cp,
                    schema=schema,
                    tables=tables,
                    column_names=column_names,
                    force_refresh=False,
                )
            )
            if cached is not None:
                rpt = cached
                info(f"Loaded cached codebase scan for {cp}")
            else:
                with step_spinner(f"Scanning codebase: {cp}"):
                    rpt = analyze_codebase(
                        cp,
                        all_schema_tables,
                        column_names=column_names,
                        known_catalog_tables=catalog,
                        index_semantic=True,
                    )
                info(
                    f"Found {sum(len(v) for v in rpt.references.values())} code references "
                    f"({sum(len(v) for v in rpt.external_mentions.values())} external-style)"
                )
                try:
                    save_cached_report(
                        profile_name=profile_nm,
                        source_path=cp,
                        schema=schema,
                        tables=tables,
                        column_names=column_names,
                        report=rpt,
                    )
                except Exception as exc:
                    warn(f"Could not save codebase cache: {exc}")
            merged_report = merge_codebase_reports(merged_report, rpt)
        except Exception as exc:
            warn(f"Codebase analysis failed for {cp}: {exc}")
    return merged_report


@analyze.command("run")
@click.argument("tables_pos", nargs=-1, metavar="[TABLE ...]")
@click.option("--schema", "-s", help="Schema to analyze.")
@click.option("--table", "-t", multiple=True, help="Specific table(s). Omit for interactive selection.")
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
        "'batch' = OpenAI Batch API (async, ~50 %% cheaper, OpenAI only)."
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
    """Run all agents to infer metadata for selected tables.

    Tables can be passed as positional arguments (e.g. /run vbrk vbrp) or via --table.
    Use --mode batch for the OpenAI Batch API (50 %% cheaper, results within 24 h).
    """
    from amx.agents.orchestrator import Orchestrator
    from amx.db.connector import DatabaseConnector
    from amx.docs.rag import RAGStore
    from amx.llm.provider import LLMProvider, BATCH_SUPPORTED_PROVIDERS

    token_tracker.reset()

    if not cfg.llm.provider or not cfg.llm.model:
        error("LLM not configured. Run `amx setup` first.")
        sys.exit(1)

    llm = LLMProvider(cfg.llm)
    db = DatabaseConnector(cfg.db)

    if not db.test_connection():
        error("Cannot connect to database.")
        sys.exit(1)

    if not apply:
        warn(
            "Without --apply, approved metadata is not written to the database. "
            "Use `/analyze` then `/apply`, or `/run-apply`, to persist comments."
        )

    # ── Mode selection ────────────────────────────────────────────────────────
    if mode is None:
        # Respect the config default, but also ask interactively if not set.
        cfg_mode = (cfg.llm.completion_mode or "chat_completions").lower()
        default_mode_label = "batch" if cfg_mode == "batch" else "chat"

        from amx.utils.console import ask_choice as _ask_choice
        mode_choices = ["chat", "batch"]
        _batch_note = " (OpenAI only)" if cfg.llm.provider not in BATCH_SUPPORTED_PROVIDERS else " (50 % cheaper, async)"
        mode = _ask_choice(
            "Select completion mode",
            mode_choices,
            default=default_mode_label,
            descriptions={
                "chat": "Chat Completions — real-time, live spinners, full price",
                "batch": f"Batch API{_batch_note} — submit all at once, results in minutes–hours",
            },
        )

    use_batch = mode == "batch"

    if use_batch and cfg.llm.provider not in BATCH_SUPPORTED_PROVIDERS:
        warn(
            f"Batch mode is only available for OpenAI (current provider: '{cfg.llm.provider}'). "
            "Falling back to Chat Completions."
        )
        use_batch = False

    if use_batch:
        from rich.panel import Panel
        from amx.utils.console import console as _console
        _console.print(Panel(
            "[bold]Batch API selected.[/bold]\n"
            "All LLM requests will be submitted as a single OpenAI Batch job.\n"
            "Typical turnaround: [bold]2–30 minutes[/bold]  |  Cost: [bold green]~50 % lower[/bold green]\n"
            "[dim]You will see live polling status below.[/dim]",
            title="[cyan]Mode: Batch[/cyan]", border_style="cyan",
        ))
    else:
        from amx.utils.console import info as _info
        _info("Mode: [bold]Chat Completions[/bold] (real-time)")

    # ── Table scope ───────────────────────────────────────────────────────────
    tables_arg = list(tables_pos) + list(table)
    ft = _finalize_table_scope(cfg, db, schema, tables_arg)
    if ft is None:
        return
    schema, tables = ft

    rag_store = None
    try:
        store = RAGStore()
        if store.doc_count > 0:
            rag_store = store
            info(f"RAG store has {store.doc_count} chunks available")
    except Exception:
        pass

    code_report = _resolve_codebase_for_run(cfg, db, schema, tables, code_profile, code_refresh)

    orch = Orchestrator(db, llm, rag_store=rag_store, code_report=code_report)

    if use_batch:
        all_results = orch.process_tables_batch_mode(schema, list(tables))
    else:
        all_results = []
        for t in tables:
            results = orch.process_table(schema, t)
            all_results.extend(results)

    heading("Summary")
    render_token_summary(token_tracker)
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


@analyze.command("apply")
@click.pass_obj
def analyze_apply(cfg: AMXConfig) -> None:
    """Write pending approved descriptions to the database (COMMENT ON TABLE/COLUMN)."""
    from amx.agents.orchestrator import apply_review_results_to_db
    from amx.db.connector import DatabaseConnector
    from amx.pending_review import clear_pending, load_pending

    pending = load_pending()
    if not pending:
        error(
            "No pending metadata. Run `/analyze` then `/run`, approve descriptions, "
            "and finish without `--apply` first."
        )
        return

    heading("Apply pending metadata to the database")
    render_table(
        "Pending comments",
        ["Asset", "Description"],
        [
            [
                f"{r.table}.{r.column}" if r.column else r.table,
                (r.final_description or "")[:72],
            ]
            for r in pending
        ],
    )
    if not confirm(f"Write {len(pending)} comment(s) to the database?", default=True):
        info("Cancelled — pending file unchanged.")
        return

    db = DatabaseConnector(cfg.db)
    if not db.test_connection():
        error("Cannot connect to database.")
        sys.exit(1)

    n = apply_review_results_to_db(db, pending)
    clear_pending()
    success(f"Applied {n} comment(s). Pending file cleared.")


# ── Config Commands ─────────────────────────────────────────────────────────


@main.command("config")
@click.pass_obj
def show_config(cfg: AMXConfig) -> None:
    """Display current configuration."""
    info(
        f"Active DB profile: {cfg.active_db_profile} → "
        f"{cfg.db.user}@{cfg.db.host}:{cfg.db.port}/{cfg.db.database}"
    )
    if cfg.db_profiles:
        names = ", ".join(sorted(cfg.db_profiles.keys()))
        info(f"DB profiles: {names}")
    info(f"Session context: schema={cfg.current_schema or '-'} table={cfg.current_table or '-'}")
    info(
        f"Active LLM profile: {cfg.active_llm_profile} → "
        f"{cfg.llm.provider}/{cfg.llm.model}"
    )
    if cfg.llm_profiles:
        info("LLM profiles: " + ", ".join(sorted(cfg.llm_profiles.keys())))
    info(f"Active document profile: {cfg.active_doc_profile or '-'}")
    info(f"Document paths (active): {cfg.effective_doc_paths() or 'none'}")
    info(f"Active codebase profile: {cfg.active_code_profile or '-'}")
    info(f"Codebase paths (active): {cfg.effective_code_paths() or 'none'}")
    info(f"Selected schemas: {cfg.selected_schemas or 'all'}")


if __name__ == "__main__":
    run_cli()
