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
    show_banner,
    success,
    warn,
)
from amx.utils.logging import LAST_PROFILE_RESPONSE_FILE, LOG_DIR, get_logger

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
        "Approved descriptions are written to PostgreSQL as COMMENT ON TABLE / COLUMN "
        "(stored in pg_catalog.pg_description)."
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
    if len(args) >= 3 and args[0] == "analyze" and args[1] == "codebase":
        return ["analyze", "codebase", args[2]] + _fix_codebase_cli_tail(args[3:])
    return args


def _rewrite_sys_argv_for_codebase(argv: list[str]) -> None:
    """In-place fix for `amx analyze codebase …` when launched from a real shell."""
    for i in range(len(argv) - 2):
        if argv[i] == "analyze" and argv[i + 1] == "codebase":
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
            "similarity",
            "query",
        }
    )
    _llm_cmd_heads = frozenset(
        {"llm-profiles", "use-llm", "add-llm-profile", "remove-llm-profile"}
    )
    _code_cmd_heads = frozenset(
        {"code-profiles", "use-code", "add-code-profile", "remove-code-profile"}
    )
    _analyze_cmd_heads = frozenset({"run", "run-apply", "apply", "codebase"})

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
                if parts[0] in {"search-docs", "similarity", "query"} and len(parts) == 1:
                    error("Usage: /search-docs <text>  (alias: /similarity; legacy: /query)")
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
  6) /scan [paths...]              Scan documents (preview); uses active profile paths if omitted
  7) /ingest [paths...]            Ingest into RAG store; uses active profile paths if omitted
  8) /search-docs <text>           Vector similarity over ingested docs (Chroma; no LLM answer)
     /similarity <text>           Same as /search-docs
     /query <text>                Deprecated alias of /search-docs

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
Commands (in order):
  1) /back                         Return to root namespace
  2) /code-profiles                List codebase profiles
  3) /use-code <name>              Switch active codebase profile
  4) /add-code-profile [name]      Add/update a codebase path (interactive)
  5) /remove-code-profile <name>   Remove a codebase profile

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
  2) /run [--schema …] [--table …] [--apply]   Run agents (/run alone asks: session defaults vs pick assets)
  3) /run-apply                    Run analysis and apply approved COMMENTs in one step
  4) /apply                        Write pending approved COMMENTs to PostgreSQL
  5) /codebase <path> [--schema …] Scan a local dir or Git repo for table-name matches (path = folder/URL, not a /code profile name)
     Tip: `/db` then `/schema sap_s6p`, then `/codebase /path/to/app` or `/codebase https://github.com/org/repo`
     Shorthand: `--sap_s6p` is accepted as `--schema sap_s6p`

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
  [bright_white]/codebase https://github.com/org/repo --schema sap_s6p[/bright_white]
  [bright_white]/codebase https://github.com/org/repo --sap_s6p[/bright_white]  (same as --schema)
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
        ("/scan", "Scan documents (/scan [paths...])"),
        ("/ingest", "Ingest documents (/ingest [paths...])"),
        ("/search-docs", "Similarity search (/search-docs <text>, no LLM)"),
        ("/similarity", "Alias of /search-docs"),
        ("/query", "Deprecated: same as /search-docs"),
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
    ]

    analyze_cmds: list[tuple[str, str]] = [
        ("/back", "Return to root namespace"),
        ("/run", "Run agents (/run [--schema …] [--table …] [--apply])"),
        ("/run-apply", "Run agents and apply approved COMMENTs"),
        ("/apply", "Write pending COMMENTs to PostgreSQL"),
        ("/codebase", "Scan codebase (/codebase <path> --schema …)"),
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
    host = ask("PostgreSQL host", cfg.db.host)
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

    paths: list[str] = []
    info(
        "Enter document roots (local dir, s3://, GitHub URL, Google Drive, SharePoint/OneDrive). "
        "Each path is checked for reachability only (no full scan)."
    )
    while True:
        p = ask("Path (empty to finish)" if paths else "Path", default="")
        if not p:
            if paths:
                break
            error("No paths added.")
            return
        try:
            test_source_reachable(p)
            success(f"Source reachable: {p}")
            paths.append(p)
        except Exception as exc:
            error(f"Source not reachable: {p}")
            warn(str(exc))
        if not confirm("Add another path?", default=False):
            break
    if not paths:
        error("No valid document sources to save.")
        return
    cfg.upsert_doc_profile(name, paths)
    if not cfg.active_doc_profile or confirm(f"Switch active document profile to {name}?", default=True):
        cfg.active_doc_profile = name
    cfg.save()
    success(f"Document profile saved: {name} ({len(paths)} path(s))")



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
        "similarity": ["docs", "similarity"],
        "query": ["docs", "query"],
        "run": ["analyze", "run"],
        "run-apply": ["analyze", "run", "--apply"],
        "apply": ["analyze", "apply"],
        "codebase": ["analyze", "codebase"],
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
        len(args) >= 3
        and args[0] == "analyze"
        and args[1] == "codebase"
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
    host = ask("PostgreSQL host (e.g. localhost)")
    while not host:
        warn("Host is required.")
        host = ask("PostgreSQL host (e.g. localhost)")
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
        paths: list[str] = []
        while True:
            p = ask("Document path" if not paths else "Another path (empty to finish)", default="")
            if not p:
                break
            try:
                test_source_reachable(p)
                success(f"Source reachable: {p}")
                paths.append(p)
            except Exception as exc:
                error(f"Source not reachable: {p}")
                warn(str(exc))
            if not confirm("Add another path?", default=False):
                break
        if paths:
            cfg.upsert_doc_profile(name, paths)
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
@click.pass_obj
def docs_scan(cfg: AMXConfig, paths: tuple[str, ...]) -> None:
    """Scan document sources and show what would be ingested."""
    from amx.docs.scanner import scan_all_sources, total_size_mb

    all_paths = list(paths) or cfg.effective_doc_paths()
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
        chunks = store.ingest(documents)
        success(f"Ingested {chunks} chunks from {len(documents)} documents")


@docs.command("ingest")
@click.argument("paths", nargs=-1)
@click.pass_obj
def docs_ingest(cfg: AMXConfig, paths: tuple[str, ...]) -> None:
    """Ingest documents directly into the RAG store."""
    from amx.docs.rag import RAGStore
    from amx.docs.scanner import scan_all_sources, total_size_mb

    all_paths = list(paths) or cfg.effective_doc_paths()
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
    chunks = store.ingest(documents)
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


@docs.command("similarity")
@click.argument("question")
@click.option("-n", "--results", default=5, help="Number of results.")
def docs_similarity(question: str, results: int) -> None:
    """Alias of search-docs: embedding similarity over the RAG index (no LLM)."""
    _run_docs_semantic_search(question, results)


@docs.command("query", hidden=True)
@click.argument("question")
@click.option("-n", "--results", default=5, help="Number of results.")
def docs_query(question: str, results: int) -> None:
    """Deprecated name for search-docs."""
    warn("/query is deprecated — use /search-docs or /similarity (vector similarity only, no LLM).")
    _run_docs_semantic_search(question, results)


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

    if not apply:
        warn(
            "Without --apply, approved metadata is not written to the database. "
            "Use `/analyze` then `/apply`, or `/run-apply`, to persist COMMENTs."
        )

    tables_arg = list(table)

    # Scope: session defaults vs interactive (when /run with no --schema/--table)
    if schema is None and not tables_arg:
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
        tables = list(tables_arg)
        if not tables:
            available = db.list_tables(schema)
            tables = ask_multi_choice("Select table(s) to analyze", available)

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
    code_paths = cfg.effective_code_paths()
    if code_paths:
        info("Analyzing codebase references...")
        all_table_names = tables
        for cp in code_paths:
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

    if approved:
        from amx.pending_review import save_pending

        save_pending(approved)
        if not apply:
            info(
                f"Saved {len(approved)} approved description(s) as pending. "
                "Run `/analyze` then `/apply` (or `/run-apply` next time) to write COMMENTs to PostgreSQL."
            )

    if apply and approved:
        if confirm("Apply these metadata comments to the database?"):
            from amx.pending_review import clear_pending

            orch.apply_results(approved)
            clear_pending()


@analyze.command("apply")
@click.pass_obj
def analyze_apply(cfg: AMXConfig) -> None:
    """Write last pending approved descriptions to PostgreSQL (COMMENT ON TABLE/COLUMN)."""
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
        "Pending COMMENTs",
        ["Asset", "Description"],
        [
            [
                f"{r.table}.{r.column}" if r.column else r.table,
                (r.final_description or "")[:72],
            ]
            for r in pending
        ],
    )
    if not confirm(f"Write {len(pending)} COMMENT(s) to PostgreSQL?", default=True):
        info("Cancelled — pending file unchanged.")
        return

    db = DatabaseConnector(cfg.db)
    if not db.test_connection():
        error("Cannot connect to database.")
        sys.exit(1)

    n = apply_review_results_to_db(db, pending)
    clear_pending()
    success(f"Applied {n} COMMENT(s). Pending file cleared.")


@analyze.command("codebase")
@click.argument("path")
@click.option(
    "--schema",
    "-s",
    default=None,
    help="Schema to match against (defaults to session current_schema from config).",
)
@click.pass_obj
def analyze_codebase_cmd(cfg: AMXConfig, path: str, schema: str | None) -> None:
    """Analyze a codebase for database asset references."""
    from amx.codebase.analyzer import analyze_codebase
    from amx.db.connector import DatabaseConnector

    schema = schema or cfg.current_schema
    if not schema:
        error(
            "Missing schema: use --schema sap_s6p or set context with `/db` then `/schema …` in session."
        )
        sys.exit(1)

    db = DatabaseConnector(cfg.db)
    tables = db.list_tables(schema)

    info(f"Scanning {path} for references to {len(tables)} tables...")
    try:
        report = analyze_codebase(path, tables)
    except Exception as exc:
        error(str(exc))
        sys.exit(1)

    info(f"Scanned {report.scanned_files}/{report.total_files} files")
    if report.total_files == 0:
        warn(
            "No source files matched (.py, .sql, .java, .ts, …). "
            "Check the path is a folder/repo root with code under it — `/codebase` does not take a profile name."
        )
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
