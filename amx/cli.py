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
        f"(set with /schema, /table)"
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
    if os.getenv("AMX_SESSION_CHILD") != "1":
        show_banner()
    if ctx.invoked_subcommand is None:
        _interactive_session(cfg=ctx.obj)


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
    info("Namespaces: /db, /docs, /analyze (use /back or Esc to return).")
    info("Tip: start typing / and use ↑/↓ to pick a command.")
    namespace = ""

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
            if cmdline in {"db", "docs", "analyze"}:
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
  2) /profiles                     List DB connection profiles
  3) /use <name>                   Switch active DB profile
  4) /add-profile <name>           Create/update a profile (interactive)
  5) /remove-profile <name>        Remove a profile (cannot remove last)
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
  2) /scan [paths...]              Scan documents (preview)
  3) /ingest [paths...]            Ingest documents into RAG store
  4) /query <question>             Query RAG store

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
  3) /codebase <path> [--schema …] Scan codebase (schema defaults to /schema context)
     Tip: `/schema sap_s6p` then `/codebase <url>` — or use `--schema sap_s6p`
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
  3) /db                           Enter database commands (/connect, /schemas, …)
  4) /docs                         Enter document/RAG commands
  5) /analyze                      Enter analysis commands

DB profiles (works anywhere):
  /profiles                        List DB profiles
  /use <name>                      Switch active DB profile
  /add-profile <name>              Add/update DB profile (interactive)
  /remove-profile <name>           Remove DB profile
  /save                            Persist ~/.amx/config.yml

LLM profiles:
  /llm-profiles                    List named LLM configs
  /use-llm <name>                  Switch active LLM profile
  /add-llm-profile <name>          Add/update LLM profile (interactive)
  /remove-llm-profile <name>       Remove LLM profile

Document sources (named lists of paths):
  /doc-profiles                    List document profiles
  /use-doc <name>                  Switch active document profile
  /add-doc-profile <name>          Add/update paths (interactive)
  /remove-doc-profile <name>       Remove document profile

Codebases (named repo / path):
  /code-profiles                   List codebase profiles
  /use-code <name>                 Switch active codebase profile
  /add-code-profile <name>       Add/update one path (local or Git URL)
  /remove-code-profile <name>     Remove codebase profile

Context helpers:
  /schema <name>                   Remember schema for defaults
  /table <name>                    Remember table for defaults

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
        ("/analyze", "Enter /analyze namespace"),
        ("/profiles", "List DB profiles"),
        ("/use", "Switch DB profile (/use <name>)"),
        ("/add-profile", "Create/update DB profile (/add-profile <name>)"),
        ("/remove-profile", "Remove DB profile (/remove-profile <name>)"),
        ("/save", "Save config to disk"),
        ("/schema", "Set current schema (/schema <name>)"),
        ("/table", "Set current table (/table <name>)"),
        ("/llm-profiles", "List LLM profiles"),
        ("/use-llm", "Switch LLM profile (/use-llm <name>)"),
        ("/add-llm-profile", "Add/update LLM profile"),
        ("/remove-llm-profile", "Remove LLM profile"),
        ("/doc-profiles", "List document profiles"),
        ("/use-doc", "Switch document profile (/use-doc <name>)"),
        ("/add-doc-profile", "Add/update document profile"),
        ("/remove-doc-profile", "Remove document profile"),
        ("/code-profiles", "List codebase profiles"),
        ("/use-code", "Switch codebase profile (/use-code <name>)"),
        ("/add-code-profile", "Add/update codebase profile"),
        ("/remove-code-profile", "Remove codebase profile"),
    ]

    db_cmds: list[tuple[str, str]] = [
        ("/back", "Return to root namespace"),
        ("/profiles", "List DB profiles"),
        ("/use", "Switch DB profile (/use <name>)"),
        ("/add-profile", "Create/update DB profile (/add-profile <name>)"),
        ("/remove-profile", "Remove DB profile (/remove-profile <name>)"),
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
        ("/scan", "Scan documents (/scan [paths...])"),
        ("/ingest", "Ingest documents (/ingest [paths...])"),
        ("/query", "Query RAG (/query <question>)"),
    ]

    analyze_cmds: list[tuple[str, str]] = [
        ("/back", "Return to root namespace"),
        ("/run", "Run agents (/run [--schema …] [--table …] [--apply])"),
        ("/codebase", "Scan codebase (/codebase <path> --schema …)"),
    ]

    if namespace == "db":
        return db_cmds
    if namespace == "docs":
        return docs_cmds
    if namespace == "analyze":
        return analyze_cmds
    return root


def _handle_session_builtin(cfg: AMXConfig, namespace: str, parts: list[str]) -> bool | str:
    head = parts[0]

    if head == "llm-profiles":
        _cmd_llm_profiles(cfg)
        return True
    if head == "use-llm":
        _cmd_use_llm(cfg, parts[1:])
        return True
    if head == "add-llm-profile":
        _cmd_add_llm_profile(cfg, parts[1:])
        return True
    if head == "remove-llm-profile":
        _cmd_remove_llm_profile(cfg, parts[1:])
        return True
    if head == "doc-profiles":
        _cmd_doc_profiles(cfg)
        return True
    if head == "use-doc":
        _cmd_use_doc(cfg, parts[1:])
        return True
    if head == "add-doc-profile":
        _cmd_add_doc_profile(cfg, parts[1:])
        return True
    if head == "remove-doc-profile":
        _cmd_remove_doc_profile(cfg, parts[1:])
        return True
    if head == "code-profiles":
        _cmd_code_profiles(cfg)
        return True
    if head == "use-code":
        _cmd_use_code(cfg, parts[1:])
        return True
    if head == "add-code-profile":
        _cmd_add_code_profile(cfg, parts[1:])
        return True
    if head == "remove-code-profile":
        _cmd_remove_code_profile(cfg, parts[1:])
        return True

    if head == "profiles":
        _cmd_profiles(cfg)
        return True
    if head == "use":
        _cmd_use(cfg, parts[1:])
        return True
    if head == "add-profile":
        _cmd_add_profile(cfg, parts[1:])
        return True
    if head == "remove-profile":
        _cmd_remove_profile(cfg, parts[1:])
        return True
    if head == "save":
        path = cfg.save()
        success(f"Saved configuration to {path}")
        return True
    if head == "schema":
        if len(parts) < 2:
            error("Usage: /schema <name>")
            return True
        cfg.current_schema = parts[1]
        cfg.save()
        info(f"Current schema set to: {cfg.current_schema}")
        return True
    if head == "table":
        if len(parts) < 2:
            error("Usage: /table <name>")
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
        error("Usage: /remove-profile <name>")
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
    paths: list[str] = []
    info("Enter document roots (local dir, s3://, GitHub URL). Empty line to finish.")
    while True:
        p = ask("Path", default="")
        if not p:
            break
        paths.append(p)
    if not paths:
        error("No paths added.")
        return
    cfg.upsert_doc_profile(name, paths)
    if not cfg.active_doc_profile or confirm(f"Switch active document profile to {name}?", default=True):
        cfg.active_doc_profile = name
    cfg.save()
    success(f"Document profile saved: {name} ({len(paths)} path(s))")


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
        "query": ["docs", "query"],
        "run": ["analyze", "run"],
        "codebase": ["analyze", "codebase"],
        "setup": ["setup"],
        "config": ["config"],
        "help": ["--help"],
    }

    if head in {"db", "docs", "analyze", "setup", "config"}:
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
    cfg.db.host = ask("PostgreSQL host", cfg.db.host)
    cfg.db.port = int(ask("Port", str(cfg.db.port)))
    cfg.db.user = ask("Username", cfg.db.user)
    cfg.db.password = ask_password("Password") or cfg.db.password
    cfg.db.database = ask("Database name", cfg.db.database)

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
        name = ask("Profile name", default="default")
        paths: list[str] = []
        while True:
            p = ask("Document path (empty to finish)", default="")
            if not p:
                break
            paths.append(p)
        if paths:
            cfg.upsert_doc_profile(name, paths)
            cfg.active_doc_profile = name

    if confirm("Add a codebase profile?", default=False):
        name = ask("Profile name", default="default")
        p = ask("Codebase path (local dir or Git URL)", default="")
        if p:
            cfg.upsert_code_profile(name, p)
            cfg.active_code_profile = name

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
        error("No document paths configured. Use /add-doc-profile, `amx setup`, or pass paths.")
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
        error("No document paths configured. Use /add-doc-profile, `amx setup`, or pass paths.")
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

    tables_arg = list(table)

    # Scope: session defaults vs interactive (when /run with no --schema/--table)
    if schema is None and not tables_arg:
        if cfg.current_schema:
            scope = ask_choice(
                "What should we analyze?",
                [
                    "Use session defaults (/schema and optional /table)",
                    "Pick schema and table(s) interactively",
                ],
                default="Use session defaults (/schema and optional /table)",
            )
        else:
            scope = "Pick schema and table(s) interactively"
            info("No /schema in session yet — choose assets below.")

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

    if apply and approved:
        if confirm("Apply these metadata comments to the database?"):
            orch.apply_results(approved)
    elif approved:
        info("Run with --apply flag or use `amx apply` to write metadata to the database.")


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
        error("Missing schema: use --schema sap_s6p or set context with /schema … in session.")
        sys.exit(1)

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
