"""AMX CLI — Agentic Metadata Extractor command-line interface."""

from __future__ import annotations

import os
import sys

import click

from amx import __version__
from amx.config import AMXConfig
from amx.cli_support.commands.analyze_flow import register_analyze_run_command
from amx.cli_support.commands.code import register_code_commands
from amx.cli_support.commands.db import (
    interactive_db_block as _interactive_db_block,
    print_db_namespace_hint as _print_db_namespace_hint,
)
from amx.cli_support.commands.docs import register_docs_commands
from amx.cli_support.commands.history import register_history_commands
from amx.cli_support.commands.manual import register_manual_commands
from amx.cli_support.commands.search import register_search_commands
from amx.cli_support.commands.profiles import (
    interactive_llm_block as _interactive_llm_block,
    warn_no_doc_paths_for_scan_or_ingest as _warn_no_doc_paths_for_scan_or_ingest,
)
from amx.cli_support.commands.run import (
    _finalize_scope,
    _resolve_codebase_for_run,
    register_analyze_commands,
)
from amx.cli_support import run_interactive_session
from amx.cli_support.root_commands import register_root_commands
from amx.utils.console import (
    error,
    info,
    show_banner,
    warn,
)
from amx.utils.logging import get_logger
from amx.storage.sqlite_store import history_store, init_history_store

log = get_logger("cli")

pass_config = click.make_pass_decorator(AMXConfig, ensure=True)


def _print_interactive_startup_summary(cfg: AMXConfig) -> None:
    """Show a concise startup summary, with first-run guidance when needed."""
    info(f"Version {__version__}")
    # Surface the on-disk config path so users can verify what is actually
    # being read at startup. The ghost-profile bug class is hard to diagnose
    # without knowing exactly which file the running session is talking to.
    info(f"Config: {cfg.config_path}")

    if cfg.is_first_run:
        warn(
            "First run detected — no profiles configured yet. "
            "Run /setup to connect a database and an LLM."
        )

    if not cfg.active_db_profile or not cfg.db_profiles:
        info("Database: (not configured — run /setup or /add-db-profile)")
    elif not cfg.db.is_configured():
        info(
            f"Database: profile '{cfg.active_db_profile}' (incomplete — "
            f"run /add-db-profile to fill in the connection)"
        )
    else:
        info(
            f"Database: profile '{cfg.active_db_profile}' → "
            f"[{cfg.db.backend}] {cfg.db.display_summary}"
        )

    if not cfg.active_llm_profile or not cfg.llm_profiles or not cfg.llm.is_configured():
        info("LLM: (not configured — run /setup or /add-llm-profile)")
    else:
        llm_line = (
            f"{cfg.llm.provider}/{cfg.llm.model} [{cfg.llm.language or 'english'}]"
        )
        info(f"LLM: profile '{cfg.active_llm_profile}' → {llm_line} (metadata language)")

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


def _install_embedding_provider(cfg: AMXConfig) -> None:
    """Configure the search-index embedding provider for this process.

    Thin shim around :func:`amx.search.embeddings.configure_from_amx_config`
    that wires the themed ``warn()`` console helper into the
    ``on_warning`` hook so misconfigured providers surface as a themed
    one-line warning rather than a stack trace.
    """
    from amx.search.embeddings import configure_from_amx_config

    configure_from_amx_config(cfg, on_warning=warn)


def _rewrite_sys_argv_for_codebase(argv: list[str]) -> None:
    """In-place fix for `amx code scan …` when launched from a real shell."""
    for i in range(len(argv) - 2):
        if argv[i] == "code" and argv[i + 1] == "scan":
            head = argv[: i + 3]
            tail = argv[i + 3 :]
            argv[:] = head + _fix_codebase_cli_tail(tail)
            return


def run_cli() -> None:
    """Entry point for the `amx` console script (argv normalization + Click).

    Wraps ``main()`` with a top-level crash handler so unhandled exceptions are
    rendered as a themed error line instead of a raw traceback. Set
    ``AMX_DEBUG=1`` (or pass ``--debug`` to ``amx``) to see the full traceback.
    """
    if len(sys.argv) >= 4:
        _rewrite_sys_argv_for_codebase(sys.argv)
    try:
        main()
    except (click.ClickException, SystemExit):
        raise
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception as exc:  # noqa: BLE001 — last-resort crash handler
        if os.getenv("AMX_DEBUG", "").lower() in {"1", "true", "yes"}:
            raise
        error(f"AMX crashed: {exc.__class__.__name__}: {exc}")
        # Persist a redacted crash report so the user can attach it to
        # an issue without leaking their DB password or API key. The
        # request id (if one is active) ties the crash report back to
        # the structured log lines for the same run.
        try:
            from amx.utils.crash import write_crash_report
            from amx.utils.logging import get_request_id

            crash_path = write_crash_report(exc, request_id=get_request_id())
            info(f"Sanitized crash report saved to {crash_path}")
        except Exception:
            # Crash-report writing must not itself crash the handler.
            pass
        info(
            "Run with --debug (or set AMX_DEBUG=1) to see the full traceback. "
            "Detailed logs: ~/.amx/logs/amx.log"
        )
        log.exception("Unhandled exception in CLI")
        sys.exit(1)


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


@click.group(invoke_without_command=True)
@click.version_option(__version__, prog_name="amx")
@click.option("--config", "cfg_path", default=None, help="Path to config YAML file.")
@click.option(
    "--debug/--no-debug",
    default=False,
    envvar="AMX_DEBUG",
    help="Show full tracebacks on errors and verbose internal logs.",
)
@click.pass_context
def main(ctx: click.Context, cfg_path: str | None, debug: bool) -> None:
    """AMX — Agentic Metadata Extractor.

    AI-powered CLI to infer, review, and apply database metadata
    using database profiling, document RAG, and codebase analysis.
    """
    if debug:
        os.environ["AMX_DEBUG"] = "1"
    ctx.ensure_object(dict)
    ctx.obj = AMXConfig.load(cfg_path)
    init_history_store(ctx.obj.CONFIG_DIR)
    _install_embedding_provider(ctx.obj)
    is_session_child = os.getenv("AMX_SESSION_CHILD") == "1"
    if not is_session_child:
        show_banner()
    if ctx.invoked_subcommand is None:
        run_interactive_session(
            cfg=ctx.obj,
            version=__version__,
            main_command=main,
            normalize_click_argv=_normalize_click_argv,
            warn_no_doc_paths_for_scan_or_ingest=_warn_no_doc_paths_for_scan_or_ingest,
            print_interactive_startup_summary=_print_interactive_startup_summary,
            print_db_namespace_hint=_print_db_namespace_hint,
            log_event=_log_app_event,
            show_banner=show_banner,
        )
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
register_search_commands(main, pass_config=pass_config, log_event=_log_app_event)
register_manual_commands(main, pass_config=pass_config, log_event=_log_app_event)
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
register_root_commands(
    main,
    interactive_db_block=_interactive_db_block,
    interactive_llm_block=_interactive_llm_block,
)


if __name__ == "__main__":
    run_cli()
