"""AMX CLI — Agentic Metadata Extractor command-line interface."""

from __future__ import annotations

import os
import sys

import click

from amx import __version__
from amx.cli_support import run_interactive_session
from amx.cli_support.commands.analyze_flow import (
    register_analyze_review_command,
    register_analyze_run_command,
)
from amx.cli_support.commands.chat_session import register_chat_session_commands
from amx.cli_support.commands.code import register_code_commands
from amx.cli_support.commands.compare import register_compare_command
from amx.cli_support.commands.db import (
    interactive_db_block as _interactive_db_block,
)
from amx.cli_support.commands.db import (
    print_db_namespace_hint as _print_db_namespace_hint,
)
from amx.cli_support.commands.docs import register_docs_commands
from amx.cli_support.commands.doctor import register_doctor_command
from amx.cli_support.commands.eval_confidence import register_eval_confidence_command
from amx.cli_support.commands.history import register_history_commands
from amx.cli_support.commands.history_store import register_history_store_commands
from amx.cli_support.commands.manual import register_manual_commands
from amx.cli_support.commands.profiles import (
    interactive_llm_block as _interactive_llm_block,
)
from amx.cli_support.commands.profiles import (
    warn_no_doc_paths_for_scan_or_ingest as _warn_no_doc_paths_for_scan_or_ingest,
)
from amx.cli_support.commands.rerun import register_rerun_command
from amx.cli_support.commands.run import (
    _finalize_scope,
    _resolve_codebase_for_run,
    register_analyze_commands,
)
from amx.cli_support.commands.schedule import register_schedule_commands
from amx.cli_support.commands.search import register_search_commands
from amx.cli_support.commands.variations import register_variations_command
from amx.cli_support.root_commands import register_root_commands
from amx.config import AMXConfig, ConfigSchemaTooNewError
from amx.storage.factory import init_history_store
from amx.storage.sqlite_store import history_store
from amx.utils.console import (
    error,
    info,
    show_banner,
    warn,
)
from amx.utils.logging import get_logger

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
        # 0.11.0 multi-pick scope: when the user has opted in via
        # ``/use-db a b c`` show the full scope so they don't lose
        # track of which DBs /ask, /run, /sync will run against.
        scope = cfg.effective_db_profiles()
        if len(scope) > 1:
            others = [n for n in scope if n != cfg.active_db_profile]
            info(
                f"  Active scope ({len(scope)} profiles): "
                f"{', '.join(scope)}. Default = '{cfg.active_db_profile}'. "
                f"Multi-DB execution is on for /ask, /run, /sync."
            )
            del others  # noqa: F841 — informational only
        # 0.11.0: database is optional per profile. When the active profile
        # has no DB pinned, give the user a one-line nudge so they know
        # they'll be prompted at command time (catalog picker, etc.).
        if not cfg.db.is_database_pinned():
            info(
                "  No database pinned — you'll be prompted to pick one when "
                "running /run, /sync, or /ask."
            )

    if not cfg.active_llm_profile or not cfg.llm_profiles or not cfg.llm.is_configured():
        info("LLM: (not configured — run /setup or /add-llm-profile)")
    else:
        llm_line = f"{cfg.llm.provider}/{cfg.llm.model} [{cfg.llm.language or 'english'}]"
        info(f"LLM: profile '{cfg.active_llm_profile}' → {llm_line} (metadata language)")

    if cfg.current_schema or cfg.current_table:
        info(f"Context: schema={cfg.current_schema or '—'} · table={cfg.current_table or '—'}")


def _fix_codebase_cli_tail(tokens: list[str]) -> list[str]:
    """Turn mistaken flags like `--sap_s6p` into `--schema sap_s6p` for `analyze codebase`."""
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

    Default-kind shortcut: when neither ``cfg.embedding_docs`` nor
    ``cfg.embedding_code`` has been customised away from MiniLM, skip
    importing ``amx.search.embeddings`` entirely. That module runs a
    module-level ``_ensure("rag")`` which on a fresh install triggers a
    chromadb pip download — and the default-MiniLM path does not need a
    custom factory anyway (``SearchIndex.__init__`` falls back to
    Chroma's bundled MiniLM when no factory is installed). So a user
    whose only intent on ``amx`` open is ``/db``, ``/llm``,
    ``/metadata``, ``/ask`` etc. never has to pay a chromadb install at
    REPL bootstrap. Custom-embedding users still pay it here, but they
    are by definition heading for a RAG flow that would have triggered
    the install on first use regardless.
    """
    default_aliases = {"", "minilm", "default", "minilm-l6-v2"}
    for side in ("embedding_docs", "embedding_code"):
        embedding = getattr(cfg, side, None)
        if embedding is None:
            continue
        kind = (getattr(embedding, "kind", "") or "").lower().strip()
        if kind not in default_aliases:
            break
    else:
        return

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
    # Route every later ``ssl.create_default_context()`` through the
    # OS trust store BEFORE any HTTPS-touching import fires (pricing
    # fetcher, scanner, litellm bootstrap). One-shot, idempotent.
    from amx.utils.network_trust import configure_trust_store

    configure_trust_store()

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


def _bootstrap_scheduler_tick() -> None:
    """Run one scheduler.tick(source='bootstrap') at every CLI invocation.

    Surfaces missed schedules and recovers stale runs without firing
    anything (the user-warning contract: when AMX was closed, the
    next session sees the list and decides). Honours
    ``AMX_SKIP_BOOTSTRAP_TICK=1`` for CI / scripted invocations and
    for the daemon's own ``amx scheduler tick`` re-entry. Failures are
    swallowed -- a broken tick must never block the CLI.
    """
    if os.getenv("AMX_SKIP_BOOTSTRAP_TICK", "").lower() in {"1", "true", "yes"}:
        return
    try:
        from amx.scheduler.tick import tick as _tick
        from amx.storage.sqlite_store import history_store as _hs_singleton

        hs = _hs_singleton()
        if hs is None:
            return
        report = _tick(store=hs, source="bootstrap")
        # Print a concise banner only when there's something to surface.
        n_missed = len(report.missed_for_review)
        n_stale = len(report.stale_recovered)
        if n_missed or n_stale:
            import click as _click

            parts = []
            if n_stale:
                parts.append(
                    f"{n_stale} interrupted run{'s' if n_stale != 1 else ''} recovered (marked failed)"
                )
            if n_missed:
                parts.append(
                    f"{n_missed} schedule{'s' if n_missed != 1 else ''} missed while AMX was closed"
                )
            _click.echo(
                "⚠️  " + "; ".join(parts) + ". Run `amx schedule list` to review.",
                err=True,
            )
    except Exception:  # noqa: BLE001 - bootstrap tick must never crash the CLI
        log.warning("bootstrap scheduler tick failed", exc_info=True)


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


def _raise_open_file_limit(target: int = 4096) -> None:
    """Lift the per-process NOFILE soft limit on macOS / Linux.

    AMX opens many file descriptors under heavy use: SQLAlchemy connection
    pools (one per ``DatabaseConnector`` instance — up to a few hundred
    columns × N tables), a SQLite history store, a Chroma vector index, the
    LiteLLM HTTP client pool, and prompt_toolkit's asyncio event loop.
    macOS' default soft limit is 256, which a long ``/ask`` REPL session can
    exhaust — at which point ``prompt_toolkit.prompt`` crashes inside
    ``asyncio.new_event_loop`` with ``OSError: [Errno 24] Too many open
    files``. Open-source users shouldn't have to set ``ulimit -n`` manually,
    so we lift the limit programmatically at startup. We never reduce the
    limit, never exceed the hard limit, and silently no-op on platforms
    that don't expose ``resource`` (e.g. native Windows).
    """
    try:
        import resource  # type: ignore[import-not-found]
    except ImportError:
        return
    try:
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    except (ValueError, OSError):
        return
    desired = max(soft, target)
    if hard != resource.RLIM_INFINITY:
        desired = min(desired, hard)
    if desired <= soft:
        return
    try:
        resource.setrlimit(resource.RLIMIT_NOFILE, (desired, hard))
    except (ValueError, OSError):
        # Some sandboxed environments forbid raising the limit; not fatal.
        return


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
    # Lift NOFILE before doing anything that might open a file descriptor.
    _raise_open_file_limit()
    ctx.ensure_object(dict)
    try:
        ctx.obj = AMXConfig.load(cfg_path)
    except ConfigSchemaTooNewError as exc:
        # Render an actionable message instead of a stack trace. This is
        # the first line of defence against the version-skew bug class
        # where a newer AMX wrote keys an older AMX would otherwise
        # silently drop on the next save (ghost-profile incident).
        error(str(exc))
        info(
            "Run `pip install --upgrade amx-cli` (or your install path's equivalent), "
            "then re-run. If you must stay on this AMX, restore the previous "
            "config from `~/.amx/config.yml.bak-*` if one exists, or back up "
            "and delete `~/.amx/config.yml` to start fresh."
        )
        sys.exit(2)
    init_history_store(ctx.obj)
    _bootstrap_scheduler_tick()
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
    # Diagnostic subcommands (currently just `doctor`) bypass the guard so
    # users can run them from a broken state without entering the REPL.
    direct_allowed: frozenset[str] = frozenset({"doctor"})
    if not is_session_child and ctx.invoked_subcommand not in direct_allowed:
        error(
            "Direct subcommands are disabled. Start with `amx`, then run slash commands "
            "inside the session (for example: /db, /connect, /run, /run-apply)."
        )
        raise click.ClickException("Use interactive mode only")


history_group = register_history_commands(main, pass_config=pass_config, log_event=_log_app_event)
register_compare_command(history_group, pass_config=pass_config, log_event=_log_app_event)
register_eval_confidence_command(history_group, pass_config=pass_config, log_event=_log_app_event)
register_search_commands(main, pass_config=pass_config, log_event=_log_app_event)
register_doctor_command(main, pass_config=pass_config, log_event=_log_app_event)
register_chat_session_commands(main, pass_config=pass_config, log_event=_log_app_event)
register_manual_commands(main, pass_config=pass_config, log_event=_log_app_event)
analyze = register_analyze_commands(main, pass_config=pass_config, log_event=_log_app_event)
register_analyze_run_command(
    analyze,
    finalize_scope=_finalize_scope,
    resolve_codebase_for_run=_resolve_codebase_for_run,
    log_event=_log_app_event,
)
register_analyze_review_command(analyze, log_event=_log_app_event)
register_rerun_command(main, pass_config=pass_config, log_event=_log_app_event)
register_variations_command(main, pass_config=pass_config, log_event=_log_app_event)
register_schedule_commands(analyze, pass_config=pass_config, log_event=_log_app_event)
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
# Attach /history-store under the /db group so it appears in the visual
# /db tab next to /db-profiles, /use-db, /add-db-profile, etc. The
# group object is cached on register_root_commands by name; pulling it
# here keeps the wiring obvious.
register_history_store_commands(
    register_root_commands._db_group,  # type: ignore[attr-defined]
    pass_config=pass_config,
    log_event=_log_app_event,
)


if __name__ == "__main__":
    run_cli()
