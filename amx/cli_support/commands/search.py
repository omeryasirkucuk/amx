"""Search namespace commands for the AMX interactive CLI."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

import click

from amx.cli_support._search_actions import (  # noqa: PLC0414
    _run_approved_search_actions as _run_approved_search_actions,
)
from amx.cli_support._search_actions import (
    _run_search_action as _run_search_action,
)
from amx.cli_support._search_actions import (
    _sync_cached_code_evidence as _sync_cached_code_evidence,
)
from amx.cli_support._search_render import (  # noqa: PLC0414
    _render_search_rows as _render_search_rows,
)
from amx.config import AMXConfig
from amx.db.connector import DatabaseConnector, ProfilingError
from amx.services.analyze_scope import finalize_scope as _finalize_scope

if TYPE_CHECKING:
    # ``amx.search.catalog`` and ``amx.search.service`` reach
    # ``amx.search.index`` which imports chromadb at module top — a
    # ~400 ms cost we don't want to pay on every CLI launch. Type
    # references are string-only thanks to ``from __future__ import
    # annotations``; runtime use sites lazy-import these names below.
    from amx.search.catalog import SearchCatalog
    from amx.search.service import SearchService
from amx.storage.sqlite_store import history_store
from amx.utils.console import (
    ask_choice,
    ask_multi_choice,
    confirm,
    console,
    error,
    info,
    info_markdown,
    info_styled,
    render_table,
    success,
    warn,
)
from amx.utils.live_commands import command_display
from amx.utils.live_display import get_display

LogEvent = Callable[..., None]


def _catalog() -> SearchCatalog | None:
    from amx.search.catalog import SearchCatalog as _SearchCatalog

    return _SearchCatalog.from_history_store()


def _service(
    cfg: AMXConfig,
    *,
    db_profiles: list[str] | None = None,
) -> SearchService | None:
    """Build a SearchService bound to the given (or persisted) DB scope.

    0.11.0: ``db_profiles`` is a per-call override used by
    ``/ask --db-profile``. When omitted the service falls back to the
    persisted active scope (``cfg.active_db_profiles``).
    """
    from amx.search.service import SearchService as _SearchService

    catalog = _catalog()
    if catalog is None:
        error("Search catalog is not initialized.")
        return None
    return _SearchService(cfg, catalog, db_profiles=db_profiles)


def _search_scope_from_answer(answer: Any) -> dict[str, list[str]]:
    scope = answer.details.get("scope") or {}
    if isinstance(scope, dict) and scope:
        out: dict[str, list[str]] = {}
        for key, values in scope.items():
            if not key or not isinstance(values, list):
                continue
            uniq = [str(value) for value in values if str(value)]
            if uniq:
                out[str(key)] = uniq
        if out:
            return out
    rows = answer.rows or []
    grouped: dict[str, list[str]] = {}
    for row in rows:
        schema_name = str(row.get("schema_name") or "")
        table_name = str(row.get("table_name") or "")
        if not schema_name or not table_name:
            continue
        grouped.setdefault(schema_name, [])
        if table_name not in grouped[schema_name]:
            grouped[schema_name].append(table_name)
    return grouped


def _search_results_payload(answer: Any) -> dict[str, Any]:
    return {
        "intent": answer.intent,
        "question_class": answer.details.get("question_class", ""),
        "question": answer.question,
        "confidence": answer.confidence,
        "summary": answer.summary,
        "provenance": answer.provenance,
        "retrieval": answer.details.get("retrieval", {}),
        "verification": answer.details.get("verification", {}),
        "policy": answer.details.get("policy", {}),
        "plan": answer.details.get("plan", {}),
        "actions": answer.details.get("actions", []),
        "action_results": answer.details.get("action_results", []),
        "ambiguity_flags": answer.details.get("ambiguity_flags", []),
        "evidence_sources": answer.details.get("evidence_sources", []),
        "stage_metrics": answer.details.get("stage_metrics", []),
        "thought_trace": answer.details.get("thought_trace", []),
        "reason": answer.details.get("reason", ""),
        "rows": [
            {
                "schema": row.get("schema_name", ""),
                "table": row.get("table_name", ""),
                "column": row.get("column_name", ""),
                "score": row.get("rank_score", row.get("score", 0)),
                "source": row.get("effective_source_kind", row.get("source", "")),
                "relationship_type": row.get("relationship_type", ""),
                "confidence_band": row.get("confidence_band", ""),
                "verified_live": bool(row.get("verified_live")),
            }
            for row in (answer.rows or [])[:10]
        ],
    }


def _answer_scope(answer: Any, cfg: AMXConfig) -> dict[str, list[str]]:
    scope = _search_scope_from_answer(answer)
    if scope:
        return scope
    if cfg.current_schema and cfg.current_table:
        return {cfg.current_schema: [cfg.current_table]}
    return {}


def launch_ask_session(
    cfg: AMXConfig,
    question_text: str,
    *,
    log_event: LogEvent,
    follow_up: bool = True,
) -> None:
    """Launch a seeded ``/ask`` session for a pre-formed question and
    leave the user in the sticky ``ask>`` REPL so they can act on
    whatever follow-up the LLM offered.

    Used by sibling commands (``/history compare``) when the user
    accepts a "next: Ask AMX about this" prompt. Performs the same
    LLM-config pre-flight as the normal ``/search ask`` Click command,
    builds a service for the persisted DB scope, routes the seed
    question through ``_run_search_ask`` so the answer renders
    identically to the user-typed path, and then drops into the
    sticky ``ask>`` REPL — without that drop the LLM's
    "If you want, I can next drill into the per-column comparison"
    nudge was unactionable: control returned to the parent slash
    prompt and the user couldn't reply without losing the chat
    session's context.

    ``follow_up=False`` skips the REPL drop (used by callers / tests
    that want strict one-shot semantics).
    """
    if not cfg.llm_profiles:
        error(
            "No LLM profile is configured. Run `/add-llm-profile` (or "
            "`/setup`) before launching Ask AMX."
        )
        return
    if not cfg.llm.provider or not cfg.llm.model:
        active = cfg.active_llm_profile or "(none)"
        error(
            f"Active LLM profile '{active}' is incomplete (provider/model "
            "unset). Run `/llm` to pick one of the configured profiles."
        )
        return
    text = (question_text or "").strip()
    if not text:
        return
    svc = _service(cfg)
    if svc is None:
        return
    with svc:
        _run_search_ask(cfg, svc, text, log_event=log_event)

    if not follow_up:
        return
    # Drop into the sticky ``ask>`` REPL. ``_run_search_ask`` updates
    # ``cfg.active_chat_session_id`` as a side effect of the agent
    # call, so the REPL resumes the same chat thread instead of
    # opening a fresh one — follow-up turns inherit the seed prompt
    # as context. We pull ``main_command`` from the active Click
    # context (compare runs inside the REPL's ``main_command.main()``
    # dispatch, so the root group is always reachable). Outside any
    # Click context (unit tests, scripted invocations) we silently
    # skip the REPL — the seed answer already rendered.
    try:
        from click import get_current_context

        from amx.cli_support.session import _run_ask_repl

        ctx = get_current_context(silent=True)
    except Exception:
        return
    if ctx is None:
        return
    main_command = ctx.find_root().command
    try:
        _run_ask_repl(cfg, main_command=main_command, log_event=log_event)
    except (EOFError, KeyboardInterrupt):
        # Non-TTY stdin (CI, piped invocation) — the seed answer
        # already printed; just return cleanly.
        pass


def _run_search_ask(
    cfg: AMXConfig,
    svc: SearchService,
    question_text: str,
    *,
    log_event: LogEvent,
    take_actions: bool = False,
    debug: bool = False,
    asset_kinds: list[str] | None = None,
    lineage_profiles: list[str] | None = None,
    pages_enabled: bool | None = None,
) -> None:
    from amx.utils.logging import clear_request_id, get_logger, set_request_id

    # Tag every log line emitted while answering this question with a
    # short id so users can extract the trace from amx.log via:
    #   jq 'select(.request_id == "...")' ~/.amx/logs/amx.log
    request_id = set_request_id()
    log = get_logger("cli.search")
    log.info(
        "search ask started: request_id=%s, question_len=%d",
        request_id,
        len(question_text),
    )
    try:
        _run_search_ask_body(
            cfg,
            svc,
            question_text,
            log_event=log_event,
            take_actions=take_actions,
            debug=debug,
            asset_kinds=asset_kinds,
            lineage_profiles=lineage_profiles,
            pages_enabled=pages_enabled,
        )
    finally:
        clear_request_id()


def _run_search_ask_body(
    cfg: AMXConfig,
    svc: SearchService,
    question_text: str,
    *,
    log_event: LogEvent,
    take_actions: bool,
    debug: bool = False,
    asset_kinds: list[str] | None = None,
    lineage_profiles: list[str] | None = None,
    pages_enabled: bool | None = None,
) -> None:
    display = get_display()
    started_display = False
    if not display.is_active:
        display.start(
            schema=cfg.current_schema or "",
            table=cfg.current_table or "",
            mode="search",
            provider=cfg.llm.provider,
            model=cfg.llm.model,
        )
        started_display = True
    # Ctrl-C cancellation: install a temporary SIGINT handler that sets
    # a threading.Event the agent loop polls between iterations. Without
    # this, a long-running LLM call (or a chained tool call sequence)
    # had to drain to completion before Python's default KeyboardInterrupt
    # path could reach the user — they reported "Ctrl-C can't end the
    # ask session". The handler ALSO raises KeyboardInterrupt the second
    # time it fires so a stuck HTTP socket eventually unblocks; the
    # first press just sets the flag for a graceful between-iteration
    # exit. Restored in ``finally`` so the REPL's own SIGINT handling
    # (return-to-prompt on Ctrl-C) takes over after the ask returns.
    import signal as _signal
    import threading as _threading

    cancel_token = _threading.Event()
    interrupt_count = {"n": 0}
    previous_handler = _signal.getsignal(_signal.SIGINT)

    def _on_sigint(_signum, _frame):
        interrupt_count["n"] += 1
        cancel_token.set()
        # First press: set the flag and let the agent loop exit cleanly.
        # Second press: raise so any blocked I/O (e.g. mid-stream HTTP
        # read that ignored the flag) terminates immediately.
        if interrupt_count["n"] >= 2:
            raise KeyboardInterrupt

    try:
        _signal.signal(_signal.SIGINT, _on_sigint)
    except Exception:
        # Worker threads / Windows compat: fall through to default.
        previous_handler = None
    try:
        answer = svc.ask(
            question_text,
            cancel_token=cancel_token,
            asset_kinds=asset_kinds,
            lineage_profiles=lineage_profiles,
            pages_enabled=pages_enabled,
        )
    except KeyboardInterrupt:
        from amx.search.catalog import SearchAnswer

        answer = SearchAnswer(
            intent="cancelled",
            question=question_text,
            rows=[],
            confidence="low",
            summary="Cancelled by user.",
            provenance=["user_cancelled"],
            details={"reason": "cancelled_by_user"},
        )
    finally:
        if started_display:
            display.stop()
        if previous_handler is not None:
            try:
                _signal.signal(_signal.SIGINT, previous_handler)
            except Exception:
                pass
    hs = history_store()
    run_id: int | None = None
    if hs is not None:
        run_id = hs.create_run(
            command="search.ask",
            mode="chat",
            db_backend=cfg.db.backend,
            db_profile=cfg.active_db_profile or "default",
            llm_provider=cfg.llm.provider,
            llm_model=cfg.llm.model,
            scope=_search_scope_from_answer(answer),
            llm_profile=cfg.active_llm_profile,
            doc_profile=cfg.active_doc_profile or None,
            code_profile=cfg.active_code_profile or None,
            # Capture the LLM-side knobs that varied between asks so
            # /history compare can surface them. The orchestration
            # knobs (dedup, missing_only, batch, review_strategy)
            # don't apply to /ask but having a uniform settings_json
            # shape across run + ask makes the compare table simpler.
            settings={
                "prompt_detail": getattr(cfg.llm, "prompt_detail", ""),
                "language": getattr(cfg.llm, "language", ""),
                "completion_mode": getattr(cfg.llm, "completion_mode", ""),
                "temperature": float(getattr(cfg.llm, "temperature", 0.0) or 0.0),
                "max_tokens": int(getattr(cfg.llm, "max_tokens", 0) or 0),
                "force_logprobs": bool(getattr(cfg.llm, "force_logprobs", True)),
            },
        )
    info_markdown(answer.summary)
    # Provenance and confidence are diagnostic, not conversational. Surface
    # them only when --debug is set or when the user explicitly opted in via
    # `/search config show_provenance true`. By default we keep the answer
    # uncluttered: a one-line summary plus the focused result panel.
    show_prov = svc.settings.get("show_provenance", "false").lower() == "true"
    show_conf = svc.settings.get("show_confidence", "false").lower() == "true"
    if (debug or show_prov) and answer.provenance:
        info("Provenance: " + "; ".join(answer.provenance))
    if debug or show_conf:
        info(f"Confidence: {answer.confidence}")
    trace = answer.details.get("thought_trace", []) or []
    if debug and trace:
        info("Thought Trace:")
        for idx, step in enumerate(trace, start=1):
            if not isinstance(step, dict):
                continue
            label = str(step.get("step") or f"step_{idx}")
            observation = str(step.get("observation") or "")
            info(f"  {idx}. {label}: {observation}")
    for action in answer.details.get("actions", []) or []:
        action_name = str((action or {}).get("action") or "").strip()
        action_reason = str((action or {}).get("reason") or "").strip()
        if action_name:
            info(
                f"Suggested next step: {action_name}"
                + (f" — {action_reason}" if action_reason else "")
            )
    action_results: list[dict[str, Any]] = []
    if take_actions:
        action_results = _run_approved_search_actions(cfg, svc, answer)
        if action_results:
            answer.details["action_results"] = action_results
    if answer.rows and bool(answer.details.get("display_rows", True)):
        _render_search_rows(
            answer.rows,
            answer_shape=str(answer.details.get("answer_shape") or ""),
            debug=debug,
        )
    payload = _search_results_payload(answer)
    status = "success"
    error_text = ""
    if answer.details.get("reason") in {"no_llm", "llm_failure"}:
        status = "failed"
        error_text = answer.summary
    if hs is not None and run_id is not None:
        hs.finish_run(
            run_id,
            status=status,
            metrics=answer.details.get("llm_usage", {}),
            tokens=answer.details.get("tokens", {}),
            results=payload,
            error_text=error_text,
        )
    log_event(
        event_type="search_ask",
        status=status,
        command="search.ask",
        details={
            "question": question_text,
            "intent": answer.intent,
            "question_class": answer.details.get("question_class", ""),
            "confidence": answer.confidence,
            "reason": answer.details.get("reason", ""),
            "scope": _search_scope_from_answer(answer),
            "provenance": answer.provenance,
            "actions": answer.details.get("actions", []),
            "action_results": action_results,
            "evidence_sources": answer.details.get("evidence_sources", []),
            "ambiguity_flags": answer.details.get("ambiguity_flags", []),
            "stage_metrics": answer.details.get("stage_metrics", []),
        },
    )

    # Discoverability nudge for /compare. If at least two prior
    # search.ask runs already touched this scope, hint that the user
    # can pivot them. Quiet, single-line, and only when there's
    # something to compare — never on the first or second ask.
    if hs is not None and status == "success":
        scope = _search_scope_from_answer(answer)
        primary_schema = next(iter(scope.keys()), "") if isinstance(scope, dict) else ""
        if primary_schema:
            try:
                prior = hs.find_runs_for_scope(
                    schema=primary_schema,
                    command_filter="search.ask",
                    limit=4,
                )
            except Exception:
                prior = []
            if len(prior) >= 3:
                console.print(
                    f"[dim]hint: /history compare --last 3 --schema {primary_schema} "
                    f"to see how this answer differs from prior runs.[/dim]"
                )


def _sync_db_scope(
    cfg: AMXConfig,
    catalog: SearchCatalog,
    *,
    scope: dict[str, list[str]],
) -> tuple[int, int]:
    db = DatabaseConnector(cfg.db)
    db_profile = cfg.active_db_profile or "default"
    database_name = cfg.db.database or cfg.db.catalog or cfg.db.project or ""
    inserted = 0
    updated = 0
    # Clear any stale degradation flag from a previous catalog reuse so
    # the post-sync hint reflects THIS run's indexing outcome only.
    if hasattr(catalog, "_index_degraded"):
        catalog._index_degraded = False
    total_assets = sum(len(asset_names) for asset_names in scope.values())
    display = get_display()
    activity_idx: int | None = None
    if total_assets and display.is_active:
        activity_idx = display.add_activity(f"Search sync 0/{total_assets}")
        display.begin_activity(activity_idx)
    processed = 0
    failed = 0
    for schema_name, asset_names in scope.items():
        for asset_name in asset_names:
            processed += 1
            if activity_idx is not None:
                display.set_context(schema=schema_name, table=asset_name)
                display.update_activity(
                    activity_idx,
                    label=f"Search sync {processed}/{total_assets}: {schema_name}.{asset_name}",
                )
            asset_kind = db.resolve_asset_kind(schema_name, asset_name)
            try:
                if activity_idx is None:
                    from amx.utils.console import step_spinner

                    with step_spinner(f"Profiling {schema_name}.{asset_name} for /search"):
                        profile = db.profile_table(
                            schema_name, asset_name, sample_size=0, asset_kind=asset_kind
                        )
                else:
                    profile = db.profile_table(
                        schema_name, asset_name, sample_size=0, asset_kind=asset_kind
                    )
            except ProfilingError as exc:
                failed += 1
                if activity_idx is not None:
                    display.add_detail(
                        activity_idx, f"Skipped {schema_name}.{asset_name}: {str(exc)[:220]}"
                    )
                warn(str(exc))
                continue
            if activity_idx is None:
                from amx.utils.console import step_spinner

                with step_spinner(f"Writing {schema_name}.{asset_name} into search catalog"):
                    catalog.sync_table_profile(
                        db_profile=db_profile,
                        db_backend=cfg.db.backend,
                        database_name=database_name,
                        profile=profile,
                        query_usage={},
                    )
            else:
                catalog.sync_table_profile(
                    db_profile=db_profile,
                    db_backend=cfg.db.backend,
                    database_name=database_name,
                    profile=profile,
                    query_usage={},
                )
            updated += 1
    if activity_idx is not None:
        summary = f"Synced {updated}/{total_assets} asset(s)"
        if failed:
            summary += f"; skipped {failed}"
            display.fail_activity(activity_idx, summary)
        else:
            display.complete_activity(activity_idx, summary)
    # Structured metadata always lands now even when semantic indexing
    # was skipped (embedding profile drifted from the vector
    # collection). Surface that once so the user knows columns/row
    # counts are synced but semantic search needs a rebuild.
    if getattr(catalog, "_index_degraded", False):
        warn(
            "Columns and row counts were synced, but semantic search indexing "
            "was skipped because the embedding profile no longer matches the "
            "vector collection. Run `/search rebuild` to restore semantic search."
        )
    return inserted, updated


def _interactive_sync_scope(
    cfg: AMXConfig,
    schema_name: str | None,
    table_name: str | None,
) -> tuple[AMXConfig, dict[str, list[str]] | None]:
    if (
        not schema_name
        and not table_name
        and len(cfg.db_profiles) > 1
        and not confirm(
            f"Continue with current DB profile '{cfg.active_db_profile or 'default'}'?",
            default=True,
        )
    ):
        selected = ask_choice(
            "Select DB profile for /search sync",
            sorted(cfg.db_profiles.keys()),
            default=cfg.active_db_profile or sorted(cfg.db_profiles.keys())[0],
        )
        cfg.set_active_db_profile(selected)
        info_styled("Active DB", selected)
    db = DatabaseConnector(cfg.db)
    scope = _finalize_scope(
        cfg,
        db,
        schema_name,
        [table_name] if table_name else [],
        ask_choice=ask_choice,
        ask_multi_choice=ask_multi_choice,
        error=error,
        warn=warn,
    )
    return cfg, scope


def register_search_commands(
    main: click.Group,
    *,
    pass_config: Callable[[Callable[..., Any]], Callable[..., Any]],
    log_event: LogEvent,
) -> click.Group:
    """Attach `/search` namespace commands to the main Click group.

    Returns the inner ``search`` Click group so callers (``cli.py``) can
    attach extra subcommands to the same namespace from sibling files —
    same pattern as ``register_analyze_commands``.
    """

    @main.group(invoke_without_command=True)
    @click.pass_context
    def search(ctx: click.Context) -> None:
        """Chat-first metadata discussion surface."""
        if ctx.invoked_subcommand is None:
            info(
                "Use `/search ask <question>` or just type a question inside the /search tab. "
                "Use `/status`, `/sync`, or `/rebuild` for catalog operations."
            )

    @search.command("ask")
    @click.option(
        "--actions",
        "take_actions",
        is_flag=True,
        help="Prompt before running approved follow-up actions.",
    )
    @click.option(
        "--debug",
        "--verbose",
        "debug",
        is_flag=True,
        help="Show the planner's thought trace, raw match scores, and source kind for each result.",
    )
    @click.option(
        "--db-profile",
        "db_profile",
        multiple=True,
        help=(
            "Override the DB profile scope for this question. "
            "Pass multiple times for multi-DB retrieval, e.g. "
            "--db-profile prod_pg --db-profile analytics_bq. "
            "When omitted, uses the persisted scope set by /use-db."
        ),
    )
    @click.option(
        "--assets",
        "assets",
        multiple=True,
        help=(
            "Restrict ingested-asset retrieval to the listed kinds "
            "(notebooks, queries, jobs, pipelines, streams, streamlit_apps). "
            "Pass `none` to disable assets entirely for this question. "
            "Omit to let the agent decide."
        ),
    )
    @click.option(
        "--lineage",
        "lineage",
        multiple=True,
        help=(
            "Restrict lineage retrieval to the listed canvas/profile names. "
            "Pass `none` to disable lineage for this question. Omit for auto."
        ),
    )
    @click.option(
        "--pages/--no-pages",
        "pages_flag",
        default=None,
        help="Toggle ingested-documentation (pages) retrieval. Default: auto.",
    )
    @click.argument("question", nargs=-1, required=True)
    @pass_config
    def search_ask(
        cfg: AMXConfig,
        take_actions: bool,
        debug: bool,
        db_profile: tuple[str, ...],
        assets: tuple[str, ...],
        lineage: tuple[str, ...],
        pages_flag: bool | None,
        question: tuple[str, ...],
    ) -> None:
        # Pre-flight: bail with a clear, actionable message when no
        # LLM is configured. Without this, the search agent enters
        # the planner loop, fails inside the first LiteLLM call with a
        # cryptic provider-side stack trace, and leaves the user
        # wondering whether the issue is the question or the setup.
        # Mirrors the analogous guard in analyze_flow.py for /run.
        if not cfg.llm_profiles:
            error(
                "No LLM profile is configured. Run `/add-llm-profile` (or `/setup`) "
                "to add one before asking a question."
            )
            return
        if not cfg.llm.provider or not cfg.llm.model:
            active = cfg.active_llm_profile or "(none)"
            error(
                f"Active LLM profile '{active}' is incomplete (provider/model unset). "
                "Run `/llm` to pick one of the configured profiles, or "
                "`/add-llm-profile` to add a new one."
            )
            return

        # 0.11.0: --db-profile (multi) lets the user override the
        # persisted active scope for a single question. Validate names
        # against configured profiles before constructing the service —
        # an unknown profile here is a user error worth surfacing
        # explicitly rather than letting retrieval silently return
        # nothing.
        scope_override: list[str] | None = None
        if db_profile:
            unknown = [n for n in db_profile if n not in cfg.db_profiles]
            if unknown:
                error(
                    f"Unknown DB profile(s): {', '.join(unknown)}. "
                    f"Available: {', '.join(sorted(cfg.db_profiles)) or '(none)'}."
                )
                return
            # Dedupe preserving order so --db-profile a --db-profile a → [a].
            seen: set[str] = set()
            scope_override = []
            for name in db_profile:
                if name not in seen:
                    seen.add(name)
                    scope_override.append(name)
        svc = _service(cfg, db_profiles=scope_override)
        if svc is None:
            return
        question_text = " ".join(question).strip()
        if not question_text:
            error("Usage: /search ask <question>")
            return

        # ``--assets`` / ``--lineage`` / ``--pages`` translate into the
        # tri-state argument shape the enrichment layer expects:
        #   * absent     → ``None``  (auto: agent / planner decides)
        #   * ``none``   → ``[]``    (explicitly off for this question)
        #   * one+ vals  → list      (restrict to those kinds/profiles)
        # Studio's per-question overrides take the same shape so the
        # CLI and web paths stay aligned.
        asset_kinds: list[str] | None
        if not assets:
            asset_kinds = None
        elif len(assets) == 1 and assets[0].lower() == "none":
            asset_kinds = []
        else:
            asset_kinds = [a for a in assets if a.lower() != "none"]
        lineage_profiles: list[str] | None
        if not lineage:
            lineage_profiles = None
        elif len(lineage) == 1 and lineage[0].lower() == "none":
            lineage_profiles = []
        else:
            lineage_profiles = [p for p in lineage if p.lower() != "none"]

        # Use the context manager so the cached live DB connector
        # (SQLAlchemy engine + connection pool) is disposed when the
        # question finishes, preventing FD leaks across REPL turns.
        with svc:
            _run_search_ask(
                cfg,
                svc,
                question_text,
                log_event=log_event,
                take_actions=take_actions,
                debug=debug,
                asset_kinds=asset_kinds,
                lineage_profiles=lineage_profiles,
                pages_enabled=pages_flag,
            )

    @search.command("status")
    @pass_config
    def search_status(cfg: AMXConfig) -> None:
        catalog = _catalog()
        if catalog is None:
            error("Search catalog is not initialized.")
            return
        status = catalog.sync_status(cfg.active_db_profile or "default")
        llm_ready = "yes" if (cfg.llm.provider and cfg.llm.model) else "no"
        total_entities = int(status["entities"].get("total_entities", 0) or 0)
        rows = [
            ["qa.ready", "yes" if total_entities > 0 else "no"],
            ["llm.ready", llm_ready],
            ["context.detail", status["settings"].get("context_detail", "standard")],
            ["verify.live_inventory", status["settings"].get("verify_live_inventory", "true")],
            ["semantic_join_inference", status["settings"].get("semantic_join_inference", "true")],
            ["entities.total", total_entities],
            ["entities.effective", status["entities"].get("effective_entities", 0)],
            ["descriptions.total", status["descriptions"].get("total_descriptions", 0)],
            ["descriptions.manual", status["descriptions"].get("manual_count", 0)],
            ["descriptions.reviewed", status["descriptions"].get("reviewed_count", 0)],
            ["descriptions.generated", status["descriptions"].get("generated_count", 0)],
            ["descriptions.rejected", status["descriptions"].get("rejected_count", 0)],
            ["last_synced_at", status["entities"].get("last_synced_at", 0)],
        ]
        render_table("Search status", ["Metric", "Value"], rows)
        if status["jobs"]:
            render_table(
                "Recent sync jobs",
                ["Type", "Status", "Inserted", "Updated", "Started", "Completed"],
                [
                    [
                        row.get("job_type", ""),
                        row.get("status", ""),
                        row.get("inserted_count", 0),
                        row.get("updated_count", 0),
                        f"{float(row.get('started_at') or 0):.0f}",
                        f"{float(row.get('completed_at') or 0):.0f}"
                        if row.get("completed_at")
                        else "",
                    ]
                    for row in status["jobs"]
                ],
            )

    @search.command("sources")
    @click.option(
        "--schema-limit",
        "schema_limit",
        default=20,
        show_default=True,
        help="How many schemas to list (most-tables first).",
    )
    @pass_config
    def search_sources(cfg: AMXConfig, schema_limit: int) -> None:
        """Show what is included in the search index for the active DB profile.

        Lists the active DB profile, the databases / schemas / tables in scope,
        and any live or cached evidence sources backing the index. Settings
        live under `/search config`.
        """
        from datetime import datetime, timezone

        catalog = _catalog()
        if catalog is None:
            error("Search catalog is not initialized.")
            return
        db_profile = cfg.active_db_profile or "default"

        def _fmt_ts(value: Any) -> str:
            try:
                ts = float(value or 0)
            except (TypeError, ValueError):
                return "—"
            if ts <= 0:
                return "—"
            return (
                datetime.fromtimestamp(ts, tz=timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M")
            )

        status = catalog.sync_status(db_profile)
        entities = status.get("entities", {}) or {}
        total_entities = int(entities.get("total_entities") or 0)
        effective = int(entities.get("effective_entities") or 0)
        last_synced = entities.get("last_synced_at")
        total_tables = catalog.count_tables(db_profile)

        backend = (cfg.db.backend or "—").lower()
        host = cfg.db.display_summary or cfg.db.host or "—"
        scope_rows = [
            ["DB profile", db_profile],
            ["Backend", backend],
            ["Connection", host],
            ["Indexed entities", str(total_entities)],
            ["Tables in scope", str(total_tables)],
            ["Effective descriptions", str(effective)],
            ["Last synced", _fmt_ts(last_synced)],
        ]
        render_table("Search scope", ["Field", "Value"], scope_rows)

        databases = catalog.known_databases(db_profile)
        if databases:
            render_table(
                "Databases in scope",
                ["Database", "Indexed entities"],
                [
                    [row.get("database_name", "—") or "—", row.get("entity_count", 0)]
                    for row in databases
                ],
            )
        else:
            info("No databases recorded yet — run `/search sync` to populate the index.")
            return

        schemas = catalog.known_schemas(db_profile)
        if schemas:
            schemas_sorted = sorted(
                schemas,
                key=lambda r: int(r.get("table_count") or 0),
                reverse=True,
            )
            shown = schemas_sorted[: max(1, int(schema_limit))]
            hidden = len(schemas_sorted) - len(shown)
            render_table(
                f"Schemas in scope ({len(schemas_sorted)} total)",
                ["Database", "Schema", "Tables"],
                [
                    [
                        row.get("database_name", "—") or "—",
                        row.get("schema_name", "—") or "—",
                        row.get("table_count", 0),
                    ]
                    for row in shown
                ],
            )
            if hidden > 0:
                info(
                    f"… {hidden} more schema(s) hidden. Re-run with `--schema-limit {len(schemas_sorted)}` to see all."
                )

        evidence_rows = catalog.sources_status(db_profile)
        if evidence_rows:
            render_table(
                "Evidence sources",
                ["Source", "Evidence", "Rows", "Last seen"],
                [
                    [
                        row.get("source_kind", ""),
                        row.get("evidence_type", ""),
                        row.get("count_rows", 0),
                        _fmt_ts(row.get("last_seen")),
                    ]
                    for row in evidence_rows
                ],
            )
        else:
            info(
                "No live or cached evidence yet. Run `/search sync` to ingest sample-data and code evidence."
            )

    @search.command("config")
    @click.argument("key", required=False)
    @click.argument("value", required=False)
    @pass_config
    def search_config(cfg: AMXConfig, key: str | None, value: str | None) -> None:
        catalog = _catalog()
        if catalog is None:
            error("Search catalog is not initialized.")
            return
        db_profile = cfg.active_db_profile or "default"
        if key and value is not None:
            catalog.set_setting(db_profile, key, value)
            success(f"Updated search config for {db_profile}: {key}={value}")
            return
        settings = catalog.get_settings(db_profile)
        render_table(
            f"Search config: {db_profile}",
            ["Key", "Value"],
            [[name, val] for name, val in sorted(settings.items())],
        )

    @search.command("context-detail")
    @click.argument("level", required=False)
    @pass_config
    def search_context_detail(cfg: AMXConfig, level: str | None) -> None:
        catalog = _catalog()
        if catalog is None:
            error("Search catalog is not initialized.")
            return
        db_profile = cfg.active_db_profile or "default"
        if level:
            normalized = level.strip().lower()
            if normalized not in {"minimal", "standard", "rich", "deep"}:
                error("Context detail must be one of: minimal, standard, rich, deep.")
                return
            catalog.set_setting(db_profile, "context_detail", normalized)
            success(f"Updated search context detail for {db_profile}: {normalized}")
            return
        info(
            f"Current search context detail: {catalog.get_settings(db_profile).get('context_detail', 'standard')}"
        )

    @search.command("sync")
    @click.option("--schema", "schema_name", default=None, help="Limit sync to one schema.")
    @click.option(
        "--table",
        "table_name",
        default=None,
        help="Limit sync to one table in the selected schema.",
    )
    @click.option(
        "--db-profile",
        "db_profile_override",
        multiple=True,
        help=(
            "Override the DB profile scope for this sync. Pass multiple "
            "times to sync several profiles in one command, e.g. "
            "--db-profile prod_pg --db-profile analytics_bq."
        ),
    )
    @pass_config
    def search_sync(
        cfg: AMXConfig,
        schema_name: str | None,
        table_name: str | None,
        db_profile_override: tuple[str, ...],
    ) -> None:
        catalog = _catalog()
        if catalog is None:
            error("Search catalog is not initialized.")
            return

        # 0.11.0: resolve scope (CLI > persisted > legacy single).
        if db_profile_override:
            unknown = [n for n in db_profile_override if n not in cfg.db_profiles]
            if unknown:
                error(
                    f"Unknown DB profile(s): {', '.join(unknown)}. "
                    f"Available: {', '.join(sorted(cfg.db_profiles)) or '(none)'}."
                )
                return
            seen: set[str] = set()
            scope_names: list[str] = []
            for name in db_profile_override:
                if name not in seen:
                    seen.add(name)
                    scope_names.append(name)
        else:
            scope_names = list(cfg.effective_db_profiles())
        if not scope_names:
            scope_names = [cfg.active_db_profile or "default"]

        is_multi = len(scope_names) > 1
        if is_multi:
            info(f"Syncing across {len(scope_names)} DB profiles: {', '.join(scope_names)}")

        original_active = cfg.active_db_profile
        try:
            for idx, profile_name in enumerate(scope_names, start=1):
                if is_multi:
                    info(f"--- Profile {idx}/{len(scope_names)}: {profile_name} ---")
                if profile_name in cfg.db_profiles:
                    object.__setattr__(cfg, "active_db_profile", profile_name)
                    cfg.db = cfg.db_profiles[profile_name]

                with command_display(
                    schema=schema_name or cfg.current_schema or "",
                    table=table_name or cfg.current_table or "",
                    mode="search-sync",
                    provider=cfg.llm.provider,
                    model=cfg.llm.model,
                ):
                    # Catalog picker for 3-level backends — fires before
                    # _interactive_sync_scope so the schema picker that
                    # runs there is already catalog-aware.
                    # 0.11.0 Phase 8: also warn for 2-level backends
                    # without a pinned database so the user knows up
                    # front the listings may be empty.
                    try:
                        from amx.cli_support.catalog_picker import (
                            ensure_hierarchy_resolved,
                            warn_when_database_unpinned,
                        )
                        from amx.db.connector import DatabaseConnector

                        _db_for_pick = DatabaseConnector(cfg.db)
                        ensure_hierarchy_resolved(_db_for_pick)
                        warn_when_database_unpinned(_db_for_pick)
                    except Exception:
                        pass
                    cfg, scope = _interactive_sync_scope(cfg, schema_name, table_name)
                    if not scope:
                        if is_multi:
                            warn(f"No scope selected for profile '{profile_name}', skipping.")
                            continue
                        return
                    db_profile = cfg.active_db_profile or profile_name
                    job_id = catalog.start_sync_job(db_profile, "sync", {"scope": scope})
                    inserted = 0
                    updated = 0
                    try:
                        inserted, updated = _sync_db_scope(cfg, catalog, scope=scope)
                        _sync_cached_code_evidence(cfg, catalog, scope=scope)
                        catalog.finish_sync_job(
                            job_id,
                            status="success",
                            inserted_count=inserted,
                            updated_count=updated,
                        )
                        success(
                            f"Search sync complete for '{profile_name}': "
                            f"inserted={inserted}, updated={updated}"
                        )
                        log_event(
                            event_type="search_sync",
                            status="success",
                            command="search.sync",
                            details={
                                "scope": scope,
                                "updated": updated,
                                "db_profile": profile_name,
                            },
                        )
                    except Exception as exc:
                        catalog.finish_sync_job(
                            job_id,
                            status="failed",
                            inserted_count=inserted,
                            updated_count=updated,
                            error_text=str(exc),
                        )
                        log_event(
                            event_type="search_sync",
                            status="failed",
                            command="search.sync",
                            details={"error": str(exc), "db_profile": profile_name},
                        )
                        if is_multi:
                            warn(
                                f"Sync failed for '{profile_name}': {exc}. "
                                "Continuing with remaining profiles."
                            )
                            continue
                        raise
        finally:
            if original_active and original_active in cfg.db_profiles:
                object.__setattr__(cfg, "active_db_profile", original_active)
                cfg.db = cfg.db_profiles[original_active]

    @search.command("rebuild")
    @pass_config
    def search_rebuild(cfg: AMXConfig) -> None:
        catalog = _catalog()
        if catalog is None:
            error("Search catalog is not initialized.")
            return
        with command_display(
            mode="search-rebuild", provider=cfg.llm.provider, model=cfg.llm.model
        ) as display:
            total_entities = int(
                catalog.sync_status(cfg.active_db_profile or "default")["entities"].get(
                    "total_entities", 0
                )
                or 0
            )
            activity_idx = display.add_activity(f"Search rebuild 0/{total_entities or '?'}")
            display.begin_activity(activity_idx)

            def _on_progress(index: int, total: int) -> None:
                display.update_activity(activity_idx, label=f"Search rebuild {index}/{total}")

            inserted, updated = catalog.rebuild_profile(
                cfg.active_db_profile or "default", on_progress=_on_progress
            )
            display.complete_activity(activity_idx, f"Rebuilt {updated} catalog entity rows")
        success(f"Search rebuild complete. inserted={inserted}, updated={updated}")
        log_event(
            event_type="search_rebuild",
            status="success",
            command="search.rebuild",
            details={
                "inserted": inserted,
                "updated": updated,
                "db_profile": cfg.active_db_profile or "default",
            },
        )

    @search.command("find-columns", hidden=True)
    @click.argument("question", nargs=-1, required=True)
    @pass_config
    def search_find_columns(cfg: AMXConfig, question: tuple[str, ...]) -> None:
        svc = _service(cfg)
        if svc is None:
            return
        with svc:
            _run_search_ask(cfg, svc, " ".join(question).strip(), log_event=log_event)

    @search.command("join-candidates", hidden=True)
    @click.argument("left_path")
    @click.argument("right_path")
    @pass_config
    def search_join_candidates(cfg: AMXConfig, left_path: str, right_path: str) -> None:
        svc = _service(cfg)
        if svc is None:
            return
        with svc:
            _run_search_ask(
                cfg,
                svc,
                f"Which columns should I join between {left_path} and {right_path}?",
                log_event=log_event,
            )

    @search.command("explain", hidden=True)
    @click.argument("question", nargs=-1, required=True)
    @pass_config
    def search_explain(cfg: AMXConfig, question: tuple[str, ...]) -> None:
        svc = _service(cfg)
        if svc is None:
            return
        with svc:
            _run_search_ask(cfg, svc, " ".join(question).strip(), log_event=log_event)

    @search.command("explain-table", hidden=True)
    @click.argument("table_path")
    @pass_config
    def search_explain_table(cfg: AMXConfig, table_path: str) -> None:
        svc = _service(cfg)
        if svc is None:
            return
        with svc:
            _run_search_ask(cfg, svc, f"What does table {table_path} do?", log_event=log_event)

    return search
