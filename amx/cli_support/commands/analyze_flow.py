"""Analyze run command flow for the AMX interactive CLI."""

from __future__ import annotations

import sys
import time
from collections.abc import Callable
from typing import Any

import click

from amx.config import AMXConfig
from amx.storage.sqlite_store import history_store
from amx.utils.console import (
    ask_choice,
    confirm,
    console,
    error,
    heading,
    info,
    render_table,
    step_spinner,
    warn,
)
from amx.utils.live_commands import command_display
from amx.utils.logging import get_logger
from amx.utils.token_tracker import tracker as token_tracker

log = get_logger("cli.analyze_flow")

FinalizeScope = Callable[[AMXConfig, object, str | None, list[str]], dict[str, list[str]] | None]
ResolveCodebaseForRun = Callable[
    [AMXConfig, object, dict[str, list[str]], str | None, bool], object | None
]
LogEvent = Callable[..., None]


def _require_llm_connection(llm: object, *, profile_label: str | None = None) -> None:
    label = f" using profile '{profile_label}'" if profile_label else ""
    cfg = getattr(llm, "cfg", None)
    target = ""
    if cfg is not None:
        provider = getattr(cfg, "provider", "") or ""
        model = getattr(cfg, "model", "") or ""
        if provider and model:
            target = f" to {provider}/{model}"
        elif provider:
            target = f" to {provider}"
    with step_spinner(f"Testing LLM connection{target} ..."):
        result = llm.test_result()
    if result.ok:
        return
    error(f"Cannot connect to the active LLM{label}.")
    if result.message:
        warn(result.message)
    sys.exit(1)


def _maybe_modify_profiles_before_run(
    cfg: AMXConfig, db: object, llm: object
) -> tuple[object, object]:
    from amx.config import DISABLED_PROFILE
    from amx.db.connector import DatabaseConnector
    from amx.llm.provider import LLMProvider

    if not confirm("Do you want to modify profiles before run?", default=False):
        return db, llm

    db_names = list(cfg.db_profiles.keys())
    if db_names:
        db_choice = ask_choice("Select DB profile", db_names, default=cfg.active_db_profile)
        cfg.set_active_db_profile(db_choice)
        info(f"Active DB: [bold cyan]{db_choice}[/]")
        db = DatabaseConnector(cfg.db)
        with step_spinner("Testing new database connection..."):
            if not db.test_connection():
                error(f"Cannot connect to database using profile '{db_choice}'.")
                sys.exit(1)

    llm_names = list(cfg.llm_profiles.keys())
    if llm_names:
        llm_choice = ask_choice("Select LLM profile", llm_names, default=cfg.active_llm_profile)
        cfg.set_active_llm_profile(llm_choice)
        info(f"Active LLM: [bold cyan]{llm_choice}[/]")
        llm = LLMProvider(cfg.llm)

    doc_names = list(cfg.doc_profiles.keys())
    if doc_names:
        options = doc_names + [DISABLED_PROFILE]
        doc_choice = ask_choice(
            "Select Document profile",
            options,
            default=cfg.active_doc_profile or DISABLED_PROFILE,
        )
        cfg.active_doc_profile = doc_choice
        info(f"Active Docs: [bold cyan]{doc_choice}[/]")

    code_names = list(cfg.code_profiles.keys())
    if code_names:
        options = code_names + [DISABLED_PROFILE]
        code_choice = ask_choice(
            "Select Codebase profile",
            options,
            default=cfg.active_code_profile or DISABLED_PROFILE,
        )
        cfg.active_code_profile = code_choice
        info(f"Active Code: [bold cyan]{code_choice}[/]")

    cfg.save()
    info("Profile selections saved to config.yml.")
    console.print()
    return db, llm


def _build_equivalence_members(
    db: object,
    scope: dict[str, list[str]],
    *,
    missing_only: bool,
) -> list[Any]:
    """Walk in-scope tables and build a flat list of ColumnMember records.

    The pre-walk happens BEFORE any LLM call so we can compute equivalence
    classes upfront. We use the same dtype/comment metadata the orchestrator
    will see later, so what we group here is what the dedup pass actually
    operates on.

    Respects ``missing_only``: when set, only columns whose existing
    live-DB comment is empty (or a known placeholder) are included; that
    way the dedup pass can't accidentally re-write a curated description.

    When the scope is a :class:`ScopeResult` with ``column_overrides``
    set (Column scope), only the explicitly-picked columns are
    considered — otherwise dedup would walk every column of the table
    and find unrelated equivalence classes the user didn't ask about.
    """
    from amx.agents.equivalence import ColumnMember
    from amx.agents.orchestrator import is_placeholder_description
    from amx.services.analyze_scope import ScopeResult

    overrides: dict[tuple[str, str], set[str]] = {}
    if isinstance(scope, ScopeResult):
        overrides = scope.column_overrides or {}

    members: list[ColumnMember] = []
    for schema_name, assets in scope.items():
        for asset_name in assets:
            override_cols = overrides.get((schema_name, asset_name))
            try:
                column_profiles = list(db.list_column_profiles(schema_name, asset_name))  # type: ignore[attr-defined]
            except Exception:
                continue
            try:
                comments_map = db.get_column_comments(schema_name, asset_name)  # type: ignore[attr-defined]
            except Exception:
                comments_map = {}
            for cp in column_profiles:
                if override_cols is not None and cp.name not in override_cols:
                    continue
                existing = (comments_map or {}).get(cp.name) or ""
                if missing_only:
                    if existing.strip() and not is_placeholder_description(existing):
                        continue
                members.append(
                    ColumnMember(
                        schema=schema_name,
                        table=asset_name,
                        column=cp.name,
                        dtype=str(cp.dtype),
                        existing_comment=existing or "",
                    )
                )
    return members


def _maybe_run_equivalence_dedup(
    cfg: AMXConfig,
    db: object,
    llm: object,
    *,
    scope: dict[str, list[str]],
    missing_only: bool,
    apply: bool,
    run_id: int | None,
) -> Any | None:
    """Pre-walk the scope, compute classes, ask the user, run the dedup pass.

    Returns the :class:`DedupOutcome` so callers can pass its
    ``skip_set`` to every Orchestrator created downstream. Returns
    ``None`` when the user declines or there's nothing to dedup.
    """
    from amx.agents.equivalence import (
        compute_column_equivalence_classes,
        summarize_classes,
    )
    from amx.agents.equivalence_agent import run_equivalence_pass

    members = _build_equivalence_members(db, scope, missing_only=missing_only)
    if not members:
        return None

    classes = compute_column_equivalence_classes(members)
    summary = summarize_classes(classes)
    if summary.multi_member_classes == 0:
        info(
            "Equivalence dedup: every in-scope column is unique by "
            "(name, dtype) — nothing to deduplicate."
        )
        return None

    # ── Equivalence analysis panel ───────────────────────────────────
    # Mirrors the /metadata edit "Bulk-update analysis for 'X'" header
    # so the user gets a consistent before-action summary across run
    # commands and bulk-edit. Heading line + key numbers + a small
    # table of the top classes that will dedup.
    heading("Equivalence analysis")
    info(
        f"  {summary.total_members} column(s) → "
        f"{summary.total_classes} class(es) "
        f"({summary.multi_member_classes} multi-member, "
        f"{summary.singleton_classes} singleton)."
    )
    info(
        f"  Largest class: '{summary.largest_class_name}' with "
        f"{summary.largest_class_size} members."
    )
    info(
        f"  Estimated LLM-call saving: {summary.llm_call_savings_pct:.1f}% "
        f"({summary.total_members - summary.total_classes} fewer column-level prompts)."
    )

    # Top-N preview: the largest classes by member count, so the user
    # can sanity-check what's about to be deduplicated.
    multi_classes = [c for c in classes.values() if not c.is_singleton]
    preview_classes = sorted(multi_classes, key=lambda c: c.size, reverse=True)[:10]
    preview_rows = []
    for klass in preview_classes:
        sample_tables = ", ".join(klass.tables(limit=3))
        if klass.size > 3:
            sample_tables += f", … (+{klass.size - 3} more)"
        preview_rows.append(
            [
                klass.name,
                klass.family,
                str(klass.size),
                sample_tables,
            ]
        )
    if preview_rows:
        render_table(
            f"Top {len(preview_rows)} classes that will dedup",
            ["Column", "Type family", "Members", "Tables (sample)"],
            preview_rows,
        )
    info(
        "  One LLM call per multi-member class will run; the description "
        "is then applied to every member."
    )
    outcome = run_equivalence_pass(
        multi_classes,
        llm=llm,
        db=db,
        apply_to_db=apply,
        run_id=run_id,
        db_profile=cfg.active_db_profile or "default",
        db_backend=cfg.db.backend,
        asset_kind="column",
    )
    return outcome


def _resolve_completion_mode(cfg: AMXConfig, llm: object, mode: str | None) -> bool:
    from rich.panel import Panel

    from amx.llm.batch import supported_providers as batch_supported_providers
    from amx.utils.console import ask_choice as prompt_choice

    batch_capable = llm.supports_batch
    batch_providers_list = batch_supported_providers()

    if mode is None:
        cfg_mode = (cfg.llm.completion_mode or "chat_completions").lower()
        default_mode_label = "batch" if cfg_mode == "batch" else "chat"
        batch_note = (
            " (50 % cheaper, async)"
            if batch_capable
            else f" (requires {', '.join(batch_providers_list)})"
        )
        mode = prompt_choice(
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
        console.print(
            Panel(
                "[bold]Batch API selected.[/bold]\n"
                "All LLM requests will be submitted as a single batch job.\n"
                "Typical turnaround: [bold]2–30 minutes[/bold]  |  Cost: [bold green]~50 % lower[/bold green]\n"
                "[dim]Live polling status will appear below.[/dim]",
                title="[cyan]Mode: Batch[/cyan]",
                border_style="cyan",
            )
        )
    else:
        info("Mode: [bold]Chat Completions[/bold] (real-time)")
    return use_batch


def _finalize_history_run(
    *,
    run_id: int | None,
    final_status: str | None,
    run_started: float,
    total_assets: int,
    total_schemas: int,
    processed_assets: list[str],
    skipped_assets: list[str],
    approved: list[object],
    skipped: list[object],
    apply: bool,
    all_results: list[object],
    final_error_text: str,
) -> None:
    if run_id is None:
        return
    hs = history_store()
    if hs is None:
        return
    try:
        hs.finish_run(
            run_id,
            status=final_status or "success",
            metrics={
                "duration_sec": round(time.monotonic() - run_started, 3),
                "model_processing_sec": round(token_tracker.total_model_processing_sec, 3),
                "total_assets": total_assets,
                "total_schemas": total_schemas,
                "processed_assets_count": len(processed_assets),
                "processed_assets": processed_assets,
                "skipped_assets_count": len(skipped_assets),
                "skipped_assets": skipped_assets,
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
                        "logprob_score": r.logprob_score,
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
                        "logprob_score": r.logprob_score,
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
                        "logprob_score": r.logprob_score,
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


def execute_analyze_run(
    cfg: AMXConfig,
    *,
    schema: str | None,
    table: tuple[str, ...],
    apply: bool,
    mode: str | None,
    tables_pos: tuple[str, ...],
    db: object,
    code_profile: str | None,
    code_refresh: bool,
    finalize_scope: FinalizeScope,
    resolve_codebase_for_run: ResolveCodebaseForRun,
    log_event: LogEvent,
) -> None:
    from amx.cli_support.commands._analyze import (
        handle_keyboard_interrupt,
        render_summary_and_apply,
        run_per_schema_loop,
    )
    from amx.config import DISABLED_PROFILE
    from amx.docs.rag import RAGStore
    from amx.llm.provider import FatalLLMError, LLMProvider
    from amx.utils.logging import clear_request_id, set_request_id

    # Tag every log line emitted during this analyze run with a stable
    # short id so users can `jq 'select(.request_id == "...")' amx.log`
    # to extract a single run from the structured log file.
    request_id = set_request_id()
    log.info(
        "analyze run started: request_id=%s, db_profile=%s, llm=%s/%s",
        request_id,
        cfg.active_db_profile,
        cfg.llm.provider,
        cfg.llm.model,
    )

    use_batch = False
    all_results: list[object] = []
    run_id: int | None = None
    run_started = time.monotonic()
    total_assets = 0
    total_schemas = 0
    approved: list[object] = []
    skipped: list[object] = []
    processed_assets: list[str] = []
    skipped_assets: list[str] = []
    # Tables the missing-only filter dropped because they were already
    # fully commented. Tracked separately from ``skipped_assets`` (which
    # only grows on ProfilingError) so /history can report planned_count
    # = total_assets - <filter skips> accurately.
    final_status: str | None = None
    final_error_text = ""
    # Pre-init these so the KeyboardInterrupt / Exception handlers
    # below don't trip an UnboundLocalError when the user cancels at the
    # scope picker (which is BEFORE the runtime questions get a chance
    # to assign these). Both get overwritten by the real prompts inside
    # the ``with command_display(...)`` block; the defaults here only
    # exist so the cancellation path can finalize history cleanly.
    review_strategy: str = "individual"
    use_dedup: bool = False
    dedup_outcome: Any | None = None

    try:
        token_tracker.reset()

        if not cfg.llm.provider or not cfg.llm.model:
            error(
                "No active LLM profile is configured. "
                "Use `/llm` then `/add-llm-profile`, or run `/setup`."
            )
            sys.exit(1)

        llm = LLMProvider(cfg.llm)

        if not apply:
            warn("Approved metadata stays in review. Use /apply or /run-apply to persist.")

        db, llm = _maybe_modify_profiles_before_run(cfg, db, llm)
        _require_llm_connection(llm, profile_label=cfg.active_llm_profile)
        use_batch = _resolve_completion_mode(cfg, llm, mode)

        # Catalog picker for 3-level backends (Databricks Unity
        # Catalog, BigQuery projects). Fires BEFORE scope finalization
        # so list_schemas / list_tables downstream are already
        # catalog-aware. Silent no-op for PG / Snowflake / single-
        # catalog Databricks.
        try:
            from amx.cli_support.catalog_picker import (
                ensure_catalog_selected,
                ensure_database_selected,
            )

            ensure_catalog_selected(db)
            # Database picker for 2-level backends (PostgreSQL /
            # Snowflake). Fires when the profile has ``database=""``
            # so list_schemas / list_tables target the user's actual
            # data instead of the ``postgres`` system DB fallback.
            # Silent no-op when a database is already pinned or the
            # backend uses catalogs.
            ensure_database_selected(db)
        except Exception:
            pass

        # ── Equivalence-class dedup choice (FIRST run-mode question) ─────────
        # Mirrors /metadata edit's binary mode-selector pattern: ask the
        # high-impact yes/no decision BEFORE any drill-down. In /run that
        # means dedup comes ahead of scope picker, coverage filter, and
        # review strategy — those are the analogue of /edit's "drill
        # down DB → schema → table → column" steps and follow the
        # mode-decision. Profile/LLM test/completion mode above are
        # infrastructure questions, not run-mode questions, so they
        # stay where they are.
        use_dedup_choice = ask_choice(
            "Equivalence-class deduplication?",
            ["dedup", "per-column"],
            default="dedup",
            descriptions={
                "dedup": (
                    "Group identical columns by (name + dtype family) across "
                    "tables; one LLM call per group, applied to every member. "
                    "Saves tokens on repeated columns (mandt, customer_id, "
                    "created_at, …). Recommended for wide schemas."
                ),
                "per-column": (
                    "Send each column to the LLM individually. Fine-grained, "
                    "but slow + expensive on SAP-style schemas where the same "
                    "column appears in dozens of tables."
                ),
            },
        )
        use_dedup = use_dedup_choice == "dedup"

        tables_arg = list(tables_pos) + list(table)
        with command_display(
            schema=schema or cfg.current_schema or "",
            table=(
                table[0] if table else (tables_pos[0] if tables_pos else cfg.current_table or "")
            ),
            mode="analyze-setup",
            provider=cfg.llm.provider,
            model=cfg.llm.model,
        ):
            scope = finalize_scope(cfg, db, schema, tables_arg)
            if scope is None:
                return

            total_assets = sum(len(v) for v in scope.values())

            # ── Comment-coverage filter ──────────────────────────────────
            # When the user picks Database / Schema / Asset scope on a DB
            # that already has SOME comments, they almost never want to
            # re-run the LLM on every column — they just want to fill the
            # gaps. Default to "missing-only"; let them opt in to a full
            # re-run when they explicitly want to overwrite.
            coverage_choice = ask_choice(
                "Run for which assets / columns?",
                ["missing-only", "all"],
                default="missing-only",
                descriptions={
                    "missing-only": (
                        "Skip tables and columns that already have a comment. "
                        "Fastest; safest default."
                    ),
                    "all": (
                        "Re-run on every selected asset and column. Existing "
                        "comments will be replaced after review."
                    ),
                },
            )
            missing_only = coverage_choice != "all"
            if missing_only:
                info("Filter: only assets / columns without an existing comment will be analyzed.")
            else:
                info(
                    "Filter: re-running on ALL selected assets (existing comments will be replaced)."
                )

            review_strategy = "individual"
            # Show the run-wide strategy prompt for any chat-mode run,
            # including single-asset scopes. The pre-2026-05-02 gate
            # (``total_assets > 1``) skipped this prompt entirely when
            # the user picked a single table — silently defaulting to
            # "individual" and hiding the ``auto-apply`` option for the
            # very flow it was built for: "I trust the agents, just
            # write the top suggestion to the DB and don't make me
            # click through a per-column review for one table." Batch
            # mode still skips because batch reviews everything at the
            # end regardless of strategy.
            if not use_batch:
                review_strategy = ask_choice(
                    "Review strategy",
                    ["individual", "deferred", "auto-apply"],
                    default="individual",
                    descriptions={
                        "individual": "Assess each asset (table) as it becomes ready",
                        "deferred": "Process everything first, then review all together at the end",
                        "auto-apply": (
                            "Skip human review — write the top LLM suggestion directly. "
                            "Fastest, but no chance to edit or reject. Use only when you trust "
                            "the agents (and ideally only with /run-apply on a non-prod DB)."
                        ),
                    },
                )
            if review_strategy == "auto-apply":
                if not apply:
                    warn(
                        "auto-apply selected but /run was used (without --apply). The top suggestions "
                        "will be marked accepted in the catalog, but nothing will be written to the DB. "
                        "Use /run-apply to actually persist the comments."
                    )
                else:
                    warn(
                        "auto-apply: every top suggestion will be written to the database without review. "
                        "Existing comments inside the chosen scope will be replaced."
                    )

            hs = history_store()
            if hs is not None:
                try:
                    # ``selected_count`` records what the user originally
                    # picked (pre missing-only filter) so /history shows the
                    # full intent. ``planned_count`` is the same for now and
                    # gets corrected to the post-filter number per-table when
                    # the orchestrator skips a fully-commented asset.
                    run_id = hs.create_run(
                        command="analyze.run",
                        mode=("batch" if use_batch else "chat"),
                        db_backend=cfg.db.backend,
                        db_profile=cfg.active_db_profile,
                        llm_provider=cfg.llm.provider,
                        llm_model=cfg.llm.model,
                        scope=scope,
                        selected_count=total_assets,
                        planned_count=total_assets,
                        review_strategy=review_strategy,
                        llm_profile=cfg.active_llm_profile,
                        doc_profile=cfg.active_doc_profile or None,
                        code_profile=cfg.active_code_profile or None,
                        # Snapshot every LLM/run knob the user can vary
                        # so /history compare can show exactly which
                        # settings differed between runs. Persisted as
                        # settings_json so adding new fields later
                        # doesn't need another schema migration.
                        settings={
                            "prompt_detail": getattr(cfg.llm, "prompt_detail", ""),
                            "language": getattr(cfg.llm, "language", ""),
                            "column_batch_size": int(getattr(cfg.llm, "column_batch_size", 0) or 0),
                            "batch_context_column_names": int(
                                getattr(cfg.llm, "batch_context_column_names", 0) or 0
                            ),
                            "n_alternatives": int(getattr(cfg.llm, "n_alternatives", 0) or 0),
                            "completion_mode": getattr(cfg.llm, "completion_mode", ""),
                            "description_verbosity": getattr(cfg.llm, "description_verbosity", ""),
                            "temperature": float(getattr(cfg.llm, "temperature", 0.0) or 0.0),
                            "max_tokens": int(getattr(cfg.llm, "max_tokens", 0) or 0),
                            "logprob_high": float(getattr(cfg.llm, "logprob_high", 0.0) or 0.0),
                            "logprob_medium": float(getattr(cfg.llm, "logprob_medium", 0.0) or 0.0),
                            "force_logprobs": bool(getattr(cfg.llm, "force_logprobs", True)),
                            "dedup_used": bool(use_dedup),
                            "missing_only": bool(missing_only),
                            "review_strategy": review_strategy,
                            "use_batch": bool(use_batch),
                        },
                    )
                except Exception as exc:
                    warn(f"History persistence disabled for this run: {exc}")

            total_schemas = len(scope)
            scope_summary = (
                f"{total_assets} asset(s) across {total_schemas} schema(s)"
                if total_schemas > 1
                else f"{total_assets} asset(s) in {next(iter(scope))}"
            )
            info(f"Scope: {scope_summary}")

        # ── Equivalence-class deduplication pass (Phase 2) ─────────────────
        # The dedup choice was made upfront (see ``use_dedup`` above,
        # asked alongside coverage + review_strategy). When opted in,
        # AMX pre-walks the scope, computes equivalence classes by
        # (column name, dtype family), and runs ONE LLM call per
        # multi-member class with all member tables in context.
        # Members that are dedup'd are added to
        # ``dedup_outcome.skip_set`` so the per-table flow below can
        # filter them out of the ProfileAgent batch. Singletons and
        # DIVERGES classes are left alone and flow through normally.
        # ``dedup_outcome`` is pre-initialised at the function top so
        # the cancellation handlers can read it without UnboundLocalError.
        if use_dedup:
            try:
                dedup_outcome = _maybe_run_equivalence_dedup(
                    cfg,
                    db,
                    llm,
                    scope=scope,
                    missing_only=missing_only,
                    apply=apply,
                    run_id=run_id,
                )
            except Exception as exc:
                warn(f"Equivalence dedup pass failed; continuing without dedup: {exc}")
                log.warning("Equivalence dedup pass failed: %s", exc, exc_info=True)
                dedup_outcome = None
        else:
            info("Equivalence dedup: opted out — every column will be profiled individually.")

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

        with command_display(
            schema=schema or cfg.current_schema or "",
            table=f"{total_assets} assets" if total_assets else "",
            mode="analyze-setup",
            provider=cfg.llm.provider,
            model=cfg.llm.model,
        ):
            code_report = resolve_codebase_for_run(cfg, db, scope, code_profile, code_refresh)
        token_tracker.reset()

        # Per-schema orchestration loop (extracted in v0.9.4). Returns
        # the accumulated results lists; mutates them as it goes so the
        # exception handlers below can still inspect partial progress.
        loop_result = run_per_schema_loop(
            cfg=cfg,
            db=db,
            llm=llm,
            scope=scope,
            rag_store=rag_store,
            code_report=code_report,
            run_id=run_id,
            use_batch=use_batch,
            missing_only=missing_only,
            review_strategy=review_strategy,
            dedup_outcome=dedup_outcome,
            total_assets=total_assets,
            total_schemas=total_schemas,
            history_store_fn=history_store,
        )
        all_results = loop_result.all_results
        processed_assets = loop_result.processed_assets
        skipped_assets = loop_result.skipped_assets
        orch = loop_result.last_orchestrator

        # Post-loop summary + apply branch (extracted in v0.9.4).
        # Returns approved/skipped lists for the finally block.
        approved, skipped = render_summary_and_apply(
            all_results=all_results,
            orch=orch,
            review_strategy=review_strategy,
            apply=apply,
            rag_store=rag_store,
            dedup_outcome=dedup_outcome,
            run_id=run_id,
            history_store_fn=history_store,
        )

        final_status = "success"
    except FatalLLMError as fatal:
        # Auth / quota / payment / model-not-found errors are NOT recoverable
        # — every queued batch fails the same way. Show the user one big,
        # specific message with what to do, then exit. Don't iterate the
        # remaining tables; don't pretend partial progress is review-ready.
        final_status = "failed"
        final_error_text = fatal.user_message
        warn("")
        error("LLM run aborted: " + fatal.user_message)
        if fatal.original_message and fatal.original_message != fatal.user_message:
            log.debug("Original LLM error: %s", fatal.original_message)
        info(
            "Some tables may have been partially processed before the abort. "
            "Fix the LLM problem above (top up credits / rotate the key / pick a "
            "different model under /llm), then re-run. AMX's missing-only filter "
            "skips the tables already finished so you don't pay twice."
        )
        log_event(
            event_type="analyze_run",
            status="failed",
            command="analyze.run",
            details={
                "mode": ("batch" if use_batch else "chat"),
                "error": "FatalLLMError",
                "error_message": fatal.user_message,
            },
        )
        return
    except KeyboardInterrupt:
        # Body extracted in v0.9.4 — see ``handle_keyboard_interrupt``.
        approved = [r for r in all_results if getattr(r, "applied", False)]
        skipped = [r for r in all_results if not getattr(r, "applied", False)]
        final_status, final_error_text = handle_keyboard_interrupt(
            all_results=all_results,
            review_strategy=review_strategy,
            use_batch=use_batch,
            run_id=run_id,
            history_store_fn=history_store,
            log_event=log_event,
        )
        return
    except Exception as exc:
        final_status = "failed"
        final_error_text = str(exc)
        log_event(
            event_type="analyze_run",
            status="failed",
            command="analyze.run",
            details={"error": str(exc), "mode": ("batch" if use_batch else "chat")},
        )
        raise
    finally:
        _finalize_history_run(
            run_id=run_id,
            final_status=final_status,
            run_started=run_started,
            total_assets=total_assets,
            total_schemas=total_schemas,
            processed_assets=processed_assets,
            skipped_assets=skipped_assets,
            approved=approved,
            skipped=skipped,
            apply=apply,
            all_results=all_results,
            final_error_text=final_error_text,
        )
        clear_request_id()


def register_analyze_run_command(
    analyze: click.Group,
    *,
    finalize_scope: FinalizeScope,
    resolve_codebase_for_run: ResolveCodebaseForRun,
    log_event: LogEvent,
) -> None:
    """Attach `/analyze run` to an existing analyze group."""

    @analyze.command("run")
    @click.argument("tables_pos", nargs=-1, metavar="[ASSET ...]")
    @click.option("--schema", "-s", help="Schema to analyze.")
    @click.option(
        "--table", "-t", multiple=True, help="Specific asset(s). Omit for interactive selection."
    )
    @click.option(
        "--apply/--no-apply", default=False, help="Apply approved metadata to the database."
    )
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
    @click.option(
        "--db-profile",
        "db_profile_override",
        multiple=True,
        help=(
            "Override the DB profile scope for this run. Pass multiple "
            "times to run the orchestrator once per profile, e.g. "
            "--db-profile prod_pg --db-profile analytics_bq. When "
            "omitted, the persisted scope (set by /use-db) is used."
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
        db_profile_override: tuple[str, ...],
    ) -> None:
        """Run all agents to infer metadata for selected assets (tables, views, etc.)."""
        from amx.db.connector import DatabaseConnector

        # 0.11.0: resolve the effective scope for this run.
        # Priority: --db-profile (CLI) > persisted active_db_profiles
        # > legacy single active_db_profile.
        if db_profile_override:
            unknown = [n for n in db_profile_override if n not in cfg.db_profiles]
            if unknown:
                error(
                    f"Unknown DB profile(s): {', '.join(unknown)}. "
                    f"Available: {', '.join(sorted(cfg.db_profiles)) or '(none)'}."
                )
                sys.exit(1)
            scope_names = []
            seen: set[str] = set()
            for name in db_profile_override:
                if name not in seen:
                    seen.add(name)
                    scope_names.append(name)
        else:
            scope_names = list(cfg.effective_db_profiles())
        if not scope_names:
            # Fall through to legacy behaviour: single active profile.
            scope_names = [cfg.active_db_profile or "default"]

        is_multi = len(scope_names) > 1
        if is_multi:
            info(f"Running analyze across {len(scope_names)} DB profiles: {', '.join(scope_names)}")

        # Save the persisted active pointer so we can restore it after
        # the loop. The orchestrator reads cfg.db / cfg.active_db_profile
        # at many points, so we temporarily switch the active profile
        # for each iteration and restore at the end.
        original_active = cfg.active_db_profile

        try:
            for idx, profile_name in enumerate(scope_names, start=1):
                if is_multi:
                    heading(f"Profile {idx}/{len(scope_names)}: {profile_name}")
                # Temporarily activate this profile so cfg.db and the
                # downstream orchestrator see the right DB. We do NOT
                # call cfg.save() inside the loop — the multi-profile
                # context is per-call, not persisted.
                if profile_name in cfg.db_profiles:
                    object.__setattr__(cfg, "active_db_profile", profile_name)
                    cfg.db = cfg.db_profiles[profile_name]

                db_init = DatabaseConnector(cfg.db)
                label = (
                    f"Testing {cfg.db.backend} connection to {cfg.db.display_summary} ..."
                    if cfg.db.display_summary
                    else f"Testing {cfg.db.backend} connection ..."
                )
                with step_spinner(label):
                    if not db_init.test_connection():
                        error(f"Cannot connect to database for profile '{profile_name}'.")
                        if is_multi:
                            warn(
                                "Skipping this profile and continuing with "
                                "the remaining profiles in scope."
                            )
                            continue
                        sys.exit(1)
                # 0.11.0: Phase 8 — flag unpinned-database profiles on
                # 2-level backends so the user knows up front the
                # subsequent listings may be empty.
                try:
                    from amx.cli_support.catalog_picker import warn_when_database_unpinned

                    warn_when_database_unpinned(db_init)
                except Exception:
                    pass

                execute_analyze_run(
                    cfg,
                    schema=schema,
                    table=table,
                    apply=apply,
                    mode=mode,
                    tables_pos=tables_pos,
                    db=db_init,
                    code_profile=code_profile,
                    code_refresh=code_refresh,
                    finalize_scope=finalize_scope,
                    resolve_codebase_for_run=resolve_codebase_for_run,
                    log_event=log_event,
                )
        except KeyboardInterrupt:
            warn("User interrupted process.")
            return
        except Exception as exc:
            raise click.ClickException(str(exc)) from exc
        finally:
            # Restore the original active pointer so subsequent commands
            # still see the user's persisted choice.
            if original_active and original_active in cfg.db_profiles:
                object.__setattr__(cfg, "active_db_profile", original_active)
                cfg.db = cfg.db_profiles[original_active]
