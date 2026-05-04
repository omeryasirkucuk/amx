"""Per-schema orchestration loop extracted from ``execute_analyze_run``.

This is the meat of an analyze run: for each schema in the scope, build
an :class:`Orchestrator`, walk the assets, call ``process_table`` or
``process_tables_batch_mode`` per asset, accumulate results, and emit
schema-level meta when there are multiple assets / schemas.

The historical inline body lived inside the 600-LOC
``execute_analyze_run`` and shared a dozen local variables with the
surrounding setup / summary code. Pulling it out into a standalone
function makes the loop independently testable and clears 110 LOC
from ``execute_analyze_run``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from amx.config import AMXConfig
from amx.db.connector import ProfilingError
from amx.services.analyze_scope import ScopeResult
from amx.utils.console import warn
from amx.utils.logging import get_logger

log = get_logger("cli.analyze_flow.run_loop")


@dataclass
class PerSchemaLoopResult:
    """What the per-schema loop accumulates for the caller.

    The function mutates these lists as it processes assets so the
    caller's exception handlers can report partial progress; on a
    clean run the caller reads the lists after :func:`run_per_schema_loop`
    returns.
    """

    all_results: list[Any] = field(default_factory=list)
    processed_assets: list[str] = field(default_factory=list)
    skipped_assets: list[str] = field(default_factory=list)
    filter_skipped_count: int = 0
    last_orchestrator: Any = None  # Most-recent Orchestrator instance —
    # the caller may need it for ``apply_results`` after the loop.


def run_per_schema_loop(
    *,
    cfg: AMXConfig,
    db: Any,
    llm: Any,
    scope: dict[str, list[str]],
    rag_store: Any,
    code_report: Any,
    run_id: int | None,
    use_batch: bool,
    missing_only: bool,
    review_strategy: str,
    dedup_outcome: Any,
    total_assets: int,
    total_schemas: int,
    history_store_fn: Any,
) -> PerSchemaLoopResult:
    """Run the per-schema orchestration loop.

    Returns a :class:`PerSchemaLoopResult` with the accumulated
    results lists; the caller is responsible for the post-loop summary
    rendering and the apply step.
    """
    from amx.agents.orchestrator import Orchestrator
    from amx.llm.provider import LLMProvider
    from amx.utils.live_display import get_display

    result = PerSchemaLoopResult()
    display = get_display()

    # Build the RAG-specific provider once per loop. Identity check vs
    # cfg.llm: effective_rag_llm() returns the same object when there
    # is no override, in which case we leave rag_llm=None so the
    # orchestrator falls back to the global llm.
    rag_cfg = cfg.effective_rag_llm()
    rag_llm = LLMProvider(rag_cfg) if rag_cfg is not cfg.llm else None

    for schema_name, assets in scope.items():
        asset_kinds = {name: db.resolve_asset_kind(schema_name, name) for name in assets}
        orch = Orchestrator(
            db,
            llm,
            rag_store=rag_store,
            code_report=code_report,
            run_id=run_id,
            search_profile=cfg.active_db_profile or "default",
            missing_only=missing_only,
            rag_llm=rag_llm,
        )
        if dedup_outcome is not None and dedup_outcome.skip_set:
            # Tell the orchestrator which (schema, table, column)
            # tuples were already handled by the dedup pass so it
            # filters them out of the ProfileAgent batch and doesn't
            # re-write descriptions for them.
            orch.dedup_skip_set = dedup_outcome.skip_set
        if isinstance(scope, ScopeResult) and scope.column_overrides:
            # When the scope was Column-level, hand the overrides to
            # the orchestrator so process_table restricts profile.columns
            # to just the chosen column(s) for the matching table.
            orch.column_overrides = scope.column_overrides

        result.last_orchestrator = orch

        display_label = ", ".join(assets) if len(assets) <= 3 else f"{len(assets)} assets"
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
                    schema_name,
                    list(assets),
                    asset_kinds=asset_kinds,
                )
                result.all_results.extend(results)
                result.processed_assets.extend(
                    [f"{schema_name}.{asset_name}" for asset_name in assets]
                )
            else:
                _process_assets_chat_mode(
                    orch=orch,
                    schema_name=schema_name,
                    assets=list(assets),
                    asset_kinds=asset_kinds,
                    review_strategy=review_strategy,
                    run_id=run_id,
                    total_assets=total_assets,
                    history_store_fn=history_store_fn,
                    result=result,
                    display=display,
                )

                if len(assets) > 1 or total_schemas > 1:
                    schema_meta = orch.process_schema_meta(
                        schema_name,
                        result.all_results,
                        auto_apply=(review_strategy == "auto-apply"),
                    )
                    result.all_results.extend(schema_meta)
        finally:
            display.stop()

    if total_schemas > 1 and result.last_orchestrator is not None:
        db_meta = result.last_orchestrator.process_database_meta(
            result.all_results,
            auto_apply=(review_strategy == "auto-apply"),
        )
        result.all_results.extend(db_meta)

    return result


def _process_assets_chat_mode(
    *,
    orch: Any,
    schema_name: str,
    assets: list[str],
    asset_kinds: dict[str, Any],
    review_strategy: str,
    run_id: int | None,
    total_assets: int,
    history_store_fn: Any,
    result: PerSchemaLoopResult,
    display: Any,
) -> None:
    """Walk ``assets`` in chat mode (one ``process_table`` call each).

    Carries the per-asset history-counter bumps that survived Ctrl+C
    in pre-v0.9.4: even if the user interrupts mid-loop, /history
    reflects what was actually processed and (for auto-apply) applied.
    """
    hs = history_store_fn()
    for asset_name in assets:
        display.set_context(table=asset_name)
        try:
            results = orch.process_table(
                schema_name,
                asset_name,
                asset_kind=asset_kinds.get(asset_name),
                interactive_review=(review_strategy == "individual"),
                auto_apply=(review_strategy == "auto-apply"),
            )
            result.all_results.extend(results)
            result.processed_assets.append(f"{schema_name}.{asset_name}")
            if hs is not None and run_id is not None:
                try:
                    if results:
                        # process_table returned suggestions — this asset
                        # truly went through agents (filter didn't skip it
                        # as fully-commented).
                        hs.increment_run_processed(run_id, by=1)
                        if review_strategy == "auto-apply":
                            applied_in_table = sum(1 for r in results if r.applied)
                            if applied_in_table:
                                hs.increment_run_applied(
                                    run_id,
                                    by=applied_in_table,
                                )
                    else:
                        # Filter dropped this asset (fully commented).
                        # Bump filter-skip tally and recompute
                        # planned_count = total - filter_skips so
                        # /history's Processed column shows
                        # processed/<remaining>.
                        result.filter_skipped_count += 1
                        hs.update_run_planned_count(
                            run_id,
                            max(0, total_assets - result.filter_skipped_count),
                        )
                except Exception as exc:
                    log.debug(
                        "Could not update analyze run counters for run_id=%s: %s",
                        run_id,
                        exc,
                    )
        except ProfilingError as exc:
            result.skipped_assets.append(f"{schema_name}.{asset_name}")
            warn(f"Skipping {schema_name}.{asset_name}: {exc}")
            continue


__all__ = ["PerSchemaLoopResult", "run_per_schema_loop"]
