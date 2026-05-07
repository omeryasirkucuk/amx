"""Per-table processing flow extracted from ``Orchestrator.process_table``.

The historical ``Orchestrator.process_table`` was a 281-line god-method
with four overlapping filter chains (missing-only, column-scope,
dedup-skip), an agent loop, and three apply branches (auto-apply,
deferred, interactive-review). Each new feature landed inside the
same method, growing it linearly. v0.9.2 extracts that flow into
:class:`TableProcessor` so each phase is independently testable and
the orchestrator's public method becomes a 4-line delegator.

The class is stateful — it carries the per-table arguments
(``schema``, ``table``, ``asset_kind``, ``auto_apply``,
``interactive_review``) and a reference back to the parent
``Orchestrator`` from which it reads the agent registry, filter sets,
and persistence helpers. Public surface is just
:meth:`TableProcessor.run`; everything else is a private phase.
"""

from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING, Any

from amx.db.connector import AssetKind
from amx.utils.console import (
    error,
    heading,
    info,
    step_spinner,
    success,
    warn,
)
from amx.utils.logging import get_logger

if TYPE_CHECKING:
    from amx.agents.orchestrator import Orchestrator, ReviewResult
    from amx.db.connector import TableProfile

log = get_logger("agents.orchestrator.table_processor")


class TableProcessor:
    """Run the full per-table flow for one ``(schema, table)`` pair.

    The flow has four phases, each as its own method:

    * :meth:`_fetch_profile` — read structure + samples from live DB.
    * :meth:`_apply_filters` — narrow ``profile.columns`` according to
      missing-only / column-scope / dedup-skip; return ``False`` if
      nothing remains to do.
    * :meth:`_run_agents_and_persist` — orchestrate Profile / RAG /
      Code agents, merge their suggestions, and persist the candidates
      so review can link to them. Returns ``(merged, result_id_map)``
      or ``(None, None)`` if no suggestions came back.
    * :meth:`_dispatch_apply_or_review` — auto-apply / deferred /
      interactive-review depending on the run-mode arguments.

    Each phase is a short method that reads from / writes to the
    ``Orchestrator`` reference so the existing helpers (``_build_context``,
    ``_merge_suggestions``, ``_ensure_complete_table_coverage``,
    ``_save_merged_suggestions``, ``_sync_search_catalog``,
    ``_record_applied_state``, ``_human_review``) stay where they are.
    """

    def __init__(
        self,
        orch: Orchestrator,
        schema: str,
        table: str,
        *,
        asset_kind: AssetKind | None = None,
        auto_apply: bool = False,
        interactive_review: bool = True,
        cancel_token: threading.Event | None = None,
    ) -> None:
        self.orch = orch
        self.schema = schema
        self.table = table
        self.asset_kind = asset_kind
        self.auto_apply = auto_apply
        self.interactive_review = interactive_review
        self.cancel_token = cancel_token
        # Populated by ``_run_agents_and_persist`` once the agent fan-out
        # returns. Maps sub-agent label → "ok" / "failed" / "cancelled" /
        # "skipped". Read by Studio SSE / run history when present.
        self.last_agent_statuses: dict[str, str] = {}

    # ── Cancellation ───────────────────────────────────────────────────

    def _check_cancel(self, *, phase: str) -> None:
        """Raise :class:`RunCancelled` if the cancel token has been set.

        Called at phase boundaries (profile fetch, filter chain, agent
        run, apply / review dispatch). When ``self.cancel_token`` is
        ``None`` (CLI invocation, programmatic call without a Studio
        job) this is a no-op, so existing call sites are unaffected.

        ``phase`` is included in the exception message so the SSE
        consumer / log can show which boundary observed the cancel.
        """
        if self.cancel_token is not None and self.cancel_token.is_set():
            from amx.agents.orchestrator import RunCancelled

            raise RunCancelled(f"Cancelled before {phase} on {self.schema}.{self.table}")

    # ── Public entry point ─────────────────────────────────────────────

    def run(self) -> list[ReviewResult]:
        """Execute the full per-table flow and return review results."""
        kind_label = (
            f" ({self.asset_kind.label})"
            if self.asset_kind and self.asset_kind != AssetKind.TABLE
            else ""
        )
        heading(f"Analyzing {self.schema}.{self.table}{kind_label}")

        self._check_cancel(phase="profile_fetch")
        profile = self._fetch_profile()

        self._check_cancel(phase="filters")
        if not self._apply_filters(profile):
            return []

        self._check_cancel(phase="agents")
        merged, result_id_map = self._run_agents_and_persist(profile)
        if merged is None:
            return []

        self._check_cancel(phase="apply_or_review")
        return self._dispatch_apply_or_review(profile, merged, result_id_map)

    # ── Phase 1: profile fetch ─────────────────────────────────────────

    def _fetch_profile(self) -> TableProfile:
        with step_spinner(f"Profiling {self.schema}.{self.table} structure and data"):
            return self.orch.db.profile_table(
                self.schema,
                self.table,
                asset_kind=self.asset_kind,
            )

    # ── Phase 2: filter chain ──────────────────────────────────────────

    def _apply_filters(self, profile: TableProfile) -> bool:
        """Run all three filters in order; return False if work is empty."""
        if not self._filter_missing_only(profile):
            return False
        if not self._filter_column_override(profile):
            return False
        return self._filter_dedup_skip(profile)

    def _filter_missing_only(self, profile: TableProfile) -> bool:
        """Skip already-commented columns when ``missing_only`` is set.

        Placeholder comments left over from previous runs (the
        "Auto-inference missed a reliable description; please review
        manually." string) are treated as MISSING so the user can
        re-analyse them by simply re-running with missing-only. This
        is how legacy DBs polluted before the v0.6.3 write-back fix get
        organically cleaned up.
        """
        from amx.agents.orchestrator import is_placeholder_description

        if not self.orch.missing_only:
            return True

        total_cols = len(profile.columns)
        cols_missing = [
            c
            for c in profile.columns
            if not (c.existing_comment or "").strip()
            or is_placeholder_description(c.existing_comment)
        ]
        table_has_comment = bool(
            (profile.existing_comment or "").strip()
            and not is_placeholder_description(profile.existing_comment)
        )

        if table_has_comment and not cols_missing:
            info(
                f"Skipping {self.schema}.{self.table}: already has a table comment "
                f"and all {total_cols} column(s) commented (missing-only filter)."
            )
            return False

        if cols_missing and len(cols_missing) < total_cols:
            skipped = total_cols - len(cols_missing)
            info(
                f"Filtering {self.schema}.{self.table}: {skipped}/{total_cols} columns "
                f"already have comments — analyzing only the {len(cols_missing)} missing one(s)."
            )
            profile.columns = cols_missing
        elif not cols_missing and not table_has_comment:
            info(
                f"{self.schema}.{self.table}: every column has a comment but the table "
                "comment is missing — analyzing the table-level description only."
            )
            profile.columns = []

        return True

    def _filter_column_override(self, profile: TableProfile) -> bool:
        """Restrict to the column-scope override set if present.

        When the user picks Column scope, the resolver populates
        ``column_overrides[(schema, table)] = {col1, ...}``. Restrict
        ``profile.columns`` to those names; nothing else (table comment,
        other columns) gets re-inferred for this run.
        """
        column_override_set = self.orch.column_overrides.get((self.schema, self.table))
        if column_override_set is None:
            return True

        before = len(profile.columns)
        profile.columns = [col for col in profile.columns if col.name in column_override_set]
        removed = before - len(profile.columns)
        if removed:
            info(
                f"Column scope: restricting {self.schema}.{self.table} to "
                f"{len(profile.columns)} column(s) "
                f"({', '.join(c.name for c in profile.columns)}); "
                f"{removed} other column(s) skipped."
            )
        if not profile.columns:
            warn(
                f"Column scope: no matching columns on {self.schema}.{self.table} "
                f"(asked for {sorted(column_override_set)}). Skipping."
            )
            return False
        return True

    def _filter_dedup_skip(self, profile: TableProfile) -> bool:
        """Drop columns already handled by the upfront equivalence pass.

        The dedup pass already wrote the description to the catalog
        and, when in apply mode, to the live DB — so this orchestrator
        must NOT spend tokens re-profiling them. Singletons and
        DIVERGES classes are not in the skip set and flow through
        normally.
        """
        if not self.orch.dedup_skip_set:
            return True

        before = len(profile.columns)
        profile.columns = [
            col
            for col in profile.columns
            if (self.schema, self.table, col.name) not in self.orch.dedup_skip_set
        ]
        removed = before - len(profile.columns)
        if removed:
            info(
                f"Equivalence skip: {removed}/{before} column(s) on "
                f"{self.schema}.{self.table} were already handled by the dedup pass."
            )
        if not profile.columns and not (
            profile.existing_comment is None or not profile.existing_comment.strip()
        ):
            info(
                f"{self.schema}.{self.table}: all columns covered by dedup pass and "
                "table comment already present; skipping."
            )
            return False
        return True

    # ── Phase 3: agent loop + persistence ──────────────────────────────

    def _run_agents_and_persist(
        self,
        profile: TableProfile,
    ) -> tuple[list[Any] | None, dict[str | None, int] | None]:
        """Run Profile / RAG / Code agents, merge, persist candidates.

        Returns ``(merged_suggestions, result_id_map)`` on success or
        ``(None, None)`` if no suggestions were produced.
        """
        ctx = self.orch._build_context(profile)

        num_cols = len(profile.columns)
        batch_size = self.orch.profile_agent.batch_size
        if num_cols > batch_size:
            n_batches = (num_cols + batch_size - 1) // batch_size
            info(f"Profile Agent: {num_cols} columns ({n_batches} batches of ≤{batch_size})")
        else:
            info(f"Profile Agent: {num_cols} columns")
        if self.orch.rag_agent:
            info(f"RAG Agent: {num_cols} columns to check against documents")
        if self.orch.code_agent:
            info(f"Code Agent: {num_cols} columns to check against codebase")

        t0 = time.monotonic()
        all_suggestions, agent_statuses = self.orch._run_enabled_agents(
            ctx, cancel_token=self.cancel_token
        )
        t1 = time.monotonic()
        info(f"Agent processing took {t1 - t0:.1f}s")
        # Stash per-agent statuses on the processor so callers (Studio
        # SSE, run history persistence) can pivot on them without
        # threading another return tuple through this layer. Keeping
        # the field optional means downstream code can continue to
        # treat ``_run_agents_and_persist`` as a two-tuple.
        self.last_agent_statuses = agent_statuses

        profile_diagnostics = self.orch.profile_agent.consume_diagnostics()
        for message in dict.fromkeys(profile_diagnostics):
            warn(message)

        merged = self.orch._merge_suggestions(all_suggestions, ctx)
        if not merged:
            warn(
                "No metadata suggestions were produced for this table. "
                "If the model replied, the raw text may be in ~/.amx/logs/last_profile_agent_response.txt "
                "— see also ~/.amx/logs/amx.log"
            )
            return None, None
        merged = self.orch._ensure_complete_table_coverage(profile, merged)

        # Persist all alternatives before human review so review picks
        # can link to a concrete run_results row.
        result_id_map = self.orch._save_merged_suggestions(
            merged,
            asset_kind=self.asset_kind,
        )
        self.orch._sync_search_catalog(profile, merged, result_id_map)
        return merged, result_id_map

    # ── Phase 4: apply / review dispatch ───────────────────────────────

    def _dispatch_apply_or_review(
        self,
        profile: TableProfile,
        merged: list[Any],
        result_id_map: dict[str | None, int],
    ) -> list[ReviewResult]:
        ak = profile.asset_kind.value if profile.asset_kind else "table"
        if self.auto_apply:
            return self._auto_apply_branch(merged, result_id_map, ak)
        if not self.interactive_review:
            return self._deferred_branch(merged, result_id_map, ak)
        return self._interactive_review_branch(merged, result_id_map, ak)

    def _auto_apply_branch(
        self,
        merged: list[Any],
        result_id_map: dict[str | None, int],
        ak: str,
    ) -> list[ReviewResult]:
        """Trust the agents — accept top suggestion, write to live DB.

        We still go through ``sync_review_decision`` per pick so the
        catalog records this as a "reviewed" description (chosen by
        auto-apply rather than a human) — that keeps the audit trail
        consistent with what the manual flow produces.
        """
        from amx.agents.orchestrator import ReviewResult, apply_review_results_to_db
        from amx.search.catalog import SearchCatalog
        from amx.storage.sqlite_store import history_store

        results: list[ReviewResult] = []
        for s in merged:
            top = s.suggestions[0] if s.suggestions else ""
            rid = result_id_map.get(s.column)
            if rid is not None and top:
                try:
                    catalog = SearchCatalog.from_history_store()
                    if catalog is not None:
                        catalog.sync_review_decision(
                            rid,
                            chosen_description=top,
                            evaluation="accepted",
                        )
                except Exception as exc:
                    log.debug("auto-apply: catalog sync_review_decision failed: %s", exc)
            hs = history_store()
            if hs is not None and rid is not None and top:
                try:
                    hs.record_evaluation(
                        rid,
                        chosen_description=top,
                        evaluation="accepted",
                    )
                except Exception as exc:
                    log.debug("auto-apply: record_evaluation failed: %s", exc)
            results.append(
                ReviewResult(
                    schema=s.schema,
                    table=s.table,
                    column=s.column,
                    final_description=top,
                    confidence=s.confidence,
                    source=s.source,
                    applied=True,
                    asset_kind=ak,
                    result_id=rid,
                    logprob_score=s.logprob_score,
                )
            )

        self.orch.results.extend(results)

        # Persist this table's accepted descriptions to the live DB
        # NOW — not at the end of the run. Without this, a Ctrl+C
        # mid-loop leaves the catalog "applied" but live DB empty for
        # every table that finished before the interrupt.
        try:
            with step_spinner(f"Writing {self.schema}.{self.table} comments to the database"):
                written = apply_review_results_to_db(
                    self.orch.db,
                    results,
                    on_applied=lambda r: self.orch._record_applied_state(r),
                )
            if written:
                success(
                    f"Auto-applied {written} comment(s) to {self.schema}.{self.table} (live DB)."
                )
        except Exception as exc:
            error(f"Failed to auto-apply {self.schema}.{self.table} comments to the live DB: {exc}")
            log.error(
                "auto-apply DB write failed for %s.%s: %s",
                self.schema,
                self.table,
                exc,
            )
        return results

    def _deferred_branch(
        self,
        merged: list[Any],
        result_id_map: dict[str | None, int],
        ak: str,
    ) -> list[ReviewResult]:
        """Wrap suggestions as un-applied ReviewResults for batch review."""
        from amx.agents.orchestrator import ReviewResult

        results: list[ReviewResult] = []
        for s in merged:
            results.append(
                ReviewResult(
                    schema=s.schema,
                    table=s.table,
                    column=s.column,
                    final_description=s.suggestions[0] if s.suggestions else "",
                    confidence=s.confidence,
                    source=s.source,
                    applied=False,
                    asset_kind=ak,
                    result_id=result_id_map.get(s.column),
                    logprob_score=s.logprob_score,
                )
            )
        self.orch.results.extend(results)
        return results

    def _interactive_review_branch(
        self,
        merged: list[Any],
        result_id_map: dict[str | None, int],
        ak: str,
    ) -> list[ReviewResult]:
        """Run the interactive per-table review picker.

        Pauses the live display while the prompt is active so
        keystrokes echo correctly, then resumes it afterwards.
        """
        from amx.utils.live_display import get_display

        display = get_display()
        if display.is_active:
            display.pause()

        t0 = time.monotonic()
        reviewed = self.orch._human_review(
            merged,
            self.schema,
            self.table,
            asset_kind=ak,
            result_id_map=result_id_map,
        )
        t1 = time.monotonic()
        info(f"Human review took {t1 - t0:.1f}s")

        if display.is_active:
            display.resume()

        self.orch.results.extend(reviewed)
        return reviewed


__all__ = ["TableProcessor"]
