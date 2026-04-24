"""Orchestrator: coordinate sub-agents, merge suggestions, and drive human-in-the-loop review."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from dataclasses import dataclass, field

from amx.agents.base import AgentContext, Confidence, MetadataSuggestion, apply_logprob_confidence
from amx.agents.code_agent import CodeAgent
from amx.agents.profile_agent import ProfileAgent
from amx.agents.rag_agent import RAGAgent
from amx.codebase.analyzer import CodebaseReport
from amx.db.connector import AssetKind, DatabaseConnector, TableProfile
from amx.docs.rag import RAGStore
from amx.llm.provider import LLMProvider
from amx.utils.console import (
    ask,
    ask_choice,
    confirm,
    console,
    error,
    heading,
    info,
    render_table,
    step_spinner,
    success,
    warn,
)
from amx.utils.logging import get_logger
from amx.utils.token_tracker import estimate_tokens, tracker

log = get_logger("agents.orchestrator")

MERGE_PROMPT = """\
You are merging metadata suggestions from multiple sources for database columns.

For each column below, multiple sources have proposed descriptions.
Produce a single best description that combines insights from all sources.

{columns_text}

Respond in this exact format for EACH column (one block per column):

COLUMN: <column_name>
BEST_DESCRIPTION: <merged description>
CONFIDENCE: <HIGH|MEDIUM|LOW>
REASONING: <why>
"""


@dataclass
class ReviewResult:
    schema: str
    table: str
    column: str | None
    final_description: str
    confidence: Confidence
    source: str
    applied: bool = False
    asset_kind: str = "table"
    result_id: int | None = None  # FK to run_results.id (for re-evaluation)


def apply_review_results_to_db(db: DatabaseConnector, results: list[ReviewResult]) -> int:
    """Write approved descriptions as COMMENT ON TABLE/VIEW/COLUMN to the database."""
    applied = 0
    for r in results:
        if not r.applied or not r.final_description:
            continue
        try:
            kind = AssetKind(r.asset_kind) if r.asset_kind else AssetKind.TABLE
        except ValueError:
            kind = AssetKind.TABLE
        try:
            if r.column is None:
                db.set_table_comment(r.schema, r.table, r.final_description, asset_kind=kind)
            else:
                db.set_column_comment(r.schema, r.table, r.column, r.final_description)
            applied += 1
        except Exception as exc:
            error(f"Failed to apply comment on {r.schema}.{r.table}.{r.column or ''}: {exc}")
    return applied


class Orchestrator:
    def __init__(
        self,
        db: DatabaseConnector,
        llm: LLMProvider,
        rag_store: RAGStore | None = None,
        code_report: CodebaseReport | None = None,
        run_id: int | None = None,
    ):
        self.db = db
        self.llm = llm
        self.run_id = run_id
        self.profile_agent = ProfileAgent(llm)
        self.rag_agent = RAGAgent(llm, rag_store) if rag_store else None
        self.code_agent = CodeAgent(llm, code_report) if code_report else None
        self.results: list[ReviewResult] = []

    def process_table(
        self,
        schema: str,
        table: str,
        asset_kind: AssetKind | None = None,
    ) -> list[ReviewResult]:
        kind_label = f" ({asset_kind.label})" if asset_kind and asset_kind != AssetKind.TABLE else ""
        heading(f"Analyzing {schema}.{table}{kind_label}")

        with step_spinner(f"Profiling {schema}.{table} structure and data"):
            profile = self.db.profile_table(schema, table, asset_kind=asset_kind)
        ctx = self._build_context(profile)

        num_cols = len(profile.columns)
        batch_size = self.profile_agent.BATCH_SIZE
        if num_cols > batch_size:
            n_batches = (num_cols + batch_size - 1) // batch_size
            info(
                f"Profile Agent: {num_cols} columns "
                f"({n_batches} batches of ≤{batch_size})"
            )
        else:
            info(f"Profile Agent: {num_cols} columns")
        if self.rag_agent:
            info(f"RAG Agent: {num_cols} columns to check against documents")
        if self.code_agent:
            info(f"Code Agent: {num_cols} columns to check against codebase")

        # Run all enabled agents in parallel in chat mode.
        all_suggestions = self._run_enabled_agents(ctx)

        merged = self._merge_suggestions(all_suggestions, ctx)
        if not merged:
            warn(
                "No metadata suggestions were produced for this table. "
                "If the model replied, the raw text may be in ~/.amx/logs/last_profile_agent_response.txt "
                "— see also ~/.amx/logs/amx.log"
            )
            return []

        # ── Persist all alternatives before human review ──────────────────
        result_id_map = self._save_merged_suggestions(merged, asset_kind=asset_kind)

        from amx.utils.live_display import get_display
        display = get_display()
        if display.is_active:
            display.pause()
        ak = profile.asset_kind.value if profile.asset_kind else "table"
        reviewed = self._human_review(merged, schema, table, asset_kind=ak, result_id_map=result_id_map)
        if display.is_active:
            display.resume()
        self.results.extend(reviewed)
        return reviewed

    def _run_enabled_agents(self, ctx: AgentContext) -> list[MetadataSuggestion]:
        jobs: list[tuple[str, object]] = [("profile", self.profile_agent)]
        if self.rag_agent:
            jobs.append(("rag", self.rag_agent))
        if self.code_agent:
            jobs.append(("code", self.code_agent))
        if not jobs:
            return []
        if len(jobs) == 1:
            label, agent = jobs[0]
            try:
                return agent.run(ctx)
            except Exception as exc:
                warn(f"{label.upper()} agent failed: {exc}")
                return []

        out: list[MetadataSuggestion] = []
        with ThreadPoolExecutor(max_workers=len(jobs)) as ex:
            fut_to_label = {
                ex.submit(agent.run, ctx): label
                for label, agent in jobs
            }
            for fut in as_completed(fut_to_label):
                label = fut_to_label[fut]
                try:
                    out.extend(fut.result() or [])
                except Exception as exc:
                    warn(f"{label.upper()} agent failed: {exc}")
        return out

    def _build_context(self, profile: TableProfile) -> AgentContext:
        db_name = self.db.cfg.database or self.db.cfg.project or self.db.cfg.catalog or "N/A"
        return AgentContext(
            schema=profile.schema,
            table=profile.name,
            asset_kind=profile.asset_kind.value,
            db_profile={
                "row_count": profile.row_count,
                "existing_comment": profile.existing_comment,
                "primary_key": profile.primary_key,
                "foreign_keys": profile.foreign_keys,
                "referenced_by": profile.referenced_by,
                "unique_constraints": profile.unique_constraints,
                "check_constraints": profile.check_constraints,
                "stats_seq_scan": profile.stats_seq_scan,
                "stats_idx_scan": profile.stats_idx_scan,
                "stats_n_live_tup": profile.stats_n_live_tup,
                "stats_source": self.db.stats_label,
                "schema_comment": profile.schema_comment,
                "database_comment": profile.database_comment,
                "related_comments": profile.related_comments,
                "columns": [
                    {
                        "name": c.name,
                        "dtype": c.dtype,
                        "nullable": c.nullable,
                        "row_count": c.row_count,
                        "null_count": c.null_count,
                        "distinct_count": c.distinct_count,
                        "cardinality_ratio": c.cardinality_ratio,
                        "min_val": c.min_val,
                        "max_val": c.max_val,
                        "samples": c.samples,
                        "existing_comment": c.existing_comment,
                    }
                    for c in profile.columns
                ],
            },
            existing_metadata={
                "database": db_name,
                "backend": self.db.backend,
                "table_comment": profile.existing_comment,
                "schema_comment": profile.schema_comment,
                "database_comment": profile.database_comment,
            },
        )

    def _merge_suggestions(
        self, suggestions: list[MetadataSuggestion], ctx: AgentContext
    ) -> list[MetadataSuggestion]:
        by_column: dict[str | None, list[MetadataSuggestion]] = defaultdict(list)
        for s in suggestions:
            by_column[s.column].append(s)

        merged: list[MetadataSuggestion] = []
        needs_merge: dict[str | None, list[MetadataSuggestion]] = {}

        for col_name, col_suggestions in by_column.items():
            if len(col_suggestions) == 1:
                merged.append(col_suggestions[0])
            else:
                needs_merge[col_name] = col_suggestions

        if not needs_merge:
            return merged

        columns_blocks: list[str] = []
        for col_name, col_suggestions in needs_merge.items():
            label = col_name or "(table-level)"
            source_text = "\n".join(
                f"  [{s.source}] (confidence={s.confidence.value}): "
                f"{', '.join(s.suggestions)}\n    reasoning: {s.reasoning}"
                for s in col_suggestions
            )
            columns_blocks.append(f"### {label}\n{source_text}")

        columns_text = "\n\n".join(columns_blocks)
        messages = [
            {"role": "user", "content": MERGE_PROMPT.format(columns_text=columns_text)},
        ]
        est = estimate_tokens(messages)
        with step_spinner(
            f"Merging suggestions: {len(needs_merge)} columns", token_estimate=est
        ):
            result = self.llm.chat(messages)
        tracker.record("merge", est, result.usage)

        parsed = self._parse_merge_response(result.content)

        merge_results: list[MetadataSuggestion] = []
        for col_name, col_suggestions in needs_merge.items():
            key = col_name or "(table-level)"
            best, conf, reasoning = parsed.get(key, ("", Confidence.MEDIUM, ""))

            all_descs = [best] if best else []
            for s in col_suggestions:
                for d in s.suggestions:
                    if d not in all_descs:
                        all_descs.append(d)

            merge_results.append(MetadataSuggestion(
                schema=ctx.schema,
                table=ctx.table,
                column=col_name,
                suggestions=all_descs[:5],
                confidence=conf,
                reasoning=reasoning,
                source="combined",
            ))

        merged.extend(apply_logprob_confidence(merge_results, result.logprobs))
        return merged

    # ── Persistence helpers ───────────────────────────────────────────────────

    def _save_merged_suggestions(
        self,
        suggestions: list[MetadataSuggestion],
        *,
        asset_kind: str = "table",
    ) -> dict[str | None, int]:
        """Save all LLM alternatives to run_results before user review.

        Returns {column_name: run_result_id} map so evaluations can be linked.
        """
        from amx.storage.sqlite_store import history_store

        hs = history_store()
        if hs is None or self.run_id is None:
            return {}
        rows = [
            {
                "schema": s.schema,
                "table": s.table,
                "column": s.column,
                "asset_kind": asset_kind,
                "source": s.source,
                "confidence": s.confidence.value,
                "reasoning": s.reasoning,
                "alternatives": s.suggestions,
            }
            for s in suggestions
        ]
        try:
            ids = hs.save_run_results(self.run_id, rows)
        except Exception as exc:
            log.warning("Could not persist run_results: %s", exc)
            return {}
        # Map column_name → DB row id  (column=None → key None)
        return {
            s.column: rid
            for s, rid in zip(suggestions, ids)
        }

    def _record_evaluation(
        self,
        result_id: int | None,
        *,
        chosen_description: str,
        evaluation: str,
    ) -> None:
        if result_id is None:
            return
        from amx.storage.sqlite_store import history_store

        hs = history_store()
        if hs is None:
            return
        try:
            hs.record_evaluation(
                result_id,
                chosen_description=chosen_description,
                evaluation=evaluation,
            )
        except Exception as exc:
            log.debug("Could not record evaluation: %s", exc)

    @staticmethod
    def _parse_merge_response(
        text: str,
    ) -> dict[str, tuple[str, Confidence, str]]:
        """Parse batched merge response into {column: (description, confidence, reasoning)}."""
        results: dict[str, tuple[str, Confidence, str]] = {}
        current_col = ""
        best = ""
        conf = Confidence.MEDIUM
        reasoning = ""

        for line in text.splitlines():
            line = line.strip()
            if line.startswith("COLUMN:"):
                if current_col and best:
                    results[current_col] = (best, conf, reasoning)
                current_col = line.split(":", 1)[1].strip()
                best = ""
                conf = Confidence.MEDIUM
                reasoning = ""
            elif line.startswith("BEST_DESCRIPTION:"):
                best = line.split(":", 1)[1].strip()
            elif line.startswith("CONFIDENCE:"):
                raw = line.split(":", 1)[1].strip().upper()
                conf = Confidence[raw] if raw in Confidence.__members__ else Confidence.MEDIUM
            elif line.startswith("REASONING:"):
                reasoning = line.split(":", 1)[1].strip()

        if current_col and best:
            results[current_col] = (best, conf, reasoning)

        return results

    def _human_review(
        self,
        suggestions: list[MetadataSuggestion],
        schema: str,
        table: str,
        asset_kind: str = "table",
        result_id_map: dict[str | None, int] | None = None,
    ) -> list[ReviewResult]:
        results: list[ReviewResult] = []
        result_id_map = result_id_map or {}

        table_suggestions = [s for s in suggestions if s.column is None]
        col_suggestions = [s for s in suggestions if s.column is not None]

        for s in table_suggestions:
            rid = result_id_map.get(s.column)  # column is None here
            result = self._review_single(s, is_table=True, asset_kind=asset_kind, result_id=rid)
            results.append(result)

        if col_suggestions:
            col_count = len(col_suggestions)
            noun = "column" if col_count == 1 else "columns"
            heading(f"Column descriptions for {schema}.{table} ({col_count} {noun})")
            rows = []
            for s in col_suggestions:
                rows.append([
                    s.column,
                    s.suggestions[0] if s.suggestions else "N/A",
                    s.confidence.value,
                    s.source,
                ])
            render_table(
                "Suggested descriptions",
                ["Column", "Best Suggestion", "Confidence", "Source"],
                rows,
            )
            console.print()

            review_mode = ask_choice(
                "How would you like to review?",
                ["one-by-one", "accept-all-high", "accept-all", "reject-all"],
                default="one-by-one",
            )

            for s in col_suggestions:
                rid = result_id_map.get(s.column)
                if review_mode == "accept-all":
                    rr = ReviewResult(
                        schema=s.schema, table=s.table, column=s.column,
                        final_description=s.suggestions[0],
                        confidence=s.confidence, source=s.source, applied=True,
                        asset_kind=asset_kind, result_id=rid,
                    )
                    self._record_evaluation(rid, chosen_description=s.suggestions[0], evaluation="accepted")
                    results.append(rr)
                elif review_mode == "accept-all-high" and s.confidence == Confidence.HIGH:
                    rr = ReviewResult(
                        schema=s.schema, table=s.table, column=s.column,
                        final_description=s.suggestions[0],
                        confidence=s.confidence, source=s.source, applied=True,
                        asset_kind=asset_kind, result_id=rid,
                    )
                    self._record_evaluation(rid, chosen_description=s.suggestions[0], evaluation="accepted")
                    results.append(rr)
                elif review_mode == "accept-all-high" and s.confidence != Confidence.HIGH:
                    rr = ReviewResult(
                        schema=s.schema, table=s.table, column=s.column,
                        final_description="",
                        confidence=s.confidence, source=s.source, applied=False,
                        asset_kind=asset_kind, result_id=rid,
                    )
                    self._record_evaluation(rid, chosen_description="", evaluation="skipped")
                    results.append(rr)
                elif review_mode == "reject-all":
                    rr = ReviewResult(
                        schema=s.schema, table=s.table, column=s.column,
                        final_description="",
                        confidence=s.confidence, source=s.source, applied=False,
                        asset_kind=asset_kind, result_id=rid,
                    )
                    self._record_evaluation(rid, chosen_description="", evaluation="skipped")
                    results.append(rr)
                else:
                    result = self._review_single(s, is_table=False, asset_kind=asset_kind, result_id=rid)
                    results.append(result)

        return results

    def _review_single(
        self,
        s: MetadataSuggestion,
        is_table: bool,
        asset_kind: str = "table",
        result_id: int | None = None,
    ) -> ReviewResult:
        kind_label = asset_kind.replace("_", " ").title() if is_table else "Column"
        asset = f"{kind_label}: {s.schema}.{s.table}" if is_table else f"Column: {s.table}.{s.column}"
        console.print(f"\n  [heading]{asset}[/heading]")
        console.print(f"  Confidence: [{'success' if s.confidence == Confidence.HIGH else 'warning'}]{s.confidence.value}[/]")
        console.print(f"  Source: {s.source}")
        console.print(f"  Reasoning: {s.reasoning}")
        console.print()

        options = list(s.suggestions) + ["Other (type your own)", "Skip"]
        choice = ask_choice("Select a description", options, default=options[0])

        if choice == "Skip":
            self._record_evaluation(result_id, chosen_description="", evaluation="skipped")
            return ReviewResult(
                schema=s.schema, table=s.table, column=s.column,
                final_description="", confidence=s.confidence,
                source=s.source, applied=False, asset_kind=asset_kind,
                result_id=result_id,
            )
        elif choice == "Other (type your own)":
            custom = ask("Enter your description")
            self._record_evaluation(result_id, chosen_description=custom, evaluation="custom")
            return ReviewResult(
                schema=s.schema, table=s.table, column=s.column,
                final_description=custom, confidence=Confidence.HIGH,
                source="human", applied=True, asset_kind=asset_kind,
                result_id=result_id,
            )
        else:
            self._record_evaluation(result_id, chosen_description=choice, evaluation="accepted")
            return ReviewResult(
                schema=s.schema, table=s.table, column=s.column,
                final_description=choice, confidence=s.confidence,
                source=s.source, applied=True, asset_kind=asset_kind,
                result_id=result_id,
            )

    # ── Batch mode ────────────────────────────────────────────────────────────

    def process_tables_batch_mode(
        self,
        schema: str,
        tables: list[str],
        asset_kinds: dict[str, AssetKind] | None = None,
    ) -> list[ReviewResult]:
        """Run the full pipeline for *tables* via the provider's Batch API.

        Falls back to Chat Completions if the provider has no batch support.
        """
        from amx.llm.batch import BatchRequest, run_batch, supported_providers

        asset_kinds = asset_kinds or {}

        if not self.llm.supports_batch:
            warn(
                f"Provider '{self.llm.cfg.provider}' does not support batch mode "
                f"(supported: {', '.join(supported_providers())}). "
                "Falling back to Chat Completions."
            )
            all_results: list[ReviewResult] = []
            for table in tables:
                all_results.extend(
                    self.process_table(schema, table, asset_kind=asset_kinds.get(table))
                )
            return all_results

        n_assets = len(tables)
        info(f"[Batch] Profiling {n_assets} asset(s)…")
        profiles: dict[str, "TableProfile"] = {}
        for table in tables:
            ak = asset_kinds.get(table)
            with step_spinner(f"Profiling {schema}.{table}"):
                profiles[table] = self.db.profile_table(schema, table, asset_kind=ak)

        all_requests: list[BatchRequest] = []
        ctx_map: dict[str, "AgentContext"] = {}

        for table in tables:
            ctx = self._build_context(profiles[table])
            ctx_map[table] = ctx

            all_requests.extend(self.profile_agent.collect_messages(ctx))
            if self.rag_agent:
                all_requests.extend(self.rag_agent.collect_messages(ctx))
            if self.code_agent:
                all_requests.extend(self.code_agent.collect_messages(ctx))

        if not all_requests:
            warn("No LLM requests to submit — all agents had nothing to process.")
            return []

        info(
            f"[Batch] Submitting {len(all_requests)} request(s) for "
            f"{n_assets} asset(s)…"
        )
        batch_results = run_batch(all_requests, self.llm.cfg)

        all_reviewed: list[ReviewResult] = []

        for table in tables:
            heading(f"Processing results: {schema}.{table}")
            ctx = ctx_map[table]
            profile = profiles[table]
            ak = profile.asset_kind.value if profile.asset_kind else "table"

            num_cols = len(profile.columns)
            batch_size = self.profile_agent.BATCH_SIZE
            n_batches = (num_cols + batch_size - 1) // batch_size

            all_suggestions: list[MetadataSuggestion] = []

            for idx in range(n_batches):
                cid = f"profile:{schema}:{table}:{idx}"
                chat_result = batch_results.get(cid)
                if chat_result and chat_result.content:
                    cols_slice = profile.columns[idx * batch_size : (idx + 1) * batch_size]
                    col_dicts = [
                        {"name": c.name, "dtype": c.dtype, "nullable": c.nullable,
                         "row_count": c.row_count, "null_count": c.null_count,
                         "distinct_count": c.distinct_count, "samples": c.samples}
                        for c in cols_slice
                    ]
                    batch_ctx = self.profile_agent._ctx_with_columns(ctx, col_dicts)
                    tracker.record("profile_agent(batch)", 0, chat_result.usage)
                    all_suggestions.extend(
                        self.profile_agent.parse_batch_result(chat_result.content, batch_ctx)
                    )

            if self.rag_agent:
                cid = f"rag:{schema}:{table}"
                chat_result = batch_results.get(cid)
                if chat_result and chat_result.content:
                    tracker.record("rag_agent(batch)", 0, chat_result.usage)
                    all_suggestions.extend(
                        self.rag_agent.parse_batch_result(chat_result.content, ctx)
                    )

            if self.code_agent:
                cid = f"code:{schema}:{table}"
                chat_result = batch_results.get(cid)
                if chat_result and chat_result.content:
                    tracker.record("code_agent(batch)", 0, chat_result.usage)
                    all_suggestions.extend(
                        self.code_agent.parse_batch_result(chat_result.content, ctx)
                    )

            if not all_suggestions:
                warn(f"No suggestions for {schema}.{table} after parsing batch results.")
                continue

            merged = self._merge_suggestions(all_suggestions, ctx)
            if not merged:
                warn(f"Merge produced no output for {schema}.{table}.")
                continue

            result_id_map = self._save_merged_suggestions(merged, asset_kind=ak)
            reviewed = self._human_review(merged, schema, table, asset_kind=ak, result_id_map=result_id_map)
            self.results.extend(reviewed)
            all_reviewed.extend(reviewed)

        return all_reviewed

    # ── Apply ────────────────────────────────────────────────────────────────

    def apply_results(self, results: list[ReviewResult] | None = None) -> int:
        results = results or self.results
        applied = apply_review_results_to_db(self.db, results)
        success(f"Applied {applied} metadata comments to the database")
        return applied
