"""Orchestrator: coordinate sub-agents, merge suggestions, and drive human-in-the-loop review."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field

from amx.agents.base import AgentContext, Confidence, MetadataSuggestion
from amx.agents.code_agent import CodeAgent
from amx.agents.profile_agent import ProfileAgent
from amx.agents.rag_agent import RAGAgent
from amx.codebase.analyzer import CodebaseReport
from amx.db.connector import DatabaseConnector, TableProfile
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


def apply_review_results_to_db(db: DatabaseConnector, results: list[ReviewResult]) -> int:
    """Write approved descriptions as COMMENT ON TABLE/COLUMN to the database."""
    applied = 0
    for r in results:
        if not r.applied or not r.final_description:
            continue
        try:
            if r.column is None:
                db.set_table_comment(r.schema, r.table, r.final_description)
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
    ):
        self.db = db
        self.llm = llm
        self.profile_agent = ProfileAgent(llm)
        self.rag_agent = RAGAgent(llm, rag_store) if rag_store else None
        self.code_agent = CodeAgent(llm, code_report) if code_report else None
        self.results: list[ReviewResult] = []

    def process_table(self, schema: str, table: str) -> list[ReviewResult]:
        heading(f"Analyzing {schema}.{table}")

        with step_spinner(f"Profiling {schema}.{table} structure and data"):
            profile = self.db.profile_table(schema, table)
        ctx = self._build_context(profile)

        all_suggestions: list[MetadataSuggestion] = []

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
        all_suggestions.extend(self.profile_agent.run(ctx))

        if self.rag_agent:
            info(f"RAG Agent: {num_cols} columns to check against documents")
            all_suggestions.extend(self.rag_agent.run(ctx))

        if self.code_agent:
            info(f"Code Agent: {num_cols} columns to check against codebase")
            all_suggestions.extend(self.code_agent.run(ctx))

        merged = self._merge_suggestions(all_suggestions, ctx)
        if not merged:
            warn(
                "No metadata suggestions were produced for this table. "
                "If the model replied, the raw text may be in ~/.amx/logs/last_profile_agent_response.txt "
                "— see also ~/.amx/logs/amx.log"
            )
            return []

        reviewed = self._human_review(merged, schema, table)
        self.results.extend(reviewed)
        return reviewed

    def _build_context(self, profile: TableProfile) -> AgentContext:
        return AgentContext(
            schema=profile.schema,
            table=profile.name,
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
                "database": self.db.cfg.database,
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

        for col_name, col_suggestions in needs_merge.items():
            key = col_name or "(table-level)"
            best, conf, reasoning = parsed.get(key, ("", Confidence.MEDIUM, ""))

            all_descs = [best] if best else []
            for s in col_suggestions:
                for d in s.suggestions:
                    if d not in all_descs:
                        all_descs.append(d)

            merged.append(MetadataSuggestion(
                schema=ctx.schema,
                table=ctx.table,
                column=col_name,
                suggestions=all_descs[:5],
                confidence=conf,
                reasoning=reasoning,
                source="combined",
            ))

        return merged

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
        self, suggestions: list[MetadataSuggestion], schema: str, table: str
    ) -> list[ReviewResult]:
        results: list[ReviewResult] = []

        table_suggestions = [s for s in suggestions if s.column is None]
        col_suggestions = [s for s in suggestions if s.column is not None]

        for s in table_suggestions:
            result = self._review_single(s, is_table=True)
            results.append(result)

        if col_suggestions:
            heading(f"Column descriptions for {schema}.{table}")
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
                if review_mode == "accept-all":
                    results.append(ReviewResult(
                        schema=s.schema, table=s.table, column=s.column,
                        final_description=s.suggestions[0],
                        confidence=s.confidence, source=s.source, applied=True,
                    ))
                elif review_mode == "accept-all-high" and s.confidence == Confidence.HIGH:
                    results.append(ReviewResult(
                        schema=s.schema, table=s.table, column=s.column,
                        final_description=s.suggestions[0],
                        confidence=s.confidence, source=s.source, applied=True,
                    ))
                elif review_mode == "reject-all":
                    results.append(ReviewResult(
                        schema=s.schema, table=s.table, column=s.column,
                        final_description="",
                        confidence=s.confidence, source=s.source, applied=False,
                    ))
                else:
                    result = self._review_single(s, is_table=False)
                    results.append(result)

        return results

    def _review_single(self, s: MetadataSuggestion, is_table: bool) -> ReviewResult:
        asset = f"Table: {s.schema}.{s.table}" if is_table else f"Column: {s.table}.{s.column}"
        console.print(f"\n  [heading]{asset}[/heading]")
        console.print(f"  Confidence: [{'success' if s.confidence == Confidence.HIGH else 'warning'}]{s.confidence.value}[/]")
        console.print(f"  Source: {s.source}")
        console.print(f"  Reasoning: {s.reasoning}")
        console.print()

        options = list(s.suggestions) + ["Other (type your own)", "Skip"]
        choice = ask_choice("Select a description", options, default=options[0])

        if choice == "Skip":
            return ReviewResult(
                schema=s.schema, table=s.table, column=s.column,
                final_description="", confidence=s.confidence,
                source=s.source, applied=False,
            )
        elif choice == "Other (type your own)":
            custom = ask("Enter your description")
            return ReviewResult(
                schema=s.schema, table=s.table, column=s.column,
                final_description=custom, confidence=Confidence.HIGH,
                source="human", applied=True,
            )
        else:
            return ReviewResult(
                schema=s.schema, table=s.table, column=s.column,
                final_description=choice, confidence=s.confidence,
                source=s.source, applied=True,
            )

    def apply_results(self, results: list[ReviewResult] | None = None) -> int:
        results = results or self.results
        applied = apply_review_results_to_db(self.db, results)
        success(f"Applied {applied} metadata comments to the database")
        return applied
