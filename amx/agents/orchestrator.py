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
    success,
    warn,
)
from amx.utils.logging import get_logger

log = get_logger("agents.orchestrator")

MERGE_PROMPT = """\
You are merging metadata suggestions from multiple sources for a database column.

Sources and their suggestions:
{source_text}

Produce a single best description that combines insights from all sources.
Also rate your confidence: HIGH / MEDIUM / LOW.

Respond exactly:
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

        info("Profiling table structure and data...")
        profile = self.db.profile_table(schema, table)
        ctx = self._build_context(profile)

        all_suggestions: list[MetadataSuggestion] = []

        info("Running profile agent (column names, types, statistics)...")
        all_suggestions.extend(self.profile_agent.run(ctx))

        if self.rag_agent:
            info("Running RAG agent (document search)...")
            all_suggestions.extend(self.rag_agent.run(ctx))

        if self.code_agent:
            info("Running code agent (codebase analysis)...")
            all_suggestions.extend(self.code_agent.run(ctx))

        merged = self._merge_suggestions(all_suggestions, ctx)
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
                "columns": [
                    {
                        "name": c.name,
                        "dtype": c.dtype,
                        "nullable": c.nullable,
                        "row_count": c.row_count,
                        "null_count": c.null_count,
                        "distinct_count": c.distinct_count,
                        "min_val": c.min_val,
                        "max_val": c.max_val,
                        "samples": c.samples,
                    }
                    for c in profile.columns
                ],
            },
            existing_metadata={
                "database": self.db.cfg.database,
                "table_comment": profile.existing_comment,
            },
        )

    def _merge_suggestions(
        self, suggestions: list[MetadataSuggestion], ctx: AgentContext
    ) -> list[MetadataSuggestion]:
        by_column: dict[str | None, list[MetadataSuggestion]] = defaultdict(list)
        for s in suggestions:
            by_column[s.column].append(s)

        merged: list[MetadataSuggestion] = []
        for col_name, col_suggestions in by_column.items():
            if len(col_suggestions) == 1:
                merged.append(col_suggestions[0])
                continue

            source_text = "\n".join(
                f"[{s.source}] (confidence={s.confidence.value}): {', '.join(s.suggestions)}\n  reasoning: {s.reasoning}"
                for s in col_suggestions
            )

            response = self.llm.chat([
                {"role": "user", "content": MERGE_PROMPT.format(source_text=source_text)},
            ])

            best = ""
            conf = Confidence.MEDIUM
            reasoning = ""
            for line in response.splitlines():
                line = line.strip()
                if line.startswith("BEST_DESCRIPTION:"):
                    best = line.split(":", 1)[1].strip()
                elif line.startswith("CONFIDENCE:"):
                    raw = line.split(":", 1)[1].strip().upper()
                    conf = Confidence[raw] if raw in Confidence.__members__ else Confidence.MEDIUM
                elif line.startswith("REASONING:"):
                    reasoning = line.split(":", 1)[1].strip()

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
        applied = 0
        for r in results:
            if not r.applied or not r.final_description:
                continue
            try:
                if r.column is None:
                    self.db.set_table_comment(r.schema, r.table, r.final_description)
                else:
                    self.db.set_column_comment(r.schema, r.table, r.column, r.final_description)
                applied += 1
            except Exception as exc:
                error(f"Failed to apply comment on {r.schema}.{r.table}.{r.column or ''}: {exc}")
        success(f"Applied {applied} metadata comments to the database")
        return applied
