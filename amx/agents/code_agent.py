"""Sub-agent: analyze codebase references to refine metadata understanding."""

from __future__ import annotations

from amx.agents.base import AgentContext, BaseAgent, Confidence, MetadataSuggestion
from amx.codebase.analyzer import CodeReference, CodebaseReport
from amx.llm.provider import LLMProvider
from amx.utils.console import step_spinner
from amx.utils.logging import get_logger
from amx.utils.token_tracker import estimate_tokens, tracker

log = get_logger("agents.code")

SYSTEM_PROMPT = """\
You are a data-catalog expert analyzing how database tables and columns are used
in application code to understand their meaning.

You are given:
- A table in a schema with its list of columns.
- Code snippets where these assets are referenced.

Based on how the code uses these assets, infer a description for EACH column.

Respond in this format for each column (one block per column):

COLUMN: <column_name>
DESCRIPTION_1: <best description based on code usage>
DESCRIPTION_2: <alternative>
DESCRIPTION_3: <alternative>
CONFIDENCE: <HIGH|MEDIUM|LOW>
REASONING: <what code patterns support this>
"""


class CodeAgent(BaseAgent):
    name = "code_agent"

    def __init__(self, llm: LLMProvider, report: CodebaseReport | None = None):
        self.llm = llm
        self.report = report

    def run(self, ctx: AgentContext) -> list[MetadataSuggestion]:
        if not self.report:
            log.info("No codebase report, skipping code agent")
            return []

        from amx.codebase.code_rag import code_collection_count, query_code_snippets

        has_refs = bool(self.report.references) or bool(self.report.external_mentions)
        has_sem = code_collection_count() > 0
        if not has_refs and not has_sem:
            log.info("No codebase references or semantic index, skipping code agent")
            return []

        columns = ctx.db_profile.get("columns", [])
        if not columns:
            return []

        table_refs = (
            self.report.references.get(ctx.table.lower(), [])
            if self.report.references
            else []
        )

        all_code_blocks: list[str] = []

        if table_refs:
            all_code_blocks.append(
                "## Table-level references\n"
                + "\n---\n".join(
                    f"File: {r.file}:{r.line_no}\n{r.context}"
                    for r in table_refs[:8]
                )
            )

        cols_with_refs: list[str] = []
        for col in columns:
            col_name = col["name"].lower()
            refs = (
                self.report.references.get(col_name, [])
                if self.report.references
                else []
            )
            if refs:
                cols_with_refs.append(col["name"])
                all_code_blocks.append(
                    f"## Column: {col['name']}\n"
                    + "\n---\n".join(
                        f"File: {r.file}:{r.line_no}\n{r.context}"
                        for r in refs[:5]
                    )
                )

        if has_sem:
            sem_hits = query_code_snippets(
                f"{ctx.schema} {ctx.table} SQL Spark dataframe usage",
                n_results=5,
            )
            if sem_hits:
                all_code_blocks.append(
                    "## Semantic code retrieval (nearest chunks)\n"
                    + "\n---\n".join(h["text"][:900] for h in sem_hits)
                )

        ext_flat: list[CodeReference] = []
        for lst in (self.report.external_mentions or {}).values():
            ext_flat.extend(lst[:2])
        if ext_flat:
            all_code_blocks.append(
                "## Other identifiers (not in connected DB catalog)\n"
                + "\n---\n".join(
                    f"File: {r.file}:{r.line_no}\n{r.context}"
                    for r in ext_flat[:5]
                )
            )

        if not all_code_blocks:
            log.info("No code context for %s.%s, skipping", ctx.schema, ctx.table)
            return []

        col_lines = "\n".join(
            f"  - {c['name']} (type={c['dtype']})" for c in columns
        )

        user_msg = (
            f"Schema: {ctx.schema}\n"
            f"Table: {ctx.table}\n\n"
            f"Columns:\n{col_lines}\n\n"
            f"Code references:\n\n" + "\n\n".join(all_code_blocks)
        )

        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_msg},
        ]
        est = estimate_tokens(messages)
        with step_spinner(
            f"Code Agent: {len(columns)} columns", token_estimate=est
        ):
            result = self.llm.chat(messages)
        tracker.record("code_agent", est, result.usage)

        return self._parse_response(result.content, ctx)

    def _parse_response(
        self, text: str, ctx: AgentContext, default_col: str = ""
    ) -> list[MetadataSuggestion]:
        suggestions: list[MetadataSuggestion] = []
        current_col = default_col
        descs: list[str] = []
        conf = Confidence.MEDIUM
        reasoning = ""

        for line in text.splitlines():
            line = line.strip()
            if line.startswith("COLUMN:"):
                if current_col and descs:
                    suggestions.append(MetadataSuggestion(
                        schema=ctx.schema, table=ctx.table, column=current_col,
                        suggestions=descs, confidence=conf, reasoning=reasoning,
                        source="codebase",
                    ))
                current_col = line.split(":", 1)[1].strip()
                descs = []
                conf = Confidence.MEDIUM
                reasoning = ""
            elif line.startswith("DESCRIPTION_"):
                descs.append(line.split(":", 1)[1].strip())
            elif line.startswith("CONFIDENCE:"):
                raw = line.split(":", 1)[1].strip().upper()
                conf = Confidence[raw] if raw in Confidence.__members__ else Confidence.MEDIUM
            elif line.startswith("REASONING:"):
                reasoning = line.split(":", 1)[1].strip()

        if current_col and descs:
            suggestions.append(MetadataSuggestion(
                schema=ctx.schema, table=ctx.table, column=current_col,
                suggestions=descs, confidence=conf, reasoning=reasoning,
                source="codebase",
            ))

        return suggestions
