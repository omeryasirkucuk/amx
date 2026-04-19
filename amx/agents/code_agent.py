"""Sub-agent: analyze codebase references to refine metadata understanding."""

from __future__ import annotations

from amx.agents.base import AgentContext, BaseAgent, Confidence, MetadataSuggestion
from amx.codebase.analyzer import CodebaseReport
from amx.llm.provider import LLMProvider
from amx.utils.logging import get_logger

log = get_logger("agents.code")

SYSTEM_PROMPT = """\
You are a data-catalog expert analyzing how database tables and columns are used
in application code to understand their meaning.

You are given:
- A table/column name and profile.
- Code snippets where this asset is referenced.

Based on how the code uses this asset, infer a description.

Respond in this format for each column:

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
        if not self.report or not self.report.references:
            log.info("No codebase references available, skipping code agent")
            return []

        suggestions: list[MetadataSuggestion] = []
        columns = ctx.db_profile.get("columns", [])

        for col in columns:
            col_name = col["name"].lower()
            refs = self.report.references.get(col_name, [])
            table_refs = self.report.references.get(ctx.table.lower(), [])

            all_refs = refs + table_refs[:5]
            if not all_refs:
                continue

            code_snippets = "\n\n---\n\n".join(
                f"File: {r.file}:{r.line_no}\n{r.context}"
                for r in all_refs[:10]
            )

            user_msg = (
                f"Schema: {ctx.schema}\n"
                f"Table: {ctx.table}\n"
                f"Column: {col['name']} (type={col['dtype']})\n\n"
                f"Code references:\n{code_snippets}"
            )

            response = self.llm.chat([
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_msg},
            ])

            for s in self._parse_response(response, ctx, col["name"]):
                suggestions.append(s)

        return suggestions

    def _parse_response(
        self, text: str, ctx: AgentContext, default_col: str
    ) -> list[MetadataSuggestion]:
        suggestions: list[MetadataSuggestion] = []
        current_col = default_col
        descs: list[str] = []
        conf = Confidence.MEDIUM
        reasoning = ""

        for line in text.splitlines():
            line = line.strip()
            if line.startswith("COLUMN:"):
                if descs:
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

        if descs:
            suggestions.append(MetadataSuggestion(
                schema=ctx.schema, table=ctx.table, column=current_col,
                suggestions=descs, confidence=conf, reasoning=reasoning,
                source="codebase",
            ))

        return suggestions
