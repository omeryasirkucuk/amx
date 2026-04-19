"""Sub-agent: infer metadata from database profile (column stats, names, types)."""

from __future__ import annotations

from amx.agents.base import AgentContext, BaseAgent, Confidence, MetadataSuggestion
from amx.llm.provider import LLMProvider
from amx.utils.logging import get_logger

log = get_logger("agents.profile")

SYSTEM_PROMPT = """\
You are a data-catalog expert. Given database profile information for a table
and its columns, infer what each column likely represents.

For EACH column provide:
1. A concise description (1-2 sentences).
2. Up to 3 alternative descriptions ranked by likelihood.
3. A confidence level: HIGH / MEDIUM / LOW.
4. Brief reasoning for your choice.

Respond in this exact format for each column (one block per column):

COLUMN: <column_name>
DESCRIPTION_1: <most likely description>
DESCRIPTION_2: <alternative>
DESCRIPTION_3: <alternative>
CONFIDENCE: <HIGH|MEDIUM|LOW>
REASONING: <why you think so>

If the table-level description is also needed, add:
TABLE_DESCRIPTION: <description>
TABLE_CONFIDENCE: <HIGH|MEDIUM|LOW>
"""


class ProfileAgent(BaseAgent):
    name = "profile_agent"

    def __init__(self, llm: LLMProvider):
        self.llm = llm

    def run(self, ctx: AgentContext) -> list[MetadataSuggestion]:
        profile = ctx.db_profile
        if not profile:
            return []

        user_msg = self._build_prompt(ctx)
        response = self.llm.chat([
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_msg},
        ])

        return self._parse_response(response, ctx)

    def _build_prompt(self, ctx: AgentContext) -> str:
        p = ctx.db_profile
        lines = [
            f"Database: {ctx.existing_metadata.get('database', 'N/A')}",
            f"Schema: {ctx.schema}",
            f"Table: {ctx.table}",
            f"Row count: {p.get('row_count', 'N/A')}",
            f"Existing table comment: {p.get('existing_comment') or 'None'}",
            "",
            "Columns:",
        ]
        for col in p.get("columns", []):
            lines.append(
                f"  - {col['name']} | type={col['dtype']} | "
                f"nulls={col['null_count']}/{col['row_count']} | "
                f"distinct={col['distinct_count']} | "
                f"min={col['min_val']} | max={col['max_val']} | "
                f"samples={col['samples']}"
            )
        return "\n".join(lines)

    def _parse_response(self, text: str, ctx: AgentContext) -> list[MetadataSuggestion]:
        suggestions: list[MetadataSuggestion] = []
        current_col: str | None = None
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
                        source="db_profile",
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
            elif line.startswith("TABLE_DESCRIPTION:"):
                table_desc = line.split(":", 1)[1].strip()
                tconf_str = "MEDIUM"
                for l2 in text.splitlines():
                    if l2.strip().startswith("TABLE_CONFIDENCE:"):
                        tconf_str = l2.strip().split(":", 1)[1].strip().upper()
                        break
                tconf = Confidence[tconf_str] if tconf_str in Confidence.__members__ else Confidence.MEDIUM
                suggestions.append(MetadataSuggestion(
                    schema=ctx.schema, table=ctx.table, column=None,
                    suggestions=[table_desc], confidence=tconf,
                    reasoning="Inferred from table name, columns, and data profile",
                    source="db_profile",
                ))

        if current_col and descs:
            suggestions.append(MetadataSuggestion(
                schema=ctx.schema, table=ctx.table, column=current_col,
                suggestions=descs, confidence=conf, reasoning=reasoning,
                source="db_profile",
            ))

        return suggestions
