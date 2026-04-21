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
        if not self.report:
            log.info("No codebase report, skipping code agent")
            return []

        from amx.codebase.code_rag import code_collection_count, query_code_snippets

        has_refs = bool(self.report.references) or bool(self.report.external_mentions)
        has_sem = code_collection_count() > 0
        if not has_refs and not has_sem:
            log.info("No codebase references or semantic index, skipping code agent")
            return []

        suggestions: list[MetadataSuggestion] = []
        columns = ctx.db_profile.get("columns", [])

        ext_flat: list[CodeReference] = []
        for lst in (self.report.external_mentions or {}).values():
            ext_flat.extend(lst[:2])
        ext_block = ""
        if ext_flat:
            ext_block = "\n\n---\n\n".join(
                f"(not in connected DB catalog) File: {r.file}:{r.line_no}\n{r.context}"
                for r in ext_flat[:5]
            )

        for col in columns:
            col_name = col["name"].lower()
            refs = self.report.references.get(col_name, []) if self.report.references else []
            table_refs = self.report.references.get(ctx.table.lower(), []) if self.report.references else []

            all_refs = refs + table_refs[:5]
            sem_block = ""
            if has_sem:
                sem_hits = query_code_snippets(
                    f"{ctx.schema} {ctx.table} {col['name']} SQL Spark dataframe usage",
                    n_results=3,
                )
                if sem_hits:
                    sem_block = "\n\nSemantic code retrieval (nearest chunks):\n" + "\n---\n".join(
                        h["text"][:900] for h in sem_hits
                    )

            if not all_refs and not sem_block and not ext_block:
                continue

            code_snippets = ""
            if all_refs:
                code_snippets = "\n\n---\n\n".join(
                    f"File: {r.file}:{r.line_no}\n{r.context}"
                    for r in all_refs[:10]
                )

            user_msg = (
                f"Schema: {ctx.schema}\n"
                f"Table: {ctx.table}\n"
                f"Column: {col['name']} (type={col['dtype']})\n\n"
                f"Code references:\n{code_snippets or '(none)'}"
            )
            if ext_block:
                user_msg += f"\n\nOther identifiers seen in repo (may be outside this DB):\n{ext_block}"
            if sem_block:
                user_msg += sem_block

            messages = [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_msg},
            ]
            est = estimate_tokens(messages)
            with step_spinner(f"Code Agent: {col['name']}", token_estimate=est):
                result = self.llm.chat(messages)
            tracker.record("code_agent", est, result.usage)

            for s in self._parse_response(result.content, ctx, col["name"]):
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
