"""Sub-agent: use RAG over documents to enrich metadata suggestions."""

from __future__ import annotations

from amx.agents.base import AgentContext, BaseAgent, Confidence, MetadataSuggestion
from amx.docs.rag import RAGStore
from amx.llm.provider import LLMProvider
from amx.utils.console import step_spinner
from amx.utils.logging import get_logger
from amx.utils.token_tracker import estimate_tokens, tracker

log = get_logger("agents.rag")

SYSTEM_PROMPT = """\
You are a data-catalog expert using documentation to understand database assets.

You are given:
- A table/column name and its profile from the database.
- Relevant document excerpts retrieved via search.

Based on the documentation, refine or correct the description of each column.

Respond in this format for each column:

COLUMN: <column_name>
DESCRIPTION_1: <best description based on docs>
DESCRIPTION_2: <alternative>
DESCRIPTION_3: <alternative>
CONFIDENCE: <HIGH|MEDIUM|LOW>
REASONING: <what doc evidence supports this>
SOURCE_DOC: <document name or path>
"""


class RAGAgent(BaseAgent):
    name = "rag_agent"

    def __init__(self, llm: LLMProvider, rag_store: RAGStore):
        self.llm = llm
        self.rag = rag_store

    def run(self, ctx: AgentContext) -> list[MetadataSuggestion]:
        if self.rag.doc_count == 0:
            log.info("No documents ingested, skipping RAG agent")
            return []

        suggestions: list[MetadataSuggestion] = []
        columns = ctx.db_profile.get("columns", [])

        query = f"table {ctx.table} in schema {ctx.schema}"
        table_hits = self.rag.query(query, n_results=5)

        for col in columns:
            col_query = f"{ctx.table}.{col['name']} column"
            col_hits = self.rag.query(col_query, n_results=3)
            all_context = table_hits + col_hits

            if not all_context:
                continue

            doc_text = "\n\n---\n\n".join(
                f"[{h['metadata'].get('source', 'unknown')}]\n{h['text']}"
                for h in all_context
            )

            user_msg = (
                f"Schema: {ctx.schema}\n"
                f"Table: {ctx.table}\n"
                f"Column: {col['name']} (type={col['dtype']}, "
                f"samples={col['samples']})\n\n"
                f"Relevant documentation:\n{doc_text}"
            )

            messages = [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_msg},
            ]
            est = estimate_tokens(messages)
            with step_spinner(f"RAG Agent: {col['name']}", token_estimate=est):
                result = self.llm.chat(messages)
            tracker.record("rag_agent", est, result.usage)

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
                        source="rag",
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
                source="rag",
            ))

        return suggestions
