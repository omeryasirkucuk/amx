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
- A table name and schema.
- A list of columns with types and sample values.
- Relevant document excerpts retrieved via search.

Based on the documentation, infer a concise description for EACH column listed.

Respond in this format for each column (one block per column):

COLUMN: <column_name>
DESCRIPTION_1: <best description based on docs>
DESCRIPTION_2: <alternative>
DESCRIPTION_3: <alternative>
CONFIDENCE: <HIGH|MEDIUM|LOW>
REASONING: <what doc evidence supports this>
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

        columns = ctx.db_profile.get("columns", [])
        if not columns:
            return []

        table_hits = self.rag.query(
            f"table {ctx.table} in schema {ctx.schema}", n_results=5
        )

        seen_docs: set[str] = set()
        unique_hits = list(table_hits)
        for col in columns:
            col_hits = self.rag.query(f"{ctx.table}.{col['name']} column", n_results=2)
            for h in col_hits:
                key = h["text"][:120]
                if key not in seen_docs:
                    seen_docs.add(key)
                    unique_hits.append(h)

        if not unique_hits:
            log.info("No RAG hits for %s.%s, skipping", ctx.schema, ctx.table)
            return []

        doc_text = "\n\n---\n\n".join(
            f"[{h['metadata'].get('source', 'unknown')}]\n{h['text']}"
            for h in unique_hits[:15]
        )

        col_lines = "\n".join(
            f"  - {c['name']} (type={c['dtype']}, samples={c.get('samples', [])})"
            for c in columns
        )

        user_msg = (
            f"Schema: {ctx.schema}\n"
            f"Table: {ctx.table}\n\n"
            f"Columns:\n{col_lines}\n\n"
            f"Relevant documentation:\n{doc_text}"
        )

        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_msg},
        ]
        est = estimate_tokens(messages)
        with step_spinner(
            f"RAG Agent: {len(columns)} columns", token_estimate=est
        ):
            result = self.llm.chat(messages)
        tracker.record("rag_agent", est, result.usage)

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

        if current_col and descs:
            suggestions.append(MetadataSuggestion(
                schema=ctx.schema, table=ctx.table, column=current_col,
                suggestions=descs, confidence=conf, reasoning=reasoning,
                source="rag",
            ))

        return suggestions
