"""Sub-agent: use RAG over documents to enrich metadata suggestions."""

from __future__ import annotations

from amx.agents.base import AgentContext, BaseAgent, Confidence, MetadataSuggestion, apply_logprob_confidence
from amx.config import PromptDetail, prompt_detail_for
from amx.docs.rag import RAGStore
from amx.llm.provider import LLMProvider
from amx.utils.console import step_spinner
from amx.utils.logging import get_logger
from amx.utils.token_tracker import estimate_tokens, tracker

log = get_logger("agents.rag")

_BASE_SYSTEM_PROMPT = """\
You are a data-catalog expert using documentation to understand database assets.

You are given:
- A table name and schema.
- A list of columns with types and sample values.
- Relevant document excerpts retrieved via search.

Based on the documentation, infer a concise description for EACH column listed.

Respond in this format for each column (one block per column):

COLUMN: <column_name>
DESCRIPTION_1: <best description based on docs>
{desc_lines}
CONFIDENCE: <HIGH|MEDIUM|LOW>
REASONING: <what doc evidence supports this>
"""


def _build_system_prompt(n_alternatives: int) -> str:
    n = max(1, min(5, n_alternatives))
    desc_lines = "\n".join(
        f"DESCRIPTION_{i}: <alternative>"
        for i in range(2, n + 1)
    ) if n > 1 else ""
    return _BASE_SYSTEM_PROMPT.format(desc_lines=desc_lines).strip() + "\n"


class RAGAgent(BaseAgent):
    name = "rag_agent"

    def __init__(self, llm: LLMProvider, rag_store: RAGStore):
        self.llm = llm
        self.rag = rag_store

    @property
    def _n_alternatives(self) -> int:
        return max(1, min(5, getattr(self.llm.cfg, "n_alternatives", 3)))

    @property
    def _prompt_detail(self) -> PromptDetail:
        return self.llm.cfg.prompt_detail_cfg

    def _build_messages(self, ctx: AgentContext) -> list[dict[str, str]] | None:
        """Build the RAG prompt messages. Returns ``None`` when no context is available."""
        if self.rag.doc_count == 0:
            return None

        columns = ctx.db_profile.get("columns", [])
        if not columns:
            return None

        pd = self._prompt_detail

        table_hits = self.rag.query(
            f"table {ctx.table} in schema {ctx.schema}", n_results=pd.rag_table_hits
        )
        seen_docs: set[str] = set()
        unique_hits = list(table_hits)

        if pd.rag_col_hits > 0:
            for col in columns:
                col_hits = self.rag.query(
                    f"{ctx.table}.{col['name']} column", n_results=pd.rag_col_hits
                )
                for h in col_hits:
                    key = h["text"][:120]
                    if key not in seen_docs:
                        seen_docs.add(key)
                        unique_hits.append(h)

        if not unique_hits:
            return None

        doc_text = "\n\n---\n\n".join(
            f"[{h['metadata'].get('source', 'unknown')}]\n{h['text']}"
            for h in unique_hits[: pd.rag_max_chunks]
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
        system = _build_system_prompt(self._n_alternatives)
        return [
            {"role": "system", "content": system},
            {"role": "user", "content": user_msg},
        ]

    def collect_messages(self, ctx: AgentContext) -> "list":
        """Return a ``BatchRequest`` for this table (or empty list when no docs)."""
        from amx.llm.batch import BatchRequest

        msgs = self._build_messages(ctx)
        if msgs is None:
            return []
        return [
            BatchRequest(
                custom_id=f"rag:{ctx.schema}:{ctx.table}",
                messages=msgs,
                max_tokens=self.llm.cfg.max_tokens,
                temperature=self.llm.cfg.temperature,
                metadata={"schema": ctx.schema, "table": ctx.table},
            )
        ]

    def parse_batch_result(self, content: str, ctx: AgentContext) -> list[MetadataSuggestion]:
        """Parse a raw LLM text response; used after Batch API completes."""
        return self._parse_response(content, ctx)

    def run(self, ctx: AgentContext) -> list[MetadataSuggestion]:
        messages = self._build_messages(ctx)
        if messages is None:
            log.info("No RAG context for %s.%s, skipping", ctx.schema, ctx.table)
            return []

        columns = ctx.db_profile.get("columns", [])
        est = estimate_tokens(messages)
        with step_spinner(
            f"RAG Agent: {len(columns)} columns", token_estimate=est
        ):
            result = self.llm.chat(messages)
        tracker.record("rag_agent", est, result.usage)

        suggestions = self._parse_response(result.content, ctx)
        return apply_logprob_confidence(suggestions, result.logprobs)

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
