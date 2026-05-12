"""Sub-agent: use RAG over documents to enrich metadata suggestions."""

from __future__ import annotations

import re

from amx.agents.base import (
    AgentContext,
    BaseAgent,
    Citation,
    Confidence,
    MetadataSuggestion,
    apply_logprob_confidence,
)
from amx.config import PromptDetail
from amx.core.token_budget import MaxTokenValidator
from amx.docs.rag import RAGStore
from amx.llm.prompts import (
    ALTERNATIVES_LENGTH_RULE_REMINDER,
    length_rule,
    per_col_token_budget,
)
from amx.llm.provider import LLMProvider
from amx.llm.style.guard import scrub_placeholders
from amx.llm.style.injector import render_style_section
from amx.llm.style.loader import load_active_style_profile
from amx.llm.style.profile import StyleProfile
from amx.utils.console import step_spinner
from amx.utils.logging import get_logger
from amx.utils.token_tracker import estimate_tokens, tracker

log = get_logger("agents.rag")


def _hits_to_citations(prompt_hits: list[dict]) -> list[Citation]:
    """Convert the retrieval hits that were fed into the prompt into citations.

    The chunk metadata (source, chunk_idx) is the ground-truth
    provenance recorded at ingest time; the rerank score is the same
    one :meth:`RAGStore.rerank` ranked the hit list by. The snippet
    is the first 200 chars of the chunk text so the UI can render a
    preview without re-fetching the document. Filters out hits with
    no chunk metadata (defensive — pre-PR-B collections may have
    missing ``source`` on legacy rows).
    """
    citations: list[Citation] = []
    seen: set[tuple[str, int]] = set()
    for h in prompt_hits or []:
        meta = h.get("metadata") or {}
        source = str(meta.get("source") or "").strip()
        if not source:
            continue
        try:
            chunk_idx = int(meta.get("chunk_idx") or 0)
        except (TypeError, ValueError):
            chunk_idx = 0
        key = (source, chunk_idx)
        if key in seen:
            continue
        seen.add(key)
        try:
            score = float(h.get("score") or 0.0)
        except (TypeError, ValueError):
            score = 0.0
        text = h.get("text") or h.get("document") or ""
        snippet = str(text)[:200].strip()
        citations.append(Citation(source=source, chunk_idx=chunk_idx, score=score, snippet=snippet))
    return citations


def _attach_citations(
    suggestions: list[MetadataSuggestion], prompt_hits: list[dict]
) -> list[MetadataSuggestion]:
    """Attach citations derived from ``prompt_hits`` to every suggestion.

    Per the PR C spec, we attach the full deduplicated citation list
    to every suggestion produced from the same prompt. Column-level
    filtering of which chunk informed which column would require
    re-ranking per column query (the agent currently fans multiple
    column queries into one prompt), and the goal is just to show
    the user which documents were consulted -- the union answers
    that question without false negatives.
    """
    if not suggestions or not prompt_hits:
        return suggestions
    citations = _hits_to_citations(prompt_hits)
    if not citations:
        return suggestions
    for s in suggestions:
        s.citations = list(citations)
    return suggestions


def _scrub_suggestions(
    suggestions: list[MetadataSuggestion],
    active_profile: StyleProfile | None,
) -> list[MetadataSuggestion]:
    """Scrub placeholder literals from suggestion text when a style profile is active."""
    if active_profile is None:
        return suggestions
    for s in suggestions:
        s.suggestions = [scrub_placeholders(text) for text in s.suggestions]
    return suggestions


_BASE_SYSTEM_PROMPT = """\
You are a data-catalog expert using documentation to understand database assets.

You are given:
- A table name and schema.
- A list of columns with types and sample values.
- Relevant document excerpts retrieved via search.

Based on the documentation, infer a concise description for EACH column listed.

Output rules:
- Write every description and reasoning string in **clear, business-friendly American English**.
- Use complete sentences ending with a period.
- Length rule (CRITICAL — honour the user's verbosity preset): {description_length_rule}
- Keep the response labels (`COLUMN`, `DESCRIPTION_1`, `CONFIDENCE`, `REASONING`) verbatim.

Write descriptions assertively and directly (e.g. "Telephone extension number.").
Do NOT start descriptions with "This column likely represents" or "This column is".
Only use claims supported by the retrieved excerpts. Documentation can be stale or generic, so do not over-claim.
Prefer excerpt-specific terminology over guesses from column names alone.
If the excerpts describe the table but not a particular column, keep the column description broad and lower confidence.
Do not invent business semantics, process stages, or cross-system mappings that are absent from the excerpts.
Confidence rules:
- HIGH: excerpts clearly describe the field or an unmistakable equivalent.
- MEDIUM: excerpts strongly suggest the meaning but indirect mapping is required.
- LOW: excerpts provide only weak surrounding context.
Reasoning must cite the document evidence pattern, not just repeat the description.

{alternatives_length_reminder}
Respond in this format for each column (one block per column):

COLUMN: <column_name>
DESCRIPTION_1: <best description based on docs>
{desc_lines}
CONFIDENCE: <HIGH|MEDIUM|LOW>
REASONING: <what doc evidence supports this>

Example style:
COLUMN: currency_code
DESCRIPTION_1: Transaction currency code.
CONFIDENCE: HIGH
REASONING: The retrieved excerpts describe monetary amounts and refer to a companion currency field in the same table context.
"""


def _build_system_prompt(
    n_alternatives: int,
    description_verbosity: str = "brief",
    style_profile: StyleProfile | None = None,
) -> str:
    n = max(1, min(5, n_alternatives))
    if n > 1:
        desc_lines = "\n".join(
            f"DESCRIPTION_{i}: <alternative description — apply the SAME length rule as DESCRIPTION_1>"
            for i in range(2, n + 1)
        )
        alternatives_length_reminder = ALTERNATIVES_LENGTH_RULE_REMINDER
    else:
        desc_lines = ""
        alternatives_length_reminder = ""
    return (
        _BASE_SYSTEM_PROMPT.format(
            desc_lines=desc_lines,
            description_length_rule=length_rule(description_verbosity),
            alternatives_length_reminder=alternatives_length_reminder,
        ).strip()
        + "\n"
        + render_style_section(style_profile)
    )


class RAGAgent(BaseAgent):
    name = "rag_agent"

    def __init__(self, llm: LLMProvider, rag_store: RAGStore):
        self.llm = llm
        self.rag = rag_store
        self._style_profile = load_active_style_profile()
        # Diagnostics buffer (mirrors :class:`ProfileAgent`): the
        # orchestrator drains it after each table to surface signals
        # that aren't fatal but the user still needs to see — today,
        # "RAG returned no relevant documents".
        self._diagnostics: list[str] = []
        # Snapshot of the exact ``prompt_hits`` fed into the most recent
        # LLM call per ``(schema, table)``. The orchestrator drains this
        # after each table so the run-context cache (PR D) can persist
        # the actual chunks for deterministic re-runs.
        self.last_prompt_hits: dict[tuple[str, str], list[dict]] = {}

    def consume_diagnostics(self) -> list[str]:
        diagnostics = list(self._diagnostics)
        self._diagnostics.clear()
        return diagnostics

    def _record_diagnostic(self, message: str) -> None:
        if message:
            self._diagnostics.append(message)

    @property
    def _n_alternatives(self) -> int:
        return max(1, min(5, getattr(self.llm.cfg, "n_alternatives", 3)))

    @property
    def _description_verbosity(self) -> str:
        return getattr(self.llm.cfg, "description_verbosity", "brief")

    def _per_col_token_budget(self) -> int:
        return per_col_token_budget(self._description_verbosity)

    def _scaled_max_tokens(self, n_columns: int) -> int:
        return max(self.llm.cfg.max_tokens, n_columns * self._per_col_token_budget())

    @property
    def _prompt_detail(self) -> PromptDetail:
        return self.llm.cfg.prompt_detail_cfg

    def _rag_query_timeout(self) -> float | None:
        """Resolved per-query timeout for retrieval (seconds), or ``None`` to disable."""
        raw = getattr(self.llm.cfg, "rag_query_timeout_sec", 5.0)
        try:
            value = float(raw)
        except (TypeError, ValueError):
            return None
        return value if value > 0 else None

    def _query_with_timeout(self, question: str, n_results: int) -> tuple[list[dict], bool]:
        """Run ``RAGStore.query`` honouring the configured timeout.

        Returns ``(hits, timed_out)`` so the caller can branch on the
        timeout case to record a user-facing diagnostic. The timeout
        path returns an empty hit list so retrieval falls back to
        "no docs used" instead of blocking the run on a stalled
        vector store. ``timeout=None`` (or ``<=0``) on the config
        runs the query without the executor wrapper.
        """
        from amx.docs.rag import RAGQueryTimeout

        timeout = self._rag_query_timeout()
        # Carry the prompt-detail floor through to the store so a doc
        # profile that's only weakly related to the table (e.g. the
        # user uploaded a resume PDF instead of warehouse docs)
        # doesn't bleed into the LLM prompt as if it were evidence.
        min_similarity = float(getattr(self._prompt_detail, "rag_min_similarity", 0.0) or 0.0)
        try:
            hits = self.rag.query(
                question,
                n_results=n_results,
                timeout=timeout,
                min_similarity=min_similarity,
            )
            return (hits, False)
        except RAGQueryTimeout:
            return ([], True)

    def _build_messages(self, ctx: AgentContext) -> tuple[list[dict[str, str]], list[dict]] | None:
        """Build the RAG prompt messages and remember the retrieval hits.

        Returns ``None`` when no context is available; otherwise a
        ``(messages, prompt_hits)`` tuple. ``prompt_hits`` is the
        truncated list of chunks actually fed into the prompt (capped
        at ``pd.rag_max_chunks``), so the agent's ``run`` method can
        attach :class:`Citation` records to every produced suggestion
        from a known-good provenance source instead of trusting the
        LLM's free-text reasoning.
        """
        columns = ctx.db_profile.get("columns", [])
        if not columns:
            return None

        pd = self._prompt_detail

        # Re-run replay path: when the orchestrator hydrated the
        # AgentContext from a snapshot that already carries the exact
        # hits the original run consumed, skip the live Chroma queries
        # entirely. This makes re-runs deterministic across re-ingests
        # and avoids re-paying retrieval latency on every re-run.
        snapshot_hits = list(getattr(ctx, "rag_hits", None) or [])
        any_timed_out = False
        if snapshot_hits:
            unique_hits = snapshot_hits
        else:
            if self.rag.doc_count == 0:
                return None
            table_hits, table_timed_out = self._query_with_timeout(
                f"table {ctx.table} in schema {ctx.schema}", pd.rag_table_hits
            )
            seen_docs: set[str] = set()
            unique_hits = list(table_hits)
            any_timed_out = table_timed_out

            if pd.rag_col_hits > 0:
                for col in columns:
                    col_hits, col_timed_out = self._query_with_timeout(
                        f"{ctx.table}.{col['name']} column", pd.rag_col_hits
                    )
                    if col_timed_out:
                        any_timed_out = True
                    for h in col_hits:
                        key = h["text"][:120]
                        if key not in seen_docs:
                            seen_docs.add(key)
                            unique_hits.append(h)

        if any_timed_out:
            timeout = self._rag_query_timeout() or 0.0
            self._record_diagnostic(
                f"RAG: retrieval timed out after {timeout:.1f}s for "
                f"{ctx.schema}.{ctx.table}; no docs used for this table"
            )

        if not unique_hits:
            return None

        prompt_hits = list(unique_hits[: pd.rag_max_chunks])
        # Stash for downstream snapshot writers; keyed by (schema,
        # table) since the agent is reused across many tables in one
        # run.
        self.last_prompt_hits[(ctx.schema, ctx.table)] = list(prompt_hits)
        doc_chunks = [
            f"[{h['metadata'].get('source', 'unknown')}]\n{h['text']}" for h in prompt_hits
        ]
        validator = MaxTokenValidator(
            comfortable_input_tokens=max(
                1_000, int(getattr(self.llm.cfg, "max_tokens", 4096) or 4096) * 3
            )
        )
        doc_chunks = validator.compact_chunks(doc_chunks)
        doc_text = "\n\n---\n\n".join(doc_chunks)
        col_lines = "\n".join(
            f"  - {c['name']} (type={c['dtype']}, samples={c.get('samples', [])})" for c in columns
        )
        from amx.agents.base import _user_instructions_block

        user_msg = (
            f"Schema: {ctx.schema}\n"
            f"Table: {ctx.table}\n\n"
            f"Columns:\n{col_lines}\n\n"
            f"Relevant documentation:\n{doc_text}" + _user_instructions_block(ctx)
        )
        system = _build_system_prompt(
            self._n_alternatives, self._description_verbosity, style_profile=self._style_profile
        )
        return (
            [
                {"role": "system", "content": system},
                {"role": "user", "content": user_msg},
            ],
            prompt_hits,
        )

    def collect_messages(self, ctx: AgentContext) -> list:
        """Return a ``BatchRequest`` for this table (or empty list when no docs)."""
        from amx.llm.batch import BatchRequest

        built = self._build_messages(ctx)
        if built is None:
            self._record_diagnostic(
                f"RAG: no relevant documents found for {ctx.schema}.{ctx.table}"
            )
            return []
        msgs, prompt_hits = built
        # Stash the hits so ``parse_batch_result`` can attach the
        # matching citations after the Batch API returns. Keyed by
        # ``(schema, table)`` because :class:`RAGAgent` is reused
        # across tables within one run.
        if not hasattr(self, "_pending_hits"):
            self._pending_hits = {}
        self._pending_hits[(ctx.schema, ctx.table)] = prompt_hits
        n_columns = len(ctx.db_profile.get("columns", []) or [])
        return [
            BatchRequest(
                custom_id=f"rag:{ctx.schema}:{ctx.table}",
                messages=msgs,
                max_tokens=self._scaled_max_tokens(n_columns),
                temperature=self.llm.cfg.temperature,
                metadata={"schema": ctx.schema, "table": ctx.table},
            )
        ]

    def parse_batch_result(self, content: str, ctx: AgentContext) -> list[MetadataSuggestion]:
        """Parse a raw LLM text response; used after Batch API completes."""
        suggestions = self._parse_response(content, ctx)
        suggestions = _scrub_suggestions(suggestions, self._style_profile)
        prompt_hits = getattr(self, "_pending_hits", {}).pop((ctx.schema, ctx.table), [])
        _attach_citations(suggestions, prompt_hits)
        return suggestions

    def run(self, ctx: AgentContext) -> list[MetadataSuggestion]:
        self._diagnostics.clear()
        built = self._build_messages(ctx)
        if built is None:
            log.info("No RAG context for %s.%s, skipping", ctx.schema, ctx.table)
            self._record_diagnostic(
                f"RAG: no relevant documents found for {ctx.schema}.{ctx.table}"
            )
            return []
        messages, prompt_hits = built

        columns = ctx.db_profile.get("columns", [])
        est = estimate_tokens(messages)
        mt = self._scaled_max_tokens(len(columns))
        with step_spinner(f"RAG Agent: {len(columns)} columns", token_estimate=est):
            result = self.llm.chat(messages, max_tokens=mt)
        tracker.record_for("rag_agent", est, self.llm, result.usage)

        suggestions = self._parse_response(result.content, ctx)
        suggestions = _scrub_suggestions(suggestions, self._style_profile)
        _attach_citations(suggestions, prompt_hits)
        return apply_logprob_confidence(
            suggestions,
            result.logprobs,
            high_threshold=self.llm.cfg.logprob_high,
            medium_threshold=self.llm.cfg.logprob_medium,
            response_text=result.content,
        )

    def _parse_response(
        self, text: str, ctx: AgentContext, default_col: str = ""
    ) -> list[MetadataSuggestion]:
        text = re.sub(r"^```[a-zA-Z0-9_-]*\s*", "", (text or "").strip())
        text = re.sub(r"\s*```$", "", text).strip()
        suggestions: list[MetadataSuggestion] = []
        current_col = default_col
        descs: list[str] = []
        conf = Confidence.MEDIUM
        reasoning = ""

        for line in text.splitlines():
            line = line.strip()
            if line.startswith("COLUMN:"):
                if current_col and descs:
                    suggestions.append(
                        MetadataSuggestion(
                            schema=ctx.schema,
                            table=ctx.table,
                            column=current_col,
                            suggestions=descs,
                            confidence=conf,
                            reasoning=reasoning,
                            source="rag",
                        )
                    )
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
            suggestions.append(
                MetadataSuggestion(
                    schema=ctx.schema,
                    table=ctx.table,
                    column=current_col,
                    suggestions=descs,
                    confidence=conf,
                    reasoning=reasoning,
                    source="rag",
                )
            )

        return suggestions
