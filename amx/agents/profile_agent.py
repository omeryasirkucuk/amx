"""Sub-agent: infer metadata from database profile (column stats, names, types)."""

from __future__ import annotations

import re
from pathlib import Path

from amx.agents.base import AgentContext, BaseAgent, Confidence, MetadataSuggestion, apply_logprob_confidence
from amx.config import PromptDetail, prompt_detail_for
from amx.llm.provider import LLMProvider
from amx.utils.console import step_spinner
from amx.utils.logging import LAST_PROFILE_RESPONSE_FILE, LOG_DIR, get_logger
from amx.utils.token_tracker import estimate_tokens, tracker

log = get_logger("agents.profile")

_BASE_SYSTEM_PROMPT = """\
You are a data-catalog expert. Given database profile information for a table
and its columns, infer what each column likely represents.

For EACH column provide:
1. A concise description (1-2 sentences).
{alt_instruction}
{extra_items}
A confidence level: HIGH / MEDIUM / LOW.
Brief reasoning for your choice.

Respond in this exact format for each column (one block per column):

COLUMN: <column_name>
DESCRIPTION_1: <most likely description>
{desc_lines}
CONFIDENCE: <HIGH|MEDIUM|LOW>
REASONING: <why you think so>

If the table-level description is also needed, add:
TABLE_DESCRIPTION: <description>
TABLE_CONFIDENCE: <HIGH|MEDIUM|LOW>
"""


def _build_system_prompt(n_alternatives: int) -> str:
    """Build the system prompt dynamically for the requested number of alternatives."""
    n = max(1, min(5, n_alternatives))
    if n == 1:
        alt_instruction = ""
        extra_items = ""
        desc_lines = ""
    else:
        alt_instruction = f"Up to {n} alternative descriptions ranked by likelihood."
        extra_items = ""
        desc_lines = "\n".join(
            f"DESCRIPTION_{i}: <alternative>"
            for i in range(2, n + 1)
        )
    return _BASE_SYSTEM_PROMPT.format(
        alt_instruction=alt_instruction,
        extra_items=extra_items,
        desc_lines=desc_lines,
    ).strip() + "\n"


class ProfileAgent(BaseAgent):
    name = "profile_agent"

    def __init__(self, llm: LLMProvider):
        self.llm = llm

    BATCH_SIZE = 10

    @property
    def _n_alternatives(self) -> int:
        return max(1, min(5, getattr(self.llm.cfg, "n_alternatives", 3)))

    @property
    def _prompt_detail(self) -> PromptDetail:
        return self.llm.cfg.prompt_detail_cfg

    def run(self, ctx: AgentContext) -> list[MetadataSuggestion]:
        profile = ctx.db_profile
        if not profile:
            return []

        columns = list(profile.get("columns") or [])
        if not columns:
            return []

        if len(columns) <= self.BATCH_SIZE:
            return self._run_single_batch(ctx, columns)

        all_suggestions: list[MetadataSuggestion] = []
        batches = [
            columns[i : i + self.BATCH_SIZE]
            for i in range(0, len(columns), self.BATCH_SIZE)
        ]
        for idx, batch in enumerate(batches, 1):
            col_names = ", ".join(c["name"] for c in batch)
            log.info(
                "Profile agent batch %d/%d (%d cols: %s)",
                idx, len(batches), len(batch), col_names,
            )
            batch_ctx = self._ctx_with_columns(ctx, batch)
            batch_suggestions = self._run_single_batch(
                batch_ctx, batch, batch_label=f"batch {idx}/{len(batches)}"
            )
            all_suggestions.extend(batch_suggestions)

        if not all_suggestions:
            log.warning(
                "Profile agent produced zero suggestions across %d batches for %s.%s.",
                len(batches), ctx.schema, ctx.table,
            )
        return all_suggestions

    def _ctx_with_columns(self, ctx: AgentContext, columns: list) -> AgentContext:
        """Return a shallow copy of the context with only the specified columns."""
        new_profile = dict(ctx.db_profile)
        new_profile["columns"] = columns
        return AgentContext(
            schema=ctx.schema,
            table=ctx.table,
            column=ctx.column,
            db_profile=new_profile,
            rag_context=ctx.rag_context,
            code_context=ctx.code_context,
            existing_metadata=ctx.existing_metadata,
        )

    def collect_messages(self, ctx: AgentContext) -> "list":
        """Return ``BatchRequest`` objects for every profile prompt without calling LLM.

        Used by the orchestrator in Batch mode.
        """
        from amx.llm.batch import BatchRequest

        profile = ctx.db_profile
        if not profile:
            return []
        columns = list(profile.get("columns") or [])
        if not columns:
            return []

        batches = (
            [columns]
            if len(columns) <= self.BATCH_SIZE
            else [
                columns[i : i + self.BATCH_SIZE]
                for i in range(0, len(columns), self.BATCH_SIZE)
            ]
        )
        requests: list[BatchRequest] = []
        for idx, batch in enumerate(batches):
            batch_ctx = self._ctx_with_columns(ctx, batch)
            msgs = self._build_messages(batch_ctx)
            requests.append(
                BatchRequest(
                    custom_id=f"profile:{ctx.schema}:{ctx.table}:{idx}",
                    messages=msgs,
                    max_tokens=self.llm.cfg.max_tokens,
                    temperature=self.llm.cfg.temperature,
                    metadata={"schema": ctx.schema, "table": ctx.table, "batch_idx": idx},
                )
            )
        return requests

    def parse_batch_result(self, content: str, ctx: AgentContext) -> list[MetadataSuggestion]:
        """Parse a raw LLM text response for one batch; used after Batch API completes."""
        suggestions = self._parse_response(content, ctx)
        if not suggestions and len(content.strip()) > 20:
            suggestions = self._parse_response_loose(content, ctx)
        if not suggestions:
            suggestions = self._parse_by_known_column_names(content, ctx)
        return suggestions

    def _build_messages(self, ctx: AgentContext) -> list[dict[str, str]]:
        """Build the messages list for a single profile batch — shared by run() and collect_messages()."""
        user_msg = self._build_prompt(ctx)
        system = _build_system_prompt(self._n_alternatives)
        return [
            {"role": "system", "content": system},
            {"role": "user", "content": user_msg},
        ]

    def _run_single_batch(
        self, ctx: AgentContext, columns: list, *, batch_label: str = ""
    ) -> list[MetadataSuggestion]:
        messages = self._build_messages(ctx)
        log.debug(
            "Profile agent prompt for %s.%s: %d chars, %d columns",
            ctx.schema, ctx.table, len(messages[-1]["content"]), len(columns),
        )
        est = estimate_tokens(messages)
        label = f"Profile Agent {batch_label}" if batch_label else "Profile Agent"
        try:
            with step_spinner(label, token_estimate=est):
                result = self.llm.chat(messages)
        except Exception as exc:
            log.error("LLM call failed in profile agent: %s", exc)
            return []

        tracker.record("profile_agent", est, result.usage)
        response = result.content
        _logprobs = result.logprobs

        if not response or not response.strip():
            log.warning(
                "LLM returned an EMPTY response for %s.%s (%d columns). "
                "Check model name, API key, and billing on the provider dashboard.",
                ctx.schema, ctx.table, len(columns),
            )
            return []

        suggestions = self._parse_response(response, ctx)
        if not suggestions and len(response.strip()) > 20:
            log.warning(
                "Strict parse found no COLUMN:/DESCRIPTION_ blocks; trying loose parser."
            )
            suggestions = self._parse_response_loose(response, ctx)
        if not suggestions:
            suggestions = self._parse_by_known_column_names(response, ctx)

        if not suggestions:
            self._save_failed_response_for_debug(response, ctx)
            log.warning(
                "Profile agent produced zero suggestions for batch. "
                "Raw reply saved to %s",
                LAST_PROFILE_RESPONSE_FILE,
            )
            return []

        return apply_logprob_confidence(suggestions, _logprobs)

    def _save_failed_response_for_debug(self, response: str, ctx: AgentContext) -> None:
        """Persist the model output when nothing could be parsed (inspect off-line)."""
        try:
            header = (
                f"# AMX profile agent — raw LLM reply (all parsers failed)\n"
                f"# schema={ctx.schema} table={ctx.table}\n"
                f"# ---\n\n"
            )
            Path(LAST_PROFILE_RESPONSE_FILE).write_text(
                header + (response or ""), encoding="utf-8"
            )
        except OSError as exc:
            log.debug("Could not write %s: %s", LAST_PROFILE_RESPONSE_FILE, exc)

    def _parse_by_known_column_names(
        self, text: str, ctx: AgentContext
    ) -> list[MetadataSuggestion]:
        """Last resort: match each profiled column name in the response and grab the line/phrase after it."""
        out: list[MetadataSuggestion] = []
        cols = (ctx.db_profile or {}).get("columns") or []
        for col in cols:
            name = str(col.get("name", "")).strip()
            if len(name) < 1:
                continue
            desc = self._description_after_column_name(text, name)
            if not desc:
                continue
            out.append(
                MetadataSuggestion(
                    schema=ctx.schema,
                    table=ctx.table,
                    column=name,
                    suggestions=[desc],
                    confidence=Confidence.MEDIUM,
                    reasoning="Matched known column name in free-form LLM text",
                    source="db_profile",
                )
            )
        return out

    def _description_after_column_name(self, text: str, name: str) -> str | None:
        """Find `NAME: ...` or `**NAME** ...` style lines in Markdown-ish output."""
        escaped = re.escape(name)
        flags = re.MULTILINE | re.IGNORECASE
        patterns = [
            rf"^\s*[-*]\s*\**{escaped}\**(?:\s*[\u2013\-:])+\s*(.+)$",
            rf"^\s*\**{escaped}\**(?:\s*[\u2013\-:])+\s*(.+)$",
            rf"^\s*COLUMN:?\s*{escaped}\s*[:\-]\s*(.+)$",
            rf"(?:^|\n)\s*#{1,4}\s+{escaped}\s*[:\-]?\s*(.+)$",
        ]
        for pat in patterns:
            m = re.search(pat, text, flags)
            if m:
                line = m.group(1).strip().strip("*`")
                if len(line) > 5:
                    return line[:2000]
        m2 = re.search(
            rf"{escaped}\s*[\u2013\-–:]\s*(.+?)(?:\n|$)",
            text,
            flags,
        )
        if m2:
            frag = m2.group(1).strip()
            if len(frag) > 5:
                return frag[:2000]
        return None

    def _build_prompt(self, ctx: AgentContext) -> str:
        pd = self._prompt_detail
        p = ctx.db_profile
        lines = [
            f"Database: {ctx.existing_metadata.get('database', 'N/A')}",
            f"Schema: {ctx.schema}",
            f"Table: {ctx.table}",
            f"Row count: {p.get('row_count', 'N/A')}",
        ]

        # ── Usage stats (pg_stat) ─────────────────────────────────────────────
        if pd.include_usage_stats:
            lines.append(
                f"Usage stats ({p.get('stats_source', 'database')}): "
                f"seq_scan={p.get('stats_seq_scan', 0)}, "
                f"idx_scan={p.get('stats_idx_scan', 0)}, "
                f"n_live_tup={p.get('stats_n_live_tup', 0)}"
            )

        # ── Existing comments ────────────────────────────────────────────────
        lines.append(f"Existing table comment: {p.get('existing_comment') or 'None'}")
        if pd.include_schema_db_comments:
            lines.append(f"Existing schema comment: {p.get('schema_comment') or 'None'}")
            lines.append(f"Existing database comment: {p.get('database_comment') or 'None'}")

        # ── Keys and constraints ────────────────────────────────────────────
        if pd.include_pk_fk:
            lines.append(f"Primary key: {p.get('primary_key') or []}")
            lines.append(f"Outgoing foreign keys (upstream dependencies): {p.get('foreign_keys') or []}")
            lines.append(f"Incoming foreign keys (downstream dependents): {p.get('referenced_by') or []}")
        if pd.include_unique_check:
            lines.append(f"Unique constraints: {p.get('unique_constraints') or []}")
            lines.append(f"Check constraints: {p.get('check_constraints') or []}")

        # ── FK neighbour comments ───────────────────────────────────────────
        if pd.include_related_comments:
            related = p.get("related_comments", []) or []
            if related:
                lines.append("")
                lines.append("Related table comments (FK neighbors):")
                for rel in related:
                    lines.append(
                        f"  - {rel.get('schema')}.{rel.get('table')}: "
                        f"{rel.get('comment') or 'None'}"
                    )

        # ── Columns ────────────────────────────────────────────────────────
        lines.extend(["", "Columns:"])
        for col in p.get("columns", []):
            parts = [
                f"  - {col['name']}",
                f"type={col['dtype']}",
            ]
            if pd.include_null_counts:
                parts.append(f"nulls={col['null_count']}/{col['row_count']}")
            if pd.include_cardinality:
                parts.append(f"distinct={col['distinct_count']}")
                parts.append(f"cardinality_ratio={col.get('cardinality_ratio', 0.0):.4f}")
            if pd.include_min_max:
                parts.append(f"min={col['min_val']}")
                parts.append(f"max={col['max_val']}")
            if pd.include_samples and col.get("samples"):
                samples = col["samples"][: pd.max_samples]
                parts.append(f"samples={samples}")
            if pd.include_existing_col_comment:
                parts.append(f"existing_comment={col.get('existing_comment') or 'None'}")
            lines.append(" | ".join(parts))

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

    def _parse_response_loose(self, text: str, ctx: AgentContext) -> list[MetadataSuggestion]:
        """Fallback when the model ignores the exact COLUMN:/DESCRIPTION_1: template."""
        suggestions: list[MetadataSuggestion] = []
        t = text.strip()
        t = re.sub(r"^```[a-z]*\s*\n", "", t, flags=re.IGNORECASE)
        t = re.sub(r"\n```\s*$", "", t)

        m_tbl = re.search(r"(?im)TABLE_DESCRIPTION:\s*([^\n]+)", t)
        if m_tbl:
            desc = m_tbl.group(1).strip().strip("*`")
            if desc:
                suggestions.append(
                    MetadataSuggestion(
                        schema=ctx.schema,
                        table=ctx.table,
                        column=None,
                        suggestions=[desc[:2000]],
                        confidence=Confidence.MEDIUM,
                        reasoning="Loose parse (table)",
                        source="db_profile",
                    )
                )

        # Split into COLUMN blocks (markdown-tolerant)
        col_iter = list(
            re.finditer(
                r"(?im)(?:^|\n)\s*#*\s*\*{0,2}COLUMN:?\*{0,2}\s*([A-Za-z0-9_]+)\s*",
                t,
            )
        )
        for i, m in enumerate(col_iter):
            col_name = m.group(1).strip()
            start = m.end()
            end = col_iter[i + 1].start() if i + 1 < len(col_iter) else len(t)
            block = t[start:end]
            descs = self._extract_descriptions_from_block(block)
            if not descs:
                continue
            suggestions.append(
                MetadataSuggestion(
                    schema=ctx.schema,
                    table=ctx.table,
                    column=col_name,
                    suggestions=descs[:5],
                    confidence=Confidence.MEDIUM,
                    reasoning="Loose parse from LLM output",
                    source="db_profile",
                )
            )

        return suggestions

    def _extract_descriptions_from_block(self, block: str) -> list[str]:
        descs: list[str] = []
        for line in block.splitlines():
            line = line.strip()
            if not line:
                continue
            m = re.match(
                r"(?i)(?:DESCRIPTION_\d+|Description|[-*]\s*|\d+\.\s*)(?:[:.]?\s*)(.+)",
                line,
            )
            if m:
                d = m.group(1).strip().strip("*`")
                if len(d) > 3:
                    descs.append(d)
            elif re.match(r"(?i)CONFIDENCE:|REASONING:", line):
                continue
        if not descs:
            for line in block.splitlines():
                line = line.strip().strip("-*•` ")
                if 15 < len(line) < 2000 and not line.startswith("#"):
                    descs.append(line)
                    if len(descs) >= 3:
                        break
        return descs
