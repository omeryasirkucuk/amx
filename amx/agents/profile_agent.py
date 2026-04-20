"""Sub-agent: infer metadata from database profile (column stats, names, types)."""

from __future__ import annotations

import re
from pathlib import Path

from amx.agents.base import AgentContext, BaseAgent, Confidence, MetadataSuggestion
from amx.llm.provider import LLMProvider
from amx.utils.logging import LAST_PROFILE_RESPONSE_FILE, LOG_DIR, get_logger

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

    BATCH_SIZE = 10

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
            batch_suggestions = self._run_single_batch(batch_ctx, batch)
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

    def _run_single_batch(
        self, ctx: AgentContext, columns: list
    ) -> list[MetadataSuggestion]:
        user_msg = self._build_prompt(ctx)
        log.debug(
            "Profile agent prompt for %s.%s: %d chars, %d columns",
            ctx.schema, ctx.table, len(user_msg), len(columns),
        )
        try:
            response = self.llm.chat([
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_msg},
            ])
        except Exception as exc:
            log.error("LLM call failed in profile agent: %s", exc)
            return []

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
        return suggestions

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
        p = ctx.db_profile
        lines = [
            f"Database: {ctx.existing_metadata.get('database', 'N/A')}",
            f"Schema: {ctx.schema}",
            f"Table: {ctx.table}",
            f"Row count: {p.get('row_count', 'N/A')}",
            f"Usage stats (pg_stat_user_tables): seq_scan={p.get('stats_seq_scan', 0)}, idx_scan={p.get('stats_idx_scan', 0)}, n_live_tup={p.get('stats_n_live_tup', 0)}",
            f"Existing table comment: {p.get('existing_comment') or 'None'}",
            f"Existing schema comment: {p.get('schema_comment') or 'None'}",
            f"Existing database comment: {p.get('database_comment') or 'None'}",
            f"Primary key: {p.get('primary_key') or []}",
            f"Outgoing foreign keys (upstream dependencies): {p.get('foreign_keys') or []}",
            f"Incoming foreign keys (downstream dependents): {p.get('referenced_by') or []}",
            f"Unique constraints: {p.get('unique_constraints') or []}",
            f"Check constraints: {p.get('check_constraints') or []}",
            "",
            "Related table comments (FK neighbors):",
        ]
        for rel in p.get("related_comments", []):
            lines.append(
                f"  - {rel.get('schema')}.{rel.get('table')}: {rel.get('comment') or 'None'}"
            )
        lines.extend(
            [
                "",
            "Columns:",
            ]
        )
        for col in p.get("columns", []):
            lines.append(
                f"  - {col['name']} | type={col['dtype']} | "
                f"nulls={col['null_count']}/{col['row_count']} | "
                f"distinct={col['distinct_count']} | "
                f"cardinality_ratio={col.get('cardinality_ratio', 0.0):.4f} | "
                f"min={col['min_val']} | max={col['max_val']} | "
                f"samples={col['samples']} | "
                f"existing_comment={col.get('existing_comment') or 'None'}"
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
