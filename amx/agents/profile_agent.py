"""Sub-agent: infer metadata from database profile (column stats, names, types)."""

from __future__ import annotations

import re
from pathlib import Path

from amx.agents._prompt_helpers import alternatives_mode_directive
from amx.agents.base import (
    AgentContext,
    BaseAgent,
    Confidence,
    MetadataSuggestion,
    apply_confidence_signals,
    apply_logprob_confidence,
)
from amx.config import DEFAULT_ALTERNATIVES_MODE, PromptDetail
from amx.llm.prompts import (
    ALTERNATIVES_LENGTH_RULE_REMINDER,
    length_rule,
    per_col_token_budget,
)
from amx.llm.provider import FatalLLMError, LLMProvider
from amx.llm.style.guard import scrub_placeholders
from amx.llm.style.injector import render_style_section
from amx.llm.style.loader import load_active_style_profile
from amx.llm.style.profile import StyleProfile
from amx.utils.console import step_spinner
from amx.utils.logging import LAST_PROFILE_RESPONSE_FILE, get_logger
from amx.utils.token_tracker import estimate_tokens, tracker

log = get_logger("agents.profile")


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
You are a data-catalog expert. Given database profile information for a table
and its columns, infer what each column likely represents.

Output rules:
- Write every description and reasoning string in **clear, business-friendly American English**.
- Use complete sentences ending with a period.
- Length rule (CRITICAL — honour the user's verbosity preset): {description_length_rule}
- Keep the response labels (`COLUMN`, `DESCRIPTION_1`, `CONFIDENCE`, `REASONING`, `TABLE_DESCRIPTION_1`, etc.) verbatim.

Write descriptions assertively and directly (e.g. "Telephone extension number." or "Indicates the fax number.").
Do NOT start descriptions with "This column likely represents" or "This column likely is".
Ground every claim in the provided profile, keys, comments, samples, stats, or usage hints.
If evidence is weak, stay generic but still useful; do not invent business jargon, vendor-specific module names, legal meanings, or workflow claims that are not supported.
Confidence rules:
- HIGH: multiple strong clues agree (name + dtype + keys/comments/usage/samples).
- MEDIUM: some plausible evidence exists but business meaning is not fully proven.
- LOW: only weak naming or type clues exist; prefer a careful generic description.
Reasoning must cite concrete evidence categories, not vague statements.
Do not copy existing comments verbatim unless they are already the clearest available description.
If a column cannot be resolved precisely, prefer a broader neutral description over hallucinating a specific one.
Coverage rule (CRITICAL): emit one COLUMN block for EVERY column listed in the input — never skip a column. `DESCRIPTION_1` must be a non-empty sentence; if evidence is thin, write a brief generic best-effort description and mark `CONFIDENCE: LOW` rather than leaving it blank or omitting the block.

For EACH column provide:
1. {description_length_rule}
{alt_instruction}
{alternatives_mode_directive}
{alternatives_length_reminder}
{extra_items}
A confidence level: HIGH / MEDIUM / LOW.
Brief reasoning for your choice.

Respond in this exact format for each column (one block per column):

COLUMN: <column_name>
DESCRIPTION_1: <most likely description>
{self_decl_line_1}{desc_lines}
CONFIDENCE: <HIGH|MEDIUM|LOW>
REASONING: <why you think so>

Also include ONE table-level description block (even when processing column batches):
TABLE_DESCRIPTION_1: <most likely table description>
{table_desc_lines}
TABLE_CONFIDENCE: <HIGH|MEDIUM|LOW>

Example style:
COLUMN: account_ref
DESCRIPTION_1: Account reference identifier.
CONFIDENCE: HIGH
REASONING: The column participates in key relationships, has identifier-like samples, and neighboring columns provide account context.
"""


def _build_system_prompt(
    n_alternatives: int,
    description_verbosity: str = "brief",
    style_profile: StyleProfile | None = None,
    emit_self_decl: bool = False,
    alternatives_mode: str = DEFAULT_ALTERNATIVES_MODE,
) -> str:
    """Build the system prompt dynamically for the requested number of alternatives.

    ``description_verbosity`` controls the LENGTH of generated descriptions:
    * ``brief`` (default): 1-2 short sentences.
    * ``detailed``: 2-4 sentences (≈2× brief output tokens).
    * ``comprehensive``: 1-2 short paragraphs / ~5-8 sentences (≈4-6× brief).
    * ``exhaustive``: multi-paragraph reference-style entry (≈8-12× brief);
      best for documentation, not interactive runs.

    ``emit_self_decl`` (Phase 2 confidence scoring) makes the model emit
    a ``CONFIDENCE_i: HIGH|MED|LOW`` line immediately after each
    ``DESCRIPTION_i`` block, so AMX can extract a per-alternative
    self-declared confidence. The legacy aggregate ``CONFIDENCE:`` line
    at the end of the block stays in place for backwards compatibility.
    """
    n = max(1, min(5, n_alternatives))
    if n == 1:
        alt_instruction = ""
        extra_items = ""
        desc_lines = ""
        table_desc_lines = ""
        alternatives_length_reminder = ""
        self_decl_line_1 = "CONFIDENCE_1: <HIGH|MED|LOW>\n" if emit_self_decl else ""
    else:
        alt_instruction = f"Up to {n} alternative descriptions ranked by likelihood."
        extra_items = ""
        if emit_self_decl:
            desc_lines = "\n".join(
                f"DESCRIPTION_{i}: <alternative description — apply the SAME length rule as DESCRIPTION_1>\n"
                f"CONFIDENCE_{i}: <HIGH|MED|LOW>"
                for i in range(2, n + 1)
            )
            self_decl_line_1 = "CONFIDENCE_1: <HIGH|MED|LOW>\n"
        else:
            desc_lines = "\n".join(
                f"DESCRIPTION_{i}: <alternative description — apply the SAME length rule as DESCRIPTION_1>"
                for i in range(2, n + 1)
            )
            self_decl_line_1 = ""
        table_desc_lines = "\n".join(
            f"TABLE_DESCRIPTION_{i}: <alternative table description — apply the SAME length rule as TABLE_DESCRIPTION_1>"
            for i in range(2, n + 1)
        )
        alternatives_length_reminder = ALTERNATIVES_LENGTH_RULE_REMINDER
    description_length_rule = length_rule(description_verbosity)
    return (
        _BASE_SYSTEM_PROMPT.format(
            description_length_rule=description_length_rule,
            alt_instruction=alt_instruction,
            alternatives_mode_directive=alternatives_mode_directive(
                alternatives_mode, n
            ),
            alternatives_length_reminder=alternatives_length_reminder,
            extra_items=extra_items,
            desc_lines=desc_lines,
            self_decl_line_1=self_decl_line_1,
            table_desc_lines=table_desc_lines,
        ).strip()
        + "\n"
        + render_style_section(style_profile)
    )


class ProfileAgent(BaseAgent):
    name = "profile_agent"

    def __init__(self, llm: LLMProvider):
        self.llm = llm
        self._diagnostics: list[str] = []
        self._style_profile = load_active_style_profile()

    def consume_diagnostics(self) -> list[str]:
        diagnostics = list(self._diagnostics)
        self._diagnostics.clear()
        return diagnostics

    def _record_diagnostic(self, message: str) -> None:
        if message:
            self._diagnostics.append(message)

    @property
    def batch_size(self) -> int:
        return max(1, getattr(self.llm.cfg, "column_batch_size", 10))

    @property
    def _n_alternatives(self) -> int:
        return max(1, min(5, getattr(self.llm.cfg, "n_alternatives", 3)))

    def _per_col_token_budget(self) -> int:
        return per_col_token_budget(getattr(self.llm.cfg, "description_verbosity", "brief"))

    @property
    def _prompt_detail(self) -> PromptDetail:
        return self.llm.cfg.prompt_detail_cfg

    def _profile_batch_workers(self, n_batches: int) -> int:
        """Choose safe concurrency for profile batches based on provider type."""
        provider = (self.llm.cfg.provider or "").lower().strip()
        if provider in {"ollama", "local", "kimi"}:
            return 1
        return min(5, n_batches)

    def run(self, ctx: AgentContext) -> list[MetadataSuggestion]:
        self._diagnostics.clear()
        profile = ctx.db_profile
        if not profile:
            return []

        columns = list(profile.get("columns") or [])
        if not columns:
            return []

        if len(columns) <= self.batch_size:
            return self._run_single_batch(ctx, columns)

        all_suggestions: list[MetadataSuggestion] = []
        batches = [
            columns[i : i + self.batch_size] for i in range(0, len(columns), self.batch_size)
        ]

        from concurrent.futures import ThreadPoolExecutor, as_completed

        max_workers = self._profile_batch_workers(len(batches))

        if max_workers == 1:
            for idx, batch in enumerate(batches, 1):
                col_names = ", ".join(c["name"] for c in batch)
                log.info(
                    "Profile agent batch %d/%d (%d cols: %s)",
                    idx,
                    len(batches),
                    len(batch),
                    col_names,
                )
                try:
                    batch_ctx = self._ctx_with_columns(ctx, batch)
                    res = self._run_single_batch(
                        batch_ctx,
                        batch,
                        batch_label=f"batch {idx}/{len(batches)}",
                    )
                    if res:
                        all_suggestions.extend(res)
                except FatalLLMError:
                    # Fatal errors (auth / quota / payment / model-not-found)
                    # propagate immediately; the orchestrator catches them at
                    # ``process_table`` so the whole /run aborts with one
                    # actionable message, instead of producing 200+ identical
                    # warnings while iterating through tables.
                    raise
                except Exception as exc:
                    self._record_diagnostic(
                        f"Profile Agent batch {idx}/{len(batches)} failed: {exc}"
                    )
                    log.error("Profile agent batch %d failed: %s", idx, exc)
        else:
            from amx.utils.live_display import run_in_thread

            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                fut_to_batch = {}
                for idx, batch in enumerate(batches, 1):
                    col_names = ", ".join(c["name"] for c in batch)
                    log.info(
                        "Profile agent batch %d/%d (%d cols: %s)",
                        idx,
                        len(batches),
                        len(batch),
                        col_names,
                    )
                    batch_ctx = self._ctx_with_columns(ctx, batch)
                    # ``run_in_thread`` snapshots the parent's
                    # ``contextvars`` (subscriber bus + correlation IDs)
                    # so ``step_spinner`` events emitted *inside* this
                    # batch's worker thread reach the SSE bridge the
                    # web worker installed on the main thread.
                    fut = run_in_thread(
                        ex,
                        self._run_single_batch,
                        batch_ctx,
                        batch,
                        batch_label=f"batch {idx}/{len(batches)}",
                    )
                    fut_to_batch[fut] = idx

                fatal_to_raise: FatalLLMError | None = None
                for fut in as_completed(fut_to_batch):
                    try:
                        res = fut.result()
                        if res:
                            all_suggestions.extend(res)
                    except FatalLLMError as fatal:
                        # Capture the first fatal so we can cancel siblings
                        # and propagate after the executor drains.
                        if fatal_to_raise is None:
                            fatal_to_raise = fatal
                        for other_fut in fut_to_batch:
                            other_fut.cancel()
                    except Exception as exc:
                        idx = fut_to_batch[fut]
                        self._record_diagnostic(
                            f"Profile Agent batch {idx}/{len(batches)} failed: {exc}"
                        )
                        log.error("Profile agent batch %d failed: %s", idx, exc)
                if fatal_to_raise is not None:
                    raise fatal_to_raise

        if not all_suggestions:
            self._record_diagnostic(
                f"Profile Agent produced zero suggestions for {ctx.schema}.{ctx.table}."
            )
            log.warning(
                "Profile agent produced zero suggestions across %d batches for %s.%s.",
                len(batches),
                ctx.schema,
                ctx.table,
            )
            return all_suggestions

        # Deduplicate table-level (column=None) suggestions across batches:
        # Keep the first one encountered; discard duplicates from other batches.
        seen_table_level = False
        deduped: list = []
        for s in all_suggestions:
            if s.column is None:
                if seen_table_level:
                    continue
                seen_table_level = True
            deduped.append(s)
        return deduped

    def _ctx_with_columns(self, ctx: AgentContext, columns: list) -> AgentContext:
        """Return a shallow copy of the context with only the specified columns."""
        full_columns = list((ctx.db_profile or {}).get("columns") or [])
        current_names = {str(c.get("name", "")).strip() for c in columns}
        remaining_names = [
            str(c.get("name", "")).strip()
            for c in full_columns
            if str(c.get("name", "")).strip()
            and str(c.get("name", "")).strip() not in current_names
        ]
        extra_setting = int(getattr(self.llm.cfg, "batch_context_column_names", 0))
        if extra_setting == -1:
            context_names = remaining_names
        elif extra_setting > 0:
            context_names = remaining_names[:extra_setting]
        else:
            context_names = []

        new_profile = dict(ctx.db_profile)
        new_profile["columns"] = columns
        new_profile["context_column_names"] = context_names
        return AgentContext(
            schema=ctx.schema,
            table=ctx.table,
            column=ctx.column,
            db_profile=new_profile,
            rag_context=ctx.rag_context,
            code_context=ctx.code_context,
            existing_metadata=ctx.existing_metadata,
        )

    def collect_messages(self, ctx: AgentContext) -> list:
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
            if len(columns) <= self.batch_size
            else [columns[i : i + self.batch_size] for i in range(0, len(columns), self.batch_size)]
        )
        requests: list[BatchRequest] = []
        for idx, batch in enumerate(batches):
            batch_ctx = self._ctx_with_columns(ctx, batch)
            msgs = self._build_messages(batch_ctx)

            mt = max(self.llm.cfg.max_tokens, len(batch) * self._per_col_token_budget())
            requests.append(
                BatchRequest(
                    custom_id=f"profile:{ctx.schema}:{ctx.table}:{idx}",
                    messages=msgs,
                    max_tokens=mt,
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
        suggestions = _scrub_suggestions(suggestions, self._style_profile)
        return suggestions

    def _build_messages(self, ctx: AgentContext) -> list[dict[str, str]]:
        """Build the messages list for a single profile batch — shared by run() and collect_messages()."""
        user_msg = self._build_prompt(ctx)
        # The system prompt only emits ``CONFIDENCE_i:`` per-alternative
        # markers when the active confidence signal is self-declaration,
        # since that is the only scorer that consumes them. Other
        # signals (logprob span, self-consistency, judge) don't need the
        # extra prompt real estate so we skip those lines entirely.
        conf_cfg = getattr(self.llm.cfg, "confidence", None)
        active_signal = getattr(self.llm.cfg, "confidence_signal", "none")
        emit_self_decl = bool(
            conf_cfg and getattr(conf_cfg, "enabled", True) and active_signal == "self_decl"
        )
        system = _build_system_prompt(
            self._n_alternatives,
            description_verbosity=getattr(self.llm.cfg, "description_verbosity", "brief"),
            style_profile=self._style_profile,
            emit_self_decl=emit_self_decl,
            alternatives_mode=getattr(
                self.llm.cfg, "alternatives_mode", DEFAULT_ALTERNATIVES_MODE
            ),
        )
        return [
            {"role": "system", "content": system},
            {"role": "user", "content": user_msg},
        ]

    def _run_single_batch(
        self,
        ctx: AgentContext,
        columns: list,
        *,
        batch_label: str = "",
        _is_retry: bool = False,
    ) -> list[MetadataSuggestion]:
        messages = self._build_messages(ctx)
        log.debug(
            "Profile agent prompt for %s.%s: %d chars, %d columns",
            ctx.schema,
            ctx.table,
            len(messages[-1]["content"]),
            len(columns),
        )
        est = estimate_tokens(messages)
        label = f"Profile Agent {batch_label}" if batch_label else "Profile Agent"

        # Per-column output budget scales with description_verbosity so a long
        # batch in `comprehensive`/`exhaustive` mode doesn't truncate halfway
        # through — truncation is the dominant cause of empty/missing per-column
        # outputs in dense batches.
        mt = max(self.llm.cfg.max_tokens, len(columns) * self._per_col_token_budget())

        try:
            with step_spinner(label, token_estimate=est):
                result = self.llm.chat(messages, max_tokens=mt)
        except Exception as exc:
            self._record_diagnostic(f"{label} failed: {exc}")
            log.error("LLM call failed in profile agent: %s", exc)
            return []

        tracker.record_for("profile_agent", est, self.llm, result.usage)
        response = result.content
        _logprobs = result.logprobs

        if not response or not response.strip():
            self._record_diagnostic(
                f"{label} returned an empty response for {ctx.schema}.{ctx.table}."
            )
            log.warning(
                "LLM returned an EMPTY response for %s.%s (%d columns). "
                "Check model name, API key, and billing on the provider dashboard.",
                ctx.schema,
                ctx.table,
                len(columns),
            )
            return []

        suggestions = self._parse_response(response, ctx)
        if not suggestions and len(response.strip()) > 20:
            log.warning("Strict parse found no COLUMN:/DESCRIPTION_ blocks; trying loose parser.")
            suggestions = self._parse_response_loose(response, ctx)
        if not suggestions:
            suggestions = self._parse_by_known_column_names(response, ctx)
        suggestions = _scrub_suggestions(suggestions, self._style_profile)

        if not suggestions:
            self._save_failed_response_for_debug(response, ctx)
            self._record_diagnostic(
                f"{label} returned text that AMX could not parse for {ctx.schema}.{ctx.table}. "
                f"Raw reply saved to {LAST_PROFILE_RESPONSE_FILE}."
            )
            log.warning(
                "Profile agent produced zero suggestions for batch. Raw reply saved to %s",
                LAST_PROFILE_RESPONSE_FILE,
            )
            return []

        suggestions = apply_logprob_confidence(
            suggestions,
            _logprobs,
            high_threshold=self.llm.cfg.logprob_high,
            medium_threshold=self.llm.cfg.logprob_medium,
            response_text=response,
        )

        try:
            apply_confidence_signals(
                suggestions=suggestions,
                logprobs_content=_logprobs,
                response_text=response,
                cfg=self.llm.cfg,
                llm=self.llm,
            )
        except Exception as exc:
            log.warning("Confidence signal scoring failed: %s", exc)

        if not _is_retry:
            suggestions = self._retry_missing_columns(
                ctx, columns, suggestions, batch_label=batch_label
            )

        return suggestions

    def _retry_missing_columns(
        self,
        ctx: AgentContext,
        requested_columns: list,
        suggestions: list[MetadataSuggestion],
        *,
        batch_label: str = "",
    ) -> list[MetadataSuggestion]:
        """One-shot targeted retry for columns the model skipped or returned blank.

        After the empty-description filter in the parser, a column is "missing"
        if no per-column suggestion was emitted for it. We re-prompt with just
        those columns; the smaller batch fits within ``max_tokens`` even if the
        first response was truncated, which is the most common failure mode.
        Bounded to a single retry so cost stays predictable on large tables.
        """
        requested_names = {
            str(c.get("name", "")).strip()
            for c in requested_columns
            if str(c.get("name", "")).strip()
        }
        if not requested_names:
            return suggestions
        returned_names = {s.column for s in suggestions if s.column}
        missing = sorted(requested_names - returned_names)
        if not missing:
            return suggestions

        missing_set = set(missing)
        missing_cols = [
            c for c in requested_columns if str(c.get("name", "")).strip() in missing_set
        ]
        if not missing_cols:
            return suggestions

        preview = ", ".join(missing[:5]) + (" …" if len(missing) > 5 else "")
        full_label = f"Profile Agent {batch_label}".strip()
        log.info(
            "%s: %d/%d columns missing or blank; retrying %s",
            full_label,
            len(missing),
            len(requested_names),
            preview,
        )
        self._record_diagnostic(
            f"Retrying {len(missing)} column(s) skipped or returned blank by the "
            f"model in {ctx.schema}.{ctx.table}: {preview}"
        )
        retry_label = f"{batch_label} retry".strip() or "retry"
        try:
            retry_ctx = self._ctx_with_columns(ctx, missing_cols)
            retry_suggestions = self._run_single_batch(
                retry_ctx,
                missing_cols,
                batch_label=retry_label,
                _is_retry=True,
            )
        except Exception as exc:
            log.warning("Retry batch failed: %s", exc)
            return suggestions

        # Drop retry's table-level suggestion; the original batch already produced one.
        for s in retry_suggestions:
            if s.column is not None:
                suggestions.append(s)
        return suggestions

    def _save_failed_response_for_debug(self, response: str, ctx: AgentContext) -> None:
        """Persist the model output when nothing could be parsed (inspect off-line)."""
        try:
            header = (
                f"# AMX profile agent — raw LLM reply (all parsers failed)\n"
                f"# schema={ctx.schema} table={ctx.table}\n"
                f"# ---\n\n"
            )
            Path(LAST_PROFILE_RESPONSE_FILE).write_text(header + (response or ""), encoding="utf-8")
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
            rf"(?:^|\n)\s*#{1, 4}\s+{escaped}\s*[:\-]?\s*(.+)$",
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
            lines.append(
                f"Outgoing foreign keys (upstream dependencies): {p.get('foreign_keys') or []}"
            )
            lines.append(
                f"Incoming foreign keys (downstream dependents): {p.get('referenced_by') or []}"
            )
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

        # ── Query-log/code usage hints ────────────────────────────────────────
        if pd.include_query_log_analysis:
            q = p.get("query_usage", {}) or {}
            if q:
                lines.append("")
                lines.append("Query usage analysis (derived from SQL/code references):")
                lines.append(
                    f"  - table_mentions={q.get('table_mentions', 0)}"
                    f", sql_like_table_mentions={q.get('sql_like_table_mentions', 0)}"
                    f", columns_with_mentions={q.get('columns_with_mentions', 0)}"
                )
                top_usage = q.get("top_column_usage", []) or []
                for row in top_usage[:10]:
                    col = row.get("column", "")
                    m = row.get("mentions", 0)
                    sample_lines = row.get("sample_sql_lines", []) or []
                    lines.append(f"  - {col}: mentions={m}")
                    for sl in sample_lines[:2]:
                        lines.append(f"      sample={sl}")

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

        context_names = list(p.get("context_column_names") or [])
        if context_names:
            lines.extend(
                [
                    "",
                    "Other column names in this table (context only):",
                    f"  {', '.join(context_names)}",
                ]
            )

        from amx.agents.base import _user_instructions_block

        return "\n".join(lines) + _user_instructions_block(ctx)

    def _parse_response(self, text: str, ctx: AgentContext) -> list[MetadataSuggestion]:
        text = re.sub(r"^```[a-zA-Z0-9_-]*\s*", "", (text or "").strip())
        text = re.sub(r"\s*```$", "", text).strip()
        suggestions: list[MetadataSuggestion] = []
        current_col: str | None = None
        descs: list[str] = []
        conf = Confidence.MEDIUM
        reasoning = ""
        table_descs: list[str] = []
        table_conf = Confidence.MEDIUM

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
                            source="db_profile",
                        )
                    )
                current_col = line.split(":", 1)[1].strip()
                descs = []
                conf = Confidence.MEDIUM
                reasoning = ""
            elif line.startswith("DESCRIPTION_"):
                # Drop blank `DESCRIPTION_n:` lines so the column isn't emitted
                # with an empty suggestion; missing-column retry then refills it.
                value = line.split(":", 1)[1].strip()
                if value:
                    descs.append(value)
            elif line.startswith("CONFIDENCE:"):
                raw = line.split(":", 1)[1].strip().upper()
                conf = Confidence[raw] if raw in Confidence.__members__ else Confidence.MEDIUM
            elif line.startswith("REASONING:"):
                reasoning = line.split(":", 1)[1].strip()
            # Match TABLE_DESCRIPTION_1:, TABLE_DESCRIPTION_2:, TABLE_DESCRIPTION: (legacy)
            elif re.match(r"TABLE_DESCRIPTION(?:_\d+)?:", line):
                value = line.split(":", 1)[1].strip()
                if value:
                    table_descs.append(value)
            elif line.startswith("TABLE_CONFIDENCE:"):
                tconf_str = line.split(":", 1)[1].strip().upper()
                table_conf = (
                    Confidence[tconf_str]
                    if tconf_str in Confidence.__members__
                    else Confidence.MEDIUM
                )

        if current_col and descs:
            suggestions.append(
                MetadataSuggestion(
                    schema=ctx.schema,
                    table=ctx.table,
                    column=current_col,
                    suggestions=descs,
                    confidence=conf,
                    reasoning=reasoning,
                    source="db_profile",
                )
            )

        # Append the table-level suggestion once with ALL alternatives
        if table_descs:
            suggestions.append(
                MetadataSuggestion(
                    schema=ctx.schema,
                    table=ctx.table,
                    column=None,
                    suggestions=table_descs,
                    confidence=table_conf,
                    reasoning="Inferred from table name, columns, and data profile",
                    source="db_profile",
                )
            )

        return suggestions

    def _parse_response_loose(self, text: str, ctx: AgentContext) -> list[MetadataSuggestion]:
        """Fallback when the model ignores the exact COLUMN:/DESCRIPTION_1: template."""
        suggestions: list[MetadataSuggestion] = []
        t = text.strip()
        t = re.sub(r"^```[a-z]*\s*\n", "", t, flags=re.IGNORECASE)
        t = re.sub(r"\n```\s*$", "", t)

        # Collect all TABLE_DESCRIPTION_N: / TABLE_DESCRIPTION: alternatives
        table_descs_loose = [
            m.group(1).strip().strip("*`")[:2000]
            for m in re.finditer(r"(?im)TABLE_DESCRIPTION(?:_\d+)?:\s*([^\n]+)", t)
            if m.group(1).strip()
        ]
        if table_descs_loose:
            tconf_str = "MEDIUM"
            m_tc = re.search(r"(?im)TABLE_CONFIDENCE:\s*(HIGH|MEDIUM|LOW)", t)
            if m_tc:
                tconf_str = m_tc.group(1).upper()
            tconf = (
                Confidence[tconf_str] if tconf_str in Confidence.__members__ else Confidence.MEDIUM
            )
            suggestions.append(
                MetadataSuggestion(
                    schema=ctx.schema,
                    table=ctx.table,
                    column=None,
                    suggestions=table_descs_loose,
                    confidence=tconf,
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
