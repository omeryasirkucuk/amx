"""Orchestrator: coordinate sub-agents, merge suggestions, and drive human-in-the-loop review."""

from __future__ import annotations

import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any

from amx.agents._orchestrator.merge import (
    MERGE_FILLUP_PROMPT as MERGE_FILLUP_PROMPT,  # noqa: PLC0414 - re-export for legacy callers
)
from amx.agents._orchestrator.merge import (
    MERGE_PROMPT as MERGE_PROMPT,  # noqa: PLC0414 - re-export for legacy callers
)
from amx.agents._orchestrator.merge import (
    MERGE_SYSTEM_PROMPT as MERGE_SYSTEM_PROMPT,  # noqa: PLC0414 - re-export for legacy callers
)
from amx.agents._orchestrator.merge import _strip_code_fences
from amx.agents._orchestrator.writeback import (
    _PLACEHOLDER_MARKERS as _PLACEHOLDER_MARKERS,  # noqa: PLC0414 - re-export for legacy callers
)
from amx.agents._orchestrator.writeback import (
    RowApplyOutcome as RowApplyOutcome,  # noqa: PLC0414 - re-export for legacy callers
)
from amx.agents._orchestrator.writeback import (
    _OldCommentReader as _OldCommentReader,  # noqa: PLC0414 - re-export for legacy callers
)
from amx.agents._orchestrator.writeback import (
    apply_review_results_to_db as apply_review_results_to_db,  # noqa: PLC0414 - re-export for legacy callers
)
from amx.agents._orchestrator.writeback import (
    create_live_writeback_progress as create_live_writeback_progress,  # noqa: PLC0414 - re-export for legacy callers
)
from amx.agents._orchestrator.writeback import (
    is_placeholder_description as is_placeholder_description,  # noqa: PLC0414 - re-export for legacy callers
)
from amx.agents.base import (
    AgentContext,
    Confidence,
    MetadataSuggestion,
    apply_logprob_confidence,
)
from amx.agents.code_agent import CodeAgent
from amx.agents.profile_agent import ProfileAgent
from amx.agents.rag_agent import RAGAgent
from amx.codebase.analyzer import CodebaseReport
from amx.config import DEFAULT_ALTERNATIVES_MODE
from amx.db.connector import AssetKind, ColumnProfile, DatabaseConnector, TableProfile
from amx.docs.rag import RAGStore
from amx.llm._provider_errors import FatalLLMError
from amx.llm.provider import LLMProvider
from amx.storage.sqlite_store import history_store
from amx.utils.console import (
    heading,
    info,
    step_spinner,
    success,
    warn,
)
from amx.utils.logging import get_logger
from amx.utils.token_tracker import tracker

log = get_logger("agents.orchestrator")

SCHEMA_META_PROMPT = """\
You are a data architect. Propose a concise description for the database SCHEMA: "{schema}".
Based on the following tables and their primary purposes:
{tables_summary}

Output rules:
- Write the description and reasoning in **clear, business-friendly American English**.
- One compact sentence. End with a period.
- Keep the response labels (`DESCRIPTION`, `CONFIDENCE`, `REASONING`) verbatim.

Summarize only the shared business or technical domain visible across the provided tables.
Do not invent organizational ownership, compliance scope, or process stages that are not evident from the summaries.

Respond in this exact format:
DESCRIPTION: <concise schema description>
CONFIDENCE: <HIGH|MEDIUM|LOW>
REASONING: <why>
"""

DATABASE_META_PROMPT = """\
You are a data architect. Propose a concise description for this DATABASE.
The following schemas and their purposes were identified:
{schemas_summary}

Output rules:
- Write the description and reasoning in **clear, business-friendly American English**.
- One compact sentence. End with a period.
- Keep the response labels (`DESCRIPTION`, `CONFIDENCE`, `REASONING`) verbatim.

Summarize only the common platform or business landscape implied by the schema summaries.
Do not invent enterprise-wide claims or implementation details that are not visible in the provided summaries.

Respond in this exact format:
DESCRIPTION: <concise database description>
CONFIDENCE: <HIGH|MEDIUM|LOW>
REASONING: <why>
"""

META_SYSTEM_PROMPT = """\
You summarize catalog structure conservatively.
Do not invent business scope beyond the provided table/schema summaries.
Return only the requested labeled fields.
"""


@dataclass
class ReviewResult:
    schema: str
    table: str
    column: str | None
    final_description: str
    confidence: Confidence
    source: str
    applied: bool = False
    asset_kind: str = "table"
    result_id: int | None = None  # FK to run_results.id (for re-evaluation)
    alternatives: list[str] = field(default_factory=list)
    logprob_score: float | None = None
    #: PR C: citation trail copied from the originating
    #: :class:`MetadataSuggestion` so the CLI run summary can render
    #: a "Sources" column without re-querying the run record. Empty
    #: list on non-RAG / merge-only results to keep the rendering
    #: branch a single ``if citations:`` check.
    citations: list = field(default_factory=list)


class RunCancelled(RuntimeError):
    """Signal raised when a run / apply / ask job has been cancelled.

    AMX Studio's ``/api/runs/{id}/cancel`` endpoint flips a
    :class:`threading.Event` plumbed through the orchestrator. Phase
    boundaries (per-table loop, post-profile, post-merge, write-back
    iteration) check the event and raise :class:`RunCancelled` so the
    JobRegistry can surface it as ``job.cancelled`` on the SSE stream.

    Cancellation is best-effort: in-flight LLM HTTP calls and
    SQLAlchemy transactions don't receive the signal, so latency is
    "one phase". The ``apply_review_results_to_db`` loop checks the
    event between rows and commits whatever was already applied —
    matching the CLI's Ctrl-C behaviour today.
    """


class Orchestrator:
    def __init__(
        self,
        db: DatabaseConnector,
        llm: LLMProvider,
        rag_store: RAGStore | None = None,
        code_report: CodebaseReport | None = None,
        run_id: int | None = None,
        search_profile: str = "default",
        missing_only: bool = False,
        *,
        rag_llm: LLMProvider | None = None,
    ):
        self.db = db
        self.llm = llm
        self.run_id = run_id
        self.search_profile = search_profile or "default"
        self.code_report = code_report
        self.profile_agent = ProfileAgent(llm)
        # ``rag_llm`` lets the caller pin the RAG agent to a different
        # LLM profile (cfg.rag_llm_profile) than the global one. None
        # falls back to the global ``llm`` so old call sites keep their
        # existing behaviour without a code change.
        self.rag_agent = RAGAgent(rag_llm or llm, rag_store) if rag_store else None
        self.code_agent = CodeAgent(llm, code_report) if code_report else None
        # ``missing_only`` skips tables that already have a table comment AND
        # every column already has a comment, and filters individual columns
        # that already have a comment so the agents only work on gaps.
        # See ``process_table`` for the per-table filter.
        self.missing_only = bool(missing_only)
        self.results: list[ReviewResult] = []
        # Equivalence-class dedup skip set. Columns in this set were
        # already handled by the upfront ``run_equivalence_pass`` (their
        # description is already written to catalog and, in apply mode,
        # to the live DB), so process_table filters them out of the
        # ProfileAgent batch. The orchestrator does NOT re-write them.
        self.dedup_skip_set: set[tuple[str, str, str]] = set()
        # Column-scope overrides. When the user picks Column scope,
        # the resolver populates ``column_overrides[(schema, table)] =
        # {col1, col2, ...}``. ``process_table`` then restricts
        # ``profile.columns`` to ONLY those columns — no filter is
        # applied for tables not in this map (so other scope levels
        # behave exactly as before).
        self.column_overrides: dict[tuple[str, str], set[str]] = {}
        # Ingested-asset context blocks indexed by lower-cased
        # ``(schema, table)``. Populated by the worker layer when the
        # user attaches notebook / query / stream / pipeline refs to
        # the run; the orchestrator copies the matching list into
        # :class:`AgentContext.asset_context` per-table so the
        # ProfileAgent prompt can render an "Ingested asset context"
        # section grounded in the actual usage patterns of each
        # referencing asset. Empty dict on normal runs preserves the
        # pre-PR4 behaviour byte-identically.
        self.asset_context_by_table: dict[tuple[str, str], list[dict[str, Any]]] = {}
        # Lineage-neighbour context blocks indexed by lower-cased
        # ``(schema, table)``. Populated by the worker layer from
        # ``catalog_relationships`` (foreign keys, view dependencies,
        # asset references, and ``/lineage fetch`` native edges). The
        # orchestrator copies the matching list into
        # :class:`AgentContext.lineage_context` per-table so the
        # ProfileAgent prompt can render a "Lineage context" section.
        # Empty dict on normal runs preserves the prior behaviour.
        self.lineage_context_by_table: dict[tuple[str, str], list[dict[str, Any]]] = {}

    _SQL_VERB_RE = re.compile(
        r"\b(select|insert|update|delete|merge|join|where|group\s+by|order\s+by)\b", re.IGNORECASE
    )

    def process_table(
        self,
        schema: str,
        table: str,
        asset_kind: AssetKind | None = None,
        interactive_review: bool = True,
        auto_apply: bool = False,
        *,
        apply: bool = True,
        cancel_token: threading.Event | None = None,
    ) -> list[ReviewResult]:
        """Run the per-table flow.

        This is now a thin delegator — every phase (filter chain, agent
        loop, apply / review dispatch) lives on
        :class:`TableProcessor` so each step is independently
        testable.

        ``cancel_token`` is the same :class:`threading.Event` AMX Studio
        already plumbs through ``apply_review_results_to_db`` and the
        per-asset loops in ``runs.py``. Today the token is forwarded to
        :class:`TableProcessor` and checked at phase boundaries so a
        cancel button click is observed within one phase's latency
        rather than waiting for the whole table to finish.
        Cooperative cancellation inside the LLM call lands in a
        follow-up PR.
        """
        from amx.agents._orchestrator import TableProcessor

        return TableProcessor(
            self,
            schema,
            table,
            asset_kind=asset_kind,
            auto_apply=auto_apply,
            interactive_review=interactive_review,
            apply=apply,
            cancel_token=cancel_token,
        ).run()

    def process_schema_meta(
        self,
        schema: str,
        table_results: list[ReviewResult],
        *,
        auto_apply: bool = False,
    ) -> list[ReviewResult]:
        """Infer description for the schema itself based on its tables.

        With ``auto_apply=True`` the produced ``ReviewResult`` is marked
        ``applied=True`` so the caller doesn't drag it through the
        human-review picker — same contract as ``process_table``.
        """
        heading(f"Analyzing Schema: {schema}")

        # Gather top-level table descriptions
        table_summaries = []
        for r in table_results:
            if r.column is None and r.schema == schema:
                table_summaries.append(f"Table: {r.table}\nDescription: {r.final_description}")

        if not table_summaries:
            log.info("No table descriptions found to summarize schema %s", schema)
            return []

        tables_text = "\n\n".join(table_summaries)
        prompt = SCHEMA_META_PROMPT.format(
            schema=schema,
            tables_summary=tables_text,
        )

        with step_spinner(f"Generating description for schema {schema}"):
            res = self.llm.chat(
                [
                    {"role": "system", "content": META_SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ]
            )

        desc, conf, reasoning = self._parse_meta_response(res.content)
        if not desc:
            return []

        result = ReviewResult(
            schema=schema,
            table="",
            column=None,
            final_description=desc,
            confidence=conf,
            source="combined",
            # auto-apply marks the meta result applied immediately so the
            # caller's batch_review skip + final apply step covers it.
            applied=bool(auto_apply),
            asset_kind=AssetKind.SCHEMA.value,
        )
        calibrated = apply_logprob_confidence(
            [
                MetadataSuggestion(
                    schema=schema,
                    table="",
                    column=None,
                    suggestions=[desc],
                    confidence=result.confidence,
                    reasoning=reasoning,
                    source="combined",
                )
            ],
            res.logprobs,
            high_threshold=self.llm.cfg.logprob_high,
            medium_threshold=self.llm.cfg.logprob_medium,
            response_text=res.content,
        )
        if calibrated:
            result.confidence = calibrated[0].confidence
            result.logprob_score = calibrated[0].logprob_score
        self.results.append(result)
        return [result]

    def process_database_meta(
        self,
        schema_results: list[ReviewResult],
        *,
        auto_apply: bool = False,
    ) -> list[ReviewResult]:
        """Infer description for the database itself based on its schemas.

        With ``auto_apply=True`` the produced ``ReviewResult`` is marked
        ``applied=True`` so the caller doesn't drag it through the
        human-review picker.
        """
        heading("Analyzing Database")

        schema_summaries = []
        for r in schema_results:
            if r.asset_kind == AssetKind.SCHEMA.value:
                schema_summaries.append(f"Schema: {r.schema}\nDescription: {r.final_description}")

        if not schema_summaries:
            log.info("No schema descriptions found to summarize database")
            return []

        schemas_text = "\n\n".join(schema_summaries)
        prompt = DATABASE_META_PROMPT.format(
            schemas_summary=schemas_text,
        )

        with step_spinner("Generating description for database"):
            res = self.llm.chat(
                [
                    {"role": "system", "content": META_SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ]
            )

        desc, conf, reasoning = self._parse_meta_response(res.content)
        if not desc:
            return []

        result = ReviewResult(
            schema="",
            table="",
            column=None,
            final_description=desc,
            confidence=conf,
            source="combined",
            applied=bool(auto_apply),
            asset_kind=AssetKind.DATABASE.value,
        )
        calibrated = apply_logprob_confidence(
            [
                MetadataSuggestion(
                    schema="",
                    table="",
                    column=None,
                    suggestions=[desc],
                    confidence=result.confidence,
                    reasoning=reasoning,
                    source="combined",
                )
            ],
            res.logprobs,
            high_threshold=self.llm.cfg.logprob_high,
            medium_threshold=self.llm.cfg.logprob_medium,
            response_text=res.content,
        )
        if calibrated:
            result.confidence = calibrated[0].confidence
            result.logprob_score = calibrated[0].logprob_score
        self.results.append(result)
        return [result]

    def _ensure_complete_table_coverage(
        self,
        profile: TableProfile,
        merged: list[MetadataSuggestion],
    ) -> list[MetadataSuggestion]:
        """Ensure table-level + every physical column has at least one suggestion.

        When model parsing misses some columns in large tables, keep them visible for review
        with a low-confidence fallback instead of silently dropping them.
        """
        out = list(merged)
        # Drop any table-level suggestion (and skip the fallback injection
        # below) when this run must not touch the table description —
        # column-scoped, or missing-only over a table that already has a
        # comment. Otherwise generating/writing a table comment here wastes
        # tokens and clobbers the existing one.
        skip_table = self._should_skip_table_level(profile)
        if skip_table:
            out = [s for s in out if s.column is not None]
        suggested_cols = {s.column for s in out if s.column is not None}
        has_table_level = any(s.column is None for s in out)

        # Pad both fallback paths to ``n_alternatives`` so a single-
        # alternative fallback row doesn't surface as ``A`` only when
        # the user's profile asked for ``A/B/C``. The duplicated
        # suggestions are all marked ``source="fallback"``; a reviewer
        # can pick any slot and use ``Variations`` on the chosen row
        # to regenerate real alternatives in one click.
        #
        # Read ``n_alternatives`` from the LLM provider's bound
        # ``LLMConfig`` (the Orchestrator stores the provider as
        # ``self.llm``; its ``.cfg`` is the per-run LLMConfig with
        # ``n_alternatives``). Defensive fall-back to 1 when the
        # attribute is missing — keeps the regression test that
        # passes a bare ``DummyLLM()`` working.
        llm_cfg = getattr(getattr(self, "llm", None), "cfg", None)
        n_alts = max(1, int(getattr(llm_cfg, "n_alternatives", 1) or 1))

        if not has_table_level and not skip_table:
            table_fallback_desc = (
                f"Table {profile.name} contains business data for schema "
                f"{profile.schema}. Auto-inference missed a reliable table "
                "description; please review manually."
            )
            out.append(
                MetadataSuggestion(
                    schema=profile.schema,
                    table=profile.name,
                    column=None,
                    suggestions=[table_fallback_desc] * n_alts,
                    confidence=Confidence.LOW,
                    reasoning=(
                        f"Fallback injected — the model produced 0 of the requested "
                        f"{n_alts} table-level suggestions. The {n_alts} entries "
                        "above are identical placeholders so a reviewer can still "
                        "pick one and continue; use ✨ Variations on the chosen "
                        "row to regenerate real alternatives."
                    ),
                    source="fallback",
                )
            )
            log.warning(
                "Fallback injected for %s.%s (table-level): padding to "
                "n_alternatives=%d. Investigate the agent output for missing "
                "TABLE_DESCRIPTION lines.",
                profile.schema,
                profile.name,
                n_alts,
            )

        for c in profile.columns:
            if c.name in suggested_cols:
                continue
            fallback_desc = (
                c.existing_comment
                or f"Column {c.name} in table {profile.name}. "
                "Auto-inference missed a reliable description; please review manually."
            )
            out.append(
                MetadataSuggestion(
                    schema=profile.schema,
                    table=profile.name,
                    column=c.name,
                    suggestions=[fallback_desc] * n_alts,
                    confidence=Confidence.LOW,
                    reasoning=(
                        f"Fallback injected — the model produced 0 of the requested "
                        f"{n_alts} suggestions for column {c.name}. The {n_alts} "
                        "entries above are identical placeholders so a reviewer can "
                        "still pick one and continue; use ✨ Variations on the "
                        "chosen row to regenerate real alternatives."
                    ),
                    source="fallback",
                )
            )
        return out

    def _parse_meta_response(self, text: str) -> tuple[str, Confidence, str]:
        """Parse meta DESCRIPTION/CONFIDENCE/REASONING blocks."""
        text = _strip_code_fences(text)
        desc = ""
        conf = Confidence.MEDIUM
        reasoning = ""

        lines = text.splitlines()
        for line in lines:
            if line.upper().startswith("DESCRIPTION:"):
                desc = line[12:].strip()
            elif line.upper().startswith("CONFIDENCE:"):
                c = line[11:].strip().upper()
                if "HIGH" in c:
                    conf = Confidence.HIGH
                elif "LOW" in c:
                    conf = Confidence.LOW
            elif line.upper().startswith("REASONING:"):
                reasoning = line[10:].strip()

        return desc, conf, reasoning

    def _run_enabled_agents(
        self,
        ctx: AgentContext,
        *,
        cancel_token: threading.Event | None = None,
    ) -> tuple[list[MetadataSuggestion], dict[str, str]]:
        """Run the enabled sub-agents and collect suggestions + per-agent
        statuses.

        Returns
        -------
        ``(suggestions, statuses)`` where ``suggestions`` is the
        flattened evidence list (legacy shape) and ``statuses`` maps
        each sub-agent label to one of ``"ok"`` / ``"failed"`` /
        ``"cancelled"``. The caller decides whether to surface the
        per-agent status in the run record / SSE stream — keeping the
        breakdown out of the merge step here so the merge logic stays
        agnostic of which agents fired.

        Cancellation
        ------------
        When ``cancel_token`` is set during the fan-out:

        * not-yet-started futures are dropped via
          ``ThreadPoolExecutor.shutdown(cancel_futures=True)`` (Python
          3.9+);
        * already-running futures keep running until the next phase
          boundary inside the agent — cooperative cancellation inside
          the LLM call is a follow-up PR;
        * any sub-agent that observed the cancel via ``RunCancelled``
          is marked ``"cancelled"`` instead of ``"failed"`` so the
          caller can distinguish "user clicked cancel" from "agent
          crashed".

        ``RunCancelled`` is re-raised after marking statuses so the
        caller's per-table loop can stop processing further tables.
        """
        jobs: list[tuple[str, object]] = [("profile", self.profile_agent)]
        if self.rag_agent:
            jobs.append(("rag", self.rag_agent))
        if self.code_agent:
            jobs.append(("code", self.code_agent))

        statuses: dict[str, str] = {label: "skipped" for label, _ in jobs}
        if not jobs:
            return [], statuses

        if len(jobs) == 1:
            label, agent = jobs[0]
            try:
                result = agent.run(ctx) or []
                statuses[label] = "ok"
                return result, statuses
            except RunCancelled:
                statuses[label] = "cancelled"
                raise
            except FatalLLMError:
                # Non-recoverable (auth / quota / model-not-found). Re-raise
                # so the run aborts at analyze_flow with one actionable
                # message instead of swallowing it here and churning through
                # every remaining table, failing identically and consuming
                # tokens each time. Mirrors the RunCancelled re-raise above.
                statuses[label] = "failed"
                raise
            except Exception as exc:
                statuses[label] = "failed"
                warn(f"{label.upper()} agent failed: {exc}")
                return [], statuses

        from amx.utils.live_display import run_in_thread

        out: list[MetadataSuggestion] = []
        cancel_observed = False
        pool = ThreadPoolExecutor(max_workers=len(jobs))
        try:
            # Use ``run_in_thread`` so each sub-agent's
            # ``step_spinner`` emits reach the subscriber bus the
            # web worker installed on the parent thread.
            fut_to_label = {run_in_thread(pool, agent.run, ctx): label for label, agent in jobs}
            for fut in as_completed(fut_to_label):
                label = fut_to_label[fut]
                try:
                    out.extend(fut.result() or [])
                    statuses[label] = "ok"
                except RunCancelled:
                    statuses[label] = "cancelled"
                    cancel_observed = True
                except FatalLLMError:
                    # Non-recoverable LLM error — re-raise so the run aborts
                    # with one actionable message rather than failing the
                    # same way on every table. The ``finally`` below still
                    # shuts the pool down cleanly before this propagates.
                    statuses[label] = "failed"
                    raise
                except Exception as exc:
                    statuses[label] = "failed"
                    warn(f"{label.upper()} agent failed: {exc}")
        finally:
            # ``cancel_futures=True`` drops anything that has not started
            # running yet. Already-running workers keep going — we can
            # only short-circuit them once cooperative cancel reaches
            # the LLM call (next PR).
            pool.shutdown(wait=True, cancel_futures=True)

        # If the caller's token flipped to set during the fan-out, we
        # must re-raise so the per-table loop stops scheduling further
        # tables. Otherwise the cancel only takes effect at the next
        # phase boundary (one extra table of latency).
        if cancel_observed or (cancel_token is not None and cancel_token.is_set()):
            raise RunCancelled("cancellation observed during agent fan-out")

        return out, statuses

    def _should_skip_table_level(self, profile: TableProfile) -> bool:
        """Whether to suppress table-level description generation for this
        table — so the agent doesn't spend tokens on, or overwrite, a
        table comment the run didn't ask for.

        True when EITHER:
        * the run is column-scoped (``column_overrides`` names this table)
          — only the picked columns are in scope; or
        * ``missing_only`` is set and the table already has a real
          (non-placeholder) comment — the table description is not
          "missing", so a missing-only run must leave it alone and only
          fill the column gaps.
        """
        if (profile.schema, profile.name) in self.column_overrides:
            return True
        if self.missing_only:
            existing = (profile.existing_comment or "").strip()
            if existing and not is_placeholder_description(existing):
                return True
        return False

    def _build_context(self, profile: TableProfile) -> AgentContext:
        db_name = self.db.cfg.database or self.db.cfg.project or self.db.cfg.catalog or "N/A"
        query_usage = self._build_query_usage_hints(profile)
        return AgentContext(
            schema=profile.schema,
            table=profile.name,
            asset_kind=profile.asset_kind.value,
            db_profile={
                "row_count": profile.row_count,
                "existing_comment": profile.existing_comment,
                "primary_key": profile.primary_key,
                "foreign_keys": profile.foreign_keys,
                "referenced_by": profile.referenced_by,
                "unique_constraints": profile.unique_constraints,
                "check_constraints": profile.check_constraints,
                "stats_seq_scan": profile.stats_seq_scan,
                "stats_idx_scan": profile.stats_idx_scan,
                "stats_n_live_tup": profile.stats_n_live_tup,
                "stats_source": self.db.stats_label,
                "schema_comment": profile.schema_comment,
                "database_comment": profile.database_comment,
                "related_comments": profile.related_comments,
                "query_usage": query_usage,
                "columns": [
                    {
                        "name": c.name,
                        "dtype": c.dtype,
                        "nullable": c.nullable,
                        "row_count": c.row_count,
                        "null_count": c.null_count,
                        "distinct_count": c.distinct_count,
                        "cardinality_ratio": c.cardinality_ratio,
                        "min_val": c.min_val,
                        "max_val": c.max_val,
                        "samples": c.samples,
                        "existing_comment": c.existing_comment,
                    }
                    for c in profile.columns
                ],
            },
            existing_metadata={
                "database": db_name,
                "backend": self.db.backend,
                "table_comment": profile.existing_comment,
                "schema_comment": profile.schema_comment,
                "database_comment": profile.database_comment,
            },
            asset_context=list(
                self.asset_context_by_table.get((profile.schema.lower(), profile.name.lower()), [])
            ),
            lineage_context=list(
                self.lineage_context_by_table.get(
                    (profile.schema.lower(), profile.name.lower()), []
                )
            ),
            # Suppress table-level generation when the run didn't ask for
            # it — column-scoped, or a missing-only run over a table that
            # already has a description. Saves tokens and protects the
            # existing table comment from being overwritten.
            skip_table_description=self._should_skip_table_level(profile),
        )

    def _build_query_usage_hints(self, profile: TableProfile) -> dict[str, object]:
        """Derive query-usage hints from code scan references (query-log-like context)."""
        if not self.code_report:
            return {}

        refs = self.code_report.references or {}
        table_key = profile.name.lower()
        table_refs = refs.get(table_key, [])
        col_names = {c.name.lower() for c in profile.columns}
        col_counts: dict[str, int] = {c.name: 0 for c in profile.columns}
        col_snippets: dict[str, list[str]] = {c.name: [] for c in profile.columns}

        sql_like_lines = 0
        for r in table_refs:
            text_line = (r.line_text or "").strip()
            if self._SQL_VERB_RE.search(text_line):
                sql_like_lines += 1

        # Column-level mention frequencies and first SQL-like snippets.
        for col in profile.columns:
            key = col.name.lower()
            hits = refs.get(key, [])
            col_counts[col.name] = len(hits)
            for h in hits:
                line = (h.line_text or "").strip()
                if line and self._SQL_VERB_RE.search(line):
                    col_snippets[col.name].append(line[:200])
                if len(col_snippets[col.name]) >= 2:
                    break

        top_cols = sorted(
            (name for name in col_counts if col_counts[name] > 0),
            key=lambda n: col_counts[n],
            reverse=True,
        )[:12]

        top_column_usage = [
            {
                "column": name,
                "mentions": col_counts[name],
                "sample_sql_lines": col_snippets.get(name, []),
            }
            for name in top_cols
        ]

        return {
            "table_mentions": len(table_refs),
            "sql_like_table_mentions": sql_like_lines,
            "top_column_usage": top_column_usage,
            "columns_with_mentions": sum(1 for c in col_names if refs.get(c)),
        }

    def _merge_suggestions(
        self, suggestions: list[MetadataSuggestion], ctx: AgentContext
    ) -> list[MetadataSuggestion]:
        from amx.agents._orchestrator.merge import merge_suggestions

        return merge_suggestions(self, suggestions, ctx)

    def _merge_fill_up(
        self,
        *,
        ctx: AgentContext,
        merge_results: list[MetadataSuggestion],
        missing_columns: list[str],
        contributors: dict[str | None, list[MetadataSuggestion]],
    ) -> list[MetadataSuggestion]:
        from amx.agents._orchestrator.merge import _merge_fill_up

        return _merge_fill_up(
            self,
            ctx=ctx,
            merge_results=merge_results,
            missing_columns=missing_columns,
            contributors=contributors,
        )

    @staticmethod
    def _parse_merge_response(
        text: str,
    ) -> dict[str, tuple[list[str], Confidence, str]]:
        from amx.agents._orchestrator.merge import parse_merge_response

        return parse_merge_response(text)

    # ── Persistence helpers ───────────────────────────────────────────────────

    def _save_merged_suggestions(
        self,
        suggestions: list[MetadataSuggestion],
        *,
        asset_kind: str = "table",
    ) -> dict[str | None, int]:
        """Save all LLM alternatives to run_results before user review.

        Returns {column_name: run_result_id} map so evaluations can be linked.
        """
        from amx.storage.sqlite_store import history_store

        hs = history_store()
        if hs is None or self.run_id is None:
            return {}
        active_alternatives_mode = getattr(
            self.llm.cfg, "alternatives_mode", DEFAULT_ALTERNATIVES_MODE
        )
        rows = [
            {
                "schema": s.schema,
                "table": s.table,
                "column": s.column,
                "asset_kind": getattr(asset_kind, "value", str(asset_kind)),
                "source": s.source,
                "confidence": s.confidence.value,
                "logprob_score": s.logprob_score,
                "raw_logprob": s.logprob_score,
                "model_version": self.llm.model_name,
                "reasoning": s.reasoning,
                "alternatives": s.suggestions,
                "alternatives_mode": active_alternatives_mode,
                # Phase 1 confidence: per-alternative score breakdown
                # serialised into the same alternatives_json column.
                # ``None`` when scoring was disabled / unavailable so
                # the storage layer falls back to the legacy list-of-
                # strings shape.
                "alternative_scores": (
                    [score.to_json() for score in s.suggestion_scores]
                    if getattr(s, "suggestion_scores", None)
                    else None
                ),
                # PR C: machine-readable provenance for RAG-derived
                # suggestions. Dataclass -> plain-dict conversion
                # happens here so the storage layer stays
                # JSON-only.
                "citations": [
                    {
                        "source": c.source,
                        "chunk_idx": c.chunk_idx,
                        "score": c.score,
                        "snippet": c.snippet,
                        # PR γ: ``line_range`` is serialised as a JSON
                        # array (``[start, end]``) when present so the
                        # frontend can render ``src/foo.py:120-145``.
                        # ``None`` for legacy doc citations keeps the
                        # field optional on the wire.
                        "line_range": (
                            list(c.line_range)
                            if getattr(c, "line_range", None) is not None
                            else None
                        ),
                    }
                    for c in (getattr(s, "citations", None) or [])
                ],
            }
            for s in suggestions
        ]
        try:
            ids = hs.save_run_results(self.run_id, rows)
        except Exception as exc:
            log.warning("Could not persist run_results: %s", exc)
            return {}
        # Map column_name → DB row id  (column=None → key None)
        return {s.column: rid for s, rid in zip(suggestions, ids, strict=False)}

    def _record_evaluation(
        self,
        result_id: int | None,
        *,
        chosen_description: str,
        evaluation: str,
    ) -> None:
        if result_id is None:
            return
        from amx.storage.sqlite_store import history_store

        hs = history_store()
        if hs is None:
            return
        try:
            hs.record_evaluation(
                result_id,
                chosen_description=chosen_description,
                evaluation=evaluation,
            )
        except Exception as exc:
            log.debug("Could not record evaluation: %s", exc)
        try:
            from amx.search.catalog import SearchCatalog

            catalog = SearchCatalog.from_history_store()
            if catalog is not None:
                catalog.sync_review_decision(
                    result_id,
                    chosen_description=chosen_description,
                    evaluation=evaluation,
                )
        except Exception as exc:
            log.debug("Could not sync review result into search catalog: %s", exc)

    def _sync_search_catalog(
        self,
        profile: TableProfile,
        suggestions: list[MetadataSuggestion],
        result_id_map: dict[str | None, int],
    ) -> None:
        try:
            from amx.search.catalog import SearchCatalog

            catalog = SearchCatalog.from_history_store()
            if catalog is None:
                return
            catalog.sync_generated_suggestions(
                db_profile=self.search_profile,
                db_backend=self.db.backend,
                database_name=(
                    self.db.cfg.database
                    or getattr(self.db.cfg, "catalog", "")
                    or getattr(self.db.cfg, "project", "")
                ),
                run_id=self.run_id,
                profile=profile,
                suggestions=suggestions,
                result_id_map=result_id_map,
                query_usage=self._build_query_usage_hints(profile),
            )
        except Exception as exc:
            log.debug("Could not sync generated metadata to search catalog: %s", exc)

    def _human_review(
        self,
        suggestions: list[MetadataSuggestion],
        schema: str,
        table: str,
        asset_kind: str = "table",
        result_id_map: dict[str | None, int] | None = None,
    ) -> list[ReviewResult]:
        from amx.agents._orchestrator.review import human_review

        return human_review(
            self,
            suggestions,
            schema,
            table,
            asset_kind=asset_kind,
            result_id_map=result_id_map,
        )

    def _review_single(
        self,
        s: MetadataSuggestion,
        is_table: bool,
        asset_kind: str = "table",
        result_id: int | None = None,
    ) -> ReviewResult:
        from amx.agents._orchestrator.review import review_single

        return review_single(
            self,
            s,
            is_table,
            asset_kind=asset_kind,
            result_id=result_id,
        )

    def _review_single_result(self, r: ReviewResult) -> ReviewResult:
        from amx.agents._orchestrator.review import review_single_result

        return review_single_result(self, r)

    def batch_review(self, results: list[ReviewResult]) -> list[ReviewResult]:
        from amx.agents._orchestrator.review import batch_review

        return batch_review(self, results)

    # ── Batch mode ────────────────────────────────────────────────────────────

    def _synthesize_profile_from_cache(
        self,
        schema: str,
        table: str,
        asset_kind: AssetKind | None,
    ) -> TableProfile:
        """Build a metadata-only TableProfile from the search catalog.

        Used when the Studio caller flagged this asset on the bulk
        run's ``cache_override_assets`` list — the live DB can't
        reflect the table (SQLAlchemy ``NoSuchTableError`` on
        ``get_columns``), but the catalog still has its column list
        from the last ``/search sync``. We synthesize a minimal
        profile so the LLM still has names, dtypes, and existing
        comments to work with. PK/FK, samples, min/max, and usage
        stats stay empty; the prompt builder already tolerates
        missing optional fields.

        Mirror of :func:`amx.web.routers.live_db._columns_from_cache`
        with a different return shape — same underlying reader so the
        Studio Browse page and the bulk worker stay in sync about
        what "from the catalog" means.
        """
        profile_name = self.db.cfg.active_db_profile or "default"
        database_name = (
            getattr(self.db.cfg.db, "database", None)
            or getattr(self.db.cfg.db, "catalog", None)
            or None
        )
        columns: list[ColumnProfile] = []
        try:
            from amx.search.catalog import SearchCatalog

            cat = SearchCatalog.from_history_store()
        except Exception:
            cat = None
        if cat is not None:
            try:
                cached = cat.fetch_columns_for_table(
                    profile_name,
                    schema_name=schema,
                    table_name=table,
                    database_name=database_name,
                )
            except Exception:
                cached = []
            for c in cached or []:
                columns.append(
                    ColumnProfile(
                        name=str(c.get("name", "")),
                        dtype=str(c.get("dtype") or ""),
                        nullable=bool(c.get("nullable", True)),
                        existing_comment=(c.get("comment") or None),
                    )
                )
        return TableProfile(
            schema=schema,
            name=table,
            asset_kind=asset_kind or AssetKind.TABLE,
            columns=columns,
        )

    def process_tables_batch_mode(
        self,
        schema: str,
        tables: list[str],
        asset_kinds: dict[str, AssetKind] | None = None,
        *,
        cancel_token: threading.Event | None = None,
        cache_override_assets: set[str] | None = None,
    ) -> list[ReviewResult]:
        """Run the full pipeline for *tables* via the provider's Batch API.

        Falls back to Chat Completions if the provider has no batch
        support, in which case ``cancel_token`` is forwarded to the
        per-table fallback loop. Inside the genuine batch path the
        token is observed between batch phases so a Studio cancel
        click stops further work even mid-batch.

        ``cache_override_assets`` carries the set of ``"schema.table"``
        identifiers the caller flagged as unreachable on the live DB
        — Studio's pre-flight check found them missing and the user
        chose "Use cached schema" in the reachability dialog. For
        those assets we substitute a catalog-cached metadata profile
        instead of calling :meth:`profile_table`.
        """
        from amx.llm.batch import BatchRequest, run_batch, supported_providers

        asset_kinds = asset_kinds or {}

        overrides = cache_override_assets or set()

        if not self.llm.supports_batch:
            warn(
                f"Provider '{self.llm.cfg.provider}' does not support batch mode "
                f"(supported: {', '.join(supported_providers())}). "
                "Falling back to Chat Completions."
            )
            all_results: list[ReviewResult] = []
            for table in tables:
                if cancel_token is not None and cancel_token.is_set():
                    raise RunCancelled(f"Cancelled before {schema}.{table}")
                all_results.extend(
                    self.process_table(
                        schema,
                        table,
                        asset_kind=asset_kinds.get(table),
                        cancel_token=cancel_token,
                    )
                )
            return all_results

        n_assets = len(tables)
        info(f"[Batch] Profiling {n_assets} asset(s)…")
        profiles: dict[str, TableProfile] = {}
        for table in tables:
            ak = asset_kinds.get(table)
            asset_path = f"{schema}.{table}"
            if asset_path in overrides:
                with step_spinner(f"Building cached-schema profile for {schema}.{table}"):
                    profiles[table] = self._synthesize_profile_from_cache(schema, table, ak)
            else:
                with step_spinner(f"Profiling {schema}.{table}"):
                    profiles[table] = self.db.profile_table(schema, table, asset_kind=ak)

        # Column scope: when the run targets specific columns, restrict each
        # profile to those columns (chat mode does this in
        # _filter_column_override; batch mode must too or it would describe
        # — and bill for — every column plus the table). The table-level
        # description is suppressed downstream via skip_table_description.
        for table, prof in list(profiles.items()):
            override_set = self.column_overrides.get((schema, table))
            if override_set is None:
                continue
            prof.columns = [c for c in prof.columns if c.name in override_set]
            if not prof.columns:
                info(f"[Batch] Column scope: no matching columns on {schema}.{table}; skipping.")
                del profiles[table]
        tables = [t for t in tables if t in profiles]
        if not profiles:
            return []

        # Apply the missing-only filter in batch mode too. Tables fully
        # commented are dropped from the request set; tables with partial
        # coverage have their column list narrowed to the gaps before
        # building agent prompts.
        if self.missing_only:
            kept: dict[str, TableProfile] = {}
            skipped_full = 0
            for table, prof in profiles.items():
                total_cols = len(prof.columns)
                cols_missing = [c for c in prof.columns if not (c.existing_comment or "").strip()]
                table_has_comment = bool((prof.existing_comment or "").strip())
                if table_has_comment and not cols_missing:
                    skipped_full += 1
                    info(f"[Batch] Skipping {schema}.{table}: fully commented (missing-only).")
                    continue
                if cols_missing and len(cols_missing) < total_cols:
                    info(
                        f"[Batch] Filtering {schema}.{table}: "
                        f"{total_cols - len(cols_missing)}/{total_cols} columns already commented; "
                        f"analyzing {len(cols_missing)} missing column(s)."
                    )
                    prof.columns = cols_missing
                elif not cols_missing and not table_has_comment:
                    info(
                        f"[Batch] {schema}.{table}: every column has a comment but the table "
                        "comment is missing — analyzing the table-level description only."
                    )
                    prof.columns = []
                kept[table] = prof
            profiles = kept
            tables = [t for t in tables if t in profiles]
            if not profiles:
                info(f"[Batch] All {n_assets} asset(s) already fully commented — nothing to do.")
                return []

        all_requests: list[BatchRequest] = []
        ctx_map: dict[str, AgentContext] = {}

        for table in tables:
            ctx = self._build_context(profiles[table])
            ctx_map[table] = ctx

            all_requests.extend(self.profile_agent.collect_messages(ctx))
            if self.rag_agent:
                all_requests.extend(self.rag_agent.collect_messages(ctx))
            if self.code_agent:
                all_requests.extend(self.code_agent.collect_messages(ctx))

        if not all_requests:
            warn("No LLM requests to submit — all agents had nothing to process.")
            return []

        info(f"[Batch] Submitting {len(all_requests)} request(s) for {n_assets} asset(s)…")
        batch_results = run_batch(all_requests, self.llm.cfg)

        all_reviewed: list[ReviewResult] = []

        for table in tables:
            heading(f"Processing results: {schema}.{table}")
            ctx = ctx_map[table]
            profile = profiles[table]
            ak = profile.asset_kind.value if profile.asset_kind else "table"

            num_cols = len(profile.columns)
            batch_size = self.profile_agent.batch_size
            n_batches = (num_cols + batch_size - 1) // batch_size

            all_suggestions: list[MetadataSuggestion] = []

            for idx in range(n_batches):
                cid = f"profile:{schema}:{table}:{idx}"
                chat_result = batch_results.get(cid)
                if chat_result and chat_result.content:
                    cols_slice = profile.columns[idx * batch_size : (idx + 1) * batch_size]
                    col_dicts = [
                        {
                            "name": c.name,
                            "dtype": c.dtype,
                            "nullable": c.nullable,
                            "row_count": c.row_count,
                            "null_count": c.null_count,
                            "distinct_count": c.distinct_count,
                            "samples": c.samples,
                        }
                        for c in cols_slice
                    ]
                    batch_ctx = self.profile_agent._ctx_with_columns(ctx, col_dicts)
                    tracker.record_for("profile_agent(batch)", 0, self.llm, chat_result.usage)
                    parsed = self.profile_agent.parse_batch_result(chat_result.content, batch_ctx)
                    all_suggestions.extend(
                        apply_logprob_confidence(
                            parsed,
                            chat_result.logprobs,
                            high_threshold=self.llm.cfg.logprob_high,
                            medium_threshold=self.llm.cfg.logprob_medium,
                            response_text=chat_result.content,
                        )
                    )

            if self.rag_agent:
                cid = f"rag:{schema}:{table}"
                chat_result = batch_results.get(cid)
                if chat_result and chat_result.content:
                    tracker.record_for("rag_agent(batch)", 0, self.llm, chat_result.usage)
                    parsed = self.rag_agent.parse_batch_result(chat_result.content, ctx)
                    all_suggestions.extend(
                        apply_logprob_confidence(
                            parsed,
                            chat_result.logprobs,
                            high_threshold=self.llm.cfg.logprob_high,
                            medium_threshold=self.llm.cfg.logprob_medium,
                            response_text=chat_result.content,
                        )
                    )

            if self.code_agent:
                cid = f"code:{schema}:{table}"
                chat_result = batch_results.get(cid)
                if chat_result and chat_result.content:
                    tracker.record_for("code_agent(batch)", 0, self.llm, chat_result.usage)
                    parsed = self.code_agent.parse_batch_result(chat_result.content, ctx)
                    all_suggestions.extend(
                        apply_logprob_confidence(
                            parsed,
                            chat_result.logprobs,
                            high_threshold=self.llm.cfg.logprob_high,
                            medium_threshold=self.llm.cfg.logprob_medium,
                            response_text=chat_result.content,
                        )
                    )

            if not all_suggestions:
                warn(f"No suggestions for {schema}.{table} after parsing batch results.")
                continue

            merged = self._merge_suggestions(all_suggestions, ctx)
            if not merged:
                warn(f"Merge produced no output for {schema}.{table}.")
                continue
            # Drop any table-level suggestion the model emitted despite the
            # suppressed prompt, so it isn't persisted or applied over the
            # existing table comment (column-scoped, or missing-only over a
            # table that already has a description).
            if self._should_skip_table_level(profile):
                merged = [s for s in merged if s.column is not None]
                if not merged:
                    continue

            result_id_map = self._save_merged_suggestions(merged, asset_kind=ak)
            reviewed = self._human_review(
                merged, schema, table, asset_kind=ak, result_id_map=result_id_map
            )
            self.results.extend(reviewed)
            all_reviewed.extend(reviewed)

        return all_reviewed

    # ── Apply ────────────────────────────────────────────────────────────────

    def _record_applied_state(self, r: ReviewResult) -> None:
        """Persist 'applied' state to history + catalog for a single result.

        Extracted so both ``apply_results`` (end-of-run batch) and
        ``process_table``'s auto-apply per-table call can share the same
        bookkeeping. Without sharing, auto-apply tables that landed in the
        live DB mid-run wouldn't show up as applied in /history or the
        search catalog.
        """
        hs = history_store()
        if hs is not None and r.result_id is not None:
            try:
                hs.record_applied(
                    r.result_id,
                    chosen_description=r.final_description or None,
                )
            except Exception as exc:
                log.debug(
                    "Could not record applied timestamp for result_id=%s: %s", r.result_id, exc
                )
        if r.result_id is not None:
            try:
                from amx.search.catalog import SearchCatalog

                catalog = SearchCatalog.from_history_store()
                if catalog is not None:
                    catalog.mark_applied(r.result_id)
            except Exception as exc:
                log.debug(
                    "Could not mark applied search catalog state for result_id=%s: %s",
                    r.result_id,
                    exc,
                )

    def apply_results(self, results: list[ReviewResult] | None = None) -> int:
        results = results or self.results
        hs = history_store()

        def _on_applied(r: ReviewResult) -> None:
            self._record_applied_state(r)

        def _on_failed(r: ReviewResult, exc: Exception) -> None:
            if hs is not None and r.result_id is not None:
                try:
                    hs.record_db_apply_failure(r.result_id, str(exc))
                except Exception as inner_exc:
                    log.debug(
                        "Could not record failed DB apply state for result_id=%s: %s",
                        r.result_id,
                        inner_exc,
                    )

        total = len([r for r in results if r.applied and r.final_description])
        _on_progress, _finish_progress = create_live_writeback_progress(
            total=total,
            backend=self.db.backend,
            provider=getattr(self.llm.cfg, "provider", ""),
            model=getattr(self.llm.cfg, "model", ""),
        )

        try:
            applied = apply_review_results_to_db(
                self.db,
                results,
                on_applied=_on_applied,
                on_failed=_on_failed,
                on_progress=_on_progress if total else None,
            )
        finally:
            if total:
                _finish_progress()
        success(f"Applied {applied} metadata comments to the database")
        return applied
