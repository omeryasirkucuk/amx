"""Re-Run executor: regenerate alternatives for one (or many) result rows.

The executor is invoked by:

* the CLI command ``amx rerun ...`` (see ``amx/cli_support/commands/rerun.py``);
* the Studio endpoint ``POST /api/runs/rerun-item`` (see
  ``amx/web/routers/rerun.py``).

Both call sites pass a list of ``target_result_ids`` (1..N), an
optional free-text ``user_instructions`` addendum, and an optional
``temperature_override``. The executor:

1. Opens a single parent ``analysis_runs`` row scoped to ``command="rerun"``.
2. For each target, freezes its ``AgentContext`` into
   ``rerun_context_snapshots`` so parallel agents see identical inputs.
3. Runs the right path per ``asset_kind``:
   * column / table → ``ProfileAgent`` on the snapshot context;
   * schema → ``SCHEMA_META_PROMPT`` against the parent run's table
     descriptions;
   * database → ``DATABASE_META_PROMPT`` against the parent run's
     schema descriptions.
4. Writes a new ``run_results`` row per target with ``parent_result_id``
   linking back to the original (+ ``rerun_seq`` for chain ordering and
   ``user_instructions`` for audit).
5. Always cleans up its snapshot rows in the ``finally`` block — the
   table is empty again the moment the worker terminates.

The original ``run_results`` row is never mutated; the chain stays
intact so the Studio history drawer can show "v1 vs v2" side-by-side.
"""

from __future__ import annotations

import dataclasses
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any

from amx.agents.base import (
    AgentContext,
    Confidence,
    MetadataSuggestion,
    apply_confidence_signals,
    apply_logprob_confidence,
)
from amx.agents.code_agent import CodeAgent
from amx.agents.profile_agent import ProfileAgent
from amx.agents.rag_agent import RAGAgent
from amx.agents.rerun_context import RerunContextError, build_context_snapshot, hydrate_context
from amx.config import AMXConfig
from amx.db.connector import AssetKind
from amx.llm.provider import LLMProvider
from amx.storage.sqlite_store import history_store
from amx.utils.logging import get_logger
from amx.utils.token_tracker import tracker as token_tracker

log = get_logger("agents._orchestrator.rerun")

# Per-item soft latency budget for a single re-run target. Real wall
# clock may exceed this (LLM HTTP calls aren't cancellable from inside
# the agent), but we log a warning so users / tests can spot the
# regression class — the design target is "no one waits more than 20s
# for a single column re-run".
RERUN_PER_ITEM_BUDGET_SEC: float = 20.0


@dataclass
class RerunOutcome:
    """One target's re-run result.

    Returned to callers (CLI prints them; web router exposes them via
    SSE ``activity.complete`` events). ``new_result_id`` is the freshly
    inserted ``run_results`` row id; ``alternatives`` is the new list
    of N options the user can pick from.
    """

    target_result_id: int
    new_result_id: int
    rerun_seq: int
    schema: str
    table: str
    column: str | None
    asset_kind: str
    alternatives: list[str]
    confidence: str
    logprob_score: float | None
    source: str
    error: str | None = None


def _llm_for_rerun(
    cfg: AMXConfig,
    *,
    overrides: dict[str, Any] | None,
    temperature_override: float | None,
) -> tuple[LLMProvider, AMXConfig]:
    """Open an :class:`LLMProvider` honouring the optional per-run override
    block.

    Mirrors :func:`amx.web.routers.runs._apply_llm_overrides`: builds a
    derived ``LLMConfig`` via :func:`dataclasses.replace` so the saved
    profile on disk is **never** mutated. Returns both the live
    provider and the derived ``AMXConfig`` so downstream call sites
    (e.g. :func:`_persist_rerun_row`) can read post-override values
    from ``derived_cfg.llm`` instead of the original profile.

    The legacy ``temperature_override`` shim (single-knob path used by
    the in-flight Studio bundles + the CLI ``/rerun --temperature``
    diversity nudge) is folded into the overrides dict here so a single
    code path applies them.
    """
    if not cfg.llm or not cfg.llm.provider or not cfg.llm.model:
        raise RerunContextError(
            "No active LLM profile is configured. Open Settings → LLM and pick one."
        )
    derived_overrides: dict[str, Any] = dict(overrides or {})
    if temperature_override is not None and "temperature" not in derived_overrides:
        try:
            derived_overrides["temperature"] = max(0.0, min(1.0, float(temperature_override)))
        except (TypeError, ValueError):
            pass
    if not derived_overrides:
        return LLMProvider(cfg.llm), cfg
    derived_llm = dataclasses.replace(cfg.llm, **derived_overrides)
    derived_cfg = dataclasses.replace(cfg, llm=derived_llm)
    return LLMProvider(derived_llm), derived_cfg


def _pick_target_suggestion(
    suggestions: list[MetadataSuggestion],
    *,
    column: str | None,
) -> MetadataSuggestion | None:
    """Find the one suggestion matching the target column.

    For column re-runs ``column`` is the column name; we want the row
    where ``s.column == column``. For table-level re-runs ``column`` is
    ``None`` and we want the row where ``s.column is None``.
    """
    for s in suggestions:
        if s.column == column:
            return s
    return None


def _try_load_rag_store(cfg: AMXConfig, doc_profile_name: str | None):
    """Open the doc profile's :class:`RAGStore` lazily.

    The store opens against the existing Chroma index on disk; no
    re-ingest happens here. Returns ``None`` when:
      * no doc profile is recorded on the original run;
      * the profile name doesn't exist in this AMXConfig;
      * the profile is the sentinel "disabled" value;
      * the index has zero chunks (nothing useful to retrieve).
    All failures are swallowed so an unavailable doc index never blocks
    a re-run — the ProfileAgent path still produces alternatives.
    """
    name = (doc_profile_name or "").strip()
    if not name:
        return None
    try:
        from amx.config import DISABLED_PROFILE
    except Exception:
        DISABLED_PROFILE = "none"  # noqa: N806 - mirror the sentinel
    if name == DISABLED_PROFILE:
        return None
    try:
        from amx.docs.rag import RAGStore
    except Exception:
        return None
    paths = cfg.doc_profiles.get(name) if hasattr(cfg, "doc_profiles") else None
    try:
        store = RAGStore(source_filters=list(paths) if paths else None)
        if store.doc_count <= 0:
            return None
        return store
    except Exception as exc:  # pragma: no cover - best-effort
        log.debug("rerun: could not open RAGStore for %s: %s", name, exc)
        return None


def _try_make_code_report(cfg: AMXConfig, code_profile_name: str | None):
    """Build a *lightweight* :class:`CodebaseReport` for semantic-only retrieval.

    The full codebase scan can take 30+ seconds on a large repo —
    blowing the 20s re-run budget. Instead we construct an empty
    :class:`CodebaseReport` whose ``path`` matches the profile's
    on-disk source filters; :class:`CodeAgent` then drives the
    "Semantic code retrieval" branch (one Chroma query, ~200ms) and
    skips the explicit ``references`` block entirely.

    Returns ``None`` when the profile is missing or the on-disk
    Chroma collection has no chunks for those source filters — both
    cases mean code context wouldn't add anything anyway.
    """
    name = (code_profile_name or "").strip()
    if not name:
        return None
    path: str | None = None
    if hasattr(cfg, "code_profiles"):
        path = cfg.code_profiles.get(name)
    if not path:
        return None
    try:
        from amx.codebase.analyzer import CodebaseReport
        from amx.codebase.code_rag import code_collection_count
    except Exception:
        return None
    source_filters = [p for p in str(path).split(";") if p] or None
    try:
        if code_collection_count(source_filters=source_filters) <= 0:
            return None
    except Exception:
        return None
    # ``references={}`` plus a non-empty ``path`` is the magic shape
    # that makes CodeAgent.run go down the semantic-only branch — see
    # ``code_agent._build_messages`` (``has_refs`` False, ``has_sem``
    # True). If the upstream agent ever changes this contract we'll
    # regret the silent skip; covered by ``test_rerun_code_agent_called``.
    return CodebaseReport(path=str(path))


def _run_rerun_agents(
    *,
    ctx: AgentContext,
    llm: LLMProvider,
    rag_store: Any | None,
    code_report: Any | None,
) -> list[MetadataSuggestion]:
    """Fan ``Profile`` / ``RAG`` / ``Code`` agents out in parallel.

    Mirrors :meth:`Orchestrator._run_enabled_agents` but with no
    cancel-token plumbing and best-effort error tolerance: a sub-agent
    raising never bubbles up — the caller still gets the
    :class:`ProfileAgent` output even if RAG/Code crashed. Wall-clock
    budget is logged when exceeded; the call still returns whatever
    came back.
    """
    started = time.monotonic()
    jobs: list[tuple[str, Any]] = [("profile", ProfileAgent(llm))]
    if rag_store is not None:
        jobs.append(("rag", RAGAgent(llm, rag_store)))
    if code_report is not None:
        jobs.append(("code", CodeAgent(llm, code_report)))

    from amx.utils.live_display import run_in_thread

    out: list[MetadataSuggestion] = []
    with ThreadPoolExecutor(max_workers=len(jobs)) as pool:
        # ``run_in_thread`` snapshots the parent's ``contextvars`` so
        # subscriber bus / correlation IDs reach the sub-agent worker
        # threads — same propagation as the analyze fan-out.
        fut_to_label = {run_in_thread(pool, agent.run, ctx): label for label, agent in jobs}
        for fut in as_completed(fut_to_label):
            label = fut_to_label[fut]
            try:
                out.extend(fut.result() or [])
            except Exception as exc:  # noqa: BLE001 - sub-agent failure is non-fatal
                log.warning("Re-run %s agent failed: %s", label, exc)

    elapsed = time.monotonic() - started
    if elapsed > RERUN_PER_ITEM_BUDGET_SEC:
        log.warning(
            "Re-run agent fan-out exceeded the %.0fs soft budget (took %.2fs) "
            "for %s.%s%s — investigate slow LLM/RAG/code paths.",
            RERUN_PER_ITEM_BUDGET_SEC,
            elapsed,
            ctx.schema,
            ctx.table,
            f".{ctx.column}" if ctx.column else "",
        )
    return out


def _merge_for_target(
    suggestions: list[MetadataSuggestion],
    *,
    column: str | None,
    llm: LLMProvider,
) -> MetadataSuggestion | None:
    """Pick the suggestion(s) for the target column and pick the best one.

    When several agents (Profile / RAG / Code) all returned a
    suggestion for the same target column we run a tiny in-process
    merge: pick the highest-confidence one, but union the
    ``suggestions`` lists so the user still sees up to ``n_alternatives``
    alternatives even if one agent only produced one. We deliberately
    skip the LLM-driven merge prompt the bulk run uses — re-runs care
    about latency, and one extra LLM round-trip per item to merge a
    handful of candidates would push the wall clock toward the 20s
    ceiling without materially improving quality.
    """
    matching = [s for s in suggestions if s.column == column]
    if not matching:
        return None
    if len(matching) == 1:
        return matching[0]

    confidence_rank = {Confidence.HIGH: 3, Confidence.MEDIUM: 2, Confidence.LOW: 1}
    matching.sort(
        key=lambda s: (
            confidence_rank.get(s.confidence, 0),
            -1 if s.logprob_score is None else s.logprob_score,
        ),
        reverse=True,
    )
    best = matching[0]

    seen: set[str] = set()
    merged_alts: list[str] = []
    for s in matching:
        for alt in s.suggestions or []:
            if alt and alt not in seen:
                seen.add(alt)
                merged_alts.append(alt)
    cap = max(1, min(5, getattr(getattr(llm, "cfg", None), "n_alternatives", 3)))
    best.suggestions = merged_alts[:cap]
    # Surface "combined" so /history can tell users this row was
    # synthesized from multiple agents instead of a single one.
    best.source = "combined"
    return best


def _rerun_table_or_column(
    cfg: AMXConfig,
    *,
    ctx: AgentContext,
    llm: LLMProvider,
    snapshot: dict[str, Any],
) -> MetadataSuggestion | None:
    """Run Profile + (optionally) RAG + (optionally) Code in parallel.

    Returns the merged suggestion for the target item or ``None`` when
    no agent produced anything parseable. Sub-agents are gated on the
    *original* run's profile names, captured in
    ``snapshot["original"]``; we never silently fall back to the
    currently-active profile because that would surprise users who
    rotated profiles between sessions.
    """
    original = snapshot.get("original") or {}
    rag_store = _try_load_rag_store(cfg, original.get("doc_profile"))
    code_report = _try_make_code_report(cfg, original.get("code_profile"))

    suggestions = _run_rerun_agents(
        ctx=ctx,
        llm=llm,
        rag_store=rag_store,
        code_report=code_report,
    )
    target = _merge_for_target(suggestions, column=ctx.column, llm=llm)
    if target is None:
        log.warning(
            "Re-run agents returned no suggestion for %s.%s.%s",
            ctx.schema,
            ctx.table,
            ctx.column,
        )
    return target


def _rerun_meta(
    *,
    ctx: AgentContext,
    llm: LLMProvider,
    asset_kind: AssetKind,
    parent_run_id: int,
) -> MetadataSuggestion | None:
    """Re-run a schema- or database-level description.

    Reuses the same prompts the orchestrator's ``process_schema_meta``
    / ``process_database_meta`` flows do, plus the ``user_instructions``
    suffix so the user's bias gets applied. Reads peer rows (table
    descriptions for schema-level, schema descriptions for db-level)
    from the parent run via the history store — no re-profiling
    required.
    """
    from amx.agents.base import _user_instructions_block
    from amx.agents.orchestrator import (
        DATABASE_META_PROMPT,
        META_SYSTEM_PROMPT,
        SCHEMA_META_PROMPT,
    )

    hs = history_store()
    if hs is None:
        return None
    rows = hs.get_run_results(int(parent_run_id))

    if asset_kind == AssetKind.SCHEMA:
        summaries: list[str] = []
        for r in rows:
            if (
                r.get("schema_name") == ctx.schema
                and not r.get("column_name")
                and (r.get("asset_kind") or "table") == "table"
            ):
                desc = r.get("chosen_description") or ""
                if not desc:
                    alts = r.get("alternatives_json") or []
                    desc = (alts[0] if alts else "") or ""
                if desc:
                    summaries.append(f"Table: {r.get('table_name')}\nDescription: {desc}")
        if not summaries:
            return None
        prompt = SCHEMA_META_PROMPT.format(
            schema=ctx.schema,
            tables_summary="\n\n".join(summaries),
        ) + _user_instructions_block(ctx)
    elif asset_kind == AssetKind.DATABASE:
        summaries = []
        for r in rows:
            if (r.get("asset_kind") or "") == "schema":
                desc = r.get("chosen_description") or ""
                if not desc:
                    alts = r.get("alternatives_json") or []
                    desc = (alts[0] if alts else "") or ""
                if desc:
                    summaries.append(f"Schema: {r.get('schema_name')}\nDescription: {desc}")
        if not summaries:
            return None
        prompt = DATABASE_META_PROMPT.format(
            schemas_summary="\n\n".join(summaries),
        ) + _user_instructions_block(ctx)
    else:  # pragma: no cover - guarded by caller
        return None

    res = llm.chat(
        [
            {"role": "system", "content": META_SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ]
    )

    desc, conf, reasoning = _parse_meta_block(res.content)
    if not desc:
        return None

    suggestion = MetadataSuggestion(
        schema=ctx.schema,
        table=ctx.table,
        column=ctx.column,
        suggestions=[desc],
        confidence=conf,
        reasoning=reasoning,
        source="combined",
    )
    calibrated = apply_logprob_confidence(
        [suggestion],
        getattr(res, "logprobs", None),
        high_threshold=getattr(llm.cfg, "logprob_high", 0.85),
        medium_threshold=getattr(llm.cfg, "logprob_medium", 0.5),
        response_text=res.content,
    )
    return calibrated[0] if calibrated else suggestion


def _parse_meta_block(text: str) -> tuple[str, Confidence, str]:
    """Parse the ``DESCRIPTION/CONFIDENCE/REASONING`` shape used by meta prompts."""
    import re

    text = re.sub(r"^```[a-zA-Z0-9_-]*\s*", "", (text or "").strip())
    text = re.sub(r"\s*```$", "", text).strip()
    desc = ""
    conf = Confidence.MEDIUM
    reasoning = ""
    for line in text.splitlines():
        upper = line.upper()
        if upper.startswith("DESCRIPTION:"):
            desc = line.split(":", 1)[1].strip()
        elif upper.startswith("CONFIDENCE:"):
            value = line.split(":", 1)[1].strip().upper()
            if "HIGH" in value:
                conf = Confidence.HIGH
            elif "LOW" in value:
                conf = Confidence.LOW
        elif upper.startswith("REASONING:"):
            reasoning = line.split(":", 1)[1].strip()
    return desc, conf, reasoning


def _persist_rerun_row(
    *,
    new_run_id: int,
    suggestion: MetadataSuggestion,
    target: dict[str, Any],
    asset_kind: str,
    user_instructions: str | None,
    model_name: str,
    alternatives_mode: str | None = None,
) -> tuple[int, int]:
    """Insert one ``run_results`` row for the re-run + return ``(new_id, seq)``.

    The chain root is whichever is non-null:
      * ``target.parent_result_id`` (if the target was itself a re-run);
      * ``target.id`` (if the target is the original).
    ``rerun_seq`` is computed via ``next_rerun_seq`` so concurrent
    re-runs each receive a monotonically increasing sequence number.
    """
    hs = history_store()
    if hs is None:
        raise RerunContextError("History store became unavailable mid-rerun.")

    chain_root = target.get("parent_result_id") or target.get("id")
    if chain_root is None:
        raise RerunContextError("Target row is missing a chain id.")
    rerun_seq = hs.next_rerun_seq(int(chain_root))

    # Carry per-alternative confidence rows so re-run rows render the
    # same SC / LP / SD / JU badge as the original run. Upstream re-run
    # executors (``rerun_one_item``) call ``apply_confidence_signals``
    # before handing the suggestion to this helper, so the attribute is
    # already populated when present. Falling back to ``None`` is fine —
    # the storage layer then emits the legacy ``list[str]`` payload.
    alternative_scores: list[dict] | None = None
    if getattr(suggestion, "suggestion_scores", None):
        alternative_scores = [score.to_json() for score in suggestion.suggestion_scores]

    row = {
        "schema": suggestion.schema,
        "table": suggestion.table,
        "column": suggestion.column,
        "asset_kind": asset_kind,
        "source": suggestion.source,
        "confidence": suggestion.confidence.value if suggestion.confidence else "medium",
        "logprob_score": suggestion.logprob_score,
        "raw_logprob": suggestion.logprob_score,
        "model_version": model_name,
        "reasoning": suggestion.reasoning,
        "alternatives": suggestion.suggestions,
        "alternative_scores": alternative_scores,
        "parent_result_id": int(chain_root),
        "rerun_seq": int(rerun_seq),
        "user_instructions": (user_instructions or "").strip() or None,
        "alternatives_mode": alternatives_mode,
    }
    [new_id] = hs.save_run_results(int(new_run_id), [row])
    return int(new_id), int(rerun_seq)


def rerun_items(
    cfg: AMXConfig,
    *,
    target_result_ids: list[int],
    user_instructions: str | None = None,
    temperature_override: float | None = None,
    llm_overrides: dict[str, Any] | None = None,
    job_id: str | None = None,
    cancel_token: threading.Event | None = None,
    on_event: Callable[[str, dict[str, Any]], None] | None = None,
) -> tuple[int, list[RerunOutcome]]:
    """Re-run one or many ``run_results`` rows with optional user guidance.

    Returns ``(new_run_id, [RerunOutcome, ...])``.

    The function is shared between CLI and Studio paths. ``on_event``
    lets the Studio worker pipe progress into its SSE queue
    (``activity.added`` / ``activity.complete`` / ``activity.fail``) —
    CLI callers can leave it ``None``.

    ``llm_overrides`` is the per-run override block mirrored from
    Studio's ``LLMOverrides`` Pydantic model — same fields, same
    semantics. ``temperature_override`` is the legacy single-knob
    shim for the CLI ``/rerun --temperature`` flag and in-flight Studio
    bundles; folded into ``llm_overrides`` internally by
    :func:`_llm_for_rerun`. The saved profile on disk is never mutated.
    """
    if not target_result_ids:
        raise RerunContextError("rerun_items called with no targets.")

    hs = history_store()
    if hs is None:
        raise RerunContextError("History store is not initialised — cannot re-run an item.")

    job_id = job_id or f"rerun-{int(time.time() * 1000)}"
    # Build the derived cfg so downstream calls
    # (``apply_confidence_signals``, ``_persist_rerun_row``, etc.) see
    # post-override values for ``alternatives_mode`` /
    # ``confidence_signal`` / ``temperature`` etc. The saved profile on
    # disk and ``cfg.llm`` on the caller side are untouched.
    llm, cfg = _llm_for_rerun(
        cfg, overrides=llm_overrides, temperature_override=temperature_override
    )

    # Resolve the original run + its scope so the new analysis_runs row
    # carries forward enough context for /history list to render it.
    targets: list[dict[str, Any]] = []
    parent_run_id: int | None = None
    for rid in target_result_ids:
        target = hs.get_run_result(int(rid))
        if target is None:
            raise RerunContextError(f"Target run_result {rid} not found.")
        targets.append(target)
        if parent_run_id is None:
            parent_run_id = int(target.get("run_id") or 0)
    parent_run = hs.get_run(int(parent_run_id or 0)) or {}

    # Open a fresh "rerun" row in analysis_runs so the new run_results
    # rows hang off a meaningful parent. Mode is "single" for a one-item
    # re-run, "batch" for multi-item.
    mode = "single" if len(target_result_ids) == 1 else "batch"
    # Inherit scope + database / catalog from the parent so Apply
    # downstream knows which database the COMMENTs target. Without this
    # the rerun's analysis_runs row had ``scope_json="{}"`` and the SPA
    # blocked the Apply pending queue button until the user manually
    # typed the database name. The parent's ``scope_json`` survives
    # ``list_recent_runs`` as a parsed dict; ``get_run`` may also leave
    # it as a JSON string so coerce defensively.
    parent_scope_raw = parent_run.get("scope_json") or parent_run.get("scope")
    parent_scope: dict[str, list[str]] = {}
    if isinstance(parent_scope_raw, dict):
        parent_scope = {
            str(k): list(v) if isinstance(v, list) else [] for k, v in parent_scope_raw.items()
        }
    elif isinstance(parent_scope_raw, str) and parent_scope_raw.strip():
        try:
            import json as _json

            decoded = _json.loads(parent_scope_raw)
            if isinstance(decoded, dict):
                parent_scope = {
                    str(k): list(v) if isinstance(v, list) else [] for k, v in decoded.items()
                }
        except Exception:
            parent_scope = {}
    parent_settings = parent_run.get("settings_json") or parent_run.get("settings") or {}
    if isinstance(parent_settings, str):
        try:
            import json as _json

            parent_settings = _json.loads(parent_settings) or {}
        except Exception:
            parent_settings = {}
    if not isinstance(parent_settings, dict):
        parent_settings = {}
    inherited_database = parent_run.get("database") or parent_settings.get("database") or None
    inherited_catalog = parent_run.get("catalog") or parent_settings.get("catalog") or None
    new_run_id = hs.create_run(
        command="rerun",
        mode=mode,
        db_backend=str(parent_run.get("db_backend") or ""),
        db_profile=str(parent_run.get("db_profile") or ""),
        llm_provider=str(parent_run.get("llm_provider") or cfg.llm.provider or ""),
        llm_model=str(parent_run.get("llm_model") or cfg.llm.model or ""),
        scope=parent_scope,
        selected_count=len(target_result_ids),
        planned_count=len(target_result_ids),
        review_strategy="individual",
        llm_profile=parent_run.get("llm_profile") or cfg.active_llm_profile,
        doc_profile=parent_run.get("doc_profile") or cfg.active_doc_profile or None,
        code_profile=parent_run.get("code_profile") or cfg.active_code_profile or None,
        settings={
            "trigger": "rerun",
            "parent_run_id": parent_run_id,
            "user_instructions": (user_instructions or "").strip() or None,
            "temperature_override": temperature_override,
            # Effective LLM config used for this re-run (post-override).
            # Mirrors the analyze.run path so ``/history show <run>``
            # surfaces the same fields whether the run came from /run or
            # from a re-run. ``llm_overrides`` records only the fields
            # the user actually overrode so a reviewer can tell at a
            # glance what the re-run changed vs inherited.
            "llm_overrides": dict(llm_overrides) if llm_overrides else None,
            "alternatives_mode": getattr(cfg.llm, "alternatives_mode", ""),
            "confidence_signal": getattr(cfg.llm, "confidence_signal", ""),
            "n_alternatives": int(getattr(cfg.llm, "n_alternatives", 0) or 0),
            "temperature": float(getattr(cfg.llm, "temperature", 0.0) or 0.0),
            "prompt_detail": getattr(cfg.llm, "prompt_detail", ""),
            "description_verbosity": getattr(cfg.llm, "description_verbosity", ""),
            # Surface the database / catalog at the settings level so
            # the SPA's history.get_run handler can flatten them onto
            # the run row (see history.py:92) and the Apply pending
            # queue button stays enabled for re-run output.
            "database": inherited_database,
            "catalog": inherited_catalog,
        },
    )

    outcomes: list[RerunOutcome] = []
    started = time.monotonic()
    # Reset the module-level singleton so the per-step tokens + USD
    # cost we attribute to this re-run are not contaminated by an
    # earlier analyze.run that left records behind in the same
    # process (Studio worker thread, long-lived CLI session).
    token_tracker.reset()
    try:
        for idx, target in enumerate(targets, start=1):
            if cancel_token is not None and cancel_token.is_set():
                break

            target_result_id = int(target["id"])
            schema = str(target.get("schema_name") or "")
            table_name = str(target.get("table_name") or "")
            column = target.get("column_name")
            asset_kind_raw = str(target.get("asset_kind") or "table")
            # Coerce to the enum where possible; fall back to TABLE for
            # legacy / column-level values (the run_results column
            # historically stores the parent table's asset_kind for
            # column rows, but older data may have "column" here).
            try:
                asset_kind = AssetKind(asset_kind_raw)
            except ValueError:
                asset_kind = AssetKind.TABLE
            label = ".".join(p for p in (schema, table_name, column or "") if p)

            if on_event is not None:
                on_event(
                    "activity.added",
                    {
                        "idx": idx,
                        "label": label,
                        "kind": asset_kind_raw,
                        "done": idx - 1,
                        "total": len(targets),
                        "result_id": target_result_id,
                    },
                )
                on_event("activity.begin", {"idx": idx})

            try:
                snapshot_id = build_context_snapshot(
                    cfg,
                    target_result_id=target_result_id,
                    job_id=job_id,
                    user_instructions=user_instructions,
                )
                snap = hs.read_rerun_snapshot(snapshot_id)
                if snap is None:
                    raise RerunContextError(
                        f"Snapshot {snapshot_id} disappeared between write and read."
                    )
                ctx = hydrate_context(snap["payload"])

                if asset_kind in (AssetKind.SCHEMA, AssetKind.DATABASE):
                    suggestion = _rerun_meta(
                        ctx=ctx,
                        llm=llm,
                        asset_kind=asset_kind,
                        parent_run_id=int(parent_run_id or 0),
                    )
                else:
                    suggestion = _rerun_table_or_column(
                        cfg, ctx=ctx, llm=llm, snapshot=snap["payload"]
                    )

                if suggestion is None or not suggestion.suggestions:
                    outcomes.append(
                        RerunOutcome(
                            target_result_id=target_result_id,
                            new_result_id=0,
                            rerun_seq=0,
                            schema=schema,
                            table=table_name,
                            column=column,
                            asset_kind=asset_kind_raw,
                            alternatives=[],
                            confidence="low",
                            logprob_score=None,
                            source="rerun",
                            error="LLM returned no parseable description.",
                        )
                    )
                    if on_event is not None:
                        on_event(
                            "activity.fail",
                            {
                                "idx": idx,
                                "detail": "LLM returned no parseable description.",
                                "result_id": target_result_id,
                            },
                        )
                    continue

                # Re-run rows must carry the per-alternative confidence
                # badge just like first-pass rows. The re-run helpers
                # don't return the underlying LLM response_text /
                # logprobs, so logprob and self_decl gracefully fall
                # back to ``score=None``; self_consistency and judge
                # work end-to-end on the alternatives alone.
                try:
                    apply_confidence_signals(
                        suggestions=[suggestion],
                        logprobs_content=None,
                        response_text=None,
                        cfg=cfg.llm,
                        llm=llm,
                    )
                except Exception as exc:  # pragma: no cover — defensive
                    log.warning("Rerun confidence scoring failed: %s", exc)

                new_id, rerun_seq = _persist_rerun_row(
                    new_run_id=new_run_id,
                    suggestion=suggestion,
                    target=target,
                    asset_kind=asset_kind_raw,
                    user_instructions=user_instructions,
                    model_name=getattr(llm, "model_name", "") or str(cfg.llm.model or ""),
                    alternatives_mode=getattr(cfg.llm, "alternatives_mode", None),
                )

                outcome = RerunOutcome(
                    target_result_id=target_result_id,
                    new_result_id=new_id,
                    rerun_seq=rerun_seq,
                    schema=schema,
                    table=table_name,
                    column=column,
                    asset_kind=asset_kind_raw,
                    alternatives=list(suggestion.suggestions),
                    confidence=(suggestion.confidence.value if suggestion.confidence else "medium"),
                    logprob_score=suggestion.logprob_score,
                    source=suggestion.source,
                )
                outcomes.append(outcome)

                if on_event is not None:
                    on_event(
                        "activity.complete",
                        {
                            "idx": idx,
                            "detail": f"{len(suggestion.suggestions)} alternative(s)",
                            "result_id": target_result_id,
                            "new_result_id": new_id,
                            "rerun_seq": rerun_seq,
                            "schema": schema,
                            "table": table_name,
                            "column": column,
                            "asset_kind": asset_kind_raw,
                            "alternatives": list(suggestion.suggestions),
                            "confidence": outcome.confidence,
                            "logprob_score": outcome.logprob_score,
                        },
                    )

            except RerunContextError as exc:
                outcomes.append(
                    RerunOutcome(
                        target_result_id=target_result_id,
                        new_result_id=0,
                        rerun_seq=0,
                        schema=schema,
                        table=table_name,
                        column=column,
                        asset_kind=asset_kind_raw,
                        alternatives=[],
                        confidence="low",
                        logprob_score=None,
                        source="rerun",
                        error=str(exc),
                    )
                )
                if on_event is not None:
                    on_event(
                        "activity.fail",
                        {
                            "idx": idx,
                            "detail": str(exc),
                            "result_id": target_result_id,
                        },
                    )
            except Exception as exc:  # noqa: BLE001 — surface upstream
                log.exception("Re-run target %s failed", target_result_id)
                outcomes.append(
                    RerunOutcome(
                        target_result_id=target_result_id,
                        new_result_id=0,
                        rerun_seq=0,
                        schema=schema,
                        table=table_name,
                        column=column,
                        asset_kind=asset_kind_raw,
                        alternatives=[],
                        confidence="low",
                        logprob_score=None,
                        source="rerun",
                        error=f"{exc.__class__.__name__}: {exc}",
                    )
                )
                if on_event is not None:
                    on_event(
                        "activity.fail",
                        {
                            "idx": idx,
                            "detail": f"{exc.__class__.__name__}: {exc}",
                            "result_id": target_result_id,
                        },
                    )

        # finish_run records duration + per-outcome counts so /history
        # can show the re-run alongside normal analyze.run rows. Mirror
        # analyze_flow's tokens={} payload so the re-run also surfaces
        # frozen USD cost in the run-detail page and in /usage.
        successful = sum(1 for o in outcomes if not o.error)
        hs.finish_run(
            int(new_run_id),
            status="success" if successful == len(outcomes) else "partial",
            metrics={
                "duration_sec": round(time.monotonic() - started, 3),
                "model_processing_sec": round(token_tracker.total_model_processing_sec, 3),
                "total_assets": len(outcomes),
                "processed_assets_count": successful,
                "failed_assets_count": len(outcomes) - successful,
                "trigger": "rerun",
                "parent_run_id": parent_run_id,
            },
            tokens={
                "total_tokens": token_tracker.total_tokens,
                "total_cost_usd": round(token_tracker.total_cost_usd, 8),
                "summary": token_tracker.summary(),
                "records": token_tracker.records(),
            },
            results={"new_result_ids": [o.new_result_id for o in outcomes if o.new_result_id]},
            error_text="",
        )
    finally:
        # Storage-maliyetsiz: snapshots disappear the moment the worker
        # exits, regardless of success / failure / cancellation.
        try:
            hs.delete_rerun_snapshots_for_job(job_id)
        except Exception as exc:
            log.warning("Snapshot cleanup failed for job %s: %s", job_id, exc)

    return int(new_run_id), outcomes


__all__ = ["RerunOutcome", "rerun_items"]
