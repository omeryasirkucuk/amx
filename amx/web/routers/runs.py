"""Run + apply trigger endpoints with SSE progress streams.

PR-C wires AMX Studio's "Run on this table" / "Apply approved"
buttons to the existing :class:`amx.agents.orchestrator.Orchestrator`
+ :func:`apply_review_results_to_db`.

Job lifecycle:

1. ``POST /api/runs`` (or ``POST /api/apply``) → spawn a worker
   thread, register a :class:`Job` in the JobRegistry, return the
   job id.
2. Worker calls into the orchestrator with the job's
   ``cancel_token`` + a progress callback that pushes events onto
   the job's queue.
3. ``GET /api/runs/{id}/events`` (and ``/api/apply/{id}/events``)
   tails the queue as Server-Sent-Events.
4. ``POST /api/runs/{id}/cancel`` flips the cancel token; the worker
   bails between rows and the SSE stream emits ``job.cancelled``.

Per-job state is in-memory only — AMX Studio is single-process
and per-CLI-session. PR-D adds the ``/ask`` job kind on the same
machinery.
"""

from __future__ import annotations

import dataclasses
import json
import threading
import time
from collections.abc import Callable
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field, field_validator
from sqlalchemy.exc import NoSuchTableError
from sse_starlette.sse import EventSourceResponse

from amx.agents.orchestrator import (
    Orchestrator,
    ReviewResult,
    RowApplyOutcome,
    RunCancelled,
    apply_review_results_to_db,
)
from amx.config import AMXConfig
from amx.db.connector import DatabaseConnector
from amx.llm.provider import LLMProvider
from amx.pending_review import (  # noqa: F401 - load_pending re-exported for tests
    clear_pending,
    clear_pending_for,
    load_pending,
    save_pending,
)
from amx.storage.sqlite_store import history_store
from amx.utils.console import quiet_console
from amx.utils.logging import get_logger, log_event
from amx.utils.token_tracker import tracker as token_tracker
from amx.web.deps import get_cfg, get_jobs
from amx.web.jobs import Job, JobRegistry
from amx.web.progress_bus import emit, emit_terminal
from amx.web.routers._runs_payloads import (  # noqa: PLC0414
    _column_details_for_table as _column_details_for_table,
)
from amx.web.routers._runs_payloads import (
    _emit_tokens_snapshot as _emit_tokens_snapshot,
)
from amx.web.routers._runs_payloads import (
    _resolve_apply_results as _resolve_apply_results,
)
from amx.web.routers._runs_payloads import (
    _review_result_from_dict as _review_result_from_dict,
)
from amx.web.routers._runs_payloads import (
    _review_result_to_event as _review_result_to_event,
)
from amx.web.routers.live_db import _connector_for_scope

router = APIRouter(prefix="/api", tags=["runs"])
log = get_logger("web.runs")


class LLMOverrides(BaseModel):
    """Per-run override of the active LLM profile's tuning knobs.

    Every field is optional — omitted = use the saved profile's value.
    Applied via :func:`_apply_llm_overrides` at the start of the run
    worker, *without ever mutating the saved profile on disk*. The
    derived :class:`AMXConfig` is scoped to the worker thread.
    """

    profile: str | None = Field(
        default=None,
        description=(
            "Optional saved-profile reference. When set, the named profile "
            "is swapped in as the base ``LLMConfig`` for this run — its "
            "``provider`` / ``model`` / ``api_key`` / ``api_base`` bundle "
            "is loaded atomically (so the right credentials route to the "
            "right endpoint) and any per-knob overrides on this same "
            "object layer on top. Unknown names degrade safely: the active "
            "profile is used and the audit drops the ``profile`` key. The "
            "saved profile on disk is never mutated."
        ),
    )
    temperature: float | None = Field(default=None, ge=0.0, le=2.0)
    max_tokens: int | None = Field(default=None, ge=256, le=262_144)
    n_alternatives: int | None = Field(default=None, ge=1, le=5)
    column_batch_size: int | None = Field(default=None, ge=1, le=200)
    prompt_detail: str | None = None
    description_verbosity: str | None = None
    confidence_signal: str | None = None
    alternatives_mode: str | None = None
    thinking_budget: int | None = Field(default=None, ge=0, le=64_000)
    logprob_high: float | None = Field(default=None, ge=0.0, le=1.0)
    logprob_medium: float | None = Field(default=None, ge=0.0, le=1.0)
    custom_input_cost_per_mtok: float | None = Field(default=None, ge=0.0)
    custom_output_cost_per_mtok: float | None = Field(default=None, ge=0.0)

    @field_validator("prompt_detail")
    @classmethod
    def _check_prompt_detail(cls, v: str | None) -> str | None:
        if v is None:
            return v
        allowed = {"minimal", "standard", "detailed", "full"}
        if v not in allowed:
            raise ValueError(f"prompt_detail must be one of {sorted(allowed)}")
        return v

    @field_validator("description_verbosity")
    @classmethod
    def _check_verbosity(cls, v: str | None) -> str | None:
        if v is None:
            return v
        allowed = {"brief", "detailed", "comprehensive", "exhaustive"}
        if v not in allowed:
            raise ValueError(f"description_verbosity must be one of {sorted(allowed)}")
        return v

    @field_validator("confidence_signal")
    @classmethod
    def _check_confidence_signal(cls, v: str | None) -> str | None:
        if v is None:
            return v
        from amx.config import CONFIDENCE_SIGNAL_CHOICES

        if v not in CONFIDENCE_SIGNAL_CHOICES:
            raise ValueError(
                f"confidence_signal must be one of {sorted(CONFIDENCE_SIGNAL_CHOICES)}"
            )
        return v

    @field_validator("alternatives_mode")
    @classmethod
    def _check_alternatives_mode(cls, v: str | None) -> str | None:
        if v is None:
            return v
        from amx.config import ALTERNATIVES_MODE_CHOICES

        normalised = v.strip().lower() if isinstance(v, str) else v
        if normalised not in ALTERNATIVES_MODE_CHOICES:
            raise ValueError(
                f"alternatives_mode must be one of {sorted(ALTERNATIVES_MODE_CHOICES)}"
            )
        return normalised

    def non_null(self) -> dict[str, Any]:
        """Return only the fields the caller actually set."""
        return {k: v for k, v in self.model_dump().items() if v is not None}


class RunRequest(BaseModel):
    """Body for ``POST /api/runs``.

    PR-C only wires the apply path through this layer; the full run
    pipeline (Orchestrator.process_table[s_batch_mode]) lands once
    the headless run plumbing on AMXApplication is ready (see
    plan §3 Run section). For now ``/api/runs`` accepts the same
    payload shape so the SPA can stub-call it; the worker emits an
    explanatory ``job.failed`` event.

    Scope (``db_profile`` / ``database`` / ``catalog``): when set, the
    worker resolves the connector via :func:`_connector_for_scope` and
    records the run under that profile name. Omit them to keep the
    legacy single-active behaviour for the pre-multi-profile SPA.

    ``llm_overrides`` lets the caller tune any LLM profile knob *for
    this run only* — temperature, n_alternatives, prompt_detail, etc.
    The saved profile on disk is never mutated; the worker derives a
    one-shot :class:`AMXConfig` via :func:`_apply_llm_overrides`.
    """

    scope: dict[str, list[str]] = Field(
        default_factory=dict,
        description="Schema → tables map. Empty {} means 'every reachable schema/table'.",
    )
    column_overrides: dict[str, list[str]] | None = Field(
        default=None,
        description=(
            "Optional per-table column restriction. Keys are 'schema.table' "
            "strings; values are the list of column names to process. When "
            "set, the orchestrator skips every other column on that table — "
            "table comment and unlisted columns are NOT re-inferred. Pass "
            "``None`` (default) to process every column, preserving the "
            "pre-existing behaviour."
        ),
    )
    apply: bool = Field(default=False, description="Auto-apply after the run completes.")
    missing_only: bool = Field(default=False)
    batch_mode: bool = Field(default=False)
    db_profile: str | None = Field(
        default=None,
        description="DB profile name. Empty means 'use the active profile' (legacy).",
    )
    database: str | None = Field(default=None)
    catalog: str | None = Field(default=None)
    llm_overrides: LLMOverrides | None = Field(
        default=None,
        description=(
            "Optional per-run overrides for the active LLM profile's "
            "tuning knobs (temperature, max_tokens, n_alternatives, "
            "column_batch_size, prompt_detail, description_verbosity, "
            "thinking_budget, logprob_high, logprob_medium, "
            "custom_input_cost_per_mtok, custom_output_cost_per_mtok). "
            "Saved profile is never mutated."
        ),
    )
    doc_profiles: list[str] | None = Field(
        default=None,
        description=(
            "PR E: one-shot multi-doc-profile override for this run. "
            "When set, the worker temporarily flips ``cfg.run_doc_profiles`` "
            "to this list and the orchestrator unions every profile's "
            "source paths into a single RAGStore retrieval scope. "
            "Pass ``None`` (or omit) to fall back to ``cfg.active_doc_profile`` "
            "/ ``cfg.run_doc_profiles`` on disk. The saved config is never "
            "mutated — the override is reverted in the worker's ``finally`` "
            "regardless of how the run terminates."
        ),
    )
    code_profiles: list[str] | None = Field(
        default=None,
        description=(
            "PR δ: one-shot multi-code-profile override for this run. "
            "Mirrors ``doc_profiles`` — the worker temporarily flips "
            "``cfg.run_code_profiles`` to this list and the CodeAgent's "
            "semantic retrieval is scoped to the union of every named "
            "profile's paths. ``None`` (or omitted) falls back to "
            "``cfg.active_code_profile`` / ``cfg.run_code_profiles`` on "
            "disk. The saved config is never mutated."
        ),
    )
    asset_context: list[dict[str, str]] = Field(
        default_factory=list,
        description=(
            "PR4: ingested-asset references attached to this run as "
            "additional LLM context. Each entry is "
            "``{'kind': 'asset_notebook'|'asset_query'|'asset_stream'|"
            "'asset_pipeline', 'ref': '<profile>:<remote_id>'}``. The "
            "worker resolves each ref against the local ``remote_*`` "
            "tables, derives the set of tables that asset references "
            "via ``catalog_relationships``, and injects a per-table "
            "context block into the ProfileAgent prompt so generated "
            "descriptions reflect the actual usage patterns. Pass an "
            "empty list (default) to leave the run pre-PR4 behaviour "
            "unchanged."
        ),
    )
    cache_override_assets: list[str] | None = Field(
        default=None,
        description=(
            "Bulk run only: ``schema.table`` identifiers the Studio "
            "reachability pre-flight (POST /api/runs/preflight) flagged "
            "as unreadable on the live database, and the user explicitly "
            "chose to substitute with the catalog cache from the last "
            "/search sync. The bulk worker skips ``profile_table`` for "
            "these assets and synthesizes a metadata-only profile "
            "(columns + dtypes + existing comments only — no samples, "
            "no PK/FK, no usage stats). Pass ``None`` (or omit) to "
            "force every asset through the live-profiling path."
        ),
    )


class PreflightRequest(BaseModel):
    """Body for ``POST /api/runs/preflight`` — reachability gate.

    Probes every ``(schema, table)`` in ``scope`` via the cheap
    metadata-only :meth:`DatabaseConnector.list_column_profiles`
    (which already swallows ``NoSuchTableError`` and returns ``[]``).
    The response splits the scope into two lists so the SPA can ask
    the user whether to substitute the catalog cache for any
    unreachable asset before submitting the actual bulk run.

    Same multi-profile scope fields as :class:`RunRequest`; pre-flight
    runs against the same connector the run will use.
    """

    scope: dict[str, list[str]] = Field(
        default_factory=dict,
        description="Schema → tables map. Empty {} means nothing to probe.",
    )
    db_profile: str | None = Field(default=None)
    database: str | None = Field(default=None)
    catalog: str | None = Field(default=None)


def _apply_llm_overrides(
    cfg: AMXConfig, overrides: LLMOverrides | None
) -> tuple[AMXConfig, dict[str, Any]]:
    """Return ``(derived_cfg, applied_dict)``.

    When ``overrides`` is ``None`` or empty the input ``cfg`` is
    returned unchanged. Otherwise we use :func:`dataclasses.replace`
    to build a derived :class:`LLMConfig` carrying the new values and
    a derived :class:`AMXConfig` that wraps it — the saved profile on
    disk and the in-memory ``cfg`` shared with the rest of the process
    are left alone, which is what makes this safe to call from a
    request handler.

    The second return value is the applied-fields dict (only keys the
    caller actually overrode), suitable for emitting as a structured
    audit event.
    """
    if overrides is None:
        return cfg, {}
    applied = overrides.non_null()
    if not applied:
        return cfg, {}
    # Profile swap is resolved first: when the caller picked a saved
    # profile by name, that profile's full LLMConfig becomes the base
    # for the derived cfg so ``provider`` / ``model`` / ``api_key`` /
    # ``api_base`` change together (they have to, otherwise we'd route
    # the wrong credentials to the wrong endpoint). Per-knob overrides
    # then layer on top. Unknown profile names degrade safely: we
    # leave ``cfg.llm`` alone and drop the ``profile`` key from the
    # audit so callers don't think the swap happened.
    profile_name = applied.pop("profile", None)
    base_llm = cfg.llm
    audit_profile: str | None = None
    if profile_name and profile_name in cfg.llm_profiles:
        base_llm = cfg.llm_profiles[profile_name]
        audit_profile = profile_name
    if not applied and audit_profile is None:
        return cfg, {}
    derived_llm = dataclasses.replace(base_llm, **applied) if applied else base_llm
    derived_cfg = dataclasses.replace(cfg, llm=derived_llm)
    audit_out = dict(applied)
    if audit_profile is not None:
        audit_out["profile"] = audit_profile
    return derived_cfg, audit_out


class ApplyRequest(BaseModel):
    """Body for ``POST /api/apply``.

    ``results`` accepts the on-disk ``ReviewResult`` shape. Omit the
    field to apply the user's pending queue
    (``~/.amx/pending_metadata.json``) end-to-end.

    ``db_profile`` / ``database`` / ``catalog`` route the writeback to a
    specific profile without flipping the active scope (multi-profile
    apply support).
    """

    results: list[dict[str, Any]] | None = None
    db_profile: str | None = Field(default=None)
    database: str | None = Field(default=None)
    catalog: str | None = Field(default=None)


def _scoped_connector(
    cfg: AMXConfig,
    db_profile: str | None,
    database: str | None,
    catalog: str | None,
) -> tuple[DatabaseConnector, str | None, str | None]:
    """Open a connector for the requested profile, or fall back to
    ``cfg.active_db_profile`` when the body omits ``db_profile``.

    Returns ``(connector, profile_name, backend)``. The Studio always
    sends ``db_profile`` explicitly (its URL encodes the scope); the
    CLI's web-bridged commands fall back to the active profile so the
    "Run" button keeps working without a scope mid-session.
    """
    name = (db_profile or "").strip()
    if name:
        conn = _connector_for_scope(cfg, name, database=database, catalog=catalog)
        base = cfg.db_profiles.get(name)
        return conn, name, getattr(base, "backend", None) if base else None
    # Active-profile fallback (CLI / pre-multi-profile tests).
    return (
        DatabaseConnector(cfg.db),
        cfg.active_db_profile,
        cfg.db.backend if cfg.db else None,
    )


@router.post("/runs/preflight")
def preflight_run(
    body: PreflightRequest,
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Reachability gate for the bulk-run path.

    Probes every ``(schema, table)`` in ``body.scope`` against the live
    DB via the cheap metadata-only :meth:`list_column_profiles` —
    which already swallows ``NoSuchTableError`` and returns ``[]`` —
    so the Studio caller can ask the user whether to substitute the
    catalog cache before submitting the actual run. No LLM work
    happens here.

    Response shape::

        {
            "blocked_assets":   [{"schema": ..., "table": ..., "reason": ...}],
            "reachable_assets": [{"schema": ..., "table": ...}],
        }

    The SPA renders a reachability dialog when ``blocked_assets`` is
    non-empty and includes the user's choice on the subsequent
    ``POST /api/runs`` call as ``cache_override_assets``.
    """
    from amx.web.routers.live_db import _connector_for_scope

    db_profile_name = (body.db_profile or cfg.active_db_profile or "").strip()
    if not db_profile_name:
        # Mirror the fallback the run worker uses so a CLI-driven
        # session without an explicit profile still pre-flights.
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="db_profile is required for pre-flight.",
        )

    try:
        db = _connector_for_scope(
            cfg, db_profile_name, database=body.database, catalog=body.catalog
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Could not open connector for pre-flight: {exc.__class__.__name__}: {exc}",
        ) from exc

    blocked: list[dict[str, str]] = []
    reachable: list[dict[str, str]] = []
    for schema, tables in (body.scope or {}).items():
        for table in tables or []:
            try:
                cols = db.list_column_profiles(schema, table)
            except Exception as exc:  # pragma: no cover — defensive
                log.warning(
                    "preflight: unexpected error probing %s.%s: %s",
                    schema,
                    table,
                    exc,
                )
                cols = []
            if cols:
                reachable.append({"schema": schema, "table": table})
            else:
                blocked.append(
                    {
                        "schema": schema,
                        "table": table,
                        "reason": "not_in_live_db",
                    }
                )
    return {
        "blocked_assets": blocked,
        "reachable_assets": reachable,
    }


@router.post("/runs")
def submit_run(
    body: RunRequest,
    cfg: AMXConfig = Depends(get_cfg),
    jobs: JobRegistry = Depends(get_jobs),
) -> dict[str, Any]:
    """Spawn a /run worker. Returns the job id immediately so the SPA
    can subscribe to the SSE event stream."""
    job = jobs.new_job("run")
    thread = threading.Thread(
        target=_run_worker,
        args=(cfg, job, body),
        name=f"amx-studio-run-{job.id}",
        daemon=True,
    )
    thread.start()
    return {"job_id": job.id, "status": job.status}


@router.post("/apply")
def submit_apply(
    body: ApplyRequest,
    cfg: AMXConfig = Depends(get_cfg),
    jobs: JobRegistry = Depends(get_jobs),
) -> dict[str, Any]:
    """Spawn an /apply worker. When ``body.results`` is omitted, the
    worker reads the pending review queue from disk."""
    job = jobs.new_job("apply")
    thread = threading.Thread(
        target=_apply_worker,
        args=(cfg, job, body),
        name=f"amx-studio-apply-{job.id}",
        daemon=True,
    )
    thread.start()
    return {"job_id": job.id, "status": job.status}


@router.get("/runs/{job_id}")
def get_job(job_id: str, jobs: JobRegistry = Depends(get_jobs)) -> dict[str, Any]:
    """Synchronous job snapshot — what the SPA polls when it can't
    keep an SSE connection open (e.g. user navigated away and back)."""
    job = jobs.get(job_id)
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"No job {job_id}")
    return job.to_public_dict()


@router.get("/apply/{job_id}")
def get_apply_job(job_id: str, jobs: JobRegistry = Depends(get_jobs)) -> dict[str, Any]:
    return get_job(job_id, jobs)


@router.get("/runs/{job_id}/events")
def stream_run_events(
    job_id: str,
    request: Request,
    jobs: JobRegistry = Depends(get_jobs),
) -> EventSourceResponse:
    return _events_endpoint(job_id, request, jobs)


@router.get("/apply/{job_id}/events")
def stream_apply_events(
    job_id: str,
    request: Request,
    jobs: JobRegistry = Depends(get_jobs),
) -> EventSourceResponse:
    return _events_endpoint(job_id, request, jobs)


@router.post("/runs/{job_id}/cancel")
def cancel_run(job_id: str, jobs: JobRegistry = Depends(get_jobs)) -> dict[str, Any]:
    if not jobs.cancel(job_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No active job {job_id} to cancel.",
        )
    return {"ok": True, "job_id": job_id}


@router.post("/apply/{job_id}/cancel")
def cancel_apply(job_id: str, jobs: JobRegistry = Depends(get_jobs)) -> dict[str, Any]:
    return cancel_run(job_id, jobs)


# ── Internals ──────────────────────────────────────────────────────────


#: Keep the SSE wire from going silent long enough for a proxy to
#: reap it. See ``amx.web.routers.ask`` for the matching constants —
#: held in sync deliberately so any future SSE-cadence tuning lands
#: in both routers in one commit.
_SSE_TAIL_TIMEOUT_SEC = 8.0
_SSE_PING_INTERVAL_SEC = 7.0


def _events_endpoint(
    job_id: str,
    request: Request,
    jobs: JobRegistry,
) -> EventSourceResponse:
    job = jobs.get(job_id)
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"No job {job_id}")
    raw_last_id = request.headers.get("last-event-id") or "0"
    try:
        last_event_id = int(raw_last_id)
    except (TypeError, ValueError):
        last_event_id = 0
    return EventSourceResponse(
        _event_generator(job, last_event_id=last_event_id),
        headers={
            "X-Accel-Buffering": "no",
            "Cache-Control": "no-store",
        },
    )


def _event_generator(job: Job, *, last_event_id: int = 0):
    """Stream the job's event log to one SSE consumer.

    Multi-consumer safe: two concurrent ``EventSource`` connections
    against the same run each track their own cursor against
    :meth:`BufferedQueue.tail_from`. The browser's auto-reconnect
    populates ``Last-Event-ID`` from the prior stream's ``id:``
    fields, which lets a transient disconnect resume cleanly instead
    of stranding the run-detail panel with whatever fragments survived
    the broken connection.

    Mirrors :mod:`amx.web.routers.ask._event_generator` deliberately —
    duplicating the small generator is cheaper than introducing a
    third "shared SSE helper" module before both call sites have a
    matching shape. Drift in either direction is a yellow flag in
    review.
    """
    cursor = max(0, int(last_event_id or 0))
    last_keepalive = time.monotonic()
    while True:
        ready = job.queue.tail_from(cursor, timeout=_SSE_TAIL_TIMEOUT_SEC)
        if ready is None:
            now = time.monotonic()
            if now - last_keepalive > _SSE_PING_INTERVAL_SEC:
                yield {
                    "id": str(cursor),
                    "event": "ping",
                    "data": json.dumps({"t": now}),
                }
                last_keepalive = now
            if job.status not in ("queued", "running"):
                # Worker terminated without enqueuing a terminal event.
                # Should not happen — ``runs`` workers emit terminal
                # via the same emit_terminal contract as ask — but
                # break cleanly to avoid tailing an idle queue.
                break
            continue
        terminal_seen = False
        for seq, event in ready:
            kind = str(event.get("type", ""))
            yield {
                "id": str(seq),
                "event": kind,
                "data": json.dumps(event),
            }
            cursor = seq
            last_keepalive = time.monotonic()
            if kind in {"job.done", "job.cancelled", "job.failed"}:
                terminal_seen = True
        if terminal_seen:
            break


def _build_progress_callback(job: Job) -> Callable[[ReviewResult, str, int, int, str], None]:
    def _on_progress(r: ReviewResult, status_word: str, idx: int, total: int, detail: str) -> None:
        emit(
            job.queue,
            "writeback.progress",
            {
                "schema": r.schema,
                "table": r.table,
                "column": r.column,
                "asset_kind": r.asset_kind,
                "status": status_word,
                "done": idx,
                "total": total,
                "detail": detail or "",
            },
        )

    return _on_progress


def _announce_phase(
    job: Job,
    hs: Any | None,
    run_id: int | None,
    label: str,
) -> None:
    """Surface a Studio-visible progress signal on two channels.

    The web worker spends 30-60s before the per-table loop starts
    (DB connector, LLM provider, history row, RAG store, code RAG,
    orchestrator). None of those phases emit anything on their own,
    so the run-detail page rendered "Waiting for the worker to
    begin…" while real work was in flight. This helper bridges that
    gap by emitting a ``step.update`` event on the live SSE queue
    (so connected tabs see the label) AND writing it to
    ``analysis_runs.current_step_label`` (so a refresh that lands
    after the in-process replay buffer was lost — e.g. Studio
    restarted mid-run — still has a current phase to render).

    Empty / whitespace labels are ignored so accidental
    ``_announce_phase(..., "")`` calls don't clutter the SSE buffer
    with blank chips. Persistence failures are swallowed because the
    live SSE channel is the load-bearing surface; the column write is
    a cold-load nice-to-have.
    """
    trimmed = (label or "").strip()
    if not trimmed:
        return
    emit(job.queue, "step.update", {"idx": 0, "label": trimmed})
    if hs is None or run_id is None:
        return
    try:
        hs.update_run_current_step(run_id, trimmed)
    except Exception:  # noqa: BLE001 - SSE emit is the load-bearing channel
        log.exception("update_run_current_step failed for run_id=%s", run_id)


def _start_heartbeat_ticker(
    hs: Any,
    run_id: int,
    *,
    interval_sec: float = 60.0,
) -> tuple[threading.Event, threading.Thread]:
    """Keep ``analysis_runs.last_heartbeat_at`` fresh for a live web run.

    The scheduler loop's ``recover_stale_runs`` sweep treats a NULL
    ``last_heartbeat_at`` as immediately stale, so a Studio run that
    never beats gets flipped to ``failed`` ~60s after the worker
    started even though its worker thread is still alive. The first
    beat fires synchronously before the helper returns so the NULL
    window is closed by the time the worker proceeds; subsequent beats
    run on a daemon thread until the caller sets the returned event.

    Mirrors the inline pattern in ``amx/runtime/worker.py`` that
    protects scheduled runs from the same sweep.
    """
    stop = threading.Event()

    def _tick() -> None:
        while not stop.is_set():
            try:
                hs.update_run_heartbeat(run_id)
            except Exception:  # noqa: BLE001 - never crash the ticker
                log.exception("web heartbeat ticker failed for run_id=%s", run_id)
            stop.wait(interval_sec)

    # First beat synchronously so ``recover_stale_runs`` never sees a
    # NULL ``last_heartbeat_at`` for this row (the load-bearing reason
    # this helper exists).
    try:
        hs.update_run_heartbeat(run_id)
    except Exception:  # noqa: BLE001
        log.exception("web heartbeat first beat failed for run_id=%s", run_id)

    thread = threading.Thread(
        target=_tick,
        name=f"amx-web-heartbeat-{run_id}",
        daemon=True,
    )
    thread.start()
    return stop, thread


def _run_worker(cfg: AMXConfig, job: Job, body: RunRequest) -> None:
    """Drive a headless ``/run`` from AMX Studio.

    Mirrors the non-interactive subset of ``cli_support/commands/
    analyze_flow.py``: create a history row, build Orchestrator, walk
    every (schema, table) in the requested scope, persist alternatives,
    and stash the deferred ReviewResults in ``~/.amx/pending_metadata.
    json`` for review on the SPA's Pending tab. Optional ``apply=True``
    chains an apply step on top.

    Compared to the CLI flow we skip:
      * Scope picker / coverage prompts (the SPA pre-selects).
      * Equivalence-class dedup pre-pass (still runs per-table normally
        once dedup lands as a per-run flag — out of scope for Stage 2).
      * Doc / code agents (RAG store + code report bindings — Stage 3
        wires those into the wizard).

    Cancellation: ``job.cancel`` is checked between tables so a long
    multi-schema run can be aborted cleanly mid-flight.
    """
    # quiet_console() silences the CLI Rich console (info / success /
    # warn / error + console.print) for this worker thread. Everything
    # the agents normally print to the parent REPL terminal is now
    # routed through SSE events to the browser instead.
    with quiet_console():
        _run_worker_body(cfg, job, body)


def _run_worker_body(cfg: AMXConfig, job: Job, body: RunRequest) -> None:
    job.status = "running"
    run_started = time.monotonic()
    token_tracker.reset()

    if not cfg.llm.provider or not cfg.llm.model:
        _fail_job(job, "No active LLM profile is configured. Use Settings to add one.")
        return

    # Per-run overrides for the active LLM profile (Studio "Advanced
    # LLM settings" disclosure on /runs/new). The derived ``cfg`` only
    # exists for the lifetime of this worker thread — the saved profile
    # on disk is never mutated. When the body omits ``llm_overrides``
    # the helper returns the original ``cfg`` unchanged, so the common
    # case stays a no-op.
    cfg, applied_overrides = _apply_llm_overrides(cfg, body.llm_overrides)
    if applied_overrides:
        emit(job.queue, "run.llm_overrides", {"overrides": applied_overrides})

    # PR E: per-run doc-profile override. The dialog lets the user pick
    # which doc profiles feed the RAG agent for *this* run only; we
    # apply the list to ``cfg.run_doc_profiles`` here and revert it in
    # the finally block at the bottom of the worker. The saved config
    # on disk is never mutated. ``None`` / empty falls through to the
    # active profile (legacy behaviour).
    _doc_profiles_saved: list[str] = list(cfg.run_doc_profiles or [])
    _doc_profiles_overridden = False
    if body.doc_profiles is not None:
        cleaned_doc_profiles = [str(p).strip() for p in body.doc_profiles if str(p or "").strip()]
        try:
            object.__setattr__(cfg, "run_doc_profiles", cleaned_doc_profiles)
        except Exception:  # pragma: no cover - defensive
            cfg.run_doc_profiles = cleaned_doc_profiles
        _doc_profiles_overridden = True
        emit(
            job.queue,
            "run.doc_profiles",
            {"doc_profiles": cleaned_doc_profiles},
        )

    # PR δ: identical override contract for the code-profile multi-select.
    _code_profiles_saved: list[str] = list(cfg.run_code_profiles or [])
    _code_profiles_overridden = False
    if body.code_profiles is not None:
        cleaned_code_profiles = [str(p).strip() for p in body.code_profiles if str(p or "").strip()]
        try:
            object.__setattr__(cfg, "run_code_profiles", cleaned_code_profiles)
        except Exception:  # pragma: no cover - defensive
            cfg.run_code_profiles = cleaned_code_profiles
        _code_profiles_overridden = True
        emit(
            job.queue,
            "run.code_profiles",
            {"code_profiles": cleaned_code_profiles},
        )
        try:
            log_event(
                "run.llm_overrides",
                trigger="studio",
                overrides=applied_overrides,
            )
        except Exception:  # pragma: no cover - audit log is best effort
            pass

    scope: dict[str, list[str]] = {
        str(s): [str(t) for t in (tables or [])] for s, tables in (body.scope or {}).items()
    }
    if not scope or not any(tbls for tbls in scope.values()):
        _fail_job(
            job,
            "Run scope is empty. Pick at least one table before submitting.",
        )
        return

    total_assets = sum(len(v) for v in scope.values())
    emit(
        job.queue,
        "run.scope.resolved",
        {
            "scope": scope,
            "total_assets": total_assets,
            "total_schemas": len(scope),
        },
    )

    # First user-visible phase. The history-store handle isn't open
    # yet (create_run runs further down) so persistence is deferred to
    # the next _announce_phase call — the SSE buffer carries this one.
    _announce_phase(
        job,
        None,
        None,
        f"Opening {(body.db_profile or '').strip() or 'active'} connector",
    )

    try:
        db, effective_profile, effective_backend = _scoped_connector(
            cfg, body.db_profile, body.database, body.catalog
        )
    except HTTPException as exc:
        _fail_job(job, f"Could not open DB connector: {exc.detail}")
        return
    except Exception as exc:
        _fail_job(job, f"Could not open DB connector: {exc}")
        return

    _announce_phase(job, None, None, f"Initializing LLM {cfg.llm.provider}/{cfg.llm.model}")

    try:
        llm = LLMProvider(cfg.llm)
    except Exception as exc:
        _fail_job(job, f"Could not initialize LLM: {exc}")
        try:
            db.close()
        except Exception:
            pass
        return

    # Batch mode is only honored when the active provider has a batch
    # implementation registered. Otherwise we silently fall through to
    # chat — same contract as the CLI's _resolve_completion_mode, but
    # without an interactive prompt: we surface the fallback as an SSE
    # event so the SPA can show a banner.
    use_batch = bool(body.batch_mode) and llm.supports_batch
    if body.batch_mode and not llm.supports_batch:
        from amx.llm.batch import supported_providers

        emit(
            job.queue,
            "run.mode.fallback",
            {
                "requested": "batch",
                "actual": "chat",
                "reason": (
                    f"Provider '{cfg.llm.provider}' does not support batch mode "
                    f"(supported: {', '.join(supported_providers())})."
                ),
            },
        )

    run_id: int | None = None
    heartbeat_stop: threading.Event | None = None
    heartbeat_thread: threading.Thread | None = None
    hs = history_store()
    if hs is not None:
        try:
            created_run_id: int = hs.create_run(
                command="analyze.run",
                mode="batch" if use_batch else "chat",
                db_backend=effective_backend,
                db_profile=effective_profile,
                llm_provider=cfg.llm.provider,
                llm_model=cfg.llm.model,
                scope=scope,
                selected_count=total_assets,
                planned_count=total_assets,
                review_strategy="individual",
                llm_profile=cfg.active_llm_profile,
                doc_profile=cfg.active_doc_profile or None,
                code_profile=cfg.active_code_profile or None,
                settings={
                    "missing_only": bool(body.missing_only),
                    "applied_flag": bool(body.apply),
                    "trigger": "studio",
                    "batch_mode": use_batch,
                    "database": (body.database or "").strip() or None,
                    "catalog": (body.catalog or "").strip() or None,
                    "llm_overrides": applied_overrides or None,
                    # Persist the column-level scope (when the user
                    # picked individual columns via the ScopeTree).
                    # Keys: "schema.table" -> [col1, col2, ...].
                    # The Ask agent's ``describe_run`` returns
                    # ``settings_json`` verbatim so this is enough to
                    # answer "did I run on just these columns?"
                    # without a second roundtrip.
                    "column_overrides": (
                        dict(body.column_overrides) if body.column_overrides else None
                    ),
                },
            )
            run_id = created_run_id
            # Bind the persistent run id to the live Job so the run
            # detail page can find this still-running worker by
            # numeric run id (Runs list → click row → /runs/{id}).
            job.run_id = created_run_id
            emit(job.queue, "run.created", {"run_id": created_run_id})
            # Close the NULL-heartbeat window before recover_stale_runs
            # can sweep this freshly-inserted row. The ticker is torn
            # down at the end of _run_worker_body.
            heartbeat_stop, heartbeat_thread = _start_heartbeat_ticker(hs, created_run_id)
        except Exception as exc:
            log.warning("Could not persist run history: %s", exc)

    _announce_phase(
        job,
        hs,
        run_id,
        (
            f"Loading docs RAG store ({cfg.active_doc_profile})"
            if cfg.active_doc_profile
            else "Preparing run context"
        ),
    )

    # PR E: open a RAG store for the run when a doc profile is active
    # (or the per-run override above asked for one). Mirrors the CLI's
    # analyze_flow logic — failure to open is downgraded to a one-line
    # ``rag_unavailable_reason`` on the run record (PR A) so the SPA's
    # Run detail page can surface "Docs unavailable: …" instead of a
    # silent run with no doc context.
    rag_store_for_run = None
    rag_extra_metrics: dict[str, Any] = {}
    effective_run_doc_paths: list[str] = []
    try:
        from amx.config import DISABLED_PROFILE as _DISABLED_PROFILE
        from amx.docs.rag import RAGStore as _RAGStore

        if cfg.active_doc_profile and cfg.active_doc_profile != _DISABLED_PROFILE:
            effective_run_doc_paths = list(cfg.effective_run_doc_paths())
            _store = _RAGStore(source_filters=effective_run_doc_paths)
            if _store.doc_count > 0:
                rag_store_for_run = _store
    except Exception as exc:  # pragma: no cover - storage-side defensive
        from amx.cli_support.commands.analyze_flow import _record_rag_unavailable_reason

        _record_rag_unavailable_reason(rag_extra_metrics, exc)
        log.warning("RAGStore init failed in studio run: %s", exc, exc_info=True)

    # Profiles list resolved for this run (union of effective doc
    # profiles when the multi-profile override is set, else the single
    # active profile). Surfaced on metrics_json so the Run detail page
    # can render "Docs: profile-a, profile-b · N hits".
    if cfg.run_doc_profiles:
        rag_extra_metrics["doc_profiles_used"] = list(cfg.run_doc_profiles)
    elif cfg.active_doc_profile:
        rag_extra_metrics["doc_profiles_used"] = [cfg.active_doc_profile]
    else:
        rag_extra_metrics["doc_profiles_used"] = []
    rag_extra_metrics["effective_run_doc_paths"] = list(effective_run_doc_paths)

    # PR δ: same persisted shape for code profiles. ``code_profiles_used``
    # surfaces in metrics_json so RunDetail can render
    # "Code: profile-a, profile-b · N chunks used".
    if cfg.run_code_profiles:
        rag_extra_metrics["code_profiles_used"] = list(cfg.run_code_profiles)
    elif cfg.active_code_profile:
        rag_extra_metrics["code_profiles_used"] = [cfg.active_code_profile]
    else:
        rag_extra_metrics["code_profiles_used"] = []
    try:
        rag_extra_metrics["effective_run_code_paths"] = list(cfg.effective_run_code_paths())
    except Exception:  # pragma: no cover - defensive
        rag_extra_metrics["effective_run_code_paths"] = []

    _announce_phase(job, hs, run_id, "Building orchestrator")

    orchestrator = Orchestrator(
        db=db,
        llm=llm,
        rag_store=rag_store_for_run,
        run_id=run_id,
        missing_only=bool(body.missing_only),
    )
    # Plumb the optional per-table column-scope through to the
    # orchestrator's pre-existing ``column_overrides`` map. The CLI
    # ``Column scope`` picker has used this mechanism for a while;
    # the Studio surface just hadn't exposed it. Keys arrive as
    # ``"schema.table"`` strings (JSON-friendly); the orchestrator
    # wants ``(schema, table) -> set[column]`` so we translate here.
    if body.column_overrides:
        translated: dict[tuple[str, str], set[str]] = {}
        for key, cols in body.column_overrides.items():
            if "." not in key:
                continue
            schema, _, table = key.partition(".")
            if not schema or not table or not cols:
                continue
            translated[(schema, table)] = set(cols)
        if translated:
            orchestrator.column_overrides = translated

    # PR4: resolve attached ingested-asset refs into per-table context
    # blocks. Each block surfaces a short excerpt of the referencing
    # notebook / query / stream / pipeline so the ProfileAgent prompt
    # can ground generated descriptions in real usage patterns. The
    # orchestrator copies the matching list into ``AgentContext`` per
    # table — see :meth:`Orchestrator._build_context`. Uses the
    # module-level ``history_store`` symbol already imported at the top.
    if body.asset_context:
        from amx.analyze.asset_context import AssetRef as _AssetRef
        from amx.analyze.asset_context import resolve_asset_context_for_run

        _hs = history_store()
        if _hs is not None:
            refs_list = [
                _AssetRef(kind=str(item.get("kind") or ""), ref=str(item.get("ref") or ""))
                for item in body.asset_context
                if item.get("kind") and item.get("ref")
            ]
            blocks_by_table, _resolved = resolve_asset_context_for_run(store=_hs, refs=refs_list)
            if blocks_by_table:
                orchestrator.asset_context_by_table = blocks_by_table

    processed_assets: list[str] = []
    skipped_assets: list[str] = []
    failed_assets: list[tuple[str, str]] = []  # (asset_path, error)
    final_error_text = ""
    final_status = "success"

    # Bridge LiveDisplay events out to the SSE queue so the Studio
    # run-detail page sees the same step-by-step narration the CLI
    # gets. Without this, the page stalled on
    # "Waiting for the worker to begin…" for the entire profile +
    # RAG + LLM batch window — sometimes 5–30 minutes — because the
    # web worker only emitted per-table activity events.
    #
    # Two halves of the bridge are required:
    # 1) ``push_subscriber`` registers a thread-local listener that
    #    forwards every ``step.*`` / ``tokens.delta`` event onto the
    #    job's SSE queue.
    # 2) ``start_headless`` flips ``LiveDisplay.is_active`` to True
    #    *without* painting a Rich ``Live`` panel on the parent CLI
    #    terminal. The agents' ``step_spinner`` blocks check
    #    ``is_active`` and only emit subscriber events when it's
    #    True; without headless mode the spinner falls through to
    #    its silent CLI-fallback path and the SSE queue never sees
    #    a single per-batch / per-agent label.
    from amx.utils.live_display import (
        get_display as _get_display,
    )
    from amx.utils.live_display import (
        pop_subscriber as _pop_display_subscriber,
    )
    from amx.utils.live_display import (
        push_subscriber as _push_display_subscriber,
    )

    def _display_to_sse(event_type: str, payload: dict[str, Any]) -> None:
        emit(job.queue, event_type, payload)
        # Mirror live-narration labels into the persisted column so a
        # page refresh after the in-process replay buffer is lost
        # (e.g. Studio restart mid-run) still shows the most recent
        # per-table phase instead of falling back to the "Waiting for
        # the worker…" placeholder.
        if event_type in ("step.update", "step.begin") and hs is not None and run_id is not None:
            label = str(payload.get("label") or "").strip()
            if label:
                try:
                    hs.update_run_current_step(run_id, label)
                except Exception:  # noqa: BLE001
                    log.exception("update_run_current_step failed for run_id=%s", run_id)

    _display = _get_display()
    _display.start_headless(
        schema=", ".join(scope.keys()) if len(scope) <= 3 else f"{len(scope)} schemas",
        table=f"{total_assets} assets",
        mode="analyze.run",
        provider=cfg.llm.provider,
        model=cfg.llm.model,
    )
    _push_display_subscriber(_display_to_sse)

    _announce_phase(job, hs, run_id, "Starting per-table processing")

    # Make ``job.cancel`` visible to every nested LLM call via a
    # ContextVar. The orchestrator's table-level cancel checks fire at
    # phase boundaries (profile / filters / agents / apply), so a
    # mid-table cancel had to wait for the whole agent fan-out to
    # finish before observing the click. The provider now reads this
    # same token and short-circuits the next ``litellm.completion()``
    # — many minutes saved on slow reasoning models. The token is
    # bound here and explicitly reset on the finally below so a
    # worker thread re-used by a future job sees the new job's state.
    from amx.utils.cancel import _active_cancel_token

    _cancel_token_sentinel = _active_cancel_token.set(job.cancel)

    cache_override_set: set[str] = {
        str(s).strip() for s in (body.cache_override_assets or []) if str(s).strip()
    }
    try:
        if use_batch:
            _process_scope_batch(
                orchestrator=orchestrator,
                scope=scope,
                job=job,
                hs=hs,
                run_id=run_id,
                total_assets=total_assets,
                processed_assets=processed_assets,
                failed_assets=failed_assets,
                cache_override_assets=cache_override_set,
            )
        else:
            idx_global = 0
            for schema, tables in scope.items():
                for table in tables:
                    idx_global += 1
                    if job.cancel.is_set():
                        raise RunCancelled(f"Cancelled before {schema}.{table}")
                    asset_path = f"{schema}.{table}"
                    emit(
                        job.queue,
                        "activity.added",
                        {
                            "idx": idx_global,
                            "label": asset_path,
                            "kind": "table",
                            "done": idx_global - 1,
                            "total": total_assets,
                        },
                    )
                    emit(job.queue, "activity.begin", {"idx": idx_global})
                    try:
                        table_results = orchestrator.process_table(
                            schema,
                            table,
                            interactive_review=False,
                            auto_apply=False,
                            cancel_token=job.cancel,
                        )
                    except RunCancelled:
                        raise
                    except Exception as exc:
                        failed_assets.append((asset_path, str(exc)))
                        log.warning("Table %s failed: %s", asset_path, exc)
                        emit(
                            job.queue,
                            "activity.fail",
                            {"idx": idx_global, "detail": f"{exc.__class__.__name__}: {exc}"},
                        )
                        continue
                    processed_assets.append(asset_path)
                    # Pull the persisted alternatives for THIS table so
                    # the live SPA shows the same per-column richness the
                    # CLI's Rich preview does. Cheap one-shot fetch:
                    # filtering by (schema, table) in Python is fine since
                    # run_results.* for a single table is small.
                    column_details = _column_details_for_table(hs, run_id, schema, table)
                    emit(
                        job.queue,
                        "activity.complete",
                        {
                            "idx": idx_global,
                            "detail": f"{len(table_results)} suggestion(s)",
                            "schema": schema,
                            "table": table,
                            "results": column_details
                            or [_review_result_to_event(r) for r in table_results],
                        },
                    )
                    _emit_tokens_snapshot(job.queue)
    except RunCancelled:
        job.status = "cancelled"
        final_status = "cancelled"
        emit_terminal(job.queue, "job.cancelled", {})
    except Exception as exc:
        log.exception("Run worker crashed")
        final_status = "failed"
        final_error_text = f"{exc.__class__.__name__}: {exc}"
        job.status = "failed"
        job.error = final_error_text
        emit_terminal(job.queue, "job.failed", {"error": final_error_text})
    finally:
        # Always remove the bridge so a future job in this same worker
        # thread doesn't accidentally inherit the previous job's
        # subscribers, and tear down headless mode so the global
        # LiveDisplay singleton goes back to inactive.
        _pop_display_subscriber(_display_to_sse)
        try:
            _display.stop_headless()
        except Exception:  # pragma: no cover - defensive
            pass
        # Restore the cancel-token ContextVar to whatever it was bound
        # to before this job — required so a worker thread re-used by
        # the next job doesn't observe this job's token as "active".
        try:
            _active_cancel_token.reset(_cancel_token_sentinel)
        except Exception:  # pragma: no cover - defensive
            pass
        # PR E: revert the one-shot doc-profile override. The saved
        # config on disk was never mutated; this only restores the
        # in-memory cfg so subsequent workers using this same cfg
        # singleton see the user's persisted run_doc_profiles list.
        if _doc_profiles_overridden:
            try:
                object.__setattr__(cfg, "run_doc_profiles", _doc_profiles_saved)
            except Exception:  # pragma: no cover - defensive
                cfg.run_doc_profiles = _doc_profiles_saved
        # PR δ: parallel revert for the code-profile override.
        if _code_profiles_overridden:
            try:
                object.__setattr__(cfg, "run_code_profiles", _code_profiles_saved)
            except Exception:  # pragma: no cover - defensive
                cfg.run_code_profiles = _code_profiles_saved

    # Persist deferred review results into the pending queue regardless
    # of cancellation — the user may want to review what *did* finish.
    deferred: list[ReviewResult] = []
    for r in orchestrator.results:
        # _deferred_branch sets applied=False; mark as pending=True so
        # save_pending() picks them up (it skips applied=False rows by
        # default — see pending_review.save_pending).
        r.applied = True
        deferred.append(r)

    pending_count = 0
    if deferred:
        try:
            save_pending(deferred)
            pending_count = len(deferred)
            emit(job.queue, "pending.saved", {"count": pending_count})
        except Exception as exc:
            log.warning("Failed to save pending queue: %s", exc)

    try:
        db.close()
    except Exception:
        pass

    # Optional: chain an apply step automatically. We only do this when
    # the run finished successfully and there's something to apply.
    applied = 0
    if (
        final_status == "success"
        and bool(body.apply)
        and pending_count > 0
        and not job.cancel.is_set()
    ):
        # Record every COMMENT this auto-apply tail writes into
        # ``apply_events`` so the Audit page surfaces them. Mirrors the
        # explicit ``_apply_worker`` call below: without these audit
        # kwargs the auto-apply path stayed silent and the Audit feed
        # rendered only events triggered through the post-review
        # "Apply" button.
        import getpass
        import socket

        try:
            applied_by = getpass.getuser()
        except Exception:
            applied_by = ""
        try:
            hostname = socket.gethostname()
        except Exception:
            hostname = ""

        try:
            db_for_apply, _, _ = _scoped_connector(
                cfg, body.db_profile, body.database, body.catalog
            )
            applied = apply_review_results_to_db(
                db_for_apply,
                deferred,
                on_progress=_build_progress_callback(job),
                cancel_token=job.cancel,
                audit_log=hs,
                audit_profile=str(body.db_profile or getattr(cfg.db, "name", "") or ""),
                audit_user=applied_by,
                audit_host=hostname,
                audit_run_id=run_id,
            )
            try:
                db_for_apply.close()
            except Exception:
                pass
        except Exception as exc:
            log.warning("Auto-apply after run failed: %s", exc)
            final_error_text = f"Auto-apply failed: {exc}"

    # Late-cancel override. If the user clicked Cancel after the
    # last table finished but BEFORE the worker reached this line —
    # or any other window where the cancel set without a ``Run-
    # Cancelled`` ever being raised — the run's true outcome is
    # ``cancelled``, not ``success``. The token check here is what
    # made the difference for the user-reported "I cancelled but it
    # still flipped to success" case.
    if final_status == "success" and job.cancel.is_set():
        final_status = "cancelled"

    # When no tables were actually processed but some failed, the run
    # is a failure — surfacing it as ``success`` (the default) had
    # been misleading users who saw zero output but a green pill. The
    # cancelled / failed status branches above remain authoritative;
    # this only fires when the worker exited cleanly with no produced
    # results AND at least one table raised inside ``orchestrator.
    # process_table``.
    if final_status == "success" and len(processed_assets) == 0 and len(failed_assets) > 0:
        final_status = "failed"
        if not final_error_text:
            first_path, first_err = failed_assets[0]
            final_error_text = (
                f"{len(failed_assets)} asset(s) failed; first: {first_path} → {first_err}"
            )

    # Demote successful runs to ``ready_for_review`` when nothing was
    # actually written to the catalog. The worker finished cleanly but
    # 119/119 suggestions still sitting in the pending queue is not a
    # "success" — it's a queue waiting on the human review step. CLI's
    # ``analyze.run`` interrupt path uses the same rule (see
    # ``amx/cli_support/commands/_analyze/interrupt.py:65``); web
    # parity here keeps both surfaces telling the user the same story.
    if final_status == "success" and pending_count > 0 and int(applied) == 0:
        final_status = "ready_for_review"

    if hs is not None and run_id is not None:
        try:
            metrics_payload: dict[str, Any] = {
                "duration_sec": round(time.monotonic() - run_started, 3),
                "total_assets": total_assets,
                "total_schemas": len(scope),
                "processed_assets_count": len(processed_assets),
                "processed_assets": processed_assets,
                "skipped_assets_count": len(skipped_assets),
                "skipped_assets": skipped_assets,
                "failed_assets_count": len(failed_assets),
                "applied_flag": bool(body.apply),
                "applied_count": int(applied),
            }
            # PR E: merge the RAG-diagnostic keys (doc_profiles_used,
            # effective_run_doc_paths, rag_unavailable_reason,
            # rag_hits_total). Base metrics win on collision so an
            # accidental key reuse can never shadow run accounting.
            try:
                if rag_store_for_run is not None:
                    rag_extra_metrics.setdefault(
                        "rag_hits_total", int(getattr(rag_store_for_run, "doc_count", 0) or 0)
                    )
            except Exception:  # pragma: no cover - defensive
                pass
            # PR δ: parallel "how many code chunks were visible to this
            # run" counter. Surfaces on RunDetail as
            # "Code: profile-a · N chunks used" alongside the docs badge.
            try:
                from amx.codebase.code_rag import code_collection_count

                code_paths = list(cfg.effective_run_code_paths())
                rag_extra_metrics.setdefault(
                    "code_hits_total",
                    int(code_collection_count(source_filters=code_paths or None) or 0),
                )
            except Exception:  # pragma: no cover - defensive
                pass
            for k, v in rag_extra_metrics.items():
                metrics_payload.setdefault(k, v)
            hs.finish_run(
                run_id,
                status=final_status,
                metrics=metrics_payload,
                tokens={
                    "total_tokens": token_tracker.total_tokens,
                    "total_cost_usd": round(token_tracker.total_cost_usd, 8),
                    "summary": token_tracker.summary(),
                    "records": token_tracker.records(),
                },
                results={"pending_count": pending_count},
                error_text=final_error_text,
            )
        except Exception as exc:
            log.warning("finish_run failed: %s", exc)

    # Dispatch the terminal SSE event based on the FINAL status (after
    # all the demotion / late-cancel / all-failed overrides above).
    # Before this, the worker only emitted ``job.done`` for success /
    # ready_for_review and relied on the ``RunCancelled`` exception
    # handler to emit ``job.cancelled``. The late-cancel and all-
    # failed overrides above don't raise — they only flip ``final_
    # status`` — so without this branch the SPA's SSE consumer was
    # left waiting indefinitely for a terminal event that never came.
    summary = {
        "run_id": run_id,
        "processed": len(processed_assets),
        "failed": len(failed_assets),
        "pending": pending_count,
        "applied": int(applied),
        "run_status": final_status,
    }
    job.summary = summary
    job.ended_at = time.time()
    if final_status in ("success", "ready_for_review"):
        job.status = "done"
        emit_terminal(job.queue, "job.done", {"summary": summary})
    elif final_status == "cancelled":
        # ``job.status`` may already be ``cancelled`` from the
        # RunCancelled handler; setting it again is a no-op. The SSE
        # emit, however, is gated on whether the handler fired — we
        # only emit here when no terminal event was sent earlier, to
        # avoid the consumer seeing two ``job.cancelled`` frames.
        if job.status != "cancelled":
            job.status = "cancelled"
            emit_terminal(job.queue, "job.cancelled", {"summary": summary})
    elif final_status == "failed":
        if job.status != "failed":
            job.status = "failed"
            job.error = final_error_text or job.error
            emit_terminal(
                job.queue,
                "job.failed",
                {"error": final_error_text or "Run failed", "summary": summary},
            )

    # The analysis_runs row is now terminal, so the scheduler sweep
    # won't touch it whether or not the ticker keeps beating; stop the
    # daemon thread so it doesn't outlive the worker.
    if heartbeat_stop is not None:
        heartbeat_stop.set()
    if heartbeat_thread is not None:
        heartbeat_thread.join(timeout=2.0)


def _fail_job(job: Job, message: str) -> None:
    """Helper: stamp a job as failed and emit a terminal event."""
    job.status = "failed"
    job.error = message
    job.ended_at = time.time()
    emit(job.queue, "activity.fail", {"idx": 0, "detail": message})
    emit_terminal(job.queue, "job.failed", {"error": message})


def _process_scope_batch(
    *,
    orchestrator: Orchestrator,
    scope: dict[str, list[str]],
    job: Job,
    hs: Any,
    run_id: int | None,
    total_assets: int,
    processed_assets: list[str],
    failed_assets: list[tuple[str, str]],
    cache_override_assets: set[str] | None = None,
) -> None:
    """Run the scope through the provider's Batch API, one schema at a time.

    The CLI's run_loop submits one batch per schema (see
    ``cli_support/commands/_analyze/run_loop.py:120``); we mirror that
    grouping so the SSE stream reports per-schema progress and the
    user sees a turnaround that reflects the underlying batch jobs.
    Within a schema there's no per-table progress because the batch
    completes as a unit — we emit one ``activity.added`` per schema
    plus one ``activity.complete`` when its results return.

    ``cache_override_assets`` is the set of ``"schema.table"`` strings
    the Studio reachability pre-flight flagged as unreadable on the
    live DB — the user chose "Use cached schema" in the dialog. For
    those assets the orchestrator substitutes a catalog-cached
    metadata profile instead of calling ``profile_table``.
    """
    asset_kinds_cache: dict[tuple[str, str], Any] = {}
    idx_global = 0
    for schema, tables in scope.items():
        if job.cancel.is_set():
            raise RunCancelled(f"Cancelled before schema {schema}")
        idx_global += 1
        label = ", ".join(tables) if len(tables) <= 3 else f"{len(tables)} assets"
        emit(
            job.queue,
            "activity.added",
            {
                "idx": idx_global,
                "label": f"{schema} ({label})",
                "kind": "schema",
                "done": idx_global - 1,
                "total": len(scope),
                "asset_count": len(tables),
            },
        )
        emit(job.queue, "activity.begin", {"idx": idx_global})
        try:
            asset_kinds = {
                t: asset_kinds_cache.setdefault(
                    (schema, t), orchestrator.db.resolve_asset_kind(schema, t)
                )
                for t in tables
            }
            schema_results = orchestrator.process_tables_batch_mode(
                schema,
                list(tables),
                asset_kinds=asset_kinds,
                cancel_token=job.cancel,
                cache_override_assets=cache_override_assets,
            )
        except RunCancelled:
            raise
        except NoSuchTableError as exc:
            # Live DB can't reflect the table even though the catalog
            # has it. Surface an actionable message instead of the raw
            # SQLAlchemy class name. Studio's pre-flight should have
            # caught this (and let the user pick the cache override);
            # this branch covers CLI / direct API callers and any race
            # where the table got dropped between pre-flight and run.
            remediation = (
                f"Live database can't read this table — was it dropped after the "
                f"last catalog sync? Re-run /search sync to refresh the catalog, "
                f"or re-submit the run from Studio to opt into the cached-schema "
                f"override. (raw: {exc.__class__.__name__})"
            )
            for table in tables:
                failed_assets.append((f"{schema}.{table}", remediation))
            log.warning(
                "Batch run for schema %s hit NoSuchTableError; surfaced remediation message",
                schema,
            )
            emit(
                job.queue,
                "activity.fail",
                {"idx": idx_global, "detail": remediation},
            )
            continue
        except Exception as exc:
            for table in tables:
                failed_assets.append((f"{schema}.{table}", str(exc)))
            log.warning("Batch run for schema %s failed: %s", schema, exc)
            emit(
                job.queue,
                "activity.fail",
                {"idx": idx_global, "detail": f"{exc.__class__.__name__}: {exc}"},
            )
            continue

        for table in tables:
            processed_assets.append(f"{schema}.{table}")

        # Stream per-table previews so the run-detail page renders the
        # same rich list as a chat-mode run. We pull from the persisted
        # history rows when available; otherwise fall back to the
        # in-memory ReviewResult list filtered by table.
        results_by_table: dict[str, list[Any]] = {}
        for r in schema_results:
            results_by_table.setdefault(getattr(r, "table", ""), []).append(r)

        for table in tables:
            column_details = _column_details_for_table(hs, run_id, schema, table)
            table_rows = results_by_table.get(table, [])
            emit(
                job.queue,
                "activity.complete",
                {
                    "idx": idx_global,
                    "detail": f"{len(table_rows)} suggestion(s) (batch)",
                    "schema": schema,
                    "table": table,
                    "results": column_details or [_review_result_to_event(r) for r in table_rows],
                },
            )
        _emit_tokens_snapshot(job.queue)


def _apply_worker(cfg: AMXConfig, job: Job, body: ApplyRequest) -> None:
    job.status = "running"
    emit(
        job.queue,
        "activity.added",
        {"idx": 0, "label": "Writing approved descriptions"},
    )
    emit(job.queue, "activity.begin", {"idx": 0})

    try:
        results = _resolve_apply_results(body)
    except FileNotFoundError as exc:
        job.status = "failed"
        job.error = str(exc)
        job.ended_at = time.time()
        emit(job.queue, "activity.fail", {"idx": 0, "detail": job.error})
        emit_terminal(job.queue, "job.failed", {"error": job.error})
        return

    if not results:
        job.status = "done"
        job.summary = {"applied": 0, "total": 0}
        job.ended_at = time.time()
        emit(job.queue, "activity.complete", {"idx": 0, "detail": "No approved rows to apply."})
        emit_terminal(job.queue, "job.done", {"summary": job.summary})
        return

    # Pending entries don't carry the originating ``db_profile`` /
    # ``database`` / ``catalog`` triple (the on-disk pending file just
    # stores schema/table/column + description). The Studio Pending
    # page submits an empty body, so without a fallback the worker
    # would open a connector against ``cfg.active_db_profile`` + the
    # profile's PINNED database — which for a profile with no pinned
    # database lands the connection on postgres' default ``postgres``
    # database, and every ``COMMENT ON TABLE`` then fails with
    # ``InvalidSchemaName`` because the user's schema lives in a
    # different database. Derive scope from the first result with a
    # known run so single-run Pending apply lands on the right
    # connection. Multi-run Pending queues still take the first
    # scope; the run scope override on the body still wins when set.
    derived_profile = body.db_profile
    derived_database = body.database
    derived_catalog = body.catalog
    if (
        not (derived_profile or "").strip()
        and not (derived_database or "").strip()
        and not (derived_catalog or "").strip()
    ):
        try:
            from amx.storage.sqlite_store import history_store as _hs_for_scope

            _hs = _hs_for_scope()
        except Exception:
            _hs = None
        if _hs is not None:
            from amx.storage._history_results import get_run_result

            for r in results:
                if r.result_id is None:
                    continue
                try:
                    rr = get_run_result(_hs, int(r.result_id))
                except Exception:
                    continue
                if not rr:
                    continue
                run_id = rr.get("run_id")
                if run_id is None:
                    continue
                try:
                    run_row = _hs.get_run(int(run_id))
                except Exception:
                    continue
                if not run_row:
                    continue
                # ``settings_json`` carries the studio.generate.singleshot
                # trigger payload (``database`` + ``catalog`` set at
                # generate time). Older analyze.run rows persist the same
                # fields via the bulk pipeline. Either way, the keys are
                # the same names we feed back into ``_scoped_connector``.
                import json as _json

                raw_settings = run_row.get("settings_json") or "{}"
                try:
                    settings = (
                        _json.loads(raw_settings) if isinstance(raw_settings, str) else raw_settings
                    )
                except Exception:
                    settings = {}
                derived_profile = run_row.get("db_profile") or derived_profile
                derived_database = settings.get("database") or derived_database
                derived_catalog = settings.get("catalog") or derived_catalog
                if derived_profile or derived_database or derived_catalog:
                    log.info(
                        "apply: derived scope from result_id=%s run_id=%s "
                        "(profile=%r database=%r catalog=%r)",
                        r.result_id,
                        run_id,
                        derived_profile,
                        derived_database,
                        derived_catalog,
                    )
                    # Mirror the derived scope back onto ``body`` so the
                    # downstream catalog / audit hooks (which still read
                    # ``body.db_profile`` / ``body.database`` /
                    # ``body.catalog``) see the same scope we open the
                    # connector under.
                    body.db_profile = derived_profile
                    body.database = derived_database
                    body.catalog = derived_catalog
                    break

    try:
        db, _, _ = _scoped_connector(cfg, derived_profile, derived_database, derived_catalog)
    except HTTPException as exc:
        job.status = "failed"
        job.error = str(exc.detail)
        job.ended_at = time.time()
        emit(job.queue, "activity.fail", {"idx": 0, "detail": job.error})
        emit_terminal(job.queue, "job.failed", {"error": job.error})
        return
    # Mirror the CLI's apply path: every successful row records its
    # applied_at timestamp + clears the rejection_reason so the SPA's
    # "queued" badge flips to "applied" after invalidateQueries; every
    # failed row records the rejection_reason. Without these the
    # Studio-initiated apply succeeded against the live DB but the
    # run_results SQLite row stayed ``applied_at=null`` and the badge
    # was stuck on "queued" indefinitely.
    from amx.storage.sqlite_store import history_store as _history_store

    hs = _history_store()

    def _on_applied(r: ReviewResult) -> None:
        if hs is not None and r.result_id is not None:
            try:
                hs.record_applied(
                    r.result_id,
                    chosen_description=r.final_description or None,
                )
            except Exception:
                pass
        # Sync the catalog so concept search reflects the just-written
        # description on the very next /ask. Without this the CLI
        # apply path already updated catalog state via
        # ``Orchestrator._record_applied_state`` but the Studio worker
        # used to only update the run_results SQLite row — meaning a
        # user could /apply a description and still get "no description
        # found" from concept search until the next manual /search sync.
        try:
            from amx.search.catalog import SearchCatalog

            catalog = SearchCatalog.from_history_store()
        except Exception as exc:  # pragma: no cover - best-effort
            catalog = None
            log.debug("Could not open catalog for post-apply sync: %s", exc)
        if catalog is None:
            return
        if r.result_id is not None:
            try:
                catalog.mark_applied(r.result_id)
            except Exception as exc:  # pragma: no cover - best-effort
                log.debug("catalog.mark_applied(%s) failed: %s", r.result_id, exc)
        # Belt-and-braces: even when ``mark_applied`` flips
        # applied_to_db=1 on an existing row, the text on that row may
        # be a stale draft (user inline-edited at apply time). Insert
        # a fresh reviewed row carrying the exact text written to the
        # live DB so the catalog's effective description matches the
        # COMMENT the user is about to read on the next /ask.
        try:
            catalog.record_applied_description(
                db_profile=str(body.db_profile or getattr(cfg.db, "name", "") or ""),
                db_backend=str(getattr(cfg.db, "backend", "") or ""),
                database_name=str(
                    body.database
                    or body.catalog
                    or getattr(cfg.db, "database", "")
                    or getattr(cfg.db, "catalog", "")
                    or getattr(cfg.db, "project", "")
                    or ""
                ),
                schema_name=r.schema,
                table_name=r.table or "",
                column_name=r.column,
                entity_kind="column" if r.column else "table",
                asset_kind=(r.asset_kind or "table"),
                description=r.final_description or "",
                run_id=getattr(r, "run_id", None),
                result_id=r.result_id,
            )
        except Exception as exc:  # pragma: no cover - best-effort
            log.debug(
                "catalog.record_applied_description failed for %s.%s.%s: %s",
                r.schema,
                r.table,
                r.column,
                exc,
            )

    def _on_failed(r: ReviewResult, exc: Exception) -> None:
        if hs is not None and r.result_id is not None:
            try:
                hs.record_db_apply_failure(r.result_id, str(exc))
            except Exception:
                pass

    # Track which runs contributed how many freshly-applied rows so we
    # can batch-increment ``analysis_runs.applied_count`` and transition
    # the run's ``status`` once after the apply loop completes. Without
    # these two updates the Runs list pill stayed at "ready" forever
    # even after the user applied every row — the CLI path already
    # does both via ``increment_run_applied`` + ``update_run_status``.
    applied_run_ids: dict[int, int] = {}

    def _track_run(r: ReviewResult) -> None:
        if hs is None or r.result_id is None:
            return
        try:
            row = hs.get_run_result(int(r.result_id))
        except Exception:
            return
        if not row:
            return
        rid = row.get("run_id")
        if rid is None:
            return
        applied_run_ids[int(rid)] = applied_run_ids.get(int(rid), 0) + 1

    original_on_applied = _on_applied

    def _on_applied_and_track(r: ReviewResult) -> None:
        original_on_applied(r)
        _track_run(r)

    # Build the audit context once so each successful COMMENT write
    # lands in apply_events with the correct attribution. Mirrors the
    # CLI path (amx/cli_support/commands/run.py): without these
    # arguments the CLI's apply path logs to apply_events but the
    # Studio path stayed silent — the Audit page rendered "0 events"
    # even after a successful Apply pending queue.
    import getpass
    import socket

    try:
        applied_by = getpass.getuser()
    except Exception:
        applied_by = ""
    try:
        hostname = socket.gethostname()
    except Exception:
        hostname = ""

    outcomes: list[RowApplyOutcome] = []
    try:
        applied = apply_review_results_to_db(
            db,
            results,
            on_applied=_on_applied_and_track,
            on_failed=_on_failed,
            on_progress=_build_progress_callback(job),
            cancel_token=job.cancel,
            audit_log=hs,
            audit_profile=str(body.db_profile or getattr(cfg.db, "name", "") or ""),
            audit_user=applied_by,
            audit_host=hostname,
            outcomes_out=outcomes,
        )
    except RunCancelled:
        # apply_review_results_to_db already commits-what-was-applied
        # before raising; this branch only triggers if a future
        # version starts raising explicitly. Treat both as cancelled.
        job.status = "cancelled"
        job.summary = {"applied": 0, "total": len(results)}
        job.ended_at = time.time()
        emit(job.queue, "activity.fail", {"idx": 0, "detail": "Cancelled."})
        emit_terminal(job.queue, "job.cancelled", {"summary": job.summary})
        return
    except Exception as exc:
        job.status = "failed"
        job.error = str(exc)
        job.ended_at = time.time()
        emit(job.queue, "activity.fail", {"idx": 0, "detail": str(exc)})
        emit_terminal(job.queue, "job.failed", {"error": job.error})
        return
    finally:
        try:
            db.close()
        except Exception:  # pragma: no cover - best-effort cleanup
            pass

    if job.cancel.is_set():
        # The loop returned cleanly, but the user's cancel signal
        # had fired. Surface as cancelled so the SPA shows the
        # "you cancelled mid-flight" banner.
        job.status = "cancelled"
        job.summary = {"applied": int(applied), "total": len(results)}
        job.ended_at = time.time()
        emit(
            job.queue,
            "activity.complete",
            {"idx": 0, "detail": f"Cancelled after {applied}/{len(results)}."},
        )
        emit_terminal(job.queue, "job.cancelled", {"summary": job.summary})
        return

    # Drain the on-disk pending queue, but ONLY for rows whose live DB
    # write actually committed. Failed rows stay queued so the user
    # sees the classified error and can retry / edit / skip without
    # re-accepting every suggestion. ``outcomes_out`` is the truth
    # source — never ``applied`` alone, because that count masks per-
    # row partial failure on partially-applied queues.
    if body.results is None:
        applied_ids = [
            o.result_id for o in outcomes if o.status == "applied" and o.result_id is not None
        ]
        if applied_ids:
            try:
                clear_pending_for(applied_ids)
            except Exception:
                pass

    # Roll the per-run applied count and status forward so the Runs
    # list pill stops claiming "ready" after the user applied rows.
    # Mirrors the CLI's ``run_loop.py`` / ``run_summary.py`` behaviour.
    # ``applied_partial`` is a new status surfacing "some applied, some
    # still pending" — distinct from plain ``success`` (everything
    # applied) and ``ready_for_review`` (nothing applied yet).
    if hs is not None and applied_run_ids:
        for rid, count in applied_run_ids.items():
            try:
                hs.increment_run_applied(rid, by=count)
            except Exception as exc:  # pragma: no cover — best-effort
                log.debug("increment_run_applied(%s, %s) failed: %s", rid, count, exc)
            try:
                run_row = hs.get_run(rid)
                applied_total = int((run_row or {}).get("applied_count") or 0)
                pending_remaining = len(hs.get_run_results(rid, unevaluated_only=True))
                if applied_total > 0:
                    new_status = "success" if pending_remaining == 0 else "applied_partial"
                    hs.update_run_status(rid, new_status)
            except Exception as exc:  # pragma: no cover — best-effort
                log.debug("post-apply status transition for run %s failed: %s", rid, exc)

    # Job summary carries per-row outcomes so the SPA can render a
    # truthful counter: "N applied, M failed" instead of the legacy
    # "N applied" that hid the failure of M rows. The ``failed`` list
    # is small (only failed rows; payload stays under a kilobyte for
    # typical queues) and the SPA renders the classified error chip
    # next to each entry. Pre-PR consumers reading just ``applied`` /
    # ``total`` still work — those keys are kept under the same names.
    failed_outcomes = [o for o in outcomes if o.status == "failed"]
    job.status = "done" if not failed_outcomes else "applied_partial"
    job.summary = {
        "applied": int(applied),
        "total": len(results),
        "failed_count": len(failed_outcomes),
        "applied_ids": [
            o.result_id for o in outcomes if o.status == "applied" and o.result_id is not None
        ],
        "failed": [
            {
                "result_id": o.result_id,
                "schema": o.schema,
                "table": o.table,
                "column": o.column,
                "asset_kind": o.asset_kind,
                "error_kind": o.error_kind,
                "error_title": o.error_title,
                "error_text": o.error_text,
                "error_action": o.error_action,
            }
            for o in failed_outcomes
        ],
    }
    job.ended_at = time.time()
    if failed_outcomes:
        detail = (
            f"Applied {applied}/{len(results)} — {len(failed_outcomes)} failed "
            f"(queue preserved)."
        )
    else:
        detail = f"Applied {applied}/{len(results)}."
    emit(job.queue, "activity.complete", {"idx": 0, "detail": detail})
    emit_terminal(job.queue, "job.done", {"summary": job.summary})
