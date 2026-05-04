"""SSE-streaming ``/ask`` endpoint for the visualizer.

The chat panel hits ``POST /api/ask`` with a question + optional
session id; the backend spawns a worker that calls
:func:`amx.search.tool_agent.run_tool_agent` with the additive
callback hooks (``on_thinking_delta`` / ``on_tool_call`` /
``cancel_token``) PR-D added. Each callback shoves an event onto
the job's queue; the SPA's EventSource consumer drains it.

Sessions reuse :class:`amx.search.session_store.ChatSessionStore`
so a turn started in the CLI's ``/ask`` shows up in the SPA's
sessions sidebar (and vice versa). Web-originated turns are
persisted with the same shape the CLI writes — single source of
truth at the SQLite level.
"""

from __future__ import annotations

import json
import threading
import time
from queue import Empty
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sse_starlette.sse import EventSourceResponse

from amx.agents.orchestrator import RunCancelled
from amx.config import AMXConfig
from amx.llm.provider import LLMProvider
from amx.search.catalog import SearchCatalog
from amx.search.session_store import ChatSessionStore
from amx.search.tool_agent import run_tool_agent
from amx.storage.sqlite_store import history_store
from amx.web.deps import get_cfg, get_jobs
from amx.web.jobs import Job, JobRegistry
from amx.web.progress_bus import emit, emit_terminal

router = APIRouter(prefix="/api/ask", tags=["ask"])


class AskRequest(BaseModel):
    question: str = Field(..., min_length=1)
    session_id: int | None = Field(
        default=None,
        description="Resume an existing chat session. Omit to start a fresh one.",
    )
    db_profile: str | None = Field(
        default=None,
        description="Override the active DB profile for retrieval scope.",
    )


@router.post("")
def submit_ask(
    body: AskRequest,
    cfg: AMXConfig = Depends(get_cfg),
    jobs: JobRegistry = Depends(get_jobs),
) -> dict[str, Any]:
    """Spawn an ask worker. Returns a job id the SPA subscribes to,
    plus the session id (existing or newly minted)."""
    db_profile = (body.db_profile or cfg.active_db_profile or "default").strip()
    llm_profile = (cfg.active_llm_profile or "default").strip()

    session_id = body.session_id
    store = _session_store_or_none()
    if store is not None and session_id is None:
        try:
            session_id = store.start_session(
                db_profile=db_profile,
                llm_profile=llm_profile,
                title=body.question[:80],
            )
        except Exception:
            session_id = None
    if store is not None and session_id is not None:
        try:
            store.append_user_turn(int(session_id), question=body.question)
        except Exception:
            # Persistence is best-effort — never fail the SSE handshake.
            pass

    job = jobs.new_job("ask")
    thread = threading.Thread(
        target=_ask_worker,
        args=(cfg, job, body.question, session_id, db_profile),
        name=f"amx-visualizer-ask-{job.id}",
        daemon=True,
    )
    thread.start()
    return {"job_id": job.id, "session_id": session_id, "status": job.status}


# NOTE: ``/sessions`` routes are declared BEFORE the ``/{job_id}``
# routes so FastAPI doesn't match the literal string ``sessions`` as
# a job id. Order matters here.


@router.get("/sessions")
def list_sessions(
    limit: int = 20,
    include_ended: bool = True,
) -> dict[str, Any]:
    """List the user's recent chat sessions. Empty list when the
    history store hasn't been initialised yet (fresh CLI session)."""
    store = _session_store_or_none()
    if store is None:
        return {"sessions": [], "count": 0}
    rows = store.list_sessions(limit=limit, include_ended=include_ended)
    return {"sessions": rows, "count": len(rows)}


@router.get("/sessions/{session_id}")
def get_session(session_id: int) -> dict[str, Any]:
    store = _session_store_or_none()
    if store is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Chat history isn't available — initialise the history store first.",
        )
    session = store.get_session(session_id)
    if session is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No chat session {session_id}.",
        )
    # ``recent_turns`` is the canonical accessor on ChatSessionStore. We
    # include compacted+summary rows so the browser can show the full
    # history (the agent itself only sees the live tail in follow-ups).
    raw_turns = store.recent_turns(
        session_id,
        include_compacted=True,
        include_summary=True,
    )
    turns = [
        {
            "role": str(t.get("role") or ""),
            "question": str(t.get("question") or ""),
            "answer_summary": str(t.get("answer_summary") or ""),
            "turn_index": int(t.get("turn_index") or 0),
            "created_at": t.get("created_at"),
        }
        for t in raw_turns
    ]
    return {"session": session, "turns": turns}


@router.get("/{job_id}")
def get_ask_job(job_id: str, jobs: JobRegistry = Depends(get_jobs)) -> dict[str, Any]:
    job = jobs.get(job_id)
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"No job {job_id}")
    return job.to_public_dict()


@router.get("/{job_id}/events")
def stream_ask_events(job_id: str, jobs: JobRegistry = Depends(get_jobs)) -> EventSourceResponse:
    job = jobs.get(job_id)
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"No job {job_id}")
    return EventSourceResponse(_event_generator(job))


@router.post("/{job_id}/cancel")
def cancel_ask(job_id: str, jobs: JobRegistry = Depends(get_jobs)) -> dict[str, Any]:
    if not jobs.cancel(job_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No active ask job {job_id} to cancel.",
        )
    return {"ok": True, "job_id": job_id}


# ── Internals ──────────────────────────────────────────────────────────


def _session_store_or_none() -> ChatSessionStore | None:
    hs = history_store()
    if hs is None:
        return None
    return ChatSessionStore(hs)


def _event_generator(job: Job):
    """Tail the job's queue until a terminal event arrives. SSE
    keepalives ride alongside so corporate proxies don't reap idle
    streams while the agent is mid-LLM-call."""
    last_keepalive = time.monotonic()
    while True:
        try:
            event = job.queue.get(timeout=15)
        except Empty:
            now = time.monotonic()
            if now - last_keepalive > 14:
                yield {"event": "ping", "data": json.dumps({"t": now})}
                last_keepalive = now
            if job.status not in ("queued", "running"):
                break
            continue
        kind = str(event.get("type", ""))
        yield {"event": kind, "data": json.dumps(event)}
        if kind in {"job.done", "job.cancelled", "job.failed"}:
            break


def _ask_worker(
    cfg: AMXConfig,
    job: Job,
    question: str,
    session_id: int | None,
    db_profile: str,
) -> None:
    """Run the tool-calling agent + stream every reasoning chunk and
    tool result back to the SSE consumer. Persists the assistant
    turn to chat_sessions on success."""
    job.status = "running"
    emit(job.queue, "activity.added", {"idx": 0, "label": "Thinking"})
    emit(job.queue, "activity.begin", {"idx": 0})

    catalog = _load_catalog()
    if catalog is None:
        job.status = "failed"
        job.error = "Search catalog isn't initialised yet — run /search sync first."
        job.ended_at = time.time()
        emit(job.queue, "activity.fail", {"idx": 0, "detail": job.error})
        emit_terminal(job.queue, "job.failed", {"error": job.error})
        return

    llm = LLMProvider(cfg.llm)

    # ``on_thinking_delta`` from tool_agent forwards CUMULATIVE reasoning
    # text (the CLI display takes a tail of it). The browser builds the
    # panel by appending each event's text, so we must emit only the new
    # suffix here — otherwise the user sees "TheThe userThe user is…".
    last_thinking = ""

    def _on_thinking(text: str) -> None:
        nonlocal last_thinking
        if not text:
            return
        if text.startswith(last_thinking):
            chunk = text[len(last_thinking):]
        else:
            chunk = text
        last_thinking = text
        if chunk:
            emit(job.queue, "thinking.delta", {"text": chunk})

    def _on_tool_call(summary: dict[str, Any]) -> None:
        emit(
            job.queue,
            "tool.call",
            {
                "name": summary.get("name", ""),
                "arguments": summary.get("arguments", "{}"),
                "result_preview": summary.get("result_preview", ""),
            },
        )

    try:
        result = run_tool_agent(
            cfg=cfg,
            catalog=catalog,
            llm=llm,
            question=question,
            answer_language=cfg.llm.language or "english",
            session_memory=None,  # PR-D ships without session memory wiring; PR-E adds it.
            display=None,
            on_thinking_delta=_on_thinking,
            on_tool_call=_on_tool_call,
            cancel_token=job.cancel,
        )
    except RunCancelled:
        job.status = "cancelled"
        job.ended_at = time.time()
        emit(job.queue, "activity.fail", {"idx": 0, "detail": "Cancelled."})
        emit_terminal(job.queue, "job.cancelled", {})
        return
    except Exception as exc:
        job.status = "failed"
        job.error = str(exc)
        job.ended_at = time.time()
        emit(job.queue, "activity.fail", {"idx": 0, "detail": job.error})
        emit_terminal(job.queue, "job.failed", {"error": job.error})
        return

    # Persist the assistant's reply (best-effort).
    if session_id is not None:
        store = _session_store_or_none()
        if store is not None:
            try:
                store.append_assistant_turn(
                    int(session_id),
                    answer=result.answer,
                )
            except Exception:
                pass

    emit(job.queue, "thinking.stop", {})
    emit(
        job.queue,
        "answer.final",
        {
            "answer": result.answer,
            "tool_calls": list(result.tool_calls),
            "iterations": result.iterations,
            "usage": dict(result.usage),
            "finish_reason": result.finish_reason,
        },
    )
    emit(
        job.queue,
        "activity.complete",
        {"idx": 0, "detail": f"{result.iterations} iteration(s)"},
    )
    job.status = "done"
    job.summary = {
        "answer": result.answer,
        "iterations": result.iterations,
        "tool_calls": len(result.tool_calls),
        "usage": dict(result.usage),
    }
    job.ended_at = time.time()
    emit_terminal(job.queue, "job.done", {"summary": job.summary})


def _load_catalog() -> SearchCatalog | None:
    """Resolve the SearchCatalog bound to the active history store.

    Returns ``None`` when the history store isn't initialised yet
    (fresh CLI session before any DB profile activated). Callers
    surface that as a clean ``job.failed`` event.
    """
    return SearchCatalog.from_history_store()
