"""SSE-streaming ``/ask`` endpoint for AMX Studio.

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
from amx.utils.logging import get_logger
from amx.web.deps import get_cfg, get_jobs
from amx.web.jobs import Job, JobRegistry
from amx.web.progress_bus import emit, emit_terminal

router = APIRouter(prefix="/api/ask", tags=["ask"])
log = get_logger("web.ask")


class AskRequest(BaseModel):
    question: str = Field(..., min_length=1)
    session_id: int | None = Field(
        default=None,
        description="Resume an existing chat session. Omit to start a fresh one.",
    )
    db_profile: str | None = Field(
        default=None,
        description=(
            "Anchor profile for chat session metadata (history rows, LLM "
            "settings). The actual retrieval scope is the list in "
            "``scope_profiles`` or, when omitted, the session's sticky scope "
            "(see PATCH /api/ask/sessions/{id}). Kept for back-compat with "
            "pre-multi-profile clients."
        ),
    )
    scope_profiles: list[str] | None = Field(
        default=None,
        description=(
            "Multi-profile retrieval scope for THIS question. When provided, "
            "overrides the session's sticky scope without persisting. To set "
            "a sticky scope across the whole chat, PATCH /api/ask/sessions/"
            "{id} instead."
        ),
    )


class UpdateSessionRequest(BaseModel):
    """Body for ``PATCH /api/ask/sessions/{id}`` — sticky scope update.

    ``scope_profiles=None`` clears the override and the session falls back
    to ``cfg.db_profiles.keys()``. ``focus_profile`` is informational
    (auto-detected from prior turns) and persisted for cross-tab display.
    """

    scope_profiles: list[str] | None = None
    focus_profile: str | None = None


def _resolve_ask_scope(
    cfg: AMXConfig,
    body_scope: list[str] | None,
    session_scope: list[str] | None,
) -> list[str]:
    """Pick the effective scope for a single ask request.

    Precedence: per-question body > session sticky > config default.
    Returns the dedup'd, valid list of DB profile names.
    """
    candidates: list[str] = []
    if body_scope is not None:
        candidates = list(body_scope)
    elif session_scope is not None:
        candidates = list(session_scope)
    else:
        # Default: every saved DB profile in the config. The user picked
        # this in PR-A planning — Studio is multi-profile by default.
        candidates = list(cfg.db_profiles.keys())
    seen: set[str] = set()
    out: list[str] = []
    for name in candidates:
        clean = (name or "").strip()
        if not clean or clean in seen:
            continue
        if clean not in cfg.db_profiles:
            # Drop ghost profiles (config edited mid-session) silently —
            # the agent operates only on profiles that exist now.
            continue
        seen.add(clean)
        out.append(clean)
    return out


@router.post("")
def submit_ask(
    body: AskRequest,
    cfg: AMXConfig = Depends(get_cfg),
    jobs: JobRegistry = Depends(get_jobs),
) -> dict[str, Any]:
    """Spawn an ask worker. Returns a job id the SPA subscribes to,
    plus the session id (existing or newly minted).

    Pre-flight: validates the LLM config so a misconfigured profile
    fails fast with a helpful 412 instead of stranding the SPA on a
    "Reasoning…" spinner that never resolves (the worker thread
    couldn't open an SSE stream with a clean error event before this
    check existed).
    """
    if not cfg.llm or not (cfg.llm.provider or "").strip():
        raise HTTPException(
            status_code=status.HTTP_412_PRECONDITION_FAILED,
            detail={
                "message": (
                    "No active LLM profile. Open Settings → LLM and pick "
                    "a provider before asking a question."
                ),
                "hint": "configure-llm",
            },
        )
    if not (cfg.llm.model or "").strip():
        raise HTTPException(
            status_code=status.HTTP_412_PRECONDITION_FAILED,
            detail={
                "message": (
                    "Active LLM profile has no model selected. Open "
                    "Settings → LLM and pick a model."
                ),
                "hint": "configure-llm",
            },
        )
    db_profile = (body.db_profile or cfg.active_db_profile or "default").strip()
    llm_profile = (cfg.active_llm_profile or "default").strip()

    session_id = body.session_id
    store = _session_store_or_none()

    # Pull the session's sticky scope (set via PATCH /api/ask/sessions/{id})
    # so an /ask without an explicit scope_profiles in the body still
    # honours what the user picked in the dropdown for THIS chat.
    session_scope: list[str] | None = None
    if store is not None and session_id is not None:
        try:
            session_scope = store.get_scope(int(session_id))
        except Exception:
            session_scope = None

    if store is not None and session_id is None:
        # First message of a new session — also seed the sticky scope so
        # the SPA can resume cleanly if the page is reloaded.
        initial_scope = list(body.scope_profiles) if body.scope_profiles is not None else None
        try:
            session_id = store.start_session(
                db_profile=db_profile,
                llm_profile=llm_profile,
                title=body.question[:80],
                scope_profiles=initial_scope,
            )
            session_scope = initial_scope
        except Exception:
            session_id = None
    if store is not None and session_id is not None:
        try:
            store.append_user_turn(int(session_id), question=body.question)
        except Exception:
            # Persistence is best-effort — never fail the SSE handshake.
            pass

    scope_profiles = _resolve_ask_scope(cfg, body.scope_profiles, session_scope)

    job = jobs.new_job("ask")
    thread = threading.Thread(
        target=_ask_worker,
        args=(cfg, job, body.question, session_id, db_profile, scope_profiles),
        name=f"amx-studio-ask-{job.id}",
        daemon=True,
    )
    thread.start()
    return {
        "job_id": job.id,
        "session_id": session_id,
        "status": job.status,
        "scope_profiles": list(scope_profiles),
    }


@router.patch("/sessions/{session_id}")
def update_session(
    session_id: int,
    body: UpdateSessionRequest,
) -> dict[str, Any]:
    """Update sticky scope (and optionally focus_profile) on a chat
    session. The Studio dropdown writes through here so the new scope
    persists across page reloads and shows on the CLI's ``/ask-scope``
    output if the user inspects the same session.
    """
    store = _session_store_or_none()
    if store is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Chat history isn't available — initialise the history store first.",
        )
    if store.get_session(session_id) is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No chat session {session_id}.",
        )
    store.update_scope(
        session_id,
        scope_profiles=body.scope_profiles,
        focus_profile=body.focus_profile,
    )
    return {
        "ok": True,
        "session_id": int(session_id),
        "scope_profiles": body.scope_profiles,
        "focus_profile": body.focus_profile,
    }


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


@router.post("/sessions/{session_id}/end")
def end_session(session_id: int, cfg: AMXConfig = Depends(get_cfg)) -> dict[str, Any]:
    """Mark a chat session as ended so the next ``/api/ask`` POST
    starts a fresh one. Mirrors the CLI's ``/session end`` command."""
    store = _session_store_or_none()
    if store is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Chat history isn't available — initialise the history store first.",
        )
    if store.get_session(session_id) is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No chat session {session_id}.",
        )
    store.end_session(session_id)
    # Clear the active chat session pointer if it was pointing here so
    # the CLI REPL also reflects the end.
    if int(getattr(cfg, "active_chat_session_id", 0) or 0) == int(session_id):
        cfg.active_chat_session_id = 0
        try:
            cfg.save()
        except Exception:
            pass
    return {"ok": True, "session_id": int(session_id)}


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


#: How many prior Q/A pairs to feed back into the agent when
#: resuming a chat. The CLI's SessionMemoryMixin reads
#: ``conversation_memory_turns`` from search settings (default 4);
#: Studio mirrors that default — covers the typical "follow-up to a
#: question 2-3 turns ago" pattern without ballooning the prompt.
_MEMORY_TURN_PAIRS = 4


def _load_session_memory(session_id: int | None) -> list[dict[str, Any]] | None:
    """Hydrate the prior-turn context the agent loop replays.

    When a Studio user reopens an old chat session and asks a follow-up,
    ``run_tool_agent`` needs to see what was said before so references
    like "that table", "the second one", "in Turkish" resolve. Without
    this, every /ask in a resumed session looks like a fresh question
    — exactly the user-reported "old chat forgot history" bug.

    Skips the most-recent user row, which the caller has already
    inserted via ``append_user_turn`` for the current question; if we
    forwarded it the LLM would see the question twice and double-
    process. Compaction summary rows are surfaced as a synthetic
    ``user`` message tagged "(prior conversation summary)" so the
    model uses them as context rather than treating them as the next
    user turn.

    Returns ``None`` when session memory isn't available (no session,
    no history store, or fetch failed) — the agent loop treats that
    identically to an empty list.
    """
    if not session_id:
        return None
    store = _session_store_or_none()
    if store is None:
        return None
    try:
        turns = store.recent_turns(
            int(session_id),
            limit=_MEMORY_TURN_PAIRS,
            include_summary=True,
            include_compacted=False,
        )
    except Exception as exc:  # pragma: no cover — best-effort
        log.warning("Could not load chat history for session %s: %s", session_id, exc)
        return None
    if not turns:
        return None
    # Drop the trailing user-row that ``submit_ask`` just inserted for
    # the question we're about to answer; sending it back would double-
    # post the question into the conversation.
    pruned = list(turns)
    while pruned and pruned[-1].get("role") == "user":
        pruned.pop()
    memory: list[dict[str, Any]] = []
    for turn in pruned:
        role = str(turn.get("role") or "")
        if role == "summary":
            summary_text = str(turn.get("answer_summary") or "").strip()
            if summary_text:
                memory.append(
                    {
                        "role": "user",
                        "content": "(prior conversation summary) " + summary_text,
                    }
                )
            continue
        if role == "user":
            content = str(turn.get("question") or "").strip()
            if content:
                memory.append({"role": "user", "content": content})
            continue
        if role == "assistant":
            content = str(turn.get("answer_summary") or "").strip()
            if content:
                memory.append({"role": "assistant", "content": content})
    return memory or None


def _ask_worker(
    cfg: AMXConfig,
    job: Job,
    question: str,
    session_id: int | None,
    db_profile: str,
    scope_profiles: list[str],
) -> None:
    """Run the tool-calling agent + stream every reasoning chunk and
    tool result back to the SSE consumer. Persists the assistant
    turn to chat_sessions on success.

    ``scope_profiles`` is the resolved retrieval scope for this turn
    (per-question body override > session sticky > config default).
    Passed through to ``run_tool_agent`` which threads it into every
    catalog tool call.
    """
    job.status = "running"
    emit(job.queue, "activity.added", {"idx": 0, "label": "Thinking"})
    emit(job.queue, "activity.begin", {"idx": 0})

    catalog = _load_catalog()
    if catalog is None:
        job.status = "failed"
        job.error = "Search catalog isn't initialised yet — run /search sync first."
        job.ended_at = time.time()
        emit(job.queue, "activity.fail", {"idx": 0, "detail": job.error})
        emit_terminal(
            job.queue,
            "job.failed",
            {"error": job.error, "hint": "sync-catalog"},
        )
        return

    # Open the LLM provider INSIDE the try-fail path so a misconfigured
    # provider (missing API key, bad model id, network down to the LLM
    # endpoint) results in a clean ``job.failed`` SSE event instead of
    # a thread crash that leaves the SPA hanging on "Reasoning…". The
    # ``hint=configure-llm`` is what the SPA's AskChat reads to show
    # a "Check Settings → LLM" CTA instead of the raw exception text.
    try:
        llm = LLMProvider(cfg.llm)
    except Exception as exc:
        job.status = "failed"
        job.error = (
            f"Couldn't initialise LLM ({cfg.llm.provider or 'unknown'}/"
            f"{cfg.llm.model or 'unknown'}): {exc.__class__.__name__}: {exc}. "
            "Check Settings → LLM (API key, model id, network)."
        )
        job.ended_at = time.time()
        emit(job.queue, "activity.fail", {"idx": 0, "detail": job.error})
        emit_terminal(
            job.queue,
            "job.failed",
            {"error": job.error, "hint": "configure-llm"},
        )
        return

    # ``on_thinking_delta`` from tool_agent forwards CUMULATIVE reasoning
    # text (the CLI display takes a tail of it). The browser builds the
    # panel by appending each event's text, so we must emit only the new
    # suffix here — otherwise the user sees "TheThe userThe user is…".
    last_thinking = ""

    def _on_thinking(text: str) -> None:
        nonlocal last_thinking
        if not text:
            return
        chunk = text[len(last_thinking) :] if text.startswith(last_thinking) else text
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

    # Hydrate the prior-turn context so a resumed chat can resolve
    # follow-up references ("that table", "the second one"). Pulled
    # from chat_sessions/chat_turns; ``submit_ask`` has already
    # appended the current user question, so the loader trims the
    # trailing user row to avoid double-posting.
    session_memory = _load_session_memory(session_id)

    try:
        result = run_tool_agent(
            cfg=cfg,
            catalog=catalog,
            llm=llm,
            question=question,
            answer_language=cfg.llm.language or "english",
            session_memory=session_memory,
            display=None,
            on_thinking_delta=_on_thinking,
            on_tool_call=_on_tool_call,
            cancel_token=job.cancel,
            db_profiles=list(scope_profiles) if scope_profiles else None,
        )
    except RunCancelled:
        job.status = "cancelled"
        job.ended_at = time.time()
        emit(job.queue, "activity.fail", {"idx": 0, "detail": "Cancelled."})
        emit_terminal(job.queue, "job.cancelled", {})
        return
    except Exception as exc:
        job.status = "failed"
        # Best-effort classification: surface a configure-llm hint when
        # the failure smells like an LLM-side problem (auth, network,
        # rate limit, model not found). The SPA uses the hint to show
        # a "Check Settings → LLM" CTA. Anything else stays generic.
        message = str(exc) or exc.__class__.__name__
        lower = message.lower()
        llm_signals = (
            "api key",
            "401",
            "403",
            "unauthorized",
            "authentication",
            "rate limit",
            "rate_limit",
            "model not found",
            "model_not_found",
            "unknown model",
            "connection refused",
            "connect timeout",
            "read timeout",
            "name or service not known",
            "could not resolve",
            "nodename nor servname",
            "litellm",
            "openai",
            "anthropic",
            "openrouter",
        )
        hint = "configure-llm" if any(token in lower for token in llm_signals) else None
        if hint == "configure-llm":
            message = (
                f"LLM call failed: {message}. Check Settings → LLM (API key, model id, network)."
            )
        job.error = message
        job.ended_at = time.time()
        emit(job.queue, "activity.fail", {"idx": 0, "detail": job.error})
        payload: dict[str, Any] = {"error": job.error}
        if hint:
            payload["hint"] = hint
        emit_terminal(job.queue, "job.failed", payload)
        return

    # Persist the assistant's reply (best-effort). Note the keyword
    # argument is ``answer_summary`` (not ``answer``) and ``run_id`` is
    # required — passing ``answer=`` previously raised a TypeError that
    # the bare ``except`` swallowed, so Studio-driven sessions
    # ended up with user-only history.
    if session_id is not None:
        store = _session_store_or_none()
        if store is not None:
            try:
                store.append_assistant_turn(
                    int(session_id),
                    run_id=None,
                    answer_summary=result.answer or "",
                )
            except Exception as exc:
                log.warning(
                    "Failed to persist assistant turn for session %s: %s",
                    session_id,
                    exc,
                )

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
            # Multi-profile observability: which profiles were in
            # scope, which one the system prompt flagged as the
            # auto-detected focus, and where the time went. The
            # SPA's footer renders "answered from 3 profiles in
            # 3.4s · focus: WAREHOUSE" off these.
            "scope_profiles": list(result.scope_profiles or []),
            "focus_profile": result.focus_profile,
            "total_latency_ms": result.total_latency_ms,
            "per_tool_latency_ms": dict(result.per_tool_latency_ms or {}),
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
