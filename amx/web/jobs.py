"""In-process registry of long-running AMX Studio jobs.

Three job kinds are submitted from the UI:

* ``run`` — calls :class:`amx.agents.orchestrator.Orchestrator` to profile
  + LLM-suggest + write pending review for one or more tables.
* ``apply`` — calls :func:`amx.agents.orchestrator.apply_review_results_to_db`
  for the pending queue or an explicit result list.
* ``ask`` — runs :class:`amx.search.service.SearchService` and streams
  thinking deltas.

Every job carries:

* a ``cancel`` :class:`threading.Event` plumbed into the orchestrator /
  search agent so the user's "Cancel" button can stop work between phase
  boundaries (LLM calls themselves can't be killed mid-flight; cancellation
  latency is one tool / agent step);
* a ``queue.Queue`` that :mod:`amx.web.progress_bus` fans out to the SSE
  consumer for the matching ``GET /events`` endpoint.

This module deliberately stays free of FastAPI imports — the registry is
a plain in-memory data structure that the routers wrap.
"""

from __future__ import annotations

import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Literal

JobKind = Literal["run", "apply", "ask", "rerun"]
JobStatus = Literal["queued", "running", "cancelled", "done", "failed"]

#: Cap on the per-job event replay buffer. The buffer keeps the most
#: recent ``EVENT_BUFFER_MAX`` events so a refreshed browser tab can
#: re-hydrate the "Live progress" panel instead of starting from a
#: blank slate. Sized to comfortably cover a multi-table run with a
#: chatty agent emitting per-batch ``step.*`` events; on overflow the
#: oldest events are dropped (deque maxlen) — the user still sees the
#: rolling tail of recent activity, just not the very first events.
EVENT_BUFFER_MAX = 4000


class BufferedQueue:
    """Bounded, multi-consumer event stream backing every SSE channel.

    Every ``put_nowait`` (the path used by :func:`amx.web.progress_bus.emit`)
    appends a copy of the event into a bounded ``deque`` and bumps a
    monotonic ``buffer_seq`` counter. SSE consumers read via the
    cursor-based :meth:`tail_from` method which lets two or more
    concurrent EventSource reconnects each track their own position
    without racing — historically the legacy ``queue.Queue.get`` path
    let the first consumer to wake steal an event the second consumer
    needed, which is what the user saw as "garbled thinking" text:
    the SPA only rendered the half of the chunks delivered to its
    *current* EventSource while the other half went to the orphaned
    pre-disconnect generator.

    Browsers populate the ``Last-Event-ID`` header on auto-reconnect
    when the prior stream's events carried an ``id:`` field. The
    cursor-based API in this class is what makes that header useful:
    the generator initialises its cursor from the header value, the
    buffer replays the missing events with their original sequence
    numbers, and only after the cursor catches up does the generator
    wait on the condition for new emits.
    """

    def __init__(self, *, buffer_max: int = EVENT_BUFFER_MAX) -> None:
        self.buffer: deque[dict[str, Any]] = deque(maxlen=buffer_max)
        self.buffer_seq: int = 0
        self._condition = threading.Condition()

    def put_nowait(self, item: Any) -> None:
        """Producer hook. Non-dict items are silently ignored — the
        legacy ``queue.Queue`` parent accepted arbitrary objects but
        the SSE channel only ever carries event dicts."""
        if not isinstance(item, dict):
            return
        with self._condition:
            self.buffer.append(item)
            self.buffer_seq += 1
            self._condition.notify_all()

    def buffer_snapshot(self) -> list[dict[str, Any]]:
        """Return a stable copy of the current replay buffer."""
        with self._condition:
            return list(self.buffer)

    def tail_from(
        self,
        seq: int,
        timeout: float,
    ) -> list[tuple[int, dict[str, Any]]] | None:
        """Cursor read: events with sequence number > *seq*.

        Returns ``[(seq, event), ...]`` when one or more events with a
        sequence > *seq* are present, otherwise blocks on the internal
        condition for up to *timeout* seconds. Returns ``None`` on
        timeout with no new events so the caller can decide whether
        to emit a keepalive ping.

        The buffer is bounded (``maxlen``) so older events may have
        rolled off. When the cursor lags behind the oldest buffered
        event, the caller silently receives the OLDEST surviving
        events — preferable to a strict "out of range" failure for
        a long-running job whose reconnecting browser was offline
        long enough to lose the prefix. Returning even a partial tail
        keeps the rendered stream coherent for everything still in
        scope.
        """
        with self._condition:

            def _collect() -> list[tuple[int, dict[str, Any]]]:
                if self.buffer_seq <= seq or not self.buffer:
                    return []
                first_buffered = self.buffer_seq - len(self.buffer) + 1
                start_seq = max(seq + 1, first_buffered)
                start_idx = start_seq - first_buffered
                return [
                    (first_buffered + i, self.buffer[i]) for i in range(start_idx, len(self.buffer))
                ]

            ready = _collect()
            if ready:
                return ready
            notified = self._condition.wait(timeout=timeout)
            if not notified:
                return None
            ready = _collect()
            return ready or None


@dataclass
class Job:
    """One submitted unit of work AMX Studio is tracking.

    Created by :class:`JobRegistry.submit`. Mutated by the worker thread
    (status, ended_at, summary, error) and by the cancel endpoint
    (``cancel.set()``). Everything else is read-only after construction.
    """

    id: str
    kind: JobKind
    status: JobStatus = "queued"
    started_at: float = field(default_factory=time.time)
    ended_at: float | None = None
    summary: dict[str, Any] = field(default_factory=dict)
    error: str | None = None
    cancel: threading.Event = field(default_factory=threading.Event)
    queue: BufferedQueue = field(default_factory=BufferedQueue)
    # Set by the run worker once the orchestrator persists the run row,
    # so a Studio user navigating to ``/runs/{numeric_run_id}`` while
    # the worker is still running can find the live job and subscribe
    # to its SSE stream. ``None`` for non-run kinds (apply / ask).
    run_id: int | None = None

    def to_public_dict(self) -> dict[str, Any]:
        """Serializable shape for the ``GET /api/runs/{id}`` endpoint."""
        return {
            "id": self.id,
            "kind": self.kind,
            "status": self.status,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "elapsed_sec": (self.ended_at or time.time()) - self.started_at,
            "summary": dict(self.summary),
            "error": self.error,
            "run_id": self.run_id,
        }


class JobRegistry:
    """Thread-safe in-memory store of submitted jobs.

    Jobs are kept until the parent CLI process exits — there's no
    persistent on-disk job log on purpose. Any audit data the UI cares
    about (run history, pending review state) lives in the existing
    SQLite history store + ``~/.amx/pending_metadata.json``; this
    registry is purely the live "is this job still running?" view.

    PR-A only exposes :meth:`new_job` / :meth:`get` / :meth:`list`.
    Action endpoints in PR-C and PR-D will add :meth:`submit_run`,
    :meth:`submit_apply`, :meth:`submit_ask` that wrap a worker thread
    around the orchestrator / search agent and emit progress events
    through :mod:`amx.web.progress_bus`.
    """

    def __init__(self) -> None:
        self._jobs: dict[str, Job] = {}
        self._lock = threading.Lock()

    def new_job(self, kind: JobKind) -> Job:
        """Mint a fresh job, register it, and return it.

        The job starts in ``queued`` status; the worker bumps it to
        ``running`` right before the first phase.
        """
        job = Job(id=uuid.uuid4().hex, kind=kind)
        with self._lock:
            self._jobs[job.id] = job
        return job

    def get(self, job_id: str) -> Job | None:
        with self._lock:
            return self._jobs.get(job_id)

    def list(self) -> list[Job]:
        with self._lock:
            return list(self._jobs.values())

    def cancel(self, job_id: str) -> bool:
        """Signal the worker to stop after the next phase boundary.

        Returns ``False`` when the job doesn't exist or already
        terminated. The cancel signal is best-effort: orchestrator /
        agent code only checks it between phases (not inside an LLM
        HTTP call), so the actual stop happens within one tool /
        column-batch step.
        """
        job = self.get(job_id)
        if job is None:
            return False
        if job.status not in ("queued", "running"):
            return False
        job.cancel.set()
        return True
