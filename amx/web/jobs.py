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
from queue import Queue
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


class BufferedQueue(Queue):
    """``queue.Queue`` that also retains a bounded replay buffer.

    Every ``put_nowait`` (the path used by :func:`amx.web.progress_bus.emit`)
    appends a copy of the event into ``buffer`` so a reconnecting SSE
    consumer can re-hydrate the in-flight panel by replaying recent
    events first, then resuming live drain. The buffer's deque already
    enforces ``maxlen``; a separate lock guards both ``buffer`` and
    ``buffer_seq`` so concurrent producers / consumers see consistent
    snapshots.

    Reads (``get``, ``get_nowait``, ``empty``) inherit unchanged: the
    SSE generator continues to consume new events through the queue;
    the buffer is *additive* and lossless for the live consumer.
    """

    def __init__(self, maxsize: int = 0, *, buffer_max: int = EVENT_BUFFER_MAX) -> None:
        super().__init__(maxsize=maxsize)
        self.buffer: deque[dict[str, Any]] = deque(maxlen=buffer_max)
        self.buffer_seq: int = 0
        self.buffer_lock = threading.Lock()

    def put_nowait(self, item: Any) -> None:  # type: ignore[override]
        super().put_nowait(item)
        if isinstance(item, dict):
            with self.buffer_lock:
                self.buffer.append(item)
                self.buffer_seq += 1

    def buffer_snapshot(self) -> list[dict[str, Any]]:
        """Return a stable copy of the current replay buffer."""
        with self.buffer_lock:
            return list(self.buffer)


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
