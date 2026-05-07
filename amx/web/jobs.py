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
from dataclasses import dataclass, field
from queue import Queue
from typing import Any, Literal

JobKind = Literal["run", "apply", "ask"]
JobStatus = Literal["queued", "running", "cancelled", "done", "failed"]


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
    queue: Queue = field(default_factory=Queue)
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
