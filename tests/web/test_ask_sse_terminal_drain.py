"""Terminal-event guarantees for /api/ask SSE streams.

Historically, when ``_load_catalog()`` raised (e.g. the on-disk
chromadb collection's embedding identity diverged from the active
profile after a model swap), the worker thread died silently. The
job status stayed ``"running"``, no ``job.failed`` ever reached the
queue, the SSE generator kept emitting pings, and the SPA eventually
gave up with "Connection lost. Reconnecting (attempt 1/5)..." — a
useless error state.

Two complementary safeguards now protect the SSE contract:

1. **Targeted handler** around ``_load_catalog()`` emits a clean
   ``job.failed`` with ``hint=rebuild-catalog`` so the SPA can render
   an actionable "Run /search rebuild" toast.
2. **Wrapper-level synth fallback** in ``_ask_worker``: a try/finally
   that synthesizes ``job.failed`` with ``hint=internal-error`` if
   any future code path escapes the explicit handlers. Belt and
   suspenders for the silent-thread-death failure mode.

Both safeguards are covered below.
"""

from __future__ import annotations

import json
import time
from unittest.mock import MagicMock

from amx.web.routers import ask as ask_router


def _drain_sse(client, path: str, headers: dict[str, str]) -> list[dict]:
    events: list[dict] = []
    t0 = time.monotonic()
    with client.stream("GET", path, headers=headers) as response:
        assert response.status_code == 200
        current_event = None
        for line in response.iter_lines():
            if not line:
                continue
            if isinstance(line, bytes):
                line = line.decode("utf-8")
            if line.startswith("event:"):
                current_event = line.split(":", 1)[1].strip()
            elif line.startswith("data:"):
                payload = line.split(":", 1)[1].strip()
                try:
                    parsed = json.loads(payload)
                except json.JSONDecodeError:
                    continue
                parsed["type"] = current_event or parsed.get("type", "")
                events.append(parsed)
                if (current_event or "") in {
                    "job.done",
                    "job.cancelled",
                    "job.failed",
                }:
                    return events
            if time.monotonic() - t0 > 5:
                break
    return events


def test_load_catalog_failure_emits_clean_terminal_with_rebuild_hint(
    client, auth_headers, monkeypatch
) -> None:
    """``_load_catalog()`` raising bubbles up as a clean ``job.failed``
    SSE event with ``hint=rebuild-catalog`` — the SPA receives a
    terminal frame and renders an actionable error instead of hanging
    until the browser drops the stream as "Connection lost"."""

    def _boom() -> None:
        raise RuntimeError(
            "Vector collection was indexed with a different embedding "
            "identity. Run /search rebuild to refresh."
        )

    monkeypatch.setattr(ask_router, "_load_catalog", _boom)
    monkeypatch.setattr(ask_router, "LLMProvider", lambda _cfg: MagicMock())

    response = client.post(
        "/api/ask",
        headers=auth_headers,
        json={"question": "are my syncs up to date"},
    )
    assert response.status_code == 200
    job_id = response.json()["job_id"]

    events = _drain_sse(client, f"/api/ask/{job_id}/events", auth_headers)
    failed = [e for e in events if e["type"] == "job.failed"]
    assert len(failed) == 1, f"Expected one job.failed event, got: {events}"
    payload = failed[0]
    assert payload.get("hint") == "rebuild-catalog"
    assert "catalog" in payload.get("error", "").lower()


def test_ask_worker_synthesizes_terminal_on_silent_death(monkeypatch) -> None:
    """If any path inside ``_ask_worker_impl`` raises without first
    emitting a terminal event, the wrapper's try/finally MUST
    synthesize a ``job.failed`` with ``hint=internal-error`` so the
    SSE consumer is never left hanging."""

    from amx.config import AMXConfig
    from amx.web.jobs import Job

    def _impl_dies(*_args, **_kwargs) -> None:
        raise RuntimeError("simulated silent worker crash")

    monkeypatch.setattr(ask_router, "_ask_worker_impl", _impl_dies)

    job = Job(id="ask-test", kind="ask")
    job.status = "running"
    job.started_at = time.time()

    ask_router._ask_worker(
        AMXConfig(),
        job,
        question="anything",
        session_id=None,
        db_profile="default",
        scope_profiles=["default"],
    )

    assert job.status == "failed"

    drained: list[dict] = []
    while True:
        try:
            drained.append(job.queue.get_nowait())
        except Exception:
            break

    terminal = [e for e in drained if e.get("type") == "job.failed"]
    assert len(terminal) == 1, f"No synth terminal emitted: {drained}"
    assert terminal[0].get("hint") == "internal-error"
    assert "log" in (terminal[0].get("error") or "").lower()
