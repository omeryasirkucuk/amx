"""PR alpha — Studio code scan reaches functional parity with the CLI.

Three properties are pinned here:

* ``index_semantic=True`` is passed to ``analyze_codebase`` so the
  ``amx_code`` Chroma collection is populated (fix C1).
* ``save_cached_report`` is called so a follow-up
  ``POST /api/code/analyze`` finds the cached report under the same
  slug the CLI writes (fix C2).
* ``catalog_store.sync_code_report`` is called so the search catalog
  has the code-evidence rows attached (fix I9).
* Per-file SSE progress events are emitted so the SPA renders a
  proper progress bar instead of one "Scanning…" line (fix I8).
"""

from __future__ import annotations

import time
from typing import Any
from unittest.mock import MagicMock


def _wait_for_status(client, job_id: str, target: str, timeout: float = 3.0) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        resp = client.get(
            f"/api/runs/{job_id}",
            headers={"Authorization": "Bearer test-studio-token-abc123"},
        )
        body = resp.json()
        if body["status"] == target:
            return body
        time.sleep(0.02)
    raise AssertionError(f"Job {job_id} never reached status {target}; last={body}")


def _patch_db(monkeypatch) -> None:
    fake_db = MagicMock(
        list_schemas=MagicMock(return_value=["public"]),
        list_tables=MagicMock(return_value=["users"]),
    )
    monkeypatch.setattr("amx.db.connector.DatabaseConnector", lambda cfg: fake_db)


def test_studio_scan_passes_index_semantic_true(client, auth_headers, cfg, monkeypatch) -> None:
    """The Studio scan worker must call ``analyze_codebase`` with
    ``index_semantic=True`` so the Chroma index gets populated. Without
    this, ``/api/code/search`` always returns empty after a Studio scan.
    """
    cfg.code_profiles["repo"] = "/abs/repo"
    cfg.active_code_profile = "repo"
    _patch_db(monkeypatch)

    captured: dict[str, Any] = {}

    def fake_analyze(path, **kw):
        captured.update(kw)
        return MagicMock(
            total_files=2,
            scanned_files=2,
            references={"users": []},
            external_mentions={},
        )

    monkeypatch.setattr("amx.codebase.analyzer.analyze_codebase", fake_analyze)

    resp = client.post("/api/code/index", headers=auth_headers, json={})
    assert resp.status_code == 200
    job_id = resp.json()["job_id"]
    _wait_for_status(client, job_id, "done")

    assert captured.get("index_semantic") is True
    # Per-file progress callback also wired in — the SPA depends on it.
    assert callable(captured.get("progress_callback"))


def test_studio_scan_persists_cached_report(client, auth_headers, cfg, monkeypatch) -> None:
    """``save_cached_report`` must be invoked with the resolved code
    profile name + source path so a follow-up ``/api/code/analyze``
    finds the manifest the CLI wrote."""
    cfg.code_profiles["repo"] = "/abs/repo"
    cfg.active_code_profile = "repo"
    _patch_db(monkeypatch)

    fake_report = MagicMock(
        total_files=1,
        scanned_files=1,
        references={},
        external_mentions={},
    )
    monkeypatch.setattr(
        "amx.codebase.analyzer.analyze_codebase",
        lambda *a, **kw: fake_report,
    )

    save_calls: list[dict[str, Any]] = []

    def _fake_save(**kwargs):
        save_calls.append(kwargs)
        from pathlib import Path

        return Path("/tmp/dummy")

    monkeypatch.setattr("amx.codebase.cache.save_cached_report", _fake_save)

    resp = client.post(
        "/api/code/index",
        headers=auth_headers,
        json={"profile": "repo"},
    )
    assert resp.status_code == 200
    _wait_for_status(client, resp.json()["job_id"], "done")

    assert len(save_calls) == 1
    call = save_calls[0]
    assert call["profile_name"] == "repo"
    assert call["source_path"] == "/abs/repo"
    assert call["report"] is fake_report


def test_studio_scan_syncs_search_catalog(client, auth_headers, cfg, monkeypatch) -> None:
    """``SearchCatalog.sync_code_report`` must be called after the scan
    so /search returns code-evidence rows."""
    cfg.code_profiles["repo"] = "/abs/repo"
    cfg.active_code_profile = "repo"
    _patch_db(monkeypatch)

    fake_report = MagicMock(
        total_files=1,
        scanned_files=1,
        references={},
        external_mentions={},
    )
    monkeypatch.setattr(
        "amx.codebase.analyzer.analyze_codebase",
        lambda *a, **kw: fake_report,
    )
    monkeypatch.setattr(
        "amx.codebase.cache.save_cached_report",
        lambda **kw: None,
    )

    sync_calls: list[dict[str, Any]] = []
    fake_catalog = MagicMock()
    fake_catalog.sync_code_report = MagicMock(
        side_effect=lambda **kw: (sync_calls.append(kw), (0, 0))[1]
    )
    monkeypatch.setattr(
        "amx.search.catalog.SearchCatalog.from_history_store",
        classmethod(lambda cls: fake_catalog),
    )

    resp = client.post(
        "/api/code/index",
        headers=auth_headers,
        json={"profile": "repo"},
    )
    assert resp.status_code == 200
    _wait_for_status(client, resp.json()["job_id"], "done")

    assert len(sync_calls) == 1
    call = sync_calls[0]
    assert call["source_path"] == "/abs/repo"
    assert call["report"] is fake_report


def test_studio_scan_emits_per_file_progress(client, auth_headers, cfg, monkeypatch) -> None:
    """The worker must invoke its per-file callback with each file
    name so the SSE bus carries per-file progress events instead of a
    single "Scanning…" line."""
    cfg.code_profiles["repo"] = "/abs/repo"
    cfg.active_code_profile = "repo"
    _patch_db(monkeypatch)

    callback_holder: dict[str, Any] = {}

    def fake_analyze(path, **kw):
        cb = kw.get("progress_callback")
        callback_holder["cb"] = cb
        # Simulate the analyzer driving the callback the same way the
        # real one does: ``__total__`` once, then ``__advance__`` per file.
        if cb is not None:
            cb("__total__", 3)
            cb("__advance__", "a.py")
            cb("__advance__", "b.py")
            cb("__advance__", "c.py")
        return MagicMock(
            total_files=3,
            scanned_files=3,
            references={},
            external_mentions={},
        )

    monkeypatch.setattr("amx.codebase.analyzer.analyze_codebase", fake_analyze)
    monkeypatch.setattr("amx.codebase.cache.save_cached_report", lambda **kw: None)

    # Capture every SSE event the worker emits.
    events: list[tuple[str, dict[str, Any]]] = []
    real_emit = __import__("amx.web.progress_bus", fromlist=["emit"]).emit  # type: ignore[assignment]

    def _spy_emit(queue, name, payload):
        events.append((name, payload))
        return real_emit(queue, name, payload)

    monkeypatch.setattr("amx.web.routers.code_ops.emit", _spy_emit)

    resp = client.post(
        "/api/code/index",
        headers=auth_headers,
        json={"profile": "repo"},
    )
    assert resp.status_code == 200
    _wait_for_status(client, resp.json()["job_id"], "done")

    progress_events = [p for n, p in events if n == "code.scan.progress"]
    # One ``__total__`` event plus one per file.
    assert len(progress_events) == 4
    per_file = [p for p in progress_events if p["file_path"]]
    assert [p["file_path"] for p in per_file] == ["a.py", "b.py", "c.py"]
    assert per_file[-1]["processed_count"] == 3
    assert per_file[-1]["total_count"] == 3
