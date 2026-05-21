"""Tests for /api/assets list, detail, and ingest (SSE) endpoints."""

from __future__ import annotations

import json
import sqlite3

from amx.config import AMXConfig
from amx.storage.sqlite_store import SQLiteHistoryStore
from amx.web.server import create_app

_TEST_TOKEN = "test-assets-token-xyz"
_AUTH = {"Authorization": f"Bearer {_TEST_TOKEN}"}


def _make_client(tmp_path):
    """Build a TestClient with an AMXConfig whose CONFIG_DIR is tmp_path.

    Initialises the history DB and returns (client, db_path).
    """
    from fastapi.testclient import TestClient

    cfg = AMXConfig()
    cfg.CONFIG_DIR = str(tmp_path)
    db_path = tmp_path / "history.db"
    SQLiteHistoryStore(db_path).init()

    app = create_app(cfg, token=_TEST_TOKEN)
    return TestClient(app), db_path


def _seed_notebook(db_path, profile="prod"):
    """Insert one remote_notebooks row and return its id."""
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO remote_notebooks
                (profile_name, platform, external_id, name, workspace_path,
                 qualified_name, language, source_text, source_hash,
                 last_modified_at, last_modified_by, owner, cell_count, ingested_at)
            VALUES (?, 'databricks', 'ext-1', 'my_nb', '/n', NULL,
                    'python', '{}', 'h', NULL, NULL, NULL, 1,
                    '2026-05-21T00:00:00')
            """,
            (profile,),
        )
        conn.commit()
        return conn.execute("SELECT id FROM remote_notebooks").fetchone()[0]


# ── Task 38: list + detail ──────────────────────────────────────────────────


def test_list_assets_notebooks(tmp_path):
    client, db_path = _make_client(tmp_path)
    _seed_notebook(db_path)
    resp = client.get("/api/assets?profile=prod&type=notebook", headers=_AUTH)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["count"] == 1
    assert body["items"][0]["name"] == "my_nb"


def test_list_assets_empty_for_unknown_profile(tmp_path):
    client, db_path = _make_client(tmp_path)
    _seed_notebook(db_path, profile="prod")
    resp = client.get("/api/assets?profile=nonexistent&type=notebook", headers=_AUTH)
    assert resp.status_code == 200, resp.text
    assert resp.json()["count"] == 0


def test_list_assets_unknown_type_returns_400(tmp_path):
    client, _db = _make_client(tmp_path)
    resp = client.get("/api/assets?profile=prod&type=banana", headers=_AUTH)
    assert resp.status_code == 400


def test_get_asset_detail_returns_source(tmp_path):
    client, db_path = _make_client(tmp_path)
    nb_id = _seed_notebook(db_path)
    resp = client.get(f"/api/assets/notebook/{nb_id}", headers=_AUTH)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["name"] == "my_nb"
    assert "source_text" in body
    assert "downstream_tables" in body
    assert isinstance(body["downstream_tables"], list)


def test_get_asset_detail_404(tmp_path):
    client, db_path = _make_client(tmp_path)
    _seed_notebook(db_path)
    resp = client.get("/api/assets/notebook/99999", headers=_AUTH)
    assert resp.status_code == 404


def test_get_asset_detail_unknown_kind_400(tmp_path):
    client, _db = _make_client(tmp_path)
    resp = client.get("/api/assets/banana/1", headers=_AUTH)
    assert resp.status_code == 400


# ── Task 39: SSE ingest coverage ────────────────────────────────────────────


def test_ingest_endpoint_returns_job_id(monkeypatch, tmp_path):
    """POST /api/assets/ingest returns a job_id and queues background work."""
    import amx.web.routers.assets as a_mod

    client, _ = _make_client(tmp_path)

    async def fake_runner(*, job_id, body, cfg, queue):
        await queue.put(
            {"state": "completed", "counts": {"notebooks": 0, "lineage": 0}, "failures": {}}
        )
        await queue.put({"_eof": True})

    monkeypatch.setattr(a_mod, "_run_ingest_job", fake_runner)
    resp = client.post(
        "/api/assets/ingest",
        json={"profile": "prod", "types": ["notebooks"], "history_days": 7, "runs_per_job": 20},
        headers=_AUTH,
    )
    assert resp.status_code == 202, resp.text
    assert "job_id" in resp.json()


def test_ingest_sse_stream_emits_completion_event(monkeypatch, tmp_path):
    import amx.web.routers.assets as a_mod

    client, _ = _make_client(tmp_path)

    async def fake_runner(*, job_id, body, cfg, queue):
        await queue.put({"state": "completed", "counts": {"notebooks": 1}, "failures": {}})
        await queue.put({"_eof": True})

    monkeypatch.setattr(a_mod, "_run_ingest_job", fake_runner)
    job_id = client.post(
        "/api/assets/ingest",
        json={"profile": "prod", "types": ["notebooks"], "history_days": 7, "runs_per_job": 20},
        headers=_AUTH,
    ).json()["job_id"]

    # Consume the SSE stream; TestClient collects the body once the generator ends.
    with client.stream("GET", f"/api/assets/ingest/{job_id}/events", headers=_AUTH) as r:
        chunks = list(r.iter_text())
    text = "".join(chunks)
    assert "completed" in text
    assert "notebooks" in text


def test_unknown_ingest_job_id_404(tmp_path):
    client, _ = _make_client(tmp_path)
    resp = client.get("/api/assets/ingest/does-not-exist/events", headers=_AUTH)
    assert resp.status_code == 404


def test_refresh_endpoint_clears_and_returns_job_id(monkeypatch, tmp_path):
    """POST /api/assets/refresh deletes existing rows and returns a job_id."""
    import amx.web.routers.assets as a_mod

    client, db_path = _make_client(tmp_path)
    _seed_notebook(db_path)

    async def fake_runner(*, job_id, body, cfg, queue):
        await queue.put({"state": "completed", "counts": {"notebooks": 0}, "failures": {}})
        await queue.put({"_eof": True})

    monkeypatch.setattr(a_mod, "_run_ingest_job", fake_runner)
    resp = client.post(
        "/api/assets/refresh",
        json={"profile": "prod", "types": ["notebooks"], "history_days": 7, "runs_per_job": 20},
        headers=_AUTH,
    )
    assert resp.status_code == 202, resp.text
    assert "job_id" in resp.json()
    # The notebook row should have been deleted before the ingest kicked off.
    with sqlite3.connect(db_path) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM remote_notebooks WHERE profile_name = 'prod'"
        ).fetchone()[0]
    assert count == 0


# ── Job/Pipeline/Streamlit detail enrichment ────────────────────────────────


def test_get_job_includes_tasks_and_runs(tmp_path):
    """Job detail surfaces the task DAG + recent run timeline.

    The route used to return only the top-level remote_jobs row, which
    rendered as a generic key/value dump in Studio. This test seeds the
    three sibling tables and asserts the response includes structured
    ``tasks`` + ``recent_runs`` arrays with decoded JSON fields.
    """
    client, db_path = _make_client(tmp_path)
    with sqlite3.connect(db_path) as conn:
        # Seed a notebook so notebook_id_fk → notebook_name resolves.
        conn.execute(
            """
            INSERT INTO remote_notebooks
                (profile_name, platform, external_id, name, workspace_path,
                 qualified_name, language, source_text, source_hash,
                 last_modified_at, last_modified_by, owner, cell_count, ingested_at)
            VALUES ('prod', 'databricks', 'ext-extract', 'extract_nb',
                    '/Users/alice/extract', NULL, 'python', '{}', 'h',
                    NULL, NULL, NULL, 1, '2026-05-21T00:00:00')
            """,
        )
        nb_id = conn.execute("SELECT id FROM remote_notebooks").fetchone()[0]
        conn.execute(
            """
            INSERT INTO remote_jobs
                (profile_name, job_id, name, schedule_cron, schedule_pause_status,
                 success_rate_30d, ingested_at)
            VALUES ('prod', 42, 'nightly_etl', '0 2 * * *', 'UNPAUSED', 0.95,
                    '2026-05-21T00:00:00')
            """,
        )
        job_pk = conn.execute("SELECT id FROM remote_jobs").fetchone()[0]
        conn.execute(
            """
            INSERT INTO remote_job_tasks
                (job_id_fk, task_key, task_type, notebook_path, notebook_id_fk,
                 depends_on_json, raw_definition_json)
            VALUES (?, 'extract', 'notebook_task', '/Users/alice/extract', ?,
                    '[]', '{}')
            """,
            (job_pk, nb_id),
        )
        conn.execute(
            """
            INSERT INTO remote_job_tasks
                (job_id_fk, task_key, task_type, notebook_path, depends_on_json,
                 raw_definition_json)
            VALUES (?, 'load', 'notebook_task', '/Users/alice/load',
                    '["extract"]', '{}')
            """,
            (job_pk,),
        )
        for run_id, start, state in (
            (1, "2026-05-21T08:00:00", "SUCCESS"),
            (2, "2026-05-20T08:00:00", "FAILED"),
            (3, "2026-05-19T08:00:00", "SUCCESS"),
        ):
            conn.execute(
                """
                INSERT INTO remote_job_runs
                    (job_id_fk, run_id, state_result, start_time, end_time,
                     setup_duration_ms, execution_duration_ms)
                VALUES (?, ?, ?, ?, ?, 100, 9000)
                """,
                (job_pk, run_id, state, start, start),
            )
        conn.commit()

    resp = client.get(f"/api/assets/job/{job_pk}", headers=_AUTH)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["name"] == "nightly_etl"
    assert body["success_rate_30d"] == 0.95
    # Tasks are ordered by task_key and carry decoded depends_on + notebook_name.
    tasks = body["tasks"]
    assert [t["task_key"] for t in tasks] == ["extract", "load"]
    assert tasks[0]["notebook_name"] == "extract_nb"
    assert tasks[0]["depends_on"] == []
    assert tasks[1]["depends_on"] == ["extract"]
    # Recent runs are most-recent first with summed duration_ms.
    runs = body["recent_runs"]
    assert [r["run_id"] for r in runs] == [1, 2, 3]
    assert runs[0]["state_result"] == "SUCCESS"
    assert runs[0]["duration_ms"] == 9100


def test_get_pipeline_decodes_libraries(tmp_path):
    client, db_path = _make_client(tmp_path)
    libraries = [
        {"notebook": {"path": "/Users/alice/dlt_main"}},
        {"file": {"path": "/Volumes/util.py"}},
    ]
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO remote_pipelines
                (profile_name, pipeline_id, name, target_schema, edition,
                 continuous, photon, libraries_json, latest_update_state,
                 latest_update_creation_time, ingested_at)
            VALUES ('prod', 'p-1', 'kpi_pipeline', 'analytics', 'ADVANCED',
                    0, 1, ?, 'COMPLETED', '2026-05-21T08:00:00',
                    '2026-05-21T09:00:00')
            """,
            (json.dumps(libraries),),
        )
        pl_id = conn.execute("SELECT id FROM remote_pipelines").fetchone()[0]
        conn.commit()

    resp = client.get(f"/api/assets/pipeline/{pl_id}", headers=_AUTH)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["libraries"] == libraries
    assert body["latest_update"] == {
        "state": "COMPLETED",
        "created_at": "2026-05-21T08:00:00",
    }


def test_get_streamlit_surfaces_launch_info(tmp_path):
    client, db_path = _make_client(tmp_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO remote_streamlit_apps
                (profile_name, qualified_name, main_file, query_warehouse,
                 root_location, owner, last_altered_at, ingested_at)
            VALUES ('prod', 'ANALYTICS.APPS.DASH_KPIS', 'streamlit_app.py',
                    'WH_S', '@APPS.DASH_STAGE/dash_kpis', 'DATA_ENG',
                    '2026-05-01T00:00:00', '2026-05-21T00:00:00')
            """,
        )
        app_id = conn.execute("SELECT id FROM remote_streamlit_apps").fetchone()[0]
        conn.commit()

    resp = client.get(f"/api/assets/streamlit/{app_id}", headers=_AUTH)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["launch_info"] == {
        "main_file": "streamlit_app.py",
        "root_location": "@APPS.DASH_STAGE/dash_kpis",
        "query_warehouse": "WH_S",
    }
