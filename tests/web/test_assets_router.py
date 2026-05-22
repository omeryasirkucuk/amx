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


def test_delete_notebook_removes_row_and_lineage(tmp_path):
    client, db_path = _make_client(tmp_path)
    nb_id = _seed_notebook(db_path)
    # Seed a catalog_entities row + lineage edge so cascade can be observed.
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO catalog_entities
                (db_profile, db_backend, database_name, schema_name, table_name,
                 entity_kind, asset_kind, updated_at)
            VALUES ('prod', 'snowflake', 'raw', 'public', 'orders',
                    'table', 'table', 0.0)
            """,
        )
        ent_id = conn.execute(
            "SELECT id FROM catalog_entities WHERE table_name='orders'"
        ).fetchone()[0]
        conn.execute(
            """
            INSERT INTO catalog_relationships
                (from_entity_id, to_entity_id, relationship_type,
                 score, source, details_json, last_seen,
                 from_entity_kind, to_entity_kind)
            VALUES (?, ?, 'asset_references_table',
                    1.0, 'test', '{}', 0.0,
                    'notebook', 'table')
            """,
            (nb_id, ent_id),
        )
        conn.commit()

    resp = client.delete(f"/api/assets/notebook/{nb_id}", headers=_AUTH)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["deleted"] is True
    assert body["counts"]["primary"] == 1
    assert body["counts"]["lineage_edges"] == 1

    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM remote_notebooks").fetchone()[0] == 0
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM catalog_relationships "
                "WHERE relationship_type = 'asset_references_table' "
                "AND from_entity_kind = 'notebook'"
            ).fetchone()[0]
            == 0
        )


def test_delete_job_cascades_tasks_and_runs(tmp_path):
    client, db_path = _make_client(tmp_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO remote_jobs (profile_name, job_id, name, ingested_at) "
            "VALUES ('prod', 42, 'j', '2026-05-21T00:00:00')"
        )
        job_pk = conn.execute("SELECT id FROM remote_jobs").fetchone()[0]
        conn.execute(
            "INSERT INTO remote_job_tasks (job_id_fk, task_key, task_type, "
            "raw_definition_json) VALUES (?, 't1', 'notebook_task', '{}')",
            (job_pk,),
        )
        conn.execute(
            "INSERT INTO remote_job_runs (job_id_fk, run_id, state_result, start_time) "
            "VALUES (?, 1, 'SUCCESS', '2026-05-21T00:00:00')",
            (job_pk,),
        )
        conn.commit()

    resp = client.delete(f"/api/assets/job/{job_pk}", headers=_AUTH)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["counts"]["children"] == 2  # one task + one run
    assert body["counts"]["primary"] == 1

    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM remote_jobs").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM remote_job_tasks").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM remote_job_runs").fetchone()[0] == 0


def test_delete_missing_asset_404(tmp_path):
    client, _db = _make_client(tmp_path)
    resp = client.delete("/api/assets/notebook/99999", headers=_AUTH)
    assert resp.status_code == 404


def test_delete_unknown_kind_400(tmp_path):
    client, _db = _make_client(tmp_path)
    resp = client.delete("/api/assets/banana/1", headers=_AUTH)
    assert resp.status_code == 400


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


# ── PR-A: /discover endpoint + selection round-trip ─────────────────────────


def _make_meta(*, kind, external_id, name, path="", owner=None):
    """Build an AssetMetadata stand-in for the connector stub."""
    from amx.db.adapters.remote_asset_types import AssetMetadata

    return AssetMetadata(
        kind=kind,
        external_id=external_id,
        name=name,
        path=path,
        owner=owner,
        last_modified=None,
    )


def test_discover_returns_metadata_rows(monkeypatch, tmp_path):
    """GET /api/assets/discover yields cheap identity rows, no content."""
    from amx.cli_support.commands import db_assets_impl as impl_mod

    client, _ = _make_client(tmp_path)

    class StubConnector:
        def list_remote_notebooks_metadata(self):
            return iter(
                [
                    _make_meta(
                        kind="notebook",
                        external_id="ext-1",
                        name="etl",
                        path="/Workspace/team-a/etl",
                        owner="alice",
                    ),
                    _make_meta(
                        kind="notebook",
                        external_id="ext-2",
                        name="etl",
                        path="/Workspace/team-b/etl",
                    ),
                ]
            )

    monkeypatch.setattr(impl_mod, "_open_connector", lambda cfg, profile: StubConnector())
    resp = client.get("/api/assets/discover?profile=prod&kind=notebooks", headers=_AUTH)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert [it["external_id"] for it in body["items"]] == ["ext-1", "ext-2"]
    # Same-name collision is preserved precisely because path stays distinct.
    assert body["items"][0]["path"] != body["items"][1]["path"]
    # No source content leaks through.
    assert "source_text" not in body["items"][0]


def test_discover_rejects_unknown_kind(monkeypatch, tmp_path):
    """Only the five pickable kinds are valid; queries/task_deps stay out."""
    client, _ = _make_client(tmp_path)
    resp = client.get("/api/assets/discover?profile=prod&kind=queries", headers=_AUTH)
    assert resp.status_code == 400
    body = resp.json()
    assert "queries" in body["detail"]


def test_discover_returns_501_when_adapter_lacks_method(monkeypatch, tmp_path):
    """An adapter without the metadata listing surfaces a clean 501.

    Empty-result Snowflake notebooks listing is fine; missing the
    method entirely is the explicit "not implemented for this
    backend" path that the router must report rather than 500.
    """
    from amx.cli_support.commands import db_assets_impl as impl_mod

    client, _ = _make_client(tmp_path)

    class BareConnector:
        pass

    monkeypatch.setattr(impl_mod, "_open_connector", lambda cfg, profile: BareConnector())
    resp = client.get("/api/assets/discover?profile=prod&kind=notebooks", headers=_AUTH)
    assert resp.status_code == 501


def test_ingest_body_honours_selection(monkeypatch, tmp_path):
    """POST /ingest forwards body.selection into IngestRequest verbatim."""
    import amx.web.routers.assets as a_mod

    client, _ = _make_client(tmp_path)
    captured: dict = {}

    async def fake_runner(*, job_id, body, cfg, queue):
        captured["selection"] = body.selection
        await queue.put({"_eof": True})

    monkeypatch.setattr(a_mod, "_run_ingest_job", fake_runner)
    resp = client.post(
        "/api/assets/ingest",
        json={
            "profile": "prod",
            "types": ["notebooks"],
            "selection": {"notebooks": ["ext-1", "ext-3"]},
        },
        headers=_AUTH,
    )
    assert resp.status_code == 202
    assert captured["selection"] == {"notebooks": ["ext-1", "ext-3"]}


# ── Hybrid search + lineage endpoint coverage ──────────────────────────────


def _seed_query_row(db_path, *, profile, external, name, sql_text):
    """Insert a remote_queries row and return its id."""
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO remote_queries
                (profile_name, platform, kind, external_id, name, sql_text,
                 sql_hash, warehouse, user_name, executed_at, duration_ms,
                 ingested_at)
            VALUES (?, 'databricks', 'history', ?, ?, ?, 'h', 'wh', NULL,
                    NULL, NULL, '2026-05-21T00:00:00')
            """,
            (profile, external, name, sql_text),
        )
        conn.commit()
        return conn.execute(
            "SELECT id FROM remote_queries WHERE external_id = ?", (external,)
        ).fetchone()[0]


def test_search_endpoint_requires_kind_and_profile(tmp_path):
    """``/search`` is now tab-scoped: both kind and profile are required."""
    client, _ = _make_client(tmp_path)
    # Missing kind: 422 from FastAPI's parameter validation.
    resp = client.get("/api/assets/search?q=trips&profile=prod", headers=_AUTH)
    assert resp.status_code == 422


def test_search_rejects_unknown_kind(tmp_path):
    client, _ = _make_client(tmp_path)
    resp = client.get("/api/assets/search?q=trips&profile=prod&kind=banana", headers=_AUTH)
    assert resp.status_code == 400


def test_search_keyword_strict_filters_to_keyword_matches(tmp_path):
    """A query containing 'trips' is returned; one that doesn't isn't."""
    client, db_path = _make_client(tmp_path)
    matching = _seed_query_row(
        db_path,
        profile="prod",
        external="q1",
        name="trips_count",
        sql_text="SELECT COUNT(*) FROM trips",
    )
    _seed_query_row(
        db_path,
        profile="prod",
        external="q2",
        name="users_count",
        sql_text="SELECT COUNT(*) FROM _amx_users",
    )
    resp = client.get(
        "/api/assets/search?q=trips&profile=prod&kind=query&mode=keyword_strict",
        headers=_AUTH,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["kind"] == "query"
    ids = [item["remote_id"] for item in body["items"]]
    assert ids == [matching]


def test_lineage_endpoint_returns_outgoing_edges(tmp_path):
    client, db_path = _make_client(tmp_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO remote_notebooks
                (profile_name, platform, external_id, name, workspace_path,
                 qualified_name, language, source_text, source_hash,
                 last_modified_at, last_modified_by, owner, cell_count,
                 ingested_at)
            VALUES ('prod', 'databricks', 'ext-nb', 'loader', '/Workspace/loader',
                    NULL, 'python', '{}', 'h', NULL, NULL, NULL, 1,
                    '2026-05-21T00:00:00')
            """
        )
        nb_id = conn.execute("SELECT id FROM remote_notebooks").fetchone()[0]
        conn.execute(
            """
            INSERT INTO remote_jobs (profile_name, job_id, name, ingested_at)
            VALUES ('prod', 7, 'main_job', '2026-05-21T00:00:00')
            """
        )
        job_id = conn.execute("SELECT id FROM remote_jobs").fetchone()[0]
        conn.execute(
            """
            INSERT INTO remote_job_tasks
                (job_id_fk, task_key, task_type, notebook_path, notebook_id_fk,
                 depends_on_json, raw_definition_json)
            VALUES (?, 'load', 'notebook_task', '/Workspace/loader', ?, '[]', '{}')
            """,
            (job_id, nb_id),
        )
        conn.commit()

    # Trigger extraction directly so we don't need to wire ingest fixtures.
    from amx.assets.lineage import LineageExtractor

    with sqlite3.connect(db_path) as conn:
        LineageExtractor(conn).extract_for_profile("prod")

    resp = client.get(f"/api/assets/job/{job_id}/lineage?profile=prod", headers=_AUTH)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["kind"] == "job"
    assert body["task_dag"] == []
    outgoing = body["outgoing"]
    assert len(outgoing) == 1
    assert outgoing[0]["to_kind"] == "notebook"
    assert outgoing[0]["to_id"] == nb_id
    assert outgoing[0]["edge_type"] == "task_runs_notebook"
    assert outgoing[0]["to_name"] == "loader"


def test_lineage_endpoint_unknown_asset_returns_404(tmp_path):
    client, _ = _make_client(tmp_path)
    resp = client.get("/api/assets/job/99999/lineage?profile=prod", headers=_AUTH)
    assert resp.status_code == 404
