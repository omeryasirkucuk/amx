from datetime import datetime

import pytest


def test_remote_notebook_roundtrip():
    from amx.db.adapters.remote_asset_types import RemoteNotebook

    nb = RemoteNotebook(
        external_id="123",
        name="weekly_etl",
        platform="databricks",
        language="python",
        workspace_path="/Users/alice/weekly_etl",
        qualified_name=None,
        source_text="{}",
        source_hash="deadbeef",
        last_modified_at=datetime(2026, 5, 1),
        last_modified_by="alice@example.com",
        owner=None,
        cell_count=12,
    )
    assert nb.platform == "databricks"
    assert nb.cell_count == 12
    with pytest.raises(Exception):
        nb.platform = "snowflake"  # frozen


def test_remote_job_with_tasks_and_runs():
    from amx.db.adapters.remote_asset_types import RemoteJob, RemoteJobRun, RemoteJobTask

    task = RemoteJobTask(
        task_key="extract",
        task_type="notebook_task",
        notebook_path="/Users/alice/extract",
        depends_on=[],
        raw_definition={"task_key": "extract"},
        sql_query_id=None,
        sql_warehouse_id=None,
        pipeline_id=None,
    )
    run = RemoteJobRun(
        run_id=99,
        state_result="SUCCESS",
        start_time=datetime(2026, 5, 1),
        end_time=datetime(2026, 5, 1, 0, 5),
        setup_duration_ms=1200,
        execution_duration_ms=12000,
    )
    job = RemoteJob(
        job_id=42,
        name="nightly",
        creator_user_name="alice",
        schedule_cron="0 2 * * *",
        schedule_timezone="UTC",
        schedule_pause_status="UNPAUSED",
        max_concurrent_runs=1,
        email_notifications={"on_failure": ["ops@example.com"]},
        tags={"team": "data"},
        tasks=(task,),
        recent_runs=(run,),
    )
    assert job.success_rate(window_days=30) == 1.0


def test_remote_query_kinds():
    from amx.db.adapters.remote_asset_types import RemoteQuery

    saved = RemoteQuery(
        platform="databricks",
        kind="saved",
        external_id="abc",
        name="daily_kpis",
        sql_text="select 1",
        sql_hash="x",
        warehouse="wh1",
        user_name="alice",
        executed_at=None,
        duration_ms=None,
    )
    history = RemoteQuery(
        platform="snowflake",
        kind="history",
        external_id="01abc",
        name=None,
        sql_text="select 2",
        sql_hash="y",
        warehouse=None,
        user_name="bob",
        executed_at=datetime(2026, 5, 1),
        duration_ms=2400,
    )
    assert saved.kind == "saved" and history.kind == "history"
