from unittest.mock import MagicMock


def _adapter_with_mock_client():
    from amx.db.adapters.databricks import DatabricksAdapter
    from types import SimpleNamespace
    a = DatabricksAdapter.__new__(DatabricksAdapter)
    a.cfg = SimpleNamespace(host="https://example", access_token="t", workspace_token=None)  # type: ignore[attr-defined]
    a._workspace_client_override = MagicMock()  # type: ignore[attr-defined]
    return a


def test_list_remote_notebooks_yields_one_per_notebook_object():
    a = _adapter_with_mock_client()
    a._workspace_client_override.list_workspace_objects.return_value = iter([
        {"object_id": 1, "object_type": "NOTEBOOK", "path": "/Users/alice/n1", "language": "PYTHON", "modified_at": 1700000000000},
        {"object_id": 2, "object_type": "DIRECTORY", "path": "/Users/alice/sub"},
        {"object_id": 3, "object_type": "NOTEBOOK", "path": "/Users/alice/sub/n2", "language": "SQL", "modified_at": 1700000000000},
    ])
    a._workspace_client_override.export_notebook_source.side_effect = [
        "# Databricks notebook source\n# COMMAND ----------\nprint(1)\n",
        "# Databricks notebook source\n# COMMAND ----------\nSELECT 1\n",
    ]
    nbs = list(a.list_remote_notebooks())
    assert len(nbs) == 2
    assert nbs[0].platform == "databricks"
    assert nbs[0].workspace_path == "/Users/alice/n1"
    assert nbs[0].language == "python"
    assert nbs[0].source_hash and len(nbs[0].source_hash) == 64  # sha256


def test_fetch_remote_notebook_source_normalizes_path():
    a = _adapter_with_mock_client()
    a._workspace_client_override.export_notebook_source.return_value = "# Databricks notebook source\n# COMMAND ----------\nprint('x')\n"
    out = a.fetch_remote_notebook_source(external_id="/Users/alice/n1")
    import json
    parsed = json.loads(out)
    assert parsed["cells"][0]["cell_type"] == "code"


def test_fetch_remote_notebook_source_resolves_object_id():
    a = _adapter_with_mock_client()
    a._workspace_client_override.path_for_object_id.return_value = "/Users/alice/by_id"
    a._workspace_client_override.export_notebook_source.return_value = "# Databricks notebook source\n# COMMAND ----------\nprint('y')\n"
    a.fetch_remote_notebook_source(external_id="42")
    a._workspace_client_override.path_for_object_id.assert_called_once_with("42")


def test_capability_remote_notebooks_flag_on():
    from amx.db.adapters.databricks import DatabricksAdapter
    assert DatabricksAdapter.capabilities.remote_notebooks is True


def test_list_remote_jobs_maps_settings_and_runs():
    a = _adapter_with_mock_client()
    a._workspace_client_override.list_jobs_full.return_value = iter([
        {
            "job_id": 42,
            "creator_user_name": "alice",
            "settings": {
                "name": "nightly_etl",
                "schedule": {"quartz_cron_expression": "0 2 * * *", "timezone_id": "UTC", "pause_status": "UNPAUSED"},
                "max_concurrent_runs": 1,
                "email_notifications": {"on_failure": ["ops@example.com"]},
                "tags": {"team": "data"},
                "tasks": [
                    {"task_key": "extract", "notebook_task": {"notebook_path": "/Users/alice/extract"}, "depends_on": []},
                    {"task_key": "load", "notebook_task": {"notebook_path": "/Users/alice/load"}, "depends_on": [{"task_key": "extract"}]},
                ],
            },
            "recent_runs": [
                {"run_id": 1, "state": {"result_state": "SUCCESS"}, "start_time": 1714521600000, "end_time": 1714521610000, "setup_duration": 100, "execution_duration": 9900},
                {"run_id": 2, "state": {"result_state": "FAILED"}, "start_time": 1714608000000, "end_time": 1714608010000, "setup_duration": 100, "execution_duration": 9900},
            ],
        }
    ])
    jobs = list(a.list_remote_jobs())
    assert len(jobs) == 1
    j = jobs[0]
    assert j.job_id == 42
    assert j.schedule_cron == "0 2 * * *"
    assert j.schedule_pause_status == "UNPAUSED"
    assert len(j.tasks) == 2
    assert j.tasks[1].depends_on == ("extract",)
    assert len(j.recent_runs) == 2


def test_capability_remote_jobs_flag_on():
    from amx.db.adapters.databricks import DatabricksAdapter
    assert DatabricksAdapter.capabilities.remote_jobs is True
