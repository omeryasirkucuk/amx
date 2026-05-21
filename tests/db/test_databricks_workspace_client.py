from unittest.mock import MagicMock, patch


def _client(token="tok-1"):
    from amx.db.adapters._databricks_workspace import DatabricksWorkspaceClient

    return DatabricksWorkspaceClient(host="https://example.cloud.databricks.com", token=token)


def test_list_workspace_objects_paginates():
    pages = [
        {
            "objects": [{"object_id": 1, "object_type": "NOTEBOOK", "path": "/a"}],
            "next_page_token": "tk",
        },
        {
            "objects": [{"object_id": 2, "object_type": "DIRECTORY", "path": "/b"}],
            "next_page_token": "",
        },
    ]
    with patch("amx.db.adapters._databricks_workspace.requests.get") as g:
        g.side_effect = [MagicMock(status_code=200, json=lambda p=p: p) for p in pages]
        out = list(_client().list_workspace_objects(path="/"))
    assert [o["path"] for o in out] == ["/a", "/b"]
    assert g.call_count == 2


def test_export_notebook_source_returns_decoded_text():
    import base64

    payload = {
        "content": base64.b64encode(b"# COMMAND ----------\nprint('hi')\n").decode(),
        "file_type": "SOURCE",
    }
    with patch("amx.db.adapters._databricks_workspace.requests.get") as g:
        g.return_value = MagicMock(status_code=200, json=lambda: payload)
        text = _client().export_notebook_source(workspace_path="/x")
    assert "print('hi')" in text


def test_list_jobs_yields_full_job_records_with_runs():
    jobs_resp = {"jobs": [{"job_id": 11, "settings": {"name": "j1"}}], "has_more": False}
    get_resp = {
        "job_id": 11,
        "settings": {
            "name": "j1",
            "tasks": [{"task_key": "t1", "notebook_task": {"notebook_path": "/n"}}],
        },
    }
    runs_resp = {
        "runs": [
            {
                "run_id": 100,
                "state": {"result_state": "SUCCESS"},
                "start_time": 1700000000000,
                "end_time": 1700000010000,
            }
        ],
        "has_more": False,
    }
    with patch("amx.db.adapters._databricks_workspace.requests.get") as g:
        g.side_effect = [
            MagicMock(status_code=200, json=lambda resp=jobs_resp: resp),
            MagicMock(status_code=200, json=lambda resp=get_resp: resp),
            MagicMock(status_code=200, json=lambda resp=runs_resp: resp),
        ]
        c = _client()
        jobs = list(c.list_jobs_full(runs_per_job=20))
    assert len(jobs) == 1
    assert jobs[0]["settings"]["tasks"][0]["task_key"] == "t1"
    assert jobs[0]["recent_runs"][0]["run_id"] == 100


def test_unauthorized_raises_with_helpful_message():
    import pytest

    from amx.db.adapters._databricks_workspace import DatabricksAuthError

    with patch("amx.db.adapters._databricks_workspace.requests.get") as g:
        g.return_value = MagicMock(status_code=401, text="invalid token")
        with pytest.raises(DatabricksAuthError):
            list(_client().list_workspace_objects(path="/"))
