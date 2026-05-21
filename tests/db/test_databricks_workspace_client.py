from unittest.mock import MagicMock, patch


def _client(token="tok-1"):
    from amx.db.adapters._databricks_workspace import DatabricksWorkspaceClient

    return DatabricksWorkspaceClient(host="https://example.cloud.databricks.com", token=token)


def test_client_prepends_https_when_host_has_no_scheme():
    """AMX stores Databricks hosts bare (no scheme) so the SQL connector
    can consume them; the REST client must add ``https://`` itself.
    """
    from amx.db.adapters._databricks_workspace import DatabricksWorkspaceClient

    client = DatabricksWorkspaceClient(host="dbc-1234.cloud.databricks.com", token="x")
    assert client.host == "https://dbc-1234.cloud.databricks.com"


def test_client_preserves_explicit_scheme():
    from amx.db.adapters._databricks_workspace import DatabricksWorkspaceClient

    client = DatabricksWorkspaceClient(host="https://dbc-1234.cloud.databricks.com/", token="x")
    assert client.host == "https://dbc-1234.cloud.databricks.com"


def test_client_preserves_http_scheme_for_localhost_dev():
    from amx.db.adapters._databricks_workspace import DatabricksWorkspaceClient

    client = DatabricksWorkspaceClient(host="http://localhost:8080", token="x")
    assert client.host == "http://localhost:8080"


def test_list_workspace_objects_paginates():
    """Pagination yields all NOTEBOOK rows within a single directory."""
    pages = [
        {
            "objects": [{"object_id": 1, "object_type": "NOTEBOOK", "path": "/a"}],
            "next_page_token": "tk",
        },
        {
            "objects": [{"object_id": 2, "object_type": "NOTEBOOK", "path": "/b"}],
            "next_page_token": "",
        },
    ]
    with patch("amx.db.adapters._databricks_workspace.requests.get") as g:
        g.side_effect = [MagicMock(status_code=200, json=lambda p=p: p) for p in pages]
        out = list(_client().list_workspace_objects(path="/single_dir"))
    assert [o["path"] for o in out] == ["/a", "/b"]
    assert g.call_count == 2


def test_list_workspace_objects_recurses_into_directories():
    """Workspace API only returns immediate children. A NOTEBOOK living
    under ``/Users/alice/`` must be reachable by recursion from ``/``."""
    pages = {
        "/": {
            "objects": [
                {"object_id": 1, "object_type": "DIRECTORY", "path": "/Users"},
                {"object_id": 2, "object_type": "DIRECTORY", "path": "/Shared"},
            ],
        },
        "/Shared": {},  # empty dir
        "/Users": {
            "objects": [
                {"object_id": 3, "object_type": "DIRECTORY", "path": "/Users/alice"},
            ],
        },
        "/Users/alice": {
            "objects": [
                {"object_id": 4, "object_type": "NOTEBOOK", "path": "/Users/alice/nb1"},
                {"object_id": 5, "object_type": "NOTEBOOK", "path": "/Users/alice/nb2"},
            ],
        },
    }

    def fake_get(url, **kwargs):
        # The client passes ``params={"path": "..."}`` so requests builds the
        # querystring on the URL it sends. We pull the path out for routing.
        params = kwargs.get("params") or {}
        path = params.get("path", "/")
        return MagicMock(status_code=200, json=lambda p=path: pages.get(p, {}))

    with patch("amx.db.adapters._databricks_workspace.requests.get", side_effect=fake_get):
        out = list(_client().list_workspace_objects(path="/"))

    notebook_paths = [o["path"] for o in out if o["object_type"] == "NOTEBOOK"]
    assert sorted(notebook_paths) == ["/Users/alice/nb1", "/Users/alice/nb2"]
    # Make sure the walker also yielded the intermediate directories so
    # debuggers / Studio can observe the full tree shape if they care.
    dir_paths = [o["path"] for o in out if o["object_type"] == "DIRECTORY"]
    assert "/Users" in dir_paths and "/Users/alice" in dir_paths


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
