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
