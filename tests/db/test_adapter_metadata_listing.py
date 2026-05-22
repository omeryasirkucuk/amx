"""PR-A: cheap metadata listing + external_id_filter on full listers.

The browse-then-pick wizard depends on two adapter contracts:

1.  ``list_remote_*_metadata`` yields ``AssetMetadata`` rows with
    no content fetch (no notebook export, no per-job ``/get`` +
    ``/runs/list`` fan-out).
2.  ``list_remote_*(external_id_filter=[...])`` restricts the
    full lister so a follow-up cherry-pick of selected ids skips
    the unselected ones entirely.

These tests stub the workspace client so we don't talk to a real
Databricks workspace.
"""

from unittest.mock import MagicMock


def _databricks_adapter():
    from types import SimpleNamespace

    from amx.db.adapters.databricks import DatabricksAdapter

    a = DatabricksAdapter.__new__(DatabricksAdapter)
    a.cfg = SimpleNamespace(  # type: ignore[attr-defined]
        host="https://example", access_token="t", workspace_token=None
    )
    a._workspace_client_override = MagicMock()  # type: ignore[attr-defined]
    return a


def test_notebooks_metadata_skips_export_call():
    """list_remote_notebooks_metadata must not invoke the export endpoint.

    A 5k-notebook workspace doing one export per notebook would
    push the wizard latency from seconds to minutes — that's the
    whole reason the metadata cousin exists.
    """
    a = _databricks_adapter()
    a._workspace_client_override.list_workspace_objects.return_value = iter(
        [
            {
                "object_id": 1,
                "object_type": "NOTEBOOK",
                "path": "/Users/alice/etl",
                "language": "PYTHON",
                "modified_at": 1700000000000,
            },
            {
                "object_id": 2,
                "object_type": "NOTEBOOK",
                "path": "/Users/bob/etl",
                "language": "SQL",
                "modified_at": 1700000000000,
            },
        ]
    )
    rows = list(a.list_remote_notebooks_metadata())
    assert [r.external_id for r in rows] == ["1", "2"]
    # Same-name collision survives because the path differs.
    assert rows[0].name == rows[1].name == "etl"
    assert rows[0].path != rows[1].path
    a._workspace_client_override.export_notebook_source.assert_not_called()


def test_pipelines_metadata_does_not_fan_out_per_pipeline():
    """list_remote_pipelines_metadata uses the list endpoint only."""
    a = _databricks_adapter()
    a._workspace_client_override.list_pipelines_headers = MagicMock(
        return_value=iter(
            [
                {"pipeline_id": "p-1", "name": "bronze", "creator_user_name": "alice"},
                {"pipeline_id": "p-2", "name": "silver", "creator_user_name": "bob"},
            ]
        )
    )
    rows = list(a.list_remote_pipelines_metadata())
    assert [r.external_id for r in rows] == ["p-1", "p-2"]


def test_pipelines_external_id_filter_restricts_iterator():
    """list_remote_pipelines(external_id_filter=[...]) drops unselected ids."""
    a = _databricks_adapter()
    a._workspace_client_override.list_pipelines.return_value = iter(
        [
            {
                "pipeline_id": "p-1",
                "spec": {"name": "bronze"},
                "latest_updates": [],
            },
            {
                "pipeline_id": "p-2",
                "spec": {"name": "silver"},
                "latest_updates": [],
            },
            {
                "pipeline_id": "p-3",
                "spec": {"name": "gold"},
                "latest_updates": [],
            },
        ]
    )
    rows = list(a.list_remote_pipelines(external_id_filter=["p-1", "p-3"]))
    assert [r.pipeline_id for r in rows] == ["p-1", "p-3"]


def test_pipelines_no_filter_yields_all():
    a = _databricks_adapter()
    a._workspace_client_override.list_pipelines.return_value = iter(
        [
            {"pipeline_id": "p-1", "spec": {"name": "bronze"}, "latest_updates": []},
            {"pipeline_id": "p-2", "spec": {"name": "silver"}, "latest_updates": []},
        ]
    )
    rows = list(a.list_remote_pipelines())
    assert len(rows) == 2


def test_notebooks_external_id_filter_skips_export():
    """external_id_filter must also avoid the heavy export call for
    notebooks outside the selected set.
    """
    a = _databricks_adapter()
    a._workspace_client_override.list_workspace_objects.return_value = iter(
        [
            {
                "object_id": 1,
                "object_type": "NOTEBOOK",
                "path": "/keep",
                "language": "PYTHON",
                "modified_at": 1700000000000,
            },
            {
                "object_id": 2,
                "object_type": "NOTEBOOK",
                "path": "/drop",
                "language": "SQL",
                "modified_at": 1700000000000,
            },
        ]
    )
    a._workspace_client_override.export_notebook_source.return_value = (
        "# Databricks notebook source\n# COMMAND ----------\nprint(1)\n"
    )
    rows = list(a.list_remote_notebooks(external_id_filter=["1"]))
    assert len(rows) == 1
    assert rows[0].workspace_path == "/keep"
    # Only the kept notebook should have been exported.
    assert a._workspace_client_override.export_notebook_source.call_count == 1
