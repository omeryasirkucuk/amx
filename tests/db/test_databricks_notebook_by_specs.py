"""Path-based single-notebook ingest — no workspace scan."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock


def _databricks_adapter():
    from amx.db.adapters.databricks import DatabricksAdapter

    a = DatabricksAdapter.__new__(DatabricksAdapter)
    a.cfg = SimpleNamespace(  # type: ignore[attr-defined]
        host="https://example", access_token="t", workspace_token=None
    )
    a._workspace_client_override = MagicMock()  # type: ignore[attr-defined]
    return a


def test_by_specs_exports_by_path_without_scanning():
    a = _databricks_adapter()
    a._workspace_client_override.export_notebook_source.return_value = (
        "# Databricks notebook source\nprint('hi')\n"
    )

    rows = list(a.list_remote_notebooks_by_specs([("2257615622929527", "/Users/me/Folder/My NB")]))

    assert len(rows) == 1
    nb = rows[0]
    assert nb.external_id == "2257615622929527"  # the workspace object_id, for reconcile
    assert nb.workspace_path == "/Users/me/Folder/My NB"
    assert nb.name == "My NB"
    assert "hi" in nb.source_text
    assert nb.source_hash  # content hashed

    # The whole point: it exports by path and never triggers a workspace scan.
    a._workspace_client_override.export_notebook_source.assert_called_once_with(
        workspace_path="/Users/me/Folder/My NB"
    )
    a._workspace_client_override.list_workspace_objects.assert_not_called()


def test_by_specs_skips_a_notebook_that_fails_to_export():
    a = _databricks_adapter()
    a._workspace_client_override.export_notebook_source.side_effect = RuntimeError("403")
    rows = list(a.list_remote_notebooks_by_specs([("1", "/A/B")]))
    assert rows == []  # failure is skipped, not raised
