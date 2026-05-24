"""PR-E: per-adapter ``list_workspace_children`` returns immediate tree level."""

from __future__ import annotations

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


def test_databricks_yields_dir_repo_and_notebook_skips_file():
    a = _databricks_adapter()
    a._workspace_client_override.list_workspace_objects_immediate.return_value = iter(
        [
            {
                "object_id": 1,
                "object_type": "DIRECTORY",
                "path": "/Users/alice/folder",
            },
            {
                "object_id": 2,
                "object_type": "NOTEBOOK",
                "path": "/Users/alice/nb1",
                "modified_at": 1700000000000,
                "creator_user_name": "alice@x.com",
            },
            {
                "object_id": 3,
                "object_type": "FILE",
                "path": "/Users/alice/readme.txt",
            },
            {
                "object_id": 4,
                "object_type": "REPO",
                "path": "/Repos/alice/my-repo",
                "creator_user_name": "alice@x.com",
            },
        ]
    )
    rows = list(a.list_workspace_children(parent_path="/Users/alice", kind="notebook"))
    assert len(rows) == 3
    folder, notebook, repo = rows
    assert folder.is_directory and folder.external_id is None
    assert folder.path == "/Users/alice/folder"
    assert folder.name == "folder"
    assert not notebook.is_directory
    assert notebook.external_id == "2"
    assert notebook.owner == "alice@x.com"
    assert repo.is_directory and repo.external_id is None
    assert repo.path == "/Repos/alice/my-repo"
    assert repo.name == "my-repo"
    assert repo.owner == "alice@x.com"


def test_databricks_repo_drillable_lists_inner_notebook():
    """A REPO node behaves like a folder: recursing into it via parent_path
    surfaces the notebooks inside the Git folder."""
    a = _databricks_adapter()
    a._workspace_client_override.list_workspace_objects_immediate.return_value = iter(
        [
            {
                "object_id": 7,
                "object_type": "NOTEBOOK",
                "path": "/Repos/alice/my-repo/etl",
                "modified_at": 1700000000000,
                "creator_user_name": "alice@x.com",
            },
        ]
    )
    rows = list(a.list_workspace_children(parent_path="/Repos/alice/my-repo", kind="notebook"))
    assert len(rows) == 1
    nb = rows[0]
    assert nb.path == "/Repos/alice/my-repo/etl"
    assert not nb.is_directory
    assert nb.external_id == "7"


def test_databricks_unknown_kind_yields_nothing():
    a = _databricks_adapter()
    a._workspace_client_override.list_workspace_objects_immediate.return_value = iter(
        [{"object_id": 1, "object_type": "NOTEBOOK", "path": "/x"}]
    )
    assert list(a.list_workspace_children(parent_path="/", kind="job")) == []


def test_snowflake_root_returns_flat_leaves():
    """Snowflake has no notebook folder hierarchy — root yields leaves."""
    from amx.db.adapters.snowflake import SnowflakeAdapter

    a = SnowflakeAdapter.__new__(SnowflakeAdapter)

    class _Meta:
        def __init__(self):
            self.kind = "notebook"
            self.path = "DB.SCHEMA.NB1"
            self.name = "NB1"
            self.external_id = "DB.SCHEMA.NB1"
            self.owner = "OWNER"
            self.last_modified = None

    def fake_metadata(engine):
        del engine
        yield _Meta()

    a.list_remote_notebooks_metadata = fake_metadata  # type: ignore[method-assign]
    rows = list(a.list_workspace_children(engine=None, parent_path="", kind="notebook"))
    assert len(rows) == 1
    assert rows[0].path == "DB.SCHEMA.NB1"
    assert rows[0].is_directory is False


def test_snowflake_subfolder_returns_empty():
    from amx.db.adapters.snowflake import SnowflakeAdapter

    a = SnowflakeAdapter.__new__(SnowflakeAdapter)
    rows = list(a.list_workspace_children(engine=None, parent_path="DB.SCHEMA", kind="notebook"))
    assert rows == []
