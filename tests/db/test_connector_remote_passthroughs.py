from unittest.mock import MagicMock


def _connector_with_mock_adapter():
    from amx.db.connector import DatabaseConnector

    conn = DatabaseConnector.__new__(DatabaseConnector)
    conn._adapter = MagicMock()  # type: ignore[attr-defined]
    return conn


def test_connector_list_remote_notebooks_delegates():
    conn = _connector_with_mock_adapter()
    conn._adapter.list_remote_notebooks.return_value = iter([])
    list(conn.list_remote_notebooks())
    conn._adapter.list_remote_notebooks.assert_called_once()


def test_connector_fetch_remote_notebook_source_delegates():
    conn = _connector_with_mock_adapter()
    conn._adapter.fetch_remote_notebook_source.return_value = "{}"
    assert conn.fetch_remote_notebook_source("nb-1") == "{}"
    conn._adapter.fetch_remote_notebook_source.assert_called_once_with("nb-1")


def test_connector_list_remote_queries_passes_kwargs():
    conn = _connector_with_mock_adapter()
    conn._adapter.list_remote_queries.return_value = iter([])
    list(conn.list_remote_queries(history_days=14, limit=500))
    conn._adapter.list_remote_queries.assert_called_once_with(history_days=14, limit=500)
