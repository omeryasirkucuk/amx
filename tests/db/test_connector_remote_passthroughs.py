from types import SimpleNamespace
from unittest.mock import MagicMock


def _connector_with_mock_adapter(engine=None):
    """Build a ``DatabaseConnector`` with mocked adapter + engine.

    Passes ``engine`` through ``self.engine`` so passthrough tests see the
    same argument the production code path uses.
    """
    from amx.db.connector import DatabaseConnector

    conn = DatabaseConnector.__new__(DatabaseConnector)
    conn._adapter = MagicMock()  # type: ignore[attr-defined]
    # ``DatabaseConnector.engine`` is a property that constructs / caches the
    # engine lazily. Tests bypass that with an explicit override stored on
    # ``self._engine`` so the passthrough sees whatever sentinel we pass in.
    conn._engine = engine  # type: ignore[attr-defined]
    return conn


def test_connector_list_remote_notebooks_delegates_with_engine():
    sentinel = SimpleNamespace(label="engine-sentinel")
    conn = _connector_with_mock_adapter(engine=sentinel)
    conn._adapter.list_remote_notebooks.return_value = iter([])
    list(conn.list_remote_notebooks())
    # PR-A: passthrough now also forwards external_id_filter (None means
    # the pre-PR-A "ingest all" path is preserved).
    conn._adapter.list_remote_notebooks.assert_called_once_with(sentinel, external_id_filter=None)


def test_connector_fetch_remote_notebook_source_delegates_with_engine():
    sentinel = SimpleNamespace(label="engine-sentinel")
    conn = _connector_with_mock_adapter(engine=sentinel)
    conn._adapter.fetch_remote_notebook_source.return_value = "{}"
    assert conn.fetch_remote_notebook_source("nb-1") == "{}"
    conn._adapter.fetch_remote_notebook_source.assert_called_once_with(sentinel, "nb-1")


def test_connector_list_remote_jobs_passes_runs_per_job():
    sentinel = SimpleNamespace(label="engine-sentinel")
    conn = _connector_with_mock_adapter(engine=sentinel)
    conn._adapter.list_remote_jobs.return_value = iter([])
    list(conn.list_remote_jobs(runs_per_job=5))
    conn._adapter.list_remote_jobs.assert_called_once_with(
        sentinel, runs_per_job=5, external_id_filter=None
    )


def test_connector_list_remote_queries_passes_engine_and_kwargs():
    sentinel = SimpleNamespace(label="engine-sentinel")
    conn = _connector_with_mock_adapter(engine=sentinel)
    conn._adapter.list_remote_queries.return_value = iter([])
    list(conn.list_remote_queries(history_days=14, limit=500))
    conn._adapter.list_remote_queries.assert_called_once_with(sentinel, history_days=14, limit=500)


def test_connector_passthroughs_satisfy_databricks_adapter_signature():
    """Regression test for the bug where Snowflake's adapter signature
    required ``engine`` but the passthrough called it with no args.

    Instead of mocking the adapter, instantiate a real one and verify the
    passthrough's call shape matches. We use Databricks here because its
    methods can be constructed without a live workspace (an injected mock
    client substitutes for HTTP). Snowflake is covered by the equivalent
    test in ``test_snowflake_remote_ingest.py``.
    """
    from amx.db.adapters.databricks import DatabricksAdapter
    from amx.db.connector import DatabaseConnector

    adapter = DatabricksAdapter.__new__(DatabricksAdapter)
    adapter._workspace_client_override = MagicMock()  # type: ignore[attr-defined]
    adapter._workspace_client_override.list_workspace_objects.return_value = iter([])
    adapter._workspace_client_override.list_jobs_full.return_value = iter([])
    adapter._workspace_client_override.list_pipelines.return_value = iter([])
    adapter._workspace_client_override.list_saved_queries.return_value = iter([])
    adapter._workspace_client_override.list_query_history.return_value = iter([])

    conn = DatabaseConnector.__new__(DatabaseConnector)
    conn._adapter = adapter  # type: ignore[attr-defined]
    conn._engine = SimpleNamespace(label="engine-sentinel")  # type: ignore[attr-defined]

    # Each of these previously raised ``TypeError: missing 1 required
    # positional argument: 'engine'`` when running against the Snowflake
    # adapter. The unified-signature fix means both adapters now accept
    # ``engine`` and the passthrough is a single shape.
    assert list(conn.list_remote_notebooks()) == []
    assert list(conn.list_remote_jobs()) == []
    assert list(conn.list_remote_pipelines()) == []
    assert list(conn.list_remote_queries(history_days=7, limit=10)) == []
