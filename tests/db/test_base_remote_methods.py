import pytest


def test_base_adapter_default_remote_methods_raise():
    """Adapters that don't override remote methods must raise UnsupportedDatabaseOperation."""
    from amx.db.adapters.base import DatabaseAdapter, UnsupportedDatabaseOperation

    # Minimal concrete subclass — must implement every abstract method on the ABC.
    class _Stub(DatabaseAdapter):
        def column_sample_sql(self, *args, **kwargs):
            return ""

        def column_stats_sql(self, *args, **kwargs):
            return ""

        @classmethod
        def create_engine(cls, *args, **kwargs):
            return None

        def set_column_comment_sql(self, *args, **kwargs):
            return ""

        def set_database_comment_sql(self, *args, **kwargs):
            return ""

        def set_schema_comment_sql(self, *args, **kwargs):
            return ""

        def set_table_comment_sql(self, *args, **kwargs):
            return ""

        def system_schemas(self):
            return frozenset()

    stub = _Stub.__new__(_Stub)  # bypass __init__ which may need an engine

    # The unified ABC signature takes ``engine`` as the first positional arg
    # (sentinel here; the default implementations raise without touching it).
    engine = object()
    with pytest.raises(UnsupportedDatabaseOperation):
        list(stub.list_remote_notebooks(engine))
    with pytest.raises(UnsupportedDatabaseOperation):
        stub.fetch_remote_notebook_source(engine, "anything")
    with pytest.raises(UnsupportedDatabaseOperation):
        list(stub.list_remote_jobs(engine))
    with pytest.raises(UnsupportedDatabaseOperation):
        list(stub.list_remote_pipelines(engine))
    with pytest.raises(UnsupportedDatabaseOperation):
        list(stub.list_remote_streamlit_apps(engine))
    with pytest.raises(UnsupportedDatabaseOperation):
        list(stub.list_remote_streams(engine))
    with pytest.raises(UnsupportedDatabaseOperation):
        list(stub.list_remote_task_dependencies(engine))
    with pytest.raises(UnsupportedDatabaseOperation):
        list(stub.list_remote_queries(engine))
