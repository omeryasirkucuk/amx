from unittest.mock import MagicMock


def _adapter():
    from amx.db.adapters.snowflake import SnowflakeAdapter
    a = SnowflakeAdapter.__new__(SnowflakeAdapter)
    return a


def _engine_with_fake(execute_fn):
    eng = MagicMock()
    eng.connect.return_value.__enter__.return_value.execute = execute_fn
    eng.connect.return_value.__exit__.return_value = False
    return eng


def test_list_remote_notebooks_yields_normalized_ipynb():
    a = _adapter()
    show_rows = [{
        "name": "WEEKLY_KPIS", "database_name": "ANALYTICS", "schema_name": "RAW",
        "owner": "DATA_ENG", "comment": "", "last_altered": "2026-05-01T00:00:00",
        "language": "python",
    }]
    desc_rows = [
        {"property": "MAIN_FILE", "value": "weekly_kpis.ipynb"},
        {"property": "QUERY_WAREHOUSE", "value": "WH_S"},
        {"property": "ROOT_LOCATION", "value": "@ANALYTICS.RAW.NB_STAGE/weekly_kpis"},
    ]
    stage_rows = [{
        "$1": '{"cells":[{"cell_type":"code","source":["select 1"],"metadata":{},"outputs":[],"execution_count":null}],"nbformat":4,"nbformat_minor":5,"metadata":{}}'
    }]

    def fake_execute(stmt, *args, **kwargs):
        s = str(stmt).upper()
        if s.startswith("SHOW NOTEBOOKS"):
            return MagicMock(mappings=lambda: MagicMock(all=lambda: show_rows))
        if s.startswith("DESC NOTEBOOK"):
            return MagicMock(mappings=lambda: MagicMock(all=lambda: desc_rows))
        if "SELECT $1" in s or s.startswith("SELECT $1"):
            return MagicMock(mappings=lambda: MagicMock(all=lambda: stage_rows))
        return MagicMock(mappings=lambda: MagicMock(all=lambda: []))

    nbs = list(a.list_remote_notebooks(_engine_with_fake(fake_execute)))
    assert len(nbs) == 1
    nb = nbs[0]
    assert nb.platform == "snowflake"
    assert nb.qualified_name == "ANALYTICS.RAW.WEEKLY_KPIS"
    assert nb.workspace_path is None
    assert nb.cell_count == 1
    assert len(nb.source_hash) == 64


def test_capability_remote_notebooks_true_for_snowflake():
    from amx.db.adapters.snowflake import SnowflakeAdapter
    assert SnowflakeAdapter.capabilities.remote_notebooks is True


def test_list_remote_streamlit_apps():
    a = _adapter()
    show_rows = [{
        "name": "DASH_KPIS", "database_name": "ANALYTICS", "schema_name": "APPS",
        "owner": "DATA_ENG", "query_warehouse": "WH_S",
        "last_altered": "2026-05-01T00:00:00",
    }]
    desc_rows = [
        {"property": "ROOT_LOCATION", "value": "@APPS.DASH_STAGE/dash_kpis"},
        {"property": "MAIN_FILE", "value": "streamlit_app.py"},
    ]
    def fake_execute(stmt, *args, **kwargs):
        s = str(stmt).upper()
        if "SHOW STREAMLITS" in s:
            return MagicMock(mappings=lambda: MagicMock(all=lambda: show_rows))
        if "DESC STREAMLIT" in s:
            return MagicMock(mappings=lambda: MagicMock(all=lambda: desc_rows))
        return MagicMock(mappings=lambda: MagicMock(all=lambda: []))
    apps = list(a.list_remote_streamlit_apps(_engine_with_fake(fake_execute)))
    assert apps[0].qualified_name == "ANALYTICS.APPS.DASH_KPIS"
    assert apps[0].main_file == "streamlit_app.py"
    assert apps[0].query_warehouse == "WH_S"


def test_capability_remote_streamlit_apps_flag_on():
    from amx.db.adapters.snowflake import SnowflakeAdapter
    assert SnowflakeAdapter.capabilities.remote_streamlit_apps is True


def test_list_remote_streams():
    a = _adapter()
    rows = [{
        "name": "ORDERS_STREAM", "database_name": "RAW", "schema_name": "PUBLIC",
        "table_name": "RAW.PUBLIC.ORDERS", "mode": "APPEND_ONLY",
        "stale_after": "2026-06-01T00:00:00", "owner": "DATA_ENG",
    }]
    def fake_execute(stmt, *args, **kwargs):
        if "SHOW STREAMS" in str(stmt).upper():
            return MagicMock(mappings=lambda: MagicMock(all=lambda: rows))
        return MagicMock(mappings=lambda: MagicMock(all=lambda: []))
    streams = list(a.list_remote_streams(_engine_with_fake(fake_execute)))
    assert streams[0].qualified_name == "RAW.PUBLIC.ORDERS_STREAM"
    assert streams[0].source_table_fqn == "RAW.PUBLIC.ORDERS"
    assert streams[0].mode == "APPEND_ONLY"


def test_capability_remote_streams_flag_on():
    from amx.db.adapters.snowflake import SnowflakeAdapter
    assert SnowflakeAdapter.capabilities.remote_streams is True


def test_list_remote_task_dependencies():
    a = _adapter()
    rows = [
        {"name_predecessor": "RAW.PUBLIC.LOAD_TASK", "name": "MARTS.GOLD.AGG_TASK"},
        {"name_predecessor": "MARTS.GOLD.AGG_TASK", "name": "MARTS.GOLD.NOTIFY_TASK"},
    ]
    def fake_execute(stmt, *args, **kwargs):
        if "TASK_DEPENDENTS" in str(stmt).upper():
            return MagicMock(mappings=lambda: MagicMock(all=lambda: rows))
        return MagicMock(mappings=lambda: MagicMock(all=lambda: []))
    edges = list(a.list_remote_task_dependencies(_engine_with_fake(fake_execute)))
    assert ("RAW.PUBLIC.LOAD_TASK", "MARTS.GOLD.AGG_TASK") in edges


def test_capability_remote_task_dependencies_flag_on():
    from amx.db.adapters.snowflake import SnowflakeAdapter
    assert SnowflakeAdapter.capabilities.remote_task_dependencies is True
