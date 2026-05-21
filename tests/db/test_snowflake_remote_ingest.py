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
