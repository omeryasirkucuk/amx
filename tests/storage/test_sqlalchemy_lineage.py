# tests/storage/test_sqlalchemy_lineage.py
import pytest
from sqlalchemy import create_engine
from amx.storage.sqlalchemy_store import SQLAlchemyHistoryStore


@pytest.fixture
def store(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path}/shared.db")
    # Use schema="main" — the only schema SQLite supports natively.
    # Production deployments use schema="AMX" on PostgreSQL/Snowflake etc.
    s = SQLAlchemyHistoryStore(engine, schema="main")
    s.init()
    return s


def test_lineage_table_handles_present(store):
    assert store._t_lineage_artifacts is not None
    assert store._t_lineage_artifact_nodes is not None
    assert store._t_lineage_artifact_edges is not None
    assert store._t_lineage_comments is not None
