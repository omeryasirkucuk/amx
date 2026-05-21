import json
import sqlite3
from datetime import datetime, timezone


def _store_and_catalog(tmp_path):
    """Build a fresh SQLiteHistoryStore + SearchCatalog rooted at tmp_path."""
    from amx.storage.sqlite_store import SQLiteHistoryStore

    store = SQLiteHistoryStore(tmp_path / "amx.db")
    store.init()
    from amx.search.catalog import SearchCatalog

    catalog = SearchCatalog(store.db_path)
    return store, catalog


def _nb(**overrides):
    from amx.db.adapters.remote_asset_types import RemoteNotebook

    defaults = {
        "external_id": "ext-1",
        "name": "n1",
        "platform": "databricks",
        "language": "python",
        "workspace_path": "/n1",
        "qualified_name": None,
        "source_text": '{"cells": [], "nbformat": 4, "nbformat_minor": 5, "metadata": {}}',
        "source_hash": "h1",
        "last_modified_at": None,
        "last_modified_by": None,
        "owner": None,
        "cell_count": 0,
    }
    defaults.update(overrides)
    return RemoteNotebook(**defaults)


def test_sync_remote_assets_upserts_notebook(tmp_path):
    store, catalog = _store_and_catalog(tmp_path)
    counts = catalog.sync_remote_assets(profile_name="prod", notebooks=[_nb()])
    assert counts.get("notebooks") == 1
    with sqlite3.connect(store.db_path) as c:
        rows = c.execute(
            "SELECT name, source_hash FROM remote_notebooks WHERE profile_name='prod'"
        ).fetchall()
    assert rows == [("n1", "h1")]


def test_sync_remote_assets_short_circuits_unchanged_source(tmp_path):
    store, catalog = _store_and_catalog(tmp_path)
    catalog.sync_remote_assets(profile_name="prod", notebooks=[_nb()])
    # Re-sync with same source_hash — count should report 0 fresh upserts.
    counts2 = catalog.sync_remote_assets(profile_name="prod", notebooks=[_nb()])
    assert counts2.get("notebooks", 0) == 0
    with sqlite3.connect(store.db_path) as c:
        n = c.execute("SELECT COUNT(*) FROM remote_notebooks WHERE profile_name='prod'").fetchone()[
            0
        ]
    assert n == 1  # not duplicated


def test_sync_remote_assets_writes_job_with_tasks_and_runs(tmp_path):
    from amx.db.adapters.remote_asset_types import RemoteJob, RemoteJobRun, RemoteJobTask

    store, catalog = _store_and_catalog(tmp_path)
    task = RemoteJobTask(
        task_key="extract",
        task_type="notebook_task",
        notebook_path="/n1",
        sql_query_id=None,
        sql_warehouse_id=None,
        pipeline_id=None,
        depends_on=("upstream",),
        raw_definition={"task_key": "extract"},
    )
    run = RemoteJobRun(
        run_id=1,
        state_result="SUCCESS",
        start_time=datetime.now(timezone.utc),
        end_time=None,
        setup_duration_ms=100,
        execution_duration_ms=9000,
    )
    job = RemoteJob(
        job_id=42,
        name="nightly",
        creator_user_name="alice",
        schedule_cron="0 2 * * *",
        schedule_timezone="UTC",
        schedule_pause_status="UNPAUSED",
        max_concurrent_runs=1,
        email_notifications={"on_failure": ["ops@example.com"]},
        tags={"team": "data"},
        tasks=(task,),
        recent_runs=(run,),
    )
    catalog.sync_remote_assets(profile_name="prod", jobs=[job])
    with sqlite3.connect(store.db_path) as c:
        job_row = c.execute(
            "SELECT job_id, name, schedule_cron, success_rate_30d FROM remote_jobs"
        ).fetchone()
        task_row = c.execute(
            "SELECT task_key, task_type, notebook_path, depends_on_json FROM remote_job_tasks"
        ).fetchone()
        run_row = c.execute("SELECT run_id, state_result FROM remote_job_runs").fetchone()
    assert job_row[0] == 42 and job_row[1] == "nightly" and job_row[2] == "0 2 * * *"
    assert job_row[3] == 1.0
    assert task_row[0] == "extract" and task_row[1] == "notebook_task"
    assert json.loads(task_row[3]) == ["upstream"]
    assert run_row == (1, "SUCCESS")


def test_sync_remote_assets_writes_task_dependencies(tmp_path):
    store, catalog = _store_and_catalog(tmp_path)
    counts = catalog.sync_remote_assets(
        profile_name="prod",
        task_dependencies=[("a.b.LOAD", "c.d.AGG"), ("c.d.AGG", "c.d.NOTIFY")],
    )
    assert counts.get("task_dependencies") == 2
    with sqlite3.connect(store.db_path) as c:
        rows = sorted(
            c.execute(
                "SELECT parent_task_fqn, child_task_fqn FROM remote_task_dependencies "
                "WHERE profile_name='prod'"
            ).fetchall()
        )
    assert rows == [("a.b.LOAD", "c.d.AGG"), ("c.d.AGG", "c.d.NOTIFY")]


def test_rebuild_remote_asset_lineage_links_notebook_to_referenced_table(tmp_path):
    store, catalog = _store_and_catalog(tmp_path)
    # Seed a catalog_entities row for the referenced table.
    with sqlite3.connect(store.db_path) as c:
        c.execute(
            """INSERT INTO catalog_entities
                   (db_profile, db_backend, database_name, schema_name, table_name,
                    entity_kind, asset_kind, updated_at)
               VALUES ('prod', 'snowflake', 'raw', 'public', 'orders',
                       'table', 'table', 0.0)"""
        )
        c.commit()
        entity_id = c.execute(
            "SELECT id FROM catalog_entities WHERE table_name='orders'"
        ).fetchone()[0]
    # Ingest a notebook whose code references raw.public.orders.
    nb_src = '{"cells":[{"cell_type":"code","source":["select * from raw.public.orders"],"metadata":{"language":"sql"},"execution_count":null,"outputs":[]}],"nbformat":4,"nbformat_minor":5,"metadata":{}}'
    catalog.sync_remote_assets(
        profile_name="prod", notebooks=[_nb(source_text=nb_src, source_hash="h2", language="sql")]
    )
    counts = catalog.rebuild_remote_asset_lineage(profile_name="prod")
    assert counts["notebooks"] >= 1
    with sqlite3.connect(store.db_path) as c:
        edges = c.execute(
            "SELECT relationship_type, from_entity_kind, to_entity_kind, to_entity_id "
            "FROM catalog_relationships WHERE relationship_type='asset_references_table'"
        ).fetchall()
    assert any(e[0] == "asset_references_table" and e[3] == entity_id for e in edges)


def test_rebuild_remote_asset_lineage_idempotent(tmp_path):
    store, catalog = _store_and_catalog(tmp_path)
    with sqlite3.connect(store.db_path) as c:
        c.execute(
            """INSERT INTO catalog_entities
                   (db_profile, db_backend, database_name, schema_name, table_name,
                    entity_kind, asset_kind, updated_at)
               VALUES ('prod', 'snowflake', 'raw', 'public', 'orders',
                       'table', 'table', 0.0)"""
        )
        c.commit()
    nb_src = '{"cells":[{"cell_type":"code","source":["select * from raw.public.orders"],"metadata":{"language":"sql"},"execution_count":null,"outputs":[]}],"nbformat":4,"nbformat_minor":5,"metadata":{}}'
    catalog.sync_remote_assets(
        profile_name="prod", notebooks=[_nb(source_text=nb_src, source_hash="h2", language="sql")]
    )
    catalog.rebuild_remote_asset_lineage(profile_name="prod")
    catalog.rebuild_remote_asset_lineage(profile_name="prod")  # second call must not duplicate
    with sqlite3.connect(store.db_path) as c:
        count = c.execute(
            "SELECT COUNT(*) FROM catalog_relationships "
            "WHERE relationship_type='asset_references_table'"
        ).fetchone()[0]
    assert count == 1


def test_rebuild_remote_asset_lineage_for_stream_resolves_source_table(tmp_path):
    from amx.db.adapters.remote_asset_types import RemoteStream

    store, catalog = _store_and_catalog(tmp_path)
    with sqlite3.connect(store.db_path) as c:
        c.execute(
            """INSERT INTO catalog_entities
                   (db_profile, db_backend, database_name, schema_name, table_name,
                    entity_kind, asset_kind, updated_at)
               VALUES ('prod', 'snowflake', 'raw', 'public', 'orders',
                       'table', 'table', 0.0)"""
        )
        c.commit()
        ent = c.execute("SELECT id FROM catalog_entities WHERE table_name='orders'").fetchone()[0]
    stream = RemoteStream(
        qualified_name="raw.public.orders_stream",
        source_table_fqn="raw.public.orders",
        mode="APPEND_ONLY",
        stale_after=None,
        owner=None,
    )
    catalog.sync_remote_assets(profile_name="prod", streams=[stream])
    counts = catalog.rebuild_remote_asset_lineage(profile_name="prod")
    assert counts["streams"] >= 1
    with sqlite3.connect(store.db_path) as c:
        edges = c.execute(
            "SELECT from_entity_kind, to_entity_id FROM catalog_relationships "
            "WHERE relationship_type='asset_references_table'"
        ).fetchall()
    assert ("stream", ent) in edges


def test_notebook_id_fk_resolved_after_job_ingest(tmp_path):
    from amx.db.adapters.remote_asset_types import RemoteJob, RemoteJobTask

    store, catalog = _store_and_catalog(tmp_path)
    # Ingest the notebook first.
    catalog.sync_remote_assets(
        profile_name="prod",
        notebooks=[_nb(workspace_path="/Users/alice/extract", external_id="ext-extract")],
    )
    # Then ingest a job whose task points at that notebook path.
    task = RemoteJobTask(
        task_key="extract",
        task_type="notebook_task",
        notebook_path="/Users/alice/extract",
        sql_query_id=None,
        sql_warehouse_id=None,
        pipeline_id=None,
        depends_on=(),
        raw_definition={},
    )
    job = RemoteJob(
        job_id=100,
        name="j",
        creator_user_name=None,
        schedule_cron=None,
        schedule_timezone=None,
        schedule_pause_status=None,
        max_concurrent_runs=None,
        email_notifications={},
        tags={},
        tasks=(task,),
        recent_runs=(),
    )
    catalog.sync_remote_assets(profile_name="prod", jobs=[job])
    with sqlite3.connect(store.db_path) as c:
        nb_id = c.execute(
            "SELECT id FROM remote_notebooks WHERE workspace_path='/Users/alice/extract'"
        ).fetchone()[0]
        task_fk = c.execute(
            "SELECT notebook_id_fk FROM remote_job_tasks WHERE task_key='extract'"
        ).fetchone()[0]
    assert task_fk == nb_id
