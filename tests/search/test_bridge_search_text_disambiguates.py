"""PR-B: bridge ``search_text`` carries the disambiguating path.

Two notebooks called ``etl`` in different workspace folders used to
land in ``catalog_entities`` with identical ``search_text='etl'``.
Semantic-search hits, lineage canvas labels, and Ask evidence
references then couldn't tell them apart.

The bridge ``_upsert_asset_entity`` now folds the source row's path
into ``search_text`` as ``"name (path)"`` when the path is set.
Different paths therefore produce distinct ``search_text`` values
while the underlying ``(kind, source_remote_id)`` key still uniquely
identifies each row.
"""

import sqlite3

from amx.search.catalog import SearchCatalog
from amx.storage.sqlite_store import SQLiteHistoryStore


def _seed_notebook(conn, *, name: str, workspace_path: str, external_id: str):
    """Insert a remote_notebooks row with a controlled path."""
    conn.execute(
        """
        INSERT INTO remote_notebooks
            (profile_name, platform, external_id, name, workspace_path,
             qualified_name, language, source_text, source_hash,
             last_modified_at, last_modified_by, owner, cell_count, ingested_at)
        VALUES ('prod', 'databricks', ?, ?, ?, NULL,
                'python', '{}', 'h', NULL, NULL, NULL, 1,
                '2026-05-21T00:00:00')
        """,
        (external_id, name, workspace_path),
    )


def test_same_name_notebooks_get_distinct_search_text(tmp_path):
    db_path = tmp_path / "history.db"
    SQLiteHistoryStore(db_path).init()
    with sqlite3.connect(db_path) as conn:
        _seed_notebook(
            conn, name="etl", workspace_path="/Workspace/team-a/etl", external_id="ext-1"
        )
        _seed_notebook(
            conn, name="etl", workspace_path="/Workspace/team-b/etl", external_id="ext-2"
        )
        conn.commit()

    catalog = SearchCatalog(db_path=str(db_path))
    catalog.rebuild_remote_asset_lineage(profile_name="prod")

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT source_remote_id, search_text FROM catalog_entities "
            "WHERE entity_kind = 'notebook' ORDER BY source_remote_id"
        ).fetchall()
    assert len(rows) == 2
    search_texts = {r[1] for r in rows}
    assert search_texts == {
        "etl (/Workspace/team-a/etl)",
        "etl (/Workspace/team-b/etl)",
    }


def test_bridge_kind_source_remote_id_still_unique(tmp_path):
    """The PR-B compound ``search_text`` must not weaken the existing
    ``(kind, source_remote_id)`` identity. A re-run of the bridge
    sync should update the existing row rather than insert a duplicate.
    """
    db_path = tmp_path / "history.db"
    SQLiteHistoryStore(db_path).init()
    with sqlite3.connect(db_path) as conn:
        _seed_notebook(
            conn, name="etl", workspace_path="/Workspace/team-a/etl", external_id="ext-1"
        )
        conn.commit()

    catalog = SearchCatalog(db_path=str(db_path))
    catalog.rebuild_remote_asset_lineage(profile_name="prod")
    catalog.rebuild_remote_asset_lineage(profile_name="prod")  # idempotent

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT id FROM catalog_entities WHERE entity_kind = 'notebook'"
        ).fetchall()
    assert len(rows) == 1


def test_query_without_path_keeps_bare_name(tmp_path):
    """Kinds without a natural path (queries) must still carry the
    bare name in ``search_text`` — the compound form would render as
    ``"foo ()"`` and look broken.
    """
    db_path = tmp_path / "history.db"
    SQLiteHistoryStore(db_path).init()
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO remote_queries
                (profile_name, platform, external_id, name, sql_text, sql_hash,
                 warehouse, user_name, executed_at, duration_ms, kind, ingested_at)
            VALUES ('prod', 'databricks', 'q-1', 'monthly_revenue',
                    'SELECT 1', 'h', NULL, NULL, NULL, NULL, 'saved',
                    '2026-05-21T00:00:00')
            """,
        )
        conn.commit()

    catalog = SearchCatalog(db_path=str(db_path))
    catalog.rebuild_remote_asset_lineage(profile_name="prod")

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT search_text FROM catalog_entities WHERE entity_kind = 'query'"
        ).fetchone()
    assert row is not None
    assert row[0] == "monthly_revenue"
