import sqlite3


def _new_store(tmp_path):
    from amx.storage.sqlite_store import SQLiteHistoryStore

    store = SQLiteHistoryStore(tmp_path / "amx.db")
    store.init()
    return store


def test_catalog_relationships_has_entity_kind_columns(tmp_path):
    store = _new_store(tmp_path)
    with sqlite3.connect(store.db_path) as conn:
        cur = conn.execute("PRAGMA table_info(catalog_relationships)")
        cols = {row[1] for row in cur.fetchall()}
    assert "from_entity_kind" in cols, f"from_entity_kind missing — got {cols}"
    assert "to_entity_kind" in cols, f"to_entity_kind missing — got {cols}"


def test_entity_kind_default_is_table(tmp_path):
    store = _new_store(tmp_path)
    with sqlite3.connect(store.db_path) as conn:
        # Insert a row WITHOUT the new columns to verify default applies.
        # catalog_relationships uses INTEGER PRIMARY KEY AUTOINCREMENT for id;
        # required NOT NULL columns are: from_entity_id, to_entity_id, relationship_type.
        # Other columns have DEFAULTs.
        conn.execute(
            "INSERT INTO catalog_relationships (from_entity_id, to_entity_id, relationship_type) "
            "VALUES (1, 2, 'join')"
        )
        row = conn.execute(
            "SELECT from_entity_kind, to_entity_kind FROM catalog_relationships "
            "WHERE from_entity_id=1 AND to_entity_id=2 AND relationship_type='join'"
        ).fetchone()
    assert row == ("table", "table")
