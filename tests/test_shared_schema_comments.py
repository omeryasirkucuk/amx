"""Dogfooding test: AMX's own shared-history schema must ship with comments.

AMX's product thesis is "every database table and column should have a
quality COMMENT ON description." This test pins that AMX's own
warehouse artifacts (the AMX schema bootstrapped by
``/history-store enable``) also satisfy that contract — every Table
and every Column in :func:`amx.storage.shared_schema.build_metadata`
carries a non-empty ``comment=`` annotation.

If a future contributor adds a Column without a comment, this test
fails fast — preventing a regression that would re-create the
embarrassment of shipping a metadata tool that does not annotate its
own outputs.
"""

from __future__ import annotations

from amx.storage.shared_schema import build_metadata


def test_every_table_has_comment() -> None:
    md = build_metadata("AMX")
    missing = [name for name, table in md.tables.items() if not (table.comment or "").strip()]
    assert not missing, f"Tables missing COMMENT: {missing}"


def test_every_column_has_comment() -> None:
    md = build_metadata("AMX")
    missing: list[str] = []
    for table in md.tables.values():
        for col in table.columns:
            if not (col.comment or "").strip():
                missing.append(f"{table.name}.{col.name}")
    assert not missing, (
        f"{len(missing)} columns missing COMMENT (first 10): {missing[:10]}"
    )


def test_create_history_tables_ddl_emits_comments_on_postgres() -> None:
    """Verify the dump-ddl pipeline renders COMMENT ON statements.

    Uses the PostgreSQL dialect because it emits ``COMMENT ON TABLE``
    and ``COMMENT ON COLUMN`` as separate statements (vs MySQL which
    inlines them), making the contract easiest to grep.
    """
    from amx.config import DBConfig
    from amx.db.adapters.postgresql import PostgreSQLAdapter

    cfg = DBConfig(
        backend="postgresql",
        host="localhost",
        port=5432,
        database="test",
        user="test",
        password="test",
    )
    ddl = PostgreSQLAdapter(cfg).create_history_tables_ddl("AMX")

    assert ddl.count("CREATE TABLE") == 5, "expected 5 CREATE TABLE statements"
    assert ddl.count("COMMENT ON TABLE") == 5, "expected 5 COMMENT ON TABLE statements"
    # 75 columns, all annotated
    assert ddl.count("COMMENT ON COLUMN") == 75, (
        f"expected 75 COMMENT ON COLUMN statements, got {ddl.count('COMMENT ON COLUMN')}"
    )
