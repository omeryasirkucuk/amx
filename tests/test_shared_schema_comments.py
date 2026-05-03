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
    assert not missing, f"{len(missing)} columns missing COMMENT (first 10): {missing[:10]}"


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


def test_jsonastext_round_trips_dict_through_text_storage() -> None:
    """Verify ``_JSONAsText`` (the TypeDecorator used on Databricks)
    serialises Python objects on write and deserialises them on read so
    callers see Python dicts/lists regardless of which backend the row
    came from. Without this, code in
    ``amx/storage/sqlalchemy_store.py:471`` (``r.get("scope_json")``)
    would receive a JSON string from Databricks and crash when it tried
    to call ``.get(schema, [])`` on it.
    """
    from amx.storage.shared_schema import _JSONAsText

    decorator = _JSONAsText()
    payload = {"schemas": ["public"], "tables": {"public": ["users", "orders"]}}

    stored = decorator.process_bind_param(payload, dialect=None)
    assert isinstance(stored, str), "process_bind_param must serialise to str"

    restored = decorator.process_result_value(stored, dialect=None)
    assert restored == payload, "round-trip must preserve the original Python value"

    # None passthrough on both sides — never serialise NULL as the
    # string ``"null"``.
    assert decorator.process_bind_param(None, dialect=None) is None
    assert decorator.process_result_value(None, dialect=None) is None

    # Already-deserialised values pass through unchanged (some drivers
    # might do their own JSON parsing).
    assert decorator.process_result_value({"x": 1}, dialect=None) == {"x": 1}


def test_databricks_create_table_ddl_compiles_for_every_table() -> None:
    """Regression: 0.12.3 and earlier raised on Databricks because the
    JSON columns hit ``GenericTypeCompiler.process can't render element
    of type JSON`` — ``databricks-sqlalchemy`` does not implement
    ``visit_JSON``. The user's symptom was a recurring warning on every
    ``amx`` startup:

        Shared history schema not initialised ((in table
        'analysis_runs', column 'scope_json'): Compiler … can't render
        element of type JSON).

    Fix in 0.12.4: ``_portable_json()`` switches to a TEXT-backed
    TypeDecorator on the Databricks dialect. Verify every table in
    the shared schema renders to DDL on Databricks AND the JSON
    columns end up as ``STRING`` instead of failing.
    """
    try:
        import databricks.sqlalchemy  # noqa: F401 — registers dialect
        from databricks.sqlalchemy import DatabricksDialect
    except ImportError as exc:
        import pytest

        pytest.skip(f"databricks-sqlalchemy not installed: {exc}")
    from sqlalchemy.schema import CreateTable

    md = build_metadata("AMX")
    dialect = DatabricksDialect()
    json_column_names = {
        "scope_json",
        "metrics_json",
        "tokens_json",
        "results_json",
        "settings_json",
        "alternatives_json",
        "details_json",
        "value_json",
    }
    seen_json_columns: set[str] = set()
    for table in md.tables.values():
        # Compiles without raising — that's the original bug.
        ddl = str(CreateTable(table).compile(dialect=dialect))
        for col in table.columns:
            if col.name in json_column_names:
                seen_json_columns.add(col.name)
                # Column must render as Databricks STRING, never JSON.
                assert f"{col.name} STRING" in ddl, (
                    f"{table.name}.{col.name} did not compile as STRING on Databricks: {ddl!r}"
                )
    # Defence in depth: confirm we actually exercised every JSON
    # column the schema declares — a future column added without the
    # ``_portable_json()`` helper would slip through otherwise.
    assert seen_json_columns == json_column_names, (
        f"Test fixture out of sync — missed columns: {json_column_names - seen_json_columns}"
    )
