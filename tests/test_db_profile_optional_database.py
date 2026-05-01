"""Phase 1 (0.11.0): ``database`` is now optional on DBConfig.

These tests pin the new contract introduced by the
``feat/multi-db-execution-and-optional-database`` branch:

* ``is_connection_configured()`` is the new "can we open an engine"
  predicate and does NOT require a database / catalog / dataset.
* ``is_database_pinned()`` reports whether the user committed to a
  specific database (per-backend semantics).
* ``is_configured()`` is back-compat-aliased to
  ``is_connection_configured()`` so the 99 existing call sites that
  use it stay valid.
* ``DBConfig.url`` builds a server-only URL when the database is
  unpinned (PostgreSQL / Snowflake), instead of emitting a trailing
  ``/`` that fails to parse.
* ``display_summary`` shows a ``(no DB pinned)`` token so the UI
  cannot regress to "phantom localhost" rows.
* ``has_legacy_database_default()`` flags the historical
  ``database='SAP'`` demo value without mutating the config.
"""

from __future__ import annotations

from amx.config import DBConfig, has_legacy_database_default

# ── is_connection_configured / is_database_pinned ────────────────────────


def test_postgresql_unpinned_database_is_still_connection_configured():
    db = DBConfig(backend="postgresql", host="db.example.com", user="alice", password="x")
    assert db.is_connection_configured() is True
    assert db.is_database_pinned() is False
    # Back-compat alias must follow connection-configured semantics.
    assert db.is_configured() is True


def test_postgresql_pinned_database_marks_pinned():
    db = DBConfig(
        backend="postgresql",
        host="db.example.com",
        user="alice",
        password="x",
        database="prod_pg",
    )
    assert db.is_connection_configured() is True
    assert db.is_database_pinned() is True
    assert db.is_configured() is True


def test_snowflake_unpinned_database_is_still_connection_configured():
    db = DBConfig(
        backend="snowflake",
        account="xy12345.us-east-1",
        user="ANALYST",
        password="x",
    )
    assert db.is_connection_configured() is True
    assert db.is_database_pinned() is False


def test_snowflake_missing_account_breaks_connection():
    db = DBConfig(backend="snowflake", user="ANALYST", password="x", database="ANALYTICS")
    assert db.is_connection_configured() is False


def test_databricks_pinned_uses_catalog_not_database():
    db = DBConfig(
        backend="databricks",
        host="adb-x.azuredatabricks.net",
        access_token="t",
    )
    assert db.is_connection_configured() is True
    assert db.is_database_pinned() is False
    db_pinned = DBConfig(
        backend="databricks",
        host="adb-x.azuredatabricks.net",
        access_token="t",
        catalog="hive_metastore",
    )
    assert db_pinned.is_database_pinned() is True


def test_bigquery_pinned_uses_dataset_not_database():
    db = DBConfig(backend="bigquery", project="my-proj")
    assert db.is_connection_configured() is True
    assert db.is_database_pinned() is False
    db_pinned = DBConfig(backend="bigquery", project="my-proj", dataset="analytics")
    assert db_pinned.is_database_pinned() is True


def test_bigquery_missing_project_breaks_connection():
    db = DBConfig(backend="bigquery", dataset="analytics")
    assert db.is_connection_configured() is False


# ── url / display_summary ────────────────────────────────────────────────


def test_postgresql_url_falls_back_to_postgres_system_db_when_unpinned():
    """When the user leaves ``database`` blank, the URL targets the
    ``postgres`` system database that every PostgreSQL install ships
    with. Pre-fix the URL had no database segment at all and libpq
    fell back to the username, which almost never works — see the
    sibling test
    ``test_postgres_url_falls_back_to_postgres_system_db_when_empty``
    for the user-flow context.
    """
    db = DBConfig(
        backend="postgresql",
        host="db.example.com",
        port=5432,
        user="alice",
        password="x",
    )
    url = db.url
    assert url == "postgresql://alice:x@db.example.com:5432/postgres"
    # No empty trailing segment that libpq would treat as "use the
    # username as the database":
    assert not url.endswith("/")
    assert not url.endswith(":5432")


def test_postgresql_url_includes_database_when_pinned():
    db = DBConfig(
        backend="postgresql",
        host="db.example.com",
        port=5432,
        user="alice",
        password="x",
        database="prod_pg",
    )
    assert db.url == "postgresql://alice:x@db.example.com:5432/prod_pg"


def test_snowflake_url_omits_database_segment_when_unpinned():
    db = DBConfig(
        backend="snowflake",
        account="xy12345.us-east-1",
        user="ANALYST",
        password="x",
        warehouse="WH",
    )
    url = db.url
    assert url == "snowflake://ANALYST:x@xy12345.us-east-1?warehouse=WH"


def test_snowflake_url_includes_database_segment_when_pinned():
    db = DBConfig(
        backend="snowflake",
        account="xy12345.us-east-1",
        user="ANALYST",
        password="x",
        database="ANALYTICS",
    )
    assert "/ANALYTICS" in db.url


def test_display_summary_marks_unpinned_database_per_backend():
    pg = DBConfig(backend="postgresql", host="db.example.com", port=5432, user="alice")
    assert "(no DB pinned)" in pg.display_summary
    sf = DBConfig(backend="snowflake", account="xy12345.us-east-1", user="ANALYST")
    assert "(no DB pinned)" in sf.display_summary
    db = DBConfig(backend="databricks", host="adb-x.azuredatabricks.net", access_token="t")
    assert "(no DB pinned)" in db.display_summary
    bq = DBConfig(backend="bigquery", project="my-proj")
    assert "(no DB pinned)" in bq.display_summary


def test_display_summary_clean_when_pinned():
    pg = DBConfig(
        backend="postgresql",
        host="db.example.com",
        port=5432,
        user="alice",
        database="prod_pg",
    )
    assert "(no DB pinned)" not in pg.display_summary


# ── has_legacy_database_default ──────────────────────────────────────────


def test_has_legacy_database_default_flags_sap_on_pg_and_snowflake():
    pg_legacy = DBConfig(backend="postgresql", host="x", user="y", database="SAP")
    sf_legacy = DBConfig(backend="snowflake", account="x", user="y", database="SAP")
    assert has_legacy_database_default(pg_legacy) is True
    assert has_legacy_database_default(sf_legacy) is True


def test_has_legacy_database_default_does_not_flag_modern_dbs():
    pg_real = DBConfig(backend="postgresql", host="x", user="y", database="prod")
    pg_blank = DBConfig(backend="postgresql", host="x", user="y")
    db = DBConfig(backend="databricks", host="x", access_token="t", catalog="SAP")
    bq = DBConfig(backend="bigquery", project="p", dataset="SAP")
    assert has_legacy_database_default(pg_real) is False
    assert has_legacy_database_default(pg_blank) is False
    # SAP value on Databricks/BigQuery is not flagged — those backends never
    # shipped that demo default.
    assert has_legacy_database_default(db) is False
    assert has_legacy_database_default(bq) is False


# ── DBConfig defaults regression ─────────────────────────────────────────


def test_dbconfig_default_database_is_empty_not_sap():
    """Regression: pre-0.11 the default leaked into UI as a phantom row."""
    db = DBConfig()
    assert db.database == ""
    assert db.is_database_pinned() is False


def test_postgres_url_falls_back_to_postgres_system_db_when_empty():
    """The /add-db-profile wizard promises ``database`` is optional —
    "leave blank to pick at command time". For that promise to hold,
    the URL builder must produce a URL libpq can actually connect to.

    Pre-fix the URL had no database segment (``postgresql://u:p@h:5432``)
    and libpq silently fell back to the username as the database name,
    which almost never exists. The user then saw
    ``FATAL: database "amx" does not exist`` and concluded the wizard
    had lied. Now we explicitly fall back to the ``postgres`` system
    database that every PostgreSQL install ships with.
    """
    db = DBConfig(
        backend="postgresql",
        host="localhost",
        port=5432,
        user="alice",
        password="secret",
        database="",
    )
    assert db.url == "postgresql://alice:secret@localhost:5432/postgres", (
        "Empty database must fall back to /postgres so libpq can connect; "
        "see DBConfig.url postgres branch."
    )


def test_postgres_url_uses_explicit_database_when_set():
    """Counterpart to the no-DB fallback: when the user explicitly pins
    a database, that name lands in the URL untouched (URL-encoded).
    """
    db = DBConfig(
        backend="postgresql",
        host="db.example.com",
        port=5432,
        user="alice",
        password="secret",
        database="analytics",
    )
    assert db.url == "postgresql://alice:secret@db.example.com:5432/analytics"
