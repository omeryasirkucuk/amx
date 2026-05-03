"""Multi-backend expansion (Phase 2): MySQL, Oracle, SQL Server, Redshift,
ClickHouse, DuckDB.

These tests pin:

* Every name in ``SUPPORTED_BACKENDS`` resolves to an adapter class.
* Each adapter declares a coherent ``BackendCapabilities`` (no abstract
  method left unimplemented; ``name`` is set).
* Each new backend's ``DBConfig.url`` builds a sensible SQLAlchemy URL
  with the canonical port substituted when the user didn't pick one.
* ``is_connection_configured`` and ``is_database_pinned`` return the
  expected booleans for each new backend.
* ``_db_to_mapping`` / ``_db_from_mapping`` round-trip the new fields.
"""

from __future__ import annotations

import pytest

from amx.config import DBConfig, _db_from_mapping, _db_to_mapping, _normalize_db_host
from amx.db.adapters import SUPPORTED_BACKENDS, get_adapter
from amx.db.adapters.base import BackendCapabilities, DatabaseAdapter


def _fixture(backend: str) -> dict[str, object]:
    """Minimal DBConfig kwargs that satisfy is_connection_configured for *backend*."""
    return {
        "postgresql": {"host": "db.example.com", "user": "alice"},
        "snowflake": {"account": "acc.example", "user": "alice"},
        "databricks": {"host": "adb.example.com", "access_token": "tok"},
        "bigquery": {"project": "my-project"},
        "mysql": {"host": "mysql.example.com", "user": "alice", "password": "x"},
        "oracle": {
            "host": "ora.example.com",
            "user": "APP",
            "password": "x",
            "service_name": "XEPDB1",
        },
        "mssql": {"host": "mssql.example.com", "user": "sa", "password": "x"},
        "redshift": {
            "host": "cluster.xyz.eu-west-1.redshift.amazonaws.com",
            "user": "admin",
            "password": "x",
            "database": "dev",
        },
        "clickhouse": {"host": "ch.example.com", "user": "default"},
        "duckdb": {"database": "/tmp/x.duckdb"},
    }[backend]


# ── Registry ──────────────────────────────────────────────────────────────


def test_all_backends_resolve_through_get_adapter():
    """Every supported backend instantiates without a live connection."""
    for backend in SUPPORTED_BACKENDS:
        cfg = DBConfig(backend=backend, **_fixture(backend))
        adapter = get_adapter(cfg)
        assert isinstance(adapter, DatabaseAdapter)
        assert adapter.name == backend


def test_unknown_backend_raises_valueerror():
    cfg = DBConfig(backend="cassandra", host="x", user="y")
    with pytest.raises(ValueError, match="Unknown database backend"):
        get_adapter(cfg)


# ── Per-adapter contract ─────────────────────────────────────────────────


@pytest.mark.parametrize("backend", SUPPORTED_BACKENDS)
def test_adapter_declares_capabilities(backend: str):
    """Adapter must expose a BackendCapabilities instance."""
    cfg = DBConfig(backend=backend, **_fixture(backend))
    adapter = get_adapter(cfg)
    assert isinstance(adapter.capabilities, BackendCapabilities)


@pytest.mark.parametrize("backend", SUPPORTED_BACKENDS)
def test_adapter_implements_required_dml_templates(backend: str):
    """Each adapter's set_*_comment_sql must return either a SQL string
    or raise UnsupportedDatabaseOperation — never NotImplementedError
    from the base class."""
    cfg = DBConfig(backend=backend, **_fixture(backend))
    adapter = get_adapter(cfg)

    from amx.db.adapters.base import UnsupportedDatabaseOperation

    # Try each comment-write template and accept either a string or the
    # adapter's "I don't support this" sentinel.
    for fn, args in [
        (adapter.set_table_comment_sql, ("schema", "table", "TABLE")),
        (adapter.set_column_comment_sql, ("schema", "table", "col")),
        (adapter.set_schema_comment_sql, ("schema",)),
        (adapter.set_database_comment_sql, ()),
    ]:
        try:
            sql = fn(*args)
        except UnsupportedDatabaseOperation:
            continue  # explicit opt-out is fine
        assert isinstance(sql, str) and sql.strip(), f"{fn.__name__} returned empty SQL"


@pytest.mark.parametrize("backend", SUPPORTED_BACKENDS)
def test_adapter_implements_required_profiling_templates(backend: str):
    cfg = DBConfig(backend=backend, **_fixture(backend))
    adapter = get_adapter(cfg)
    fqn = adapter.fully_qualified_name("schema", "table")
    quoted = adapter.quote_identifier("col")
    assert isinstance(adapter.column_stats_sql(fqn, quoted), str)
    assert isinstance(adapter.column_sample_sql(fqn, quoted), str)


# ── New-backend URL building ─────────────────────────────────────────────


def test_mysql_url_builds_with_canonical_port():
    db = DBConfig(backend="mysql", host="db.example.com", user="alice", password="pw")
    assert db.url.startswith("mysql+pymysql://alice:pw@db.example.com:3306")


def test_mysql_url_includes_database_when_pinned():
    db = DBConfig(
        backend="mysql", host="db.example.com", user="alice", password="pw", database="warehouse"
    )
    assert db.url.endswith("/warehouse")


def test_oracle_url_prefers_service_name():
    db = DBConfig(
        backend="oracle",
        host="ora.example.com",
        user="APP",
        password="pw",
        service_name="XEPDB1",
    )
    assert "service_name=XEPDB1" in db.url
    assert db.url.startswith("oracle+oracledb://APP:pw@ora.example.com:1521")


def test_oracle_url_falls_back_to_database_as_sid():
    db = DBConfig(
        backend="oracle",
        host="ora.example.com",
        user="APP",
        password="pw",
        database="XE",
    )
    assert "service_name" not in db.url
    assert db.url.endswith("/XE")


def test_mssql_url_defaults_driver_and_encrypts():
    db = DBConfig(
        backend="mssql",
        host="mssql.example.com",
        user="sa",
        password="pw",
        database="analytics",
    )
    assert db.url.startswith("mssql+pyodbc://sa:pw@mssql.example.com:1433/analytics")
    assert "driver=ODBC+Driver+18+for+SQL+Server" in db.url
    assert "Encrypt=yes" in db.url


def test_mssql_url_can_disable_encrypt():
    db = DBConfig(
        backend="mssql",
        host="mssql.example.com",
        user="sa",
        password="pw",
        encrypt=False,
        trust_server_certificate=True,
    )
    assert "Encrypt=no" in db.url
    assert "TrustServerCertificate=yes" in db.url


def test_redshift_url_uses_canonical_port():
    db = DBConfig(
        backend="redshift",
        host="cluster.xxx.redshift.amazonaws.com",
        user="admin",
        password="pw",
        database="dev",
    )
    assert db.url.startswith(
        "redshift+redshift_connector://admin:pw@cluster.xxx.redshift.amazonaws.com:5439/dev"
    )


def test_clickhouse_url_secure_picks_https_and_8443():
    db = DBConfig(
        backend="clickhouse",
        host="ch.example.com",
        user="default",
        password="",
        database="analytics",
        secure=True,
    )
    assert db.url.startswith("clickhouse+https://default:@ch.example.com:8443/analytics")


def test_clickhouse_url_insecure_picks_http_and_8123():
    db = DBConfig(backend="clickhouse", host="ch.example.com", user="default")
    assert db.url.startswith("clickhouse+http://default:@ch.example.com:8123")


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("dbc-xxx.cloud.databricks.com", "dbc-xxx.cloud.databricks.com"),
        ("https://dbc-xxx.cloud.databricks.com", "dbc-xxx.cloud.databricks.com"),
        ("https://dbc-xxx.cloud.databricks.com/", "dbc-xxx.cloud.databricks.com"),
        ("http://dbc-xxx.cloud.databricks.com/sql/", "dbc-xxx.cloud.databricks.com"),
        ("  dbc-xxx.cloud.databricks.com  ", "dbc-xxx.cloud.databricks.com"),
        ("", ""),
        (None, ""),
    ],
)
def test_normalize_db_host_strips_scheme_and_trailing_slash(raw, expected):
    assert _normalize_db_host(raw) == expected


def test_snowflake_list_databases_runs_show_databases_and_filters_managed():
    """Snowflake adapter exposes a ``list_databases`` override (added in
    0.12.3) so the unified runtime database picker can present
    user-visible databases when the profile leaves the database field
    blank. Verify the override:
      - issues ``SHOW DATABASES``,
      - filters Snowflake-managed DBs (``SNOWFLAKE``,
        ``SNOWFLAKE_SAMPLE_DATA``) when other DBs are visible,
      - returns the unfiltered list when those are the only DBs visible
        (sandbox accounts).
    """
    from unittest.mock import MagicMock

    from amx.db.adapters.snowflake import SnowflakeAdapter

    cfg = DBConfig(backend="snowflake", account="acc.example", user="alice")
    adapter = SnowflakeAdapter(cfg)

    def _engine(rows):
        engine = MagicMock()
        cm = engine.connect.return_value.__enter__.return_value
        # rows yielded by SHOW DATABASES: Snowflake returns at least
        # (created_on, name, ...). The adapter prefers ``mapping['name']``
        # when available — supply both shapes via MagicMock attrs.
        result = MagicMock()
        result.fetchall.return_value = rows
        cm.execute.return_value = result
        return engine

    class _Row:
        def __init__(self, name: str) -> None:
            self._name = name
            self._mapping = {"name": name}

        def __getitem__(self, idx):
            # Snowflake column 1 is ``name``.
            return [None, self._name][idx]

    # Mixed: managed DBs + user DBs → filter the managed ones.
    mixed = adapter.list_databases(_engine([_Row("APP"), _Row("SNOWFLAKE"), _Row("METRICS")]))
    assert "SNOWFLAKE" not in mixed
    assert "APP" in mixed
    assert "METRICS" in mixed

    # Only managed DBs visible (fresh sandbox) → return them unfiltered.
    only_managed = adapter.list_databases(
        _engine([_Row("SNOWFLAKE"), _Row("SNOWFLAKE_SAMPLE_DATA")])
    )
    assert only_managed == ["SNOWFLAKE", "SNOWFLAKE_SAMPLE_DATA"]


def test_databricks_url_normalizes_host_with_trailing_slash():
    """Regression: pasting the full workspace URL with a trailing slash
    used to crash schema listing with ``invalid literal for int() with
    base 10: ''`` because ``host:443`` became ``host/:443``."""
    db = DBConfig(
        backend="databricks",
        host="https://dbc-xxx.cloud.databricks.com/",
        access_token="tok",
    )
    assert "@dbc-xxx.cloud.databricks.com:443" in db.url
    assert "https://" not in db.url.split("@", 1)[1]


def test_duckdb_url_uses_path():
    db = DBConfig(backend="duckdb", database="/tmp/warehouse.duckdb")
    assert db.url == "duckdb:////tmp/warehouse.duckdb"


def test_duckdb_url_in_memory_when_blank():
    db = DBConfig(backend="duckdb", database="")
    assert db.url == "duckdb:///:memory:"


# ── is_connection_configured / is_database_pinned ────────────────────────


@pytest.mark.parametrize(
    "backend",
    ("mysql", "oracle", "mssql", "redshift", "clickhouse", "duckdb"),
)
def test_new_backend_minimum_fields_pass_connection_configured(backend: str):
    db = DBConfig(backend=backend, **_fixture(backend))
    assert db.is_connection_configured() is True


def test_oracle_unpinned_when_neither_service_name_nor_database():
    db = DBConfig(backend="oracle", host="ora", user="APP", password="x")
    assert db.is_connection_configured() is False  # service_name OR database required
    assert db.is_database_pinned() is False


def test_duckdb_in_memory_counts_as_pinned():
    """DuckDB has no separate "pick a DB later" flow — :memory: or a
    file path are both legitimate active choices."""
    db = DBConfig(backend="duckdb", database="")
    assert db.is_database_pinned() is True


# ── (De)serialize round-trip ─────────────────────────────────────────────


@pytest.mark.parametrize(
    "backend",
    ("mysql", "oracle", "mssql", "redshift", "clickhouse", "duckdb"),
)
def test_new_backend_roundtrips_through_mapping(backend: str):
    original = DBConfig(backend=backend, **_fixture(backend))
    restored = _db_from_mapping(_db_to_mapping(original))
    assert restored.backend == original.backend
    assert restored.url == original.url
    assert restored.display_summary == original.display_summary


# ── MissingDriverError ───────────────────────────────────────────────────


def test_missing_driver_error_class_exists():
    """The MissingDriverError class is part of the public adapters API
    so callers can catch it specifically and surface install hints."""
    from amx.db.adapters import MissingDriverError

    assert issubclass(MissingDriverError, ImportError)


# ── Connector list_* gating ──────────────────────────────────────────────


def test_connector_extended_listings_short_circuit_on_unsupported_capabilities():
    """When a backend has a capability flag set to False, the matching
    connector accessor must return [] without ever touching the engine
    (so adapters without the upstream support don't blow up)."""
    from amx.db.connector import DatabaseConnector

    # DuckDB has stored_procedures=False, dictionaries=False, datashares=False.
    cfg = DBConfig(backend="duckdb", database=":memory:")
    conn = DatabaseConnector(cfg)
    # These should hit the capability gate, never the engine.
    assert conn.list_stored_procedures("main") == []
    assert conn.list_dictionaries() == []
    assert conn.list_datashares() == []
    assert conn.list_packages("main") == []
    assert conn.list_synonyms("main") == []
