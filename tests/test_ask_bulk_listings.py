"""Phase 4 of the perf plan: ``list_volumes`` and ``find_table_by_name``
prefer a bulk INFORMATION_SCHEMA query over per-schema enumeration.

These tools used to walk every schema in the catalog and issue one
``SHOW VOLUMES`` / ``SHOW TABLES`` per schema — 50 schemas = 50
round-trips. Bulk variants on the adapter let the connector ask the
question once, and we only fall back to the per-schema loop when
the adapter / role can't serve the bulk path.
"""

from __future__ import annotations

from amx.config import DBConfig
from amx.db.adapters.databricks import DatabricksAdapter
from amx.db.adapters.postgresql import PostgreSQLAdapter
from amx.db.connector import AssetKind, DatabaseConnector


def test_default_bulk_methods_return_none() -> None:
    """A concrete adapter that didn't override the bulk methods returns None."""
    # Postgres adapter inherits the base default (None) — the per-schema
    # ``list_tables`` loop is already cheap on Postgres and doesn't need
    # bulking.
    adapter = PostgreSQLAdapter(DBConfig(backend="postgresql", host="x", user="u"))
    assert adapter.list_volumes_bulk(engine=None, catalog="cat") is None  # type: ignore[arg-type]
    assert adapter.list_assets_bulk(engine=None, catalog="cat") is None  # type: ignore[arg-type]


def test_databricks_list_volumes_bulk_query_shape() -> None:
    """The Databricks bulk volume query targets the catalog's own
    information_schema.volumes (per-catalog, not the system aggregator)."""
    captured: list[tuple[str, dict]] = []

    class FakeConn:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, sql, params=None):
            captured.append((str(sql), dict(params or {})))

            class R:
                def fetchall(self_inner):
                    return [
                        type(
                            "Row",
                            (),
                            {
                                "_mapping": {
                                    "volume_schema": "sales",
                                    "volume_name": "raw_landing",
                                    "volume_type": "MANAGED",
                                    "comment": "raw S3 drop",
                                }
                            },
                        )()
                    ]

            return R()

    class FakeEngine:
        def connect(self):
            return FakeConn()

    cfg = DBConfig(backend="databricks", host="x", access_token="x")
    adapter = DatabricksAdapter(cfg)

    out = adapter.list_volumes_bulk(FakeEngine(), "prod_catalog")  # type: ignore[arg-type]

    assert out == [
        {
            "schema": "sales",
            "name": "raw_landing",
            "type": "managed",
            "comment": "raw S3 drop",
        }
    ]
    assert len(captured) == 1
    sql, params = captured[0]
    assert "`prod_catalog`.information_schema.volumes" in sql
    assert "system.information_schema" not in sql
    assert params == {}


def test_databricks_list_assets_bulk_normalises_kinds() -> None:
    """``BASE TABLE`` / ``VIEW`` / ``MATERIALIZED VIEW`` map to AssetKind."""
    rows_data = [
        {"table_schema": "ops", "table_name": "orders", "table_type": "BASE TABLE"},
        {"table_schema": "ops", "table_name": "v_orders", "table_type": "VIEW"},
        {"table_schema": "ops", "table_name": "mv_orders", "table_type": "MATERIALIZED VIEW"},
        # System schema must be filtered out.
        {
            "table_schema": "information_schema",
            "table_name": "tables",
            "table_type": "VIEW",
        },
    ]

    class FakeConn:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, sql, params=None):
            class R:
                def fetchall(self_inner):
                    return [type("Row", (), {"_mapping": d})() for d in rows_data]

            return R()

    class FakeEngine:
        def connect(self):
            return FakeConn()

    cfg = DBConfig(backend="databricks", host="x", access_token="x")
    adapter = DatabricksAdapter(cfg)

    raw = adapter.list_assets_bulk(FakeEngine(), "prod")  # type: ignore[arg-type]
    assert raw == [
        ("ops", "orders", "BASE TABLE"),
        ("ops", "v_orders", "VIEW"),
        ("ops", "mv_orders", "MATERIALIZED VIEW"),
    ]
    # information_schema row was dropped by the system_schemas() filter.

    # Connector wrapper normalises raw kind strings to AssetKind.
    conn = DatabaseConnector.__new__(DatabaseConnector)
    conn.cfg = cfg
    conn._adapter = adapter
    conn._engine = FakeEngine()  # type: ignore[assignment]
    out = conn.list_assets_bulk("prod")
    assert out == [
        ("ops", "orders", AssetKind.TABLE),
        ("ops", "v_orders", AssetKind.VIEW),
        ("ops", "mv_orders", AssetKind.MATERIALIZED_VIEW),
    ]


def test_connector_list_volumes_bulk_returns_none_when_no_capability() -> None:
    """If the backend doesn't claim the volumes capability, bulk returns None."""
    cfg = DBConfig(backend="postgresql", host="x", user="u")
    conn = DatabaseConnector(cfg)
    # Postgres adapter doesn't expose volumes — bulk path returns None
    # so the caller falls back to its existing per-schema loop.
    assert conn.list_volumes_bulk("anything") is None


def test_connector_list_assets_bulk_returns_none_for_default_adapter() -> None:
    """No-op when the adapter doesn't override list_assets_bulk."""
    cfg = DBConfig(backend="postgresql", host="x", user="u")
    conn = DatabaseConnector(cfg)
    assert conn.list_assets_bulk("anything") is None
