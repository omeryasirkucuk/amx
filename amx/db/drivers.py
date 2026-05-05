"""Canonical mapping from a backend label to its driver packages.

Two paths used to know about backend drivers separately:

* ``cli_support/commands/db.py:_BACKEND_DRIVER_PROBES`` — used by the
  ``/add-db-profile`` wizard to offer an inline install.
* The DatabaseConnector path — which would simply explode with
  ``ModuleNotFoundError: No module named 'psycopg2'`` when a profile
  was already saved (e.g. AMX Studio's "Live database" panel hitting
  a configured Postgres profile on a fresh slim install).

This module is the single source of truth. ``ensure_backend_driver``
silently auto-installs the matching pip targets via
``amx.utils.optional_deps.ensure`` so every code path that touches a
configured backend behaves the same way: the user never sees a raw
import error, and the install runs once with pip's progress streaming
to the terminal.
"""

from __future__ import annotations

#: backend label → list of ``(importable_module, pip_target)`` pairs.
#: Each pip target needs its OWN importable probe — passing bare pip
#: names that aren't valid Python module identifiers (anything with a
#: hyphen, e.g. ``databricks-sql-connector``) makes ``find_spec`` return
#: ``None`` on every launch and re-runs the install subprocess even
#: when pip already has the package satisfied.
BACKEND_DRIVER_PACKAGES: dict[str, list[tuple[str, str]]] = {
    "postgresql": [("psycopg2", "psycopg2-binary")],
    "snowflake": [
        ("snowflake.connector", "snowflake-connector-python"),
        ("snowflake.sqlalchemy", "snowflake-sqlalchemy"),
    ],
    "databricks": [
        ("databricks.sql", "databricks-sql-connector"),
        ("databricks.sqlalchemy", "databricks-sqlalchemy"),
    ],
    "bigquery": [
        ("google.cloud.bigquery", "google-cloud-bigquery"),
        ("sqlalchemy_bigquery", "sqlalchemy-bigquery"),
    ],
    "mysql": [("pymysql", "pymysql"), ("cryptography", "cryptography")],
    "oracle": [("oracledb", "oracledb")],
    "mssql": [("pyodbc", "pyodbc")],
    "redshift": [
        ("redshift_connector", "redshift_connector"),
        ("sqlalchemy_redshift", "sqlalchemy-redshift"),
    ],
    "clickhouse": [
        ("clickhouse_connect", "clickhouse-connect"),
        ("clickhouse_sqlalchemy", "clickhouse-sqlalchemy"),
    ],
    "duckdb": [("duckdb", "duckdb"), ("duckdb_engine", "duckdb-engine")],
}


def ensure_backend_driver(backend: str) -> None:
    """Make sure *backend*'s driver packages are importable.

    No-op when the backend is unrecognised (custom adapters via
    third-party packages are the user's responsibility) or when the
    driver is already installed. On first hit, runs the auto-install
    path with pip's progress streaming to the terminal.
    """
    pairs = BACKEND_DRIVER_PACKAGES.get(backend)
    if not pairs:
        return

    from amx.utils.optional_deps import ensure

    ensure(list(pairs), feature=f"{backend} backend")
