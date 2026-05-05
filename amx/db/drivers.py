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

#: backend label → (import probe, pip targets). The probe mirrors
#: what each adapter actually does ``import`` at engine-create time.
#: pip targets include any companion sqlalchemy plugin so the
#: post-install ``create_engine`` call can resolve the dialect.
BACKEND_DRIVER_PACKAGES: dict[str, tuple[str, list[str]]] = {
    "postgresql": ("psycopg2", ["psycopg2-binary"]),
    "snowflake": (
        "snowflake.connector",
        ["snowflake-sqlalchemy", "snowflake-connector-python"],
    ),
    "databricks": (
        "databricks.sql",
        ["databricks-sqlalchemy", "databricks-sql-connector"],
    ),
    "bigquery": (
        "google.cloud.bigquery",
        ["sqlalchemy-bigquery", "google-cloud-bigquery"],
    ),
    "mysql": ("pymysql", ["pymysql", "cryptography"]),
    "oracle": ("oracledb", ["oracledb"]),
    "mssql": ("pyodbc", ["pyodbc"]),
    "redshift": (
        "redshift_connector",
        ["redshift_connector", "sqlalchemy-redshift"],
    ),
    "clickhouse": (
        "clickhouse_connect",
        ["clickhouse-connect", "clickhouse-sqlalchemy"],
    ),
    "duckdb": ("duckdb", ["duckdb-engine", "duckdb"]),
}


def ensure_backend_driver(backend: str) -> None:
    """Make sure *backend*'s driver packages are importable.

    No-op when the backend is unrecognised (custom adapters via
    third-party packages are the user's responsibility) or when the
    driver is already installed. On first hit, runs the auto-install
    path with pip's progress streaming to the terminal.
    """
    entry = BACKEND_DRIVER_PACKAGES.get(backend)
    if not entry:
        return
    probe, pip_targets = entry

    from amx.utils.optional_deps import ensure

    # The probe + first pip target form the "known" pair we cache;
    # additional pip targets (sqlalchemy plugins, cryptography, …)
    # are appended via plain pip names so they install even when the
    # probe alone is already present.
    spec_list: list = [(probe, pip_targets[0])]
    spec_list.extend(pip_targets[1:])
    ensure(spec_list, feature=f"{backend} backend")
