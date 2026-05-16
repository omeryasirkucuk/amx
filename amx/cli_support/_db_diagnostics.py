"""Diagnostic helpers for the ``amx db`` wizard.

Extracted from :mod:`amx.cli_support.commands.db`. The four functions
share the role of giving the user actionable hints when their
environment is the obstacle rather than the wizard input.

``db.py`` re-exports each name so call sites and existing test
imports (``tests/test_mssql_system_prereq_hint.py``) keep working.
"""

from __future__ import annotations

import os
from pathlib import Path

from amx.utils.console import error, info

_BACKEND_DRIVER_PROBES: dict[str, tuple[str, str]] = {
    "postgresql": ("postgresql", "psycopg2"),
    "snowflake": ("snowflake", "snowflake.connector"),
    "databricks": ("databricks", "databricks.sql"),
    "bigquery": ("bigquery", "google.cloud.bigquery"),
    "mysql": ("mysql", "pymysql"),
    "oracle": ("oracle", "oracledb"),
    "mssql": ("mssql", "pyodbc"),
    "redshift": ("redshift", "redshift_connector"),
    "clickhouse": ("clickhouse", "clickhouse_connect"),
    "duckdb": ("duckdb", "duckdb"),
}


def _print_system_prereq_hint(backend: str) -> None:
    """Surface platform-specific system-package install instructions.

    Pure-Python drivers (psycopg2-binary, snowflake-sqlalchemy, …)
    pip-install fine on every supported platform. ODBC-based drivers
    (MSSQL via pyodbc) require a separate system package — without it
    the connection fails with ``Can't open lib 'ODBC Driver 18 for SQL
    Server'`` even after ``pip install amx-cli[mssql]`` succeeds.
    Print the right command for the user's OS so they don't have to
    google the error.
    """
    import platform

    if backend != "mssql":
        return

    system = platform.system()
    if system == "Darwin":
        info(
            "Note: macOS also needs the Microsoft ODBC system driver. "
            "If the connection fails with 'Can't open lib', run:\n"
            "    brew tap microsoft/mssql-release "
            "https://github.com/Microsoft/homebrew-mssql-release\n"
            "    brew install msodbcsql18 mssql-tools18"
        )
    elif system == "Linux":
        info(
            "Note: Linux also needs the Microsoft ODBC system driver. "
            "See https://learn.microsoft.com/sql/connect/odbc/linux-mac/"
            "installing-the-microsoft-odbc-driver-for-sql-server for "
            "your distro's package (``msodbcsql18`` on Debian / RHEL)."
        )
    elif system == "Windows":
        info(
            "Note: Windows installers for the Microsoft ODBC driver "
            "are bundled with SSMS or downloadable from "
            "https://learn.microsoft.com/sql/connect/odbc/."
        )


def _offer_to_install_backend_driver(backend: str) -> None:
    """Auto-install *backend*'s driver during the ``/add-db-profile`` wizard.

    Pre-0.12.9 this asked a Y/n prompt and ran ``pip install
    'amx-cli[<extra>]'``. The Y/n was friction (the user had just
    picked the backend; "do you want it to work?" is not a useful
    question), and the wizard's pre-install was duplicated by
    ``DatabaseConnector.__init__`` which would have triggered the
    same install on first use anyway. Calling
    ``ensure_backend_driver`` here pre-warms the same path so the
    wizard's "test connection" step at the end of the flow finds the
    driver already loaded.
    """
    probe = _BACKEND_DRIVER_PROBES.get(backend)
    if not probe:
        return
    from amx.db.drivers import ensure_backend_driver

    _print_system_prereq_hint(backend)

    try:
        ensure_backend_driver(backend)
    except RuntimeError as exc:
        # Surface the failure but don't abort the wizard — the user
        # can still finish entering connection details and AMX will
        # re-attempt the install (or report the same hint) on first
        # connect.
        error(str(exc))


def _is_databricks_tls_failure(message: str) -> bool:
    msg = (message or "").lower()
    return any(
        token in msg
        for token in (
            "tls",
            "certificate",
            "ssl",
            "trusted ca bundle",
            "self-signed",
        )
    )


def _env_trusted_ca_candidate() -> tuple[str, str] | None:
    for env_name in (
        "AMX_DATABRICKS_TRUSTED_CA_FILE",
        "DATABRICKS_TRUSTED_CA_FILE",
        "REQUESTS_CA_BUNDLE",
        "SSL_CERT_FILE",
    ):
        raw = os.environ.get(env_name, "").strip()
        if not raw:
            continue
        resolved = Path(os.path.expandvars(os.path.expanduser(raw)))
        if resolved.is_file():
            return env_name, str(resolved)
    return None
