"""Database backend adapters for AMX."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from amx.config import DBConfig
    from amx.db.adapters.base import DatabaseAdapter

_BACKEND_REGISTRY: dict[str, type[DatabaseAdapter]] = {}

SUPPORTED_BACKENDS = (
    "postgresql",
    "snowflake",
    "databricks",
    "bigquery",
    "mysql",
    "oracle",
    "mssql",
    "redshift",
    "clickhouse",
    "duckdb",
)

# Each backend lives in its own optional-dependencies extra in
# pyproject.toml. When a user picks a backend without installing the
# driver, ``_ensure_registry()`` catches the ImportError and raises an
# actionable ``MissingDriverError`` instead of a raw ``ModuleNotFoundError``.
_BACKEND_EXTRAS: dict[str, str] = {
    "postgresql": "postgresql",
    "snowflake": "snowflake",
    "databricks": "databricks",
    "bigquery": "bigquery",
    "mysql": "mysql",
    "oracle": "oracle",
    "mssql": "mssql",
    "redshift": "redshift",
    "clickhouse": "clickhouse",
    "duckdb": "duckdb",
}


class MissingDriverError(ImportError):
    """Raised when a backend's optional driver dependency is not installed.

    Surfaces the concrete `pip install amx-cli[<extra>]` remediation so the
    user is never left staring at a generic ``ModuleNotFoundError``.
    """


def _import_adapter(backend: str) -> type[DatabaseAdapter]:
    """Import the adapter class for *backend*, translating ImportError
    into a user-actionable :class:`MissingDriverError`.
    """
    try:
        if backend == "postgresql":
            from amx.db.adapters.postgresql import PostgreSQLAdapter

            return PostgreSQLAdapter
        if backend == "snowflake":
            from amx.db.adapters.snowflake import SnowflakeAdapter

            return SnowflakeAdapter
        if backend == "databricks":
            from amx.db.adapters.databricks import DatabricksAdapter

            return DatabricksAdapter
        if backend == "bigquery":
            from amx.db.adapters.bigquery import BigQueryAdapter

            return BigQueryAdapter
        if backend == "mysql":
            from amx.db.adapters.mysql import MySQLAdapter

            return MySQLAdapter
        if backend == "oracle":
            from amx.db.adapters.oracle import OracleAdapter

            return OracleAdapter
        if backend == "mssql":
            from amx.db.adapters.mssql import MSSQLAdapter

            return MSSQLAdapter
        if backend == "redshift":
            from amx.db.adapters.redshift import RedshiftAdapter

            return RedshiftAdapter
        if backend == "clickhouse":
            from amx.db.adapters.clickhouse import ClickHouseAdapter

            return ClickHouseAdapter
        if backend == "duckdb":
            from amx.db.adapters.duckdb import DuckDBAdapter

            return DuckDBAdapter
    except ImportError as exc:
        extra = _BACKEND_EXTRAS.get(backend, backend)
        raise MissingDriverError(
            f"The {backend!r} backend requires its optional driver. Install it with:\n"
            f"    pip install 'amx-cli[{extra}]'\n"
            f"(Underlying import error: {exc})"
        ) from exc
    raise ValueError(
        f"Unknown database backend {backend!r}. Supported: {', '.join(SUPPORTED_BACKENDS)}"
    )


def _ensure_registry() -> None:
    """Lazy-fill the registry on first use.

    Each adapter import is wrapped so a missing driver only fails the
    backends the user actually picks — not every import on startup.
    """
    if _BACKEND_REGISTRY:
        return
    # Insert sentinels so ``get_adapter`` knows which backends are valid
    # without forcing a driver import for every one of them at startup.
    for name in SUPPORTED_BACKENDS:
        _BACKEND_REGISTRY[name] = _AdapterPlaceholder  # type: ignore[assignment]


class _AdapterPlaceholder:
    """Marker type — replaced with the real adapter class on first use."""


def get_adapter(cfg: DBConfig) -> DatabaseAdapter:
    """Return the correct adapter instance for *cfg.backend*.

    Imports the adapter module on demand so users only pay the driver
    cost for the backends they actually use. Missing-driver errors are
    translated into :class:`MissingDriverError` with a concrete
    ``pip install amx-cli[<extra>]`` hint.
    """
    _ensure_registry()
    backend = getattr(cfg, "backend", "postgresql") or "postgresql"
    if backend not in _BACKEND_REGISTRY:
        raise ValueError(
            f"Unknown database backend {backend!r}. Supported: {', '.join(SUPPORTED_BACKENDS)}"
        )
    cls = _BACKEND_REGISTRY[backend]
    if cls is _AdapterPlaceholder:
        cls = _import_adapter(backend)
        _BACKEND_REGISTRY[backend] = cls
    return cls(cfg)
