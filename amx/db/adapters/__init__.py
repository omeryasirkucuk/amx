"""Database backend adapters for AMX."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from amx.config import DBConfig
    from amx.db.adapters.base import DatabaseAdapter

_BACKEND_REGISTRY: dict[str, type["DatabaseAdapter"]] = {}

SUPPORTED_BACKENDS = ("postgresql", "snowflake", "databricks", "bigquery")


def _ensure_registry() -> None:
    if _BACKEND_REGISTRY:
        return
    from amx.db.adapters.bigquery import BigQueryAdapter
    from amx.db.adapters.databricks import DatabricksAdapter
    from amx.db.adapters.postgresql import PostgreSQLAdapter
    from amx.db.adapters.snowflake import SnowflakeAdapter

    _BACKEND_REGISTRY["postgresql"] = PostgreSQLAdapter
    _BACKEND_REGISTRY["snowflake"] = SnowflakeAdapter
    _BACKEND_REGISTRY["databricks"] = DatabricksAdapter
    _BACKEND_REGISTRY["bigquery"] = BigQueryAdapter


def get_adapter(cfg: "DBConfig") -> "DatabaseAdapter":
    """Return the correct adapter instance for *cfg.backend*."""
    _ensure_registry()
    backend = getattr(cfg, "backend", "postgresql") or "postgresql"
    cls = _BACKEND_REGISTRY.get(backend)
    if cls is None:
        raise ValueError(
            f"Unknown database backend {backend!r}. "
            f"Supported: {', '.join(SUPPORTED_BACKENDS)}"
        )
    return cls(cfg)
