"""View-DDL lineage extraction.

Primary signal — works without FK metadata. Cache-first by construction:
the extractor never opens a wire connection in ``cache_only`` mode and
only fetches/parses in ``db_fill`` mode after the service layer has
confirmed with the user.

Sqlglot column-lineage is computed once at cache-fill time and stored
verbatim in ``view_definitions_cache.parsed_lineage_json``. Subsequent
reads emit edges with zero parsing.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from amx.lineage import store as lineage_store
from amx.lineage.types import (
    ColumnRef,
    CostHint,
    Edge,
    ExtractMode,
    ExtractResult,
    Scope,
    ScopeFragment,
)

# Adapter dialect → sqlglot dialect tag. Adapters that already match
# sqlglot's name are absent from this map (sqlglot accepts the same
# string).
_DIALECT_MAP = {
    "postgresql": "postgres",
    "redshift": "redshift",
    "mssql": "tsql",
    "databricks": "databricks",
    "mysql": "mysql",
    "snowflake": "snowflake",
    "bigquery": "bigquery",
    "duckdb": "duckdb",
    "clickhouse": "clickhouse",
    "oracle": "oracle",
    "trino": "trino",
    "presto": "presto",
}

# Per-view fetch is conservatively budgeted at 0.04s. Real warehouses
# vary; this is just a cost-hint shown to the user before they confirm.
_PER_VIEW_SECONDS = 0.04

# Adapter facade injected by the service layer. The factory returns a
# small struct with everything the extractor needs to do one db_fill
# round-trip: the engine, the adapter (for list_views_with_definitions),
# and the backend dialect tag.
ConnectorFactory = Callable[[str], "ConnectorHandle | None"]


class ConnectorHandle:
    """Minimal handle the extractor receives — see service layer."""

    def __init__(self, engine: Any, adapter: Any, backend: str) -> None:
        self.engine = engine
        self.adapter = adapter
        self.backend = backend


class ViewDDLExtractor:
    name = "view_ddl"

    def __init__(
        self,
        *,
        connector_factory: ConnectorFactory | None = None,
        ttl_seconds: float = lineage_store.DEFAULT_VIEW_CACHE_TTL_SECONDS,
    ) -> None:
        self._connector_factory = connector_factory
        self._ttl_seconds = ttl_seconds

    def extract(
        self,
        *,
        hs: Any,
        scope: Scope,
        mode: ExtractMode = "cache_only",
    ) -> ExtractResult:
        database = scope.anchor.database
        schema = scope.anchor.schema
        if not schema:
            return ExtractResult()

        cached = lineage_store.lookup_view_definitions(
            hs,
            db_profile=scope.profile,
            database=database,
            schema=schema,
        )
        if cached:
            return ExtractResult(
                edges=list(_edges_from_cached(cached, scope, database, schema)),
                cache_status="hit",
            )

        # Cache empty/stale for this scope.
        if mode == "cache_only":
            return ExtractResult(
                edges=[],
                cache_status="miss",
                missing_scope=[ScopeFragment(database=database, schema=schema)],
                estimated_db_cost=CostHint(),  # unknown until we hit the DB
            )

        # db_fill: open a connection, list views, parse, persist, emit.
        if self._connector_factory is None:
            return ExtractResult(
                edges=[],
                cache_status="miss",
                missing_scope=[ScopeFragment(database=database, schema=schema)],
            )
        handle = self._connector_factory(scope.profile)
        if handle is None:
            return ExtractResult(
                edges=[],
                cache_status="miss",
                missing_scope=[ScopeFragment(database=database, schema=schema)],
            )

        view_payloads = _fetch_view_definitions(handle, schema)
        dialect = _DIALECT_MAP.get(handle.backend, handle.backend)
        entries = list(_build_cache_entries(view_payloads, dialect))
        lineage_store.upsert_view_definitions(
            hs,
            db_profile=scope.profile,
            database=database,
            schema=schema,
            entries=entries,
            ttl_seconds=self._ttl_seconds,
        )
        return ExtractResult(
            edges=list(
                _edges_from_cached(
                    lineage_store.lookup_view_definitions(
                        hs,
                        db_profile=scope.profile,
                        database=database,
                        schema=schema,
                    ),
                    scope,
                    database,
                    schema,
                )
            ),
            cache_status="hit",
            estimated_db_cost=CostHint(
                estimated_views=len(view_payloads),
                estimated_seconds=len(view_payloads) * _PER_VIEW_SECONDS,
            ),
        )


def _fetch_view_definitions(handle: ConnectorHandle, schema: str) -> list[dict[str, Any]]:
    """Wrapper so adapters that don't implement the method don't crash."""
    method = getattr(handle.adapter, "list_views_with_definitions", None)
    if method is None:
        return []
    try:
        return list(method(handle.engine, schema) or [])
    except Exception:
        return []


def _build_cache_entries(
    view_payloads: list[dict[str, Any]],
    dialect: str,
) -> list[dict[str, Any]]:
    """Parse each DDL with sqlglot, build per-view cache rows."""
    out: list[dict[str, Any]] = []
    parser = _load_sqlglot_lineage()
    for vp in view_payloads:
        view_name = str(vp.get("name") or "")
        ddl = str(vp.get("definition") or "")
        if not view_name or not ddl:
            continue
        parsed, status, error = _parse_view_lineage(parser, ddl, dialect, view_name)
        out.append(
            {
                "view_name": view_name,
                "ddl_text": ddl,
                "dialect": dialect,
                "parsed_lineage": parsed,
                "parse_status": status,
                "parse_error": error,
            }
        )
    return out


def _parse_view_lineage(
    parser: Any | None,
    ddl: str,
    dialect: str,
    view_name: str,
) -> tuple[list[dict[str, Any]] | None, str, str]:
    """Walk the SELECT body and collect (target_alias, [source columns]) pairs.

    We deliberately avoid ``sqlglot.lineage.lineage`` here — without a real
    catalog schema it leaks table names into the leaf set and the resulting
    edges are noisy. The simpler walk: for each SELECT projection, find the
    nested ``Column`` references that feed it. Aliased ``FROM`` / ``JOIN``
    targets resolve to their underlying table name when present.
    """
    sqlglot = _load_sqlglot_module()
    if sqlglot is None:
        return None, "unsupported_dialect", "sqlglot not installed"
    try:
        select_stmt = _select_from_ddl(sqlglot, ddl, dialect)
        if select_stmt is None:
            return None, "parse_failed", "could not isolate SELECT in DDL"

        alias_map = _alias_to_table_map(sqlglot, select_stmt)
        out: list[dict[str, Any]] = []
        for projection in _select_projections(select_stmt):
            alias = projection.alias_or_name
            if not alias:
                continue
            sources = _collect_source_columns(sqlglot, projection, alias_map)
            out.append({"target": str(alias), "sources": sources})
        return out, "ok", ""
    except Exception as exc:  # defensive
        return None, "parse_failed", str(exc).splitlines()[0][:200]


def _select_from_ddl(sqlglot: Any, ddl: str, dialect: str) -> Any | None:
    """Pull the SELECT body out of CREATE VIEW or accept a bare SELECT."""
    try:
        tree = sqlglot.parse_one(ddl, dialect=dialect)
    except Exception:
        try:
            tree = sqlglot.parse_one(ddl)
        except Exception:
            return None
    expression = (
        tree.expression if hasattr(tree, "expression") and tree.expression is not None else tree
    )
    return expression


def _select_projections(select_stmt: Any) -> list[Any]:
    """Best-effort: return the SELECT's projection expressions."""
    try:
        return list(select_stmt.expressions or [])
    except Exception:
        return []


def _alias_to_table_map(sqlglot: Any, select_stmt: Any) -> dict[str, str]:
    """Map ``alias`` → ``table_name`` for FROM / JOIN sources of *select_stmt*."""
    exp = sqlglot.exp
    out: dict[str, str] = {}
    try:
        for table in select_stmt.find_all(exp.Table):
            alias = table.alias_or_name
            base = table.name
            if alias:
                out[str(alias)] = str(base or alias)
    except Exception:
        return {}
    return out


def _collect_source_columns(
    sqlglot: Any,
    projection: Any,
    alias_map: dict[str, str],
) -> list[dict[str, str]]:
    """Return ``[{table, column}, …]`` for every ``Column`` under ``projection``."""
    exp = sqlglot.exp
    seen: set[tuple[str, str]] = set()
    out: list[dict[str, str]] = []
    # When the SELECT has exactly one underlying table and a column has
    # no explicit qualifier, attribute it to that table — otherwise the
    # source FQN is partial and downstream edges never materialise.
    default_table = ""
    distinct_tables = {v for v in alias_map.values() if v}
    if len(distinct_tables) == 1:
        default_table = next(iter(distinct_tables))
    try:
        for col in projection.find_all(exp.Column):
            column_name = str(col.name or "")
            if not column_name:
                continue
            raw_table = str(col.table or "")
            resolved_table = alias_map.get(raw_table, raw_table) or default_table
            key = (resolved_table, column_name)
            if key in seen:
                continue
            seen.add(key)
            out.append({"table": resolved_table, "column": column_name})
    except Exception:
        return []
    return out


def _load_sqlglot_module() -> Any | None:
    try:
        import sqlglot  # type: ignore

        return sqlglot
    except ImportError:
        # sqlglot is in the optional ``lineage`` extra. Try to
        # auto-install via amx.utils.optional_deps so first-time users
        # don't hit a hard wall — the registered bundle prompts before
        # any large download.
        try:
            from amx.utils.optional_deps import ensure

            ensure("lineage", feature="/lineage view-DDL parser")
            import sqlglot  # type: ignore

            return sqlglot
        except Exception:
            return None
    except Exception:
        return None


def _load_sqlglot_lineage() -> Any | None:
    """Kept for backwards-compat with the parser parameter; returns sqlglot
    when available so the legacy call sites continue to short-circuit."""
    return _load_sqlglot_module()


def _edges_from_cached(
    cached_rows: list[dict[str, Any]],
    scope: Scope,
    database: str,
    schema: str,
) -> Any:
    """Emit ``Edge``s from already-parsed cache rows."""
    for row in cached_rows:
        if row.get("parse_status") != "ok":
            continue
        parsed = row.get("parsed_lineage") or []
        view_name = row.get("view_name") or ""
        for col_entry in parsed:
            target_col = str(col_entry.get("target") or "")
            if not target_col:
                continue
            target_ref = ColumnRef(
                database=database, schema=schema, table=view_name, column=target_col
            )
            for src in col_entry.get("sources") or []:
                src_table = str(src.get("table") or "")
                src_col = str(src.get("column") or "")
                if not src_table or not src_col:
                    continue
                yield Edge(
                    source=ColumnRef(
                        database=database, schema=schema, table=src_table, column=src_col
                    ),
                    target=target_ref,
                    relationship_type="lineage_view_ddl",
                    extractor="view_ddl",
                    confidence=1.0,
                    evidence=f"view {schema}.{view_name}",
                )


__all__ = ["ViewDDLExtractor", "ConnectorHandle", "ConnectorFactory"]
