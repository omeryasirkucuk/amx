"""Manual metadata inspection, editing, and coverage helpers."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from amx.config import AMXConfig
from amx.db.connector import AssetKind

ErrorFn = Callable[[str], None]


@dataclass
class MetadataCoverage:
    schema: str
    assets: int = 0
    assets_with_comments: int = 0
    columns: int = 0
    columns_with_comments: int = 0

    @property
    def asset_percent(self) -> float:
        return _percent(self.assets_with_comments, self.assets)

    @property
    def column_percent(self) -> float:
        return _percent(self.columns_with_comments, self.columns)


def _percent(part: int, total: int) -> float:
    return (float(part) / float(total) * 100.0) if total else 0.0


def resolve_schema_arg(cfg: AMXConfig, schema: str | None, *, error: ErrorFn) -> str | None:
    resolved = schema or cfg.current_schema
    if not resolved:
        error("Schema is required. Use /schema <name> in /db or pass a schema.")
        return None
    return resolved


def resolve_table_arg(cfg: AMXConfig, table: str | None, *, error: ErrorFn) -> str | None:
    resolved = table or cfg.current_table
    if not resolved:
        error("Table is required. Use /table <name> in /db or pass a table.")
        return None
    return resolved


def display_comment(value: str | None) -> str:
    return value if value else "[dim](empty)[/dim]"


def collect_metadata_coverage(db: object, schema: str) -> MetadataCoverage:
    """Collect table/view and column comment coverage for one schema."""
    coverage = MetadataCoverage(schema=schema)
    assets = list(db.list_assets(schema))  # type: ignore[attr-defined]
    coverage.assets = len(assets)
    for name, _kind in assets:
        try:
            if db.get_table_comment(schema, name):  # type: ignore[attr-defined]
                coverage.assets_with_comments += 1
        except Exception:
            pass
        try:
            col_comments = db.get_column_comments(schema, name)  # type: ignore[attr-defined]
        except Exception:
            col_comments = {}
        coverage.columns += len(col_comments)
        coverage.columns_with_comments += sum(1 for comment in col_comments.values() if comment)
    return coverage


def build_inspect_rows(
    cfg: AMXConfig,
    db: object,
    schema: str | None,
    table: str | None,
    *,
    error: ErrorFn,
) -> tuple[str, list[list[str]]] | None:
    """Build table rows for `/manual inspect`."""
    if not schema and not table:
        rows = [
            ["Database", cfg.db.database or cfg.db.display_summary, display_comment(db.get_database_comment())],
        ]
        active_schema = cfg.current_schema
        if active_schema:
            rows.append(["Schema", active_schema, display_comment(db.get_schema_comment(active_schema))])
        return "Manual metadata", rows

    resolved_schema = resolve_schema_arg(cfg, schema, error=error)
    if not resolved_schema:
        return None

    if not table:
        rows = [["Schema", resolved_schema, display_comment(db.get_schema_comment(resolved_schema))]]
        for asset_name, kind in db.list_assets(resolved_schema):
            rows.append([
                kind.label,
                asset_name,
                display_comment(db.get_table_comment(resolved_schema, asset_name)),
            ])
        return f"Manual metadata: {resolved_schema}", rows

    asset_kind = db.resolve_asset_kind(resolved_schema, table)  # type: ignore[attr-defined]
    rows = [
        [
            asset_kind.label,
            table,
            display_comment(db.get_table_comment(resolved_schema, table)),
        ]
    ]
    for col, comment in sorted(db.get_column_comments(resolved_schema, table).items()):  # type: ignore[attr-defined]
        rows.append(["column", col, display_comment(comment)])
    return f"Manual metadata: {resolved_schema}.{table}", rows


def build_monitor_rows(cfg: AMXConfig, db: object, schema: str | None) -> list[list[str]]:
    """Build coverage rows for `/manual monitor`."""
    schemas = [schema] if schema else ([cfg.current_schema] if cfg.current_schema else db.list_schemas())  # type: ignore[attr-defined]
    rows: list[list[str]] = []
    for sch in schemas:
        if not sch:
            continue
        coverage = collect_metadata_coverage(db, sch)
        rows.append([
            coverage.schema,
            f"{coverage.assets_with_comments}/{coverage.assets}",
            f"{coverage.asset_percent:.1f}%",
            f"{coverage.columns_with_comments}/{coverage.columns}",
            f"{coverage.column_percent:.1f}%",
        ])
    return rows


def resolve_manual_target(
    cfg: AMXConfig,
    db: object,
    scope: str,
    names: list[str],
    *,
    error: ErrorFn,
) -> tuple[str, Callable[[str], None]] | None:
    """Resolve a manual-edit target and return a writer callback."""
    if scope == "database":
        if names:
            error("Usage: /edit database [--comment TEXT]")
            return None
        return "database", lambda comment: db.set_database_comment(comment)  # type: ignore[attr-defined]

    if scope == "schema":
        schema = names[0] if names else resolve_schema_arg(cfg, None, error=error)
        if not schema:
            return None
        if len(names) > 1:
            error("Usage: /edit schema [schema] [--comment TEXT]")
            return None
        return f"schema {schema}", lambda comment: db.set_schema_comment(schema, comment)  # type: ignore[attr-defined]

    if scope == "table":
        schema = names[0] if len(names) >= 2 else resolve_schema_arg(cfg, None, error=error)
        table = names[1] if len(names) >= 2 else (names[0] if names else resolve_table_arg(cfg, None, error=error))
        if not schema or not table:
            return None
        if len(names) > 2:
            error("Usage: /edit table [schema] [table] [--comment TEXT]")
            return None
        kind = db.resolve_asset_kind(schema, table)  # type: ignore[attr-defined]
        if not isinstance(kind, AssetKind):
            kind = AssetKind.TABLE
        return (
            f"{kind.label} {schema}.{table}",
            lambda comment: db.set_table_comment(schema, table, comment, asset_kind=kind),  # type: ignore[attr-defined]
        )

    if scope == "column":
        if len(names) >= 3:
            schema, table, column = names[0], names[1], names[2]
        elif len(names) == 2:
            schema = resolve_schema_arg(cfg, None, error=error)
            table, column = names[0], names[1]
        elif len(names) == 1:
            schema = resolve_schema_arg(cfg, None, error=error)
            table = resolve_table_arg(cfg, None, error=error)
            column = names[0]
        else:
            schema = resolve_schema_arg(cfg, None, error=error)
            table = resolve_table_arg(cfg, None, error=error)
            column = None
        if not schema or not table or not column:
            error("Usage: /edit column [schema] [table] <column> [--comment TEXT]")
            return None
        if len(names) > 3:
            error("Usage: /edit column [schema] [table] <column> [--comment TEXT]")
            return None
        return (
            f"column {schema}.{table}.{column}",
            lambda comment: db.set_column_comment(schema, table, column, comment),  # type: ignore[attr-defined]
        )

    return None

