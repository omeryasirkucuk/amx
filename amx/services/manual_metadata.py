"""Manual metadata inspection, editing, and coverage helpers."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum

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


class ManualTargetKind(str, Enum):
    DATABASE = "database"
    SCHEMA = "schema"
    TABLE = "table"
    COLUMN = "column"


@dataclass(frozen=True)
class ManualEditTarget:
    profile: str
    kind: ManualTargetKind
    label: str
    writer: Callable[[str], None]


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


def _split_qualified_name(value: str) -> list[str]:
    parts = [part.strip() for part in value.split(".")]
    return parts if all(parts) else [value]


def split_metadata_path(value: str) -> list[str]:
    """Split a user-facing metadata target path into non-empty dotted parts."""
    return _split_qualified_name(value)


def _manual_table_usage(error: ErrorFn) -> None:
    error("Choose a table/view explicitly: /edit table <table> or /edit table <schema>.<table>")


def _manual_schema_usage(error: ErrorFn) -> None:
    error("Choose a schema explicitly: /edit schema <schema>")


def _manual_column_usage(error: ErrorFn) -> None:
    error(
        "Choose a column explicitly: /edit column <column>, /edit column <table>.<column>, "
        "or /edit column <schema>.<table>.<column>"
    )


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
            [
                "Database",
                cfg.db.database or cfg.db.display_summary,
                display_comment(db.get_database_comment()),
            ],
        ]
        active_schema = cfg.current_schema
        if active_schema:
            rows.append(
                ["Schema", active_schema, display_comment(db.get_schema_comment(active_schema))]
            )
        return "Manual metadata", rows

    resolved_schema = resolve_schema_arg(cfg, schema, error=error)
    if not resolved_schema:
        return None

    if not table:
        rows = [
            ["Schema", resolved_schema, display_comment(db.get_schema_comment(resolved_schema))]
        ]
        for asset_name, kind in db.list_assets(resolved_schema):
            rows.append(
                [
                    kind.label,
                    asset_name,
                    display_comment(db.get_table_comment(resolved_schema, asset_name)),
                ]
            )
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
    schemas = (
        [schema] if schema else ([cfg.current_schema] if cfg.current_schema else db.list_schemas())
    )  # type: ignore[attr-defined]
    rows: list[list[str]] = []
    for sch in schemas:
        if not sch:
            continue
        coverage = collect_metadata_coverage(db, sch)
        rows.append(
            [
                coverage.schema,
                f"{coverage.assets_with_comments}/{coverage.assets}",
                f"{coverage.asset_percent:.1f}%",
                f"{coverage.columns_with_comments}/{coverage.columns}",
                f"{coverage.column_percent:.1f}%",
            ]
        )
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
    normalized_scope = "database" if scope == "db" else scope

    if normalized_scope == "database":
        if names:
            error(
                "The active database is edited with /edit database. Switch DB profiles under /db to edit another database."
            )
            return None
        return "database", lambda comment: db.set_database_comment(comment)  # type: ignore[attr-defined]

    if normalized_scope == "schema":
        if len(names) != 1 or len(_split_qualified_name(names[0])) != 1:
            _manual_schema_usage(error)
            return None
        schema = names[0]
        return f"schema {schema}", lambda comment: db.set_schema_comment(schema, comment)  # type: ignore[attr-defined]

    if normalized_scope == "table":
        if len(names) == 1:
            parts = _split_qualified_name(names[0])
            if len(parts) == 1:
                schema = resolve_schema_arg(cfg, None, error=error)
                table = parts[0]
            elif len(parts) == 2:
                schema, table = parts
            else:
                _manual_table_usage(error)
                return None
        elif len(names) == 2:
            schema, table = names[0], names[1]
        else:
            _manual_table_usage(error)
            return None
        if not schema or not table:
            return None
        kind = db.resolve_asset_kind(schema, table)  # type: ignore[attr-defined]
        if not isinstance(kind, AssetKind):
            kind = AssetKind.TABLE
        return (
            f"{kind.label} {schema}.{table}",
            lambda comment: db.set_table_comment(schema, table, comment, asset_kind=kind),  # type: ignore[attr-defined]
        )

    if normalized_scope == "column":
        if len(names) == 1:
            parts = _split_qualified_name(names[0])
            if len(parts) == 1:
                schema = resolve_schema_arg(cfg, None, error=error)
                table = resolve_table_arg(cfg, None, error=error)
                column = parts[0]
            elif len(parts) == 2:
                schema = resolve_schema_arg(cfg, None, error=error)
                table, column = parts
            elif len(parts) == 3:
                schema, table, column = parts
            else:
                _manual_column_usage(error)
                return None
        elif len(names) == 2:
            schema = resolve_schema_arg(cfg, None, error=error)
            table, column = names[0], names[1]
        elif len(names) == 3:
            schema, table, column = names[0], names[1], names[2]
        else:
            _manual_column_usage(error)
            return None
        if not schema or not table or not column:
            return None
        return (
            f"column {schema}.{table}.{column}",
            lambda comment: db.set_column_comment(schema, table, column, comment),  # type: ignore[attr-defined]
        )

    return None


def resolve_path_target(
    cfg: AMXConfig,
    db: object,
    profile_name: str,
    path: str,
    *,
    error: ErrorFn,
) -> ManualEditTarget | None:
    """Resolve `/edit <db>[.<schema>[.<table>[.<column>]]]` path syntax."""
    parts = split_metadata_path(path)
    if len(parts) == 1:
        db_name = parts[0]
        if (
            db_name not in cfg.db_profiles
            and db_name != profile_name
            and db_name != cfg.db.database
        ):
            error(f"Unknown DB profile: {db_name}. Use /db then /db-profiles to list profiles.")
            return None
        return ManualEditTarget(
            profile=profile_name,
            kind=ManualTargetKind.DATABASE,
            label=f"database {profile_name}",
            writer=lambda comment: db.set_database_comment(comment),  # type: ignore[attr-defined]
        )
    if len(parts) == 2:
        db_name, schema = parts
        if (
            db_name not in cfg.db_profiles
            and db_name != profile_name
            and db_name != cfg.db.database
        ):
            error(f"Unknown DB profile: {db_name}. Use /db then /db-profiles to list profiles.")
            return None
        return ManualEditTarget(
            profile=profile_name,
            kind=ManualTargetKind.SCHEMA,
            label=f"schema {db_name}.{schema}",
            writer=lambda comment: db.set_schema_comment(schema, comment),  # type: ignore[attr-defined]
        )
    if len(parts) == 3:
        db_name, schema, table = parts
        if (
            db_name not in cfg.db_profiles
            and db_name != profile_name
            and db_name != cfg.db.database
        ):
            error(f"Unknown DB profile: {db_name}. Use /db then /db-profiles to list profiles.")
            return None
        kind = db.resolve_asset_kind(schema, table)  # type: ignore[attr-defined]
        if not isinstance(kind, AssetKind):
            kind = AssetKind.TABLE
        return ManualEditTarget(
            profile=profile_name,
            kind=ManualTargetKind.TABLE,
            label=f"{kind.label} {db_name}.{schema}.{table}",
            writer=lambda comment: db.set_table_comment(schema, table, comment, asset_kind=kind),  # type: ignore[attr-defined]
        )
    if len(parts) == 4:
        db_name, schema, table, column = parts
        if (
            db_name not in cfg.db_profiles
            and db_name != profile_name
            and db_name != cfg.db.database
        ):
            error(f"Unknown DB profile: {db_name}. Use /db then /db-profiles to list profiles.")
            return None
        return ManualEditTarget(
            profile=profile_name,
            kind=ManualTargetKind.COLUMN,
            label=f"column {db_name}.{schema}.{table}.{column}",
            writer=lambda comment: db.set_column_comment(schema, table, column, comment),  # type: ignore[attr-defined]
        )
    error(
        "Use /edit <db>, <db>.<schema>, <db>.<schema>.<table>, or <db>.<schema>.<table>.<column>."
    )
    return None
