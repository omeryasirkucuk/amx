"""Database introspection and metadata extraction.

Supports multiple backends via the adapter layer in ``amx.db.adapters``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

from amx.config import DBConfig
from amx.utils.logging import get_logger

log = get_logger("db.connector")


class AssetKind(Enum):
    TABLE = "table"
    VIEW = "view"
    MATERIALIZED_VIEW = "materialized_view"

    @property
    def label(self) -> str:
        return self.value.replace("_", " ")

    @property
    def comment_keyword(self) -> str:
        """SQL keyword for COMMENT ON <keyword>."""
        return {
            AssetKind.TABLE: "TABLE",
            AssetKind.VIEW: "VIEW",
            AssetKind.MATERIALIZED_VIEW: "MATERIALIZED VIEW",
        }[self]


@dataclass
class ColumnProfile:
    name: str
    dtype: str
    nullable: bool
    row_count: int = 0
    null_count: int = 0
    distinct_count: int = 0
    cardinality_ratio: float = 0.0
    min_val: Any = None
    max_val: Any = None
    samples: list[Any] = field(default_factory=list)
    existing_comment: str | None = None


@dataclass
class TableProfile:
    schema: str
    name: str
    asset_kind: AssetKind = AssetKind.TABLE
    row_count: int = 0
    columns: list[ColumnProfile] = field(default_factory=list)
    existing_comment: str | None = None
    primary_key: list[str] = field(default_factory=list)
    foreign_keys: list[dict[str, Any]] = field(default_factory=list)
    referenced_by: list[dict[str, Any]] = field(default_factory=list)
    unique_constraints: list[list[str]] = field(default_factory=list)
    check_constraints: list[str] = field(default_factory=list)
    stats_seq_scan: int = 0
    stats_idx_scan: int = 0
    stats_n_live_tup: int = 0
    schema_comment: str | None = None
    database_comment: str | None = None
    related_comments: list[dict[str, str]] = field(default_factory=list)


class DatabaseConnector:
    """Unified database connector that delegates backend-specific work to adapters."""

    def __init__(self, cfg: DBConfig):
        self.cfg = cfg
        self._engine: Engine | None = None

        from amx.db.adapters import get_adapter
        self._adapter = get_adapter(cfg)

    @property
    def engine(self) -> Engine:
        if self._engine is None:
            self._engine = self._adapter.create_engine()
            url_tail = self.cfg.url.split("@")[-1] if "@" in self.cfg.url else self.cfg.url
            log.info("Connected via %s to %s", self._adapter.name, url_tail)
        return self._engine

    @property
    def backend(self) -> str:
        return self._adapter.name

    def test_connection(self) -> bool:
        try:
            with self.engine.connect() as conn:
                conn.execute(text(self._adapter.test_connection_sql()))
            return True
        except Exception as exc:
            log.error("Connection failed: %s", exc)
            return False

    # ── Schema / asset listing ────────────────────────────────────────────

    def list_schemas(self) -> list[str]:
        insp = inspect(self.engine)
        system = self._adapter.system_schemas()
        return [s for s in insp.get_schema_names() if s not in system]

    def list_tables(self, schema: str) -> list[str]:
        insp = inspect(self.engine)
        return insp.get_table_names(schema=schema)

    def list_views(self, schema: str) -> list[str]:
        insp = inspect(self.engine)
        return insp.get_view_names(schema=schema)

    def list_materialized_views(self, schema: str) -> list[str]:
        return self._adapter.list_materialized_views(self.engine, schema)

    def list_assets(self, schema: str) -> list[tuple[str, AssetKind]]:
        """All analyzable assets (tables, views, materialized views) in a schema."""
        assets: list[tuple[str, AssetKind]] = []
        for t in self.list_tables(schema):
            assets.append((t, AssetKind.TABLE))
        for v in self.list_views(schema):
            assets.append((v, AssetKind.VIEW))
        for mv in self.list_materialized_views(schema):
            assets.append((mv, AssetKind.MATERIALIZED_VIEW))
        assets.sort(key=lambda x: x[0])
        return assets

    def resolve_asset_kind(self, schema: str, name: str) -> AssetKind:
        """Determine whether *name* is a table, view, or materialized view."""
        tables = set(self.list_tables(schema))
        if name in tables:
            return AssetKind.TABLE
        views = set(self.list_views(schema))
        if name in views:
            return AssetKind.VIEW
        matviews = set(self.list_materialized_views(schema))
        if name in matviews:
            return AssetKind.MATERIALIZED_VIEW
        return AssetKind.TABLE

    # ── Comments (read) ───────────────────────────────────────────────────

    def get_table_comment(self, schema: str, table: str) -> str | None:
        insp = inspect(self.engine)
        try:
            info = insp.get_table_comment(table, schema=schema)
            return info.get("text")
        except Exception:
            return None

    def get_column_comments(self, schema: str, table: str) -> dict[str, str | None]:
        insp = inspect(self.engine)
        cols = insp.get_columns(table, schema=schema)
        return {c["name"]: c.get("comment") for c in cols}

    def get_schema_comment(self, schema: str) -> str | None:
        return self._adapter.get_schema_comment(self.engine, schema)

    def get_database_comment(self) -> str | None:
        return self._adapter.get_database_comment(self.engine)

    # ── Profiling ─────────────────────────────────────────────────────────

    def profile_table(
        self,
        schema: str,
        table: str,
        sample_size: int = 5,
        asset_kind: AssetKind | None = None,
    ) -> TableProfile:
        if asset_kind is None:
            asset_kind = self.resolve_asset_kind(schema, table)
        log.info("Profiling %s.%s (%s) via %s", schema, table, asset_kind.label, self.backend)

        adapter = self._adapter
        fqn = adapter.fully_qualified_name(schema, table)
        profile = TableProfile(
            schema=schema,
            name=table,
            asset_kind=asset_kind,
            existing_comment=self.get_table_comment(schema, table),
            schema_comment=self.get_schema_comment(schema),
            database_comment=self.get_database_comment(),
        )

        with self.engine.connect() as conn:
            row_count = conn.execute(text(f"SELECT COUNT(*) FROM {fqn}")).scalar() or 0
            profile.row_count = row_count

        stats = adapter.get_table_stats(self.engine, schema, table)
        profile.stats_seq_scan = stats.get("seq_scan", 0)
        profile.stats_idx_scan = stats.get("idx_scan", 0)
        profile.stats_n_live_tup = stats.get("n_live_tup", 0)

        insp = inspect(self.engine)

        try:
            pk = insp.get_pk_constraint(table, schema=schema) or {}
            profile.primary_key = list(pk.get("constrained_columns") or [])
        except Exception:
            profile.primary_key = []

        try:
            profile.foreign_keys = list(insp.get_foreign_keys(table, schema=schema) or [])
        except Exception:
            profile.foreign_keys = []

        try:
            profile.unique_constraints = [
                list((u or {}).get("column_names") or [])
                for u in (insp.get_unique_constraints(table, schema=schema) or [])
            ]
        except Exception:
            profile.unique_constraints = []

        try:
            profile.check_constraints = [
                str((c or {}).get("sqltext") or "")
                for c in (insp.get_check_constraints(table, schema=schema) or [])
                if (c or {}).get("sqltext")
            ]
        except Exception:
            profile.check_constraints = []

        profile.referenced_by = adapter.get_incoming_foreign_keys(
            self.engine, schema, table
        )
        profile.related_comments = self.get_related_table_comments(
            profile.foreign_keys, profile.referenced_by
        )

        raw_cols = insp.get_columns(table, schema=schema)

        for col_info in raw_cols:
            col_name = col_info["name"]
            quoted_col = adapter.quote_identifier(col_name)
            cp = ColumnProfile(
                name=col_name,
                dtype=str(col_info["type"]),
                nullable=col_info.get("nullable", True),
                row_count=profile.row_count,
            )

            with self.engine.connect() as conn:
                stats_sql = adapter.column_stats_sql(fqn, quoted_col)
                col_stats = conn.execute(text(stats_sql)).fetchone()
                if col_stats:
                    cp.null_count = col_stats[0] or 0
                    cp.distinct_count = col_stats[1] or 0
                    cp.min_val = col_stats[2]
                    cp.max_val = col_stats[3]
                    cp.cardinality_ratio = (
                        float(cp.distinct_count) / float(cp.row_count)
                        if cp.row_count > 0
                        else 0.0
                    )

                sample_sql = adapter.column_sample_sql(fqn, quoted_col)
                samples_row = conn.execute(
                    text(sample_sql), {"lim": sample_size}
                ).fetchall()
                cp.samples = [r[0] for r in samples_row]

            cp.existing_comment = col_info.get("comment")
            profile.columns.append(cp)

        return profile

    # ── Relationships ─────────────────────────────────────────────────────

    def get_incoming_foreign_keys(self, schema: str, table: str) -> list[dict[str, Any]]:
        return self._adapter.get_incoming_foreign_keys(self.engine, schema, table)

    def get_related_table_comments(
        self,
        outgoing_fks: list[dict[str, Any]],
        incoming_fks: list[dict[str, Any]],
    ) -> list[dict[str, str]]:
        """Fetch comments for tables connected through FK relationships."""
        related: set[tuple[str, str]] = set()
        for fk in outgoing_fks:
            rs = str(fk.get("referred_schema") or "")
            rt = str(fk.get("referred_table") or "")
            if rs and rt:
                related.add((rs, rt))
        for fk in incoming_fks:
            rs = str(fk.get("source_schema") or "")
            rt = str(fk.get("source_table") or "")
            if rs and rt:
                related.add((rs, rt))

        out: list[dict[str, str]] = []
        for rs, rt in sorted(related):
            cmt = self.get_table_comment(rs, rt) or ""
            out.append({"schema": rs, "table": rt, "comment": cmt})
        return out

    # ── Comments (write) ──────────────────────────────────────────────────

    def set_table_comment(
        self,
        schema: str,
        table: str,
        comment: str,
        asset_kind: AssetKind = AssetKind.TABLE,
    ) -> None:
        keyword = asset_kind.comment_keyword
        stmt = self._adapter.set_table_comment_sql(schema, table, keyword)
        with self.engine.begin() as conn:
            conn.execute(text(stmt), {"cmt": comment})
        log.info("Set comment on %s.%s (%s)", schema, table, asset_kind.label)

    def set_column_comment(
        self, schema: str, table: str, column: str, comment: str
    ) -> None:
        stmt = self._adapter.set_column_comment_sql(schema, table, column)
        with self.engine.begin() as conn:
            conn.execute(text(stmt), {"cmt": comment})
        log.info("Set comment on %s.%s.%s", schema, table, column)

    # ── Adapter metadata ──────────────────────────────────────────────────

    @property
    def stats_label(self) -> str:
        """Human-readable label for the stats source (passed to LLM prompts)."""
        return self._adapter.stats_label()

    # ── Cleanup ───────────────────────────────────────────────────────────

    def close(self) -> None:
        if self._engine:
            self._engine.dispose()
