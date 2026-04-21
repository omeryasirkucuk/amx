"""Database introspection and metadata extraction."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import pandas as pd
from sqlalchemy import create_engine, inspect, text
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
    def __init__(self, cfg: DBConfig):
        self.cfg = cfg
        self._engine: Engine | None = None

    @property
    def engine(self) -> Engine:
        if self._engine is None:
            self._engine = create_engine(self.cfg.url, pool_pre_ping=True)
            log.info("Connected to %s", self.cfg.url.split("@")[-1])
        return self._engine

    def test_connection(self) -> bool:
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception as exc:
            log.error("Connection failed: %s", exc)
            return False

    def list_schemas(self) -> list[str]:
        insp = inspect(self.engine)
        return [s for s in insp.get_schema_names() if s not in ("information_schema", "pg_catalog", "pg_toast")]

    def list_tables(self, schema: str) -> list[str]:
        insp = inspect(self.engine)
        return insp.get_table_names(schema=schema)

    def list_views(self, schema: str) -> list[str]:
        insp = inspect(self.engine)
        return insp.get_view_names(schema=schema)

    def list_materialized_views(self, schema: str) -> list[str]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT c.relname FROM pg_class c "
                    "JOIN pg_namespace n ON n.oid = c.relnamespace "
                    "WHERE n.nspname = :schema AND c.relkind = 'm' "
                    "ORDER BY c.relname"
                ),
                {"schema": schema},
            ).fetchall()
        return [r[0] for r in rows]

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

    def get_table_comment(self, schema: str, table: str) -> str | None:
        insp = inspect(self.engine)
        info = insp.get_table_comment(table, schema=schema)
        return info.get("text")

    def get_column_comments(self, schema: str, table: str) -> dict[str, str | None]:
        insp = inspect(self.engine)
        cols = insp.get_columns(table, schema=schema)
        return {c["name"]: c.get("comment") for c in cols}

    def profile_table(
        self,
        schema: str,
        table: str,
        sample_size: int = 5,
        asset_kind: AssetKind | None = None,
    ) -> TableProfile:
        if asset_kind is None:
            asset_kind = self.resolve_asset_kind(schema, table)
        log.info("Profiling %s.%s (%s)", schema, table, asset_kind.label)
        fqn = f'"{schema}"."{table}"'
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
            usage = conn.execute(
                text(
                    """
                    SELECT COALESCE(seq_scan, 0), COALESCE(idx_scan, 0), COALESCE(n_live_tup, 0)
                    FROM pg_stat_user_tables
                    WHERE schemaname = :schema AND relname = :table
                    """
                ),
                {"schema": schema, "table": table},
            ).fetchone()
            if usage:
                profile.stats_seq_scan = int(usage[0] or 0)
                profile.stats_idx_scan = int(usage[1] or 0)
                profile.stats_n_live_tup = int(usage[2] or 0)

        insp = inspect(self.engine)
        pk = insp.get_pk_constraint(table, schema=schema) or {}
        profile.primary_key = list(pk.get("constrained_columns") or [])
        profile.foreign_keys = list(insp.get_foreign_keys(table, schema=schema) or [])
        profile.unique_constraints = [
            list((u or {}).get("column_names") or [])
            for u in (insp.get_unique_constraints(table, schema=schema) or [])
        ]
        profile.check_constraints = [
            str((c or {}).get("sqltext") or "")
            for c in (insp.get_check_constraints(table, schema=schema) or [])
            if (c or {}).get("sqltext")
        ]
        profile.referenced_by = self.get_incoming_foreign_keys(schema, table)
        profile.related_comments = self.get_related_table_comments(profile.foreign_keys, profile.referenced_by)
        raw_cols = insp.get_columns(table, schema=schema)

        for col_info in raw_cols:
            col_name = col_info["name"]
            quoted_col = f'"{col_name}"'
            cp = ColumnProfile(
                name=col_name,
                dtype=str(col_info["type"]),
                nullable=col_info.get("nullable", True),
                row_count=profile.row_count,
            )

            with self.engine.connect() as conn:
                stats = conn.execute(
                    text(
                        f"SELECT "
                        f"  COUNT(*) FILTER (WHERE {quoted_col} IS NULL) AS null_cnt, "
                        f"  COUNT(DISTINCT {quoted_col}) AS dist_cnt, "
                        f"  MIN({quoted_col}::text) AS min_val, "
                        f"  MAX({quoted_col}::text) AS max_val "
                        f"FROM {fqn}"
                    )
                ).fetchone()
                if stats:
                    cp.null_count = stats[0] or 0
                    cp.distinct_count = stats[1] or 0
                    cp.min_val = stats[2]
                    cp.max_val = stats[3]
                    cp.cardinality_ratio = (
                        float(cp.distinct_count) / float(cp.row_count)
                        if cp.row_count > 0
                        else 0.0
                    )

                samples_row = conn.execute(
                    text(
                        f"SELECT DISTINCT {quoted_col}::text FROM {fqn} "
                        f"WHERE {quoted_col} IS NOT NULL LIMIT :lim"
                    ),
                    {"lim": sample_size},
                ).fetchall()
                cp.samples = [r[0] for r in samples_row]

            cp.existing_comment = col_info.get("comment")
            profile.columns.append(cp)

        return profile

    def get_schema_comment(self, schema: str) -> str | None:
        with self.engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT obj_description(n.oid, 'pg_namespace')
                    FROM pg_namespace n
                    WHERE n.nspname = :schema
                    """
                ),
                {"schema": schema},
            ).fetchone()
            return row[0] if row else None

    def get_database_comment(self) -> str | None:
        with self.engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT shobj_description(d.oid, 'pg_database')
                    FROM pg_database d
                    WHERE d.datname = current_database()
                    """
                )
            ).fetchone()
            return row[0] if row else None

    def get_incoming_foreign_keys(self, schema: str, table: str) -> list[dict[str, Any]]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT
                        src_ns.nspname AS source_schema,
                        src.relname AS source_table,
                        src_col.attname AS source_column,
                        tgt_col.attname AS target_column
                    FROM pg_constraint con
                    JOIN pg_class src ON src.oid = con.conrelid
                    JOIN pg_namespace src_ns ON src_ns.oid = src.relnamespace
                    JOIN pg_class tgt ON tgt.oid = con.confrelid
                    JOIN pg_namespace tgt_ns ON tgt_ns.oid = tgt.relnamespace
                    JOIN unnest(con.conkey) WITH ORDINALITY AS src_key(attnum, ord) ON TRUE
                    JOIN unnest(con.confkey) WITH ORDINALITY AS tgt_key(attnum, ord) ON src_key.ord = tgt_key.ord
                    JOIN pg_attribute src_col ON src_col.attrelid = src.oid AND src_col.attnum = src_key.attnum
                    JOIN pg_attribute tgt_col ON tgt_col.attrelid = tgt.oid AND tgt_col.attnum = tgt_key.attnum
                    WHERE con.contype = 'f'
                      AND tgt_ns.nspname = :schema
                      AND tgt.relname = :table
                    ORDER BY src_ns.nspname, src.relname, src_col.attname
                    """
                ),
                {"schema": schema, "table": table},
            ).fetchall()
        return [
            {
                "source_schema": str(r[0]),
                "source_table": str(r[1]),
                "source_column": str(r[2]),
                "target_column": str(r[3]),
            }
            for r in rows
        ]

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
            out.append(
                {
                    "schema": rs,
                    "table": rt,
                    "comment": cmt,
                }
            )
        return out

    def set_table_comment(
        self,
        schema: str,
        table: str,
        comment: str,
        asset_kind: AssetKind = AssetKind.TABLE,
    ) -> None:
        keyword = asset_kind.comment_keyword
        stmt = f'COMMENT ON {keyword} "{schema}"."{table}" IS :cmt'
        with self.engine.begin() as conn:
            conn.execute(text(stmt), {"cmt": comment})
        log.info("Set comment on %s.%s (%s)", schema, table, asset_kind.label)

    def set_column_comment(self, schema: str, table: str, column: str, comment: str) -> None:
        stmt = f'COMMENT ON COLUMN "{schema}"."{table}"."{column}" IS :cmt'
        with self.engine.begin() as conn:
            conn.execute(text(stmt), {"cmt": comment})
        log.info("Set comment on %s.%s.%s", schema, table, column)

    def close(self) -> None:
        if self._engine:
            self._engine.dispose()
