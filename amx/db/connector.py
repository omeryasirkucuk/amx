"""Database introspection and metadata extraction."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine

from amx.config import DBConfig
from amx.utils.logging import get_logger

log = get_logger("db.connector")


@dataclass
class ColumnProfile:
    name: str
    dtype: str
    nullable: bool
    row_count: int = 0
    null_count: int = 0
    distinct_count: int = 0
    min_val: Any = None
    max_val: Any = None
    samples: list[Any] = field(default_factory=list)


@dataclass
class TableProfile:
    schema: str
    name: str
    row_count: int = 0
    columns: list[ColumnProfile] = field(default_factory=list)
    existing_comment: str | None = None


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

    def get_table_comment(self, schema: str, table: str) -> str | None:
        insp = inspect(self.engine)
        info = insp.get_table_comment(table, schema=schema)
        return info.get("text")

    def get_column_comments(self, schema: str, table: str) -> dict[str, str | None]:
        insp = inspect(self.engine)
        cols = insp.get_columns(table, schema=schema)
        return {c["name"]: c.get("comment") for c in cols}

    def profile_table(self, schema: str, table: str, sample_size: int = 5) -> TableProfile:
        log.info("Profiling %s.%s", schema, table)
        fqn = f'"{schema}"."{table}"'
        profile = TableProfile(
            schema=schema,
            name=table,
            existing_comment=self.get_table_comment(schema, table),
        )

        with self.engine.connect() as conn:
            row_count = conn.execute(text(f"SELECT COUNT(*) FROM {fqn}")).scalar() or 0
            profile.row_count = row_count

        insp = inspect(self.engine)
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

    def set_table_comment(self, schema: str, table: str, comment: str) -> None:
        stmt = f'COMMENT ON TABLE "{schema}"."{table}" IS :cmt'
        with self.engine.begin() as conn:
            conn.execute(text(stmt), {"cmt": comment})
        log.info("Set comment on %s.%s", schema, table)

    def set_column_comment(self, schema: str, table: str, column: str, comment: str) -> None:
        stmt = f'COMMENT ON COLUMN "{schema}"."{table}"."{column}" IS :cmt'
        with self.engine.begin() as conn:
            conn.execute(text(stmt), {"cmt": comment})
        log.info("Set comment on %s.%s.%s", schema, table, column)

    def close(self) -> None:
        if self._engine:
            self._engine.dispose()
