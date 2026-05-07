"""Synthetic in-process DuckDB fixture for perf benchmarks.

Builds a deterministic schema/table/column layout so micro-benchmarks
(``profile_table``, FK resolution, pool acquire) run on the same shape
across machines. DuckDB is portable and ships in the ``[perf]`` extra,
so this fixture keeps benchmarks runnable without docker-compose.

Usage:

    from tests.perf.fixtures.synthetic_db import build_synthetic

    con = duckdb.connect(":memory:")
    build_synthetic(con, schemas=2, tables_per_schema=10, cols_per_table=20)
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SyntheticShape:
    schemas: int
    tables_per_schema: int
    cols_per_table: int
    rows_per_table: int


def build_synthetic(
    con,
    *,
    schemas: int = 2,
    tables_per_schema: int = 10,
    cols_per_table: int = 20,
    rows_per_table: int = 1_000,
    add_fks: bool = True,
) -> SyntheticShape:
    """Populate an open DuckDB connection with a synthetic catalog.

    Returns the realised shape so benchmarks can assert against it.
    DuckDB's ``ALTER TABLE ... ADD CONSTRAINT FOREIGN KEY`` is parsed
    but constraint enforcement is partial; for FK *resolution* benches
    we only need the catalog rows, which DuckDB does record.
    """
    for s in range(schemas):
        schema = f"s{s}"
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        for t in range(tables_per_schema):
            cols_sql = ", ".join(
                f"c{c} {'BIGINT' if c == 0 else 'VARCHAR'}" for c in range(cols_per_table)
            )
            pk = "" if cols_per_table == 0 else ", PRIMARY KEY (c0)"
            con.execute(f"CREATE TABLE {schema}.t{t} ({cols_sql}{pk})")
            if rows_per_table > 0:
                # Single bulk insert keeps fixture build fast even at high counts.
                con.execute(
                    f"INSERT INTO {schema}.t{t} "
                    f"SELECT i AS c0, "
                    + ", ".join(f"'v{c}_' || i" for c in range(1, cols_per_table))
                    + f" FROM range({rows_per_table}) tbl(i)"
                )
            if add_fks and t > 0:
                con.execute(
                    f"ALTER TABLE {schema}.t{t} "
                    f"ADD CONSTRAINT fk_t{t}_to_t{t - 1} "
                    f"FOREIGN KEY (c0) REFERENCES {schema}.t{t - 1}(c0)"
                )
    return SyntheticShape(
        schemas=schemas,
        tables_per_schema=tables_per_schema,
        cols_per_table=cols_per_table,
        rows_per_table=rows_per_table,
    )
