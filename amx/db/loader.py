"""Bulk-load CSV files into PostgreSQL for initial data setup."""

from __future__ import annotations

import csv
import io
from pathlib import Path

import pandas as pd
from sqlalchemy import text
from tqdm import tqdm

from amx.db.connector import DatabaseConnector
from amx.utils.logging import get_logger

log = get_logger("db.loader")

CHUNK_SIZE = 50_000


def load_csvs_to_schema(
    connector: DatabaseConnector,
    csv_dir: str | Path,
    schema: str = "sap_s6p",
) -> list[str]:
    csv_dir = Path(csv_dir)
    csv_files = sorted(csv_dir.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files in {csv_dir}")

    with connector.engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
    log.info("Ensured schema %s exists", schema)

    loaded: list[str] = []
    for csv_path in tqdm(csv_files, desc="Loading CSVs", unit="table"):
        table_name = csv_path.stem.lower()
        try:
            _load_single_csv(connector, csv_path, schema, table_name)
            loaded.append(table_name)
        except Exception as exc:
            log.error("Failed to load %s: %s", csv_path.name, exc)

    return loaded


def _load_single_csv(
    connector: DatabaseConnector,
    csv_path: Path,
    schema: str,
    table_name: str,
) -> None:
    header_df = pd.read_csv(csv_path, nrows=0)
    columns = list(header_df.columns)

    col_defs = ", ".join(f'"{c}" TEXT' for c in columns)
    fqn = f'"{schema}"."{table_name}"'
    with connector.engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {fqn}"))
        conn.execute(text(f"CREATE TABLE {fqn} ({col_defs})"))

    raw_conn = connector.engine.raw_connection()
    try:
        cur = raw_conn.cursor()
        quoted_cols = ", ".join(f'"{c}"' for c in columns)

        with open(csv_path, "r") as f:
            next(f)  # skip header
            cur.copy_expert(
                f"COPY {fqn} ({quoted_cols}) FROM STDIN WITH (FORMAT CSV, NULL '', QUOTE '\"')",
                f,
            )
        raw_conn.commit()

        cur.execute(f"SELECT COUNT(*) FROM {fqn}")
        row_count = cur.fetchone()[0]
        log.info("Loaded %s (%d rows, %d cols)", table_name, row_count, len(columns))
    except Exception:
        raw_conn.rollback()
        raise
    finally:
        raw_conn.close()
