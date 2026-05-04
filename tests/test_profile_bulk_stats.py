"""Phase 1 of the perf plan: column stats are emitted as one bulk query
per N columns, not N queries per N columns.

We pin the behavior with two layers:

1. Each adapter's ``column_stats_bulk_sql`` returns syntactically-correct
   SQL for its dialect, with deterministic per-column aliases the
   connector parses by index.
2. The connector's ``_collect_bulk_stats`` actually issues the bulk
   query against the (mocked) engine — and falls back per-column when
   the bulk query raises.

The goal of these tests is to keep the per-column query count from
silently regressing if someone reverts the bulk path or breaks the
batch-size handling.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from amx.config import DBConfig
from amx.db.adapters import SUPPORTED_BACKENDS, get_adapter
from amx.db.connector import ColumnProfile, DatabaseConnector


def _fixture(backend: str) -> dict[str, object]:
    return {
        "postgresql": {"host": "db.example.com", "user": "alice"},
        "snowflake": {"account": "acc", "user": "alice"},
        "databricks": {"host": "adb.example.com", "access_token": "tok"},
        "bigquery": {"project": "my-project"},
        "mysql": {"host": "h", "user": "u", "password": "p"},
        "oracle": {"host": "h", "user": "u", "password": "p", "service_name": "XEPDB1"},
        "mssql": {"host": "h", "user": "u", "password": "p"},
        "redshift": {"host": "h", "user": "u", "password": "p"},
        "clickhouse": {"host": "h", "user": "u", "password": "p"},
        "duckdb": {"database": ":memory:"},
    }[backend]


@pytest.mark.parametrize("backend", sorted(SUPPORTED_BACKENDS))
def test_column_stats_bulk_sql_is_well_formed(backend: str) -> None:
    """Every adapter produces a single SELECT with positional aliases."""
    cfg = DBConfig(backend=backend, **_fixture(backend))  # type: ignore[arg-type]
    adapter = get_adapter(cfg)

    # 3 columns × 4 metrics = 12 aliased expressions. Aliases follow
    # ``c{i}_null/_dist/_min/_max`` so the connector parses by index.
    sql = adapter.column_stats_bulk_sql('"sch"."tbl"', ['"a"', '"b"', '"c"'])

    assert sql.count("c0_null") == 1
    assert sql.count("c1_null") == 1
    assert sql.count("c2_null") == 1
    assert sql.count("c0_dist") == 1
    assert sql.count("c1_dist") == 1
    assert sql.count("c2_dist") == 1
    assert sql.lower().count("from") == 1
    assert sql.lower().count("select") == 1


def test_column_stats_bulk_sql_rejects_empty() -> None:
    cfg = DBConfig(backend="postgresql", **_fixture("postgresql"))  # type: ignore[arg-type]
    adapter = get_adapter(cfg)
    with pytest.raises(ValueError):
        adapter.column_stats_bulk_sql('"sch"."tbl"', [])


def _make_recording_connector(batch_size: int = 50) -> tuple[DatabaseConnector, list[str]]:
    """Build a DatabaseConnector with an engine that records every query."""
    cfg = DBConfig(
        backend="postgresql",
        host="x",
        user="u",
        profiling_stats_batch_size=batch_size,
    )
    conn = DatabaseConnector(cfg)
    recorded: list[str] = []

    class RecordingResult:
        def fetchone(self):
            # Bulk path expects 4 fields per column. We don't know the
            # column count in this fixture so we return a long row of
            # neutral values that any batch size can index into.
            return [0, 0, "", ""] * 200

        def fetchall(self):
            return []

    class RecordingConn:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, sql, params=None):
            recorded.append(str(sql))
            return RecordingResult()

    class RecordingEngine:
        def connect(self):
            return RecordingConn()

    conn._engine = RecordingEngine()  # type: ignore[assignment]
    return conn, recorded


def test_bulk_stats_collapses_300_cols_to_6_queries() -> None:
    """The user-reported 300-col case: 6 queries at default batch_size=50."""
    conn, recorded = _make_recording_connector(batch_size=50)
    adapter = get_adapter(conn.cfg)
    cps = [
        ColumnProfile(name=f"col_{i}", dtype="int", nullable=True, row_count=1000)
        for i in range(300)
    ]

    conn._collect_bulk_stats(
        schema="sch",
        table="tbl",
        fqn='"sch"."tbl"',
        adapter=adapter,
        column_profiles=cps,
        row_count=1000,
        batch_size=50,
    )

    assert len(recorded) == 6, f"Expected 6 bulk queries, got {len(recorded)}"
    # And every column got stats populated.
    assert all(cp.distinct_count is not None for cp in cps)


@pytest.mark.parametrize("backend", sorted(SUPPORTED_BACKENDS))
def test_bulk_sample_sql_is_well_formed(backend: str) -> None:
    """Every adapter produces a single SELECT yielding all columns at once."""
    cfg = DBConfig(backend=backend, **_fixture(backend))  # type: ignore[arg-type]
    adapter = get_adapter(cfg)

    sql = adapter.bulk_sample_sql('"sch"."tbl"', ['"a"', '"b"', '"c"'], 1000)

    # Every column must appear in the SELECT list (the connector parses
    # by index, so missing columns would shift everything).
    assert sql.count('"a"') >= 1
    assert sql.count('"b"') >= 1
    assert sql.count('"c"') >= 1
    assert "1000" in sql


def test_bulk_sample_sql_rejects_empty() -> None:
    cfg = DBConfig(backend="postgresql", **_fixture("postgresql"))  # type: ignore[arg-type]
    adapter = get_adapter(cfg)
    with pytest.raises(ValueError):
        adapter.bulk_sample_sql('"sch"."tbl"', [], 1000)


def test_bulk_samples_collapses_300_cols_to_1_query_when_distincts_are_plentiful() -> None:
    """The user-reported 300-col case: bulk sample, no per-column escalation."""
    cfg = DBConfig(backend="postgresql", host="x", user="u")
    conn = DatabaseConnector(cfg)
    adapter = get_adapter(cfg)
    recorded: list[str] = []

    # Simulated rows: 50 rows where each column has a unique value per
    # row (distinct values are plentiful — escalation should not trigger).
    fake_rows = [tuple(f"val_{c}_{r}" for c in range(300)) for r in range(50)]

    class RecordingConn:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, sql, params=None):
            recorded.append(str(sql))

            class R:
                def fetchall(self_inner):
                    return fake_rows

                def fetchone(self_inner):
                    return None

            return R()

    class RecordingEngine:
        def connect(self):
            return RecordingConn()

    conn._engine = RecordingEngine()  # type: ignore[assignment]

    cps = [
        ColumnProfile(name=f"col_{i}", dtype="text", nullable=True, row_count=1000)
        for i in range(300)
    ]
    conn._collect_bulk_samples(
        schema="sch",
        table="tbl",
        fqn='"sch"."tbl"',
        adapter=adapter,
        column_profiles=cps,
        effective_sample_size=5,
    )

    # One bulk sample query, no escalation.
    assert len(recorded) == 1, f"Expected 1 query, got {len(recorded)}"
    # Every column got 5 distinct samples.
    assert all(len(cp.samples) == 5 for cp in cps)


def test_bulk_samples_escalates_only_short_columns() -> None:
    """Columns whose bulk sample yielded < threshold distincts get a per-column query."""
    cfg = DBConfig(backend="postgresql", host="x", user="u")
    conn = DatabaseConnector(cfg)
    adapter = get_adapter(cfg)
    recorded: list[str] = []

    # 5 columns, 100 rows. col_0 and col_1 have plenty of distincts.
    # col_2..col_4 are constant — only 1 distinct each, below threshold.
    fake_rows = []
    for r in range(100):
        fake_rows.append((f"a_{r}", f"b_{r}", "constant", "constant", "constant"))

    class RecordingConn:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, sql, params=None):
            recorded.append(str(sql))

            class R:
                def fetchall(self_inner):
                    return fake_rows

                def fetchone(self_inner):
                    return None

            return R()

    class RecordingEngine:
        def connect(self):
            return RecordingConn()

    conn._engine = RecordingEngine()  # type: ignore[assignment]

    cps = [
        ColumnProfile(name=f"col_{i}", dtype="text", nullable=True, row_count=1000)
        for i in range(5)
    ]
    conn._collect_bulk_samples(
        schema="sch",
        table="tbl",
        fqn='"sch"."tbl"',
        adapter=adapter,
        column_profiles=cps,
        effective_sample_size=5,
    )

    # 1 bulk + 3 per-column escalations (col_2, col_3, col_4 had 1 distinct
    # each, below threshold = min(5, 3) = 3).
    assert len(recorded) == 4, f"Expected 4 queries (1 bulk + 3 escalations), got {len(recorded)}"


def test_bulk_stats_falls_back_to_per_column_on_failure() -> None:
    """If the bulk SELECT raises, the connector retries per-column."""
    cfg = DBConfig(backend="postgresql", host="x", user="u")
    conn = DatabaseConnector(cfg)
    adapter = get_adapter(cfg)
    recorded: list[tuple[str, str]] = []  # (label, sql)

    class FailingFirstThenPerColumn:
        """First call raises (bulk); subsequent calls succeed (per-column)."""

        def __init__(self):
            self._calls = 0

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, sql, params=None):
            self._calls += 1
            sql_str = str(sql)
            if self._calls == 1:
                recorded.append(("bulk-attempt", sql_str))
                raise RuntimeError("bulk SQL not supported (synthetic)")
            recorded.append(("per-col", sql_str))

            class R:
                def fetchone(self_inner):
                    return [0, 0, "", ""]

                def fetchall(self_inner):
                    return []

            return R()

    state = FailingFirstThenPerColumn()

    class Engine:
        def connect(self):
            # Each connect() call returns a fresh context manager backed
            # by the same shared call counter so the first context-block
            # raises and later ones succeed.
            return state

    conn._engine = Engine()  # type: ignore[assignment]

    cps = [
        ColumnProfile(name=f"col_{i}", dtype="int", nullable=True, row_count=100)
        for i in range(3)
    ]
    conn._collect_bulk_stats(
        schema="sch",
        table="tbl",
        fqn='"sch"."tbl"',
        adapter=adapter,
        column_profiles=cps,
        row_count=100,
        batch_size=50,
    )

    labels = [label for label, _ in recorded]
    assert labels[0] == "bulk-attempt"
    # 3 per-column queries after the bulk failure
    assert labels.count("per-col") == 3
