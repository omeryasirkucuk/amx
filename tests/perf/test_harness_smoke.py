"""Smoke benchmarks proving the perf harness works end-to-end.

Real hot-path benchmarks (FK resolution, profile_table, embedding
rebuild) land in dedicated ``bench_*.py`` files in subsequent PRs. The
tests here only:

  * confirm ``pytest tests/perf -m perf`` collects and runs;
  * confirm pytest-benchmark + duckdb are wired up correctly;
  * give ``make perf-baseline`` something to write a baseline JSON for.

Each test is fast enough to run in CI's nightly perf job without
adding noticeable runtime.
"""

from __future__ import annotations

import duckdb
import pytest

from tests.perf.fixtures.docs import make_chunks
from tests.perf.fixtures.llm_mock import MockLLM
from tests.perf.fixtures.synthetic_db import build_synthetic


@pytest.mark.perf
def test_synthetic_db_build(benchmark):
    """Building the in-memory synthetic catalog should stay snappy."""

    def _build() -> int:
        con = duckdb.connect(":memory:")
        try:
            shape = build_synthetic(
                con,
                schemas=2,
                tables_per_schema=10,
                cols_per_table=10,
                rows_per_table=100,
            )
            return shape.schemas * shape.tables_per_schema
        finally:
            con.close()

    total_tables = benchmark(_build)
    assert total_tables == 20


@pytest.mark.perf
def test_mock_llm_call(benchmark):
    """MockLLM round-trip overhead — establishes a baseline for fan-out
    benchmarks that wrap the same call shape."""
    llm = MockLLM(latency_per_token_s=0.0, output_tokens=8)
    messages = [{"role": "user", "content": "describe column transactions.posting"}]
    result = benchmark(lambda: llm.chat(messages))
    assert result["content"] == "MOCK_RESPONSE"
    assert llm.stats.calls >= 1


@pytest.mark.perf
def test_make_chunks(benchmark):
    """Doc-set generation cost (seeded, deterministic)."""
    chunks = benchmark(lambda: make_chunks(n_chunks=200, seed=42))
    assert len(chunks) == 200
    # Determinism check — same seed ⇒ same first chunk text.
    again = make_chunks(n_chunks=1, seed=42)
    assert chunks[0].text == again[0].text
