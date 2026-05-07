# Performance benchmarks

Opt-in micro- and end-to-end benchmarks. Not part of the wheel, not
collected by the default `pytest` invocation.

## Install

```bash
pip install -e ".[perf]"
```

This adds `pytest-benchmark`, `psutil`, and `duckdb` on top of the
regular dev environment.

## Run

```bash
pytest tests/perf -m perf --benchmark-only
```

The default `pytest` (no flags) excludes this tree via the `perf`
marker filter in `pyproject.toml`.

## Capture a baseline / compare

```bash
make perf-baseline   # writes tests/perf/baselines/<timestamp>.json
make perf-compare    # runs again, compares against the latest baseline
```

`perf-clean` removes baseline JSON files. Baseline files are
`.gitignore`d — capture locally, share via PR description or upload to
the GitHub Pages dashboard once that lands.

## Scope

This first PR ships the harness only:

- Synthetic DuckDB fixture (`fixtures/synthetic_db.py`).
- Deterministic LLM mock (`fixtures/llm_mock.py`).
- Seeded doc-set generator (`fixtures/docs.py`).
- A handful of smoke benchmarks (`test_harness_smoke.py`) so
  `make perf-baseline` has something to record.

Hot-path benchmarks (FK resolution, `profile_table`, embedding
rebuild, orchestrator fan-out, `amx --help` cold start) land in
follow-up PRs that target each optimisation, so every change reports
its own before/after delta.
