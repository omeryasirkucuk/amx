# Retrieval evaluation harness

A small kit for measuring AMX search retrieval quality and comparing
embedding providers (MiniLM / OpenAI-compatible / SentenceTransformers).

## What's here

- `metrics.py` — pure functions for `hit@k`, `reciprocal_rank`,
  `mean_reciprocal_rank`, `precision@k`, and `ndcg@k`.
- `test_retrieval_metrics.py` — unit tests for the metrics. Runs in CI
  on every PR via the standard `pytest` invocation.
- `test_smoke.py` — end-to-end smoke harness with a fake retriever, so
  contributors can copy the shape into a real eval script without
  having to assemble the plumbing from scratch.

## Running

```bash
# All eval tests (metrics + smoke)
pytest tests/eval/

# Just the metric unit tests
pytest tests/eval/test_retrieval_metrics.py

# A specific metric
pytest tests/eval/test_retrieval_metrics.py::NdcgAtKTests
```

## Adding a real eval

1. Drop a fixture file into `tests/eval/fixtures/` mapping questions
   to the set of entity ids you consider relevant for each. We
   recommend JSON or YAML — pick one and stay consistent.
2. Write a script (or a `pytest` test) that:
   - loads the fixture,
   - constructs a `SearchCatalog` against your real `~/.amx/history.db`
     (or a frozen copy),
   - runs each question through `catalog.query(...)`,
   - projects the result rows down to entity-id strings (e.g.
     `f"{row['schema_name']}.{row['table_name']}"`),
   - scores each query with the metrics in `tests/eval/metrics.py`.
3. To compare embedding providers, run the same script three times
   under different `cfg.embedding.kind` settings and report the
   per-query and aggregate metrics side-by-side.

## What the metrics measure

| Metric | What it answers |
|--------|----------------|
| `hit@k` | Did the retriever surface *any* correct answer in the top-k? |
| `reciprocal_rank` | At what position did the first correct answer appear? |
| `MRR` | Average reciprocal rank across many queries. Sensitive to ranking quality, not just inclusion. |
| `precision@k` | What fraction of the top-k are correct? (Cares about result *purity*.) |
| `nDCG@k` | Like precision@k but penalises late-position hits with a logarithmic discount. The standard ranking-quality scalar. |

## Why per-query scoring matters

Aggregate MRR can hide regressions on a small but important slice of
queries. When comparing providers, also report per-query deltas so a
better-on-average provider that *worsens* the worst-case query is
visible.

## Description-quality comparisons (companion to retrieval metrics)

The metrics in this module score *retrieval* — did the right entity
appear at rank k. To compare *description* quality across LLM / doc /
code profile combinations, run the same scope under each profile via
`/run`, then use `/compare` to pivot the runs side-by-side and
`/compare --json out.json` to dump a long-format JSON document for
your notebook. The JSON shape is:

```json
{
  "schema_version": 1,
  "generated_at": "2026-05-01T22:00:00",
  "amx_version": "0.11.0",
  "run_count": 3,
  "run_summary":      [{"run_id": ..., "llm_profile": ..., ...}, ...],
  "per_column":       [{"schema": ..., "table": ..., "column": ...,
                        "run_id": ..., "description": ...,
                        "confidence": "high|medium|low",
                        "logprob_score": ..., "token_count": ...}, ...],
  "aggregate_metrics":[{"metric": ..., "run_id": ..., "value": ...}, ...]
}
```

Both arrays are long-format so they pivot cleanly with
`pd.DataFrame(payload["per_column"]).pivot(...)` for thesis charts.

Example notebook snippet:

```python
import json, pandas as pd
payload = json.load(open("compare.json"))
runs    = pd.DataFrame(payload["run_summary"])
per_col = pd.DataFrame(payload["per_column"])
agg     = pd.DataFrame(payload["aggregate_metrics"])

# Avg logprob by LLM profile
agg.merge(runs, on="run_id").query("metric == 'avg_logprob_score'") \
   .groupby("llm_profile")["value"].mean().plot.bar()
```
