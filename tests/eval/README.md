# Retrieval evaluation harness

Measures AMX retrieval quality and gates CI against regression. Two
layers live here:

1. **Pure metrics** (`metrics.py`) — `hit@k`, `reciprocal_rank`, `MRR`,
   `precision@k`, `ndcg@k`. No IO, no state. Re-usable from notebooks.
2. **End-to-end gold-set runner** (`runner.py` + `fixtures/` +
   `baselines/`) — ingests a synthetic corpus into a fresh `RAGStore`,
   runs each gold-set question through the live retrieval +
   heuristic-rerank surface, scores, and compares the result against a
   committed baseline JSON. `test_baselines.py` fails the CI build
   if gated metrics regress beyond their tolerances.

## What's here

| File | Role |
| --- | --- |
| `metrics.py` | Pure scoring functions. No AMX deps. |
| `test_retrieval_metrics.py` | Unit tests for the metrics. |
| `test_smoke.py` | Fake-retriever smoke shape; copy when prototyping. |
| `runner.py` | End-to-end driver against `RAGStore`. |
| `generate_baselines.py` | Regenerates `baselines/docs_baseline.json`. |
| `test_baselines.py` | CI gate. Fails on regression. |
| `fixtures/docs/` | Synthetic Markdown corpus (~6 docs). |
| `fixtures/docs_gold.jsonl` | 20 question/expected-source/expected-content rows. |
| `baselines/docs_baseline.json` | Committed baseline; the CI floor. |

## Running

```bash
# Everything (CI default)
pytest tests/eval/

# Just the metric unit tests
pytest tests/eval/test_retrieval_metrics.py

# Just the gold-set runner smoke + baseline gate
pytest tests/eval/test_baselines.py
```

## Updating the baseline (intentional regression)

When a PR changes retrieval behaviour on purpose, regenerate the
baseline and commit it alongside the code change so reviewers see the
metric delta:

```bash
python -m tests.eval.generate_baselines --print
git add tests/eval/baselines/docs_baseline.json
```

CI rejects changes that drop `hit@3` below the baseline (hard floor),
`precision@5` by more than 2 pp, or `MRR` by more than 3 pp. `nDCG@5`
and `keyword_recall` are tracked but not gated.

## Adding a real-corpus eval

The gold set under `fixtures/docs/` is synthetic on purpose — it must
run offline in CI under `AMX_NO_NETWORK=1`. To eval against a real
corpus locally:

1. Point a script at your live `~/.amx/chroma_db/` (or a frozen copy).
2. Call `runner.run_docs_eval(persist_dir, fixture_dir=..., gold_path=...)`
   with your own corpus + gold set; the metric/aggregation logic is
   re-usable.
3. To compare embedding providers, run the same script under different
   `cfg.embedding.kind` settings and report per-query deltas.

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
