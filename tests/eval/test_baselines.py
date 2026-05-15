"""CI gate: runs the gold-set evaluation and asserts no regression vs
the committed baseline.

The baseline lives at ``tests/eval/baselines/docs_baseline.json``. When
a PR intentionally changes retrieval behaviour, regenerate the baseline
by running::

    python -m tests.eval.generate_baselines

and commit the updated JSON in the same PR. The CI workflow does not
auto-update; a baseline change is always an explicit author action.

Tolerances mirror the rollout plan (``docs/superpowers/specs`` audit,
§4 — Evaluation Harness):

- ``hit_at_3``: must not regress at all (hard floor).
- ``precision_at_5``: 2 pp tolerance (reranker tweaks are noisy).
- ``mrr``: 3 pp tolerance (small-scale ranking changes have variance).
- ``ndcg_at_5`` / ``keyword_recall``: tracked, not gated.

Skipped when the baseline file is missing — useful during the initial
PR that introduces the harness, before a baseline has been committed.
The skip turns into a hard requirement once the baseline lands.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.eval.runner import run_docs_eval

BASELINES_DIR = Path(__file__).parent / "baselines"
DOCS_BASELINE_PATH = BASELINES_DIR / "docs_baseline.json"

# Tolerance: how much each metric is allowed to drop relative to the
# baseline before the test fails. ``hit_at_3`` is 0.0 — any drop fails.
TOLERANCES: dict[str, float] = {
    "hit_at_3": 0.0,
    "precision_at_5": 0.02,
    "mrr": 0.03,
}


@pytest.mark.skipif(
    not DOCS_BASELINE_PATH.exists(),
    reason="docs baseline not yet committed; run tests/eval/generate_baselines.py",
)
def test_docs_eval_no_regression_vs_baseline(tmp_path: Path) -> None:
    """Run the gold set against the current code; fail if gated metrics
    regress beyond their tolerances."""
    with DOCS_BASELINE_PATH.open("r", encoding="utf-8") as fh:
        baseline = json.load(fh)
    baseline_metrics = baseline.get("metrics", {})
    assert baseline_metrics, "baseline JSON must contain a 'metrics' object"

    report = run_docs_eval(tmp_path / "chroma")
    current = report.to_baseline_dict()

    # Cross-check the gold set hasn't grown out from under the baseline
    # without an intentional rebaseline. ``n_queries`` shifting silently
    # would let a hand-trimmed gold set fake an "improvement."
    assert current["n_queries"] == baseline_metrics["n_queries"], (
        f"Gold-set size changed ({baseline_metrics['n_queries']} -> {current['n_queries']}) "
        "without rebaselining. Regenerate tests/eval/baselines/docs_baseline.json."
    )

    failures: list[str] = []
    for metric, tolerance in TOLERANCES.items():
        baseline_value = float(baseline_metrics[metric])
        current_value = float(current[metric])
        floor = baseline_value - tolerance
        if current_value < floor:
            failures.append(
                f"{metric}: {current_value:.4f} < {floor:.4f} "
                f"(baseline {baseline_value:.4f}, tolerance {tolerance:.2f})"
            )

    if failures:
        details = "\n  ".join(failures)
        pytest.fail(
            "Docs RAG retrieval regressed vs baseline:\n  "
            + details
            + "\n\nIf this regression is intentional, regenerate the baseline:\n"
            "  python -m tests.eval.generate_baselines\n"
            "and commit tests/eval/baselines/docs_baseline.json in the same PR."
        )


def test_docs_eval_runner_smoke(tmp_path: Path) -> None:
    """Always-on smoke check: the runner must execute end-to-end and
    return a non-trivial report regardless of whether a baseline file
    exists. Runs in CI on every PR — catches a broken runner before the
    baseline test (which is conditionally skipped) ever sees it."""
    report = run_docs_eval(tmp_path / "chroma")
    assert report.n_queries > 0, "runner returned zero queries"
    assert report.per_query, "runner returned an empty per-query list"
    # The fixture set is small but the bundled MiniLM is good enough to
    # surface SOME relevant document for at least half the questions.
    # Anything below this is a runner wiring bug, not a model-quality
    # observation.
    assert report.hit_at_5 >= 0.5, (
        f"hit@5 collapsed to {report.hit_at_5:.2f}; "
        "suggests fixture ingest or query wiring is broken, not a "
        "retrieval-quality issue."
    )
