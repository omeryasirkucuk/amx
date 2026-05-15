"""Regenerate ``tests/eval/baselines/*.json`` from the current code.

Invoke when an intentional retrieval-behaviour change is part of the
PR; commit the resulting JSON in the same commit so reviewers see the
metric delta alongside the code change.

Usage::

    python -m tests.eval.generate_baselines           # docs only
    python -m tests.eval.generate_baselines --print   # also echo metrics

The script writes to ``tests/eval/baselines/docs_baseline.json`` and
exits non-zero on any unexpected error (so a broken AMX install fails
loudly rather than silently writing an empty baseline).
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path

from tests.eval.runner import run_docs_eval

BASELINES_DIR = Path(__file__).parent / "baselines"
DOCS_BASELINE_PATH = BASELINES_DIR / "docs_baseline.json"

# Bumped by hand when the baseline file format changes incompatibly.
# CI's baseline reader checks this so an old runner against a new
# baseline file (or vice-versa) fails loudly rather than scoring stale
# metrics against fresh data.
BASELINE_SCHEMA_VERSION = 1


def _amx_version() -> str:
    try:
        from importlib.metadata import version

        return version("amx-cli")
    except Exception:
        return "unknown"


def _write_docs_baseline(verbose: bool) -> dict[str, float | int]:
    with tempfile.TemporaryDirectory(prefix="amx-eval-baseline-") as tmp:
        report = run_docs_eval(Path(tmp) / "chroma")
    metrics = report.to_baseline_dict()

    payload = {
        "schema_version": BASELINE_SCHEMA_VERSION,
        "pipeline": "docs",
        "embedder": {
            "provider": "minilm",
            "model": "minilm-l6-v2",
            "dimension": 384,
        },
        "amx_version": _amx_version(),
        "metrics": metrics,
    }
    BASELINES_DIR.mkdir(parents=True, exist_ok=True)
    DOCS_BASELINE_PATH.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    if verbose:
        print(f"wrote {DOCS_BASELINE_PATH}")
        for k, v in metrics.items():
            print(f"  {k}: {v}")
    return metrics


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--print", action="store_true", help="echo metrics after writing")
    args = parser.parse_args(argv)
    try:
        _write_docs_baseline(verbose=bool(args.print))
    except Exception as exc:  # noqa: BLE001 — top-level CLI guard
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
