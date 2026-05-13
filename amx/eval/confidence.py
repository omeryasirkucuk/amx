"""Confidence-signal evaluation harness.

Reads ``run_results`` rows where the user has chosen one of the
alternatives (``accepted IS NOT NULL``) and treats that choice as
ground truth. The single-signal pivot stores exactly one signal per
row on each alternative (``alt["signal"]`` + ``alt["score"]``), so we
group ``usable`` rows by signal and compute, per group:

* **Top-1 accuracy**: the signal's highest-scored alternative matches
  the user-accepted text.
* **Top-2 accuracy**: the user-accepted text is among the signal's
  two highest-scored alternatives.

These are the same metrics the chapter on alternative-selection
strategies quotes; the harness writes one Markdown report that compares
runs done with different ``confidence_signal`` settings side by side.

Rows whose alternatives have no signal recorded (legacy ``list[str]``
or old ensemble payload) are excluded — the harness only measures runs
produced under the single-signal pipeline.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def _rank_indices(scores: list[float | None]) -> list[int]:
    """Return indices of ``scores`` sorted highest-first, ties broken
    by original position."""
    return sorted(
        (i for i, s in enumerate(scores) if s is not None),
        key=lambda i: (-(scores[i] or 0.0), i),
    )


def _accepted_index(alternatives: list[dict[str, Any]], accepted: str) -> int | None:
    for idx, alt in enumerate(alternatives):
        if (alt.get("text") or "").strip() == accepted.strip():
            return idx
    return None


def _row_scores(alternatives: list[dict[str, Any]]) -> list[float | None]:
    """Return the per-alternative numeric score from the current shape."""
    out: list[float | None] = []
    for alt in alternatives:
        v = alt.get("score")
        out.append(float(v) if isinstance(v, (int, float)) else None)
    return out


def _row_signal(alternatives: list[dict[str, Any]]) -> str | None:
    """All alternatives in one row share the same ``signal`` (only one
    scorer ran). Pull the first non-empty value; return ``None`` for
    legacy rows that have no signal recorded."""
    for alt in alternatives:
        sig = alt.get("signal")
        if isinstance(sig, str) and sig:
            return sig
    return None


def compute_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Compute per-signal top-1 / top-2 accuracy across ``rows``.

    Each row must carry an ``alternatives`` list (parsed JSON) and an
    ``accepted`` string. Rows without a non-empty ``accepted``, whose
    ``accepted`` text does not match any alternative, or whose
    alternatives have no recorded ``signal`` (legacy / disabled
    confidence) are excluded.

    Returns a dict with::

        {
          "generated_at": "<ISO 8601 UTC>",
          "sample_count": <usable rows>,
          "signals": {
              "<signal>": {
                  "scored_rows": int,
                  "top1_accuracy": float,
                  "top2_accuracy": float,
              },
              …
          }
        }
    """
    # (alternatives, accepted_idx, signal)
    usable: list[tuple[list[dict[str, Any]], int, str]] = []
    for row in rows:
        accepted = (row.get("accepted") or "").strip()
        if not accepted:
            continue
        alts = row.get("alternatives") or []
        if not isinstance(alts, list) or not alts:
            continue
        idx = _accepted_index(alts, accepted)
        if idx is None:
            continue
        signal = _row_signal(alts)
        if signal is None:
            continue
        usable.append((alts, idx, signal))

    sample_count = len(usable)

    by_signal: dict[str, dict[str, int]] = {}
    for alts, accepted_idx, signal in usable:
        bucket = by_signal.setdefault(signal, {"scored_rows": 0, "top1": 0, "top2": 0})
        ranks = _rank_indices(_row_scores(alts))
        if not ranks:
            continue
        bucket["scored_rows"] += 1
        if ranks[0] == accepted_idx:
            bucket["top1"] += 1
        if accepted_idx in ranks[:2]:
            bucket["top2"] += 1

    signals: dict[str, Any] = {}
    for name, bucket in by_signal.items():
        scored = bucket["scored_rows"]
        if scored == 0:
            continue
        signals[name] = {
            "scored_rows": scored,
            "top1_accuracy": bucket["top1"] / scored,
            "top2_accuracy": bucket["top2"] / scored,
        }

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sample_count": sample_count,
        "signals": signals,
    }


def render_markdown(metrics: dict[str, Any]) -> str:
    """Turn ``compute_metrics`` output into a Markdown report."""
    lines = [
        "# Confidence Evaluation Report",
        "",
        f"- Generated at: `{metrics.get('generated_at', 'n/a')}`",
        f"- Sample count (rows with `accepted` matching an alternative): "
        f"**{metrics.get('sample_count', 0)}**",
        "",
    ]
    signals = metrics.get("signals") or {}
    if not signals:
        lines.append(
            "No usable rows for evaluation. Run `/analyze` with a "
            "`confidence_signal` configured and review at least one row."
        )
        return "\n".join(lines) + "\n"

    lines.extend(
        [
            "## Summary",
            "",
            "| Signal | Scored rows | Top-1 accuracy | Top-2 accuracy |",
            "|---|---:|---:|---:|",
        ]
    )
    for name, m in signals.items():
        lines.append(
            f"| `{name}` | {m['scored_rows']} | "
            f"{m['top1_accuracy']:.2%} | {m['top2_accuracy']:.2%} |"
        )
    lines.append("")

    for name, m in signals.items():
        lines.extend(
            [
                f"## {name}",
                "",
                f"- Scored rows: {m['scored_rows']}",
                f"- Top-1 accuracy: {m['top1_accuracy']:.4f}",
                f"- Top-2 accuracy: {m['top2_accuracy']:.4f}",
                "",
            ]
        )
    return "\n".join(lines)


__all__ = ["compute_metrics", "render_markdown"]
