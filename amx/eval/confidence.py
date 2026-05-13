"""Confidence-signal evaluation harness.

Reads ``run_results`` rows where the user has chosen one of the
alternatives (``accepted IS NOT NULL``) and treats that choice as
ground truth. For each available confidence signal — logprob,
self-consistency, self-declaration, judge, and the combined ensemble
— computes:

* **Top-1 accuracy**: the signal's highest-scored alternative matches
  the user-accepted text.
* **Top-2 accuracy**: the user-accepted text is among the signal's
  two highest-scored alternatives.

These are the same metrics the thesis evaluation chapter quotes, so
the harness doubles as a research artefact. Rows where a particular
signal has no scores (e.g. Anthropic + logprob) are excluded from
that signal's denominator only; other signals continue to count.

The Markdown renderer turns the metric dict into a report suitable
for pasting into the chapter or sharing with reviewers.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

_SIGNAL_KEYS = ("logprob", "self_consistency", "self_decl", "judge")


def _row_score_for_signal(alternatives: list[dict[str, Any]], signal: str) -> list[float | None]:
    """Pull one signal's scores out of a parsed alternatives list.

    ``ensemble`` lives on the row itself (one field per alternative);
    every other key lives under ``alternative["scores"]``.
    """
    out: list[float | None] = []
    for alt in alternatives:
        if signal == "ensemble":
            v = alt.get("ensemble")
        else:
            scores = alt.get("scores") or {}
            v = scores.get(signal)
        out.append(float(v) if isinstance(v, (int, float)) else None)
    return out


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


def compute_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Compute per-signal top-1 / top-2 accuracy across ``rows``.

    Each row must carry an ``alternatives`` list (parsed JSON) and an
    ``accepted`` string. Rows without a non-empty ``accepted``, or
    whose ``accepted`` text does not match any alternative, are
    excluded from the harness entirely (``sample_count`` denominator).

    Returns a dict with:

        {
          "generated_at": <ISO 8601 UTC timestamp>,
          "sample_count": <usable rows>,
          "signals": {
              "<signal>": {
                  "scored_rows": <rows where signal had at least one non-None score>,
                  "top1_accuracy": <float in [0, 1] or None>,
                  "top2_accuracy": <float in [0, 1] or None>,
              },
              …
          }
        }
    """
    usable: list[tuple[list[dict[str, Any]], int]] = []
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
        usable.append((alts, idx))

    sample_count = len(usable)
    signals: dict[str, Any] = {}
    for signal in (*_SIGNAL_KEYS, "ensemble"):
        scored = 0
        top1 = 0
        top2 = 0
        for alts, accepted_idx in usable:
            ranks = _rank_indices(_row_score_for_signal(alts, signal))
            if not ranks:
                continue
            scored += 1
            if ranks[0] == accepted_idx:
                top1 += 1
            if accepted_idx in ranks[:2]:
                top2 += 1
        if scored == 0:
            continue
        signals[signal] = {
            "scored_rows": scored,
            "top1_accuracy": top1 / scored,
            "top2_accuracy": top2 / scored,
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
        lines.append("No usable rows for evaluation. Apply some reviewed runs first.")
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
