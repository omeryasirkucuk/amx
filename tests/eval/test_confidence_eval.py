"""Confidence eval harness — per-signal top-1 / top-2 accuracy."""

from __future__ import annotations


def _row(alts, accepted, **extra):
    return {"alternatives": alts, "accepted": accepted, **extra}


def _alt(text, *, signal="self_consistency", score=None, band=None):
    return {"text": text, "signal": signal, "score": score, "band": band}


def test_zero_rows_returns_empty_metrics():
    from amx.eval.confidence import compute_metrics

    metrics = compute_metrics([])
    assert metrics["sample_count"] == 0
    assert metrics["signals"] == {}


def test_skips_rows_without_accepted():
    from amx.eval.confidence import compute_metrics

    rows = [
        _row([_alt("a", score=0.9), _alt("b", score=0.1)], accepted=None),
        _row([_alt("a", score=0.9), _alt("b", score=0.1)], accepted=""),
    ]
    assert compute_metrics(rows)["sample_count"] == 0


def test_skips_rows_where_accepted_does_not_match_any_alternative():
    from amx.eval.confidence import compute_metrics

    rows = [
        _row(
            [_alt("a", score=0.9), _alt("b", score=0.1)],
            accepted="user-edited text",
        ),
    ]
    assert compute_metrics(rows)["sample_count"] == 0


def test_skips_rows_without_signal_recorded():
    """Legacy / disabled-confidence rows have ``signal=None`` on their
    alternatives; the harness ignores them."""
    from amx.eval.confidence import compute_metrics

    rows = [
        _row(
            [
                _alt("a", signal=None, score=None),
                _alt("b", signal=None, score=None),
            ],
            accepted="a",
        ),
    ]
    metrics = compute_metrics(rows)
    assert metrics["sample_count"] == 0
    assert metrics["signals"] == {}


def test_top1_top2_for_single_signal():
    """Two rows, both with self_consistency scores. User accepted
    alt-0 in row 1 (matches signal's top-1) and alt-1 in row 2
    (matches signal's top-2). → top-1 50 %, top-2 100 %."""
    from amx.eval.confidence import compute_metrics

    rows = [
        _row(
            [
                _alt("a", signal="self_consistency", score=0.9),
                _alt("b", signal="self_consistency", score=0.1),
            ],
            accepted="a",
        ),
        _row(
            [
                _alt("a", signal="self_consistency", score=0.9),
                _alt("b", signal="self_consistency", score=0.1),
            ],
            accepted="b",
        ),
    ]
    metrics = compute_metrics(rows)
    assert metrics["sample_count"] == 2
    sc = metrics["signals"]["self_consistency"]
    assert sc["scored_rows"] == 2
    assert sc["top1_accuracy"] == 0.5
    assert sc["top2_accuracy"] == 1.0


def test_two_signals_grouped_separately():
    """Each row carries its own active ``signal`` field; the harness
    groups by signal and reports one section per group."""
    from amx.eval.confidence import compute_metrics

    rows = [
        _row(
            [
                _alt("a", signal="logprob", score=0.9),
                _alt("b", signal="logprob", score=0.1),
            ],
            accepted="a",
        ),
        _row(
            [
                _alt("a", signal="self_decl", score=0.3),
                _alt("b", signal="self_decl", score=0.9),
            ],
            accepted="b",
        ),
    ]
    metrics = compute_metrics(rows)
    assert metrics["sample_count"] == 2
    assert set(metrics["signals"]) == {"logprob", "self_decl"}
    assert metrics["signals"]["logprob"]["top1_accuracy"] == 1.0
    assert metrics["signals"]["self_decl"]["top1_accuracy"] == 1.0


def test_render_markdown_emits_signal_section_per_signal():
    from amx.eval.confidence import compute_metrics, render_markdown

    rows = [
        _row(
            [
                _alt("a", signal="self_consistency", score=0.9),
                _alt("b", signal="self_consistency", score=0.1),
            ],
            accepted="a",
        ),
    ]
    md = render_markdown(compute_metrics(rows))
    assert "# Confidence Evaluation Report" in md
    assert "## self_consistency" in md
    # Only one signal in the row set → no other signal sections leak in.
    assert "## logprob" not in md
    assert "## self_decl" not in md
    assert "## judge" not in md


def test_render_markdown_emits_friendly_empty_state():
    from amx.eval.confidence import compute_metrics, render_markdown

    md = render_markdown(compute_metrics([]))
    assert "No usable rows" in md
