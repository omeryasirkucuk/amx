"""Confidence eval harness — accuracy of each signal vs. ``accepted``."""

from __future__ import annotations


def _row(alts, accepted, **extra):
    """Build one already-parsed run_results row for the eval engine."""
    return {"alternatives": alts, "accepted": accepted, **extra}


def _alt(text, *, logprob=None, self_consistency=None, self_decl=None, judge=None, ensemble=None):
    return {
        "text": text,
        "scores": {
            "logprob": logprob,
            "self_consistency": self_consistency,
            "self_decl": self_decl,
            "judge": judge,
        },
        "ensemble": ensemble,
        "band": None,
    }


def test_zero_rows_returns_empty_metrics():
    from amx.eval.confidence import compute_metrics

    metrics = compute_metrics([])
    assert metrics["sample_count"] == 0
    assert metrics["signals"] == {}


def test_skips_rows_without_accepted():
    from amx.eval.confidence import compute_metrics

    rows = [
        _row([_alt("a", logprob=0.9), _alt("b", logprob=0.1)], accepted=None),
        _row([_alt("a", logprob=0.9), _alt("b", logprob=0.1)], accepted=""),
    ]
    metrics = compute_metrics(rows)
    assert metrics["sample_count"] == 0


def test_skips_rows_where_accepted_does_not_match_any_alternative():
    """If the user edited the description before applying, the text won't
    line up — those rows are excluded from the accuracy denominator."""
    from amx.eval.confidence import compute_metrics

    rows = [
        _row(
            [_alt("a", logprob=0.9), _alt("b", logprob=0.1)],
            accepted="user-edited text",
        ),
    ]
    metrics = compute_metrics(rows)
    assert metrics["sample_count"] == 0


def test_top1_top2_for_single_signal():
    """Two rows, both with logprob favouring alt-0; user accepted alt-0
    in row 1 and alt-1 in row 2. Top-1 accuracy = 50 %; top-2 = 100 %."""
    from amx.eval.confidence import compute_metrics

    rows = [
        _row(
            [_alt("a", logprob=0.9), _alt("b", logprob=0.1)],
            accepted="a",
        ),
        _row(
            [_alt("a", logprob=0.9), _alt("b", logprob=0.1)],
            accepted="b",
        ),
    ]
    metrics = compute_metrics(rows)
    assert metrics["sample_count"] == 2
    logprob_metrics = metrics["signals"]["logprob"]
    assert logprob_metrics["top1_accuracy"] == 0.5
    assert logprob_metrics["top2_accuracy"] == 1.0
    assert logprob_metrics["scored_rows"] == 2


def test_signal_skips_rows_where_scores_are_all_none():
    """When a signal isn't available on a row (e.g. Anthropic + logprob),
    that row doesn't count against the signal's denominator."""
    from amx.eval.confidence import compute_metrics

    rows = [
        _row(
            [_alt("a", logprob=0.9), _alt("b", logprob=0.1)],
            accepted="a",
        ),
        # Second row has logprob signal missing entirely.
        _row(
            [_alt("a"), _alt("b")],
            accepted="a",
        ),
    ]
    metrics = compute_metrics(rows)
    logprob_metrics = metrics["signals"]["logprob"]
    assert logprob_metrics["scored_rows"] == 1
    assert logprob_metrics["top1_accuracy"] == 1.0


def test_ensemble_signal_is_first_class_metric():
    """The aggregated ``ensemble`` score gets its own row in the report."""
    from amx.eval.confidence import compute_metrics

    rows = [
        _row(
            [_alt("a", ensemble=0.8), _alt("b", ensemble=0.2)],
            accepted="a",
        ),
        _row(
            [_alt("a", ensemble=0.3), _alt("b", ensemble=0.7)],
            accepted="b",
        ),
    ]
    metrics = compute_metrics(rows)
    ens = metrics["signals"]["ensemble"]
    assert ens["scored_rows"] == 2
    assert ens["top1_accuracy"] == 1.0


def test_render_markdown_emits_signal_section_per_signal():
    """The renderer produces a Markdown report with one section per
    signal that has at least one scored row."""
    from amx.eval.confidence import compute_metrics, render_markdown

    rows = [
        _row(
            [_alt("a", logprob=0.9, self_consistency=0.7, ensemble=0.8)],
            accepted="a",
        ),
    ]
    md = render_markdown(compute_metrics(rows))
    assert "# Confidence Evaluation Report" in md
    assert "## logprob" in md
    assert "## self_consistency" in md
    assert "## ensemble" in md
    assert "self_decl" not in md  # no scored rows → omitted
