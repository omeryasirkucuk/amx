"""Signal C — self-consistency / pairwise embedding similarity."""

from __future__ import annotations

import pytest


def test_n_equals_1_returns_degenerate_one():
    from amx.llm.confidence.self_consistency import score_per_alternative

    out = score_per_alternative(["only one"])
    assert out == [1.0]


def test_empty_list_returns_empty():
    from amx.llm.confidence.self_consistency import score_per_alternative

    assert score_per_alternative([]) == []


def test_three_paraphrases_plus_outlier_marks_outlier_lowest():
    """With three near-paraphrases and one semantic outlier, the outlier
    should receive the lowest pairwise similarity score."""
    pytest.importorskip("sentence_transformers")

    from amx.llm.confidence.self_consistency import score_per_alternative

    alts = [
        "Stores the customer's email address.",
        "Holds the email address for the customer.",
        "Records the customer email.",
        "Primary key for the orders table.",  # outlier
    ]
    scores = score_per_alternative(alts)
    assert len(scores) == 4
    assert all(s is not None for s in scores)
    # Outlier should be strictly lowest.
    assert min(scores) == scores[3]
    # Paraphrase trio's scores cluster above the outlier.
    assert scores[3] < min(scores[0], scores[1], scores[2])


def test_n_equals_2_returns_identical_scores():
    """Pairwise similarity for N=2 produces the same score for both alternatives."""
    pytest.importorskip("sentence_transformers")

    from amx.llm.confidence.self_consistency import score_per_alternative

    scores = score_per_alternative(["foo bar baz", "qux quux corge"])
    assert len(scores) == 2
    assert scores[0] == pytest.approx(scores[1], abs=1e-9)
