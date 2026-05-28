"""``review_single`` builds a ReviewResult on every branch.

Regression guard: ReviewResult is defined in ``amx.agents.orchestrator``
(circular with this module, so it's TYPE_CHECKING-only). The runtime
construction had no deferred import, so accept / skip / custom each
raised ``NameError: name 'ReviewResult' is not defined`` the moment a
user picked anything in the one-by-one review — the core human-in-the-
loop gate. These tests exercise all three branches.
"""

from __future__ import annotations

import pytest

import amx.agents._orchestrator.review as review
from amx.agents.base import Confidence, MetadataSuggestion


class _StubOrch:
    def __init__(self) -> None:
        self.recorded: list[tuple[int | None, str, str]] = []

    def _record_evaluation(
        self, result_id: int | None, chosen_description: str, evaluation: str
    ) -> None:
        self.recorded.append((result_id, chosen_description, evaluation))


def _suggestion() -> MetadataSuggestion:
    return MetadataSuggestion(
        schema="s",
        table="t",
        column="c",
        suggestions=["desc one", "desc two"],
        confidence=Confidence.HIGH,
        reasoning="",
        source="profile",
    )


def test_accept_builds_applied_result(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(review, "ask_choice", lambda *a, **k: "desc one")
    orch = _StubOrch()
    rr = review.review_single(orch, _suggestion(), is_table=False, asset_kind="table", result_id=1)
    assert rr.applied is True
    assert rr.final_description == "desc one"
    assert orch.recorded == [(1, "desc one", "accepted")]


def test_skip_builds_unapplied_result(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(review, "ask_choice", lambda *a, **k: "Skip")
    orch = _StubOrch()
    rr = review.review_single(orch, _suggestion(), is_table=False, asset_kind="table", result_id=2)
    assert rr.applied is False
    assert rr.final_description == ""
    assert orch.recorded == [(2, "", "skipped")]


def test_custom_text_builds_human_result(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(review, "ask_choice", lambda *a, **k: "Other (type your own)")
    monkeypatch.setattr(review, "ask", lambda *a, **k: "my own text")
    orch = _StubOrch()
    rr = review.review_single(orch, _suggestion(), is_table=False, asset_kind="table", result_id=3)
    assert rr.applied is True
    assert rr.final_description == "my own text"
    assert rr.source == "human"
    assert orch.recorded == [(3, "my own text", "custom")]
