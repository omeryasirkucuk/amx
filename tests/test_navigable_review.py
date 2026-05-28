"""Navigable one-by-one review loop (``_navigable_review``).

Adds a position counter and back/goto/filter/help navigation to the
per-column review while preserving the old "every item gets a decision"
termination. Enter accepts the top suggestion and advances. Because the
loop is interactive, these tests drive it by scripting the ``ask`` reader
(monkeypatched) — covering each navigation path, re-decide-on-revisit,
invalid-input re-prompt, and the non-interactive step-budget backstop.
"""

from __future__ import annotations

import pytest

import amx.agents._orchestrator.review as review
from amx.agents.base import Confidence, MetadataSuggestion


class _StubOrch:
    def __init__(self) -> None:
        self.recorded: list[tuple[int | None, str, str]] = []

    def _record_evaluation(self, result_id, chosen_description, evaluation) -> None:  # noqa: ANN001
        self.recorded.append((result_id, chosen_description, evaluation))


def _items(*cols: str) -> list[MetadataSuggestion]:
    out = []
    for i, col in enumerate(cols):
        out.append(
            MetadataSuggestion(
                schema="s",
                table="t",
                column=col,
                suggestions=[f"{col}-first", f"{col}-second"],
                confidence=Confidence.HIGH,
                reasoning="",
                source="profile",
            )
        )
    return out


def _script(monkeypatch: pytest.MonkeyPatch, inputs: list[str]) -> None:
    it = iter(inputs)
    monkeypatch.setattr(review, "ask", lambda *a, **k: next(it))


def _rid_map(items: list[MetadataSuggestion]) -> dict[str | None, int]:
    return {s.column: i + 1 for i, s in enumerate(items)}


def test_enter_accepts_top_and_advances_through_all(monkeypatch: pytest.MonkeyPatch) -> None:
    items = _items("a", "b")
    _script(monkeypatch, ["", ""])  # Enter, Enter
    orch = _StubOrch()
    out = review._navigable_review(orch, items, "table", _rid_map(items))
    assert [r.final_description for r in out] == ["a-first", "b-first"]
    assert all(r.applied for r in out)


def test_number_selects_specific_suggestion(monkeypatch: pytest.MonkeyPatch) -> None:
    items = _items("a")
    _script(monkeypatch, ["2"])  # pick the 2nd suggestion
    out = review._navigable_review(_StubOrch(), items, "table", _rid_map(items))
    assert out[0].final_description == "a-second"


def test_skip_shortcut_marks_unapplied(monkeypatch: pytest.MonkeyPatch) -> None:
    items = _items("a")
    _script(monkeypatch, ["s"])
    out = review._navigable_review(_StubOrch(), items, "table", _rid_map(items))
    assert out[0].applied is False
    assert out[0].final_description == ""


def test_prev_navigation_allows_redeciding(monkeypatch: pytest.MonkeyPatch) -> None:
    items = _items("a", "b")
    # item a -> pick "2" (advance to b); at b -> "p" (back to a); at a -> "1"
    # (overwrite to first); advance to b; Enter accepts b.
    _script(monkeypatch, ["2", "p", "1", ""])
    out = review._navigable_review(_StubOrch(), items, "table", _rid_map(items))
    assert out[0].final_description == "a-first"  # overwritten on revisit
    assert out[1].final_description == "b-first"


def test_goto_jumps_to_row(monkeypatch: pytest.MonkeyPatch) -> None:
    items = _items("a", "b", "c")
    # 'g 3' jumps to row 3 (c); decide it; then Enter through the rest.
    _script(monkeypatch, ["g 3", "1", "", ""])
    out = review._navigable_review(_StubOrch(), items, "table", _rid_map(items))
    assert out[2].final_description == "c-first"
    assert all(r.applied for r in out)


def test_filter_narrows_then_restores(monkeypatch: pytest.MonkeyPatch) -> None:
    items = _items("alpha", "beta", "gamma")
    # Filter to 'beta' only, decide it; cleared filter (no match path) keeps
    # all, then Enter through remaining undecided.
    _script(monkeypatch, ["/beta", "1", "", ""])
    out = review._navigable_review(_StubOrch(), items, "table", _rid_map(items))
    # beta decided via filter, the rest via Enter — all decided, order kept.
    assert [r.column for r in out] == ["alpha", "beta", "gamma"]
    assert out[1].final_description == "beta-first"


def test_invalid_input_reprompts_without_deciding(monkeypatch: pytest.MonkeyPatch) -> None:
    items = _items("a")
    _script(monkeypatch, ["zzz", "1"])  # garbage then a valid pick
    orch = _StubOrch()
    out = review._navigable_review(orch, items, "table", _rid_map(items))
    assert out[0].final_description == "a-first"
    # Exactly one decision recorded despite two prompts.
    assert len(orch.recorded) == 1


def test_step_budget_terminates_on_pathological_input(monkeypatch: pytest.MonkeyPatch) -> None:
    # A caller that only ever navigates (never decides) must not loop
    # forever — the step budget skips the remainder and returns.
    items = _items("a", "b")
    monkeypatch.setattr(review, "ask", lambda *a, **k: "n")  # always "next"
    out = review._navigable_review(_StubOrch(), items, "table", _rid_map(items))
    assert len(out) == 2
    assert all(r.applied is False for r in out)  # forced-skipped at the budget
