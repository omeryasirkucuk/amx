"""Unit tests for the bulk-review interactive picker (PR B)."""

from __future__ import annotations

import pytest

from amx.cli_support import review_picker
from amx.cli_support.review_picker import (
    bulk_confirm,
    paginate_rows,
    paginate_with_prompt,
    parse_row_spec,
    pick_rows,
)

# ── parse_row_spec ───────────────────────────────────────────────────────────


def test_parse_row_spec_single_indices() -> None:
    assert parse_row_spec("1,3,5", max_n=10) == [0, 2, 4]


def test_parse_row_spec_ranges() -> None:
    assert parse_row_spec("1-3,7-9", max_n=10) == [0, 1, 2, 6, 7, 8]


def test_parse_row_spec_all() -> None:
    assert parse_row_spec("all", max_n=4) == [0, 1, 2, 3]


def test_parse_row_spec_all_case_insensitive() -> None:
    assert parse_row_spec("ALL", max_n=2) == [0, 1]


def test_parse_row_spec_dedupe_preserves_first_seen_order() -> None:
    assert parse_row_spec("5,1,3,1,5", max_n=10) == [4, 0, 2]


def test_parse_row_spec_reversed_range_normalised() -> None:
    assert parse_row_spec("3-1", max_n=10) == [0, 1, 2]


def test_parse_row_spec_invalid_token_raises() -> None:
    with pytest.raises(ValueError, match="Invalid token"):
        parse_row_spec("foo,bar", max_n=10)


def test_parse_row_spec_out_of_bounds_raises() -> None:
    with pytest.raises(ValueError, match="out of bounds"):
        parse_row_spec("1,99", max_n=10)


def test_parse_row_spec_empty_raises() -> None:
    with pytest.raises(ValueError, match="Empty row spec"):
        parse_row_spec("   ", max_n=10)


# ── pick_rows fzf fallback ───────────────────────────────────────────────────


def test_fzf_fallback_to_numbered_prompt(monkeypatch: pytest.MonkeyPatch) -> None:
    """When fzf is not in PATH the picker uses the numbered prompt."""
    monkeypatch.setattr(review_picker, "fzf_available", lambda: False)
    printed: list[str] = []
    answers = iter(["1,3"])
    picked = pick_rows(
        ["a", "b", "c"],
        print_fn=printed.append,
        input_fn=lambda _prompt: next(answers),
    )
    assert picked == [0, 2]
    # The numbered listing was rendered.
    assert any(line.startswith("1)") for line in printed)


def test_pick_rows_empty_labels_returns_empty() -> None:
    assert pick_rows([]) == []


def test_pick_rows_numbered_prompt_reprompts_on_invalid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A bad first answer surfaces the ValueError + the prompt repeats."""
    monkeypatch.setattr(review_picker, "fzf_available", lambda: False)
    answers = iter(["bogus", "2"])
    printed: list[str] = []
    picked = pick_rows(
        ["a", "b", "c"],
        print_fn=printed.append,
        input_fn=lambda _p: next(answers),
    )
    assert picked == [1]
    assert any("Invalid token" in p for p in printed)


# ── Pagination ───────────────────────────────────────────────────────────────


def test_paginate_rows_yields_full_list_when_disabled() -> None:
    pages = list(paginate_rows([1, 2, 3], page_size=0))
    assert pages == [(1, 1, [1, 2, 3])]


def test_paginate_rows_splits_evenly() -> None:
    pages = list(paginate_rows([1, 2, 3, 4, 5], page_size=2))
    assert pages == [(1, 3, [1, 2]), (2, 3, [3, 4]), (3, 3, [5])]


def test_paginate_with_prompt_stops_on_q(monkeypatch: pytest.MonkeyPatch) -> None:
    rendered: list[tuple[int, int, list[int]]] = []
    answers = iter(["q"])
    paginate_with_prompt(
        [1, 2, 3, 4],
        page_size=2,
        render_page=lambda p, t, sl: rendered.append((p, t, list(sl))),
        input_fn=lambda _: next(answers),
        print_fn=lambda _: None,
    )
    # Only the first page rendered before the user quit.
    assert rendered == [(1, 2, [1, 2])]


def test_paginate_with_prompt_runs_through_when_space(monkeypatch: pytest.MonkeyPatch) -> None:
    rendered: list[tuple[int, int, list[int]]] = []
    answers = iter(["", ""])
    paginate_with_prompt(
        [1, 2, 3, 4, 5],
        page_size=2,
        render_page=lambda p, t, sl: rendered.append((p, t, list(sl))),
        input_fn=lambda _: next(answers, ""),
        print_fn=lambda _: None,
    )
    # All three pages rendered.
    assert [p for (p, _, _) in rendered] == [1, 2, 3]


# ── Bulk-action confirmation ─────────────────────────────────────────────────


def test_bulk_confirm_returns_true_on_yes() -> None:
    assert bulk_confirm(
        action="accept",
        count=3,
        sample=["a", "b", "c"],
        input_fn=lambda _: "yes",
        print_fn=lambda _: None,
    )


def test_bulk_confirm_returns_false_on_no() -> None:
    assert not bulk_confirm(
        action="accept",
        count=3,
        sample=["a"],
        input_fn=lambda _: "no",
        print_fn=lambda _: None,
    )


def test_bulk_confirm_rejects_bare_y() -> None:
    """A bare ``y`` is NOT a confirmation — we want explicit ``yes``."""
    assert not bulk_confirm(
        action="apply",
        count=1,
        sample=[],
        input_fn=lambda _: "y",
        print_fn=lambda _: None,
    )


def test_bulk_confirm_renders_extra_warning() -> None:
    captured: list[str] = []
    bulk_confirm(
        action="apply",
        count=2,
        sample=[],
        extra_warning="DB write is permanent.",
        input_fn=lambda _: "no",
        print_fn=captured.append,
    )
    assert any("DB write is permanent." in line for line in captured)


def test_bulk_confirm_returns_false_on_eof() -> None:
    def _raise_eof(_prompt: str) -> str:
        raise EOFError

    assert not bulk_confirm(
        action="accept",
        count=1,
        sample=[],
        input_fn=_raise_eof,
        print_fn=lambda _: None,
    )
