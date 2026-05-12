"""Unit tests for the bulk-review individual-loop key navigation (PR B)."""

from __future__ import annotations

from amx.cli_support.review_keynav import (
    KEYNAV_HELP_LINES,
    NavResult,
    format_help,
    parse_nav_command,
)


def test_empty_command_advances_one() -> None:
    out = parse_nav_command("", position=2, queue_len=10)
    assert out.position == 3
    assert out.action == "next"


def test_n_and_j_aliases_advance() -> None:
    for cmd in ("n", "j", "N", "J", "next"):
        out = parse_nav_command(cmd, position=0, queue_len=5)
        assert out.position == 1
        assert out.action == "next"


def test_review_loop_handles_p_command() -> None:
    """Typing 'p' at the review prompt steps backward one row."""
    out = parse_nav_command("p", position=5, queue_len=10)
    assert out.position == 4
    assert out.action == "prev"


def test_k_alias_steps_back() -> None:
    out = parse_nav_command("k", position=3, queue_len=10)
    assert out == NavResult(position=2, action="prev")


def test_prev_at_zero_clamps() -> None:
    out = parse_nav_command("p", position=0, queue_len=10)
    assert out.position == 0


def test_next_at_end_clamps() -> None:
    out = parse_nav_command("n", position=9, queue_len=10)
    assert out.position == 9


def test_review_loop_handles_g_jump() -> None:
    """Typing 'g 5' jumps to row 5 (1-indexed → position 4)."""
    out = parse_nav_command("g 5", position=0, queue_len=20)
    assert out.position == 4
    assert out.action == "goto"
    assert out.payload == "5"


def test_g_alone_opens_subprompt() -> None:
    out = parse_nav_command("g", position=2, queue_len=10)
    assert out.action == "goto"
    assert out.payload == ""
    # Position stays so the caller can re-render after the sub-prompt.
    assert out.position == 2


def test_goto_clamps_beyond_end() -> None:
    out = parse_nav_command("g 99", position=0, queue_len=10)
    assert out.position == 9
    assert out.action == "goto"


def test_capital_G_jumps_last() -> None:
    out = parse_nav_command("G", position=0, queue_len=12)
    assert out.position == 11
    assert out.action == "last"


def test_review_loop_filter_sub_prompt() -> None:
    """Typing '/sales' surfaces a filter action with the pattern as payload."""
    out = parse_nav_command("/sales", position=2, queue_len=10)
    assert out.action == "filter"
    assert out.payload == "sales"
    assert out.position == 2  # position unchanged — caller re-narrows queue


def test_help_action() -> None:
    out = parse_nav_command("?", position=4, queue_len=10)
    assert out.action == "help"
    assert out.position == 4


def test_unknown_command_keeps_position() -> None:
    out = parse_nav_command("xyzzy", position=3, queue_len=10)
    assert out.action == "unknown"
    assert out.position == 3


def test_format_help_contains_every_key() -> None:
    text = format_help()
    for line in KEYNAV_HELP_LINES:
        assert line.split()[0] in text


def test_empty_queue_clamps_to_zero() -> None:
    """Defensive: an empty queue must not produce a negative position."""
    out = parse_nav_command("G", position=0, queue_len=0)
    assert out.position == 0
