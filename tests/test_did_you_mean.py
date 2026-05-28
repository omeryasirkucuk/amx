"""Mistyped slash commands get a 'Did you mean /X?' pointer.

The unknown-command path used to print a flat 'Unknown command' with no
suggestion, even though the full command catalogue is right there in the
registry. Now a near-miss typo is matched against every command head +
namespace.
"""

from __future__ import annotations

import pytest

from amx.cli_support.session import _did_you_mean


@pytest.mark.parametrize(
    ("typo", "expected"),
    [
        ("dbb", "/db"),
        ("conect", "/connect"),
        ("runn", "/run"),
        ("lineag", "/lineage"),
        ("tabels", "/tables"),
    ],
)
def test_near_miss_suggests_the_real_command(typo: str, expected: str) -> None:
    assert _did_you_mean(typo) == f" Did you mean {expected}?"


def test_takes_the_head_word_for_multiword_input() -> None:
    # site B passes the whole line; the head word is what we match
    assert _did_you_mean("dbb tables") == " Did you mean /db?"


def test_no_close_match_returns_empty() -> None:
    assert _did_you_mean("xyzzy") == ""
    assert _did_you_mean("") == ""
    assert _did_you_mean("   ") == ""
