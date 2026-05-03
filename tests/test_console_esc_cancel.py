"""Tests for Esc-as-soft-cancel behaviour on the prompt helpers.

When the user presses Esc inside any AMX prompt
(``ask`` / ``ask_password`` / ``ask_choice`` / ``ask_multi_choice``
/ ``confirm``), the underlying ``_safe_pt_prompt`` raises
:class:`PromptCancelled`. Each helper must catch that, surface a
"Cancelled." note (so the keystroke does not look like a no-op), and
return the appropriate sentinel for that helper's contract:

* ``ask`` / ``ask_password`` → empty string
* ``ask_choice`` → empty string
* ``ask_multi_choice`` → empty list
* ``confirm`` → False (treated as 'no' — the safe answer for every
  AMX confirm() which is always a destructive or scoping decision)

These tests stub ``_safe_pt_prompt`` to raise the exception; the real
prompt_toolkit binding is not exercised under pytest because pt_prompt
needs a TTY that pytest does not provide. The keystroke→exception
glue is verified manually in the user-facing flows.
"""

from __future__ import annotations

from unittest.mock import patch

from amx.utils import console as c
from amx.utils.console import (
    PromptCancelled,
    ask,
    ask_choice,
    ask_multi_choice,
    ask_password,
    confirm,
)


def test_ask_returns_empty_on_esc() -> None:
    with patch.object(c, "_safe_pt_prompt", side_effect=PromptCancelled()):
        assert ask("name") == ""


def test_ask_password_returns_empty_on_esc() -> None:
    with patch.object(c, "_safe_pt_prompt", side_effect=PromptCancelled()):
        assert ask_password("token") == ""


def test_ask_choice_returns_empty_on_esc() -> None:
    with patch.object(c, "_safe_pt_prompt", side_effect=PromptCancelled()):
        # The default would normally come back if the user pressed Enter
        # on an empty buffer; Esc should bypass the default and return
        # the empty string so callers see "no choice made".
        assert ask_choice("pick", ["a", "b", "c"], default="b") == ""


def test_ask_multi_choice_returns_empty_list_on_esc() -> None:
    with patch.object(c, "_safe_pt_prompt", side_effect=PromptCancelled()):
        assert ask_multi_choice("pick some", ["a", "b", "c"]) == []


def test_confirm_returns_false_on_esc_regardless_of_default() -> None:
    """Esc on a confirm() always means 'do not proceed', even when the
    default would have been True (Enter → Yes). This is intentional —
    every confirm() in AMX is destructive or scoping; explicit cancel
    is the safe interpretation."""
    with patch.object(c, "_safe_pt_prompt", side_effect=PromptCancelled()):
        assert confirm("Apply 200 comments?", default=True) is False
        assert confirm("Drop schema?", default=False) is False


def test_prompt_cancelled_is_distinct_from_empty_input() -> None:
    """``PromptCancelled`` must be a separate exception type — callers
    who DO want to tell cancel apart from empty-Enter can catch it
    around their own _safe_pt_prompt call."""
    assert issubclass(PromptCancelled, Exception)
    assert not issubclass(PromptCancelled, KeyboardInterrupt)
    assert not issubclass(PromptCancelled, SystemExit)
