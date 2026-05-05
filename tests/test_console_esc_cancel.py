"""Tests for Esc-as-soft-cancel behaviour on the prompt helpers.

When the user presses Esc inside any AMX prompt
(``ask`` / ``ask_password`` / ``ask_choice`` / ``ask_multi_choice``
/ ``confirm``), the underlying ``_safe_pt_prompt`` raises
:class:`PromptCancelled`. Pre-0.12.9 each helper caught that and
returned an empty/false sentinel — but multi-step wizards never
checked the sentinel, so Esc let the wizard march on with garbage
state (``/add-db-profile`` would save a profile with an empty
name). The contract is now: helpers re-raise ``PromptCancelled``,
and the interactive session dispatcher prints a single "Cancelled."
at the wizard boundary.

These tests stub ``_safe_pt_prompt`` to raise the exception; the
real prompt_toolkit binding is not exercised under pytest because
pt_prompt needs a TTY that pytest does not provide. The
keystroke→exception glue is verified manually in the user-facing
flows.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from amx.utils import console as c
from amx.utils.console import (
    PromptCancelled,
    ask,
    ask_choice,
    ask_multi_choice,
    ask_password,
    confirm,
)


def test_ask_reraises_on_esc() -> None:
    with patch.object(c, "_safe_pt_prompt", side_effect=PromptCancelled()):
        with pytest.raises(PromptCancelled):
            ask("name")


def test_ask_password_reraises_on_esc() -> None:
    with patch.object(c, "_safe_pt_prompt", side_effect=PromptCancelled()):
        with pytest.raises(PromptCancelled):
            ask_password("token")


def test_ask_choice_reraises_on_esc() -> None:
    with patch.object(c, "_safe_pt_prompt", side_effect=PromptCancelled()):
        with pytest.raises(PromptCancelled):
            ask_choice("pick", ["a", "b", "c"], default="b")


def test_ask_multi_choice_reraises_on_esc() -> None:
    with patch.object(c, "_safe_pt_prompt", side_effect=PromptCancelled()):
        with pytest.raises(PromptCancelled):
            ask_multi_choice("pick some", ["a", "b", "c"])


def test_confirm_reraises_on_esc() -> None:
    """Esc on a confirm() now aborts the surrounding wizard rather
    than silently returning False. Pre-0.12.9 the False return made
    intermediate-step Esc indistinguishable from "no" and let the
    wizard keep walking with partial state."""
    with patch.object(c, "_safe_pt_prompt", side_effect=PromptCancelled()):
        with pytest.raises(PromptCancelled):
            confirm("Apply 200 comments?", default=True)
        with pytest.raises(PromptCancelled):
            confirm("Drop schema?", default=False)


def test_prompt_cancelled_is_distinct_from_empty_input() -> None:
    """``PromptCancelled`` must be a separate exception type — callers
    who DO want to tell cancel apart from empty-Enter can catch it
    around their own _safe_pt_prompt call."""
    assert issubclass(PromptCancelled, Exception)
    assert not issubclass(PromptCancelled, KeyboardInterrupt)
    assert not issubclass(PromptCancelled, SystemExit)
