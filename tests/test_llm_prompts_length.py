"""Tests for the shared verbosity → length-rule helper.

Both ``ProfileAgent`` (bulk runs) and the Studio single-shot generate
endpoints now import from :mod:`amx.llm.prompts.length`, so any
behavioural drift here would silently desync the two paths.
"""

from __future__ import annotations

from amx.llm.prompts.length import (
    DESCRIPTION_LENGTH_RULES,
    length_rule,
    per_col_token_budget,
)


def test_length_rule_known_presets() -> None:
    assert "1-2 sentences" in length_rule("brief")
    assert "DETAILED" in length_rule("detailed")
    assert "COMPREHENSIVE" in length_rule("comprehensive")
    assert "EXHAUSTIVE" in length_rule("exhaustive")


def test_length_rule_handles_none_and_empty() -> None:
    assert length_rule(None) == DESCRIPTION_LENGTH_RULES["brief"]
    assert length_rule("") == DESCRIPTION_LENGTH_RULES["brief"]
    assert length_rule("   ") == DESCRIPTION_LENGTH_RULES["brief"]


def test_length_rule_unknown_value_falls_back_to_brief() -> None:
    assert length_rule("bogus") == DESCRIPTION_LENGTH_RULES["brief"]
    assert length_rule("VERBOSE") == DESCRIPTION_LENGTH_RULES["brief"]


def test_length_rule_case_insensitive() -> None:
    assert length_rule("DETAILED") == length_rule("detailed")
    assert length_rule("  Brief  ") == length_rule("brief")


def test_per_col_token_budget_scales_with_verbosity() -> None:
    assert per_col_token_budget("brief") < per_col_token_budget("detailed")
    assert per_col_token_budget("detailed") < per_col_token_budget("comprehensive")
    assert per_col_token_budget("comprehensive") < per_col_token_budget("exhaustive")


def test_per_col_token_budget_unknown_falls_back_to_brief() -> None:
    assert per_col_token_budget("bogus") == per_col_token_budget("brief")
    assert per_col_token_budget(None) == per_col_token_budget("brief")
