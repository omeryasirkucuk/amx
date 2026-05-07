"""Shared prompt-construction helpers used by both the bulk profile
agent and the single-shot Studio generate endpoints.

Centralising these strings keeps the user-visible "verbosity" preset
consistent across paths — a user who sets ``description_verbosity =
detailed`` in Settings sees identical length expectations whether the
description is produced by ``ProfileAgent`` (bulk run) or
``/api/generate/{database,schema,table,column}`` (Studio single-shot).
"""

from amx.llm.prompts.length import (
    DESCRIPTION_LENGTH_RULES,
    VERBOSITY_PER_COL_TOKEN_BUDGET,
    length_rule,
    per_col_token_budget,
)

__all__ = [
    "DESCRIPTION_LENGTH_RULES",
    "VERBOSITY_PER_COL_TOKEN_BUDGET",
    "length_rule",
    "per_col_token_budget",
]
