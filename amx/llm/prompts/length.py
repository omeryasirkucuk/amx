"""Verbosity → length-rule mapping used by every LLM prompt that
asks for a description.

The four presets here mirror the ``description_verbosity`` Settings
option (``brief | detailed | comprehensive | exhaustive``). Both the
bulk ``ProfileAgent`` and the single-shot ``/api/generate/...``
endpoints import from this module so the user-visible length
expectation stays consistent across paths.
"""

from __future__ import annotations

DESCRIPTION_LENGTH_RULES: dict[str, str] = {
    "brief": "A concise description (1-2 sentences).",
    "detailed": (
        "A DETAILED description (2-4 sentences). Cover the column's purpose, "
        "the typical kind of values it stores, and any relationships to other "
        "tables/keys/business processes that the evidence supports. Write "
        "concrete, specific sentences — do not pad with filler. If evidence "
        "for a 4-sentence answer is missing, write fewer sentences rather "
        "than invent context."
    ),
    "comprehensive": (
        "A COMPREHENSIVE description (1-2 short paragraphs, roughly 5-8 "
        "sentences). Cover the column's purpose, typical values and ranges, "
        "relationships to other tables/keys/business processes, common usage "
        "patterns in analytical queries, and any caveats or edge cases that "
        "the evidence reveals (NULL handling, distinct cardinality, dominant "
        "values). Stay specific and grounded in the evidence — never invent "
        "context. If evidence is thin, shorten the answer rather than pad."
    ),
    "exhaustive": (
        "An EXHAUSTIVE reference-style description (multiple short "
        "paragraphs). Document, in order: (1) semantic meaning and business "
        "purpose; (2) typical values, ranges, and data-type considerations; "
        "(3) relationships to other tables, foreign keys, and upstream/"
        "downstream business processes; (4) common analytical and reporting "
        "patterns this column participates in; (5) edge cases, NULL "
        "semantics, and any data-quality observations visible in the "
        "evidence. Use multiple short paragraphs for readability. Cite only "
        "what the evidence supports — omit sections you cannot ground."
    ),
}


# Reminder line injected into every prompt that asks for N ranked
# description alternatives. Without it the LLM treats DESCRIPTION_2..N
# as "shorter rephrasings" of DESCRIPTION_1 — collapsing comprehensive /
# exhaustive presets back to one-sentence briefs for every alternative
# slot. Pinning the rule next to the slot template restores the
# user-visible length expectation across all alternatives.
ALTERNATIVES_LENGTH_RULE_REMINDER = (
    "The Length rule above applies EQUALLY to every DESCRIPTION_N slot. "
    "An alternative is a different interpretation, not a shorter version — "
    "do not collapse alternatives into one-sentence summaries when the "
    "verbosity preset is comprehensive or exhaustive."
)


# Per-column output budget used to size ``max_tokens`` for a batch.
# Scaled by ``description_verbosity`` so a 100-column batch in
# ``comprehensive`` / ``exhaustive`` mode doesn't truncate halfway
# through — truncation is the dominant cause of empty/missing per-
# column outputs in long batches. Single-shot generate doesn't batch,
# so it ignores this map; only ``ProfileAgent`` consumes it.
VERBOSITY_PER_COL_TOKEN_BUDGET: dict[str, int] = {
    "brief": 150,
    "detailed": 350,
    "comprehensive": 800,
    "exhaustive": 1600,
}


def length_rule(verbosity: str | None) -> str:
    """Return the length-rule sentence for a verbosity preset.

    Falls back to ``brief`` for ``None``, empty, or unrecognised values
    so callers can pass user input straight through without their own
    validation.
    """
    key = (verbosity or "brief").lower().strip()
    return DESCRIPTION_LENGTH_RULES.get(key, DESCRIPTION_LENGTH_RULES["brief"])


def per_col_token_budget(verbosity: str | None) -> int:
    """Return the per-column token budget for a verbosity preset."""
    key = (verbosity or "brief").lower().strip()
    return VERBOSITY_PER_COL_TOKEN_BUDGET.get(key, VERBOSITY_PER_COL_TOKEN_BUDGET["brief"])
