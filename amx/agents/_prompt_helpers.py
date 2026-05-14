"""Shared prompt fragments threaded into profile / rag / code / merge agents.

The directives here exist so that ``alternatives_mode`` (see
:mod:`amx.config`) produces the same instruction text wherever the LLM
is asked to emit ``DESCRIPTION_1..N`` blocks. Centralising the strings
keeps every agent aligned and lets the orchestrator's merge step reuse
the same wording so its parser stays in sync.
"""

from __future__ import annotations

from amx.config import DEFAULT_ALTERNATIVES_MODE

_SEMANTIC_DIRECTIVE = (
    "ALTERNATIVES DIVERSITY (semantic mode): Each DESCRIPTION_i (i>1) MUST "
    "express a meaningfully DIFFERENT interpretation of the column's "
    "purpose — a distinct candidate meaning consistent with the column "
    "name, samples, neighbours, and key relationships. Do NOT paraphrase "
    "DESCRIPTION_1. The user picks the interpretation that matches reality."
)

_LEXICAL_DIRECTIVE = (
    "ALTERNATIVES DIVERSITY (lexical mode): Every DESCRIPTION_i MUST "
    "express the SAME interpretation as DESCRIPTION_1, varying only in "
    "phrasing, sentence structure, or word choice. Do NOT introduce new "
    "meanings — the user has already accepted the interpretation and "
    "only wants the strongest wording."
)


def alternatives_mode_directive(mode: str | None, n_alternatives: int) -> str:
    """Return the directive paragraph for the active ``alternatives_mode``.

    Empty string when the directive would be a no-op:
    * ``n_alternatives <= 1`` — there are no alternates to differentiate.
    * mode is missing or unrecognised — caller falls back to the default
      and we still emit the matching directive.
    """
    if n_alternatives <= 1:
        return ""
    resolved = (mode or DEFAULT_ALTERNATIVES_MODE).strip().lower()
    if resolved == "lexical":
        return _LEXICAL_DIRECTIVE
    return _SEMANTIC_DIRECTIVE


_SEMANTIC_MERGE_NOTE = (
    "ALTERNATIVES DIVERSITY (semantic mode): The inputs may carry "
    "meaningfully different interpretations. Choose the strongest as "
    "DESCRIPTION_1 and PRESERVE the remaining distinct interpretations "
    "verbatim as DESCRIPTION_2..N — do not collapse them into "
    "paraphrases of DESCRIPTION_1."
)

_LEXICAL_MERGE_NOTE = (
    "ALTERNATIVES DIVERSITY (lexical mode): All inputs express the same "
    "meaning. Choose the strongest phrasing as DESCRIPTION_1 and keep "
    "the next-best phrasings as DESCRIPTION_2..N. Do not introduce new "
    "interpretations during the merge."
)


def alternatives_mode_merge_note(mode: str | None, n_alternatives: int) -> str:
    """Directive variant for the orchestrator merge prompt.

    The merge step has its own context (it sees already-generated
    alternatives) so the wording is tuned for "preserve / collapse"
    rather than "generate".
    """
    if n_alternatives <= 1:
        return ""
    resolved = (mode or DEFAULT_ALTERNATIVES_MODE).strip().lower()
    if resolved == "lexical":
        return _LEXICAL_MERGE_NOTE
    return _SEMANTIC_MERGE_NOTE
