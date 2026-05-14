"""Shared prompt fragments threaded into profile / rag / code / merge agents.

The directives here exist so that ``alternatives_mode`` (see
:mod:`amx.config`) produces the same instruction text wherever the LLM
is asked to emit ``DESCRIPTION_1..N`` blocks. Centralising the strings
keeps every agent aligned and lets the orchestrator's merge step reuse
the same wording so its parser stays in sync.

Per Definition 1 (standard NLP usage):

* ``semantic`` similarity → alternatives **preserve the meaning** of
  DESCRIPTION_1 but vary the surface form (synonyms, restructured
  phrasing, different word choices). **No** new concepts, attributes,
  or nuances may be introduced or removed.
* ``lexical`` similarity → alternatives **preserve surface-level
  features** (shared keywords, similar phrasing, overlapping
  vocabulary) but may diverge in meaning. New conceptual nuances,
  narrower or broader framings, or shifted emphases are allowed as
  long as core tokens overlap with DESCRIPTION_1.

The previous shipping (PR #441) had these definitions inverted; future
maintainers MUST NOT re-invert them. The self-consistency scorer
measures cosine similarity over sentence-transformer embeddings, so it
is itself a semantic-similarity metric: under Definition 1 expect
**high** SC across true semantic alts and **lower / spread** SC across
true lexical alts.
"""

from __future__ import annotations

from amx.config import DEFAULT_ALTERNATIVES_MODE

# Per Definition 1: semantic ⇒ same meaning / different words.
# Do NOT re-invert. See module docstring above.
_SEMANTIC_DIRECTIVE = (
    "ALTERNATIVES DIVERSITY (semantic mode — paraphrase only): Each "
    "DESCRIPTION_i (i>1) MUST be a PARAPHRASE of DESCRIPTION_1.\n"
    "DO:\n"
    "  • Hold the factual content fixed: same referent, same scope, "
    "same units, same attributes, same business purpose.\n"
    "  • Vary the surface form: substitute synonyms, restructure the "
    "sentence, change voice (active ⇄ passive), reorder clauses, swap "
    "noun phrases for equivalent ones.\n"
    "  • Aim for high embedding similarity to DESCRIPTION_1; a reader "
    "should extract identical facts from every DESCRIPTION_i.\n"
    "DO NOT:\n"
    "  • Introduce any new concept, attribute, qualifier, scope, "
    "constraint, or nuance (no 'sequential', no 'internal', no "
    "'primary' unless those terms are already in DESCRIPTION_1).\n"
    "  • Drop any factual content present in DESCRIPTION_1.\n"
    "  • Reframe the referent (e.g., do NOT swap 'record' for "
    "'physical entity' — that is a meaning shift).\n"
    "  • Echo DESCRIPTION_1 verbatim; the contract is paraphrase, not "
    "duplication.\n"
    "The user picks the phrasing that reads best — meaning is held "
    "constant by contract."
)

# Per Definition 1: lexical ⇒ shared vocabulary / shifted meaning.
# Do NOT re-invert. See module docstring above.
_LEXICAL_DIRECTIVE = (
    "ALTERNATIVES DIVERSITY (lexical mode — shared vocabulary, drifted "
    "meaning): Each DESCRIPTION_i (i>1) MUST share core vocabulary "
    "with DESCRIPTION_1 while expressing a meaningfully DIFFERENT "
    "interpretation.\n"
    "DO:\n"
    "  • Carry over at least 2–3 key tokens from DESCRIPTION_1 (the "
    "column-relevant nouns and domain terms) verbatim.\n"
    "  • Introduce a new conceptual nuance in each alternative: add a "
    "qualifier (e.g., 'sequential', 'internal', 'primary', "
    "'composite'), reframe the referent (e.g., 'record' → 'physical "
    "place', 'entry' → 'reference point'), or narrow / broaden the "
    "scope (e.g., 'identifier' → 'lookup key for an external system').\n"
    "  • Make every DESCRIPTION_i (i>1) carry a clearly distinct "
    "candidate meaning from DESCRIPTION_1 and from each other — a "
    "reviewer should be able to articulate the semantic difference in "
    "one short sentence.\n"
    "DO NOT:\n"
    "  • Produce a paraphrase. If you can substitute one DESCRIPTION_i "
    "for DESCRIPTION_1 with no loss or addition of meaning, you have "
    "failed lexical mode — that is semantic mode.\n"
    "  • Strip the shared vocabulary; alternatives unanchored from "
    "DESCRIPTION_1's keywords lose the lexical contract.\n"
    "  • Invent meanings the evidence does not support; reach for the "
    "next-closest plausible reading, not a wild guess.\n"
    "Expect lower embedding-cosine similarity to DESCRIPTION_1 than "
    "semantic mode would produce — that spread is the intentional "
    "signal the user picks against."
)


_WORKED_EXAMPLE_BLOCK = (
    "EXAMPLES (anchor for both modes — do not echo into the output):\n"
    '  Source: "Unique identifier for a geographic location record."\n'
    "\n"
    "  SEMANTIC (preserve meaning, vary surface form):\n"
    '    • "Distinct numeric key assigned to every individual '
    'geographic location."\n'
    '    • "Primary identifier that distinguishes each geographic '
    'location entry."\n'
    "\n"
    "  LEXICAL (share vocabulary, allow shifted meaning):\n"
    '    • "Sequential numeric key assigned to each distinct '
    "geolocation entry.\"   (adds 'sequential' — new attribute)\n"
    '    • "Internal reference number for a physical place or mapped '
    'point."   (reframes as internal reference + physical place)'
)


def alternatives_mode_directive(mode: str | None, n_alternatives: int) -> str:
    """Return the directive paragraph for the active ``alternatives_mode``.

    The returned string contains the per-mode contract AND the shared
    worked-example block, separated by a blank line. Empty string when
    the directive would be a no-op:

    * ``n_alternatives <= 1`` — there are no alternates to differentiate.
    * mode is missing or unrecognised — caller falls back to the default
      and we still emit the matching directive.
    """
    if n_alternatives <= 1:
        return ""
    resolved = (mode or DEFAULT_ALTERNATIVES_MODE).strip().lower()
    directive = _LEXICAL_DIRECTIVE if resolved == "lexical" else _SEMANTIC_DIRECTIVE
    return f"{directive}\n\n{_WORKED_EXAMPLE_BLOCK}"


# Per Definition 1: semantic ⇒ paraphrases of same meaning.
# Do NOT re-invert.
_SEMANTIC_MERGE_NOTE = (
    "ALTERNATIVES DIVERSITY (semantic mode — paraphrase only): The "
    "inputs are paraphrases of the same factual content. Choose the "
    "strongest phrasing as DESCRIPTION_1; the remaining "
    "DESCRIPTION_2..N MUST stay paraphrases of it (same meaning, "
    "different surface form). Do NOT inject new attributes, scopes, "
    "or nuances during the merge."
)

# Per Definition 1: lexical ⇒ shared vocabulary, drifted meaning.
# Do NOT re-invert.
_LEXICAL_MERGE_NOTE = (
    "ALTERNATIVES DIVERSITY (lexical mode — shared vocabulary, drifted "
    "meaning): The inputs share core vocabulary but may carry shifted "
    "nuances or different framings. Choose the strongest as "
    "DESCRIPTION_1; the remaining DESCRIPTION_2..N keep the core tokens "
    "while preserving genuinely distinct candidate meanings. Do NOT "
    "collapse the meaning-shifted alternates into paraphrases."
)


def alternatives_mode_merge_note(mode: str | None, n_alternatives: int) -> str:
    """Directive variant for the orchestrator merge prompt.

    The merge step has its own context (it sees already-generated
    alternatives) so the wording is tuned for "preserve / collapse"
    rather than "generate". Definitions match :func:`alternatives_mode_directive`.
    """
    if n_alternatives <= 1:
        return ""
    resolved = (mode or DEFAULT_ALTERNATIVES_MODE).strip().lower()
    if resolved == "lexical":
        return _LEXICAL_MERGE_NOTE
    return _SEMANTIC_MERGE_NOTE
