"""Pure short-circuit detectors for the /ask flow.

The mixin-based ``SearchAgent`` has a family of methods named
``_handle_*`` that recognise question patterns and reply directly
without driving the full plan/retrieve/synthesize loop. The
recognition logic in each handler is pure — it inspects the question
string + a small set of constants — but it's currently entangled with
side effects (session-store writes, ``SearchAnswer`` construction).

This module extracts the **pure detection step** so each short
circuit becomes unit-testable in isolation. The mixin retains the
side-effect glue (record assistant turn, construct the answer) and
delegates the detection here.

PR 5 of the /ask agent refactor migrates one short circuit at a time
(``chitchat`` first); future PRs extract ``meta_query`` and
``followup_reaffirmation`` under the same shape.
"""

from __future__ import annotations

import re
from collections.abc import Iterable

# The canned reply for chitchat — kept as a constant exposed via the
# ``chitchat_summary`` function so callers can't accidentally mutate
# it. The wording matches the legacy mixin verbatim so behaviour
# parity is provable.
_CHITCHAT_SUMMARY = (
    "Hi! I'm AMX's metadata search assistant — I'm built to answer questions about "
    "your database schemas, tables, and columns rather than chat. "
    "Try: `what is the vbrk table?`, `which tables relate to pricing?`."
)

# Pre-compiled splitter for tokenising chitchat candidates. Strips
# whitespace and common punctuation in one pass so ``is_chitchat``
# stays allocation-light on every ``/ask`` turn.
_CHITCHAT_SPLIT_RE = re.compile(r"[\s\?!.,;:]+")


def is_chitchat(question: str, chitchat_tokens: Iterable[str]) -> bool:
    """Detect conversational filler (greetings, thanks, single-word
    politeness).

    Returns ``True`` when the question is short (≤4 words after
    splitting on whitespace + common punctuation) AND every word is
    in ``chitchat_tokens``. Mirrors the legacy
    :meth:`amx.search._agent.short_circuits.ShortCircuitsMixin._handle_chitchat`
    detection step byte-for-byte.

    The token set is parameterised (not a module-level constant)
    because ``SearchAgent`` owns the canonical
    ``_CHITCHAT_TOKENS`` frozen set and may extend it per
    deployment.
    """
    sample = (question or "").strip().lower()
    if not sample:
        return False
    words = [tok for tok in _CHITCHAT_SPLIT_RE.split(sample) if tok]
    if not words or len(words) > 4:
        return False
    token_set = (
        chitchat_tokens
        if isinstance(chitchat_tokens, (set, frozenset))
        else frozenset(chitchat_tokens)
    )
    return all(word in token_set for word in words)


def chitchat_summary() -> str:
    """The canned reply text the mixin returns when chitchat is
    detected. Exposed as a function so the constant cannot be
    mutated in place by accident."""
    return _CHITCHAT_SUMMARY


# Meta-query patterns: questions about the conversation itself
# ("what was my previous question?"). The patterns come from the
# legacy mixin verbatim so the regression suite passes unchanged.
_META_PATTERNS: tuple[str, ...] = (
    r"\b(?:previous|prior|last)\s+question\b",
    r"\bwhat\s+(?:did|was)\s+(?:i|my)\s+(?:last\s+|previous\s+|prior\s+)?(?:question|ask)\b",
    r"\bwhat\s+have\s+i\s+(?:asked|been\s+asking)\b",
)

_META_COMPILED: tuple[re.Pattern[str], ...] = tuple(re.compile(p) for p in _META_PATTERNS)


def is_meta_query(question: str) -> bool:
    """Detect questions about the conversation itself.

    Returns ``True`` for ``"what was my previous question?"`` and
    close variants. Mirrors
    :meth:`amx.search._agent.short_circuits.ShortCircuitsMixin._handle_meta_query`
    detection step. Empty input is rejected up front so callers
    don't have to guard.
    """
    sample = (question or "").strip().lower()
    if not sample:
        return False
    return any(pat.search(sample) for pat in _META_COMPILED)


def meta_query_summary(prior_question: str) -> str:
    """Compose the canned reply for a meta-query short circuit.

    When ``prior_question`` is empty the reply admits this is the
    first turn in the session; otherwise it quotes the user's prior
    question verbatim. Wording matches the legacy mixin.
    """
    cleaned = (prior_question or "").strip()
    if not cleaned:
        return "This is the first question in this session; no prior question is on record."
    return f'Your previous question was: "{cleaned}"'


# Followup reaffirmation patterns: short pushback phrasings ("are
# you sure?", "really?", "why?"). Each is anchored with ^/$ to
# require the whole input is just the pushback — a fresh question
# that happens to contain "sure" must NOT short-circuit.
_AFFIRM_PATTERNS: tuple[str, ...] = (
    r"^\s*(?:are\s+you\s+sure|you\s+sure|really|seriously|sure\?+)\s*[\.\?\!]*\s*$",
    r"^\s*(?:is\s+that\s+(?:right|correct|true)|you\s+positive|positive\?+)\s*[\.\?\!]*\s*$",
    r"^\s*(?:why|why\??|how\s+come|how)\s*[\.\?\!]*\s*$",
)

_AFFIRM_COMPILED: tuple[re.Pattern[str], ...] = tuple(re.compile(p) for p in _AFFIRM_PATTERNS)


def is_followup_reaffirmation(question: str) -> bool:
    """Detect short pushback phrasings on the prior answer.

    Returns ``True`` for ``"are you sure?"``, ``"really"``, ``"why?"``
    and close variants. Patterns are anchored with ``^/$`` so a fresh
    question that happens to contain ``"sure"`` does not match.

    Mirrors
    :meth:`amx.search._agent.short_circuits.ShortCircuitsMixin._handle_followup_reaffirmation`
    detection step byte-for-byte; the mixin keeps ownership of the
    session-store lookup and ``SearchAnswer`` construction.
    """
    sample = (question or "").strip().lower()
    if not sample:
        return False
    return any(pat.match(sample) for pat in _AFFIRM_COMPILED)


def reaffirmation_summary(prior_assistant: str) -> str:
    """Compose the canned reply for a reaffirmation short circuit.

    Quotes the prior assistant answer verbatim so the user sees the
    confirmation came from the same source they originally received.
    Wording matches the legacy mixin.
    """
    cleaned = (prior_assistant or "").strip()
    return (
        "Yes, I'm sure — the previous answer came from live database metadata. To restate: "
        + cleaned
    )
