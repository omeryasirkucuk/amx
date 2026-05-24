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
