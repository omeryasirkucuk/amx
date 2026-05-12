"""Cheap post-generation regex sweep. Flags outputs that contain a
placeholder literal (the model copied the few-shot mask) or that
exactly reproduce one of the redacted examples (the model
plagiarized the example wholesale).
"""

from __future__ import annotations

import re

from amx.llm.style.profile import PLACEHOLDERS, StyleProfile

_PLACEHOLDER_PATTERN = re.compile(
    r"\s*<(?:ENTITY|METRIC|DATE_FIELD|STATUS|IDENTIFIER)>\s*"
)


def scrub_placeholders(text: str) -> str:
    """Remove any of the style placeholder literals from a string.

    Collapses surrounding whitespace so "Holds <ENTITY> data" becomes
    "Holds data" rather than "Holds  data".
    """
    if not text:
        return text
    cleaned = _PLACEHOLDER_PATTERN.sub(" ", text)
    return re.sub(r"\s+", " ", cleaned).strip()


def contains_leakage(text: str, profile: StyleProfile) -> bool:
    if not text:
        return False
    for tag in PLACEHOLDERS:
        if tag in text:
            return True
    normalized = text.strip()
    for ex in profile.redacted_examples:
        if normalized == ex.strip():
            return True
    return False
