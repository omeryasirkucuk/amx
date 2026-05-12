"""Cheap post-generation regex sweep. Flags outputs that contain a
placeholder literal (the model copied the few-shot mask) or that
exactly reproduce one of the redacted examples (the model
plagiarized the example wholesale).
"""

from __future__ import annotations

from amx.llm.style.profile import PLACEHOLDERS, StyleProfile


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
