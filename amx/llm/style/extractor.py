"""Stage 1: turn a reference table's column metadata into a
StyleProfile via one LLM call.

This module is intentionally I/O-free. The caller (slash command or
HTTP router) reads column comments from the DB and selects the LLM
callable to pass in.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable

from amx.llm.style.profile import StyleProfile

MIN_SAMPLES = 3
DEFAULT_CAP = 30


class NoSamplesError(Exception):
    """Raised when the reference table has too few non-empty comments."""


def _bucket(text: str) -> int:
    n = len(text.split())
    if n <= 6:
        return 0
    if n <= 14:
        return 1
    return 2


def sample_descriptions(
    raw: dict[str, str | None],
    cap: int = DEFAULT_CAP,
    min_samples: int = MIN_SAMPLES,
) -> list[tuple[str, str]]:
    non_empty = [(c, (v or "").strip()) for c, v in raw.items() if v and v.strip()]
    if len(non_empty) < min_samples:
        raise NoSamplesError(
            f"reference table has only {len(non_empty)} non-empty "
            f"descriptions; need at least {min_samples}"
        )
    if len(non_empty) <= cap:
        return non_empty
    buckets: list[list[tuple[str, str]]] = [[], [], []]
    for item in non_empty:
        buckets[_bucket(item[1])].append(item)
    quota = cap // 3
    picked: list[tuple[str, str]] = []
    for b in buckets:
        picked.extend(b[:quota])
    if len(picked) < cap:
        leftovers: list[tuple[str, str]] = []
        for b in buckets:
            leftovers.extend(b[quota:])
        picked.extend(leftovers[: cap - len(picked)])
    return picked[:cap]


_SYSTEM = (
    "You distill a user's description-writing style from sample column "
    "comments. Reply with a single JSON object exactly matching the "
    "StyleProfile schema:\n"
    '{"language": str, "tone": str, "avg_length_words": int, '
    '"length_range": [int, int], "person": str, "capitalization": str, '
    '"ends_with_period": bool, "structural_patterns": [str], '
    '"vocabulary_register": str, "redacted_examples": [str]}\n'
    "Rules for redacted_examples (3-5 entries):\n"
    "- Replace every domain noun (entity name, metric, date field, "
    "status value, identifier) with one of these placeholders: "
    "<ENTITY>, <METRIC>, <DATE_FIELD>, <STATUS>, <IDENTIFIER>.\n"
    "- Preserve sentence structure, tone, capitalization, and punctuation.\n"
    "- Never invent placeholders outside that set.\n"
    "Return JSON only, no commentary."
)


def _user_prompt(samples: Iterable[tuple[str, str]]) -> str:
    lines = [f"- {name}: {desc}" for name, desc in samples]
    return "Samples:\n" + "\n".join(lines)


def _extract_json(text: str) -> str:
    """Permissive JSON locator: strip code fences if present."""
    t = text.strip()
    if t.startswith("```"):
        t = t.split("\n", 1)[1] if "\n" in t else t
        if t.endswith("```"):
            t = t.rsplit("```", 1)[0]
    return t.strip()


def extract_style(
    raw: dict[str, str | None],
    llm_call: Callable[[str, str], str],
    cap: int = DEFAULT_CAP,
) -> tuple[StyleProfile, int]:
    samples = sample_descriptions(raw, cap=cap)
    user = _user_prompt(samples)

    last_err: Exception | None = None
    for _ in range(2):
        raw_resp = llm_call(_SYSTEM, user)
        try:
            return StyleProfile.from_json(_extract_json(raw_resp)), len(samples)
        except (json.JSONDecodeError, ValueError) as e:
            last_err = e
            continue
    raise ValueError(f"distillation LLM returned invalid JSON twice: {last_err}")
