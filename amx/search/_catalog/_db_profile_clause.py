"""Helpers for building parameterised ``db_profile`` SQL filters.

Pre-0.11 every catalog read method accepted a single ``db_profile: str``
and emitted ``WHERE db_profile = ?`` with one bind. The 0.11 multi-DB
execution model lets ``/ask`` retrieval span multiple profiles in one
question, so several read paths now accept ``str | Sequence[str]`` and
expand to a parameterised ``IN (?, ?, …)`` filter.

The helpers here are intentionally tiny — they're called from many
hot-path mixin methods, so they avoid imports of the wider catalog
machinery and produce SQL fragments suitable for direct interpolation
into hand-written queries.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Union

DBProfileFilter = Union[str, Sequence[str]]


def _normalise_profiles(value: DBProfileFilter) -> list[str]:
    """Coerce a single string or a sequence of strings into a deduped list.

    Empty strings are dropped. Order is preserved (callers may rely on
    it for stable ranking when scoring rows from multiple profiles).
    """
    if isinstance(value, str):
        return [value] if value else []
    seen: set[str] = set()
    out: list[str] = []
    for raw in value:
        n = (raw or "").strip()
        if not n or n in seen:
            continue
        seen.add(n)
        out.append(n)
    return out


def build_db_profile_clause(
    value: DBProfileFilter,
    *,
    column: str = "db_profile",
) -> tuple[str, list[str]]:
    """Build a SQL fragment + bind list filtering ``column`` by *value*.

    Returns one of:
        * ``"db_profile = ?", [name]``       — single profile (or single-element seq)
        * ``"db_profile IN (?, ?)", [...]``  — multi-profile
        * ``"1=0", []``                      — empty input → match nothing
    """
    profiles = _normalise_profiles(value)
    if not profiles:
        return "1=0", []
    if len(profiles) == 1:
        return f"{column} = ?", profiles
    placeholders = ", ".join("?" for _ in profiles)
    return f"{column} IN ({placeholders})", profiles


def normalise_db_profile_filter(value: DBProfileFilter) -> list[str]:
    """Public alias of ``_normalise_profiles`` for use in mixin code.

    Returns the deduped list — useful when callers need to inspect the
    profile count (``is_multi``) before deciding which code path to
    take.
    """
    return _normalise_profiles(value)


__all__ = [
    "DBProfileFilter",
    "build_db_profile_clause",
    "normalise_db_profile_filter",
]
