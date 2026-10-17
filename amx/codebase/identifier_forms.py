"""Spelling variants of a column name as it tends to appear in application code.

A column stored as ``cust_nm`` is usually written ``custNm`` in Java or Kotlin,
``CustNm`` in C# or Go and ``cust-nm`` in a template, often behind an accessor
prefix (``getCustNm``, ``m_custNm``, ``_custNm``). The exact-name scan misses
all of these. This module maps such spellings back to the catalog column.

Matching is case-insensitive, so one separator-free variant covers camelCase,
PascalCase and all-lowercase spellings at once.
"""

from __future__ import annotations

import re

_TOKEN_RX = re.compile(r"[A-Z]+(?![a-z])|[A-Z]?[a-z]+|\d+")
_SPLIT_RX = re.compile(r"[_\-\s.]+")

# Separator-free variants shorter than this are too likely to be ordinary
# words or unrelated identifiers.
MIN_JOINED_LEN = 5

# Accessor / field prefixes allowed in front of a variant.
ACCESSOR_PREFIXES = ("get", "set", "is", "has", "with", "m_", "_")


def name_tokens(name: str) -> list[str]:
    """Split an identifier into lowercase word tokens (snake, kebab, camel, Pascal)."""
    out: list[str] = []
    for part in _SPLIT_RX.split(name.strip()):
        out.extend(t.lower() for t in _TOKEN_RX.findall(part))
    return out


def name_variants(name: str) -> set[str]:
    """Lowercase spellings of ``name`` other than the name itself."""
    tokens = name_tokens(name)
    if len(tokens) < 2:
        return set()
    variants = {"_".join(tokens), "-".join(tokens)}
    joined = "".join(tokens)
    if len(joined) >= MIN_JOINED_LEN:
        variants.add(joined)
    variants.discard(name.lower())
    return variants


def variant_map(column_names: list[str], reserved: set[str]) -> dict[str, str]:
    """Map each variant to its column, dropping variants that are ambiguous.

    ``reserved`` holds names already matched exactly (tables and columns); a
    variant equal to one of them, or shared by two columns, is left out so a
    reference is never attributed to the wrong column.
    """
    owners: dict[str, set[str]] = {}
    for col in column_names:
        canonical = str(col).lower()
        for v in name_variants(str(col)):
            owners.setdefault(v, set()).add(canonical)
    return {
        v: next(iter(cols))
        for v, cols in owners.items()
        if len(cols) == 1 and v not in reserved
    }


def variant_pattern(variants: list[str]) -> re.Pattern[str] | None:
    """Regex matching any variant, optionally behind an accessor prefix."""
    if not variants:
        return None
    alts = "|".join(re.escape(v) for v in sorted(variants, key=len, reverse=True))
    prefixes = "|".join(re.escape(p) for p in ACCESSOR_PREFIXES)
    return re.compile(rf"\b(?:{prefixes})?({alts})\b", re.IGNORECASE)
