"""Order code references so the first few shown to a model are the useful ones.

The scanner records references in file-walk order, and callers keep only the
first handful per asset. Schema definitions (DDL, migrations) tend to come
first and say nothing about what a column means. This module reorders each
asset's references: usage before definitions, and one reference per file
before a second one from the same file.
"""

from __future__ import annotations

import re
from pathlib import PurePosixPath
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from amx.codebase.analyzer import CodeReference

_DDL_RX = re.compile(
    r"^\s*(create|alter|drop)\s+(table|index|unique\s+index|view)\b"
    r"|\badd\s+(column\s+)?\S+\s+\w+"
    r"|\bdrop\s+column\b",
    re.IGNORECASE,
)
# A bare column definition inside CREATE TABLE: "name TYPE[(n)] [NULL|NOT NULL|...],"
_SQL_TYPES = (
    r"(big|small|tiny|medium)?int(eger)?|serial|bigserial|numeric|decimal|money|real|float|double(\s+precision)?"
    r"|bit|bool(ean)?|n?var(char)?|n?char|character(\s+varying)?|n?text|varchar2|nvarchar2|clob|blob"
    r"|date|time|datetime2?|datetimeoffset|timestamp(tz)?|interval|uuid|uniqueidentifier|json|jsonb|xml|bytea"
    r"|(var)?binary|image"
)
_COLDEF_RX = re.compile(
    rf"^\s*[\"`\[]?\w+[\"`\]]?\s+({_SQL_TYPES})\b(\s*\(\s*(\d+|max)(\s*,\s*\d+)?\s*\))?[^;=]*,?\s*$",
    re.IGNORECASE,
)
_MIGRATION_DIRS = {
    "migrations",
    "migration",
    "migrate",
    "versions",
    "flyway",
    "liquibase",
    "changelog",
}


def is_definition(ref: CodeReference) -> bool:
    """True when the reference only declares the schema rather than using it."""
    parts = {p.lower() for p in PurePosixPath(ref.file.replace("\\", "/")).parts[:-1]}
    if parts & _MIGRATION_DIRS:
        return True
    line = ref.line_text
    if _DDL_RX.search(line):
        return True
    return ref.file.lower().endswith(".sql") and bool(_COLDEF_RX.match(line))


def _round_robin(refs: list[CodeReference]) -> list[CodeReference]:
    by_file: dict[str, list[CodeReference]] = {}
    for r in refs:
        by_file.setdefault(r.file, []).append(r)
    queues = list(by_file.values())
    out: list[CodeReference] = []
    depth = 0
    while len(out) < len(refs):
        for q in queues:
            if depth < len(q):
                out.append(q[depth])
        depth += 1
    return out


def rank_references(refs: list[CodeReference]) -> list[CodeReference]:
    """Usage first, definitions last; within each, spread across files."""
    seen: set[tuple[str, int]] = set()
    unique: list[CodeReference] = []
    for r in refs:
        key = (r.file, r.line_no)
        if key not in seen:
            seen.add(key)
            unique.append(r)
    usage = [r for r in unique if not is_definition(r)]
    definitions = [r for r in unique if is_definition(r)]
    return _round_robin(usage) + _round_robin(definitions)


def rank_report_references(references: dict[str, list[CodeReference]]) -> None:
    """Rank every asset's reference list in place."""
    for asset, refs in references.items():
        references[asset] = rank_references(refs)
