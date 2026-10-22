"""Keep agent suggestions on columns that exist in the table being analyzed.

Agents parse ``COLUMN: <name>`` from free-form LLM output. Models sometimes
echo the profile line (``COLUMN: ad_nhl | type=TEXT | nulls=0/7761``), wrap
the name in markup, or change its case. Without a check, each variant was
saved as a separate result row for a column the table does not have.
"""

from __future__ import annotations

from amx.agents.base import AgentContext, MetadataSuggestion
from amx.utils.logging import get_logger

log = get_logger("agents.column_guard")


def _bare(name: str) -> str:
    """The name without an echoed profile suffix, markup, or quotes."""
    return name.split("|", 1)[0].strip().strip("*`'\"").strip().lower()


def keep_known_columns(
    suggestions: list[MetadataSuggestion], ctx: AgentContext
) -> list[MetadataSuggestion]:
    """Map near-miss column names to the real column and drop unknown ones.

    Table-level suggestions (``column is None``) always pass. When the
    context carries no profiled columns, nothing is filtered. A recovered
    name never displaces a suggestion that named the column exactly.
    """
    known = [str(c.get("name", "")) for c in (ctx.db_profile or {}).get("columns") or []]
    known = [n for n in known if n]
    if not known:
        return suggestions
    exact = set(known)
    by_bare = {n.lower(): n for n in known}
    named_exactly = {s.column for s in suggestions if s.column in exact}

    kept: list[MetadataSuggestion] = []
    for s in suggestions:
        if s.column is None or s.column in exact:
            kept.append(s)
            continue
        real = by_bare.get(_bare(s.column))
        if real is None:
            log.warning(
                "Dropped a suggestion for unknown column %r in %s.%s",
                s.column,
                ctx.schema,
                ctx.table,
            )
        elif real not in named_exactly:
            s.column = real
            named_exactly.add(real)
            kept.append(s)
    return kept
