"""Pure functions for the bulk-review UX: filter, sort, group, status.

PR A of the bulk-review UX series ships these helpers as the shared
vocabulary used by both surfaces:

* the CLI's post-run summary table (``run_summary.render_summary_and_apply``)
* the CLI's standalone ``/review`` entry point
* (parity reference for) the Studio ``ResultsFilterBar`` TSX

The functions operate on duck-typed records that expose the orchestrator's
:class:`amx.agents.orchestrator.ReviewResult` shape — ``schema``,
``table``, ``column``, ``final_description``, ``confidence`` (a
:class:`amx.agents.base.Confidence` or a plain string), ``logprob_score``,
``citations``, and a write-state flag (``applied`` / a pending-queue
predicate). All operations are non-mutating: callers receive a new list.

The module deliberately lives outside ``cli_support.commands`` because
it has zero dependency on Click / Rich / config — that keeps it cheap
to unit-test and reusable from the web router if PR B decides to
surface filter/sort on the server.
"""

from __future__ import annotations

import re
from collections import OrderedDict
from collections.abc import Callable, Iterable
from typing import Any

# ── Public enum-like constants ──────────────────────────────────────────

SORT_KEYS: tuple[str, ...] = (
    "conf-asc",
    "conf-desc",
    "logprob-asc",
    "logprob-desc",
    "name-asc",
    "status",
)
"""Sort keys accepted by ``--sort``. Mirrors the Studio dropdown."""

GROUP_KEYS: tuple[str, ...] = ("none", "schema", "table")
"""Group-by keys accepted by ``--group-by``. ``none`` is the default."""

STATUS_PENDING = "pending"
STATUS_ACCEPTED = "accepted"
STATUS_SKIPPED = "skipped"
STATUS_APPLIED = "applied"

STATUS_ORDER: dict[str, int] = {
    STATUS_PENDING: 0,
    STATUS_ACCEPTED: 1,
    STATUS_SKIPPED: 2,
    STATUS_APPLIED: 3,
}

# Confidence → numeric weight. Lower-confidence rows sort first under
# ``conf-asc``; the spec's "low confidence" preset reads as "needs the
# most human attention first."
_CONFIDENCE_WEIGHTS: dict[str, float] = {
    "low": 0.3,
    "medium": 0.6,
    "high": 0.9,
}


# ── Helpers — duck-type accessors ──────────────────────────────────────


def _attr(obj: Any, name: str, default: Any = None) -> Any:
    """Return ``obj.name`` or ``obj[name]`` or ``default``.

    Review results may be dataclass instances (``ReviewResult``) or plain
    dicts (the web layer hands rows around as JSON). One accessor keeps
    every caller agnostic.
    """
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def confidence_value(obj: Any) -> str:
    """Return the lowercase confidence string for a row.

    Accepts a :class:`Confidence` enum (``.value``), a raw string, or
    ``None``. Defaults to ``"medium"`` when missing so an unset
    confidence does not skew sort order to one extreme.
    """
    raw = _attr(obj, "confidence", None)
    if raw is None:
        return "medium"
    val = getattr(raw, "value", None)
    if isinstance(val, str):
        return val.lower()
    if isinstance(raw, str):
        return raw.lower()
    return "medium"


def confidence_weight(obj: Any) -> float:
    """Return a numeric weight for confidence (low→high)."""
    return _CONFIDENCE_WEIGHTS.get(confidence_value(obj), 0.6)


def asset_path(obj: Any) -> str:
    """Return ``"schema.table.column"`` (column omitted for table-level)."""
    schema = _attr(obj, "schema", "") or _attr(obj, "schema_name", "") or ""
    table = _attr(obj, "table", "") or _attr(obj, "table_name", "") or ""
    column = _attr(obj, "column", None) or _attr(obj, "column_name", None)
    parts = [str(schema), str(table)]
    if column:
        parts.append(str(column))
    return ".".join(p for p in parts if p)


def row_citations(obj: Any) -> list[Any]:
    """Return the citation list (empty when missing)."""
    cit = _attr(obj, "citations", None)
    if cit is None:
        cit = _attr(obj, "citations_json", None)
    return list(cit) if cit else []


# ── Status derivation ──────────────────────────────────────────────────


def derive_status(
    obj: Any,
    *,
    is_pending: Callable[[Any], bool] | None = None,
) -> str:
    """Return the bulk-review status for a row.

    Resolution order:

    1. ``applied`` truthy → ``"applied"``
    2. ``is_pending`` callback returns True → ``"accepted"`` (the row
       is queued for write-back, i.e. the user accepted the suggestion)
    3. ``skipped`` truthy → ``"skipped"``
    4. otherwise → ``"pending"``

    Studio's per-row badge calls the result ``Unreviewed`` for
    ``"pending"``; the CLI uses ``Pending``. The string returned here
    is the machine value — call sites map to the user-facing label.
    """
    if _attr(obj, "applied", False):
        return STATUS_APPLIED
    if is_pending is not None and is_pending(obj):
        return STATUS_ACCEPTED
    if _attr(obj, "skipped", False):
        return STATUS_SKIPPED
    return STATUS_PENDING


# ── Filters ────────────────────────────────────────────────────────────


def match_regex(obj: Any, pattern: re.Pattern[str] | str) -> bool:
    """Return True when ``schema.table.column`` matches the regex.

    ``re.search`` is used (not ``fullmatch``) so a user can type
    ``sales`` to match every asset under the ``sales`` schema without
    knowing the table or column. Compilation is case-insensitive.
    """
    if isinstance(pattern, str):
        pattern = re.compile(pattern, re.IGNORECASE)
    return bool(pattern.search(asset_path(obj)))


def match_query(obj: Any, query: str) -> bool:
    """Case-insensitive substring match across schema/table/column/description.

    Mirrors the Studio FilterBar's free-text input. An empty / whitespace
    ``query`` matches every row.
    """
    q = query.strip().lower()
    if not q:
        return True
    haystack_parts = [
        _attr(obj, "schema", "") or _attr(obj, "schema_name", "") or "",
        _attr(obj, "table", "") or _attr(obj, "table_name", "") or "",
        _attr(obj, "column", None) or _attr(obj, "column_name", None) or "",
        _attr(obj, "final_description", "") or _attr(obj, "chosen_description", "") or "",
    ]
    haystack = " ".join(str(p) for p in haystack_parts).lower()
    return q in haystack


def apply_filters(
    rows: Iterable[Any],
    *,
    pattern: str | None = None,
    only_unreviewed: bool = False,
    only_low_conf: bool = False,
    has_citations: bool = False,
    table_only: bool = False,
    is_pending: Callable[[Any], bool] | None = None,
    low_conf_threshold: float = 0.7,
) -> list[Any]:
    """Apply the bulk-review filter combo to ``rows``.

    All flags are independent (AND-ed). ``low_conf_threshold`` defaults
    to ``0.7`` per the spec's preset; pass a different value if a future
    UI exposes the slider.
    """
    compiled = re.compile(pattern, re.IGNORECASE) if pattern else None

    out: list[Any] = []
    for row in rows:
        if compiled is not None and not match_regex(row, compiled):
            continue
        if only_unreviewed and derive_status(row, is_pending=is_pending) != STATUS_PENDING:
            continue
        if only_low_conf and confidence_weight(row) >= low_conf_threshold:
            continue
        if has_citations and not row_citations(row):
            continue
        if table_only:
            col = _attr(row, "column", None) or _attr(row, "column_name", None)
            if col:
                continue
        out.append(row)
    return out


# ── Sorters ────────────────────────────────────────────────────────────


def _logprob_or(obj: Any, default: float) -> float:
    """Return ``logprob_score`` as a float, falling back when missing."""
    val = _attr(obj, "logprob_score", None)
    if val is None:
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _name_sort_key(obj: Any) -> tuple[str, str, str]:
    """Return ``(schema, table, column-or-tilde)`` for stable lexicographic sort.

    Table-level rows (``column is None``) sort BEFORE column rows of
    the same table — same convention the Studio ``groupByTable`` uses.
    The tilde sentinel is ``""`` so empty sorts first.
    """
    schema = _attr(obj, "schema", "") or _attr(obj, "schema_name", "") or ""
    table = _attr(obj, "table", "") or _attr(obj, "table_name", "") or ""
    column = _attr(obj, "column", None) or _attr(obj, "column_name", None)
    return (schema, table, column or "")


def apply_sort(
    rows: list[Any],
    *,
    sort_key: str,
    is_pending: Callable[[Any], bool] | None = None,
) -> list[Any]:
    """Return a new list sorted by ``sort_key``.

    Unknown sort keys are a no-op (caller-friendly so an upstream typo
    in a saved view doesn't crash a run summary). Sort is stable —
    Python's Timsort preserves input order for ties.
    """
    xs = list(rows)
    if sort_key == "conf-asc":
        xs.sort(key=confidence_weight)
    elif sort_key == "conf-desc":
        xs.sort(key=confidence_weight, reverse=True)
    elif sort_key == "logprob-asc":
        # Missing logprobs sort to the END for ``asc`` so a sparse run
        # surfaces real low values first, not a wall of ``N/A``.
        xs.sort(key=lambda r: _logprob_or(r, float("inf")))
    elif sort_key == "logprob-desc":
        xs.sort(key=lambda r: _logprob_or(r, float("-inf")), reverse=True)
    elif sort_key == "name-asc":
        xs.sort(key=_name_sort_key)
    elif sort_key == "status":
        xs.sort(key=lambda r: STATUS_ORDER.get(derive_status(r, is_pending=is_pending), 99))
    return xs


# ── Grouping ───────────────────────────────────────────────────────────


def group_rows(
    rows: Iterable[Any],
    *,
    by: str,
) -> list[tuple[str, list[Any]]]:
    """Return rows partitioned by group key.

    Returns a list of ``(group_label, rows)`` tuples preserving the
    first-seen order of group labels. For ``by == "none"`` the result
    is a single ``("", rows)`` entry so downstream renderers don't need
    a separate branch.
    """
    if by not in GROUP_KEYS:
        by = "none"

    if by == "none":
        return [("", list(rows))]

    grouped: OrderedDict[str, list[Any]] = OrderedDict()
    for row in rows:
        schema = _attr(row, "schema", "") or _attr(row, "schema_name", "") or ""
        if by == "schema":
            key = schema
        else:
            table = _attr(row, "table", "") or _attr(row, "table_name", "") or ""
            key = f"{schema}.{table}" if schema else table
        grouped.setdefault(key, []).append(row)
    return list(grouped.items())


# ── Composed pipeline ──────────────────────────────────────────────────


def filter_sort_group(
    rows: Iterable[Any],
    *,
    pattern: str | None = None,
    sort_key: str | None = None,
    group_by: str = "none",
    only_unreviewed: bool = False,
    only_low_conf: bool = False,
    has_citations: bool = False,
    table_only: bool = False,
    is_pending: Callable[[Any], bool] | None = None,
) -> list[tuple[str, list[Any]]]:
    """Apply filter → sort → group in one call (mirrors Studio's ``useMemo``)."""
    xs = apply_filters(
        rows,
        pattern=pattern,
        only_unreviewed=only_unreviewed,
        only_low_conf=only_low_conf,
        has_citations=has_citations,
        table_only=table_only,
        is_pending=is_pending,
    )
    if sort_key:
        xs = apply_sort(xs, sort_key=sort_key, is_pending=is_pending)
    return group_rows(xs, by=group_by)


# ── Footer formatting (CLI summary) ────────────────────────────────────


def format_summary_footer(
    *,
    total: int,
    visible: int,
    pattern: str | None = None,
    sort_key: str | None = None,
    group_by: str = "none",
) -> str:
    """Return the one-line footer for the Rich summary table.

    When no filter / sort / group is active the line collapses to
    ``"Showing N rows"``. Mirrors the Studio FilterBar's right-aligned
    chip so both surfaces share the wording.
    """
    if visible == total and not pattern and not sort_key and group_by == "none":
        return f"Showing {total} rows"
    parts = [f"Showing {visible} of {total} rows"]
    if pattern:
        parts.append(f"filter: {pattern}")
    if sort_key:
        parts.append(f"sort: {sort_key}")
    if group_by and group_by != "none":
        parts.append(f"group: {group_by}")
    return " · ".join(parts)


__all__ = [
    "SORT_KEYS",
    "GROUP_KEYS",
    "STATUS_PENDING",
    "STATUS_ACCEPTED",
    "STATUS_SKIPPED",
    "STATUS_APPLIED",
    "STATUS_ORDER",
    "confidence_value",
    "confidence_weight",
    "asset_path",
    "row_citations",
    "derive_status",
    "match_regex",
    "match_query",
    "apply_filters",
    "apply_sort",
    "group_rows",
    "filter_sort_group",
    "format_summary_footer",
]
