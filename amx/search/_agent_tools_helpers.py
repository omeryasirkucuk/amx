"""Pure helpers extracted from ``amx.search.agent_tools``.

The historical ``agent_tools.py`` is a 5,000+ line god module that
mixes a 50-method ``ToolBox`` class with a handful of small utility
functions. Pulling the utilities out so:

- the public ``agent_tools`` import surface stays unchanged (this
  module is private — call sites still ``from amx.search.agent_tools
  import _name_overlap_score`` or use the re-exports below);
- a future PR can split the ``ToolBox`` class itself into mixins
  without entangling the helpers in the same diff;
- `_safe_json` and the scoring heuristics become independently
  testable without spinning up the full ToolBox + connector chain.

Nothing here depends on the ``ToolBox`` instance. Every function is
pure (modulo a ``re`` import and ``json.dumps``); the only "state"
is the ``families`` table baked into ``_dtype_compat_score``.
"""

from __future__ import annotations

import json
import re as _re
from difflib import SequenceMatcher
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from amx.db.connector import DatabaseConnector


class _ToolError(RuntimeError):
    """Raised by a tool when it can't fulfil the request — surfaced verbatim
    to the LLM so it can adjust and try a different tool."""


def _name_overlap_score(left: str, right: str) -> float:
    """Score column-name similarity in [0, 1].

    Combines token-level Jaccard overlap with character-level
    SequenceMatcher ratio so ``customer_id`` and ``cust_id`` score high
    (token "id" matches), while ``customer_id`` and ``payment_status``
    score 0. Used by the cross-profile JOIN finder.
    """
    a = (left or "").strip().lower()
    b = (right or "").strip().lower()
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0

    def _tok(s: str) -> set[str]:
        parts = _re.split(r"[_\W]+|(?=[A-Z])", s)
        return {p.lower() for p in parts if p and len(p) >= 2}

    tokens_a = _tok(a)
    tokens_b = _tok(b)
    jaccard = 0.0
    if tokens_a and tokens_b:
        intersect = tokens_a & tokens_b
        union = tokens_a | tokens_b
        jaccard = len(intersect) / max(1, len(union))
    char = SequenceMatcher(None, a, b).ratio()
    return max(jaccard, char if char >= 0.7 else 0.0)


def _dtype_compat_score(left: str, right: str) -> float:
    """Score dtype compatibility for join purposes.

    Returns 1.0 for same-family (INT↔BIGINT, VARCHAR↔TEXT), 0.5 for
    weakly compatible (INT↔NUMERIC), 0.0 for incompatible
    (VARCHAR↔INT). Coarse buckets are sufficient — joins on
    incompatible dtypes won't actually work in SQL anyway.
    """
    families = {
        "int": ("int", "bigint", "smallint", "tinyint", "int2", "int4", "int8"),
        "float": (
            "float",
            "double",
            "real",
            "numeric",
            "decimal",
            "float8",
            "float4",
        ),
        "string": ("char", "varchar", "text", "string", "nvarchar", "nchar"),
        "bool": ("bool", "boolean", "bit"),
        "date": ("date",),
        "timestamp": ("timestamp", "datetime", "timestamptz"),
        "uuid": ("uuid",),
        "binary": ("bytea", "blob", "binary", "varbinary"),
    }
    canon = lambda s: (s or "").strip().lower().split("(", 1)[0]  # noqa: E731
    a = canon(left)
    b = canon(right)
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0

    def _family(name: str) -> str | None:
        for fam, members in families.items():
            if name in members:
                return fam
            for member in members:
                if name.startswith(member):
                    return fam
        return None

    fa = _family(a)
    fb = _family(b)
    if fa and fa == fb:
        return 1.0
    if {fa, fb} == {"int", "float"}:
        return 0.5
    return 0.0


def _description_proximity(left: str, right: str) -> float:
    """Cheap text-similarity proxy for the vector signal in the
    cross-profile JOIN finder. Returns 0.0 when either side has no
    description (we don't silently inflate the score on undocumented
    columns).
    """
    a = (left or "").strip().lower()
    b = (right or "").strip().lower()
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def _safe_json(value: Any, *, max_len: int = 6000) -> str:
    """Serialize a tool result; truncate so the prompt stays manageable."""
    try:
        text = json.dumps(value, ensure_ascii=False, default=str)
    except Exception:  # pragma: no cover - JSON of catalog rows always works
        text = str(value)
    if len(text) > max_len:
        text = text[: max_len - 18] + "...<truncated>"
    return text


def _sample_distinct_values(
    db: DatabaseConnector,
    schema: str,
    table: str,
    column: str,
    limit: int,
) -> tuple[list[str], int | None]:
    """Pull up to *limit* distinct non-null values from one column.

    Shared by ``_tool_sample_column_values`` (LLM-facing) and the
    ``value_overlap`` join-inference strategy. The same SQL shape is
    used in both: a single ``SELECT DISTINCT col ... LIMIT N`` plus a
    best-effort ``COUNT(DISTINCT col)`` that soft-fails on un-indexed
    columns where the planner gives up.

    Returns ``(samples, distinct_count)`` where ``distinct_count`` is
    ``None`` when the count query failed. Raises ``Exception`` from
    the engine layer when the main SELECT itself fails — callers
    decide whether to swallow that into a per-row "skipped" marker
    or bubble it up.
    """
    from sqlalchemy import text as _text

    adapter = db._adapter  # noqa: SLF001
    fqn = adapter.fully_qualified_name(schema, table)
    col_q = adapter.quote_identifier(column)
    n = max(1, int(limit))
    with db.engine.connect() as conn:
        rows = conn.execute(
            _text(f"SELECT DISTINCT {col_q} AS v FROM {fqn} WHERE {col_q} IS NOT NULL LIMIT :n"),
            {"n": n},
        ).fetchall()
        samples = [str(r[0]) for r in rows if r and r[0] is not None]
        try:
            distinct_row = conn.execute(
                _text(f"SELECT COUNT(DISTINCT {col_q}) FROM {fqn}"),
            ).fetchone()
            distinct_count: int | None = (
                int(distinct_row[0]) if distinct_row and distinct_row[0] is not None else None
            )
        except Exception:
            distinct_count = None
    return samples, distinct_count
