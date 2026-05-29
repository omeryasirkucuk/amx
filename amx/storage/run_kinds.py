"""Canonical definition of which ``analysis_runs.command`` kinds are
*comparable* — i.e. produce per-asset descriptions that the Compare
feature can pivot side-by-side.

Comparison (CLI ``/compare`` + Studio "Compare runs") works by pivoting
the per-asset descriptions a run wrote. Free-text Q&A turns (``search.ask``)
and any non-description command produce nothing to pivot, so the picker and
the CLI auto-resolution must only ever surface comparable runs.

This is the single source of truth shared by the storage queries
(``amx.storage._history_runs``), the CLI resolver
(``amx.cli_support._compare_runs``), and the web endpoints
(``amx.web.routers.history``). It mirrors the frontend ``commandKind``
bucketing in ``frontend/src/lib/runDisplay.ts`` minus the ``ask``/``other``
buckets — if a new description-producing command is added later, update both
this module and ``commandKind``.
"""

from __future__ import annotations

# Exact ``command`` values that produce per-asset descriptions.
COMPARABLE_EXACT: tuple[str, ...] = (
    "analyze.run",
    "analyze.apply",
    "rerun",
    "schedule",
)

# Command prefixes that are comparable (``generate.table``,
# ``generate.column``, ``generate.schema``, …).
COMPARABLE_PREFIXES: tuple[str, ...] = ("generate.",)


def is_comparable_command(command: str | None) -> bool:
    """True when ``command`` produces per-asset descriptions worth comparing."""
    cmd = (command or "").strip().lower()
    if cmd in COMPARABLE_EXACT:
        return True
    return any(cmd.startswith(prefix) for prefix in COMPARABLE_PREFIXES)


def comparable_sql(column: str = "command") -> tuple[str, list[str]]:
    """Return a ``(where_fragment, params)`` pair selecting comparable rows.

    The fragment is a parenthesized ``IN (...) OR LIKE ...`` clause ready to
    drop into a larger WHERE; ``column`` names the SQL column to test (default
    ``"command"``). Example::

        frag, params = comparable_sql()
        # frag   == "(command IN (?,?,?,?) OR command LIKE ?)"
        # params == ["analyze.run", "analyze.apply", "rerun", "schedule",
        #            "generate.%"]
    """
    placeholders = ",".join("?" for _ in COMPARABLE_EXACT)
    like_clauses = " OR ".join(f"{column} LIKE ?" for _ in COMPARABLE_PREFIXES)
    fragment = f"({column} IN ({placeholders}) OR {like_clauses})"
    params: list[str] = [*COMPARABLE_EXACT, *(f"{prefix}%" for prefix in COMPARABLE_PREFIXES)]
    return fragment, params


__all__ = [
    "COMPARABLE_EXACT",
    "COMPARABLE_PREFIXES",
    "is_comparable_command",
    "comparable_sql",
]
