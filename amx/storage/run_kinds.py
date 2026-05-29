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


# ── Kind buckets ──────────────────────────────────────────────────────
#
# The Runs page groups every ``analysis_runs.command`` into one of these
# user-visible buckets. This is the Python mirror of the frontend
# ``commandKind`` in ``frontend/src/lib/runDisplay.ts`` — keep the two
# in sync if a new command kind is added.

# Bucket -> exact ``command`` values that map into it.
_KIND_EXACT: dict[str, tuple[str, ...]] = {
    "analyze": ("analyze.run", "analyze.apply"),
    "rerun": ("rerun",),
    "ask": ("search.ask", "ask.run"),
    "schedule": ("schedule",),
}
# Bucket -> command prefixes that map into it.
_KIND_PREFIXES: dict[str, tuple[str, ...]] = {
    "generate": ("generate.",),
}
# Buckets a user can filter by (everything else falls into "other").
KIND_BUCKETS: tuple[str, ...] = ("analyze", "generate", "rerun", "ask", "schedule")


def command_bucket(command: str | None) -> str:
    """Bucket a raw ``command`` into analyze / generate / rerun / ask /
    schedule / other — the Python mirror of the frontend ``commandKind``."""
    cmd = (command or "").strip().lower()
    for bucket, values in _KIND_EXACT.items():
        if cmd in values:
            return bucket
    for bucket, prefixes in _KIND_PREFIXES.items():
        if any(cmd.startswith(p) for p in prefixes):
            return bucket
    return "other"


def kind_bucket_sql(kind: str | None, column: str = "command") -> tuple[str, list[str]]:
    """Return a ``(where_fragment, params)`` pair selecting rows in one kind
    bucket, mirroring :func:`comparable_sql`.

    ``kind`` is one of :data:`KIND_BUCKETS`, ``"other"``, or ``"all"``/``None``
    (no filter — returns ``("", [])``). ``"other"`` is the negation of every
    known bucket so unmapped commands (sync, scan, …) stay reachable.
    """
    key = (kind or "").strip().lower()
    if key in {"", "all"}:
        return "", []
    if key == "other":
        # NOT in any known exact value AND NOT matching any known prefix.
        all_exact = [v for values in _KIND_EXACT.values() for v in values]
        all_prefixes = [p for prefixes in _KIND_PREFIXES.values() for p in prefixes]
        placeholders = ",".join("?" for _ in all_exact)
        not_likes = " AND ".join(f"{column} NOT LIKE ?" for _ in all_prefixes)
        fragment = f"({column} NOT IN ({placeholders}) AND {not_likes})"
        params: list[str] = [*all_exact, *(f"{p}%" for p in all_prefixes)]
        return fragment, params
    exact = _KIND_EXACT.get(key, ())
    prefixes = _KIND_PREFIXES.get(key, ())
    clauses: list[str] = []
    params = []
    if exact:
        placeholders = ",".join("?" for _ in exact)
        clauses.append(f"{column} IN ({placeholders})")
        params.extend(exact)
    for p in prefixes:
        clauses.append(f"{column} LIKE ?")
        params.append(f"{p}%")
    if not clauses:
        # Unknown bucket name — match nothing rather than everything.
        return "1 = 0", []
    fragment = "(" + " OR ".join(clauses) + ")"
    return fragment, params


__all__ = [
    "COMPARABLE_EXACT",
    "COMPARABLE_PREFIXES",
    "KIND_BUCKETS",
    "is_comparable_command",
    "comparable_sql",
    "command_bucket",
    "kind_bucket_sql",
]
