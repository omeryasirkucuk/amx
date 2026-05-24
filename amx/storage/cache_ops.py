"""Explicit DB-cache management — read + flush the SQLite catalog caches.

The user-facing surface (``/db cache-show`` / ``/db cache-stats`` /
``/db cache-clear`` and the matching ``/api/db/cache/*`` router)
all funnel through this module so the REPL and Studio share one
implementation. Every function here is read-only or DELETE-only on
the three persisted cache tables:

* ``schemas_cache`` — per-catalog schema metadata.
* ``column_comments_cache`` — per-table comment + column JSON.
* ``catalog_entities`` / ``catalog_descriptions`` / ``catalog_profile_state``
  — full skeleton-sync output keyed by ``(profile, database, schema, table, column)``.

We never duplicate SQL — the helpers compose the store's own
``invalidate_*`` methods plus narrowly-scoped reads for the inventory
and stats views.
"""

from __future__ import annotations

import sqlite3
import time
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

from amx.storage.sqlite_store import history_store

#: Cache-table identifiers used in clear ``types`` filters. ``"all"``
#: is a shorthand the router/REPL expand into the full set.
CACHE_TYPES = ("schemas", "columns", "catalog")
_VALID_CLEAR_TYPES = frozenset({*CACHE_TYPES, "all"})


@dataclass
class CacheRow:
    """One row in the per-(profile, database) inventory view."""

    profile: str
    database: str
    schemas_rows: int
    columns_rows: int
    catalog_rows: int
    last_fetch: float | None


@dataclass
class CacheStat:
    """Aggregate metrics for one cache table."""

    table: str
    total_rows: int
    distinct_profiles: int
    distinct_databases: int
    oldest_fetch: float | None
    newest_fetch: float | None
    expired_rows: int
    ttl_aware: bool


@dataclass
class CacheClearReport:
    """Per-table delete counts returned from :func:`cache_clear`."""

    deleted: dict[str, int] = field(default_factory=dict)
    scope: dict[str, str | None] = field(default_factory=dict)
    types: list[str] = field(default_factory=list)

    @property
    def total(self) -> int:
        return sum(self.deleted.values())


def _resolve_db_path() -> str | None:
    hs = history_store()
    if hs is None:
        return None
    path = str(getattr(hs, "db_path", "") or "")
    return path or None


def _open(read_only: bool = True) -> sqlite3.Connection | None:
    """Open a short-lived sqlite3 connection bound to the active store.

    Returns ``None`` when the store hasn't been initialised yet (fresh
    install, no history-store created). Callers MUST treat that as
    "nothing to show" rather than an error — the same convention every
    other catalog-aware surface follows.
    """
    path = _resolve_db_path()
    if not path:
        return None
    try:
        conn = sqlite3.connect(path, timeout=5)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.OperationalError:
        return None


def cache_inventory(
    *,
    profile: str | None = None,
    database: str | None = None,
) -> list[CacheRow]:
    """Return per-(profile, database) row counts + last-fetch timestamp
    across the three cache tables.

    Optional ``profile`` / ``database`` filters narrow the result so the
    REPL ``/db cache-show --profile=X --database=Y`` form only renders
    the rows the user asked for. With both filters omitted, every
    (profile, database) pair with at least one cached row appears.
    """
    conn = _open()
    if conn is None:
        return []
    try:
        seen: dict[tuple[str, str], dict[str, Any]] = {}

        def _accum(rows: list[sqlite3.Row], key: str) -> None:
            for r in rows:
                k = (str(r["db_profile"] or ""), str(r["database_name"] or ""))
                entry = seen.setdefault(
                    k,
                    {
                        "schemas_rows": 0,
                        "columns_rows": 0,
                        "catalog_rows": 0,
                        "last_fetch": None,
                    },
                )
                entry[key] = int(r["n"] or 0)
                last = r["last_fetch"]
                if last is not None:
                    last_f = float(last)
                    if entry["last_fetch"] is None or last_f > entry["last_fetch"]:
                        entry["last_fetch"] = last_f

        params: list[Any] = []
        where = "1 = 1"
        if profile:
            where += " AND db_profile = ?"
            params.append(profile)
        if database is not None:
            where += " AND database_name = ?"
            params.append(database)

        _accum(
            conn.execute(
                f"""SELECT db_profile, database_name,
                          COUNT(*) AS n,
                          MAX(fetched_at) AS last_fetch
                   FROM schemas_cache WHERE {where}
                   GROUP BY db_profile, database_name""",
                params,
            ).fetchall(),
            "schemas_rows",
        )
        _accum(
            conn.execute(
                f"""SELECT db_profile, database_name,
                          COUNT(*) AS n,
                          MAX(fetched_at) AS last_fetch
                   FROM column_comments_cache WHERE {where}
                   GROUP BY db_profile, database_name""",
                params,
            ).fetchall(),
            "columns_rows",
        )
        # catalog_entities uses last_synced_at instead of fetched_at; we
        # surface it under the same ``last_fetch`` column so the UI
        # renders one timestamp per row regardless of source.
        _accum(
            conn.execute(
                f"""SELECT db_profile, database_name,
                          COUNT(*) AS n,
                          MAX(last_synced_at) AS last_fetch
                   FROM catalog_entities WHERE {where}
                   GROUP BY db_profile, database_name""",
                params,
            ).fetchall(),
            "catalog_rows",
        )
    finally:
        conn.close()

    return sorted(
        (
            CacheRow(
                profile=p,
                database=d,
                schemas_rows=int(v["schemas_rows"]),
                columns_rows=int(v["columns_rows"]),
                catalog_rows=int(v["catalog_rows"]),
                last_fetch=v["last_fetch"],
            )
            for (p, d), v in seen.items()
        ),
        key=lambda r: (r.profile, r.database),
    )


def cache_stats(
    *,
    valid_profiles: Iterable[str] | None = None,
) -> dict[str, CacheStat]:
    """Aggregate metrics per cache table.

    ``ttl_aware`` is True for ``schemas_cache`` + ``column_comments_cache``
    (they carry ``expires_at`` so a stale-row count is meaningful) and
    False for ``catalog_entities`` — the catalog is rewritten by
    ``/sync``, never sweeps itself.

    When ``valid_profiles`` is set, every row in the underlying tables
    whose ``db_profile`` is *not* a member is excluded from the
    aggregate. The Studio Catalog cache page passes the configured
    profile set so a deleted-profile tombstone never inflates the
    headline numbers. Pass ``None`` (the default) to keep the legacy,
    unfiltered shape — used by the REPL ``/db cache-stats`` view.
    """
    conn = _open()
    if conn is None:
        return {}
    try:
        now = time.time()
        result: dict[str, CacheStat] = {}

        valid_filter, valid_params = _profile_filter(valid_profiles)

        def _stat_ttl(table: str) -> CacheStat:
            row = conn.execute(
                f"""SELECT COUNT(*) AS n,
                          COUNT(DISTINCT db_profile) AS p,
                          COUNT(DISTINCT database_name) AS d,
                          MIN(fetched_at) AS oldest,
                          MAX(fetched_at) AS newest,
                          SUM(CASE WHEN expires_at < ? THEN 1 ELSE 0 END) AS expired
                     FROM {table}
                     WHERE 1 = 1{valid_filter}""",
                (now, *valid_params),
            ).fetchone()
            return CacheStat(
                table=table,
                total_rows=int(row["n"] or 0),
                distinct_profiles=int(row["p"] or 0),
                distinct_databases=int(row["d"] or 0),
                oldest_fetch=float(row["oldest"]) if row["oldest"] else None,
                newest_fetch=float(row["newest"]) if row["newest"] else None,
                expired_rows=int(row["expired"] or 0),
                ttl_aware=True,
            )

        result["schemas"] = _stat_ttl("schemas_cache")
        result["columns"] = _stat_ttl("column_comments_cache")

        # catalog_entities uses last_synced_at; never expires.
        row = conn.execute(
            f"""SELECT COUNT(*) AS n,
                      COUNT(DISTINCT db_profile) AS p,
                      COUNT(DISTINCT database_name) AS d,
                      MIN(last_synced_at) AS oldest,
                      MAX(last_synced_at) AS newest
                 FROM catalog_entities
                 WHERE 1 = 1{valid_filter}""",
            valid_params,
        ).fetchone()
        result["catalog"] = CacheStat(
            table="catalog_entities",
            total_rows=int(row["n"] or 0),
            distinct_profiles=int(row["p"] or 0),
            distinct_databases=int(row["d"] or 0),
            oldest_fetch=float(row["oldest"]) if row["oldest"] else None,
            newest_fetch=float(row["newest"]) if row["newest"] else None,
            expired_rows=0,
            ttl_aware=False,
        )
        return result
    finally:
        conn.close()


def _profile_filter(
    valid_profiles: Iterable[str] | None,
) -> tuple[str, tuple[str, ...]]:
    """Render an ``AND db_profile IN (...)`` clause + bind values.

    Returns ``("", ())`` when ``valid_profiles`` is ``None`` so the
    legacy (unfiltered) callers keep the same SQL shape. When the
    caller supplies an empty iterable the clause becomes
    ``AND 1 = 0`` — that's the "user has zero configured profiles"
    state and matching no rows is the correct answer.
    """
    if valid_profiles is None:
        return "", ()
    names = tuple({str(p) for p in valid_profiles if p})
    if not names:
        return " AND 1 = 0", ()
    placeholders = ",".join("?" for _ in names)
    return f" AND db_profile IN ({placeholders})", names


def cache_runtime_counters() -> dict[str, Any]:
    """Snapshot the in-process counters that complement the persistent
    cache stats.

    Returns two sections:

    * ``listing_memo`` — wizard-driven ``list_catalogs`` / ``list_databases``
      hit/miss tallies from the connector's in-memory memo.
    * ``drift_probe`` — how many handshakes the cache-age gate skipped
      vs how many actually ran a live count.

    Imports are deferred so this helper stays cheap to call when those
    modules haven't been loaded yet (e.g. ``/db cache stats`` before
    any DB connection has been opened).
    """
    out: dict[str, Any] = {"listing_memo": {}, "drift_probe": {}}
    try:
        from amx.db.connector import get_listing_memo_counters

        out["listing_memo"] = get_listing_memo_counters()
    except Exception:
        pass
    try:
        from amx.search.drift import get_drift_probe_counters

        out["drift_probe"] = get_drift_probe_counters()
    except Exception:
        pass
    return out


def _expand_types(types: list[str] | None) -> list[str]:
    if not types:
        return list(CACHE_TYPES)
    bad = [t for t in types if t not in _VALID_CLEAR_TYPES]
    if bad:
        raise ValueError(
            f"Unknown cache types: {bad!r}; expected one of {sorted(_VALID_CLEAR_TYPES)!r}"
        )
    if "all" in types:
        return list(CACHE_TYPES)
    # Dedupe while preserving order.
    seen: set[str] = set()
    out: list[str] = []
    for t in types:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


def cache_clear(
    *,
    profile: str | None = None,
    database: str | None = None,
    types: list[str] | None = None,
) -> CacheClearReport:
    """Delete rows from the requested cache tables, scoped to
    ``(profile, database)``. ``types=None`` (the default) clears all
    three tables — schemas, columns, and catalog. Pass a subset
    (e.g. ``["schemas"]``) to flush one cache type at a time.

    Both filters may be ``None`` simultaneously, in which case the
    caller is asking for a global flush — every profile, every
    database. The REPL + router gate that behind an explicit ``--force``
    flag; this function doesn't ask, it just does what it was told.
    """
    expanded = _expand_types(types)
    report = CacheClearReport(
        scope={"profile": profile or None, "database": database if database is not None else None},
        types=expanded,
    )

    hs = history_store()
    if hs is None:
        return report

    # ``schemas_cache`` flush — reuse the store's invalidate helper. It
    # gates on ``catalog`` for finer scoping, but at this layer we want
    # whole-database flushes so we pass only the profile / database.
    if "schemas" in expanded:
        rowcount = _delete_scoped(
            "schemas_cache",
            profile=profile,
            database=database,
        )
        report.deleted["schemas"] = rowcount

    if "columns" in expanded:
        rowcount = _delete_scoped(
            "column_comments_cache",
            profile=profile,
            database=database,
        )
        report.deleted["columns"] = rowcount

    if "catalog" in expanded:
        deleted = _clear_catalog_tables(profile=profile, database=database)
        report.deleted["catalog"] = deleted

    return report


def _delete_scoped(
    table: str,
    *,
    profile: str | None,
    database: str | None,
) -> int:
    """Targeted DELETE for the TTL-aware caches. Composes the same
    ``(db_profile, database_name)`` predicate the store's
    ``invalidate_*`` helpers use, but lives here so we can run the
    DELETE inside a single connection alongside the catalog flush.
    """
    conn = _open()
    if conn is None:
        return 0
    try:
        params: list[Any] = []
        sql = f"DELETE FROM {table} WHERE 1 = 1"
        if profile is not None:
            sql += " AND db_profile = ?"
            params.append(profile)
        if database is not None:
            sql += " AND database_name = ?"
            params.append(database)
        cur = conn.execute(sql, params)
        conn.commit()
        return int(cur.rowcount or 0)
    finally:
        conn.close()


def purge_orphan_profile_rows(
    valid_profiles: Iterable[str],
) -> dict[str, int]:
    """Delete every cache row whose ``db_profile`` is not in
    ``valid_profiles``. Returns per-table delete counts.

    Idempotent and safe to call at startup. Covers the three on-disk
    cache tables (``schemas_cache``, ``column_comments_cache``,
    ``catalog_entities``) plus the dependent rows in
    ``catalog_descriptions`` and ``catalog_profile_state`` so the
    state row that drives the Studio freshness pill disappears with
    the rest of the profile's footprint.

    Used by:

    * the Studio app startup hook (one-time backfill sweep so the
      Catalog cache page reflects only configured profiles), and
    * :func:`amx.config.AMXConfig.remove_db_profile` (eager purge the
      moment a profile is removed) — through the same helper so the
      two paths can't drift.
    """
    names = tuple({str(p) for p in valid_profiles if p})
    conn = _open(read_only=False)
    if conn is None:
        return {}
    deleted: dict[str, int] = {}
    try:
        if not names:
            # No configured profiles → every row is an orphan.
            ce_count = int(
                conn.execute(
                    "SELECT COUNT(*) AS n FROM catalog_entities"
                ).fetchone()["n"]
                or 0
            )
            conn.execute("DELETE FROM catalog_descriptions")
            conn.execute("DELETE FROM catalog_entities")
            try:
                conn.execute("DELETE FROM catalog_profile_state")
            except sqlite3.OperationalError:
                pass
            sc = conn.execute("DELETE FROM schemas_cache").rowcount or 0
            cc = conn.execute(
                "DELETE FROM column_comments_cache"
            ).rowcount or 0
            conn.commit()
            return {
                "catalog_entities": ce_count,
                "schemas_cache": int(sc),
                "column_comments_cache": int(cc),
            }

        placeholders = ",".join("?" for _ in names)
        not_in = f"db_profile NOT IN ({placeholders})"

        # Count catalog_entities orphans BEFORE deleting; the
        # post-delete rowcount is not portable across sqlite versions
        # when a foreign key dependency was wiped in the same txn.
        ce_count = int(
            conn.execute(
                f"SELECT COUNT(*) AS n FROM catalog_entities WHERE {not_in}",
                names,
            ).fetchone()["n"]
            or 0
        )
        # Descriptions first so the FK isn't orphaned.
        conn.execute(
            f"""DELETE FROM catalog_descriptions
                 WHERE entity_id IN (
                     SELECT id FROM catalog_entities WHERE {not_in}
                 )""",
            names,
        )
        conn.execute(
            f"DELETE FROM catalog_entities WHERE {not_in}",
            names,
        )
        try:
            conn.execute(
                f"DELETE FROM catalog_profile_state WHERE {not_in}",
                names,
            )
        except sqlite3.OperationalError:
            pass
        sc = conn.execute(
            f"DELETE FROM schemas_cache WHERE {not_in}",
            names,
        ).rowcount or 0
        cc = conn.execute(
            f"DELETE FROM column_comments_cache WHERE {not_in}",
            names,
        ).rowcount or 0
        conn.commit()
        deleted = {
            "catalog_entities": ce_count,
            "schemas_cache": int(sc),
            "column_comments_cache": int(cc),
        }
    finally:
        conn.close()
    return deleted


def _clear_catalog_tables(
    *,
    profile: str | None,
    database: str | None,
) -> int:
    """Wipe ``catalog_entities`` + ``catalog_descriptions`` +
    ``catalog_profile_state`` rows under the requested scope. Returns
    the count from ``catalog_entities`` (the headline number); the
    descriptions and state rows are joined-cleared in the same
    transaction so a future ``/sync`` repopulates from a clean slate.

    ``catalog_descriptions`` is keyed by ``entity_id`` (FK to
    ``catalog_entities``), so we delete via subquery rather than
    requiring the caller to know the foreign-key layout.

    ``catalog_profile_state`` is per-profile only — it has no
    ``database_name`` column. When a database filter is provided we
    leave the state row in place because there's no per-database
    state to flush, and rewriting the row could mask other databases'
    in-progress sync state. When the scope is a whole profile the row
    is cleared so the freshness pill flips back to "never".
    """
    conn = _open()
    if conn is None:
        return 0
    try:
        params: list[Any] = []
        where = "1 = 1"
        if profile is not None:
            where += " AND db_profile = ?"
            params.append(profile)
        if database is not None:
            where += " AND database_name = ?"
            params.append(database)

        # Delete descriptions first so the FK isn't orphaned.
        conn.execute(
            f"""DELETE FROM catalog_descriptions
                 WHERE entity_id IN (
                     SELECT id FROM catalog_entities WHERE {where}
                 )""",
            params,
        )
        cur = conn.execute(
            f"DELETE FROM catalog_entities WHERE {where}",
            params,
        )
        entity_count = int(cur.rowcount or 0)

        if database is None:
            # Whole-profile flush: clear the state row too.
            state_sql = "DELETE FROM catalog_profile_state WHERE 1 = 1"
            state_params: list[Any] = []
            if profile is not None:
                state_sql += " AND db_profile = ?"
                state_params.append(profile)
            try:
                conn.execute(state_sql, state_params)
            except sqlite3.OperationalError:
                # Older databases predating the state table; tolerate.
                pass
        conn.commit()
        return entity_count
    finally:
        conn.close()
