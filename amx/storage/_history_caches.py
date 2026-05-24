"""History-store cache table helpers.

Extracted from :mod:`amx.storage.sqlite_store` so the three cache
tables (``run_context_cache``, ``column_comments_cache``,
``schemas_cache``) and their save/lookup/invalidate/gc methods live
in one focused module. Each function takes the store as ``hs`` and
reuses its ``_lock`` + ``_connect()`` plumbing — no DDL touched.

The :class:`SQLiteHistoryStore` keeps a thin delegator for every name
so the dozens of callers (orchestrator, connector, /db cache CLI
commands, web routers) stay byte-compatible.
"""

from __future__ import annotations

import json
import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from amx.storage.sqlite_store import SQLiteHistoryStore


def _context_cache_key(
    *,
    db_profile: str,
    database: str,
    schema: str,
    table: str,
) -> str:
    return f"{db_profile}|{database or ''}|{schema}|{table}"


def save_run_context_cache(
    hs: SQLiteHistoryStore,
    *,
    db_profile: str,
    database: str,
    schema: str,
    table: str,
    payload: dict[str, Any],
    source_run_id: int | None = None,
    ttl_seconds: float = 86400.0,
) -> None:
    """Persist a table-level context snapshot for re-use on re-run.

    ``payload`` is the JSON-serialisable dict the rerun executor
    normally rebuilds via ``_build_db_profile_dict`` — keys at
    minimum: ``db_profile`` (the column-aware profile dict) and
    ``existing_metadata``.  ``ttl_seconds`` defaults to 24 hours so
    a stale schema can't silently produce wrong descriptions when
    the user re-runs a week later.

    Uses ``INSERT OR REPLACE`` keyed on
    (db_profile, database, schema, table) so a re-analyze of the
    same table refreshes the cache rather than appending duplicates.
    """
    cache_key = hs._context_cache_key(
        db_profile=str(db_profile or ""),
        database=str(database or ""),
        schema=str(schema or ""),
        table=str(table or ""),
    )
    now = time.time()
    with hs._lock, hs._connect() as conn:
        conn.execute(
            """
            INSERT INTO run_context_cache
                (cache_key, db_profile, database_name, schema_name, table_name,
                 payload_json, source_run_id, created_at, expires_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(cache_key) DO UPDATE SET
                payload_json = excluded.payload_json,
                source_run_id = excluded.source_run_id,
                created_at = excluded.created_at,
                expires_at = excluded.expires_at
            """,
            (
                cache_key,
                str(db_profile or ""),
                str(database or ""),
                str(schema or ""),
                str(table or ""),
                json.dumps(payload, ensure_ascii=True),
                int(source_run_id) if source_run_id is not None else None,
                now,
                (now + float(ttl_seconds)) if ttl_seconds > 0 else None,
            ),
        )


def lookup_run_context_cache(
    hs: SQLiteHistoryStore,
    *,
    db_profile: str,
    database: str,
    schema: str,
    table: str,
) -> dict[str, Any] | None:
    """Return the cached payload for a table, or ``None`` if missing/expired.

    Expired rows are kept on disk (cheaper than rewriting) but the
    lookup pretends they're absent so callers always rebuild from
    the live database. ``gc_run_context_cache`` reaps them.
    """
    cache_key = hs._context_cache_key(
        db_profile=str(db_profile or ""),
        database=str(database or ""),
        schema=str(schema or ""),
        table=str(table or ""),
    )
    with hs._connect() as conn:
        row = conn.execute(
            """
            SELECT payload_json, expires_at, source_run_id, created_at
            FROM run_context_cache
            WHERE cache_key = ?
            """,
            (cache_key,),
        ).fetchone()
    if row is None:
        return None
    expires_at = row["expires_at"]
    if expires_at is not None and float(expires_at) < time.time():
        return None
    try:
        payload = json.loads(row["payload_json"])
    except Exception:
        return None
    return {
        "payload": payload,
        "source_run_id": row["source_run_id"],
        "created_at": float(row["created_at"]),
    }


def delete_run_context_cache(
    hs: SQLiteHistoryStore,
    *,
    db_profile: str,
    database: str,
    schema: str,
    table: str,
) -> int:
    """Drop the cache row for a single table; returns rowcount.

    Called from the apply path after a successful COMMENT write so
    we don't keep stale-but-valid context around for a row the
    user has already accepted.
    """
    cache_key = hs._context_cache_key(
        db_profile=str(db_profile or ""),
        database=str(database or ""),
        schema=str(schema or ""),
        table=str(table or ""),
    )
    with hs._lock, hs._connect() as conn:
        cur = conn.execute(
            "DELETE FROM run_context_cache WHERE cache_key = ?",
            (cache_key,),
        )
        return int(cur.rowcount or 0)


def gc_run_context_cache(hs: SQLiteHistoryStore) -> int:
    """Sweep cache rows past their TTL; called at process startup."""
    now = time.time()
    with hs._lock, hs._connect() as conn:
        cur = conn.execute(
            "DELETE FROM run_context_cache WHERE expires_at IS NOT NULL AND expires_at < ?",
            (now,),
        )
        return int(cur.rowcount or 0)


def _ccc_key(*, db_profile: str, database: str, schema: str, table: str) -> str:
    return f"{db_profile}|{database or ''}|{schema}|{table}"


def save_column_comments_cache(
    hs: SQLiteHistoryStore,
    *,
    db_profile: str,
    database: str,
    schema: str,
    entries: dict[str, dict[str, Any]],
    ttl_seconds: float = 3600.0,
    bulk_filled: bool = False,
) -> int:
    """Bulk upsert per-table entries after one ``bulk_schema_metadata`` call.

    ``entries`` maps each ``table_name`` to a dict with keys:
    ``table_comment`` (str | None), ``columns`` (dict[col_name, comment_or_none]),
    ``kind`` ("TABLE" | "VIEW" | "MATERIALIZED VIEW"). Missing keys default
    to ``None`` / empty / "TABLE" so callers can pass partial payloads
    when the backend only returns column-level data.

    ``bulk_filled`` records *how* the entries arrived: ``True`` for a
    successful bulk-adapter call (the dict covers every table in the
    schema by contract), ``False`` for per-table fallback writes. The
    flag is what lets ``list_assets`` know whether the cache is safe
    to read instead of re-issuing SHOW TABLES.
    """
    if not entries:
        return 0
    now = time.time()
    # ``ttl_seconds == 0`` defaults to one hour to match the helper's
    # default kwarg; negative values are honoured verbatim so tests
    # can stamp rows as already-expired.
    if ttl_seconds == 0:
        ttl_seconds = 3600.0
    expires_at = now + float(ttl_seconds)
    flag = 1 if bulk_filled else 0
    rows = [
        (
            hs._ccc_key(
                db_profile=str(db_profile or ""),
                database=str(database or ""),
                schema=str(schema or ""),
                table=str(table or ""),
            ),
            str(db_profile or ""),
            str(database or ""),
            str(schema or ""),
            str(table or ""),
            payload.get("table_comment"),
            json.dumps(payload.get("columns") or {}, ensure_ascii=True),
            str(payload.get("kind") or "TABLE"),
            now,
            expires_at,
            flag,
        )
        for table, payload in entries.items()
    ]
    with hs._lock, hs._connect() as conn:
        conn.executemany(
            """
            INSERT INTO column_comments_cache
                (cache_key, db_profile, database_name, schema_name, table_name,
                 table_comment, columns_json, kind, fetched_at, expires_at, bulk_filled)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(cache_key) DO UPDATE SET
                table_comment = excluded.table_comment,
                columns_json  = excluded.columns_json,
                kind          = excluded.kind,
                fetched_at    = excluded.fetched_at,
                expires_at    = excluded.expires_at,
                bulk_filled   = MAX(column_comments_cache.bulk_filled, excluded.bulk_filled)
            """,
            rows,
        )
    return len(rows)


def schema_has_bulk_filled_cache(
    hs: SQLiteHistoryStore,
    *,
    db_profile: str,
    database: str,
    schema: str,
) -> bool:
    """``True`` when at least one fresh ``bulk_filled=1`` row exists
    for ``(profile, database, schema)``.

    Presence of one bulk-filled row implies the whole schema is
    covered by the cache (bulk_schema_metadata returns every table
    in the schema by contract). ``list_assets`` keys off this flag
    to decide whether reading from cache is safe — partial caches
    produced by the per-table fallback path are not.
    """
    now = time.time()
    with hs._connect() as conn:
        row = conn.execute(
            """
            SELECT 1 FROM column_comments_cache
            WHERE db_profile = ? AND database_name = ? AND schema_name = ?
              AND bulk_filled = 1 AND expires_at >= ?
            LIMIT 1
            """,
            (
                str(db_profile or ""),
                str(database or ""),
                str(schema or ""),
                now,
            ),
        ).fetchone()
    return row is not None


def lookup_column_comments_cache(
    hs: SQLiteHistoryStore,
    *,
    db_profile: str,
    database: str,
    schema: str,
    table: str,
) -> dict[str, Any] | None:
    """Return a single fresh cache entry, or ``None`` if missing/expired.

    Returned shape: ``{"table_comment": ..., "columns": {...}, "kind": ...,
    "fetched_at": ..., "expires_at": ...}``. Expired rows are kept on disk
    (cheaper than rewriting) but the lookup pretends they're absent.
    """
    cache_key = hs._ccc_key(
        db_profile=str(db_profile or ""),
        database=str(database or ""),
        schema=str(schema or ""),
        table=str(table or ""),
    )
    with hs._connect() as conn:
        row = conn.execute(
            """
            SELECT table_comment, columns_json, kind, fetched_at, expires_at
            FROM column_comments_cache
            WHERE cache_key = ?
            """,
            (cache_key,),
        ).fetchone()
    if row is None:
        return None
    if float(row["expires_at"]) < time.time():
        return None
    try:
        columns = json.loads(row["columns_json"])
    except Exception:
        columns = {}
    return {
        "table_comment": row["table_comment"],
        "columns": columns,
        "kind": row["kind"] or "TABLE",
        "fetched_at": float(row["fetched_at"]),
        "expires_at": float(row["expires_at"]),
    }


def lookup_column_comments_cache_bulk(
    hs: SQLiteHistoryStore,
    *,
    db_profile: str,
    database: str,
    schema: str,
) -> dict[str, dict[str, Any]]:
    """Return ``{table_name: cached_entry}`` for every fresh row in a schema.

    Used by the connector's bulk path to decide whether a refetch is
    needed at all. Expired rows are skipped.
    """
    now = time.time()
    with hs._connect() as conn:
        rows = conn.execute(
            """
            SELECT table_name, table_comment, columns_json, kind, fetched_at, expires_at
            FROM column_comments_cache
            WHERE db_profile = ? AND database_name = ? AND schema_name = ?
              AND expires_at >= ?
            """,
            (
                str(db_profile or ""),
                str(database or ""),
                str(schema or ""),
                now,
            ),
        ).fetchall()
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        try:
            columns = json.loads(row["columns_json"])
        except Exception:
            columns = {}
        out[str(row["table_name"])] = {
            "table_comment": row["table_comment"],
            "columns": columns,
            "kind": row["kind"] or "TABLE",
            "fetched_at": float(row["fetched_at"]),
            "expires_at": float(row["expires_at"]),
        }
    return out


def lookup_column_profiles_cache(
    hs: SQLiteHistoryStore,
    *,
    db_profile: str,
    database: str,
    schema: str,
    table: str,
) -> list[dict[str, Any]]:
    """Return cached column profiles ``[{"name", "dtype", "nullable"}]``.

    Reads the ``column_profiles_cache`` table populated by historical
    profiling runs. Three properties set this apart from
    :func:`lookup_column_comments_cache`:

    1. **TTL-agnostic.** Rows are served regardless of ``expires_at``.
       The Studio cache-first browse contract is "surface whatever was
       cached rather than silently issue a live-DB round-trip the user
       did not ask for" — so an expired profile row is still better
       than an empty table page.
    2. **Scope-tolerant.** The exact ``(profile, database, schema,
       table)`` row is preferred, but the SPA sometimes omits the
       ``database`` query parameter on cold navigation; the freshest
       row for ``(profile, schema, table)`` across any database is the
       fallback.
    3. **Legacy-safe.** ``column_profiles_cache`` is not in the current
       init DDL and has no live writer — a fresh install simply lacks
       the table. A missing table (or any read error) yields ``[]`` so
       callers fall through to their next source.

    Carries column dtype + nullable, which the name-only
    ``column_comments_cache`` map cannot.
    """
    profile = str(db_profile or "")
    db = str(database or "")
    sch = str(schema or "")
    tbl = str(table or "")
    try:
        with hs._connect() as conn:
            row = None
            if db:
                row = conn.execute(
                    """
                    SELECT profiles_json FROM column_profiles_cache
                    WHERE db_profile = ? AND database_name = ?
                      AND schema_name = ? AND table_name = ?
                    ORDER BY fetched_at DESC LIMIT 1
                    """,
                    (profile, db, sch, tbl),
                ).fetchone()
            if row is None:
                row = conn.execute(
                    """
                    SELECT profiles_json FROM column_profiles_cache
                    WHERE db_profile = ? AND schema_name = ? AND table_name = ?
                    ORDER BY fetched_at DESC LIMIT 1
                    """,
                    (profile, sch, tbl),
                ).fetchone()
    except Exception:
        # Table absent on a fresh install, or any read error — let the
        # caller fall through to its next cache layer.
        return []
    if row is None:
        return []
    try:
        data = json.loads(row["profiles_json"]) or []
    except Exception:
        return []
    out: list[dict[str, Any]] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "")
        if not name:
            continue
        out.append(
            {
                "name": name,
                "dtype": str(item.get("dtype") or ""),
                "nullable": bool(item.get("nullable", True)),
            }
        )
    return out


def invalidate_column_comments_cache(
    hs: SQLiteHistoryStore,
    *,
    db_profile: str,
    database: str = "",
    schema: str | None = None,
    table: str | None = None,
    match_any_database: bool = False,
) -> int:
    """Drop cached rows at one of three granularities.

    * ``schema`` + ``table`` set → single row (column/table comment write).
    * ``schema`` set, ``table`` ``None`` → whole schema (schema comment write).
    * Both ``None`` → whole profile (database comment write, profile reset).

    ``match_any_database`` widens the ``schema``-level and table-level
    filters to ignore ``database_name``. Used by the apply path: a
    pending entry doesn't carry the originating ``db_profile`` /
    ``database`` / ``catalog`` triple (the file just stores schema +
    table + column + final_description), so the apply worker falls
    back to ``cfg.active_db_profile`` + the profile's pinned database.
    When the Studio snapshot endpoint was opened with a different
    ``?database=`` query (the user navigated to a non-pinned DB), the
    apply's targeted invalidate wipes a different cache row than the
    snapshot reads on its next refresh, and the stale entry survives.
    Wiping across every ``database_name`` for the same
    ``(db_profile, schema, table)`` triple removes that gap.

    Returns rowcount. Always safe — a no-op on a cold cache returns 0.
    """
    params: list[Any] = [str(db_profile or "")]
    sql = "DELETE FROM column_comments_cache WHERE db_profile = ?"
    # ``database_name`` is empty string when the profile is single-db;
    # we filter on it whenever a schema is named so a multi-db profile
    # only wipes the affected database — unless ``match_any_database``
    # is True, in which case the apply-path belt-and-braces drops every
    # ``database_name`` row for the same ``(profile, schema, table)``.
    if schema is not None:
        if match_any_database:
            sql += " AND schema_name = ?"
            params.append(str(schema or ""))
        else:
            sql += " AND database_name = ? AND schema_name = ?"
            params.append(str(database or ""))
            params.append(str(schema or ""))
    if table is not None:
        sql += " AND table_name = ?"
        params.append(str(table or ""))
    with hs._lock, hs._connect() as conn:
        cur = conn.execute(sql, params)
        return int(cur.rowcount or 0)


def gc_column_comments_cache(hs: SQLiteHistoryStore) -> int:
    """Sweep expired rows; called at process startup alongside other GC."""
    now = time.time()
    with hs._lock, hs._connect() as conn:
        cur = conn.execute(
            "DELETE FROM column_comments_cache WHERE expires_at < ?",
            (now,),
        )
        return int(cur.rowcount or 0)


def _sc_key(*, db_profile: str, database: str, catalog: str, schema: str) -> str:
    return f"{db_profile}|{database or ''}|{catalog or ''}|{schema}"


def save_schemas_cache(
    hs: SQLiteHistoryStore,
    *,
    db_profile: str,
    database: str,
    catalog: str,
    entries: dict[str, str | None],
    ttl_seconds: float = 3600.0,
    bulk_filled: bool = False,
) -> int:
    """Bulk upsert schema-level entries for one catalog.

    ``entries`` maps schema name → schema comment (``None`` when the
    schema has no comment). ``bulk_filled`` mirrors the column
    cache's flag: ``True`` when a single ``bulk_catalog_metadata``
    call produced the dict (covers every schema in the catalog),
    ``False`` for per-schema fallback writes.
    """
    if not entries:
        return 0
    now = time.time()
    if ttl_seconds == 0:
        ttl_seconds = 3600.0
    expires_at = now + float(ttl_seconds)
    flag = 1 if bulk_filled else 0
    rows = [
        (
            hs._sc_key(
                db_profile=str(db_profile or ""),
                database=str(database or ""),
                catalog=str(catalog or ""),
                schema=str(schema or ""),
            ),
            str(db_profile or ""),
            str(database or ""),
            str(catalog or ""),
            str(schema or ""),
            comment,
            flag,
            now,
            expires_at,
        )
        for schema, comment in entries.items()
    ]
    with hs._lock, hs._connect() as conn:
        conn.executemany(
            """
            INSERT INTO schemas_cache
                (cache_key, db_profile, database_name, catalog_name, schema_name,
                 schema_comment, bulk_filled, fetched_at, expires_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(cache_key) DO UPDATE SET
                schema_comment = excluded.schema_comment,
                bulk_filled    = MAX(schemas_cache.bulk_filled, excluded.bulk_filled),
                fetched_at     = excluded.fetched_at,
                expires_at     = excluded.expires_at
            """,
            rows,
        )
    return len(rows)


def lookup_schemas_cache(
    hs: SQLiteHistoryStore,
    *,
    db_profile: str,
    database: str,
    catalog: str,
    schema: str,
) -> dict[str, Any] | None:
    """Return one fresh schema entry or ``None`` if missing/expired."""
    cache_key = hs._sc_key(
        db_profile=str(db_profile or ""),
        database=str(database or ""),
        catalog=str(catalog or ""),
        schema=str(schema or ""),
    )
    with hs._connect() as conn:
        row = conn.execute(
            """
            SELECT schema_comment, bulk_filled, fetched_at, expires_at
            FROM schemas_cache WHERE cache_key = ?
            """,
            (cache_key,),
        ).fetchone()
    if row is None or float(row["expires_at"]) < time.time():
        return None
    return {
        "schema_comment": row["schema_comment"],
        "bulk_filled": bool(row["bulk_filled"]),
        "fetched_at": float(row["fetched_at"]),
        "expires_at": float(row["expires_at"]),
    }


def catalog_has_bulk_filled_cache(
    hs: SQLiteHistoryStore,
    *,
    db_profile: str,
    database: str,
    catalog: str,
) -> bool:
    """``True`` when at least one fresh ``bulk_filled=1`` row exists
    for ``(profile, database, catalog)``.

    ``list_schemas`` keys off this flag to decide whether reading
    schema names from the cache is safe instead of re-issuing
    SHOW SCHEMAS / pg_namespace lookups.
    """
    now = time.time()
    with hs._connect() as conn:
        row = conn.execute(
            """
            SELECT 1 FROM schemas_cache
            WHERE db_profile = ? AND database_name = ? AND catalog_name = ?
              AND bulk_filled = 1 AND expires_at >= ?
            LIMIT 1
            """,
            (
                str(db_profile or ""),
                str(database or ""),
                str(catalog or ""),
                now,
            ),
        ).fetchone()
    return row is not None


def list_schemas_from_cache(
    hs: SQLiteHistoryStore,
    *,
    db_profile: str,
    database: str,
    catalog: str,
) -> list[tuple[str, str | None]]:
    """Return ``[(schema_name, schema_comment), …]`` for every fresh
    row of this catalog. Caller is responsible for checking
    ``catalog_has_bulk_filled_cache`` first if it needs to know
    whether the list is exhaustive.
    """
    now = time.time()
    with hs._connect() as conn:
        rows = conn.execute(
            """
            SELECT schema_name, schema_comment FROM schemas_cache
            WHERE db_profile = ? AND database_name = ? AND catalog_name = ?
              AND expires_at >= ?
            ORDER BY schema_name
            """,
            (
                str(db_profile or ""),
                str(database or ""),
                str(catalog or ""),
                now,
            ),
        ).fetchall()
    return [(str(r[0]), r[1]) for r in rows]


def invalidate_schemas_cache(
    hs: SQLiteHistoryStore,
    *,
    db_profile: str,
    database: str = "",
    catalog: str | None = None,
    schema: str | None = None,
) -> int:
    """Drop schema-cache rows at one of three granularities.

    * ``catalog`` + ``schema`` set → single schema row.
    * ``catalog`` only → whole catalog.
    * Both ``None`` → whole profile.
    """
    params: list[Any] = [str(db_profile or "")]
    sql = "DELETE FROM schemas_cache WHERE db_profile = ?"
    if catalog is not None:
        sql += " AND database_name = ? AND catalog_name = ?"
        params.append(str(database or ""))
        params.append(str(catalog or ""))
    if schema is not None:
        sql += " AND schema_name = ?"
        params.append(str(schema or ""))
    with hs._lock, hs._connect() as conn:
        cur = conn.execute(sql, params)
        return int(cur.rowcount or 0)


def gc_schemas_cache(hs: SQLiteHistoryStore) -> int:
    """Sweep expired schemas_cache rows."""
    now = time.time()
    with hs._lock, hs._connect() as conn:
        cur = conn.execute(
            "DELETE FROM schemas_cache WHERE expires_at < ?",
            (now,),
        )
        return int(cur.rowcount or 0)


def purge_out_of_scope(
    hs: SQLiteHistoryStore,
    *,
    db_profile: str,
    container: str,
) -> dict[str, int]:
    """Delete cached rows for ``db_profile`` whose container does not
    match ``container``. Idempotent. Returns deletion counts per
    table for the audit log.

    Three cache tables are purged in one transaction:

    * ``catalog_entities`` — keyed by ``database_name``
    * ``schemas_cache`` — keyed by ``database_name`` and ``catalog_name``
      (a row is kept when either column equals ``container``)
    * ``column_comments_cache`` — keyed by ``database_name``

    When ``container`` is empty the call is a no-op (the profile is
    unpinned and the legacy multi-container behavior applies).
    """
    container = str(container or "")
    counts = {
        "catalog_entities": 0,
        "schemas_cache": 0,
        "column_comments_cache": 0,
    }
    if not container:
        return counts
    with hs._lock, hs._connect() as conn:
        cur = conn.execute(
            "DELETE FROM catalog_entities WHERE db_profile = ? AND IFNULL(database_name, '') != ?",
            (db_profile, container),
        )
        counts["catalog_entities"] = int(cur.rowcount or 0)
        cur = conn.execute(
            "DELETE FROM schemas_cache "
            "WHERE db_profile = ? "
            "AND IFNULL(database_name, '') != ? "
            "AND IFNULL(catalog_name, '') != ?",
            (db_profile, container, container),
        )
        counts["schemas_cache"] = int(cur.rowcount or 0)
        cur = conn.execute(
            "DELETE FROM column_comments_cache "
            "WHERE db_profile = ? AND IFNULL(database_name, '') != ?",
            (db_profile, container),
        )
        counts["column_comments_cache"] = int(cur.rowcount or 0)
    return counts
