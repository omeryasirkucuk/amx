"""Column profiling for :class:`DatabaseConnector`.

Extracted from :mod:`amx.db.connector` so the heavy ``profile_table``
flow + its five ``_collect_*`` helpers live in one focused module.
The functions take ``_db: DatabaseConnector`` as their first
argument and read the backend / adapter / engine state off it.

``connector.py`` keeps a one-line ``DatabaseConnector.profile_table``
delegator so existing call sites (core/inference, agents, tests)
keep working unchanged.

The five helpers were originally ``DatabaseConnector._collect_*``
methods and stay underscore-prefixed in this module to signal their
internal status; only ``profile_table`` is the public surface.
"""

from __future__ import annotations

from dataclasses import fields as dc_fields
from typing import TYPE_CHECKING, Any

from sqlalchemy import text
from sqlalchemy.exc import NoSuchTableError

from amx.core.errors import actionable_error_message
from amx.db._connector_types import (
    AnalyticsMetadata,
    AssetKind,
    ColumnProfile,
    ProfilingError,
    TableProfile,
)
from amx.utils.logging import get_logger

if TYPE_CHECKING:
    from amx.db.connector import DatabaseConnector

log = get_logger("db.connector.profile")


def profile_table(
    _db: DatabaseConnector,
    schema: str,
    table: str,
    sample_size: int | None = None,
    asset_kind: AssetKind | None = None,
) -> TableProfile:
    # Fold *once* at the entry point so every downstream
    # inspector/adapter call (get_pk_constraint, get_columns,
    # fully_qualified_name, etc.) sees identifiers in the form the
    # backend stores them. Oracle / Snowflake fold to upper; other
    # backends pass through unchanged.
    schema = _db._normalize_id(schema)
    table = _db._normalize_id(table)
    if asset_kind is None:
        asset_kind = _db.resolve_asset_kind(schema, table)
    log.info("Profiling %s.%s (%s) via %s", schema, table, asset_kind.label, _db.backend)

    adapter = _db._adapter
    fqn = adapter.fully_qualified_name(schema, table)
    mode = str(getattr(_db.cfg, "profiling_mode", "full") or "full").lower().strip()
    if mode not in {"full", "sampled", "metadata"}:
        mode = "full"
    max_rows = max(0, int(getattr(_db.cfg, "profiling_max_rows", 1_000_000) or 0))
    effective_sample_size = max(
        0,
        int(
            sample_size
            if sample_size is not None
            else getattr(_db.cfg, "profiling_sample_size", 5) or 0
        ),
    )
    profile = TableProfile(
        schema=schema,
        name=table,
        asset_kind=asset_kind,
        existing_comment=_db.get_table_comment(schema, table),
        schema_comment=_db.get_schema_comment(schema),
        database_comment=_db.get_database_comment(),
    )

    try:
        stats = adapter.get_table_stats(_db.engine, schema, table)
        profile.stats_seq_scan = stats.get("seq_scan", 0)
        profile.stats_idx_scan = stats.get("idx_scan", 0)
        profile.stats_n_live_tup = stats.get("n_live_tup", 0)
    except Exception as exc:
        actionable = adapter.actionable_profile_error(exc) or actionable_error_message(
            exc, backend=_db.backend
        )
        # Always include the exception class so the user can tell a
        # ``KeyError('cars.data')`` (corrupt catalog cache) from a
        # ``SQLAlchemyOperationalError`` (DB down) — the plain
        # ``str(exc)`` fallback strips that signal. Log the full
        # traceback at WARNING so operators have the chain in
        # studio.log even when the UI shows only the one-liner.
        exc_class = type(exc).__name__
        actionable_str = (actionable or "").strip() or "(no detail)"
        msg = f"Profiling failed for {schema}.{table}: {actionable_str} [{exc_class}]"
        log.warning(
            "Profiling failed for %s.%s (class=%s, raw=%r)",
            schema,
            table,
            exc_class,
            str(exc),
            exc_info=True,
        )
        raise ProfilingError(schema, table, msg) from exc
    estimated_rows = int(profile.stats_n_live_tup or 0)
    full_scan_blocked = bool(max_rows and estimated_rows > max_rows)
    if (
        mode == "full"
        and max_rows
        and estimated_rows <= 0
        and not _db.capabilities.full_scan_when_row_count_unknown
    ):
        full_scan_blocked = True

    if mode == "full" and not full_scan_blocked:
        try:
            with _db.engine.connect() as conn:
                row_count = conn.execute(text(f"SELECT COUNT(*) FROM {fqn}")).scalar() or 0
                profile.row_count = int(row_count or 0)
        except Exception as exc:
            # Demoted from WARNING to DEBUG in v0.10.9: the exact
            # COUNT(*) failure is fully recovered by falling back
            # to the estimated row count, so the user sees no
            # functional regression. The previous WARNING leaked
            # through the live-display panel during /ask answers
            # ("[WARNING] amx.db.connector — Exact row count
            # failed for public.bkpf: ...") which alarmed users
            # despite being a no-op recovery. Operators who want
            # to investigate slow / blocked counts can still get
            # the line via ``AMX_LOG_LEVEL=debug``.
            log.debug(
                "Exact row count failed for %s.%s; falling back to "
                "estimated row count (%d). Detail: %s",
                schema,
                table,
                estimated_rows,
                exc,
            )
            profile.row_count = estimated_rows
    else:
        profile.row_count = estimated_rows

    if mode == "full" and max_rows and profile.row_count > max_rows:
        full_scan_blocked = True
    scan_column_stats = mode == "full" and not full_scan_blocked
    scan_samples = mode in {"full", "sampled"} and effective_sample_size > 0

    insp = _db._get_inspector()

    try:
        pk = insp.get_pk_constraint(table, schema=schema) or {}
        profile.primary_key = list(pk.get("constrained_columns") or [])
    except Exception:
        profile.primary_key = []

    try:
        profile.foreign_keys = list(insp.get_foreign_keys(table, schema=schema) or [])
    except Exception:
        profile.foreign_keys = []

    try:
        profile.unique_constraints = [
            list((u or {}).get("column_names") or [])
            for u in (insp.get_unique_constraints(table, schema=schema) or [])
        ]
    except Exception:
        profile.unique_constraints = []

    try:
        profile.check_constraints = [
            str((c or {}).get("sqltext") or "")
            for c in (insp.get_check_constraints(table, schema=schema) or [])
            if (c or {}).get("sqltext")
        ]
    except Exception:
        profile.check_constraints = []

    profile.referenced_by = _db.get_incoming_foreign_keys(schema, table)
    profile.related_comments = _db.get_related_table_comments(
        profile.foreign_keys, profile.referenced_by
    )

    try:
        raw_cols = insp.get_columns(table, schema=schema)
    except NoSuchTableError:
        # Re-raise as itself instead of wrapping in ``ProfilingError``.
        # The bulk worker narrow-catches this exact class to surface a
        # user-actionable "table not reachable in live DB" remediation
        # message (and, when the Studio caller supplied a cache
        # override for this asset, to substitute a metadata-only
        # profile from the catalog cache).
        log.warning(
            "NoSuchTableError on %s.%s -- propagating to caller for cache fallback",
            schema,
            table,
        )
        raise
    except Exception as exc:
        actionable = adapter.actionable_profile_error(exc) or actionable_error_message(
            exc, backend=_db.backend
        )
        # Always include the exception class so the user can tell a
        # ``KeyError('cars.data')`` (corrupt catalog cache) from a
        # ``SQLAlchemyOperationalError`` (DB down) — the plain
        # ``str(exc)`` fallback strips that signal. Log the full
        # traceback at WARNING so operators have the chain in
        # studio.log even when the UI shows only the one-liner.
        exc_class = type(exc).__name__
        actionable_str = (actionable or "").strip() or "(no detail)"
        msg = f"Profiling failed for {schema}.{table}: {actionable_str} [{exc_class}]"
        log.warning(
            "Profiling failed for %s.%s (class=%s, raw=%r)",
            schema,
            table,
            exc_class,
            str(exc),
            exc_info=True,
        )
        raise ProfilingError(schema, table, msg) from exc

    # Build the ColumnProfile list first; we need it indexed before
    # the bulk stats query so we can map result-row positions back
    # to columns.
    for col_info in raw_cols:
        col_name = col_info["name"]
        cp = ColumnProfile(
            name=col_name,
            dtype=str(col_info["type"]),
            nullable=col_info.get("nullable", True),
            row_count=profile.row_count,
        )
        cp.existing_comment = col_info.get("comment")
        profile.columns.append(cp)

    if (scan_column_stats or scan_samples) and profile.columns:
        collect_column_stats_and_samples(
            _db,
            schema=schema,
            table=table,
            fqn=fqn,
            adapter=adapter,
            column_profiles=profile.columns,
            row_count=profile.row_count,
            scan_column_stats=scan_column_stats,
            scan_samples=scan_samples,
            effective_sample_size=effective_sample_size,
        )

    # Analytics metadata — best-effort populate of partition /
    # clustering / size / format / freshness / tags. Each adapter
    # ships a backend-specific implementation; the default is an
    # empty dict so old call sites (and adapters that don't need
    # this) keep working unchanged.
    try:
        am = _db._adapter.get_analytics_metadata(_db.engine, schema, table)
        if am:
            # Whitelisted assignment so unknown keys don't blow up
            # the dataclass when an adapter passes extra fields.
            allowed = {f.name for f in dc_fields(AnalyticsMetadata)}
            for key, value in am.items():
                if key in allowed:
                    setattr(profile.analytics, key, value)
    except Exception as exc:
        # Analytics metadata is purely additive — never let a
        # failure here prevent the user from seeing the basic
        # profile they asked for.
        log.debug(
            "Analytics metadata fetch failed for %s.%s: %s",
            schema,
            table,
            exc,
        )

    return profile


def collect_column_stats_and_samples(
    _db: DatabaseConnector,
    *,
    schema: str,
    table: str,
    fqn: str,
    adapter: Any,
    column_profiles: list[ColumnProfile],
    row_count: int,
    scan_column_stats: bool,
    scan_samples: bool,
    effective_sample_size: int,
) -> None:
    """Populate stats / samples on ``column_profiles`` in place.

    Stats path uses one bulk query (``column_stats_bulk_sql``) per
    chunk of N columns instead of one query per column — on a
    300-column table this collapses 300 queries to 6 (at the
    default batch size of 50). On a warehouse-billed backend
    (Databricks/Snowflake/BigQuery) every query saved is one fewer
    full-table scan. If the bulk query fails (rare — usually a
    single column with a type the bulk cast can't handle),
    per-column-fallback runs only for the unprofiled columns of
    that batch, so a single bad column never masks the rest of
    the table.

    Sample path is still per-column for now — Phase 2 will collapse
    it the same way (one bounded sample of the table, distill per
    column in Python).
    """
    if scan_column_stats:
        batch_size = max(
            1,
            int(getattr(_db.cfg, "profiling_stats_batch_size", 50) or 50),
        )
        collect_bulk_stats(
            _db,
            schema=schema,
            table=table,
            fqn=fqn,
            adapter=adapter,
            column_profiles=column_profiles,
            row_count=row_count,
            batch_size=batch_size,
        )

    if scan_samples and effective_sample_size > 0:
        collect_bulk_samples(
            _db,
            schema=schema,
            table=table,
            fqn=fqn,
            adapter=adapter,
            column_profiles=column_profiles,
            effective_sample_size=effective_sample_size,
        )


def collect_bulk_stats(
    _db: DatabaseConnector,
    *,
    schema: str,
    table: str,
    fqn: str,
    adapter: Any,
    column_profiles: list[ColumnProfile],
    row_count: int,
    batch_size: int,
) -> None:
    """Run bulk stats query in chunks; fall back per-column on failure."""
    for batch_start in range(0, len(column_profiles), batch_size):
        batch = column_profiles[batch_start : batch_start + batch_size]
        quoted_cols = [adapter.quote_identifier(cp.name) for cp in batch]
        try:
            bulk_sql = adapter.column_stats_bulk_sql(fqn, quoted_cols)
            with _db.engine.connect() as conn:
                row = conn.execute(text(bulk_sql)).fetchone()
            if row is None:
                continue
            for j, cp in enumerate(batch):
                base = j * 4
                cp.null_count = row[base] or 0
                cp.distinct_count = row[base + 1] or 0
                cp.min_val = row[base + 2]
                cp.max_val = row[base + 3]
                cp.cardinality_ratio = (
                    float(cp.distinct_count) / float(row_count) if row_count > 0 else 0.0
                )
        except Exception as exc:
            # Bulk failed — most likely one column in this batch has
            # a type the cast can't handle. Retry per-column for
            # this batch only; columns that fail individually get
            # logged and skipped (the original behavior).
            actionable = adapter.actionable_profile_error(exc) or actionable_error_message(
                exc, backend=_db.backend
            )
            log.debug(
                "Bulk column stats failed for %s.%s (batch %d-%d), falling back to per-column: %s",
                schema,
                table,
                batch_start,
                batch_start + len(batch) - 1,
                actionable or exc,
            )
            collect_per_column_stats_fallback(
                _db,
                schema=schema,
                table=table,
                fqn=fqn,
                adapter=adapter,
                batch=batch,
                row_count=row_count,
            )


def collect_per_column_stats_fallback(
    _db: DatabaseConnector,
    *,
    schema: str,
    table: str,
    fqn: str,
    adapter: Any,
    batch: list[ColumnProfile],
    row_count: int,
) -> None:
    """Original one-query-per-column path. Used only on bulk failure."""
    for cp in batch:
        quoted_col = adapter.quote_identifier(cp.name)
        try:
            stats_sql = adapter.column_stats_sql(fqn, quoted_col)
            with _db.engine.connect() as conn:
                col_stats = conn.execute(text(stats_sql)).fetchone()
            if col_stats:
                cp.null_count = col_stats[0] or 0
                cp.distinct_count = col_stats[1] or 0
                cp.min_val = col_stats[2]
                cp.max_val = col_stats[3]
                cp.cardinality_ratio = (
                    float(cp.distinct_count) / float(row_count) if row_count > 0 else 0.0
                )
        except Exception as exc:
            actionable = adapter.actionable_profile_error(exc) or actionable_error_message(
                exc, backend=_db.backend
            )
            log.warning(
                "Skipping profile stats for %s.%s.%s: %s",
                schema,
                table,
                cp.name,
                actionable or exc,
            )


def collect_bulk_samples(
    _db: DatabaseConnector,
    *,
    schema: str,
    table: str,
    fqn: str,
    adapter: Any,
    column_profiles: list[ColumnProfile],
    effective_sample_size: int,
) -> None:
    """One bulk sample query for all columns; escalate per-column only
    for columns that didn't get enough distinct values.

    Wide-table win: a 300-column table at the default sample size
    of 5 distincts/col used to issue 300 separate
    ``SELECT DISTINCT col FROM big_table TABLESAMPLE … LIMIT 5``
    queries. We now issue one ``SELECT col1, col2, …, colN FROM
    big_table TABLESAMPLE … LIMIT row_cap`` and distill per-column
    distincts in Python. row_cap is adaptive — 1000 baseline plus
    50 × column_count so very wide tables get a deeper sample.

    Quality safety net: if a column emerged from the bulk sample
    with fewer than ``min(target, 3)`` distinct values, the
    connector escalates to a per-column query for *that column
    only*. This catches the rare case of a billion-row table
    whose 1000-row TABLESAMPLE happened to land on a near-constant
    slice for some skewed column.
    """
    n_cols = len(column_profiles)
    row_cap = max(1000, 50 * n_cols)
    quoted_cols = [adapter.quote_identifier(cp.name) for cp in column_profiles]

    try:
        bulk_sql = adapter.bulk_sample_sql(fqn, quoted_cols, row_cap)
        with _db.engine.connect() as conn:
            rows = conn.execute(text(bulk_sql)).fetchall()
    except Exception as exc:
        # Bulk failed entirely — fall back to per-column for all
        # columns so the user still gets samples.
        actionable = adapter.actionable_profile_error(exc) or actionable_error_message(
            exc, backend=_db.backend
        )
        log.debug(
            "Bulk sample failed for %s.%s, falling back to per-column: %s",
            schema,
            table,
            actionable or exc,
        )
        collect_per_column_samples(
            _db,
            schema=schema,
            table=table,
            fqn=fqn,
            adapter=adapter,
            column_profiles=column_profiles,
            effective_sample_size=effective_sample_size,
        )
        return

    # Distill per-column distinct values from the wide row set.
    # ``rows`` may be empty (very small / heavily filtered table);
    # in that case every column ends up needing escalation.
    short_columns: list[ColumnProfile] = []
    threshold = min(effective_sample_size, 3)
    for col_idx, cp in enumerate(column_profiles):
        seen: set[Any] = set()
        samples: list[Any] = []
        for row in rows:
            v = row[col_idx]
            if v is None or v in seen:
                continue
            seen.add(v)
            samples.append(v)
            if len(samples) >= effective_sample_size:
                break
        cp.samples = samples
        if len(samples) < threshold:
            short_columns.append(cp)

    if short_columns:
        log.debug(
            "Escalating sample collection for %d/%d columns of %s.%s "
            "(bulk row_cap=%d returned <%d distincts)",
            len(short_columns),
            n_cols,
            schema,
            table,
            row_cap,
            threshold,
        )
        collect_per_column_samples(
            _db,
            schema=schema,
            table=table,
            fqn=fqn,
            adapter=adapter,
            column_profiles=short_columns,
            effective_sample_size=effective_sample_size,
        )


def collect_per_column_samples(
    _db: DatabaseConnector,
    *,
    schema: str,
    table: str,
    fqn: str,
    adapter: Any,
    column_profiles: list[ColumnProfile],
    effective_sample_size: int,
) -> None:
    """Per-column sample fetch. Used as the fallback path when the
    bulk query fails or as escalation for columns that didn't get
    enough distinct values from the bulk sample.
    """
    for cp in column_profiles:
        quoted_col = adapter.quote_identifier(cp.name)
        try:
            sample_sql = adapter.column_sample_sql(fqn, quoted_col)
            with _db.engine.connect() as conn:
                samples_row = conn.execute(
                    text(sample_sql), {"lim": effective_sample_size}
                ).fetchall()
            cp.samples = [r[0] for r in samples_row]
        except Exception as exc:
            actionable = adapter.actionable_profile_error(exc) or actionable_error_message(
                exc, backend=_db.backend
            )
            log.warning(
                "Skipping sample collection for %s.%s.%s: %s",
                schema,
                table,
                cp.name,
                actionable or exc,
            )
