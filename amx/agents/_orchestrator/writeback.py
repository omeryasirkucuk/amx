"""Writeback layer for orchestrator review results.

Extracted from ``amx.agents.orchestrator`` so the live-DB application
of accepted review results, the audit log, the placeholder detector,
the old-comment cache reader, the savepoint guard, the asset-label
helper, and the live progress factory all live in one focused module.
The public surface (``apply_review_results_to_db``,
``is_placeholder_description``, ``create_live_writeback_progress``)
is re-exported from ``orchestrator.py`` so legacy callers stay
byte-compatible.
"""

from __future__ import annotations

import contextlib
import threading
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from amx.db.connector import AssetKind, DatabaseConnector
from amx.utils.console import error
from amx.utils.logging import get_logger

if TYPE_CHECKING:
    from amx.agents.orchestrator import ReviewResult

log = get_logger("agents.orchestrator.writeback")


# Sentinel descriptions the orchestrator writes when the LLM produced no
# real description for a column or table. These must NEVER be written to
# the live database — they're a UI hint for human review, not metadata.
# Detection is substring-based (not exact match) so they survive minor
# edits the user might have made before clicking Skip.
_PLACEHOLDER_MARKERS: tuple[str, ...] = (
    "Auto-inference missed a reliable description",
    "Auto-inference missed a reliable table description",
)


def _writeback_asset_label(result: ReviewResult, *, include_column: bool = True) -> str:
    schema = getattr(result, "schema", "") or ""
    table = getattr(result, "table", "") or ""
    column = getattr(result, "column", "") or ""
    if include_column and column:
        return ".".join(part for part in (schema, table, column) if part)
    if table:
        return ".".join(part for part in (schema, table) if part)
    return schema


def create_live_writeback_progress(
    *,
    total: int,
    backend: str,
    provider: str = "",
    model: str = "",
) -> tuple[Callable[[ReviewResult, str, int, int, str], None], Callable[[], None]]:
    from amx.utils.live_display import get_display

    display = get_display()
    started_display = False
    if total and not display.is_active:
        display.start(
            schema="",
            table=f"{total} comments",
            mode="apply",
            provider=provider,
            model=model,
        )
        started_display = True

    activity_idx = display.add_activity(f"Writeback 0/{total}")
    applied_count = 0
    failed_count = 0
    started = False

    def _on_progress(
        result: ReviewResult, status: str, index: int, total_count: int, detail: str
    ) -> None:
        nonlocal applied_count, failed_count, started
        if status == "started":
            label = f"Writeback {index}/{total_count}: {_writeback_asset_label(result)}"
            display.update_activity(activity_idx, label=label)
            if not started:
                display.begin_activity(activity_idx)
                started = True
            return
        if status == "applied":
            applied_count += 1
            label = f"Writeback {applied_count}/{total_count} applied"
            if failed_count:
                label += f", {failed_count} failed"
            display.update_activity(activity_idx, label=label)
            return
        if status == "failed":
            failed_count += 1
            label = f"Writeback {applied_count + failed_count}/{total_count} processed"
            if failed_count:
                label += f", {failed_count} failed"
            display.update_activity(activity_idx, label=label)
            display.add_detail(
                activity_idx,
                f"Failed {_writeback_asset_label(result)}: {detail[:220]}",
            )

    def _finish() -> None:
        summary = f"Applied {applied_count}/{total} comment(s)"
        if failed_count:
            summary += f"; failed {failed_count}"
            display.fail_activity(activity_idx, summary)
        else:
            display.complete_activity(activity_idx, summary)
        if started_display:
            display.stop()

    return _on_progress, _finish


def is_placeholder_description(text: str | None) -> bool:
    """Return True when ``text`` is the auto-inference fallback placeholder.

    Used as a guard before COMMENT ON SQL: writing the placeholder
    pollutes the live DB metadata. ``apply_review_results_to_db`` skips
    placeholder rows; ``/run`` with missing-only treats placeholder
    comments as "still missing" so the user can re-analyse them.
    """
    if not text:
        return False
    sample = str(text).strip()
    return any(marker in sample for marker in _PLACEHOLDER_MARKERS)


def _record_audit(
    audit_log: Any,
    r: ReviewResult,
    *,
    audit_profile: str,
    audit_user: str,
    audit_host: str,
    audit_run_id: int | None,
    old_comment: str | None = None,
) -> None:
    """Write one ``apply_events`` row for a successful COMMENT.

    No-op when ``audit_log`` is ``None`` so the legacy code path stays
    untouched. Failures are swallowed at debug level — the audit log
    is best-effort and must never abort an otherwise-successful apply.

    ``old_comment`` is the verbatim comment text that was on the
    asset *before* this apply overwrote it. ``None`` means we
    couldn't read it (adapter doesn't expose a read API, or the
    pre-write read raised); ``/history rollback`` treats ``None``
    as "original state unknown — skip this row".
    """
    if audit_log is None:
        return
    try:
        audit_log.record_apply_event(
            run_id=audit_run_id,
            result_id=getattr(r, "result_id", None),
            profile_name=audit_profile,
            schema_name=r.schema,
            table_name=r.table or "",
            column_name=r.column,
            asset_kind=r.asset_kind or "table",
            old_comment=old_comment,
            new_comment=r.final_description or "",
            applied_by=audit_user,
            hostname=audit_host,
            sql_template="",
        )
    except Exception as exc:
        log.debug("audit_log.record_apply_event failed for %s.%s: %s", r.schema, r.table, exc)


class _OldCommentReader:
    """Read prior COMMENT values from the live DB before they are
    overwritten, with a per-(schema, table) column-comment cache so
    a 200-row apply against one table costs one
    ``get_column_comments`` call rather than 200.

    Misses (adapter without a read API, query failure, asset kind
    AMX cannot read) return ``None`` — the audit row records that
    as "original unknown" and ``/history rollback`` skips that
    row instead of synthesising garbage.
    """

    def __init__(self, db: DatabaseConnector) -> None:
        self.db = db
        self._column_cache: dict[tuple[str, str], dict[str, str | None]] = {}

    def read(self, r: ReviewResult, kind: AssetKind) -> str | None:
        try:
            if kind == AssetKind.SCHEMA:
                return self.db.get_schema_comment(r.schema)
            if kind == AssetKind.DATABASE:
                return self.db.get_database_comment()
            if r.column is None:
                return self.db.get_table_comment(r.schema, r.table)
            key = (r.schema, r.table)
            cached = self._column_cache.get(key)
            if cached is None:
                try:
                    cached = self.db.get_column_comments(r.schema, r.table) or {}
                except Exception as exc:
                    log.debug(
                        "get_column_comments failed for %s.%s (%s); "
                        "audit will record old_comment=None",
                        r.schema,
                        r.table,
                        exc,
                    )
                    cached = {}
                self._column_cache[key] = cached
            return cached.get(r.column)
        except Exception as exc:
            log.debug(
                "old-comment read failed for %s.%s.%s (%s); audit will record old_comment=None",
                r.schema,
                r.table,
                r.column or "",
                exc,
            )
            return None


@contextlib.contextmanager
def _savepoint_or_passthrough(conn: Any):
    """Per-row SAVEPOINT context.

    PostgreSQL aborts the entire transaction when one statement
    fails; without a savepoint, every subsequent COMMENT in the same
    apply batch fails with ``InFailedSqlTransaction``. SQLAlchemy's
    ``conn.begin_nested()`` issues ``SAVEPOINT`` and rolls just that
    savepoint back on exception, leaving the outer tx alive.

    Some adapters / test fakes don't implement ``begin_nested``; fall
    back to a passthrough so they keep working as they did before
    (Postgres is the only common backend with the cascade behaviour).
    """
    nested = getattr(conn, "begin_nested", None)
    if not callable(nested):
        yield None
        return
    try:
        cm = nested()
    except Exception:
        # Some fakes raise on construction — be conservative and skip
        # the savepoint, falling back to the legacy single-tx path.
        yield None
        return
    enter = getattr(cm, "__enter__", None)
    exit_ = getattr(cm, "__exit__", None)
    if not callable(enter) or not callable(exit_):
        yield None
        return
    enter()
    try:
        yield cm
    except BaseException as exc:
        if not exit_(type(exc), exc, exc.__traceback__):
            raise
    else:
        exit_(None, None, None)


def apply_review_results_to_db(
    db: DatabaseConnector,
    results: list[ReviewResult],
    *,
    on_applied: Callable[[ReviewResult], None] | None = None,
    on_failed: Callable[[ReviewResult, Exception], None] | None = None,
    on_progress: Callable[[ReviewResult, str, int, int, str], None] | None = None,
    cancel_token: threading.Event | None = None,
    dry_run: bool = False,
    audit_log: Any = None,
    audit_profile: str = "",
    audit_user: str = "",
    audit_host: str = "",
    audit_run_id: int | None = None,
) -> int:
    """Write approved descriptions as COMMENT ON TABLE/VIEW/COLUMN to the database.

    ``cancel_token`` is checked between rows so AMX Studio's
    "Cancel job" button stops the loop within one row latency. The
    transaction commits whatever was applied so far — matching the
    CLI's Ctrl-C behaviour, and avoiding a multi-minute rollback that
    would discard the user's already-confirmed work.

    ``dry_run=True`` short-circuits before any DB connection is opened.
    Each row that *would* be applied is reported through ``on_progress``
    with ``status="preview"`` and a ``detail`` string carrying the SQL
    template the adapter would execute (with the comment text bound
    via the ``:cmt`` parameter, never inlined). The function still
    returns ``0`` because nothing was actually applied — callers can
    distinguish "preview" from "applied" by inspecting the progress
    callbacks, not by trusting the return value.
    """
    applied = 0
    # Filter out fallback placeholders BEFORE we hit the DB. They're a UI
    # hint for human review (so the user can see which columns the LLM
    # missed); writing them would replace real metadata with text like
    # "Auto-inference missed a reliable description; please review
    # manually." and the user would have no idea their schema got polluted.
    pending = [
        r
        for r in results
        if r.applied and r.final_description and not is_placeholder_description(r.final_description)
    ]
    if not pending:
        return 0

    if dry_run:
        # Pure preview path — no transaction, no DB writes. We only
        # render the SQL template each row would execute and forward
        # it to the progress callback so the caller can show users
        # exactly what /apply would do.
        total_preview = len(pending)
        for idx, r in enumerate(pending, start=1):
            if cancel_token is not None and cancel_token.is_set():
                break
            try:
                kind = AssetKind(r.asset_kind) if r.asset_kind else AssetKind.TABLE
            except ValueError:
                kind = AssetKind.TABLE
            try:
                sql = db.preview_comment_sql(
                    schema=r.schema,
                    table=r.table,
                    column=r.column,
                    asset_kind=kind,
                )
            except Exception as exc:
                # Adapter glue should never raise from preview_comment_sql,
                # but if it does we tag the row as a preview-failure and
                # keep going — partial preview is still useful to the user.
                if on_progress is not None:
                    on_progress(r, "preview_failed", idx, total_preview, str(exc))
                continue
            detail = sql or "(unsupported by backend — would be skipped)"
            if on_progress is not None:
                on_progress(r, "preview", idx, total_preview, detail)
        # Caller receives 0 — nothing was written. Use the progress
        # callback to count "would-apply" rows when needed.
        return 0

    # Build the old-comment reader once per apply call. Cache scope is
    # the call so concurrent apply runs (multi-user shared store) get
    # independent caches; nothing escapes this function. The reader is
    # only consulted when ``audit_log`` is set — saves a round-trip on
    # every legacy caller that doesn't audit.
    old_reader = _OldCommentReader(db) if audit_log is not None else None

    with db.engine.begin() as conn:
        total = len(pending)
        index = 0
        while index < total:
            if cancel_token is not None and cancel_token.is_set():
                # User clicked "Cancel" in AMX Studio (or the CLI
                # job orchestrator set the token). Commit whatever was
                # already written and let the caller surface the
                # cancellation as a job.cancelled event.
                break
            r = pending[index]
            try:
                kind = AssetKind(r.asset_kind) if r.asset_kind else AssetKind.TABLE
            except ValueError:
                kind = AssetKind.TABLE
            if r.column is not None and kind == AssetKind.TABLE and index + 1 < total:
                group = [r]
                next_index = index + 1
                while next_index < total:
                    candidate = pending[next_index]
                    try:
                        candidate_kind = (
                            AssetKind(candidate.asset_kind)
                            if candidate.asset_kind
                            else AssetKind.TABLE
                        )
                    except ValueError:
                        candidate_kind = AssetKind.TABLE
                    if (
                        candidate.column is None
                        or candidate_kind != AssetKind.TABLE
                        or candidate.schema != r.schema
                        or candidate.table != r.table
                    ):
                        break
                    group.append(candidate)
                    next_index += 1
                if len(group) > 1:
                    batched_comments = [
                        (item.column or "", item.final_description) for item in group
                    ]
                    # Pre-fetch the prior comment for every item in the
                    # group BEFORE the batch overwrite — once committed,
                    # the original text is gone and rollback can't
                    # recover it. The reader caches at (schema, table)
                    # level, so this is one ``get_column_comments``
                    # call per table regardless of group size.
                    pre_old: list[str | None] = []
                    if old_reader is not None:
                        pre_old = [old_reader.read(item, kind) for item in group]
                    # Wrap the batch in a SAVEPOINT so a failed batch
                    # (or a failed individual statement inside the
                    # batch on PostgreSQL, which would otherwise abort
                    # the outer tx and cascade-fail every subsequent
                    # row with ``InFailedSqlTransaction``) only rolls
                    # back its own savepoint and leaves the per-row
                    # fallback path usable.
                    try:
                        if on_progress is not None:
                            on_progress(
                                group[0], "started", index + 1, total, f"batch:{len(group)}"
                            )
                        with _savepoint_or_passthrough(conn):
                            applied_batch = db.apply_column_comments_batch(
                                r.schema, r.table, batched_comments, conn=conn
                            )
                        if applied_batch:
                            for offset, item in enumerate(group, start=1):
                                applied += 1
                                if on_progress is not None:
                                    on_progress(
                                        item,
                                        "applied",
                                        index + offset,
                                        total,
                                        f"batch:{len(group)}",
                                    )
                                if on_applied is not None:
                                    on_applied(item)
                                _record_audit(
                                    audit_log,
                                    item,
                                    audit_profile=audit_profile,
                                    audit_user=audit_user,
                                    audit_host=audit_host,
                                    audit_run_id=audit_run_id,
                                    old_comment=(pre_old[offset - 1] if pre_old else None),
                                )
                                # Mirror the per-row branch's cache GC
                                # so a successful batch apply also drops
                                # the cached first-run profile for the
                                # touched table. Best-effort.
                                if audit_log is not None:
                                    try:
                                        audit_log.delete_run_context_cache(
                                            db_profile=audit_profile or "",
                                            database=db.cfg.database
                                            or db.cfg.project
                                            or db.cfg.catalog
                                            or "",
                                            schema=item.schema,
                                            table=item.table or "",
                                        )
                                    except Exception as cache_exc:
                                        log.debug(
                                            "delete_run_context_cache (batch) failed for %s.%s: %s",
                                            item.schema,
                                            item.table,
                                            cache_exc,
                                        )
                                # Also invalidate the column-comments
                                # cache for this table so the very next
                                # sidebar / CLI inspect sees the freshly
                                # written COMMENT and never the prior
                                # placeholder.
                                try:
                                    db.invalidate_column_comments_cache(
                                        schema=item.schema, table=item.table or ""
                                    )
                                except Exception as cache_exc:
                                    log.debug(
                                        "invalidate_column_comments_cache (batch) failed for %s.%s: %s",
                                        item.schema,
                                        item.table,
                                        cache_exc,
                                    )
                            index = next_index
                            continue
                    except Exception as batch_exc:
                        log.debug(
                            "Falling back to per-column writeback for %s.%s after batch failure: %s",
                            r.schema,
                            r.table,
                            batch_exc,
                        )
            # Pre-fetch the prior comment for this row before the
            # overwrite. ``read`` swallows its own errors and returns
            # None on failure, so a misbehaving adapter doesn't break
            # the apply itself — the audit row just lands with
            # old_comment=None and rollback skips it.
            pre_old_value = old_reader.read(r, kind) if old_reader is not None else None
            try:
                if on_progress is not None:
                    on_progress(r, "started", index + 1, total, "")
                # Per-row SAVEPOINT: keeps a single failed COMMENT (eg.
                # "schema does not exist" on Postgres) from poisoning
                # the rest of the batch with InFailedSqlTransaction.
                # The savepoint auto-rolls back on exception when used
                # as a context manager, so the outer tx stays alive
                # for the next row.
                with _savepoint_or_passthrough(conn):
                    db.apply_comment(
                        schema=r.schema,
                        table=r.table,
                        column=r.column,
                        comment=r.final_description,
                        asset_kind=kind,
                        conn=conn,
                    )
                applied += 1
                if on_progress is not None:
                    on_progress(r, "applied", index + 1, total, "")
                if on_applied is not None:
                    on_applied(r)
                _record_audit(
                    audit_log,
                    r,
                    audit_profile=audit_profile,
                    audit_user=audit_user,
                    audit_host=audit_host,
                    audit_run_id=audit_run_id,
                    old_comment=pre_old_value,
                )
                # Drop the cached first-run profile for this table so
                # we don't keep stale-but-valid context around for a
                # row the user has already accepted. The cache write
                # path is best-effort, so the GC must be too — a
                # failed delete must never break the apply loop.
                if audit_log is not None:
                    try:
                        audit_log.delete_run_context_cache(
                            db_profile=audit_profile or "",
                            database=db.cfg.database or db.cfg.project or db.cfg.catalog or "",
                            schema=r.schema,
                            table=r.table or "",
                        )
                    except Exception as cache_exc:
                        log.debug(
                            "delete_run_context_cache failed for %s.%s: %s",
                            r.schema,
                            r.table,
                            cache_exc,
                        )
                # Same belt-and-braces invalidation for the column-
                # comments cache as the batch branch above. Keeps
                # post-apply reads guaranteed-fresh.
                try:
                    db.invalidate_column_comments_cache(schema=r.schema, table=r.table or "")
                except Exception as cache_exc:
                    log.debug(
                        "invalidate_column_comments_cache failed for %s.%s: %s",
                        r.schema,
                        r.table,
                        cache_exc,
                    )
            except Exception as exc:
                if on_progress is not None:
                    on_progress(r, "failed", index + 1, total, str(exc))
                if on_failed is not None:
                    on_failed(r, exc)
                # Surface the proximate cause (eg. schema/table not
                # found) instead of the cascade noise. Postgres
                # ``InFailedSqlTransaction`` errors used to dominate
                # the log when one row failed inside a single tx;
                # SAVEPOINTs above kill that cascade, but we still
                # render the original error message cleanly.
                error(
                    f"Failed to apply comment on {r.schema}.{r.table or ''}.{r.column or ''} ({r.asset_kind}): {exc}"
                )
            index += 1
    return applied
