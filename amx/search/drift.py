"""Background catalog-drift detection.

When a user adds, drops, or renames a table in the live DB, AMX's
persistent catalog (``catalog_entities`` / ``catalog_descriptions``)
stays at the last ``/search sync`` snapshot. Concept search reads the
catalog, so the new table is invisible to ``/ask`` until the user
manually re-syncs — a common source of "AMX says it doesn't exist
but it's right there" confusion.

This module exposes :func:`fire_drift_probe`, called by the Studio
``/api/ask`` handshake and the REPL bootstrap. It spawns a daemon
thread that:

1. For every DB profile in scope, counts ``catalog_entities`` rows.
2. Asks each profile's live connector for the current table count
   (cheap ``SELECT COUNT(*) FROM information_schema.tables`` or the
   backend-specific equivalent).
3. If the counts differ by more than ``epsilon`` (default 0), fires
   :func:`amx.search._catalog.sync.sync_table_profile_async` for that
   profile so the catalog catches up before the next question.

The probe is fire-and-forget. The current ``/ask`` call uses whatever
catalog state is there; the NEXT call reflects the new schema. Honors
``AMX_SKIP_DRIFT_PROBE=1`` so power users can opt out.
"""

from __future__ import annotations

import os
import threading
import time
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from amx.utils.logging import get_logger

log = get_logger("search.drift")

#: Seconds between probes for the same (process, profile). Avoids
#: hammering the live DB when the user fires several /ask calls in
#: quick succession. Memo lives in-process only.
_PROBE_COOLDOWN_SEC = 60.0

#: Last probe timestamp per profile.
_LAST_PROBE: dict[str, float] = {}
_LAST_PROBE_LOCK = threading.Lock()


@dataclass
class DriftResult:
    """Per-profile probe outcome — surfaced for tests."""

    profile: str
    catalog_count: int
    live_count: int | None
    drifted: bool
    error: str | None = None


def _build_connector(cfg: Any, profile: str) -> Any:
    """Open a ``DatabaseConnector`` for *profile* using whatever AMX
    config we have. PR #414 referenced a non-existent
    ``amx.db.factory.build_connector`` — every drift sync silently
    failed at import. The real path is ``DatabaseConnector`` directly,
    mirroring ``amx/web/routers/live_db.py:_connector_for_scope``.
    """
    if cfg is None:
        raise RuntimeError("cfg required to open connector")
    profile_name = (profile or "").strip()
    profile_map = getattr(cfg, "db_profiles", {}) or {}
    base = profile_map.get(profile_name)
    if base is None:
        raise RuntimeError(f"No DB profile named {profile_name!r}")
    from amx.db.connector import DatabaseConnector

    return DatabaseConnector(base, profile_name=profile_name)


def _cooldown_blocks(profile: str, now: float) -> bool:
    with _LAST_PROBE_LOCK:
        last = _LAST_PROBE.get(profile)
        if last is not None and (now - last) < _PROBE_COOLDOWN_SEC:
            return True
        _LAST_PROBE[profile] = now
        return False


def _catalog_table_count(catalog_db_path: str, profile: str) -> int:
    """Number of ``entity_kind='table'`` rows the persistent catalog
    holds for *profile*. Zero means the profile was never synced."""
    import sqlite3

    try:
        conn = sqlite3.connect(catalog_db_path, timeout=5)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT COUNT(*) AS n FROM catalog_entities
            WHERE db_profile = ? AND entity_kind = 'table'
            """,
            (profile,),
        ).fetchone()
        return int(row["n"] or 0) if row else 0
    except Exception as exc:  # pragma: no cover - best-effort
        log.debug("catalog_table_count(%s) failed: %s", profile, exc)
        return 0
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _live_table_count(cfg, profile: str) -> int | None:
    """Live-DB count via the active connector. Returns ``None`` when
    the connector refuses to answer (auth blip, network down) so the
    caller treats it as "no drift signal" rather than mis-firing a
    sync against an unreachable database."""
    try:
        connector = _build_connector(cfg, profile)
    except Exception as exc:
        log.debug("build_connector(%s) failed: %s", profile, exc)
        return None
    try:
        schemas = connector.list_schemas() or []
    except Exception as exc:
        log.debug("list_schemas(%s) failed: %s", profile, exc)
        return None
    total = 0
    for schema in schemas:
        try:
            assets = connector.list_assets(schema) or []
            total += len(assets)
        except Exception as exc:
            log.debug("list_assets(%s/%s) failed: %s", profile, schema, exc)
            # Partial signal is worse than no signal — the missing
            # schema looks like a drop.
            return None
    return total


def _probe_one(cfg, profile: str, catalog_db_path: str) -> DriftResult:
    catalog_n = _catalog_table_count(catalog_db_path, profile)
    live_n = _live_table_count(cfg, profile)
    if live_n is None:
        return DriftResult(profile, catalog_n, None, False, "live count unavailable")
    drifted = live_n != catalog_n
    return DriftResult(profile, catalog_n, live_n, drifted)


def _enqueue_sync(cfg, profile: str) -> None:
    """Run a **skeleton** sync for *profile*: enumerate schemas + table
    names + asset kinds, INSERT-IGNORE the rows into
    ``catalog_entities`` so the sidebar / schedule / run pickers /
    Ask tools can stand on the catalog. The deep per-table profile
    (row counts, column stats, descriptions) is left to ``/run`` and
    ``/search sync`` — they're heavy enough that running them inside
    the drift probe was the source of the silent-partial-catalog bug.

    Always finishes with ``catalog_profile_state`` flipped to
    ``done`` or ``failed`` so the cache-first read gate and the
    freshness pill have a trustworthy signal.
    """
    try:
        from amx.search.catalog import SearchCatalog
    except Exception as exc:
        log.debug("Could not import SearchCatalog for drift sync: %s", exc)
        return
    try:
        catalog = SearchCatalog.from_history_store()
    except Exception as exc:
        log.debug("SearchCatalog.from_history_store failed: %s", exc)
        return
    if catalog is None:
        return
    try:
        sync_profile_skeleton(cfg, profile, catalog)
    except Exception as exc:
        log.warning("Drift skeleton sync failed for %s: %s", profile, exc)


def sync_profile_skeleton(cfg, profile: str, catalog) -> dict:
    """Synchronous skeleton sync. Returns a summary dict so the caller
    (drift probe, ``POST /api/catalog/sync``, or a test) can inspect
    the outcome. Never raises — every error path lands on
    ``finish_skeleton_sync(ok=False, error=...)`` so the state
    machine always terminates.
    """
    summary: dict[str, Any] = {
        "profile": profile,
        "state": "none",
        "total": 0,
        "processed": 0,
        "error": "",
    }
    db_backend = str(getattr(cfg.db, "backend", "") or "") if cfg is not None else ""
    database_name = ""
    if cfg is not None:
        database_name = str(
            getattr(cfg.db, "database", "")
            or getattr(cfg.db, "catalog", "")
            or getattr(cfg.db, "project", "")
            or ""
        )

    # Step 1: open the connector + enumerate schemas. Failure here
    # means we can't even count tables; mark the profile failed and
    # bail before touching catalog_profile_state's progress fields.
    try:
        connector = _build_connector(cfg, profile)
        schemas = connector.list_schemas() or []
    except Exception as exc:
        log.warning("Skeleton sync connect failed for %s: %s", profile, exc)
        catalog.start_skeleton_sync(profile, total_tables=0)
        catalog.finish_skeleton_sync(profile, ok=False, error=str(exc))
        summary["state"] = "failed"
        summary["error"] = str(exc)
        return summary

    # ``list_schemas`` succeeded but returned zero rows. Treat this as a
    # failure (not a silent success) so the freshness pill renders a
    # Retry CTA + actionable error instead of "never · stale" with no
    # explanation. The most common cause on Databricks is a profile
    # without a catalog pinned; on 2-level backends it's a permission
    # gap. Tailor the message so the user knows exactly what to check.
    if not schemas:
        if db_backend == "databricks":
            error_msg = (
                "Connected but no schemas were visible. Pin a catalog on the "
                "profile (Settings → DB profile) or check that your warehouse "
                "permissions expose at least one schema."
            )
        else:
            error_msg = (
                "Connected but no schemas were visible. Check that the active "
                "database is correct and the connection user has at least "
                "``USAGE`` on one schema."
            )
        log.warning(
            "Skeleton sync for %s returned empty schemas - marking failed",
            profile,
        )
        catalog.start_skeleton_sync(profile, total_tables=0)
        catalog.finish_skeleton_sync(profile, ok=False, error=error_msg)
        summary["state"] = "failed"
        summary["error"] = error_msg
        return summary

    # Step 2: pass 1 — count tables across all schemas. Lets the UI
    # show ``Syncing 0 / N…`` from the first tick rather than
    # ``Syncing… N unknown``.
    schema_assets: list[tuple[str, list]] = []
    total = 0
    for schema in schemas:
        try:
            assets = connector.list_assets(schema) or []
        except Exception as exc:
            log.warning("Skeleton sync list_assets failed for %s/%s: %s", profile, schema, exc)
            assets = []
        schema_assets.append((schema, assets))
        total += len(assets)

    catalog.start_skeleton_sync(profile, total_tables=total)
    summary["state"] = "syncing"
    summary["total"] = total

    # Step 3: pass 2 — INSERT each table row. We use the
    # entity-crud's ``_upsert_entity`` so the unique index keeps a
    # re-run idempotent, and we skip ``profile_table`` entirely —
    # that's the heavy per-table SQL that made the old drift loop
    # take minutes.
    processed = 0
    try:
        with catalog._connect() as conn:  # noqa: SLF001 — owns the catalog conn
            for schema, assets in schema_assets:
                for asset in assets:
                    name, kind = _asset_name_and_kind(asset)
                    if not name:
                        continue
                    try:
                        catalog._upsert_entity(  # noqa: SLF001
                            conn,
                            db_profile=profile,
                            db_backend=db_backend,
                            database_name=database_name,
                            schema_name=schema,
                            table_name=name,
                            column_name=None,
                            entity_kind="table",
                            asset_kind=str(kind or "table"),
                        )
                        processed += 1
                    except Exception as exc:
                        # A single bad row mustn't kill the whole
                        # skeleton — log and continue so the rest of
                        # the catalog still benefits.
                        log.debug(
                            "Skeleton upsert failed for %s/%s.%s: %s",
                            profile,
                            schema,
                            name,
                            exc,
                        )
                # Per-schema progress write keeps SQLite WAL writes
                # bounded; updating once per row would thrash on a
                # 10k-table catalog. Sharing the open connection
                # keeps the progress write inside the same
                # transaction as the upserts so SQLite doesn't deadlock
                # on a second writer.
                catalog.record_skeleton_progress(profile, processed, conn=conn)
    except Exception as exc:
        log.warning("Skeleton sync upsert pass failed for %s: %s", profile, exc)
        catalog.finish_skeleton_sync(profile, ok=False, error=str(exc))
        summary["state"] = "failed"
        summary["processed"] = processed
        summary["error"] = str(exc)
        return summary

    catalog.finish_skeleton_sync(profile, ok=True)
    summary["state"] = "done"
    summary["processed"] = processed
    return summary


def _asset_name_and_kind(asset: Any) -> tuple[str, str]:
    """Connectors return ``list_assets`` rows in two shapes — a tuple
    ``(name, kind)`` or a single string ``name``. Normalize so the
    skeleton sync handles both."""
    if isinstance(asset, tuple) and len(asset) >= 2:
        name = str(asset[0] or "")
        kind = getattr(asset[1], "value", None) or asset[1]
        return name, str(kind or "table")
    if isinstance(asset, str):
        return asset, "table"
    return str(getattr(asset, "name", "") or ""), str(getattr(asset, "kind", "") or "table")


def fire_drift_probe(cfg, profiles: Iterable[str], *, force: bool = False) -> None:
    """Spawn a daemon thread that runs the drift probe for each
    profile in scope. No-op when ``AMX_SKIP_DRIFT_PROBE=1`` is set or
    the catalog file isn't available yet (fresh install with no
    history store).

    ``force=True`` is used by the manual ``/api/catalog/sync`` route:
    the per-profile cooldown is bypassed AND the sync runs even when
    the live / catalog counts match. Without force, an auto-probe
    that already ran in the last 60s would silently absorb a manual
    click and the freshness pill would never reflect the user's
    intent.
    """
    if os.environ.get("AMX_SKIP_DRIFT_PROBE", "").strip() in ("1", "true", "yes"):
        return
    profile_list = [p for p in (profiles or []) if p]
    if not profile_list:
        return
    try:
        from amx.storage.sqlite_store import history_store

        hs = history_store()
    except Exception:
        return
    if hs is None:
        return
    catalog_db_path = str(getattr(hs, "db_path", "") or "")
    if not catalog_db_path:
        return
    now = time.time()
    if force:
        # Manual click — skip the cooldown gate entirely and refresh
        # the timestamp so the next auto-probe still respects pacing
        # relative to this manual trigger.
        with _LAST_PROBE_LOCK:
            for profile in profile_list:
                _LAST_PROBE[profile] = now
        fresh = list(profile_list)
    else:
        fresh = [p for p in profile_list if not _cooldown_blocks(p, now)]
    if not fresh:
        return

    def _worker() -> None:
        for profile in fresh:
            result = _probe_one(cfg, profile, catalog_db_path)
            if force or result.drifted:
                _enqueue_sync(cfg, profile)

    thread = threading.Thread(
        target=_worker,
        name="amx-drift-probe",
        daemon=True,
    )
    thread.start()


__all__ = ["fire_drift_probe", "DriftResult"]
