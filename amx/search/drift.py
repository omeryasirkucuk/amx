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
        from amx.db.factory import build_connector

        connector = build_connector(cfg, profile_name=profile)
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
    """Hand off the sync to whichever path already exists. Imported
    lazily because the catalog module pulls SQLAlchemy + Chroma; the
    drift probe must stay cheap when no drift is detected."""
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
        from amx.db.factory import build_connector

        connector = build_connector(cfg, profile_name=profile)
        schemas = connector.list_schemas() or []
    except Exception as exc:
        log.debug("Could not build connector for drift sync (%s): %s", profile, exc)
        return
    log.info("Drift detected on %s; running async catalog sync", profile)
    for schema in schemas:
        try:
            assets = connector.list_assets(schema) or []
        except Exception:
            continue
        for asset in assets:
            try:
                profile_obj = connector.profile_table(schema, asset)
                catalog.sync_table_profile(
                    db_profile=profile,
                    db_backend=str(getattr(cfg.db, "backend", "") or ""),
                    database_name=str(
                        getattr(cfg.db, "database", "")
                        or getattr(cfg.db, "catalog", "")
                        or getattr(cfg.db, "project", "")
                        or ""
                    ),
                    profile=profile_obj,
                )
            except Exception as exc:
                log.debug(
                    "Drift sync failed for %s/%s.%s: %s", profile, schema, asset, exc
                )


def fire_drift_probe(cfg, profiles: Iterable[str]) -> None:
    """Spawn a daemon thread that runs the drift probe for each
    profile in scope. No-op when ``AMX_SKIP_DRIFT_PROBE=1`` is set or
    the catalog file isn't available yet (fresh install with no
    history store)."""
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
    fresh = [p for p in profile_list if not _cooldown_blocks(p, now)]
    if not fresh:
        return

    def _worker() -> None:
        for profile in fresh:
            result = _probe_one(cfg, profile, catalog_db_path)
            if result.drifted:
                _enqueue_sync(cfg, profile)

    thread = threading.Thread(
        target=_worker,
        name="amx-drift-probe",
        daemon=True,
    )
    thread.start()


__all__ = ["fire_drift_probe", "DriftResult"]
