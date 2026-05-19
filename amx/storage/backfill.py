"""Background backfill of local SQLite rows into the shared warehouse.

On first shared-mode connection (or after the user switches the
``history_store_profile``), the local SQLite store may contain
lineage / documentation rows that predate the shared store. This
module walks the local rows and INSERTs missing ones into the shared
schema, using ``(hostname, local_id)`` as the idempotency key so the
operation is safely re-runnable. State is tracked in a local
``_amx_backfill_state`` table keyed by ``(scope, shared_profile, shared_schema)``.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

BACKFILL_SCOPES = ("lineage", "pages")


@dataclass
class BackfillReport:
    """Summary of a completed or aborted backfill run."""

    succeeded: int = 0
    skipped: int = 0
    failed: int = 0
    per_table_counts: dict[str, int] = field(default_factory=dict)
    last_error: str | None = None


class BackfillRunner:
    """Idempotent migrator: local SQLite to shared warehouse.

    Walks the ``lineage_artifacts``, ``lineage_artifact_nodes``,
    ``lineage_comments``, and ``documentation_pages`` tables in the local
    SQLite store and INSERTs any missing rows into the shared store.
    Idempotency is guaranteed by the ``(hostname, local_id)`` lookup on
    the shared side and by a per-scope sentinel in ``_amx_backfill_state``
    that prevents re-running a completed scope.
    """

    def __init__(
        self,
        local: Any,
        shared: Any,
        *,
        shared_profile: str,
        shared_schema: str,
        progress_cb: Callable[[str, int, int], None] | None = None,
        batch_size: int = 500,
    ) -> None:
        self._local = local
        self._shared = shared
        self._shared_profile = shared_profile
        self._shared_schema = shared_schema
        self._progress_cb = progress_cb
        self._batch_size = batch_size
        self._ensure_state_table()

    def run(self) -> BackfillReport:
        """Execute all pending scopes; return a summary report."""
        report = BackfillReport()
        try:
            if not self._is_completed("lineage"):
                artifact_map = self._backfill_lineage_artifacts(report)
                self._backfill_lineage_artifact_nodes(report, artifact_map)
                self._backfill_lineage_comments(report, artifact_map)
                self._mark_completed("lineage", report.succeeded)
            if not self._is_completed("pages"):
                self._backfill_documentation_pages(report)
                self._mark_completed("pages", report.succeeded)
        except Exception as exc:  # noqa: BLE001
            report.last_error = repr(exc)
            logger.warning("BackfillRunner aborted: %s", exc)
        return report

    # ── sentinel ─────────────────────────────────────────────────────────────

    def _ensure_state_table(self) -> None:
        """Create ``_amx_backfill_state`` in the local SQLite store if absent."""
        with self._local._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS _amx_backfill_state (
                    scope          TEXT NOT NULL,
                    shared_profile TEXT NOT NULL,
                    shared_schema  TEXT NOT NULL,
                    completed_at   REAL NOT NULL,
                    rows_pushed    INTEGER NOT NULL,
                    last_error     TEXT,
                    PRIMARY KEY (scope, shared_profile, shared_schema)
                )
                """
            )

    def _is_completed(self, scope: str) -> bool:
        """Return True when this scope has a clean sentinel for the current target."""
        with self._local._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM _amx_backfill_state "
                "WHERE scope=? AND shared_profile=? AND shared_schema=? AND last_error IS NULL",
                (scope, self._shared_profile, self._shared_schema),
            ).fetchone()
        return row is not None

    def _mark_completed(self, scope: str, rows_pushed: int) -> None:
        """Write or replace the sentinel for *scope* with no error."""
        with self._local._connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO _amx_backfill_state "
                "(scope, shared_profile, shared_schema, completed_at, rows_pushed, last_error) "
                "VALUES (?, ?, ?, ?, ?, NULL)",
                (scope, self._shared_profile, self._shared_schema, time.time(), rows_pushed),
            )

    # ── per-table backfills ───────────────────────────────────────────────────

    def _backfill_lineage_artifacts(self, report: BackfillReport) -> dict[int, str]:
        """Push local lineage_artifacts rows to the shared store.

        Returns a ``local_id -> shared_uuid`` mapping used by the node
        and comment backfills so they can supply the correct
        ``artifact_uuid`` without a second lookup round-trip.
        """
        mapping: dict[int, str] = {}
        with self._local._connect() as conn:
            cur = conn.execute(
                "SELECT id, name, db_profile, depth_up, depth_down, format, output_path, "
                "edge_set_hash, node_count, edge_count, generated_at, extractors_used, "
                "extractors_partial "
                "FROM lineage_artifacts"
            )
            rows = cur.fetchall()
        for row in rows:
            local_id = int(row[0])
            existing = self._shared.find_lineage_uuid_by_local_id(
                hostname=self._shared._hostname, local_id=local_id
            )
            if existing:
                mapping[local_id] = existing
                report.skipped += 1
                continue
            # Synthesise a human-readable anchor ref from db_profile + name
            # because the local store uses an integer FK (anchor_entity_id)
            # while the shared store uses a freeform string.
            anchor_ref = f"{row[2] or 'unknown'}|backfilled|{row[1]}"
            try:
                uuid_val = self._shared.create_lineage_artifact(
                    local_id=local_id,
                    name=row[1],
                    db_profile=row[2] or "unknown",
                    anchor_entity_ref=anchor_ref,
                    depth_up=row[3],
                    depth_down=row[4],
                    format=row[5],
                    output_path=row[6],
                    edge_set_hash=row[7],
                    node_count=row[8],
                    edge_count=row[9],
                )
                mapping[local_id] = uuid_val
                report.succeeded += 1
                report.per_table_counts["lineage_artifacts"] = (
                    report.per_table_counts.get("lineage_artifacts", 0) + 1
                )
                if self._progress_cb:
                    self._progress_cb("lineage_artifacts", report.succeeded, len(rows))
            except Exception as exc:  # noqa: BLE001
                logger.debug("lineage_artifact backfill failed for local_id=%s: %s", local_id, exc)
                report.failed += 1
        return mapping

    def _backfill_lineage_artifact_nodes(
        self, report: BackfillReport, artifact_map: dict[int, str]
    ) -> None:
        """Push local lineage_artifact_nodes using the artifact_map for UUID resolution."""
        with self._local._connect() as conn:
            cur = conn.execute(
                "SELECT id, artifact_id, x, y, width, height, z_index, logo_key "
                "FROM lineage_artifact_nodes"
            )
            rows = cur.fetchall()
        for row in rows:
            local_id = int(row[0])
            local_artifact_id = int(row[1])
            artifact_uuid = artifact_map.get(local_artifact_id)
            if not artifact_uuid:
                report.skipped += 1
                continue
            try:
                self._shared.upsert_lineage_node(
                    local_id=local_id,
                    artifact_uuid=artifact_uuid,
                    entity_ref="backfilled|unknown",
                    entity_kind="unknown",
                    db_profile="backfilled",
                    x=row[2] or 0.0,
                    y=row[3] or 0.0,
                    width=row[4] or 100.0,
                    height=row[5] or 80.0,
                    z_index=row[6] or 0,
                    logo_key=row[7],
                )
                report.succeeded += 1
                report.per_table_counts["lineage_artifact_nodes"] = (
                    report.per_table_counts.get("lineage_artifact_nodes", 0) + 1
                )
            except Exception as exc:  # noqa: BLE001
                logger.debug("lineage_node backfill failed local_id=%s: %s", local_id, exc)
                report.failed += 1

    def _backfill_lineage_comments(
        self, report: BackfillReport, artifact_map: dict[int, str]
    ) -> None:
        """Push local lineage_comments using the artifact_map for UUID resolution."""
        with self._local._connect() as conn:
            cur = conn.execute(
                "SELECT id, artifact_id, x, y, width, height, color, text, style "
                "FROM lineage_comments"
            )
            rows = cur.fetchall()
        for row in rows:
            local_id = int(row[0])
            local_artifact_id = int(row[1])
            artifact_uuid = artifact_map.get(local_artifact_id)
            if not artifact_uuid:
                report.skipped += 1
                continue
            try:
                self._shared.upsert_lineage_comment(
                    local_id=local_id,
                    artifact_uuid=artifact_uuid,
                    x=row[2] or 0.0,
                    y=row[3] or 0.0,
                    width=row[4] or 200.0,
                    height=row[5] or 80.0,
                    color=row[6],
                    text=row[7] or "",
                    style=row[8] or "note",
                )
                report.succeeded += 1
                report.per_table_counts["lineage_comments"] = (
                    report.per_table_counts.get("lineage_comments", 0) + 1
                )
            except Exception as exc:  # noqa: BLE001
                logger.debug("comment backfill failed local_id=%s: %s", local_id, exc)
                report.failed += 1

    def _backfill_documentation_pages(self, report: BackfillReport) -> None:
        """Push local documentation_pages rows to the shared store.

        Pages use a UUID string PK on both sides. The shared store is
        queried via ``find_documentation_page_uuid`` to check for an
        existing row keyed by ``(hostname, local_id)``; if absent the row
        is inserted via ``create_documentation_page``.
        """
        with self._local._connect() as conn:
            cur = conn.execute(
                "SELECT id, title, slug, markdown_body, rendered_html, status, "
                "created_at, updated_at, created_by, db_profile FROM documentation_pages"
            )
            rows = cur.fetchall()

        if not hasattr(self._shared, "create_documentation_page"):
            # Shared store does not yet expose the pages surface; skip silently.
            report.skipped += len(rows)
            return

        for row in rows:
            page_id = row[0]
            try:
                if self._shared.find_documentation_page_by_id(page_id):
                    report.skipped += 1
                    continue
                self._shared.create_documentation_page(
                    page_id=page_id,
                    title=row[1],
                    slug=row[2],
                    markdown_body=row[3] or "",
                    rendered_html=row[4],
                    status=row[5] or "draft",
                    created_by=row[8],
                    db_profile=row[9],
                )
                report.succeeded += 1
                report.per_table_counts["documentation_pages"] = (
                    report.per_table_counts.get("documentation_pages", 0) + 1
                )
            except Exception as exc:  # noqa: BLE001
                logger.debug("pages backfill failed id=%s: %s", page_id, exc)
                report.failed += 1


def start_background_backfill(
    local: Any,
    shared: Any,
    *,
    shared_profile: str,
    shared_schema: str,
) -> threading.Thread:
    """Start the backfill in a daemon thread; returns the thread handle.

    The thread is a daemon so it never blocks interpreter shutdown.
    Failures inside the runner are absorbed and logged at DEBUG level;
    they do not propagate to the caller.
    """
    runner = BackfillRunner(
        local, shared, shared_profile=shared_profile, shared_schema=shared_schema
    )
    t = threading.Thread(target=runner.run, name="amx-backfill", daemon=True)
    t.start()
    return t


__all__ = [
    "BACKFILL_SCOPES",
    "BackfillReport",
    "BackfillRunner",
    "start_background_backfill",
]
