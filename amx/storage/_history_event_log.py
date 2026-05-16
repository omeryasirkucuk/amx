"""``app_events`` audit logging extracted from :mod:`amx.storage.sqlite_store`.

A single one-shot ``INSERT INTO app_events`` row per CLI command keeps
the lightweight telemetry trail the ``/history events`` view reads. The
function takes the store as ``hs`` and uses its ``_lock`` +
``_connect()`` plumbing — no DDL touched.
"""

from __future__ import annotations

import json
import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from amx.storage.sqlite_store import SQLiteHistoryStore


def log_event(
    hs: SQLiteHistoryStore,
    *,
    event_type: str,
    status: str,
    command: str,
    details: dict[str, Any] | None = None,
) -> None:
    with hs._lock, hs._connect() as conn:
        conn.execute(
            """
            INSERT INTO app_events (created_at, event_type, status, command, details_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                time.time(),
                event_type,
                status,
                command,
                json.dumps(details or {}, ensure_ascii=True),
            ),
        )
