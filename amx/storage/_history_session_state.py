"""Session/agent state KV storage extracted from :mod:`amx.storage.sqlite_store`.

The two helpers read/write the ``session_state`` table that AMX uses
as a tiny namespaced key-value bucket for transient agent state
(active scope, recent picks, etc.). Values are JSON-serialised.

The functions take the store as ``hs`` and use its ``_lock`` +
``_connect()`` plumbing — no DDL touched.
"""

from __future__ import annotations

import json
import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from amx.storage.sqlite_store import SQLiteHistoryStore


def set_session_state(hs: SQLiteHistoryStore, namespace: str, key: str, value: Any) -> None:
    """Write-through session/agent state storage."""
    payload = json.dumps(value, ensure_ascii=True)
    with hs._lock, hs._connect() as conn:
        conn.execute(
            """
            INSERT INTO session_state (namespace, key_name, value_json, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(namespace, key_name) DO UPDATE SET
                value_json = excluded.value_json,
                updated_at = excluded.updated_at
            """,
            (namespace, key, payload, time.time()),
        )


def get_session_state(hs: SQLiteHistoryStore, namespace: str, key: str, default: Any = None) -> Any:
    with hs._connect() as conn:
        row = conn.execute(
            "SELECT value_json FROM session_state WHERE namespace = ? AND key_name = ?",
            (namespace, key),
        ).fetchone()
    if not row:
        return default
    try:
        return json.loads(str(row["value_json"] or ""))
    except Exception:
        return default
