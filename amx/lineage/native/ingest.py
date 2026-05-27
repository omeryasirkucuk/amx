"""On-demand single-query content ingest for native-lineage assets.

When the user clicks a saved-query node on the lineage canvas, this pulls
its SQL into ``remote_queries`` (the same store the Assets page reads) by
id — one REST call — so the node becomes a full, drillable, searchable
asset instead of a name-only ghost. Returns the ``remote_queries.id`` (or
``None`` on no-access / empty SQL) for the lazy-ingest endpoint to hand
back to the canvas.
"""

from __future__ import annotations

import hashlib
import time
from types import SimpleNamespace
from typing import Any


def ingest_query_by_id(
    conn: Any, profile: str, client: Any, catalog: Any, query_id: str
) -> int | None:
    """Ingest one saved query by id into ``remote_queries``; return its id or None."""
    body = client.query_definition(query_id)
    sql = str(body.get("query_text") or body.get("query") or "")
    if not sql:
        return None
    dto = SimpleNamespace(
        platform="databricks",
        kind="saved_query",
        external_id=query_id,
        name=str(body.get("display_name") or body.get("name") or query_id),
        sql_text=sql,
        sql_hash=hashlib.sha256(sql.encode("utf-8")).hexdigest(),
        warehouse=None,
        user_name=None,
        executed_at=None,
        duration_ms=None,
    )
    catalog._upsert_remote_queries(conn, profile, [dto], _iso_now())
    row = conn.execute(
        "SELECT id FROM remote_queries WHERE profile_name = ? AND platform = 'databricks' "
        "AND kind = 'saved_query' AND external_id = ?",
        (profile, query_id),
    ).fetchone()
    return int(row[0]) if row else None


def _iso_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())


__all__ = ["ingest_query_by_id"]
