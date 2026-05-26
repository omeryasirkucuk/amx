"""On-demand content ingest for native-lineage-discovered assets.

When native lineage names a notebook / query the user can read, this
pulls its actual content (notebook source, query SQL) into the local
``remote_*`` tables — the same store the Assets page reads — so the
asset persists under Assets and the lineage canvas can drill into it.
Targeted by id (two REST calls per asset) rather than a full-workspace
list, so a fetch stays fast.

Returns the ``remote_<kind>s.id`` so the materializer can build a
``full`` bridge whose edge points at the real, drillable asset instead
of a name-only ghost. Any access failure returns ``None`` → the asset
stays name-only.
"""

from __future__ import annotations

import hashlib
import time
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any

from amx.lineage.native import provider as P
from amx.utils.logging import get_logger

log = get_logger("lineage.native.ingest")

# Callable the materializer invokes per asset node:
# ``(conn, node) -> remote_id | None``.
AssetIngester = Callable[[Any, P.NativeLineageNode], "int | None"]


def build_asset_ingester(*, profile: str, client: Any, catalog: Any) -> AssetIngester:
    """Build an ingester bound to one profile's workspace client + catalog."""

    def ingest(conn: Any, node: P.NativeLineageNode) -> int | None:
        if not node.external_id:
            return None
        try:
            # Notebooks are intentionally not content-ingested here:
            # resolving a notebook id → path needs a full workspace scan
            # (minutes). Notebook content comes from the normal Assets
            # ingest; native fetch only reconciles to it.
            if node.kind == P.QUERY:
                return _ingest_query(conn, profile, client, catalog, node.external_id)
        except Exception as exc:  # noqa: BLE001 — no access / API shape → stay name-only
            log.debug(
                "native lineage: content ingest skipped for %s %s: %s",
                node.kind,
                node.external_id,
                exc,
            )
            return None
        return None

    return ingest


def _ingest_query(conn: Any, profile: str, client: Any, catalog: Any, query_id: str) -> int | None:
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


__all__ = ["AssetIngester", "build_asset_ingester"]
