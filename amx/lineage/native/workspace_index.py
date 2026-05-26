"""Resolve Databricks notebook ids to real names via a cached workspace index.

Unity Catalog lineage hands back a bare ``notebook_id`` for every notebook
asset, and Databricks has no cheap ``notebook_id → name`` endpoint
(``workspace/get-status`` requires a *path*, not an id). The only public
mapping is the recursive ``workspace/list`` scan — which is too slow to run
inline on every fetch (it was the source of a multi-minute hang).

This module builds that scan **once per process** and caches the resulting
``object_id → name`` map (keyed by profile + host), so the cost is incurred
on the first fetch of a session and every later fetch resolves instantly. The
scan is time-budgeted so it can never hang a fetch: on a very large
workspace it stops at the budget and resolves whatever it indexed; notebooks
it didn't reach keep their ``"notebook <id>"`` placeholder (unchanged from
today — so this is purely additive, never a regression).

Jobs / pipelines / queries are NOT handled here: they already resolve via
their own direct REST GETs in ``DatabricksWorkspaceClient.resolve_entity_name``.
"""

from __future__ import annotations

import threading
import time
from dataclasses import replace
from typing import TYPE_CHECKING

from amx.lineage.native import provider as P
from amx.utils.logging import get_logger

if TYPE_CHECKING:
    from amx.db.adapters._databricks_workspace import DatabricksWorkspaceClient

log = get_logger("lineage.native.workspace_index")

# Default wall-clock budget for the first (cold-cache) workspace scan. A
# huge workspace won't finish in this window — that's deliberate: we cap
# the wait, cache what we got, and leave the rest as placeholders rather
# than hang the fetch.
DEFAULT_BUDGET_S = 40.0
# How long a built index stays valid within the process before a rebuild.
DEFAULT_TTL_S = 3600.0

# Process-wide cache: key ``"<profile>|<host>"`` → {object_id: notebook name}.
# Guarded by a lock so two concurrent fetches don't both scan.
_CACHE: dict[str, dict[str, str]] = {}
_CACHE_AT: dict[str, float] = {}
_LOCK = threading.Lock()


def _basename(path: str) -> str:
    p = (path or "").rstrip("/")
    return p.rsplit("/", 1)[-1] if "/" in p else p


def _build_index(client: DatabricksWorkspaceClient, budget_s: float) -> dict[str, str]:
    """Scan the workspace tree (time-budgeted) into an object_id → name map.

    Only NOTEBOOK objects are kept — those are the ids lineage leaves
    unresolved. Listing still descends every directory, so the budget is
    what bounds the cost; we stop the moment it is exceeded.
    """
    out: dict[str, str] = {}
    deadline = time.monotonic() + max(1.0, budget_s)
    try:
        for obj in client.list_workspace_objects():
            if time.monotonic() > deadline:
                log.info(
                    "workspace index: hit %.0fs budget, partial map (%d notebooks)",
                    budget_s,
                    len(out),
                )
                break
            if obj.get("object_type") != "NOTEBOOK":
                continue
            oid = obj.get("object_id")
            path = obj.get("path")
            if oid is not None and path:
                out[str(oid)] = _basename(str(path))
    except Exception as exc:  # noqa: BLE001 — name resolution is best-effort
        log.info("workspace index: scan failed (%s); resolved %d so far", exc, len(out))
    return out


def _index_for(
    client: DatabricksWorkspaceClient,
    cache_key: str,
    *,
    budget_s: float,
    ttl_s: float,
) -> dict[str, str]:
    """Return the cached object_id → name map, building it if cold / stale."""
    with _LOCK:
        built_at = _CACHE_AT.get(cache_key)
        if built_at is not None and (time.time() - built_at) < ttl_s:
            return _CACHE[cache_key]
        index = _build_index(client, budget_s)
        _CACHE[cache_key] = index
        _CACHE_AT[cache_key] = time.time()
        return index


def resolve_notebook_names(
    result: P.NativeLineageResult,
    client: DatabricksWorkspaceClient,
    *,
    profile: str,
    budget_s: float = DEFAULT_BUDGET_S,
    ttl_s: float = DEFAULT_TTL_S,
) -> None:
    """Rewrite ``notebook <id>`` placeholders in ``result`` with real names.

    Best-effort and in place: builds (or reuses) the cached workspace index
    and replaces the name of every notebook node whose id it knows. Nodes it
    can't resolve are left untouched. No-op when the result has no notebook
    nodes still showing a placeholder, so a fetch with no notebooks never
    triggers a scan.
    """
    has_notebook = any(
        node.kind == P.NOTEBOOK and node.external_id
        for edge in result.edges
        for node in (edge.source, edge.target)
    )
    if not has_notebook:
        return

    host = getattr(client, "host", "")
    index = _index_for(client, f"{profile}|{host}", budget_s=budget_s, ttl_s=ttl_s)
    if not index:
        return

    def named(node: P.NativeLineageNode) -> P.NativeLineageNode:
        if node.kind != P.NOTEBOOK or not node.external_id:
            return node
        name = index.get(node.external_id)
        return replace(node, name=name) if name else node

    result.edges = [
        replace(e, source=named(e.source), target=named(e.target)) for e in result.edges
    ]


def clear_cache() -> None:
    """Drop the process-wide index cache (used by tests)."""
    with _LOCK:
        _CACHE.clear()
        _CACHE_AT.clear()


__all__ = ["resolve_notebook_names", "clear_cache", "DEFAULT_BUDGET_S"]
