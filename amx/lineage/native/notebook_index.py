"""Persisted, background-built notebook name index for Databricks.

Unity Catalog lineage identifies a notebook only by its workspace
``object_id``, and Databricks exposes no ``object_id -> name`` reverse
lookup: ``/api/2.0/workspace/get-status`` *requires* a ``path`` and
rejects an ``object_id`` (HTTP 400 "Missing required field: path"). The
only public mapping is the recursive ``workspace/list`` scan.

This module runs that scan once, **off the fetch path**, in a daemon
thread, and persists the resulting ``object_id -> name`` map to a JSON
cache next to the history DB (keyed by profile + host). A fetch reads
whatever the cache holds and never blocks on the scan: the first fetch
on a cold cache leaves notebooks as ``"notebook <id>"`` placeholders and
kicks off the background build; later fetches resolve instantly. The
cache is rebuilt when older than a TTL.
"""

from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING

from amx.lineage.native import provider as P
from amx.utils.logging import get_logger

if TYPE_CHECKING:
    from amx.db.adapters._databricks_workspace import DatabricksWorkspaceClient

log = get_logger("lineage.native.notebook_index")

# How long a built index stays valid before a background rebuild.
DEFAULT_TTL_S = 24 * 3600.0

# Guards against two concurrent background builds for the same cache file.
_BUILDING: set[str] = set()
_LOCK = threading.Lock()


def _basename(path: str) -> str:
    p = (path or "").rstrip("/")
    return p.rsplit("/", 1)[-1] if "/" in p else p


def _slug(value: str) -> str:
    out = "".join(ch if ch.isalnum() else "_" for ch in (value or "")).strip("_")
    return out or "default"


def cache_path(cache_dir: Path, profile: str, host: str) -> Path:
    """Return the JSON cache path for one ``profile`` + ``host`` index."""
    return Path(cache_dir) / f"notebook_index_{_slug(profile)}_{_slug(host)}.json"


def load_names(path: Path) -> dict[str, str]:
    """Return the cached ``object_id -> name`` map, or ``{}`` if absent/unreadable."""
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    names = data.get("names") if isinstance(data, dict) else None
    if not isinstance(names, dict):
        return {}
    return {str(k): str(v) for k, v in names.items()}


def built_at(path: Path) -> float:
    """Epoch seconds when the cache was last written, or ``0.0`` if absent."""
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return 0.0
    if not isinstance(data, dict):
        return 0.0
    try:
        return float(data.get("built_at", 0.0))
    except (TypeError, ValueError):
        return 0.0


def is_stale(path: Path, *, ttl_s: float = DEFAULT_TTL_S) -> bool:
    """True when the cache is missing or older than ``ttl_s`` seconds."""
    ts = built_at(path)
    return ts <= 0.0 or (time.time() - ts) > ttl_s


def build_index(client: DatabricksWorkspaceClient, path: Path) -> int:
    """Scan the workspace tree into the JSON cache; return the notebook count.

    Blocking and uncapped — meant to run in a background thread, never
    inline on a fetch. Persists atomically (temp file + ``os.replace``)
    so a concurrent reader never sees a half-written cache.
    """
    names: dict[str, str] = {}
    try:
        for obj in client.list_workspace_objects():
            if obj.get("object_type") != "NOTEBOOK":
                continue
            oid = obj.get("object_id")
            obj_path = obj.get("path")
            if oid is not None and obj_path:
                names[str(oid)] = _basename(str(obj_path))
    except Exception as exc:  # noqa: BLE001 — best-effort; persist whatever we reached
        log.info("notebook index: scan stopped (%s); resolved %d so far", exc, len(names))

    payload = {"built_at": time.time(), "names": names}
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".tmp")
    tmp.write_text(json.dumps(payload), encoding="utf-8")
    os.replace(tmp, target)
    log.info("notebook index: built %d notebooks at %s", len(names), target)
    return len(names)


def ensure_background_build(
    client: DatabricksWorkspaceClient, path: Path, *, ttl_s: float = DEFAULT_TTL_S
) -> None:
    """Build the index in a daemon thread if cold/stale (never blocks the caller)."""
    if not is_stale(path, ttl_s=ttl_s):
        return
    key = str(path)
    with _LOCK:
        if key in _BUILDING:
            return
        _BUILDING.add(key)

    def _run() -> None:
        try:
            build_index(client, path)
        except Exception as exc:  # noqa: BLE001 — never let a background build crash the app
            log.info("notebook index: background build failed: %s", exc)
        finally:
            with _LOCK:
                _BUILDING.discard(key)

    threading.Thread(target=_run, name="amx-notebook-index", daemon=True).start()


def resolve_names(result: P.NativeLineageResult, path: Path) -> None:
    """Rewrite ``notebook <id>`` placeholders in ``result`` from the cache, in place.

    Best-effort: notebooks whose id is not in the cache (or a cold cache)
    keep their placeholder, so this is purely additive and never raises.
    """
    names = load_names(path)
    if not names:
        return

    def named(node: P.NativeLineageNode) -> P.NativeLineageNode:
        if node.kind != P.NOTEBOOK or not node.external_id:
            return node
        nm = names.get(str(node.external_id))
        return replace(node, name=nm) if nm else node

    result.edges = [
        replace(e, source=named(e.source), target=named(e.target)) for e in result.edges
    ]


__all__ = [
    "cache_path",
    "load_names",
    "built_at",
    "is_stale",
    "build_index",
    "ensure_background_build",
    "resolve_names",
    "DEFAULT_TTL_S",
]
