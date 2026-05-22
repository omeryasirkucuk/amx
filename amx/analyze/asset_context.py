"""Resolve ingested-asset refs into per-table context blocks for ``/analyze run``.

When the user attaches notebooks / queries / streams / pipelines to a
Run as additional context, the worker calls
:func:`resolve_asset_context_for_run` to:

1. Look up each ``<profile>:<remote_id>`` ref in the local
   ``remote_*`` tables and extract a short text excerpt per asset.
2. Resolve the tables that asset references via the
   ``asset_references_table`` edges in ``catalog_relationships`` (and
   their ``catalog_entities`` bridge rows — see PR3).
3. Return a ``dict[(schema, table) -> list[asset_block]]`` so the
   orchestrator can attach the matching blocks to ``AgentContext``
   for each table it processes.

The blocks are intentionally compact (~5 KB per asset) so they fit
inside the existing LLM prompt budget without triggering the
context-trim heuristics in ``profile_agent``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from amx.utils.logging import get_logger

log = get_logger("analyze.asset_context")

_NOTEBOOK_CAP = 6 * 1024
_QUERY_CAP = 4 * 1024
_STREAM_CAP = 600
_PIPELINE_CAP = 1500


@dataclass(slots=True)
class AssetRef:
    """A single ingested-asset reference for run context.

    ``kind`` ∈ {asset_notebook, asset_job, asset_pipeline,
    asset_query, asset_stream, asset_streamlit_app}. ``ref`` is
    ``<profile>:<remote_id>``.
    """

    kind: str
    ref: str


@dataclass(slots=True)
class ResolvedAsset:
    """A ref after lookup against the local history store.

    ``tables`` is the set of ``(schema, table)`` tuples that this
    asset's source text references — gathered via the
    ``asset_references_table`` edges so the same scope expansion as
    the lineage canvas applies.
    """

    kind: str
    name: str
    profile: str
    excerpt: str
    tables: set[tuple[str, str]]


def resolve_asset_context_for_run(
    *,
    store: Any,
    refs: list[AssetRef],
) -> tuple[dict[tuple[str, str], list[dict[str, Any]]], list[ResolvedAsset]]:
    """Resolve ``refs`` into per-table context blocks for a Run.

    Returns ``(blocks_by_table, resolved_assets)``.

    * ``blocks_by_table`` maps each referenced table identity (lower-
      cased ``(schema, table)``) to a list of dict blocks the
      orchestrator copies into ``AgentContext.asset_context``.
    * ``resolved_assets`` is the audit-trail view of the same data,
      flattened so the worker can persist it on ``analysis_runs``.
    """
    blocks_by_table: dict[tuple[str, str], list[dict[str, Any]]] = {}
    resolved: list[ResolvedAsset] = []
    if not refs or store is None:
        return blocks_by_table, resolved
    with store._connect() as conn:  # noqa: SLF001
        for r in refs:
            payload = _load_one_asset(conn, r)
            if payload is None:
                continue
            block = {
                "kind": payload["kind"],
                "name": payload["name"],
                "profile": payload["profile"],
                "excerpt": payload["excerpt"],
            }
            for schema, table in payload["tables"]:
                key = (schema.lower(), table.lower())
                blocks_by_table.setdefault(key, []).append(block)
            resolved.append(
                ResolvedAsset(
                    kind=payload["kind"],
                    name=payload["name"],
                    profile=payload["profile"],
                    excerpt=payload["excerpt"],
                    tables=payload["tables"],
                )
            )
    return blocks_by_table, resolved


def _load_one_asset(conn: Any, ref: AssetRef) -> dict[str, Any] | None:
    """Fetch text + referenced tables for a single ``asset_*`` ref."""
    kind = (ref.kind or "").removeprefix("asset_")
    if ":" not in ref.ref:
        return None
    profile, remote_id_str = ref.ref.split(":", 1)
    try:
        remote_id = int(remote_id_str)
    except ValueError:
        return None

    spec = _ASSET_SPECS.get(kind)
    if spec is None:
        return None
    excerpt, display_name = spec(conn, profile, remote_id)
    if excerpt is None:
        return None

    # Resolve referenced tables via the catalog_entities bridge row
    # (PR3) → asset_references_table edges → table rows.
    rows = conn.execute(
        """
        SELECT ce.schema_name, ce.table_name
        FROM catalog_entities bridge
        JOIN catalog_relationships r ON r.from_entity_id = bridge.id
        JOIN catalog_entities ce ON ce.id = r.to_entity_id
        WHERE bridge.entity_kind = ?
          AND bridge.source_remote_id = ?
          AND bridge.db_profile = ?
          AND r.relationship_type = 'asset_references_table'
          AND ce.entity_kind = 'table'
        """,
        (kind, remote_id, profile),
    ).fetchall()
    tables: set[tuple[str, str]] = set()
    for schema, table in rows:
        if schema and table:
            tables.add((str(schema), str(table)))
    return {
        "kind": kind,
        "name": display_name,
        "profile": profile,
        "excerpt": excerpt,
        "tables": tables,
    }


def _load_notebook(conn: Any, profile: str, remote_id: int) -> tuple[str | None, str]:
    row = conn.execute(
        "SELECT name, source_text FROM remote_notebooks WHERE profile_name = ? AND id = ?",
        (profile, remote_id),
    ).fetchone()
    if row is None:
        return None, ""
    name, source = row
    return _excerpt_notebook(str(source or ""), _NOTEBOOK_CAP), str(name or "")


def _load_query(conn: Any, profile: str, remote_id: int) -> tuple[str | None, str]:
    row = conn.execute(
        "SELECT name, sql_text FROM remote_queries WHERE profile_name = ? AND id = ?",
        (profile, remote_id),
    ).fetchone()
    if row is None:
        return None, ""
    name, sql = row
    text = (sql or "").strip()
    if len(text) > _QUERY_CAP:
        text = text[:_QUERY_CAP] + "\n[truncated]"
    return text, str(name or "")


def _load_stream(conn: Any, profile: str, remote_id: int) -> tuple[str | None, str]:
    row = conn.execute(
        "SELECT qualified_name, source_table_fqn, mode, stale_after "
        "FROM remote_streams WHERE profile_name = ? AND id = ?",
        (profile, remote_id),
    ).fetchone()
    if row is None:
        return None, ""
    qname, source_fqn, mode, stale = row
    text = (
        f"Stream {qname or ''}\n"
        f"- source: {source_fqn or 'n/a'}\n"
        f"- mode: {mode or 'n/a'}\n"
        f"- stale_after: {stale or 'n/a'}"
    )
    if len(text) > _STREAM_CAP:
        text = text[:_STREAM_CAP]
    return text, str(qname or "")


def _load_pipeline(conn: Any, profile: str, remote_id: int) -> tuple[str | None, str]:
    row = conn.execute(
        "SELECT name, target_schema, libraries_json FROM remote_pipelines "
        "WHERE profile_name = ? AND id = ?",
        (profile, remote_id),
    ).fetchone()
    if row is None:
        return None, ""
    name, target, libs_json = row
    try:
        libs = json.loads(libs_json or "[]")
    except (json.JSONDecodeError, TypeError):
        libs = []
    notebook_paths = [
        str(lib.get("notebook", {}).get("path", ""))
        for lib in libs
        if isinstance(lib, dict) and lib.get("notebook")
    ]
    text = (
        f"Pipeline {name or ''}\n"
        f"- target schema: {target or 'n/a'}\n"
        f"- notebook libraries: {', '.join(notebook_paths) if notebook_paths else 'n/a'}"
    )
    if len(text) > _PIPELINE_CAP:
        text = text[:_PIPELINE_CAP]
    return text, str(name or "")


_ASSET_SPECS = {
    "notebook": _load_notebook,
    "query": _load_query,
    "stream": _load_stream,
    "pipeline": _load_pipeline,
}


def _excerpt_notebook(source_text: str, cap: int) -> str:
    if not source_text:
        return ""
    try:
        nb = json.loads(source_text)
    except (json.JSONDecodeError, TypeError):
        return source_text[:cap]
    cells = nb.get("cells") if isinstance(nb, dict) else None
    if not isinstance(cells, list):
        return source_text[:cap]
    out: list[str] = []
    code_seen = 0
    for cell in cells:
        if not isinstance(cell, dict):
            continue
        ctype = cell.get("cell_type")
        src = cell.get("source", "")
        if isinstance(src, list):
            src = "".join(s for s in src if isinstance(s, str))
        if not isinstance(src, str):
            continue
        if ctype == "markdown":
            out.append(src.strip())
        elif ctype == "code" and code_seen < 8:
            out.append("```\n" + src.strip() + "\n```")
            code_seen += 1
    text = "\n\n".join(b for b in out if b)
    if len(text) > cap:
        text = text[:cap] + "\n[truncated]"
    return text


__all__ = [
    "AssetRef",
    "ResolvedAsset",
    "resolve_asset_context_for_run",
]
