"""Anchor-based ingested-asset retrieval for the ASK pipeline.

When the SearchAgent has resolved catalog_entities (tables/columns)
relevant to a question, this module pulls back notebooks, queries,
streams, and pipelines that reference those tables via the
``asset_references_table`` edges in ``catalog_relationships``. The
edges are populated at ingest time by
``SyncMixin.rebuild_remote_asset_lineage()``.

Snippets are scored with the same BM25-lite formula used by
``amx.pages.evidence`` so the ranking model stays consistent across
evidence kinds.
"""

from __future__ import annotations

import re
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

_WORD_RE = re.compile(r"[A-Za-z0-9_]+")

_ASSET_KINDS = ("notebook", "query", "stream", "pipeline")

_ASSET_KIND_TO_TABLE: dict[str, tuple[str, tuple[str, ...]]] = {
    "notebook": (
        "remote_notebooks",
        ("name", "workspace_path", "qualified_name", "source_text"),
    ),
    "query": ("remote_queries", ("name", "sql_text")),
    "stream": (
        "remote_streams",
        ("qualified_name", "source_table_fqn"),
    ),
    "pipeline": ("remote_pipelines", ("name", "target_schema")),
}


@dataclass(slots=True)
class AssetEvidenceItem:
    asset_id: str
    kind: str
    name: str
    profile: str
    location: str
    excerpt: str


@dataclass(slots=True)
class AssetsEvidence:
    items: list[AssetEvidenceItem] = field(default_factory=list)

    @property
    def is_empty(self) -> bool:
        return not self.items


def build_assets_evidence(
    *,
    store: Any,
    entity_ids: Iterable[int],
    question_terms: Iterable[str],
    max_assets: int = 3,
    max_excerpt_chars: int = 400,
    enabled: bool = True,
) -> AssetsEvidence:
    """Return up to ``max_assets`` ingested assets that reference any of
    ``entity_ids``. Skips silently when no edges or no assets exist."""
    if not enabled:
        return AssetsEvidence()
    ids = [int(i) for i in entity_ids if int(i) > 0]
    if not ids:
        return AssetsEvidence()
    terms = [t.lower() for t in question_terms if t and len(t) > 2]
    kinds_placeholder = ",".join("?" for _ in _ASSET_KINDS)
    ids_placeholder = ",".join("?" for _ in ids)
    with store._connect() as conn:  # noqa: SLF001
        rows = conn.execute(
            f"""
            SELECT DISTINCT r.from_entity_kind, r.from_entity_id
            FROM catalog_relationships r
            WHERE r.relationship_type = 'asset_references_table'
              AND r.from_entity_kind IN ({kinds_placeholder})
              AND r.to_entity_id IN ({ids_placeholder})
            """,
            (*_ASSET_KINDS, *ids),
        ).fetchall()
        scored: list[tuple[float, AssetEvidenceItem]] = []
        for from_kind, from_id in rows:
            kind = str(from_kind)
            spec = _ASSET_KIND_TO_TABLE.get(kind)
            if spec is None:
                continue
            table_name, text_fields = spec
            payload = _load_asset_row(conn, table_name, int(from_id), text_fields)
            if payload is None:
                continue
            text = " ".join(payload["text_blobs"])
            excerpt = _best_excerpt(text, terms, max_excerpt_chars)
            score = _bm25_lite_score(text, terms)
            scored.append(
                (
                    score,
                    AssetEvidenceItem(
                        asset_id=str(payload["id"]),
                        kind=kind,
                        name=str(payload["name"]),
                        profile=str(payload["profile"]),
                        location=str(payload["location"]),
                        excerpt=excerpt,
                    ),
                )
            )
    scored.sort(key=lambda pair: pair[0], reverse=True)
    return AssetsEvidence(items=[item for _, item in scored[:max_assets]])


def _load_asset_row(
    conn: Any,
    table_name: str,
    row_id: int,
    text_fields: tuple[str, ...],
) -> dict[str, Any] | None:
    """Fetch a single ``remote_*`` row and return a normalised payload.

    The schemas of the six remote tables diverge: some carry a
    ``name``, some a ``qualified_name``; ``workspace_path`` only
    exists on notebooks. The mapping keeps this resolver
    schema-agnostic at the call site.
    """
    select_fields = "profile_name, " + ", ".join(text_fields)
    sql = f"SELECT id, {select_fields} FROM {table_name} WHERE id = ?"
    row = conn.execute(sql, (row_id,)).fetchone()
    if row is None:
        return None
    profile = row[1] or ""
    blobs = [str(v or "") for v in row[2:]]
    name = blobs[0] if blobs else str(row[0])
    location = blobs[1] if len(blobs) >= 2 else ""
    return {
        "id": row[0],
        "profile": profile,
        "name": name,
        "location": location,
        "text_blobs": blobs,
    }


def _bm25_lite_score(body: str, terms: list[str]) -> float:
    if not terms or not body:
        return 0.0
    tokens = [m.group(0).lower() for m in _WORD_RE.finditer(body)]
    if not tokens:
        return 0.0
    counts = Counter(tokens)
    total = len(tokens)
    score = 0.0
    for term in terms:
        tf = counts.get(term, 0)
        if tf:
            score += tf / (tf + 1.5 * (total / 500))
    return score


def _best_excerpt(body: str, terms: list[str], cap: int) -> str:
    if not body:
        return ""
    body = body.strip()
    if len(body) <= cap:
        return body
    if not terms:
        return body[:cap].rstrip() + "..."
    chunks = re.split(r"\n\s*\n|;\s*", body)
    chunks = [c.strip() for c in chunks if c.strip()]
    if not chunks:
        return body[:cap].rstrip() + "..."
    best = max(chunks, key=lambda c: sum(c.lower().count(t) for t in terms))
    if len(best) <= cap:
        return best
    return best[:cap].rstrip() + "..."


__all__ = [
    "AssetEvidenceItem",
    "AssetsEvidence",
    "build_assets_evidence",
]
