"""Anchor-based ingested-asset retrieval for the ASK pipeline.

When the SearchAgent has resolved catalog_entities (tables/columns)
relevant to a question, this module pulls back notebooks, queries,
streams, and pipelines that reference those tables via the
``asset_references_table`` edges in ``catalog_relationships``. The
edges are populated at ingest time by
``SyncMixin.rebuild_remote_asset_lineage()``.

Snippets are scored by the chunked + embedded asset RAG store
(:class:`amx.assets.rag.AssetRAGStore`) so a question about
"loading orders" matches a notebook whose source says "ingest the
orders table" even though no keyword overlaps. When the store is
unavailable (no ingest yet / missing optional deps), the BM25-lite
fallback over the raw asset text keeps the surface working.
"""

from __future__ import annotations

import re
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

_WORD_RE = re.compile(r"[A-Za-z0-9_]+")

_ASSET_KINDS = ("notebook", "query", "job", "pipeline", "stream", "streamlit_app")

_ASSET_KIND_TO_TABLE: dict[str, tuple[str, tuple[str, ...]]] = {
    "notebook": (
        "remote_notebooks",
        ("name", "workspace_path", "qualified_name", "source_text"),
    ),
    "query": ("remote_queries", ("name", "sql_text")),
    # Jobs carry no body text; surface the name + creator + tags JSON
    # blob so the LLM can still see what the workflow is about.
    "job": ("remote_jobs", ("name", "creator_user_name", "tags_json")),
    "pipeline": ("remote_pipelines", ("name", "target_schema")),
    "stream": (
        "remote_streams",
        ("qualified_name", "source_table_fqn"),
    ),
    "streamlit_app": (
        "remote_streamlit_apps",
        ("qualified_name", "main_file", "root_location"),
    ),
}


def _try_asset_rag_store():
    """Construct an :class:`AssetRAGStore` instance for retrieval, or
    return ``None`` when the store cannot be opened.

    Defensive degradation matters here: Ask is a hot user-facing
    path and a missing chromadb / sentence-transformers install (or
    a one-time CollectionIdentityMismatch after the user swaps
    embedding models) must not 500 the answer pipeline. Falling
    back to the legacy BM25-lite scorer keeps the user moving.
    """
    try:
        from amx.assets.rag import AssetRAGStore

        return AssetRAGStore()
    except Exception:  # noqa: BLE001 — best-effort; fall back to BM25-lite
        return None


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
    rag_store: Any | None = None,
    kinds: list[str] | None = None,
) -> AssetsEvidence:
    """Return up to ``max_assets`` ingested assets that reference any of
    ``entity_ids``. Skips silently when no edges or no assets exist.

    Tries the chunked + embedded :class:`AssetRAGStore` first
    (semantic retrieval scoped to assets whose bridge row references
    one of ``entity_ids``); falls back to the BM25-lite scorer over
    the raw asset text when the store is unavailable (Chroma not
    installed, collection empty, ``CollectionIdentityMismatch`` after
    an embedding swap).

    ``kinds`` (when not ``None``) restricts the SQL ``from_entity_kind``
    IN clause to the given singulars (subset of
    :data:`_ASSET_KINDS`). The default (``None``) keeps the legacy
    behaviour of considering every kind. An empty list returns an
    empty payload — callers gate ``enabled=False`` for that case.
    """
    if not enabled:
        return AssetsEvidence()
    ids = [int(i) for i in entity_ids if int(i) > 0]
    if not ids:
        return AssetsEvidence()
    terms = [t.lower() for t in question_terms if t and len(t) > 2]
    question_text = " ".join(terms)
    effective_kinds: tuple[str, ...]
    if kinds is None:
        effective_kinds = _ASSET_KINDS
    else:
        effective_kinds = tuple(k for k in kinds if k in _ASSET_KINDS)
        if not effective_kinds:
            return AssetsEvidence()
    kinds_placeholder = ",".join("?" for _ in effective_kinds)
    ids_placeholder = ",".join("?" for _ in ids)
    with store._connect() as conn:  # noqa: SLF001
        # ``from_entity_id`` points at a catalog_entities bridge row
        # (entity_kind='notebook'|'query'|'stream'|'pipeline',
        # schema_name='__assets'); ``source_remote_id`` + ``db_profile``
        # locate the canonical remote_<kind>s row for the asset
        # content.
        rows = conn.execute(
            f"""
            SELECT DISTINCT r.from_entity_kind, ce.source_remote_id,
                   ce.db_profile
            FROM catalog_relationships r
            JOIN catalog_entities ce ON ce.id = r.from_entity_id
            WHERE r.relationship_type = 'asset_references_table'
              AND r.from_entity_kind IN ({kinds_placeholder})
              AND r.to_entity_id IN ({ids_placeholder})
              AND ce.source_remote_id IS NOT NULL
            """,
            (*effective_kinds, *ids),
        ).fetchall()
        scoped: list[tuple[str, int, str]] = []
        for from_kind, remote_id, profile in rows:
            if remote_id is None or not profile:
                continue
            kind = str(from_kind)
            if kind not in _ASSET_KIND_TO_TABLE:
                continue
            scoped.append((kind, int(remote_id), str(profile)))
        if not scoped:
            return AssetsEvidence()

        # Try the asset RAG store first. Group by (profile, kind) so
        # we can pass tight ``remote_ids`` filters to each query.
        if question_text:
            store_obj = rag_store if rag_store is not None else _try_asset_rag_store()
            if store_obj is not None:
                hits = _rag_query(
                    store=store_obj,
                    scoped=scoped,
                    question=question_text,
                    max_assets=max_assets,
                    max_excerpt_chars=max_excerpt_chars,
                )
                if hits:
                    return AssetsEvidence(items=hits)

        # Fallback: BM25-lite over the raw asset text (pre-RAG path).
        scored: list[tuple[float, AssetEvidenceItem]] = []
        for kind, remote_id, _profile in scoped:
            spec = _ASSET_KIND_TO_TABLE.get(kind)
            if spec is None:
                continue
            table_name, text_fields = spec
            payload = _load_asset_row(conn, table_name, int(remote_id), text_fields)
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


def _rag_query(
    *,
    store: Any,
    scoped: list[tuple[str, int, str]],
    question: str,
    max_assets: int,
    max_excerpt_chars: int,
) -> list[AssetEvidenceItem]:
    """Group ``scoped`` triples by (profile, kind), query the asset RAG
    store per group, and merge the hits by score."""
    from collections import defaultdict

    groups: dict[tuple[str, str], list[int]] = defaultdict(list)
    for kind, remote_id, profile in scoped:
        groups[(profile, kind)].append(remote_id)

    all_hits: list[Any] = []
    for (profile, kind), remote_ids in groups.items():
        try:
            results = store.query(
                question,
                top_k=max_assets,
                profile=profile,
                kind=kind,
                remote_ids=remote_ids,
            )
        except Exception:  # noqa: BLE001
            results = []
        all_hits.extend(results)

    if not all_hits:
        return []

    all_hits.sort(key=lambda h: float(getattr(h, "score", 0.0)), reverse=True)
    out: list[AssetEvidenceItem] = []
    seen: set[tuple[str, int]] = set()
    for hit in all_hits:
        key = (str(hit.kind), int(hit.remote_id))
        if key in seen:
            continue
        seen.add(key)
        excerpt = (hit.text or "").strip()
        if len(excerpt) > max_excerpt_chars:
            excerpt = excerpt[:max_excerpt_chars].rstrip() + "..."
        out.append(
            AssetEvidenceItem(
                asset_id=str(hit.remote_id),
                kind=str(hit.kind),
                name=str(hit.name or hit.chunk_id),
                profile=str(hit.profile),
                location=str(
                    hit.metadata.get("workspace_path") or hit.metadata.get("warehouse") or ""
                ),
                excerpt=excerpt,
            )
        )
        if len(out) >= max_assets:
            break
    return out


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
