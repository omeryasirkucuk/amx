"""Hybrid keyword + semantic search over ingested remote assets.

The Studio Assets page and ``/db assets search`` CLI surface used
to drive their results purely through dense semantic retrieval
(:class:`amx.assets.rag.AssetRAGStore`). MiniLM's small embedding
space is generous with its match radius — typing ``trips`` in the
Queries tab would return SQL without the word *trips* anywhere
because the embedding had landed close to a different cluster.

This module replaces that flow with a keyword-first design:

1. SQLite FTS5 virtual tables (``fts_notebooks``, ``fts_queries``,
   ...) generate the candidate set. BM25 ranks the matches; the
   candidate's name / text is guaranteed to contain the query
   tokens, so "result does not contain the search term" is
   impossible by construction.
2. :meth:`AssetRAGStore.rerank` orders those candidate ``remote_id``
   values by cosine similarity so the most semantically relevant
   hit floats to the top of the candidate set.

Semantic-only fallback is opt-in (``mode="semantic_only"``) for the
power-user synonym path. ``mode="auto"`` runs keyword-strict first
and falls back to semantic-only when there are zero keyword hits;
auto-fallback hits are tagged ``match_type="semantic_only"`` so the
UI can show a "synonym match" badge.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any, Literal

from amx.utils.logging import get_logger

if TYPE_CHECKING:
    from amx.assets.rag import AssetRAGStore
    from amx.assets.types import AssetQueryHit

log = get_logger("assets.search")


SearchMode = Literal["keyword_strict", "semantic_only", "auto"]


KIND_TO_FTS_TABLE: dict[str, str] = {
    "notebook": "fts_notebooks",
    "query": "fts_queries",
    "job": "fts_jobs",
    "pipeline": "fts_pipelines",
    "stream": "fts_streams",
    "streamlit": "fts_streamlit",
}


_FTS_SAFE_TOKEN = re.compile(r"[^\w]+", re.UNICODE)


def _build_fts_match(query: str) -> str:
    """Turn a free-form user query into an FTS5 MATCH expression.

    FTS5 syntax treats unbalanced quotes and reserved punctuation as
    syntax errors. We strip everything that is not a word character
    and join tokens with ``AND`` so every token must appear (FTS5's
    implicit operator is ``AND`` but explicit is safer when tokens
    are very short).

    Tokens shorter than 2 characters are dropped — they balloon the
    candidate set and never produce useful matches.
    """
    if not query or not query.strip():
        return ""
    tokens = [t for t in _FTS_SAFE_TOKEN.split(query.strip().lower()) if len(t) >= 2]
    if not tokens:
        return ""
    # Use prefix matching on each token so a search for "trip"
    # surfaces "trips" without forcing the user to know the full
    # form. The leading word boundary in FTS5 already prevents
    # mid-word matches.
    return " AND ".join(f"{t}*" for t in tokens)


class HybridAssetSearch:
    """Keyword-first asset search with semantic reranking.

    Holds a SQLite connection and an :class:`AssetRAGStore`. Each
    :meth:`search` call resolves the kind to its FTS5 table, runs a
    BM25-ranked candidate query, then reranks those candidates by
    cosine similarity. Idempotent and stateless beyond the two
    connections.
    """

    def __init__(self, conn: Any, rag_store: AssetRAGStore) -> None:
        self.conn = conn
        self.rag_store = rag_store

    def search(
        self,
        query_text: str,
        *,
        kind: str,
        profile: str,
        limit: int = 20,
        mode: SearchMode = "keyword_strict",
        candidate_multiplier: int = 5,
    ) -> list[AssetQueryHit]:
        """Run a hybrid keyword-first search scoped to one asset kind.

        ``query_text``
            The user's free-form search input. Tokenised into FTS5
            MATCH terms and passed straight to :class:`AssetRAGStore`
            for the rerank pass.

        ``kind``
            Required. One of the keys in :data:`KIND_TO_FTS_TABLE`.

        ``profile``
            Required. AMX DB profile name; isolates one user's
            assets from another's.

        ``limit``
            Maximum number of hits to return.

        ``mode``
            * ``keyword_strict`` (default) — return only hits whose
              text contains the query tokens (FTS5 candidates,
              semantically reranked).
            * ``semantic_only`` — skip FTS5 and run pure semantic
              retrieval. The synonym path.
            * ``auto`` — try ``keyword_strict``; fall back to
              ``semantic_only`` when zero keyword hits exist. Hits
              from the fallback path carry ``match_type =
              "semantic_only"`` in their metadata so the UI can
              tag them.

        ``candidate_multiplier``
            How many FTS5 candidates to fetch per requested hit
            before the semantic rerank. Default 5 keeps the rerank
            cheap while still giving the semantic model some slack
            to reorder.
        """
        if not query_text or not query_text.strip():
            return []
        if kind not in KIND_TO_FTS_TABLE:
            log.warning("HybridAssetSearch: unknown kind %r", kind)
            return []
        if limit <= 0:
            return []
        limit = int(limit)

        if mode == "semantic_only":
            hits = self.rag_store.query(
                query_text,
                top_k=limit,
                profile=profile,
                kind=kind,
            )
            return _tag_match_type(hits, "semantic_only")

        candidates = self._fts_candidates(
            query_text,
            kind=kind,
            profile=profile,
            limit=limit * max(int(candidate_multiplier), 1),
        )

        if candidates:
            hits = self.rag_store.rerank(
                candidates,
                query_text,
                profile=profile,
                kind=kind,
                top_k=limit,
            )
            # If rerank returns nothing (e.g. row exists in SQLite
            # but was never embedded), surface a minimal hit set
            # built from the FTS rows so the UI shows the keyword
            # matches anyway.
            if not hits:
                hits = self._minimal_hits_from_fts(candidates, kind=kind, profile=profile)
            return _tag_match_type(hits, "keyword_strict")

        if mode == "auto":
            hits = self.rag_store.query(
                query_text,
                top_k=limit,
                profile=profile,
                kind=kind,
            )
            return _tag_match_type(hits, "semantic_only")

        # keyword_strict with zero FTS candidates: empty result.
        return []

    # ── internals ────────────────────────────────────────────────

    def _fts_candidates(
        self,
        query_text: str,
        *,
        kind: str,
        profile: str,
        limit: int,
    ) -> list[int]:
        """Resolve FTS5 candidate ``remote_id`` values, BM25-ranked."""
        fts_table = KIND_TO_FTS_TABLE[kind]
        match_expr = _build_fts_match(query_text)
        if not match_expr:
            return []
        try:
            rows = self.conn.execute(
                f"""
                SELECT remote_id
                FROM {fts_table}
                WHERE {fts_table} MATCH ? AND profile_name = ?
                ORDER BY bm25({fts_table})
                LIMIT ?
                """,  # noqa: S608 — identifiers controlled, ? for user input
                (match_expr, profile, int(limit)),
            ).fetchall()
        except Exception as exc:  # noqa: BLE001
            log.warning("FTS5 query failed on %s (%r): %s", fts_table, query_text, exc)
            return []
        out: list[int] = []
        seen: set[int] = set()
        for row in rows:
            try:
                rid = int(row[0])
            except (TypeError, ValueError):
                continue
            if rid in seen:
                continue
            seen.add(rid)
            out.append(rid)
        return out

    def _minimal_hits_from_fts(
        self, remote_ids: list[int], *, kind: str, profile: str
    ) -> list[AssetQueryHit]:
        """Build a no-vector fallback when an FTS-only hit lacks an embed.

        Used when an asset row exists in SQLite + FTS5 but its chunks
        are not (yet) in Chroma. Keeps the result list non-empty
        instead of dropping perfectly good keyword matches.
        """
        from amx.assets.types import AssetQueryHit

        if not remote_ids:
            return []
        # Resolve a display name per kind.
        table, name_col = _RESOLVE_NAME[kind]
        placeholders = ",".join("?" for _ in remote_ids)
        try:
            rows = self.conn.execute(
                f"SELECT id, {name_col} FROM {table} "  # noqa: S608 — identifiers controlled
                f"WHERE profile_name = ? AND id IN ({placeholders})",
                (profile, *[int(r) for r in remote_ids]),
            ).fetchall()
        except Exception as exc:  # noqa: BLE001
            log.warning("HybridAssetSearch: fallback name lookup failed: %s", exc)
            return []
        by_id = {int(r[0]): str(r[1] or "") for r in rows}
        out: list[AssetQueryHit] = []
        for rid in remote_ids:
            if rid not in by_id:
                continue
            out.append(
                AssetQueryHit(
                    chunk_id=f"fts:{kind}:{profile}:{rid}",
                    kind=kind,
                    profile=profile,
                    remote_id=int(rid),
                    name=by_id[rid],
                    text="",
                    score=0.0,
                    metadata={},
                )
            )
        return out


# Map asset kind to the (table, name_column) tuple used by the
# fallback display-name lookup. Mirrors ASSET_KINDS in the assets
# router; kept here so this module does not depend on FastAPI.
_RESOLVE_NAME: dict[str, tuple[str, str]] = {
    "notebook": ("remote_notebooks", "name"),
    "query": ("remote_queries", "name"),
    "job": ("remote_jobs", "name"),
    "pipeline": ("remote_pipelines", "name"),
    "stream": ("remote_streams", "qualified_name"),
    "streamlit": ("remote_streamlit_apps", "qualified_name"),
}


def _tag_match_type(hits: list[AssetQueryHit], match_type: str) -> list[AssetQueryHit]:
    """Annotate hits with ``metadata["match_type"]`` for UI badging.

    ``AssetQueryHit`` is frozen, but its ``metadata`` dict field is
    mutable in place; we only ever annotate, never reassign.
    """
    for hit in hits:
        hit.metadata["match_type"] = match_type
    return hits


__all__ = ["HybridAssetSearch", "KIND_TO_FTS_TABLE", "SearchMode"]
