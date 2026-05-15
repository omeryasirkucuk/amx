"""PR-E: FTS5 sidecar + RRF fusion tests for Document RAG.

Two layers under test:

1. ``amx.rag_core.fusion.reciprocal_rank_fusion`` — pure math. Verifies
   the RRF score formula, multi-ranking aggregation, and the edge
   cases (single ranking, empty rankings, candidates absent from one
   side).
2. ``amx.docs._fts5_sidecar.FTS5Sidecar`` — SQLite FTS5 mirror. Verifies
   upsert/delete-by-ids/delete-by-source/query semantics, including
   the BM25 ordering and the source filter passthrough.

Plus a smoke test that hybrid retrieval through ``RAGStore.query``
surfaces a chunk that pure dense retrieval misses (rare keyword
case — the canonical motivation for hybrid).
"""

from __future__ import annotations

from pathlib import Path

from amx.docs._fts5_sidecar import FTS5Sidecar, _sanitise_match
from amx.docs.rag import RAGStore
from amx.docs.scanner import DocInfo
from amx.rag_core.fusion import reciprocal_rank_fusion

# ── RRF math ─────────────────────────────────────────────────────────


def test_rrf_single_ranking_preserves_order() -> None:
    """With a single input ranking, RRF degenerates to that ranking's
    own order (1 / (k+1) > 1 / (k+2) > …)."""
    scores = reciprocal_rank_fusion([["a", "b", "c"]])
    assert scores["a"] > scores["b"] > scores["c"]


def test_rrf_combines_two_rankings() -> None:
    """A candidate that ranks high in both inputs gets a higher
    fused score than a candidate that ranks high in only one."""
    scores = reciprocal_rank_fusion(
        [
            ["a", "b", "c"],  # ranking 1
            ["a", "x", "y"],  # ranking 2
        ]
    )
    # "a" is rank 1 in both; "b" is only in ranking 1.
    assert scores["a"] > scores["b"]
    assert scores["a"] > scores["x"]


def test_rrf_candidate_absent_from_ranking_gets_zero_contribution() -> None:
    """A candidate that only appears in one of N rankings still gets
    a non-zero overall score (single contribution), but lower than a
    candidate present in both."""
    scores = reciprocal_rank_fusion([["a", "b"], ["a", "c"]])
    only_in_one = {"b", "c"}
    in_both = {"a"}
    for missing in only_in_one:
        for present in in_both:
            assert scores[present] > scores[missing]


def test_rrf_score_uses_recommended_constant() -> None:
    """First-rank contribution should equal ``1 / (k + 1)`` for the
    documented constant ``k = 60``. Pin this so a future caller
    changing the default has to update the test consciously."""
    scores = reciprocal_rank_fusion([["a"]])
    assert abs(scores["a"] - 1.0 / 61.0) < 1e-9


def test_rrf_empty_input_returns_empty_dict() -> None:
    assert reciprocal_rank_fusion([]) == {}
    assert reciprocal_rank_fusion([[]]) == {}


def test_rrf_rejects_non_positive_k() -> None:
    import pytest

    with pytest.raises(ValueError):
        reciprocal_rank_fusion([["a"]], k=0)
    with pytest.raises(ValueError):
        reciprocal_rank_fusion([["a"]], k=-5)


# ── _sanitise_match safety net ───────────────────────────────────────


def test_sanitise_match_quotes_each_token() -> None:
    """User-supplied input must never inject FTS5 operators
    (``AND``, ``OR``, ``NOT``, ``NEAR``). Each token is double-
    quoted so FTS5 treats it as literal."""
    out = _sanitise_match("AND NOT order")
    # Note: very-short tokens like "or" (case-folded) are dropped
    # because of the ``len(t) >= 2`` filter — but words like AND
    # and NOT are 3 chars and survive, quoted.
    assert '"AND"' in out
    assert '"NOT"' in out
    assert '"order"' in out


def test_sanitise_match_drops_short_tokens() -> None:
    """Single-character tokens add noise — every chunk matches ``a``
    once stemming runs. Drop them."""
    assert _sanitise_match("a b c") == ""


def test_sanitise_match_handles_empty_input() -> None:
    assert _sanitise_match("") == ""
    assert _sanitise_match("   ") == ""


# ── FTS5Sidecar lifecycle ────────────────────────────────────────────


def test_sidecar_upsert_then_query(tmp_path: Path) -> None:
    """End-to-end: upsert a few rows, search by a token, verify BM25
    ranks the matching row first."""
    side = FTS5Sidecar(tmp_path)
    inserted = side.upsert(
        [
            ("chunk-1", "/path/orders.txt", "The orders table stores customer purchases."),
            ("chunk-2", "/path/customers.txt", "The customers table holds buyer emails."),
            ("chunk-3", "/path/products.txt", "Products have SKUs and prices."),
        ]
    )
    assert inserted == 3
    hits = side.query("customer purchases", k=5)
    assert hits, "expected at least one match"
    # The orders chunk talks about \"customer purchases\" — should be top hit.
    top_id, top_score = hits[0]
    assert top_id == "chunk-1"
    assert top_score > 0


def test_sidecar_upsert_skips_empty_content(tmp_path: Path) -> None:
    """Empty / whitespace-only content would pollute the index with
    zero-length tokens — skip the row at upsert time."""
    side = FTS5Sidecar(tmp_path)
    inserted = side.upsert(
        [
            ("chunk-1", "/path/a.txt", "real content"),
            ("chunk-2", "/path/b.txt", "   "),
            ("chunk-3", "/path/c.txt", ""),
        ]
    )
    assert inserted == 1
    assert side.count() == 1


def test_sidecar_delete_by_ids(tmp_path: Path) -> None:
    side = FTS5Sidecar(tmp_path)
    side.upsert(
        [
            ("chunk-1", "/p/a.txt", "alpha content"),
            ("chunk-2", "/p/b.txt", "beta content"),
        ]
    )
    deleted = side.delete_by_ids(["chunk-1"])
    assert deleted == 1
    assert side.count() == 1
    # The deleted chunk is gone from queries.
    hits = side.query("alpha", k=5)
    assert all(cid != "chunk-1" for cid, _ in hits)


def test_sidecar_delete_by_source(tmp_path: Path) -> None:
    """``delete_by_source`` drops every chunk for a given file —
    used by ``RAGStore.delete_chunks_for_sources``."""
    side = FTS5Sidecar(tmp_path)
    side.upsert(
        [
            ("chunk-1", "/p/orders.txt", "alpha"),
            ("chunk-2", "/p/orders.txt", "beta"),
            ("chunk-3", "/p/customers.txt", "gamma"),
        ]
    )
    deleted = side.delete_by_source("/p/orders.txt")
    assert deleted == 2
    assert side.count() == 1


def test_sidecar_query_respects_source_filters(tmp_path: Path) -> None:
    """The BM25 query supports a list of source paths to restrict
    to — used by docs RAG's source_filters."""
    side = FTS5Sidecar(tmp_path)
    side.upsert(
        [
            ("chunk-1", "/p/orders.txt", "stock_on_hand counter"),
            ("chunk-2", "/p/inventory.txt", "stock_on_hand counter"),
        ]
    )
    # Without filter: both match.
    hits_all = side.query("stock_on_hand", k=5)
    ids_all = {cid for cid, _ in hits_all}
    assert ids_all == {"chunk-1", "chunk-2"}
    # With filter on inventory: only that one.
    hits_inv = side.query("stock_on_hand", k=5, source_filters=["/p/inventory.txt"])
    ids_inv = {cid for cid, _ in hits_inv}
    assert ids_inv == {"chunk-2"}


def test_sidecar_query_returns_empty_for_no_match(tmp_path: Path) -> None:
    side = FTS5Sidecar(tmp_path)
    side.upsert([("chunk-1", "/p/a.txt", "some content")])
    hits = side.query("nothingmatchesthis", k=5)
    assert hits == []


def test_sidecar_persists_across_reopens(tmp_path: Path) -> None:
    """The sidecar is durable — closing and re-opening recovers
    every previously-inserted row."""
    side_a = FTS5Sidecar(tmp_path)
    side_a.upsert([("chunk-1", "/p/a.txt", "persistent content")])
    side_b = FTS5Sidecar(tmp_path)  # second instance on the same dir
    assert side_b.count() == 1
    hits = side_b.query("persistent", k=5)
    assert hits and hits[0][0] == "chunk-1"


# ── RAGStore wiring: end-to-end hybrid behaviour ─────────────────────


def _make_doc(tmp_path: Path, body: str, name: str = "fixture.txt") -> DocInfo:
    p = tmp_path / name
    p.write_text(body, encoding="utf-8")
    return DocInfo(
        path=str(p),
        size_bytes=p.stat().st_size,
        extension=".txt",
        source_type="local",
        source_root=str(tmp_path),
    )


def test_ragstore_ingest_mirrors_chunks_into_fts(tmp_path: Path) -> None:
    """When ``RAGStore.ingest`` runs, every chunk lands in the FTS5
    sidecar in addition to Chroma."""
    store = RAGStore(
        persist_dir=str(tmp_path / "chroma"),
        embedding_function=None,
        embedding_provider="minilm",
        embedding_model="minilm-l6-v2",
    )
    doc = _make_doc(tmp_path, "Phantom stock bug references stock_reserved.", name="inv.txt")
    summary = store.ingest([doc])
    assert summary.chunk_count > 0
    assert store._fts.count() == summary.chunk_count


def test_ragstore_delete_sources_drops_fts_rows(tmp_path: Path) -> None:
    """``delete_chunks_for_sources`` keeps Chroma and the sidecar in
    sync — no orphan BM25 hits."""
    store = RAGStore(
        persist_dir=str(tmp_path / "chroma"),
        embedding_function=None,
        embedding_provider="minilm",
        embedding_model="minilm-l6-v2",
    )
    doc = _make_doc(tmp_path, "Orders table stores customer purchases.", name="o.txt")
    store.ingest([doc])
    before = store._fts.count()
    assert before > 0
    store.delete_chunks_for_sources([doc.path])
    assert store._fts.count() == 0


def test_ragstore_hybrid_surfaces_rare_keyword(tmp_path: Path) -> None:
    """Hybrid retrieval surfaces a chunk that contains a rare token
    even when the dense channel might prefer something semantically
    fuzzier. We use a deliberately rare identifier (a UUID-shaped
    string) as the gold answer — keyword-heavy queries are exactly
    where BM25 + RRF earn their keep."""
    rare_token = "ZX9921CORG"  # alphanumeric, unlikely in embeddings
    body_a = f"Customer feedback summary. Reference id is {rare_token}."
    body_b = "Generic customer feedback. Routine churn summary."
    body_c = "Unrelated SKU pricing rules and rollout schedule."
    docs = [
        _make_doc(tmp_path, body_a, name="ticket-a.txt"),
        _make_doc(tmp_path, body_b, name="ticket-b.txt"),
        _make_doc(tmp_path, body_c, name="pricing.txt"),
    ]
    store = RAGStore(
        persist_dir=str(tmp_path / "chroma"),
        embedding_function=None,
        embedding_provider="minilm",
        embedding_model="minilm-l6-v2",
    )
    store.ingest(docs)

    # Query by the rare identifier. Hybrid retrieval should rank
    # ticket-a top because BM25 matches the literal token whereas
    # the dense channel might not (uncommon tokens are weakly
    # represented in MiniLM's vocabulary).
    hits = store.query(f"reference id {rare_token}", n_results=3)
    assert hits, "hybrid retrieval returned no hits"
    sources = [Path(h["metadata"].get("source", "")).name for h in hits]
    assert sources[0] == "ticket-a.txt", (
        f"expected ticket-a.txt at rank 1 for rare-keyword query, got {sources}"
    )


def test_ragstore_first_open_backfills_fts_from_chroma(tmp_path: Path) -> None:
    """A pre-PR-E collection (Chroma populated, FTS5 sidecar absent
    or empty) backfills the sidecar on the next ``RAGStore`` open so
    hybrid retrieval works immediately without a manual reindex."""
    # Populate via one RAGStore instance, then delete the FTS db
    # file and re-open to simulate the upgrade path.
    persist = tmp_path / "chroma"
    store_a = RAGStore(
        persist_dir=str(persist),
        embedding_function=None,
        embedding_provider="minilm",
        embedding_model="minilm-l6-v2",
    )
    store_a.ingest([_make_doc(tmp_path, "Backfill smoke check body.", name="bf.txt")])
    chunks_before = store_a._fts.count()
    assert chunks_before > 0
    # Simulate pre-PR-E by removing the sidecar file.
    fts_file = persist / "docs_fts.sqlite"
    if fts_file.exists():
        fts_file.unlink()

    store_b = RAGStore(
        persist_dir=str(persist),
        embedding_function=None,
        embedding_provider="minilm",
        embedding_model="minilm-l6-v2",
    )
    # The backfill should have re-populated the FTS table.
    assert store_b._fts.count() == chunks_before
