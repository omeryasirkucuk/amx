"""PR-I: Maximal Marginal Relevance (diversity) tests.

Pure-math tests for the new ``maximal_marginal_relevance`` helper in
``amx.rag_core.fusion``, plus a smoke test verifying that
``RAGStore.query`` honours its ``use_mmr=True`` default by fetching
embeddings and applying MMR after rerank.

The end-to-end retrieval-quality impact is captured by the
docs-RAG eval baseline (``tests/eval/baselines/docs_baseline.json``);
this file pins the algorithmic contract so a future refactor (PR-J
shared-core extraction) catches accidental drift.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from amx.docs.rag import RAGStore
from amx.docs.scanner import DocInfo
from amx.rag_core.fusion import maximal_marginal_relevance

# ── pure MMR math ────────────────────────────────────────────────────


def test_mmr_first_pick_is_pure_relevance() -> None:
    """With no prior selection there's nothing to diversify
    against, so MMR's first pick is the highest-relevance
    candidate regardless of lambda."""
    cands = [
        ("low", 0.1, [1.0, 0.0]),
        ("high", 0.9, [0.0, 1.0]),
        ("mid", 0.5, [0.5, 0.5]),
    ]
    picked = maximal_marginal_relevance(candidates=cands, k=1, lambda_=0.5)
    assert picked == ["high"]


def test_mmr_demotes_near_duplicates() -> None:
    """When λ=0.7 (default), MMR should pick two diverse vectors
    over two similar high-relevance vectors."""
    cands = [
        ("c1", 0.9, [1.0, 0.0]),
        ("c2", 0.85, [0.99, 0.05]),  # near-dup of c1
        ("c3", 0.7, [0.0, 1.0]),  # orthogonal — diverse
    ]
    picked = maximal_marginal_relevance(candidates=cands, k=2, lambda_=0.7)
    assert picked == ["c1", "c3"]


def test_mmr_lambda_one_is_pure_relevance() -> None:
    """``lambda_=1.0`` weights diversity at zero — MMR degenerates
    to the relevance ordering."""
    cands = [
        ("c1", 0.9, [1.0, 0.0]),
        ("c2", 0.85, [0.99, 0.05]),  # near-dup but high relevance
        ("c3", 0.7, [0.0, 1.0]),
    ]
    picked = maximal_marginal_relevance(candidates=cands, k=2, lambda_=1.0)
    assert picked == ["c1", "c2"]


def test_mmr_lambda_zero_is_pure_diversity_after_first_pick() -> None:
    """``lambda_=0.0`` weights relevance at zero. First pick is
    still the most relevant (by tie-break), but subsequent picks
    maximise distance from already-picked candidates."""
    cands = [
        ("c1", 0.9, [1.0, 0.0]),
        ("c2", 0.85, [0.99, 0.05]),  # near-dup of c1
        ("c3", 0.1, [0.0, 1.0]),  # orthogonal, low relevance
    ]
    picked = maximal_marginal_relevance(candidates=cands, k=2, lambda_=0.0)
    # First pick = c1 (highest relevance). Second pick should be
    # the most distant from c1 — that's c3 (orthogonal), not c2
    # (near-duplicate of c1).
    assert picked == ["c1", "c3"]


def test_mmr_empty_input_returns_empty_list() -> None:
    assert maximal_marginal_relevance(candidates=[], k=5) == []


def test_mmr_k_zero_returns_empty_list() -> None:
    cands = [("c1", 0.9, [1.0, 0.0])]
    assert maximal_marginal_relevance(candidates=cands, k=0) == []


def test_mmr_k_larger_than_pool_returns_full_pool() -> None:
    """``k`` greater than the candidate pool size reorders the
    whole pool in MMR-pick order without raising."""
    cands = [
        ("c1", 0.9, [1.0, 0.0]),
        ("c2", 0.5, [0.0, 1.0]),
    ]
    picked = maximal_marginal_relevance(candidates=cands, k=10, lambda_=0.7)
    assert set(picked) == {"c1", "c2"}
    assert len(picked) == 2


def test_mmr_rejects_lambda_outside_zero_one() -> None:
    cands = [("c1", 0.9, [1.0, 0.0])]
    with pytest.raises(ValueError):
        maximal_marginal_relevance(candidates=cands, k=1, lambda_=-0.1)
    with pytest.raises(ValueError):
        maximal_marginal_relevance(candidates=cands, k=1, lambda_=1.5)


def test_mmr_handles_zero_vectors() -> None:
    """Degenerate embeddings (all zeros) get a similarity of 0 against
    everything — they don't crash MMR. Relevance still drives the
    ordering."""
    cands = [
        ("zero_a", 0.5, [0.0, 0.0, 0.0]),
        ("zero_b", 0.9, [0.0, 0.0, 0.0]),
        ("normal", 0.3, [1.0, 0.0, 0.0]),
    ]
    picked = maximal_marginal_relevance(candidates=cands, k=2, lambda_=0.7)
    # First pick: zero_b (highest relevance).
    # Second pick: zero_a vs normal. Zero similarity to zero_b
    # for both, so relevance wins → zero_a (0.5) > normal (0.3).
    assert picked[0] == "zero_b"
    assert picked[1] == "zero_a"


def test_mmr_preserves_ids_as_strings() -> None:
    """Output ids are the same string identity as input. No
    canonicalisation, no surprise type coercion."""
    cands = [
        ("path/with/slash::5", 0.9, [1.0, 0.0]),
        ("path/with/slash::6", 0.5, [0.0, 1.0]),
    ]
    picked = maximal_marginal_relevance(candidates=cands, k=2)
    assert picked == ["path/with/slash::5", "path/with/slash::6"]


# ── end-to-end RAGStore wire-up ──────────────────────────────────────


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


def test_ragstore_query_default_uses_mmr(tmp_path: Path) -> None:
    """Default ``use_mmr=True`` runs MMR over the reranked hits;
    setting ``use_mmr=False`` skips it. Smoke check that both
    paths return a non-empty result on a real ingest."""
    store = RAGStore(
        persist_dir=str(tmp_path / "chroma"),
        embedding_function=None,
        embedding_provider="minilm",
        embedding_model="minilm-l6-v2",
    )
    docs = [
        _make_doc(tmp_path, "Orders table holds customer purchases.", name="o.txt"),
        _make_doc(tmp_path, "Customers table holds buyer details.", name="c.txt"),
        _make_doc(tmp_path, "Products table holds catalogue entries.", name="p.txt"),
    ]
    store.ingest(docs)
    hits_mmr = store.query("customer purchases", n_results=3)
    hits_no_mmr = store.query("customer purchases", n_results=3, use_mmr=False)
    assert hits_mmr, "MMR-on path should return hits"
    assert hits_no_mmr, "MMR-off path should return hits"
    # Both paths should surface at least the orders chunk.
    src_mmr = {Path(h["metadata"].get("source", "")).name for h in hits_mmr}
    src_no_mmr = {Path(h["metadata"].get("source", "")).name for h in hits_no_mmr}
    assert "o.txt" in src_mmr
    assert "o.txt" in src_no_mmr


def test_ragstore_mmr_diversifies_near_duplicate_chunks(tmp_path: Path) -> None:
    """When several chunks of the SAME document semantically match
    the query, MMR should pick at most one of them and prefer a
    chunk from a different document when one's available with
    comparable relevance.

    The fixture: orders.txt has two paragraphs about customer
    purchases; customers.txt has one paragraph mentioning
    customer purchases too. With three documents producing four
    candidate chunks total (orders has two chunks, the other two
    one each), MMR should surface orders.txt AND customers.txt at
    the top — not two orders chunks in a row.
    """
    paragraph_a = (
        "Customer purchases are recorded as orders. Each order links a customer "
        "to one or more product variants and computes a total amount. " * 4
    )
    paragraph_b = (
        "Order totals include line items, tax, and adjustments. Refunds are "
        "tracked separately and never mutate the original order row. " * 4
    )
    orders_body = paragraph_a + "\n\n---\n\n" + paragraph_b
    customers_body = (
        "Customer profiles capture identity for purchase orders. Buyers "
        "with consistent purchase patterns receive loyalty perks. " * 4
    )
    store = RAGStore(
        persist_dir=str(tmp_path / "chroma"),
        embedding_function=None,
        embedding_provider="minilm",
        embedding_model="minilm-l6-v2",
    )
    docs = [
        _make_doc(tmp_path, orders_body, name="orders.txt"),
        _make_doc(tmp_path, customers_body, name="customers.txt"),
    ]
    store.ingest(docs)

    hits_no_mmr = store.query("customer purchases", n_results=3, use_mmr=False)
    hits_mmr = store.query("customer purchases", n_results=3, use_mmr=True)

    sources_no_mmr = [Path(h["metadata"].get("source", "")).name for h in hits_no_mmr]
    sources_mmr = [Path(h["metadata"].get("source", "")).name for h in hits_mmr]

    # The without-MMR path may concentrate on orders.txt because
    # both its chunks score highly. The with-MMR path must
    # surface customers.txt as well — verifying diversity has the
    # intended effect on at least one of the top-3.
    assert "customers.txt" in sources_mmr, (
        f"MMR should surface customers.txt; got sources_mmr={sources_mmr}, "
        f"sources_no_mmr={sources_no_mmr}"
    )


def test_ragstore_mmr_handles_empty_embedding_fetch(tmp_path: Path, monkeypatch) -> None:
    """When Chroma's ``get(ids=..., include=['embeddings'])`` returns
    no usable rows (mismatched ids, indexing race), MMR degrades
    gracefully to the rerank order rather than dropping hits."""
    store = RAGStore(
        persist_dir=str(tmp_path / "chroma"),
        embedding_function=None,
        embedding_provider="minilm",
        embedding_model="minilm-l6-v2",
    )
    store.ingest([_make_doc(tmp_path, "Some content.", name="a.txt")])

    original_get = store.collection.get

    def _broken_get(*args, **kwargs):
        # Simulate Chroma returning nothing for the embedding fetch.
        if kwargs.get("include") == ["embeddings"]:
            return {"ids": [], "embeddings": []}
        return original_get(*args, **kwargs)

    monkeypatch.setattr(store.collection, "get", _broken_get)
    hits = store.query("some content", n_results=3)
    assert hits, "fallback to rerank order should preserve hits"
