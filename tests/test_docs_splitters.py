"""PR-D: format-dispatching splitter tests.

Pins three behaviours that the new ``amx.docs.splitters`` module
introduces:

1. ``get_splitter(extension)`` returns the Markdown-aware splitter
   for ``.md`` and ``.markdown`` and the original
   ``RecursiveCharacterTextSplitter`` for everything else. Unknown
   extensions fall back to the default rather than raising.
2. The Markdown splitter preserves ``h1`` / ``h2`` / ``h3`` heading
   paths on every output chunk's metadata, including chunks
   produced by the second-stage character splitter on long sections.
3. The end-to-end ``RAGStore.ingest`` path propagates the heading
   metadata onto the Chroma chunk metadata when the source is
   Markdown — that's the channel future PRs (PR-H assembly
   citation headers) consume.
"""

from __future__ import annotations

from pathlib import Path

from langchain_core.documents import Document
from langchain_text_splitters import RecursiveCharacterTextSplitter

from amx.docs.rag import RAGStore
from amx.docs.scanner import DocInfo
from amx.docs.splitters import _MarkdownAwareSplitter, get_splitter

# ── dispatcher routing ───────────────────────────────────────────────


def test_get_splitter_returns_markdown_aware_for_md() -> None:
    assert isinstance(get_splitter(".md"), _MarkdownAwareSplitter)


def test_get_splitter_returns_markdown_aware_for_markdown_long_form() -> None:
    assert isinstance(get_splitter(".markdown"), _MarkdownAwareSplitter)


def test_get_splitter_is_case_insensitive() -> None:
    assert isinstance(get_splitter(".MD"), _MarkdownAwareSplitter)


def test_get_splitter_returns_default_for_txt() -> None:
    assert isinstance(get_splitter(".txt"), RecursiveCharacterTextSplitter)


def test_get_splitter_returns_default_for_unknown_extension() -> None:
    """Unknown extensions fall back to the default rather than
    raising KeyError — keeps the dispatcher safe for arbitrary user
    input."""
    assert isinstance(get_splitter(".whatever"), RecursiveCharacterTextSplitter)


def test_get_splitter_handles_empty_extension() -> None:
    assert isinstance(get_splitter(""), RecursiveCharacterTextSplitter)


# ── Markdown header metadata preservation ────────────────────────────


def test_markdown_splitter_records_h1_h2_h3_on_chunks() -> None:
    """Each output chunk carries the heading path it lives under in
    its ``metadata`` dict."""
    body = """# Orders table

The orders table stores one row per order.

## Columns

- order_id
- customer_id
- total_amount

## Constraints

Total amount must be non-negative.

### Why it matters

Refunds depend on this invariant.
"""
    splitter = _MarkdownAwareSplitter()
    chunks = splitter.split_documents([Document(page_content=body, metadata={"source": "fake.md"})])
    assert len(chunks) >= 3, "expected one chunk per ##/### section"

    # Every chunk should have h1 set from the top-level heading.
    for chunk in chunks:
        assert chunk.metadata.get("h1") == "Orders table"

    # h2 should differ between Columns and Constraints sections.
    h2_values = {chunk.metadata.get("h2") for chunk in chunks if chunk.metadata.get("h2")}
    assert {"Columns", "Constraints"}.issubset(h2_values)

    # The "Why it matters" subsection (under ## Constraints) should
    # have its h3 recorded.
    h3_chunks = [c for c in chunks if c.metadata.get("h3")]
    assert any(c.metadata["h3"] == "Why it matters" for c in h3_chunks)


def test_markdown_splitter_preserves_source_metadata_from_loader() -> None:
    """The loader stamps ``source`` on the input Document; the
    splitter must not drop it on output."""
    body = "# Top\n\nbody\n"
    splitter = _MarkdownAwareSplitter()
    chunks = splitter.split_documents(
        [Document(page_content=body, metadata={"source": "/path/to/file.md"})]
    )
    for chunk in chunks:
        assert chunk.metadata.get("source") == "/path/to/file.md"


def test_markdown_splitter_chunks_long_section() -> None:
    """A single section longer than the chunk_size budget gets
    further split by the body splitter — header metadata propagates
    onto every sub-chunk."""
    long_body = "# Big section\n\n" + ("Lorem ipsum dolor sit amet. " * 200)
    splitter = _MarkdownAwareSplitter()
    chunks = splitter.split_documents([Document(page_content=long_body, metadata={})])
    assert len(chunks) >= 2, "long section should produce multiple sub-chunks"
    for chunk in chunks:
        assert chunk.metadata.get("h1") == "Big section"


def test_markdown_splitter_strips_no_headers() -> None:
    """The heading line itself stays in the chunk body so the LLM
    sees ``## Section\\n...`` rather than just the body. The
    structured metadata exists in parallel for citation use; the
    body keeps the human-readable structure."""
    body = "# Foo\n\nBody one.\n\n## Bar\n\nBody two.\n"
    splitter = _MarkdownAwareSplitter()
    chunks = splitter.split_documents([Document(page_content=body, metadata={})])
    joined = "\n".join(c.page_content for c in chunks)
    assert "# Foo" in joined
    assert "## Bar" in joined


# ── end-to-end RAGStore ingest preserves header metadata ─────────────


def test_ragstore_ingest_records_h2_on_chunk_metadata(tmp_path: Path) -> None:
    """When ``RAGStore.ingest`` is given a .md file, the resulting
    Chroma chunks carry h1/h2/h3 metadata in addition to the usual
    source / source_root / chunk_idx fields."""
    md_path = tmp_path / "fixture.md"
    md_path.write_text(
        "# Orders\n\n## total_amount\n\nThe `total_amount` column "
        "stores the sum of line items plus tax.\n",
        encoding="utf-8",
    )
    store = RAGStore(
        persist_dir=str(tmp_path / "chroma"),
        embedding_function=None,
        embedding_provider="minilm",
        embedding_model="minilm-l6-v2",
    )
    summary = store.ingest(
        [
            DocInfo(
                path=str(md_path),
                size_bytes=md_path.stat().st_size,
                extension=".md",
                source_type="local",
                source_root=str(tmp_path),
            )
        ]
    )
    assert summary.chunk_count > 0
    # Inspect the stored metadata directly.
    rows = store.collection.get(where={"source": str(md_path)}, include=["metadatas"])
    metas = rows.get("metadatas") or []
    assert metas, "ingest should have produced at least one chunk"
    found_h1 = any(m.get("h1") == "Orders" for m in metas)
    found_h2 = any(m.get("h2") == "total_amount" for m in metas)
    assert found_h1, f"expected at least one chunk with h1=Orders, got metas={metas}"
    assert found_h2, f"expected at least one chunk with h2=total_amount, got metas={metas}"


def test_ragstore_ingest_does_not_set_header_keys_for_txt(tmp_path: Path) -> None:
    """The .txt path uses the default RecursiveCharacterTextSplitter
    which doesn't produce header metadata. Header keys must be
    absent from those chunks' metadata so the eval baseline stays
    stable and downstream prompt assembly doesn't see stale h2/h3
    values."""
    txt_path = tmp_path / "plain.txt"
    txt_path.write_text("Some plain text without markdown structure.\n", encoding="utf-8")
    store = RAGStore(
        persist_dir=str(tmp_path / "chroma"),
        embedding_function=None,
        embedding_provider="minilm",
        embedding_model="minilm-l6-v2",
    )
    summary = store.ingest(
        [
            DocInfo(
                path=str(txt_path),
                size_bytes=txt_path.stat().st_size,
                extension=".txt",
                source_type="local",
                source_root=str(tmp_path),
            )
        ]
    )
    assert summary.chunk_count >= 1
    rows = store.collection.get(where={"source": str(txt_path)}, include=["metadatas"])
    metas = rows.get("metadatas") or []
    for meta in metas:
        assert "h1" not in meta
        assert "h2" not in meta
        assert "h3" not in meta
