"""Pin the per-file outcome contract of ``RAGStore.ingest``.

Before PR A, ``ingest`` returned a bare ``int`` (total chunk count)
and swallowed per-file failures into ``log.error``. A folder of 50
PDFs where 49 failed would surface as "Ingested 12 chunks" with no
per-file diagnostics.

The new contract returns an :class:`IngestSummary` dataclass carrying
``succeeded`` (paths), ``failed`` (path + short error message) and
``chunk_count`` (preserved for backwards compatibility). ``int(...)``
on the summary still yields the chunk count so the historic CLI/Studio
counter logic continues to work without per-call rewrites.
"""

from __future__ import annotations

from amx.docs.rag import IngestSummary, RAGStore
from amx.docs.scanner import DocInfo


def test_ingest_summary_is_int_compatible_for_legacy_callers() -> None:
    """``int(summary)`` returns the chunk count so CLI/Studio code
    that used to receive ``int`` doesn't have to change in lock-step
    with this contract upgrade."""
    summary = IngestSummary(succeeded=["a.md"], failed=[], chunk_count=7)
    assert int(summary) == 7
    assert summary.chunk_count == 7


def test_ingest_summary_carries_failed_files_with_reasons() -> None:
    summary = IngestSummary(
        succeeded=["good.md"],
        failed=[("bad.pdf", "PdfReadError: trailer not found")],
        chunk_count=3,
    )
    assert summary.failed == [("bad.pdf", "PdfReadError: trailer not found")]
    assert summary.succeeded == ["good.md"]


def test_ingest_returns_summary_with_unsupported_extension_in_failed(monkeypatch) -> None:
    """A file with an unknown extension lands in ``failed`` with a
    clear reason — not silently dropped on the floor."""
    store = RAGStore.__new__(RAGStore)
    # Stub out chroma so we never touch real state.
    store.source_filters = []

    class _FakeCollection:
        def upsert(self, **kw) -> None:
            pass

    store.collection = _FakeCollection()

    class _FakeSplitter:
        def split_documents(self, pages):
            return []

    store.splitter = _FakeSplitter()
    store._normalize_source_filter = lambda s: s  # type: ignore[assignment]

    doc = DocInfo(
        path="/tmp/whatever.unknownext",
        size_bytes=10,
        extension=".unknownext",
        source_type="local",
    )
    result = store.ingest([doc])
    assert isinstance(result, IngestSummary)
    assert result.succeeded == []
    assert len(result.failed) == 1
    assert result.failed[0][0] == "/tmp/whatever.unknownext"
    assert "loader" in result.failed[0][1].lower() or "unsupported" in result.failed[0][1].lower()
    assert result.chunk_count == 0
