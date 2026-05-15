"""Format-aware splitter dispatch for Document RAG (PR-D).

The previous design at ``amx/docs/rag.py`` ran every document — PDF,
DOCX, Markdown, HTML, CSV, plain text, ``.py`` source — through a
single :class:`RecursiveCharacterTextSplitter` instance. That works
acceptably for prose but is structurally blind:

- Markdown is split mid-section, so ``## Section`` headers are lost
  from the chunks they govern. The LLM sees ``column X is the …``
  with no header to anchor *which* table the column lives in.
- CSV rows get chunked at random offsets, breaking cells in half.
- ``.py`` files go through a character splitter that knows nothing
  about function or class boundaries, even though
  :mod:`amx.codebase.code_rag` already has an AST-aware chunker that
  produces the right shape.

This module exposes :func:`get_splitter` — a tiny dispatcher keyed on
the file extension — and a Markdown-header-aware splitter that
preserves the section path (``h1`` / ``h2`` / ``h3``) in the chunk
metadata so the assembled prompt can cite \"orders.md → h2: total_amount\"
instead of just \"orders.md\". The default splitter for everything else
is the same recursive character splitter, preserved here so the eval
baseline is unaffected by this PR — only Markdown-typed documents see
a behavior change.

Token-counted budgets (audit F1.2), CSV row-group splitting (F1.5),
and AST routing for ``.py`` via ``/docs ingest`` (F1.1 follow-up) are
deferred to subsequent PRs; this one establishes the dispatcher
seam and adds the Markdown specialisation.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from langchain_core.documents import Document
from langchain_text_splitters import (
    MarkdownHeaderTextSplitter,
    RecursiveCharacterTextSplitter,
    TextSplitter,
)

# Mirror the previous hard-coded defaults so this dispatcher is a
# drop-in replacement for non-Markdown inputs. Bumped to its own
# constants here so a future PR-D-2 can move them into
# ``cfg.docs.chunking`` without touching every caller.
DEFAULT_CHUNK_SIZE_CHARS = 1000
DEFAULT_CHUNK_OVERLAP_CHARS = 200
DEFAULT_SEPARATORS = ["\n\n", "\n", ". ", " ", ""]

# Markdown sections frequently exceed 1000 chars, especially in the
# AMX corpus's schema docs (a single column entry can run several
# hundred words). After header-splitting we still need to chunk long
# sections so they fit a typical context window — pick the same
# size/overlap as the default splitter so chunk granularity is
# consistent across formats.
MARKDOWN_HEADERS_TO_SPLIT_ON = [
    ("#", "h1"),
    ("##", "h2"),
    ("###", "h3"),
]

# Extensions handled by the Markdown specialisation. The
# :class:`LOADER_MAP` in ``amx/docs/rag.py`` maps both ``.md`` and
# ``.markdown`` to ``UnstructuredMarkdownLoader``, which can either
# preserve or flatten heading markers depending on its parser
# strategy. The dispatcher operates AFTER the loader has run, so it
# sees whatever text the loader produced; for Markdown inputs we
# re-parse the headings ourselves because the loader's behaviour
# varies between versions.
_MARKDOWN_EXTENSIONS = {".md", ".markdown"}


def _make_default_splitter() -> RecursiveCharacterTextSplitter:
    """The exact splitter ``RAGStore`` used pre-PR-D. Preserved so
    every non-Markdown extension keeps the previous behaviour and the
    docs RAG eval baseline does not shift."""
    return RecursiveCharacterTextSplitter(
        chunk_size=DEFAULT_CHUNK_SIZE_CHARS,
        chunk_overlap=DEFAULT_CHUNK_OVERLAP_CHARS,
        separators=DEFAULT_SEPARATORS,
    )


class _MarkdownAwareSplitter(TextSplitter):
    """Two-stage Markdown splitter.

    Stage 1: :class:`MarkdownHeaderTextSplitter` cuts the document
    along ``#`` / ``##`` / ``###`` headers and records the heading
    path on each output document's metadata (``h1`` / ``h2`` / ``h3``
    keys).

    Stage 2: :class:`RecursiveCharacterTextSplitter` chunks each
    section to the configured ``chunk_size`` so long sections (a
    single ``##`` body that runs 3000 characters) don't produce one
    over-sized chunk that hogs the LLM context. The header metadata
    is propagated onto every sub-chunk via
    ``RecursiveCharacterTextSplitter.split_documents`` (LangChain's
    built-in metadata-preservation).

    Note: ``TextSplitter.split_documents`` is the only method
    ``RAGStore`` calls, so we implement that and let the inherited
    ``split_text`` raise via the abstract contract (we deliberately
    don't support raw-text input — the caller has a
    :class:`Document` because the loader already ran).
    """

    def __init__(
        self,
        *,
        chunk_size: int = DEFAULT_CHUNK_SIZE_CHARS,
        chunk_overlap: int = DEFAULT_CHUNK_OVERLAP_CHARS,
        separators: Sequence[str] | None = None,
    ) -> None:
        super().__init__(chunk_size=chunk_size, chunk_overlap=chunk_overlap)
        self._header_splitter = MarkdownHeaderTextSplitter(
            headers_to_split_on=MARKDOWN_HEADERS_TO_SPLIT_ON,
            # Preserve the heading line in the body of the section so
            # the LLM sees \"## Pricing\\n...\" rather than just the
            # body. The metadata still carries the heading separately
            # for citation use.
            strip_headers=False,
        )
        self._body_splitter = RecursiveCharacterTextSplitter(
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            separators=list(separators) if separators is not None else DEFAULT_SEPARATORS,
        )

    def split_text(self, text: str) -> list[str]:
        """Plain-text entry point — exists to satisfy the abstract
        base class. Splits as Markdown but discards the header
        metadata since ``str`` can't carry it. Callers that need the
        metadata should use ``split_documents`` with the loader's
        :class:`Document` output."""
        sections = self._header_splitter.split_text(text)
        out: list[str] = []
        for section in sections:
            out.extend(self._body_splitter.split_text(section.page_content))
        return out

    def split_documents(self, documents: list[Document]) -> list[Document]:
        """Dispatch-target for :meth:`RAGStore.ingest`.

        For each input :class:`Document`:

        1. Run the header splitter on the page content. The result is
           a list of :class:`Document` objects, each tagged with the
           heading path that introduced the section.
        2. Carry the input document's metadata onto every section
           (the loader recorded ``source``, ``page``, etc; we don't
           want to lose them).
        3. Run the body splitter to chop each tagged section into
           chunk-sized documents. The section's metadata is
           propagated to every sub-chunk by LangChain's built-in
           metadata-preservation.
        """
        out: list[Document] = []
        for doc in documents:
            section_docs = self._header_splitter.split_text(doc.page_content)
            for section in section_docs:
                merged_meta: dict[str, Any] = {**(doc.metadata or {}), **(section.metadata or {})}
                section.metadata = merged_meta
            out.extend(self._body_splitter.split_documents(section_docs))
        return out


def get_splitter(extension: str) -> TextSplitter:
    """Return the splitter that should chunk a document with the
    given file extension.

    The extension is matched case-insensitively. Unknown extensions
    fall back to the recursive-character default so callers handling
    arbitrary user input never see ``KeyError`` here.
    """
    ext = (extension or "").lower()
    if ext in _MARKDOWN_EXTENSIONS:
        return _MarkdownAwareSplitter()
    return _make_default_splitter()


__all__ = [
    "DEFAULT_CHUNK_OVERLAP_CHARS",
    "DEFAULT_CHUNK_SIZE_CHARS",
    "DEFAULT_SEPARATORS",
    "MARKDOWN_HEADERS_TO_SPLIT_ON",
    "get_splitter",
]
