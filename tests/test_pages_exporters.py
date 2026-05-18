"""Tests for the pages markdown / PDF exporters."""

from __future__ import annotations

from amx.pages.exporters import to_markdown, to_pdf


def test_to_markdown_is_identity() -> None:
    assert to_markdown("# Hi") == "# Hi"


def test_to_pdf_returns_bytes_starting_with_pdf_header() -> None:
    pdf = to_pdf("# Hello\n\nbody")
    assert pdf.startswith(b"%PDF-")
    assert len(pdf) > 200
