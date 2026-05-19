"""Tests for the pages markdown / PDF exporters."""

from __future__ import annotations

import pytest

from amx.pages.exporters import to_markdown, to_pdf


def test_to_markdown_is_identity() -> None:
    assert to_markdown("# Hi") == "# Hi"


def test_to_pdf_returns_bytes_starting_with_pdf_header() -> None:
    # PDF export is gated behind the optional ``[pages]`` extra
    # because ``xhtml2pdf`` transitively pulls ``svglib`` -> ``pycairo``,
    # which needs a Cairo system library that is not installed on the
    # GitHub Actions Linux runners. Skip when the dep isn't available
    # so the suite reflects runtime behaviour (the to_pdf call also
    # raises a clear RuntimeError pointing at the same install hint).
    pytest.importorskip(
        "xhtml2pdf",
        reason="xhtml2pdf is only installed with the [pages] extra",
    )
    pdf = to_pdf("# Hello\n\nbody")
    assert pdf.startswith(b"%PDF-")
    assert len(pdf) > 200
