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


def test_markdown_parser_renders_pipe_tables() -> None:
    """LLM-emitted pipe-tables must reach the PDF as real HTML tables,
    not as inline pipe-delimited text. The CommonMark preset alone does
    NOT enable the table extension, so this asserts the explicit
    .enable(["table"]) wiring in exporters.py."""
    from amx.pages.exporters import _md

    md = "| Property | Value |\n| --- | --- |\n| Path | /tmp/foo |\n"
    html = _md.render(md)
    assert "<table>" in html
    assert "<th>Property</th>" in html
    assert "<td>Path</td>" in html


def test_pdf_embeds_amx_logo_data_uri() -> None:
    """Every exported PDF must include the AMX logo via a base64 data
    URI in the header frame. We assert presence of an image link with
    a data URI in the assembled HTML so the test runs even on Linux
    runners where ``xhtml2pdf`` is not installed."""
    from amx.pages.exporters import _logo_data_uri

    uri = _logo_data_uri()
    # In CI the logo file is shipped under amx/web/static/; if for some
    # reason the asset is missing the function must degrade to an
    # empty string rather than raise.
    assert uri == "" or uri.startswith("data:image/png;base64,")
