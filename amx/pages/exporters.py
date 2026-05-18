"""Markdown / PDF exporters for documentation pages.

Markdown export is identity; PDF goes via markdown-it-py for the
HTML render and xhtml2pdf for the binary output. xhtml2pdf is
pure-Python so it installs cleanly on Windows; WeasyPrint is parked
as a higher-fidelity follow-up.
"""

from __future__ import annotations

import io

from markdown_it import MarkdownIt

_md = MarkdownIt("commonmark", {"html": False, "linkify": True, "typographer": True})

_PDF_CSS = """
@page { size: A4; margin: 22mm; }
body  { font-family: Helvetica, Arial, sans-serif; font-size: 11pt; line-height: 1.45; color: #111; }
h1    { font-size: 22pt; margin-top: 0; }
h2    { font-size: 16pt; margin-top: 18pt; }
h3    { font-size: 13pt; margin-top: 14pt; }
code  { font-family: "Courier New", monospace; background: #f4f4f4; padding: 1pt 3pt; }
pre   { background: #f4f4f4; padding: 8pt; }
table { border-collapse: collapse; width: 100%; }
th, td { border: 1px solid #ddd; padding: 4pt 6pt; }
"""


def to_markdown(markdown_body: str) -> str:
    return markdown_body


def to_pdf(markdown_body: str) -> bytes:
    # Deferred so a missing xhtml2pdf install does not block the
    # rest of the pages router from mounting at server startup.
    try:
        from xhtml2pdf import pisa
    except ImportError as exc:
        raise RuntimeError(
            "PDF export requires xhtml2pdf. Install it with: pip install xhtml2pdf"
        ) from exc

    html_body = _md.render(markdown_body)
    html = f"<html><head><style>{_PDF_CSS}</style></head><body>{html_body}</body></html>"
    buf = io.BytesIO()
    pisa.CreatePDF(html, dest=buf, encoding="utf-8")
    return buf.getvalue()
