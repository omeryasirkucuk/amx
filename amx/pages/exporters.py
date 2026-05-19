"""Markdown / PDF exporters for documentation pages.

Markdown export is identity; PDF goes via markdown-it-py for the
HTML render and xhtml2pdf for the binary output. xhtml2pdf is
pure-Python so it installs cleanly on Windows; WeasyPrint is parked
as a higher-fidelity follow-up.

The markdown parser starts from the CommonMark preset and explicitly
enables ``table`` and ``strikethrough`` — without ``table`` enabled,
pipe-tables emitted by the LLM render as raw inline text in the PDF
even though they look correct in Studio's preview (which uses a
GitHub-flavoured renderer).
"""

from __future__ import annotations

import base64
import io
from pathlib import Path

from markdown_it import MarkdownIt

_md = MarkdownIt("commonmark", {"html": False, "linkify": True, "typographer": True}).enable(
    ["table", "strikethrough"]
)

#: Path to the AMX logo embedded as a base64 data URI in the PDF
#: header. Resolved lazily so missing-asset deployments do not crash
#: page generation; the header simply renders without a logo.
_LOGO_PATH = Path(__file__).resolve().parent.parent / "web" / "static" / "amx-logo.png"

_PDF_CSS = """
/* Page 1 has a top margin of 0 so the cover banner sits flush
   against the physical top edge of the page. xhtml2pdf does not
   honour negative margins past @page boundaries, so we instead pull
   the banner up by zeroing the top margin and reintroducing the
   spacing via the banner's own padding. Subsequent pages would
   otherwise also start at the top edge — xhtml2pdf has no portable
   ":first" support — so the content wrapper carries a top padding
   on every page; on page 1 the banner sits above that padding, on
   pages 2+ the padding alone provides the breathing room. */
@page {
  size: A4;
  margin: 0 22mm 22mm 22mm;
}
body  { font-family: Helvetica, Arial, sans-serif; font-size: 11pt; line-height: 1.45; color: #111; margin: 0; padding: 0; }
h1    { font-size: 22pt; margin-top: 0; }
h2    { font-size: 16pt; margin-top: 18pt; }
h3    { font-size: 13pt; margin-top: 14pt; }
code  { font-family: "Courier New", monospace; background: #f4f4f4; padding: 1pt 3pt; }
pre   { background: #f4f4f4; padding: 8pt; }
table { border-collapse: collapse; width: 100%; }
th, td { border: 1px solid #ddd; padding: 4pt 6pt; }

.cover-banner {
  background: #f5f5f5;
  margin: 0 -22mm 6mm -22mm;
  padding: 4mm 22mm;
  text-align: center;
  border-bottom: 0.5pt solid #e5e5e5;
}
.cover-banner img { height: 6mm; }

/* Body content begins below the banner. Top padding gives the body
   space on every page; on page 1 the banner replaces some of that
   visual space, on pages 2+ it provides the standard top margin. */
.page-body { padding-top: 16mm; }
"""


def _logo_data_uri() -> str:
    """Return the AMX logo as a base64 PNG data URI, or empty string if
    the asset is missing (e.g. in slim runtime images that did not
    bundle the Studio static directory)."""
    try:
        raw = _LOGO_PATH.read_bytes()
    except OSError:
        return ""
    encoded = base64.b64encode(raw).decode("ascii")
    return f"data:image/png;base64,{encoded}"


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
    logo = _logo_data_uri()
    banner_html = f'<div class="cover-banner"><img src="{logo}" alt="AMX" /></div>' if logo else ""
    html = (
        "<html><head><style>"
        + _PDF_CSS
        + "</style></head><body>"
        + banner_html
        + '<div class="page-body">'
        + html_body
        + "</div></body></html>"
    )
    buf = io.BytesIO()
    pisa.CreatePDF(html, dest=buf, encoding="utf-8")
    return buf.getvalue()
