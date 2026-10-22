"""Word (.docx) loader used by the docs scanner.

Reads the document with ``python-docx`` (a declared ``[docs]``
dependency) and emits markdown: heading paragraphs become ``#``
headings, body paragraphs stay as text, and each table becomes a
GitHub-style markdown table, all in document order. Langchain's
``Docx2txtLoader`` was used before, but it needs ``docx2txt``, which
AMX does not install, so every .docx failed at ingest time.
"""

from __future__ import annotations

from pathlib import Path

import docx
from docx.table import Table
from docx.text.paragraph import Paragraph

MAX_CELL_LEN = 4096


def _cell(text: str) -> str:
    s = " ".join(text.split()).replace("|", "\\|")
    return s if len(s) <= MAX_CELL_LEN else s[: MAX_CELL_LEN - 1] + "…"


def _paragraph(p: Paragraph) -> str:
    text = p.text.strip()
    if not text:
        return ""
    style = (p.style.name if p.style is not None else "") or ""
    if style == "Title":
        return f"# {text}"
    if style.startswith("Heading "):
        level = style.removeprefix("Heading ").strip()
        depth = int(level) if level.isdigit() else 1
        return f"{'#' * min(max(depth, 1), 6)} {text}"
    return text


def _table(t: Table) -> str:
    rows = [[_cell(c.text) for c in row.cells] for row in t.rows]
    if not rows:
        return ""
    lines = ["| " + " | ".join(rows[0]) + " |", "| " + " | ".join("---" for _ in rows[0]) + " |"]
    lines += ["| " + " | ".join(r) + " |" for r in rows[1:]]
    return "\n".join(lines)


def load_docx(path: str | Path) -> str:
    document = docx.Document(str(path))
    blocks: list[str] = []
    for item in document.iter_inner_content():
        block = _paragraph(item) if isinstance(item, Paragraph) else _table(item)
        if block:
            blocks.append(block)
    return "\n\n".join(blocks)
