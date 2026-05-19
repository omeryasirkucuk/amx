"""Excel (.xlsx) loader used by the docs scanner.

Each worksheet becomes one markdown section: the sheet name is the
H2 heading, and the cells are emitted as a GitHub-style markdown
table. Empty sheets are skipped. Cell values are coerced with the
sheet's number format when present; long cells are truncated to
keep the LLM prompt budget under control.
"""

from __future__ import annotations

from pathlib import Path

import openpyxl

MAX_CELL_LEN = 4096


def _coerce(value: object) -> str:
    if value is None:
        return ""
    s = str(value)
    if len(s) > MAX_CELL_LEN:
        return s[: MAX_CELL_LEN - 1] + "…"
    return s


def load_xlsx(path: str | Path) -> str:
    wb = openpyxl.load_workbook(Path(path), data_only=True, read_only=True)
    sections: list[str] = []
    for sheet in wb.worksheets:
        rows = list(sheet.iter_rows(values_only=True))
        if not rows:
            continue
        header = [_coerce(c) for c in rows[0]]
        sections.append(f"## {sheet.title}\n")
        sections.append("| " + " | ".join(header) + " |")
        sections.append("| " + " | ".join("---" for _ in header) + " |")
        for row in rows[1:]:
            cells = [_coerce(c) for c in row]
            sections.append("| " + " | ".join(cells) + " |")
        sections.append("")
    return "\n".join(sections)
