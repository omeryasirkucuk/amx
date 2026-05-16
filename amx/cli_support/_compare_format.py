"""Pure formatting helpers used by ``amx compare`` rendering and export.

Extracted from :mod:`amx.cli_support.commands.compare` so the ten
stateless formatters live in a small focused module. They have no
dependency on the compare command's internal data structures, only on
stdlib + Rich's :class:`~rich.style.Style` typing. The compare module
re-exports each name so historical imports
(``from amx.cli_support.commands.compare import _fmt_dt`` etc.) keep
working unchanged.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

#: Rich-markup glyph emitted in front of each alternative description
#: when a confidence band is known. Lives here so both compare.py and
#: any future renderer can share the same glyph table without coupling
#: through compare's module-private scope.
_BAND_GLYPH = {
    "HIGH": "[green][H][/green]",
    "MED": "[yellow][M][/yellow]",
    "LOW": "[red][L][/red]",
}


def _band_prefix(alt_entry: Any) -> str:
    """Return a Rich-markup band glyph for one alternative entry.

    ``alt_entry`` is the parsed dict produced by
    :func:`amx.storage.sqlite_store.parse_alternatives_json`. Returns
    empty string when no band is available (legacy rows / confidence
    disabled / unknown band).
    """
    if not isinstance(alt_entry, dict):
        return ""
    band = alt_entry.get("band")
    if not band:
        return ""
    glyph = _BAND_GLYPH.get(band)
    return f"{glyph} " if glyph else ""


def _confidence_style(band: str) -> str:
    b = (band or "").lower()
    if b == "high":
        return "bold green"
    if b == "medium" or b == "med":
        return "yellow"
    if b == "low":
        return "red"
    return "dim"


def _fmt_dt(ts: float | None) -> str:
    if not ts:
        return "—"
    try:
        return datetime.fromtimestamp(float(ts)).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return "—"


def _fmt_duration(sec: float | None) -> str:
    if sec is None or sec <= 0:
        return "—"
    s = float(sec)
    if s < 60:
        return f"{s:.1f}s"
    m, rem = divmod(s, 60)
    return f"{int(m)}m {rem:0.0f}s"


def _fmt_float(n: Any, places: int = 2) -> str:
    try:
        return f"{float(n):.{places}f}"
    except Exception:
        return "—"


def _fmt_int(n: Any) -> str:
    try:
        return f"{int(n):,}"
    except Exception:
        return "—"


def _fmt_or_dash(val: Any) -> str:
    if val is None:
        return "—"
    s = str(val).strip()
    return s if s else "—"


def _md_escape(s: Any) -> str:
    text = "" if s is None else str(s)
    # Pipes break GFM tables; backslashes preserve them. Newlines collapse.
    return text.replace("|", "\\|").replace("\n", " ").strip()


def _md_table(headers: list[str], rows: list[list[Any]]) -> str:
    if not rows:
        return f"| {' | '.join(headers)} |\n| {' | '.join(['---'] * len(headers))} |\n| {' | '.join(['—'] * len(headers))} |\n"
    body_lines = [f"| {' | '.join(headers)} |", f"| {' | '.join(['---'] * len(headers))} |"]
    for row in rows:
        body_lines.append("| " + " | ".join(_md_escape(c) for c in row) + " |")
    return "\n".join(body_lines) + "\n"


def _truncate(text: str, max_len: int = 60) -> str:
    s = (text or "").strip()
    if len(s) <= max_len:
        return s
    return s[: max_len - 1].rstrip() + "…"
