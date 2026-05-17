"""DOT generation + ``dot`` subprocess for lineage diagrams.

The DOT builder is a pure function that converts an edge set + metadata
into a Graphviz DOT string — no I/O, fully testable. The render entry
point shells out to the ``dot`` binary (resolved via
:func:`shutil.which` so Windows / macOS / Linux work identically) to
turn the DOT into the requested image format.

Cross-platform contract:

* Binary discovery uses ``shutil.which`` only — no hard-coded paths.
* Subprocess invocation uses a list-form argv with ``shell=False`` —
  identical quoting behavior on Windows.
* Temp files live under :func:`tempfile.gettempdir` and are cleaned up
  in a ``finally`` block.
* Artifact open helper dispatches on :data:`sys.platform`:
  ``os.startfile`` on Windows, ``open`` on macOS, ``xdg-open`` on Linux.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from amx.lineage.types import ColumnRef, Edge

# Scale guardrails — referenced by the service layer.
SOFT_NODE_LIMIT = 200
HARD_NODE_LIMIT = 500
HARD_EDGE_LIMIT = 1000

SUPPORTED_FORMATS = ("svg", "png", "jpg")


class DotBinaryNotFound(RuntimeError):
    """Raised when ``shutil.which('dot')`` returns ``None``."""


@dataclass
class RenderInput:
    """Everything :func:`render_lineage_image` needs.

    ``described_entities`` is the set of entity FQNs that have a description —
    used to draw the ✓/◌ badge on each node. ``partial_warning`` is the
    footer banner string (empty when no partial-render banner is needed).
    """

    edges: list[Edge]
    anchor: ColumnRef
    described_entities: set[str]
    title: str = ""
    partial_warning: str = ""


def build_dot(payload: RenderInput) -> str:
    """Produce a Graphviz DOT string for the given edges + metadata.

    Side-effect free: no DB, no filesystem. Safe to call from tests.
    """
    nodes: dict[str, dict[str, Any]] = {}
    anchor_id = _node_id(payload.anchor)
    nodes[anchor_id] = {
        "label": _node_label(
            payload.anchor, anchor=True, described=anchor_id in payload.described_entities
        ),
        "anchor": True,
    }

    edge_lines: list[str] = []
    for edge in payload.edges:
        src_id = _node_id(edge.source)
        tgt_id = _node_id(edge.target)
        nodes.setdefault(
            src_id,
            {
                "label": _node_label(
                    edge.source, anchor=False, described=src_id in payload.described_entities
                )
            },
        )
        nodes.setdefault(
            tgt_id,
            {
                "label": _node_label(
                    edge.target, anchor=False, described=tgt_id in payload.described_entities
                )
            },
        )
        edge_lines.append(_edge_line(src_id, tgt_id, edge))

    node_lines = [_node_line(node_id, attrs) for node_id, attrs in nodes.items()]

    title_block = ""
    if payload.title:
        title_block = (
            f'  labelloc="t";\n  label={_dot_quote(payload.title)};\n  fontname="Helvetica";\n'
        )

    footer_block = ""
    if payload.partial_warning:
        # Render the warning as an isolated "banner" node with a soft fill.
        footer_block = (
            "  __partial__ [shape=note, fontsize=10, "
            'style="filled", fillcolor="#fff3cd", color="#856404", '
            f"label={_dot_quote(payload.partial_warning)}];\n"
        )

    return (
        "digraph lineage {\n"
        '  rankdir="LR";\n'
        '  node [shape=box, style="rounded,filled", fillcolor="#f8f9fa", '
        'fontname="Helvetica", fontsize=11];\n'
        '  edge [fontname="Helvetica", fontsize=9];\n'
        f"{title_block}" + "".join(node_lines) + "".join(edge_lines) + footer_block + "}\n"
    )


def render_lineage_image(
    *,
    payload: RenderInput,
    fmt: str,
    output_path: Path,
) -> Path:
    """Convert ``payload`` to an image at ``output_path`` via ``dot``.

    Returns the resolved output path. Raises :class:`DotBinaryNotFound`
    when ``dot`` is not on PATH (callers surface an install hint).
    """
    fmt = fmt.lower()
    if fmt not in SUPPORTED_FORMATS:
        raise ValueError(f"unsupported format {fmt!r}; expected one of {SUPPORTED_FORMATS}")

    dot_binary = shutil.which("dot")
    if not dot_binary:
        raise DotBinaryNotFound(
            "graphviz `dot` is required. Install it: "
            "macOS `brew install graphviz`; "
            "Debian/Ubuntu `apt install graphviz`; "
            "Windows `winget install graphviz` or download from https://graphviz.org/."
        )

    output_path = Path(output_path).expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    dot_text = build_dot(payload)
    tmpdir = Path(tempfile.gettempdir())
    dot_file = tmpdir / f"amx-lineage-{os.getpid()}-{output_path.stem}.dot"
    try:
        dot_file.write_text(dot_text, encoding="utf-8")
        cmd = [dot_binary, f"-T{fmt}", str(dot_file), "-o", str(output_path)]
        subprocess.run(cmd, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(
            f"graphviz failed (exit {exc.returncode}): {exc.stderr.strip() or 'no stderr'}"
        ) from exc
    finally:
        try:
            dot_file.unlink(missing_ok=True)
        except OSError:
            pass
    return output_path


def open_artifact(path: Path) -> bool:
    """Cross-platform "open this file in the default viewer".

    Returns ``True`` when the platform dispatch fired without raising,
    ``False`` when no display is available (headless CI) so callers can
    fall back to printing the absolute path.
    """
    path = Path(path)
    plat = sys.platform
    try:
        if plat == "win32":
            os.startfile(str(path))  # type: ignore[attr-defined]
            return True
        if plat == "darwin":
            subprocess.run(["open", str(path)], check=False)
            return True
        if not os.environ.get("DISPLAY") and not os.environ.get("WAYLAND_DISPLAY"):
            return False
        subprocess.run(["xdg-open", str(path)], check=False)
        return True
    except Exception:
        return False


def count_nodes(edges: Iterable[Edge], anchor: ColumnRef) -> int:
    """Count distinct nodes that would appear in a render of ``edges`` + ``anchor``."""
    ids = {_node_id(anchor)}
    for edge in edges:
        ids.add(_node_id(edge.source))
        ids.add(_node_id(edge.target))
    return len(ids)


# ── internal helpers ─────────────────────────────────────────────────────


def _node_id(ref: ColumnRef) -> str:
    return ".".join(p for p in (ref.database, ref.schema, ref.table, ref.column) if p)


def _node_label(ref: ColumnRef, *, anchor: bool, described: bool) -> str:
    badge = "✓" if described else "○"  # ✓ vs ○ (ASCII-safe ring instead of ◌)
    main = ".".join(p for p in (ref.schema, ref.table) if p)
    if ref.column:
        main = f"{main}.{ref.column}" if main else ref.column
    if anchor:
        return f"★ {badge} {main}"
    return f"{badge} {main}"


def _node_line(node_id: str, attrs: dict[str, Any]) -> str:
    label = attrs["label"]
    extras = []
    if attrs.get("anchor"):
        extras.append("penwidth=2.4")
        extras.append('fillcolor="#fff4e6"')
        extras.append('color="#d97706"')
    extras_text = (", " + ", ".join(extras)) if extras else ""
    return f"  {_dot_id(node_id)} [label={_dot_quote(label)}{extras_text}];\n"


def _edge_line(src_id: str, tgt_id: str, edge: Edge) -> str:
    if edge.relationship_type == "lineage_name_match":
        style = ', style="dashed", color="#9ca3af"'
        label_tag = "≈name"  # ≈name
    elif edge.relationship_type == "lineage_fk":
        style = ', color="#1f2937"'
        label_tag = "fk"
    elif edge.relationship_type == "lineage_view_ddl":
        style = ', color="#1f2937"'
        label_tag = "view"
    else:
        style = ""
        label_tag = edge.extractor
    return f"  {_dot_id(src_id)} -> {_dot_id(tgt_id)} [label={_dot_quote(label_tag)}{style}];\n"


def _dot_id(node_id: str) -> str:
    """Quote any non-trivial identifier."""
    return _dot_quote(node_id)


def _dot_quote(text: str) -> str:
    escaped = text.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


__all__ = [
    "DotBinaryNotFound",
    "RenderInput",
    "SOFT_NODE_LIMIT",
    "HARD_NODE_LIMIT",
    "HARD_EDGE_LIMIT",
    "SUPPORTED_FORMATS",
    "build_dot",
    "render_lineage_image",
    "open_artifact",
    "count_nodes",
]
