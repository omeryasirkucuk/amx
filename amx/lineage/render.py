"""Lineage diagram rendering — pip-installable, no system binaries.

The image render path uses ``networkx`` (graph + layout) and
``matplotlib`` (rasterisation). Both ship as pip wheels on every
supported platform, so the user never has to install Graphviz,
Cairo, or any other OS package. AMX auto-installs the bundle via
:func:`amx.utils.optional_deps.ensure("lineage")` on first render.

A pure-string ``build_dot`` helper is kept for tests, debugging, and
paste-into-graphviz.org workflows — but the user-facing image output
does not depend on a ``dot`` binary anywhere.
"""

from __future__ import annotations

import math
import os
import subprocess
import sys
from collections import defaultdict, deque
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
    """Retained for backwards-compatibility; pip-only render never raises this."""


@dataclass
class RenderInput:
    """Everything :func:`render_lineage_image` needs.

    ``described_entities`` is the set of entity FQNs that have a description —
    used to draw the ✓/○ badge on each node. ``partial_warning`` is the
    footer banner string (empty when no partial-render banner is needed).
    """

    edges: list[Edge]
    anchor: ColumnRef
    described_entities: set[str]
    title: str = ""
    partial_warning: str = ""


def render_lineage_image(
    *,
    payload: RenderInput,
    fmt: str,
    output_path: Path,
) -> Path:
    """Rasterise ``payload`` to ``output_path`` via matplotlib + networkx.

    ``fmt`` must be one of :data:`SUPPORTED_FORMATS`. Auto-installs the
    ``lineage`` optional extra (``matplotlib`` + ``networkx`` + ``sqlglot``)
    on first call so the only thing the user has to do is run
    ``/lineage create``.
    """
    fmt = fmt.lower()
    if fmt not in SUPPORTED_FORMATS:
        raise ValueError(f"unsupported format {fmt!r}; expected one of {SUPPORTED_FORMATS}")
    nx, plt = _load_render_stack()

    output_path = Path(output_path).expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    graph, anchor_id, node_attrs, edge_attrs = _build_graph(payload, nx)
    positions = _hierarchical_layout(graph, anchor_id, nx)

    fig, ax = plt.subplots(figsize=_figure_size(len(graph)))
    ax.set_axis_off()
    _set_axis_limits(ax, positions)
    _draw_nodes(ax, positions, node_attrs)
    _draw_edges(ax, positions, edge_attrs)
    if len(graph) == 1 and anchor_id in positions:
        _draw_empty_result_caption(ax, positions[anchor_id])
    if payload.title:
        fig.suptitle(payload.title, fontsize=11, x=0.04, ha="left", y=0.96)
    if payload.partial_warning:
        fig.text(
            0.5,
            0.02,
            payload.partial_warning,
            ha="center",
            fontsize=9,
            color="#856404",
            bbox={
                "boxstyle": "round,pad=0.4",
                "facecolor": "#fff3cd",
                "edgecolor": "#856404",
            },
        )

    # Explicit margins instead of tight_layout: tight_layout fights
    # ax.set_axis_off() on near-empty axes and prints a UserWarning when
    # it gives up. Hard-coded margins also keep the canvas size stable
    # across renders of the same artifact slot.
    fig.subplots_adjust(left=0.04, right=0.96, top=0.92, bottom=0.10)
    fig.savefig(output_path, format=fmt, dpi=160)
    plt.close(fig)
    return output_path


def build_dot(payload: RenderInput) -> str:
    """Pure-string Graphviz DOT representation of ``payload``.

    The user-facing render path no longer needs the ``dot`` binary, but
    this helper is kept so callers can serialise a graph for tests,
    debugging, or paste-into-graphviz.org workflows.
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


# ── internals: render stack loader ───────────────────────────────────────


def _load_render_stack() -> tuple[Any, Any]:
    """Return ``(networkx, matplotlib.pyplot)``. Auto-install on first call.

    The ``lineage`` bundle in :mod:`amx.utils.optional_deps` carries
    ``networkx`` and ``matplotlib`` so users never have to run pip by
    hand. If install fails (offline, locked corp env), the underlying
    ``ImportError`` propagates with the pip command the user can run.
    """
    try:
        import networkx as nx  # type: ignore
    except ImportError:
        from amx.utils.optional_deps import ensure

        ensure("lineage", feature="/lineage diagram renderer")
        import networkx as nx  # type: ignore

    try:
        import matplotlib

        matplotlib.use("Agg", force=True)
        import matplotlib.pyplot as plt
    except ImportError:
        from amx.utils.optional_deps import ensure

        ensure("lineage", feature="/lineage diagram renderer")
        import matplotlib

        matplotlib.use("Agg", force=True)
        import matplotlib.pyplot as plt

    # Keep SVG text as literal strings instead of glyph paths so the
    # output is searchable / accessible / smaller. Without this every
    # character becomes an opaque <use xlink:href="#DejaVuSans-XX">,
    # which defeats Ctrl-F in any viewer.
    matplotlib.rcParams["svg.fonttype"] = "none"

    return nx, plt


# ── internals: graph build + layout ──────────────────────────────────────


def _build_graph(
    payload: RenderInput, nx: Any
) -> tuple[Any, str, dict[str, dict[str, Any]], list[dict[str, Any]]]:
    """Build a networkx DiGraph and attribute maps for nodes + edges."""
    graph = nx.DiGraph()
    anchor_id = _node_id(payload.anchor)
    node_attrs: dict[str, dict[str, Any]] = {}

    def _add_node(ref: ColumnRef, *, anchor: bool) -> str:
        node_id = _node_id(ref)
        if node_id not in node_attrs:
            node_attrs[node_id] = {
                "label": _node_label(
                    ref, anchor=anchor, described=node_id in payload.described_entities
                ),
                "anchor": anchor,
            }
            graph.add_node(node_id)
        elif anchor:
            node_attrs[node_id]["anchor"] = True
            node_attrs[node_id]["label"] = _node_label(
                ref, anchor=True, described=node_id in payload.described_entities
            )
        return node_id

    _add_node(payload.anchor, anchor=True)

    edge_attrs: list[dict[str, Any]] = []
    for edge in payload.edges:
        src_id = _add_node(edge.source, anchor=False)
        tgt_id = _add_node(edge.target, anchor=False)
        graph.add_edge(src_id, tgt_id)
        edge_attrs.append(
            {
                "src": src_id,
                "tgt": tgt_id,
                "style": _edge_style(edge),
                "label": _edge_label(edge),
            }
        )

    return graph, anchor_id, node_attrs, edge_attrs


def _hierarchical_layout(graph: Any, anchor_id: str, nx: Any) -> dict[str, tuple[float, float]]:
    """Left-right layout: upstream on the left, downstream on the right.

    Walks the graph with BFS from the anchor along incoming and outgoing
    edges separately to assign layer indices. Nodes sharing a layer are
    spread evenly along the Y axis. Falls back to ``nx.spring_layout``
    when the anchor sits in a disconnected node set we cannot layer.
    """
    if anchor_id not in graph:
        return nx.spring_layout(graph, seed=7)

    layers: dict[str, int] = {anchor_id: 0}

    queue: deque[tuple[str, int]] = deque([(anchor_id, 0)])
    while queue:
        node, depth = queue.popleft()
        for pred in graph.predecessors(node):
            if pred not in layers:
                layers[pred] = depth - 1
                queue.append((pred, depth - 1))

    queue = deque([(anchor_id, 0)])
    while queue:
        node, depth = queue.popleft()
        for succ in graph.successors(node):
            if succ not in layers:
                layers[succ] = depth + 1
                queue.append((succ, depth + 1))

    for node in graph.nodes:
        layers.setdefault(node, 0)

    by_layer: dict[int, list[str]] = defaultdict(list)
    for node, layer in layers.items():
        by_layer[layer].append(node)

    pos: dict[str, tuple[float, float]] = {}
    x_spacing = 3.0
    y_spacing = 1.4
    for layer, nodes in by_layer.items():
        x = layer * x_spacing
        nodes_sorted = sorted(nodes)
        n = len(nodes_sorted)
        for i, node in enumerate(nodes_sorted):
            y = (i - (n - 1) / 2.0) * y_spacing
            pos[node] = (x, y)
    return pos


def _figure_size(node_count: int) -> tuple[float, float]:
    """Figure size that grows with node count without wasting canvas on tiny graphs.

    The previous implementation started at 10×6 inches regardless of
    content; that made the single-node case look like an empty page
    with a tiny anchor marooned in one corner. The curve below starts
    at 6×4 for the anchor-only case, hits ~7×5 at ten nodes, ~9×6 at a
    hundred, and ~11×7 at five hundred — enough room as the graph
    grows, no waste when it doesn't.
    """
    base_w, base_h = 6.0, 4.0
    if node_count <= 1:
        return base_w, base_h
    extra = math.log2(max(node_count, 2))
    return base_w + extra * 0.9, base_h + extra * 0.55


def _set_axis_limits(
    ax: Any,
    positions: dict[str, tuple[float, float]],
    *,
    h_pad: float = 2.0,
    v_pad: float = 1.2,
) -> None:
    """Set xlim/ylim from the node bounding box so the anchor lands centred.

    Without an explicit limit matplotlib autoscales to the data
    extents, which for a single point at (0, 0) gives a degenerate
    view that ``bbox_inches="tight"`` then crops into the bottom-left
    corner. Symmetric padding around the actual content keeps the
    anchor visually centred even when the BFS-derived layers are
    asymmetric (e.g. anchor + one upstream + zero downstream).
    """
    if not positions:
        ax.set_xlim(-1, 1)
        ax.set_ylim(-1, 1)
        return
    xs = [p[0] for p in positions.values()]
    ys = [p[1] for p in positions.values()]
    x_min, x_max = min(xs), max(xs)
    y_min, y_max = min(ys), max(ys)
    if x_min == x_max:
        ax.set_xlim(x_min - h_pad, x_max + h_pad)
    else:
        ax.set_xlim(x_min - h_pad * 0.5, x_max + h_pad * 0.5)
    if y_min == y_max:
        ax.set_ylim(y_min - v_pad, y_max + v_pad)
    else:
        ax.set_ylim(y_min - v_pad * 0.5, y_max + v_pad * 0.5)


def _draw_empty_result_caption(ax: Any, anchor_pos: tuple[float, float]) -> None:
    """Render the 'no related entities found' caption under the anchor.

    Replaces what would otherwise be a blank canvas when every
    extractor returns an empty edge set. The caption tells the user
    *what happened* and *what to do next* instead of leaving them
    staring at a single tiny box in the corner.
    """
    ax_x, ax_y = anchor_pos
    ax.text(
        ax_x,
        ax_y - 1.2,
        "No related entities found in cache.\n"
        "Try /lineage refresh --no-cache to pull view DDL and FK relationships.",
        ha="center",
        va="top",
        fontsize=9,
        color="#6b7280",
        wrap=True,
        bbox={
            "boxstyle": "round,pad=0.5",
            "facecolor": "#f3f4f6",
            "edgecolor": "#d1d5db",
            "linewidth": 0.8,
        },
    )


def _draw_nodes(
    ax: Any,
    positions: dict[str, tuple[float, float]],
    node_attrs: dict[str, dict[str, Any]],
) -> None:
    """Render labelled rounded boxes for every node."""
    for node_id, (x, y) in positions.items():
        attrs = node_attrs.get(node_id, {})
        is_anchor = bool(attrs.get("anchor"))
        label = attrs.get("label", node_id)
        face = "#fff4e6" if is_anchor else "#f8f9fa"
        edge_color = "#d97706" if is_anchor else "#cccccc"
        ax.text(
            x,
            y,
            label,
            ha="center",
            va="center",
            fontsize=9,
            bbox={
                "boxstyle": "round,pad=0.4",
                "facecolor": face,
                "edgecolor": edge_color,
                "linewidth": 1.6 if is_anchor else 0.8,
            },
        )


def _draw_edges(
    ax: Any,
    positions: dict[str, tuple[float, float]],
    edge_attrs: list[dict[str, Any]],
) -> None:
    """Render straight arrows for every edge with style hints baked in."""
    for ea in edge_attrs:
        src = positions.get(ea["src"])
        tgt = positions.get(ea["tgt"])
        if src is None or tgt is None:
            continue
        style = ea["style"]
        ax.annotate(
            "",
            xy=tgt,
            xytext=src,
            arrowprops={
                "arrowstyle": "->",
                "linewidth": style["linewidth"],
                "color": style["color"],
                "linestyle": style["linestyle"],
                "shrinkA": 14,
                "shrinkB": 14,
            },
        )
        mx = (src[0] + tgt[0]) / 2.0
        my = (src[1] + tgt[1]) / 2.0
        # Nudge the label off the line so it doesn't sit on top of nodes
        # that happen to fall along the midpoint axis. The offset is
        # perpendicular to the edge direction so labels don't cluster.
        dx, dy = tgt[0] - src[0], tgt[1] - src[1]
        length = max((dx * dx + dy * dy) ** 0.5, 1e-6)
        offset_magnitude = 0.18
        off_x = -dy / length * offset_magnitude
        off_y = dx / length * offset_magnitude
        ax.text(
            mx + off_x,
            my + off_y,
            ea["label"],
            fontsize=7,
            color=style["color"],
            ha="center",
            va="center",
            bbox={
                "boxstyle": "round,pad=0.1",
                "facecolor": "white",
                "edgecolor": "none",
                "alpha": 0.85,
            },
        )


# ── internals: per-edge / per-node styling shared with build_dot ─────────


def _edge_style(edge: Edge) -> dict[str, Any]:
    if edge.relationship_type == "lineage_name_match":
        return {"color": "#9ca3af", "linewidth": 0.8, "linestyle": "dashed"}
    return {"color": "#1f2937", "linewidth": 1.0, "linestyle": "solid"}


def _edge_label(edge: Edge) -> str:
    if edge.relationship_type == "lineage_name_match":
        return "≈name"
    if edge.relationship_type == "lineage_fk":
        return "fk"
    if edge.relationship_type == "lineage_view_ddl":
        return "view"
    return edge.extractor


def _node_id(ref: ColumnRef) -> str:
    return ".".join(p for p in (ref.database, ref.schema, ref.table, ref.column) if p)


def _node_label(ref: ColumnRef, *, anchor: bool, described: bool) -> str:
    badge = "✓" if described else "○"
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
    label_tag = _edge_label(edge)
    if edge.relationship_type == "lineage_name_match":
        style = ', style="dashed", color="#9ca3af"'
    elif edge.relationship_type in {"lineage_fk", "lineage_view_ddl"}:
        style = ', color="#1f2937"'
    else:
        style = ""
    return f"  {_dot_id(src_id)} -> {_dot_id(tgt_id)} [label={_dot_quote(label_tag)}{style}];\n"


def _dot_id(node_id: str) -> str:
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
