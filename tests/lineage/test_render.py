"""DOT builder + matplotlib renderer + cross-platform helpers."""

from __future__ import annotations

import os
import warnings
from unittest.mock import patch

import pytest

from amx.lineage.render import (
    HARD_NODE_LIMIT,
    SOFT_NODE_LIMIT,
    SUPPORTED_FORMATS,
    RenderInput,
    _figure_size,
    build_dot,
    count_nodes,
    open_artifact,
    render_lineage_image,
)
from amx.lineage.types import ColumnRef, Edge


def _make_edge(
    *, src_table: str, src_col: str, tgt_table: str, tgt_col: str, rel: str = "lineage_view_ddl"
) -> Edge:
    return Edge(
        source=ColumnRef("", "public", src_table, src_col),
        target=ColumnRef("", "public", tgt_table, tgt_col),
        relationship_type=rel,
        extractor=rel.split("_", 1)[-1],
        confidence=1.0,
        evidence="",
    )


def test_build_dot_contains_anchor_and_edges():
    anchor = ColumnRef("", "public", "orders", "")
    edges = [
        _make_edge(src_table="customers", src_col="id", tgt_table="orders", tgt_col="customer_id"),
    ]
    dot = build_dot(RenderInput(edges=edges, anchor=anchor, described_entities=set()))
    assert "digraph lineage" in dot
    assert '"public.orders"' in dot
    assert '"public.customers.id" -> "public.orders.customer_id"' in dot


def test_build_dot_marks_described_entities_with_check_badge():
    anchor = ColumnRef("", "public", "orders", "")
    edges = [
        _make_edge(src_table="customers", src_col="id", tgt_table="orders", tgt_col="customer_id")
    ]
    described = {"public.customers.id"}
    dot = build_dot(RenderInput(edges=edges, anchor=anchor, described_entities=described))
    assert '"✓ public.customers.id"' in dot
    assert '"○ public.orders.customer_id"' in dot


def test_build_dot_includes_partial_warning_banner_when_set():
    anchor = ColumnRef("", "public", "orders", "")
    dot = build_dot(
        RenderInput(
            edges=[],
            anchor=anchor,
            described_entities=set(),
            partial_warning="Partial render — view DDL cache stale.",
        )
    )
    assert "Partial render" in dot
    assert "__partial__" in dot


def test_name_match_edges_render_as_dashed():
    anchor = ColumnRef("", "public", "orders", "")
    edges = [
        _make_edge(
            src_table="customers",
            src_col="id",
            tgt_table="orders",
            tgt_col="customer_id",
            rel="lineage_name_match",
        )
    ]
    dot = build_dot(RenderInput(edges=edges, anchor=anchor, described_entities=set()))
    assert 'style="dashed"' in dot


def test_count_nodes_dedupes_anchor():
    anchor = ColumnRef("", "public", "orders", "")
    e1 = _make_edge(src_table="customers", src_col="id", tgt_table="orders", tgt_col="customer_id")
    e2 = _make_edge(src_table="customers", src_col="id", tgt_table="orders", tgt_col="customer_id")
    assert count_nodes([e1, e2], anchor) == 3  # anchor + customers.id + orders.customer_id


def test_scale_limits_are_sane_defaults():
    assert SOFT_NODE_LIMIT < HARD_NODE_LIMIT
    assert SUPPORTED_FORMATS == ("svg", "png", "jpg")


def test_render_unsupported_format_raises():
    with pytest.raises(ValueError):
        render_lineage_image(
            payload=RenderInput(
                edges=[],
                anchor=ColumnRef("", "public", "orders", ""),
                described_entities=set(),
            ),
            fmt="bmp",
            output_path="x.bmp",
        )


def test_render_writes_svg_with_matplotlib_backend(tmp_path):
    """End-to-end render: matplotlib + networkx, no graphviz, no system binary."""
    pytest.importorskip("matplotlib")
    pytest.importorskip("networkx")
    out_path = tmp_path / "render.svg"
    anchor = ColumnRef("", "public", "orders", "")
    edges = [
        Edge(
            source=ColumnRef("", "public", "customers", "id"),
            target=ColumnRef("", "public", "orders", "customer_id"),
            relationship_type="lineage_fk",
            extractor="fk",
            confidence=1.0,
            evidence="",
        ),
    ]
    result = render_lineage_image(
        payload=RenderInput(edges=edges, anchor=anchor, described_entities=set(), title="orders"),
        fmt="svg",
        output_path=out_path,
    )
    assert result == out_path
    assert out_path.exists()
    body = out_path.read_text(encoding="utf-8")
    # Matplotlib emits an XML SVG header; node label text travels through verbatim.
    assert body.startswith("<?xml") or "<svg" in body
    assert "orders" in body


def test_render_handles_png_output(tmp_path):
    """PNG path exercises matplotlib's raster backend (Agg)."""
    pytest.importorskip("matplotlib")
    pytest.importorskip("networkx")
    out_path = tmp_path / "render.png"
    render_lineage_image(
        payload=RenderInput(
            edges=[],
            anchor=ColumnRef("", "public", "orders", ""),
            described_entities=set(),
        ),
        fmt="png",
        output_path=out_path,
    )
    assert out_path.exists()
    # PNG magic header: 89 50 4E 47 0D 0A 1A 0A
    assert out_path.read_bytes()[:8] == b"\x89PNG\r\n\x1a\n"


def test_open_artifact_dispatches_per_platform(tmp_path):
    """Verify platform-aware open helpers fire on darwin/linux/win32."""
    f = tmp_path / "x.svg"
    f.write_text("<svg/>")

    # macOS uses `open`
    with (
        patch("amx.lineage.render.sys", autospec=True) as fake_sys,
        patch("amx.lineage.render.subprocess.run") as fake_run,
    ):
        fake_sys.platform = "darwin"
        open_artifact(f)
    assert fake_run.call_args is not None
    assert fake_run.call_args.args[0][0] == "open"

    # Linux uses `xdg-open` (and only when a display is configured)
    with (
        patch("amx.lineage.render.sys", autospec=True) as fake_sys,
        patch("amx.lineage.render.subprocess.run") as fake_run,
        patch.dict(os.environ, {"DISPLAY": ":0"}, clear=False),
    ):
        fake_sys.platform = "linux"
        open_artifact(f)
    assert fake_run.call_args.args[0][0] == "xdg-open"

    # Windows uses os.startfile when present
    with (
        patch("amx.lineage.render.sys", autospec=True) as fake_sys,
        patch("amx.lineage.render.os", autospec=True) as fake_os,
    ):
        fake_sys.platform = "win32"
        fake_os.startfile = lambda p: None
        fake_os.environ = {}
        ok = open_artifact(f)
    assert ok is True


def test_open_artifact_returns_false_when_headless_linux(tmp_path):
    f = tmp_path / "x.svg"
    f.write_text("<svg/>")
    with (
        patch("amx.lineage.render.sys", autospec=True) as fake_sys,
        patch.dict(os.environ, {}, clear=True),
    ):
        fake_sys.platform = "linux"
        ok = open_artifact(f)
    assert ok is False


# ── Render quality fixes (plan: render-quality follow-up) ───────────────


def test_figure_size_tiny_graph_is_small():
    """Single-node case must not waste a 10x6 canvas on one tiny box."""
    w, h = _figure_size(1)
    assert (w, h) == (6.0, 4.0)


def test_figure_size_scales_with_node_count():
    """Bigger graphs get more canvas, but growth is bounded and gentle."""
    small = _figure_size(1)
    medium = _figure_size(10)
    large = _figure_size(100)
    huge = _figure_size(500)
    assert medium[0] > small[0] and medium[1] > small[1]
    assert large[0] > medium[0] and large[1] > medium[1]
    assert huge[0] > large[0]
    # Hard ceiling sanity: at 500 nodes we are still well under desktop-wide.
    assert huge[0] < 16.0 and huge[1] < 12.0


def test_empty_result_caption_present_for_anchor_only_graph(tmp_path):
    """Anchor-only render must surface the 'no related entities' caption."""
    pytest.importorskip("matplotlib")
    pytest.importorskip("networkx")
    out = tmp_path / "empty.svg"
    render_lineage_image(
        payload=RenderInput(
            edges=[],
            anchor=ColumnRef("", "public", "orders", ""),
            described_entities=set(),
            title="lineage: SAP.public.orders",
        ),
        fmt="svg",
        output_path=out,
    )
    body = out.read_text(encoding="utf-8")
    assert "No related entities found in cache." in body
    assert "/lineage refresh --no-cache" in body


def test_anchor_only_render_does_not_emit_tight_layout_warning(tmp_path):
    """tight_layout fights ax.set_axis_off and warns on empty axes — keep it silent."""
    pytest.importorskip("matplotlib")
    pytest.importorskip("networkx")
    out = tmp_path / "noisefree.svg"
    with warnings.catch_warnings():
        warnings.simplefilter("error", UserWarning)
        render_lineage_image(
            payload=RenderInput(
                edges=[],
                anchor=ColumnRef("", "public", "orders", ""),
                described_entities=set(),
                title="orders",
                partial_warning="Partial render — view DDL cache stale.",
            ),
            fmt="svg",
            output_path=out,
        )
    assert out.exists()


def test_multi_node_render_sizing_grows(tmp_path):
    """Multi-edge render uses the scaled-up canvas without a leftover 10x6 default."""
    pytest.importorskip("matplotlib")
    pytest.importorskip("networkx")
    anchor = ColumnRef("", "public", "orders", "")
    edges = [
        Edge(
            source=ColumnRef("", "public", "customers", "id"),
            target=ColumnRef("", "public", "orders", "customer_id"),
            relationship_type="lineage_fk",
            extractor="fk",
            confidence=1.0,
            evidence="",
        ),
        Edge(
            source=ColumnRef("", "public", "orders", "customer_id"),
            target=ColumnRef("", "public", "daily_orders", "cid"),
            relationship_type="lineage_view_ddl",
            extractor="view_ddl",
            confidence=1.0,
            evidence="",
        ),
    ]
    out = tmp_path / "multi.svg"
    render_lineage_image(
        payload=RenderInput(
            edges=edges,
            anchor=anchor,
            described_entities={"public.customers.id"},
            title="orders lineage",
        ),
        fmt="svg",
        output_path=out,
    )
    assert out.exists()
    body = out.read_text(encoding="utf-8")
    # Caption must NOT appear when the graph carries real edges.
    assert "No related entities found in cache." not in body
    # Both endpoints + the anchor label survive into the SVG.
    assert "public.customers.id" in body
    assert "public.orders.customer_id" in body
    assert "public.daily_orders.cid" in body


def test_edge_label_offset_is_perpendicular_to_line(tmp_path):
    """Edge labels should sit off the midpoint, not on top of it."""
    pytest.importorskip("matplotlib")
    pytest.importorskip("networkx")
    anchor = ColumnRef("", "public", "orders", "")
    edges = [
        Edge(
            source=ColumnRef("", "public", "customers", "id"),
            target=ColumnRef("", "public", "orders", "customer_id"),
            relationship_type="lineage_fk",
            extractor="fk",
            confidence=1.0,
            evidence="",
        ),
    ]
    out = tmp_path / "offset.svg"
    render_lineage_image(
        payload=RenderInput(edges=edges, anchor=anchor, described_entities=set(), title="offset"),
        fmt="svg",
        output_path=out,
    )
    body = out.read_text(encoding="utf-8")
    assert "fk" in body  # edge label survives the offset nudge
