"""DOT builder + cross-platform helpers."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest

from amx.lineage.render import (
    HARD_NODE_LIMIT,
    SOFT_NODE_LIMIT,
    SUPPORTED_FORMATS,
    DotBinaryNotFound,
    RenderInput,
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


def test_render_raises_when_dot_binary_missing(tmp_path):
    with patch("amx.lineage.render.shutil.which", return_value=None):
        with pytest.raises(DotBinaryNotFound):
            render_lineage_image(
                payload=RenderInput(
                    edges=[],
                    anchor=ColumnRef("", "public", "orders", ""),
                    described_entities=set(),
                ),
                fmt="svg",
                output_path=tmp_path / "x.svg",
            )


def test_render_uses_subprocess_list_argv(tmp_path):
    captured: dict[str, object] = {}

    def fake_run(cmd, **kwargs):
        captured["cmd"] = cmd
        captured["shell"] = kwargs.get("shell", False)
        out = Path(cmd[cmd.index("-o") + 1])
        out.write_text("ok")
        return subprocess.CompletedProcess(cmd, 0, "", "")

    with (
        patch("amx.lineage.render.shutil.which", return_value="dot"),
        patch("amx.lineage.render.subprocess.run", side_effect=fake_run),
    ):
        out = render_lineage_image(
            payload=RenderInput(
                edges=[],
                anchor=ColumnRef("", "public", "orders", ""),
                described_entities=set(),
            ),
            fmt="svg",
            output_path=tmp_path / "x.svg",
        )
    assert isinstance(captured["cmd"], list)
    assert captured["shell"] is False
    assert out.exists()


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
