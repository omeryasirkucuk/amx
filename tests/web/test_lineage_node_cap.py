"""PR-C (scale): lineage payloads trim to a max node count.

The Studio canvas chokes past a few hundred react-flow nodes and
the data is unreadable anyway. The router's ``_cap_lineage_nodes``
helper trims by degree, preserves the anchor, drops orphaned edges,
and stamps ``truncated`` + ``original_node_count`` on the payload.
"""

from amx.web.routers.lineage import _cap_lineage_nodes


def _payload(node_count: int):
    """Build a synthetic payload with ``node_count`` table nodes."""
    nodes = [{"id": f"n{i}", "kind": "table", "table": f"t_{i}"} for i in range(node_count)]
    # Chain edges so the anchor (id=n0) has highest degree.
    edges = [{"source": "n0", "target": f"n{i}"} for i in range(1, node_count)]
    return {
        "anchor": {"table": "t_0"},
        "nodes": nodes,
        "edges": edges,
        "partial": False,
        "extractors_used": [],
        "generated_at": 0.0,
    }


def test_payload_under_limit_is_passthrough():
    p = _payload(50)
    result = _cap_lineage_nodes(p, limit=200)
    assert result["truncated"] is False
    assert len(result["nodes"]) == 50
    assert result["edges"] == p["edges"]


def test_payload_over_limit_trims_to_limit():
    p = _payload(500)
    result = _cap_lineage_nodes(p, limit=200)
    assert result["truncated"] is True
    assert result["original_node_count"] == 500
    assert len(result["nodes"]) == 200


def test_anchor_table_always_kept():
    """Even if the anchor has zero edges, the canvas needs it."""
    p = _payload(300)
    # Strip every edge so degree-based ordering would push the anchor out.
    p["edges"] = []
    result = _cap_lineage_nodes(p, limit=200)
    kept_tables = {n.get("table") for n in result["nodes"]}
    assert "t_0" in kept_tables


def test_edges_pointing_at_dropped_nodes_are_pruned():
    """No edge should reference a node id outside the kept set."""
    p = _payload(300)
    result = _cap_lineage_nodes(p, limit=50)
    kept_ids = {n["id"] for n in result["nodes"]}
    for e in result["edges"]:
        assert e["source"] in kept_ids
        assert e["target"] in kept_ids
