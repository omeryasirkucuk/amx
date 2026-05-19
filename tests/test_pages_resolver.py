"""Tests for the pages resolver, focused on the asset-text shape that
ends up in the composer's CONTEXT block. Lineage in particular must
expose its rendered image as inline markdown so the LLM forwards it
into the page body verbatim."""

from __future__ import annotations

from pathlib import Path

import pytest

from amx.config import AMXConfig
from amx.lineage.store import insert_lineage_artifact
from amx.pages._resolver import AMXResolver
from amx.storage import sqlite_store as _store_module
from amx.storage.sqlite_store import SQLiteHistoryStore


@pytest.fixture
def hs(tmp_path: Path):
    store = SQLiteHistoryStore(tmp_path / "history.db")
    store.init()
    prior = _store_module._store
    _store_module._store = store
    try:
        yield store
    finally:
        _store_module._store = prior


def _seed_artifact(hs: SQLiteHistoryStore, *, output_path: str) -> str:
    name = "orders_pipeline"
    insert_lineage_artifact(
        hs,
        name=name,
        db_profile="prod",
        anchor_entity_id=1,
        depth_up=2,
        depth_down=2,
        fmt="png",
        output_path=output_path,
        edge_set_hash="deadbeef",
        node_count=12,
        edge_count=17,
        extractors_used=["fk", "view"],
        extractors_partial=False,
    )
    return name


def test_lineage_resolver_embeds_image_markdown(hs, tmp_path: Path) -> None:
    """The lineage block must include a markdown image link so the
    composer can forward it into the page body without re-deriving the
    path."""
    img_path = tmp_path / "lineage_diagram.png"
    img_path.write_bytes(b"\x89PNG\r\n")
    name = _seed_artifact(hs, output_path=str(img_path))

    resolver = AMXResolver(AMXConfig())
    block = resolver.resolve_lineage(f"lineage:{name}")

    assert f"## Lineage `{name}`" in block
    assert f"![{name}](" in block
    # The path must reach the markdown link in some form (absolute or
    # relative to cwd, depending on whether the test runs under the
    # repo root).
    assert img_path.name in block
    # Numeric metadata stays.
    assert "nodes: 12" in block
    assert "edges: 17" in block
    # Old "- output: <path>" line must be gone — superseded by the
    # image link.
    assert "- output:" not in block


def test_lineage_resolver_missing_artifact_returns_stub(hs) -> None:
    resolver = AMXResolver(AMXConfig())
    block = resolver.resolve_lineage("lineage:nonexistent")
    assert "not found" in block


def test_image_link_prefers_relative_for_cwd_paths(tmp_path: Path, monkeypatch) -> None:
    """When the rendered image sits under the current working dir, the
    emitted link is a POSIX-form relative path so exported markdown is
    portable across machines."""
    monkeypatch.chdir(tmp_path)
    nested = tmp_path / "artifacts" / "lineage.png"
    nested.parent.mkdir(parents=True, exist_ok=True)
    nested.write_bytes(b"\x89PNG\r\n")

    link = AMXResolver._image_link(str(nested))
    assert link == "artifacts/lineage.png"


def test_image_link_falls_back_to_absolute(tmp_path: Path, monkeypatch) -> None:
    """When the image is outside the cwd, the absolute path is returned
    verbatim so Studio's markdown renderer can still resolve it."""
    monkeypatch.chdir(tmp_path)
    outside = Path("/tmp/some_other_dir/lineage.png")
    link = AMXResolver._image_link(str(outside))
    assert link == str(outside)


def _seed_entities_and_relationships(hs: SQLiteHistoryStore, edges) -> dict[str, int]:
    """Helper: insert catalog_entities + catalog_relationships rows so
    list_artifact_edges + the resolver have data to walk. ``edges`` is
    a list of ``(from_path, from_kind, to_path, to_kind, source,
    rel_type)`` tuples — paths are 'profile/database/schema/table' or
    'profile/.../table.column' for column-grain."""
    paths: dict[str, int] = {}

    def _ensure(path: str, kind: str) -> int:
        if path in paths:
            return paths[path]
        parts = path.split("/")
        profile = parts[0]
        rest = parts[1:]
        column = ""
        if rest and "." in rest[-1]:
            tbl, column = rest[-1].split(".", 1)
            rest[-1] = tbl
        database = rest[0] if len(rest) >= 3 else ""
        schema = rest[-2] if len(rest) >= 2 else ""
        table = rest[-1] if rest else ""
        with hs._lock, hs._connect() as conn:
            cur = conn.execute(
                "INSERT INTO catalog_entities (db_profile, db_backend, "
                "database_name, schema_name, table_name, column_name, "
                "entity_kind) VALUES (?, '', ?, ?, ?, ?, ?)",
                (profile, database, schema, table, column or None, kind),
            )
            eid = int(cur.lastrowid or 0)
        paths[path] = eid
        return eid

    for from_path, from_kind, to_path, to_kind, source, rel_type in edges:
        f_id = _ensure(from_path, from_kind)
        t_id = _ensure(to_path, to_kind)
        with hs._lock, hs._connect() as conn:
            conn.execute(
                "INSERT INTO catalog_relationships "
                "(from_entity_id, to_entity_id, relationship_type, source, "
                "details_json, last_seen) VALUES (?, ?, ?, ?, '{}', 0)",
                (f_id, t_id, rel_type, source),
            )

    return paths


def test_list_artifact_edges_returns_upstream_and_downstream(hs) -> None:
    """The helper must walk catalog_relationships both up and down from
    the anchor and surface every node with its entity_kind, including
    non-table inputs like Power BI reports."""
    from amx.lineage.store import list_artifact_edges

    paths = _seed_entities_and_relationships(
        hs,
        [
            (
                "prod/sap/sap_test/adrc",
                "table",
                "prod/sap/sap_s6p/adr6",
                "table",
                "fk",
                "lineage_fk",
            ),
            (
                "prod/sap/sap_s6p/but020",
                "table",
                "prod/sap/sap_s6p/adr6",
                "table",
                "view_ddl",
                "lineage_view_ddl",
            ),
            (
                "pbi/SalesByRegion",
                "report",
                "prod/sap/sap_s6p/adr6",
                "table",
                "codebase",
                "lineage_codebase",
            ),
            (
                "prod/sap/sap_s6p/adr6",
                "table",
                "prod/sap/sap_s6p/vbrl",
                "table",
                "fk",
                "lineage_fk",
            ),
            (
                "prod/sap/sap_s6p/adr6",
                "table",
                "pbi/CustomerOverview",
                "report",
                "codebase",
                "lineage_codebase",
            ),
        ],
    )
    anchor_id = paths["prod/sap/sap_s6p/adr6"]
    artifact = {
        "anchor_entity_id": anchor_id,
        "depth_up": 2,
        "depth_down": 2,
        "extractors_used": ["fk", "view_ddl", "codebase"],
    }
    payload = list_artifact_edges(hs, artifact=artifact, limit=100)

    assert payload["truncated"] is False
    assert len(payload["edges"]) == 5
    kinds = {n["kind"] for n in payload["nodes"]}
    assert kinds == {"table", "report"}

    # Spot-check that the Power BI node round-trips with its kind.
    pbi_edges = [e for e in payload["edges"] if "pbi" in e["from_path"] or "pbi" in e["to_path"]]
    assert pbi_edges
    assert any(e["from_kind"] == "report" or e["to_kind"] == "report" for e in pbi_edges)


def test_list_artifact_edges_filters_by_extractor(hs) -> None:
    """When the artifact's extractors_used list is non-empty, the
    helper must drop edges produced by other extractors so the
    resolver output stays consistent with what the diagram renders."""
    from amx.lineage.store import list_artifact_edges

    paths = _seed_entities_and_relationships(
        hs,
        [
            ("p/db/s/anchor", "table", "p/db/s/down1", "table", "fk", "lineage_fk"),
            ("p/db/s/anchor", "table", "p/db/s/down2", "table", "llm", "lineage_llm"),
        ],
    )
    artifact = {
        "anchor_entity_id": paths["p/db/s/anchor"],
        "depth_up": 1,
        "depth_down": 1,
        "extractors_used": ["fk"],  # excludes llm
    }
    payload = list_artifact_edges(hs, artifact=artifact, limit=100)
    sources = {e["source"] for e in payload["edges"]}
    assert sources == {"fk"}
    assert len(payload["edges"]) == 1


def test_resolve_lineage_emits_edge_bullets_with_kind_labels(hs, tmp_path: Path) -> None:
    """End-to-end: the resolver block fed to the composer must contain
    real edges with kind labels so the LLM can write a concrete
    Source Systems / Targets narrative."""
    from amx.lineage.store import insert_lineage_artifact

    paths = _seed_entities_and_relationships(
        hs,
        [
            (
                "local/sap/sap_test/adrc",
                "table",
                "local/sap/sap_s6p/adr6",
                "table",
                "fk",
                "lineage_fk",
            ),
            (
                "local/sap/sap_s6p/but020",
                "table",
                "local/sap/sap_s6p/adr6",
                "table",
                "view_ddl",
                "lineage_view_ddl",
            ),
            (
                "pbi/SalesByRegion",
                "report",
                "local/sap/sap_s6p/adr6",
                "table",
                "codebase",
                "lineage_codebase",
            ),
            (
                "local/sap/sap_s6p/adr6",
                "table",
                "local/sap/sap_s6p/vbrl",
                "table",
                "fk",
                "lineage_fk",
            ),
        ],
    )
    anchor_id = paths["local/sap/sap_s6p/adr6"]
    img_path = tmp_path / "lineage.png"
    img_path.write_bytes(b"\x89PNG\r\n")
    insert_lineage_artifact(
        hs,
        name="adr6_pipeline",
        db_profile="local",
        anchor_entity_id=anchor_id,
        depth_up=2,
        depth_down=2,
        fmt="png",
        output_path=str(img_path),
        edge_set_hash="deadbeef",
        node_count=5,
        edge_count=4,
        extractors_used=["fk", "view_ddl", "codebase"],
        extractors_partial=False,
    )

    resolver = AMXResolver(AMXConfig())
    block = resolver.resolve_lineage("lineage:adr6_pipeline")

    # Anchor labelled with its kind.
    assert "anchor: [table] local/sap/sap_s6p/adr6" in block
    # Image markdown still present.
    assert "![adr6_pipeline]" in block
    # Upstream + downstream sections appear.
    assert "Upstream edges" in block
    assert "Downstream edges" in block
    # Power BI report node round-trips with its kind label.
    assert "[report] pbi/SalesByRegion" in block
    # Extractors badged on each edge.
    assert "lineage_fk · fk" in block
    # Node mix line summarises kinds.
    assert "Node mix" in block
    assert "report" in block
