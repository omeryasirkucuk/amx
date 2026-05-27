"""The lineage appendix renders neighbour names, not raw entity ids."""

from __future__ import annotations

from amx.search.tool_agent import _format_lineage_pages_appendix


def test_appendix_renders_names_not_ids() -> None:
    lineage = {
        "kind": "lineage",
        "artifact_names": ["orders-canvas"],
        "upstream": [
            {"name": "sales.customers", "kind": "table", "relationship": "lineage_native_table"}
        ],
        "downstream": [
            {"name": "ETL nb", "kind": "notebook", "relationship": "lineage_native_asset"}
        ],
        "upstream_entity_ids": [20],
        "downstream_entity_ids": [30],
        "external_systems": ["databricks"],
        "comments": [],
    }
    text = _format_lineage_pages_appendix(lineage, None)
    assert "sales.customers" in text
    assert "ETL nb" in text
    assert "orders-canvas" in text
    assert "databricks" in text
    # Raw entity-id lines must be gone.
    assert "entity ids" not in text
    assert "20" not in text and "30" not in text


def test_appendix_handles_missing_names_gracefully() -> None:
    # No upstream/downstream name lists (e.g. canvas-only data) -- header
    # + canvas still render, no crash.
    lineage = {"kind": "lineage", "artifact_names": ["c"], "comments": []}
    text = _format_lineage_pages_appendix(lineage, None)
    assert "Lineage evidence" in text
    assert "c" in text
