"""ProfileAgent renders the optional lineage-block ``detail`` field."""

from __future__ import annotations

from amx.agents.profile_agent import _render_lineage_section


def test_renders_detail_when_present() -> None:
    blocks = [
        {
            "direction": "upstream",
            "kind": "table",
            "name": "sales.customers",
            "relationship": "lineage_native_table",
            "detail": "Master list of customers.",
        }
    ]
    text = "\n".join(_render_lineage_section(blocks))
    assert "sales.customers" in text
    assert "Master list of customers." in text


def test_omits_detail_when_absent() -> None:
    blocks = [
        {
            "direction": "downstream",
            "kind": "notebook",
            "name": "ETL nb",
            "relationship": "lineage_native_asset",
        }
    ]
    text = "\n".join(_render_lineage_section(blocks))
    assert "ETL nb" in text
    assert "—" not in text  # no trailing detail separator
