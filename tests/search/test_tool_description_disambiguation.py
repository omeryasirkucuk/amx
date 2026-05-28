"""Tool descriptions separate ASK selection from /run history.

The LLM confused "which assets did I select" (a SELECTION CONTEXT
question) with ``list_past_runs`` (analyze history). These assertions
pin the disambiguating language onto the relevant tool descriptions so
the routing stays unambiguous.
"""

from __future__ import annotations

from amx.search._tool_schemas import tool_schemas


def _desc(name: str) -> str:
    for entry in tool_schemas():
        fn = entry["function"]
        if fn["name"] == name:
            return fn["description"]
    raise AssertionError(f"tool {name!r} not found in schemas")


def test_list_past_runs_marked_history_only() -> None:
    desc = _desc("list_past_runs")
    assert "HISTORY ONLY" in desc
    assert "SELECTION CONTEXT" in desc
    assert "NOT from run history" in desc


def test_describe_run_not_a_selection() -> None:
    desc = _desc("describe_run")
    assert "never use this to answer 'what did I select'" in desc


def test_search_assets_points_to_selection_context() -> None:
    desc = _desc("search_assets")
    assert "SELECTED-ASSETS retrieval tool" in desc
    assert "NEVER answer an asset question with list_past_runs" in desc


def test_lineage_tools_point_to_selection_context() -> None:
    for name in ("lineage_for_table", "lineage_for_column"):
        desc = _desc(name)
        assert "SELECTED-LINEAGE retrieval tool" in desc
        assert "SELECTION CONTEXT" in desc
