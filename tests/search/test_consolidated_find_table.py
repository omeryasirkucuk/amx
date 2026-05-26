"""Parity contract for the consolidated `find_table` dispatcher.

The dispatcher in ``amx/search/tools/consolidated.py`` is not yet
LLM-visible; the schema swap happens in a later PR. Until then this
suite proves the dispatcher produces byte-identical output to the
legacy tools given matching arguments — that's the contract the
future schema flip relies on.

Tests use a stubbed ToolBox whose legacy methods are recorded so the
assertion targets the dispatch shape (which method was called with
which kwargs) and the pass-through of the return value.
"""

from __future__ import annotations

from typing import Any

from amx.search.tools.consolidated import find_table


class _RecordingToolbox:
    """Stub ToolBox capturing legacy-method calls.

    Mirrors only the three methods the dispatcher delegates to; any
    other access raises AttributeError so an accidental new branch
    in ``find_table`` is caught immediately.
    """

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def _tool_find_table_by_name(self, *, name: str, force_fresh: bool) -> dict[str, Any]:
        self.calls.append(("find_table_by_name", {"name": name, "force_fresh": force_fresh}))
        return {"found": True, "name": name, "via": "find_table_by_name"}

    def _tool_search_tables_by_concept(self, *, concept: str, limit: int) -> dict[str, Any]:
        self.calls.append(("search_tables_by_concept", {"concept": concept, "limit": limit}))
        return {"concept": concept, "limit": limit, "via": "search_tables_by_concept"}

    def _tool_list_tables_in_schema(
        self, *, schema: str, catalog: str, force_fresh: bool
    ) -> dict[str, Any]:
        self.calls.append(
            (
                "list_tables_in_schema",
                {"schema": schema, "catalog": catalog, "force_fresh": force_fresh},
            )
        )
        return {"schema": schema, "via": "list_tables_in_schema"}


def test_exact_strategy_routes_to_find_table_by_name() -> None:
    """`strategy='exact'` must call the legacy exact-match tool with
    the same `name` + `force_fresh` it received."""
    tb = _RecordingToolbox()

    result = find_table(tb, name="customers", strategy="exact", force_fresh=True)  # type: ignore[arg-type]

    assert tb.calls == [("find_table_by_name", {"name": "customers", "force_fresh": True})]
    # Pass-through: the dispatcher must not modify the legacy result.
    assert result == {"found": True, "name": "customers", "via": "find_table_by_name"}


def test_semantic_strategy_routes_to_search_tables_by_concept() -> None:
    """`strategy='semantic'` treats `name` as the concept query and
    forwards `limit` verbatim."""
    tb = _RecordingToolbox()

    result = find_table(tb, name="customer activity", strategy="semantic", limit=5)  # type: ignore[arg-type]

    assert tb.calls == [("search_tables_by_concept", {"concept": "customer activity", "limit": 5})]
    assert result["via"] == "search_tables_by_concept"


def test_list_in_schema_strategy_routes_to_list_tables_in_schema() -> None:
    """`strategy='list_in_schema'` uses `name` as the schema and
    threads `scope` into the legacy tool's `catalog` parameter."""
    tb = _RecordingToolbox()

    result = find_table(  # type: ignore[arg-type]
        tb,
        name="public",
        strategy="list_in_schema",
        scope="my_db",
    )

    assert tb.calls == [
        (
            "list_tables_in_schema",
            {"schema": "public", "catalog": "my_db", "force_fresh": False},
        )
    ]
    assert result["via"] == "list_tables_in_schema"


def test_default_strategy_is_exact() -> None:
    """Calling without `strategy=` must default to exact match — keeps
    the dispatcher's call site obvious for the common path."""
    tb = _RecordingToolbox()

    find_table(tb, name="orders")  # type: ignore[arg-type]

    assert tb.calls[0][0] == "find_table_by_name"


def test_empty_name_returns_error_envelope_without_calling_legacy() -> None:
    """Empty `name` is rejected up front — no legacy call, structured
    error envelope. Matches the legacy tools' own validation."""
    tb = _RecordingToolbox()

    result = find_table(tb, name="", strategy="exact")  # type: ignore[arg-type]

    assert tb.calls == []
    assert "required" in result["error"]


def test_whitespace_only_name_treated_as_empty() -> None:
    """Whitespace-only `name` is the same as empty — legacy tools
    strip and reject, the dispatcher does the same up front."""
    tb = _RecordingToolbox()

    result = find_table(tb, name="   ", strategy="exact")  # type: ignore[arg-type]

    assert tb.calls == []
    assert "required" in result["error"]


def test_unknown_strategy_returns_error_envelope() -> None:
    """An unknown strategy must surface a clear error envelope —
    callers should never silently get the wrong tool's output."""
    tb = _RecordingToolbox()

    result = find_table(tb, name="x", strategy="fuzzy")  # type: ignore[arg-type]

    assert tb.calls == []
    assert "Unknown strategy" in result["error"]
    assert "fuzzy" in result["error"]
