"""Enforce the cache_ok / live_only contract on every ToolBox tool.

Background: in commit a8fb14a6 the ``find_assets_missing_comment``
tool was annotated ``freshness="cache_ok"`` but its body calls
``self._live_db()`` / ``db.get_table_comment()`` per asset — so cache-
only Ask mode silently hit the warehouse anyway. Detecting this at
review time is hard; this regression test catches it automatically.

Rule: a tool annotated ``cache_ok`` must NOT reach a live database
connector under any code path. We enforce that mechanically by
swapping ``_live_db`` / ``_connector_for_profile`` with raising stubs
on a real ToolBox built in cache-only mode and dispatching each
cache_ok tool through ``invoke()``.

Some tools legitimately need cooperating ``self.catalog`` data
(search_text rows for FTS, profile_state rows for sync status). We
provide a minimal in-memory SearchCatalog seeded with a tiny fixture
so the catalog path resolves to something testable. Tools that touch
live for OPTIONAL fallback (e.g. unfreezable code paths) must guard
their cache-only branches; if they fail this test, fix the tool or
re-classify it as live_only.
"""

from __future__ import annotations

import json

import pytest

from amx.search._tool_schemas import (
    FRESHNESS_CACHE_OK,
    FRESHNESS_LIVE_ONLY,
    tool_schemas,
)

# Per-tool minimal arguments. Tools not listed here get an empty dict;
# the tool body should still not reach the live DB even when arguments
# are missing — error payloads are fine, live-DB hits are not.
_TOOL_ARGS: dict[str, dict[str, object]] = {
    "list_schemas": {},
    "list_tables_in_schema": {"schema": "public"},
    "find_table_by_name": {"name": "orders"},
    "describe_table": {"schema": "public", "table": "orders"},
    "search_tables_by_concept": {"concept": "orders"},
    "search_columns_by_concept": {"concept": "email"},
    "get_join_candidates": {"left": "public.orders", "right": "public.users"},
    "find_columns_by_dtype": {"dtype": "varchar"},
    "list_past_runs": {"limit": 5},
    "describe_run": {"run_id": 1},
    "compare_runs": {"run_id_a": 1, "run_id_b": 2},
    "list_chat_sessions": {"limit": 5},
    "search_docs": {"query": "schema design"},
    "search_code": {"query": "sales orders"},
    "list_schedules": {},
    "get_schedule": {"schedule_id": 1},
    "catalog_sync_status": {},
    "catalog_coverage_summary": {},
    "catalog_inventory": {"scope": "schemas"},
    "describe_column": {
        "schema": "public",
        "table": "orders",
        "column": "order_id",
    },
}


@pytest.fixture(scope="module")
def seeded_catalog(tmp_path_factory):
    """A SearchCatalog with two profiles, two schemas, a few tables, and
    a couple of described entities — enough to exercise the lookup
    helpers cache_ok tools call (fetch_distinct_schemas / search_tables
    / find_tables_by_exact_name / get_profile_state / …)."""
    from amx.search.catalog import SearchCatalog
    from amx.storage.sqlite_store import SQLiteHistoryStore

    db_path = tmp_path_factory.mktemp("freshness") / "history.db"
    store = SQLiteHistoryStore(db_path)
    store.init()
    cat = SearchCatalog(db_path)

    # Seed two tables × two profiles via the public catalog API so the
    # downstream FTS mirror / description join stays in sync.
    for profile, schema, table, has_desc in [
        ("prod-pg", "public", "orders", True),
        ("prod-pg", "public", "users", False),
        ("prod-pg", "sap", "vbrk", True),
        ("dbr-oyk", "amx_test", "adrc", True),
    ]:
        cat.record_applied_description(
            db_profile=profile,
            db_backend="postgresql" if profile == "prod-pg" else "databricks",
            database_name="" if profile == "prod-pg" else "amx_test",
            schema_name=schema,
            table_name=table,
            column_name=None,
            entity_kind="table",
            asset_kind="table",
            description=(f"Demo description for {schema}.{table}" if has_desc else ""),
        )
    return cat


@pytest.fixture
def cache_only_toolbox(seeded_catalog):
    """ToolBox in cache-only mode whose live connectors raise on
    access. Any cache_ok tool that wrongly reaches a live path will
    bubble up an AssertionError into the test."""
    from amx.config import AMXConfig
    from amx.search.agent_tools import ToolBox

    cfg = AMXConfig()
    cfg.db.backend = "postgresql"
    box = ToolBox(
        cfg,
        seeded_catalog,
        db_profiles=["prod-pg", "dbr-oyk"],
        allow_live_refresh=False,
    )

    def _raise_live(*_args, **_kwargs):
        raise AssertionError("cache_ok tool reached a live connector — re-classify or fix")

    box._live_db = _raise_live  # type: ignore[method-assign]
    box._connector_for_profile = _raise_live  # type: ignore[method-assign]
    return box


def _cache_ok_tool_names() -> list[str]:
    return [
        entry["function"]["name"]
        for entry in tool_schemas()
        if entry.get("freshness") == FRESHNESS_CACHE_OK
    ]


def _live_only_tool_names() -> list[str]:
    return [
        entry["function"]["name"]
        for entry in tool_schemas()
        if entry.get("freshness") == FRESHNESS_LIVE_ONLY
    ]


@pytest.mark.parametrize("tool_name", _cache_ok_tool_names())
def test_cache_ok_tools_never_reach_live(cache_only_toolbox, tool_name) -> None:
    """Every cache_ok tool must dispatch without opening a live
    connector. The raising stub on ``_live_db`` / ``_connector_for_profile``
    converts any accidental live access into a hard test failure."""
    args = _TOOL_ARGS.get(tool_name, {})
    raw = json.dumps(args)
    # ``invoke`` swallows tool exceptions into error payloads; we have
    # to dig past that to check the raise we care about — but AssertionError
    # is wrapped as ``{"error": "Tool X failed: ..."}``. Detect that pattern
    # explicitly so the test message is precise.
    result_json = cache_only_toolbox.invoke(tool_name, raw)
    result = json.loads(result_json) if result_json else {}
    err = result.get("error") if isinstance(result, dict) else None
    if isinstance(err, str) and "reached a live connector" in err:
        pytest.fail(
            f"Tool {tool_name!r} (freshness=cache_ok) attempted to open a "
            f"live connector — re-classify the tool as live_only or fix "
            f"its body. Error returned: {err}"
        )


def test_all_tools_are_classified() -> None:
    """No tool should have a missing freshness annotation."""
    bad = [
        entry["function"]["name"]
        for entry in tool_schemas()
        if entry.get("freshness") not in (FRESHNESS_CACHE_OK, FRESHNESS_LIVE_ONLY)
    ]
    assert not bad, (
        f"Tools missing or with invalid freshness annotation: {bad}. "
        f"Add freshness='cache_ok' or 'live_only' to each entry in "
        f"amx/search/_tool_schemas.py."
    )


def test_live_only_tools_blocked_by_invoke_in_cache_only_mode(
    cache_only_toolbox,
) -> None:
    """Belt-and-braces: every live_only tool dispatched in cache-only
    mode should short-circuit via the existing invoke() guard. The
    response payload must indicate the block so the SPA can render
    the 'Enable Live refresh & retry' button."""
    for tool_name in _live_only_tool_names():
        raw = json.dumps(_TOOL_ARGS.get(tool_name, {}))
        result = json.loads(cache_only_toolbox.invoke(tool_name, raw))
        assert isinstance(result, dict)
        # Either the legacy live_only_tool_disabled error or the new
        # needs_live_refresh envelope is acceptable here — both keep
        # the tool from running its body.
        if result.get("needs_live_refresh") is True:
            continue
        if result.get("error") == "live_only_tool_disabled":
            continue
        pytest.fail(
            f"Tool {tool_name!r} (freshness=live_only) was dispatched in "
            f"cache-only mode but did not return a structured refusal "
            f"payload. Response: {result}"
        )
