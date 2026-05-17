"""Cache-only Ask + catalog-None guardrails.

Three orthogonal concerns this file pins down:

1. ``AskRequest`` defaults the new ``allow_live_refresh`` field to
   ``False`` — the Studio composer keeps Ask cache-only unless the
   user flips the toggle for the turn.
2. ``ToolBox._gate_force_fresh`` neutralises the tool-level
   ``force_fresh`` argument when ``allow_live_refresh`` is False,
   regardless of what the LLM asked for. With it True, the flag
   passes through unchanged.
3. The Databricks adapter no longer falls through to SQLAlchemy's
   ``SHOW TABLES FROM `None`.<schema>`` when the profile has no
   ``catalog`` configured — it returns ``[]`` and logs a warning.
4. ``validate_required_fields`` raises ``ProfileValidationError`` for
   blank / missing Databricks catalog so the operator sees a clean
   400 at save time instead of a confusing Spark error mid-question.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest

from amx.db.adapters.databricks import DatabricksAdapter
from amx.db.profile_schema import (
    ProfileValidationError,
    validate_required_fields,
)
from amx.search.agent_tools import ToolBox
from amx.web.routers.ask import AskRequest

# ── AskRequest defaults ─────────────────────────────────────────────


def test_ask_request_defaults_to_cache_only() -> None:
    body = AskRequest(question="are my tables in sync")
    assert body.allow_live_refresh is False, (
        "Ask is cache-only by default — the toggle must be opt-in. "
        "Changing this default would resurrect the silent live-DB read."
    )


def test_ask_request_accepts_explicit_live_refresh() -> None:
    body = AskRequest(question="any change?", allow_live_refresh=True)
    assert body.allow_live_refresh is True


# ── ToolBox._gate_force_fresh ───────────────────────────────────────


class _StubToolBox:
    """Minimal stand-in for ToolBox so we can exercise the gate in
    isolation without spinning up SearchCatalog + a real DB."""

    _allow_live_refresh: bool

    def __init__(self, allow: bool) -> None:
        self._allow_live_refresh = allow

    _gate_force_fresh = ToolBox._gate_force_fresh  # type: ignore[assignment]


def test_gate_force_fresh_suppresses_when_toggle_off(
    caplog: pytest.LogCaptureFixture,
) -> None:
    box = _StubToolBox(allow=False)
    with caplog.at_level(logging.DEBUG, logger="search.agent_tools"):
        # LLM asks for fresh; toggle off → coerced to False.
        assert box._gate_force_fresh(True) is False
        # Also returns False when the LLM didn't ask in the first place.
        assert box._gate_force_fresh(False) is False
    # The suppression should leave an audit-trail debug log.
    suppressed = [rec for rec in caplog.records if "force_fresh suppressed" in rec.getMessage()]
    assert suppressed, "expected a debug log line when force_fresh is suppressed"


def test_gate_force_fresh_passes_through_when_toggle_on() -> None:
    box = _StubToolBox(allow=True)
    assert box._gate_force_fresh(True) is True
    # Still respects an explicit False from the LLM.
    assert box._gate_force_fresh(False) is False


# ── Databricks list_tables / list_views guards ─────────────────────


class _RaisingEngine:
    """Sentinel engine — any attempt to connect() means the guard
    failed and we'd have issued the bogus SQL."""

    def connect(self):  # pragma: no cover — only fires on failure
        raise AssertionError(
            "Databricks adapter must not open an engine connection when "
            "the catalog is unset; the SHOW TABLES FROM `None`.<schema> "
            "regression is back."
        )


def test_databricks_list_tables_returns_empty_when_catalog_missing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    adapter = DatabricksAdapter(cfg=SimpleNamespace(catalog=""))
    with caplog.at_level(logging.WARNING):
        result = adapter.list_tables(_RaisingEngine(), schema="sales", catalog="")
    assert result == []
    assert any("no catalog" in rec.getMessage() for rec in caplog.records), (
        "expected a warning log explaining the missing catalog so the "
        "operator notices the misconfiguration"
    )


def test_databricks_list_tables_returns_none_when_schema_missing() -> None:
    """No schema → can't query at all; legacy ``None`` fallback is fine
    here because the inspector path also has nothing to ask for."""
    adapter = DatabricksAdapter(cfg=SimpleNamespace(catalog=""))
    assert adapter.list_tables(_RaisingEngine(), schema="", catalog="main") is None


def test_databricks_list_views_returns_empty_when_catalog_missing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    adapter = DatabricksAdapter(cfg=SimpleNamespace(catalog=""))
    with caplog.at_level(logging.WARNING):
        result = adapter.list_views(_RaisingEngine(), schema="sales", catalog=None)
    assert result == []


# ── profile_schema.validate_required_fields ─────────────────────────


def test_validate_required_fields_rejects_null_databricks_catalog() -> None:
    payload = {
        "host": "adb-1234.azuredatabricks.net",
        "http_path": "/sql/1.0/warehouses/abc",
        "access_token": "tok",
        "catalog": None,
    }
    with pytest.raises(ProfileValidationError) as excinfo:
        validate_required_fields("databricks", payload)
    assert "catalog" in excinfo.value.missing


def test_validate_required_fields_rejects_blank_databricks_catalog() -> None:
    payload = {
        "host": "adb-1234.azuredatabricks.net",
        "http_path": "/sql/1.0/warehouses/abc",
        "access_token": "tok",
        "catalog": "   ",
    }
    with pytest.raises(ProfileValidationError) as excinfo:
        validate_required_fields("databricks", payload)
    assert "catalog" in excinfo.value.missing


def test_validate_required_fields_accepts_complete_databricks_payload() -> None:
    payload = {
        "host": "adb-1234.azuredatabricks.net",
        "http_path": "/sql/1.0/warehouses/abc",
        "access_token": "tok",
        "catalog": "main",
    }
    # Should not raise.
    validate_required_fields("databricks", payload)


def test_validate_required_fields_unknown_backend_is_noop() -> None:
    """Forwards-compatible: a Studio build adding a new backend must
    not 400 against an older server that hasn't registered the spec."""
    validate_required_fields("brand-new-backend", {"anything": ""})


# ── Schema freshness annotations ────────────────────────────────────


def test_every_tool_schema_carries_freshness_annotation() -> None:
    """Every entry in ``_tool_schemas`` must be tagged ``cache_ok`` or
    ``live_only`` so the ToolBox filter has no ambiguity. A new tool
    without an annotation would silently default to ``cache_ok`` —
    catch it here instead of in production."""
    from amx.search._tool_schemas import (
        FRESHNESS_CACHE_OK,
        FRESHNESS_LIVE_ONLY,
        tool_schemas,
    )

    valid = {FRESHNESS_CACHE_OK, FRESHNESS_LIVE_ONLY}
    for entry in tool_schemas():
        name = entry.get("function", {}).get("name", "<missing>")
        freshness = entry.get("freshness")
        assert freshness in valid, (
            f"Tool {name!r} is missing or has invalid freshness "
            f"annotation ({freshness!r}). Expected one of {valid}."
        )


def test_catalog_sync_status_is_registered_as_cache_ok() -> None:
    from amx.search._tool_schemas import tool_schemas

    entry = next(e for e in tool_schemas() if e["function"]["name"] == "catalog_sync_status")
    assert entry["freshness"] == "cache_ok"


# ── ToolBox.available_schemas() filter ──────────────────────────────


class _CacheOnlyToolBox:
    """Minimal stand-in mixing in the real filter implementation."""

    _allow_live_refresh: bool

    def __init__(self, allow: bool) -> None:
        self._allow_live_refresh = allow

    schemas = staticmethod(ToolBox.schemas)  # type: ignore[assignment]
    available_schemas = ToolBox.available_schemas  # type: ignore[assignment]
    _is_live_only_tool = ToolBox._is_live_only_tool  # type: ignore[assignment]


def test_available_schemas_returns_every_tool_in_either_mode() -> None:
    """As of the needs_live_refresh envelope, live-only tools stay
    visible in the LLM's menu regardless of toggle state. The agent
    sees them, the dispatcher refuses them in cache-only mode with a
    structured payload that the SPA renders as a retry button."""
    off = _CacheOnlyToolBox(allow=False)
    on = _CacheOnlyToolBox(allow=True)
    off_names = {e["function"]["name"] for e in off.available_schemas()}
    on_names = {e["function"]["name"] for e in on.available_schemas()}
    full = {e["function"]["name"] for e in ToolBox.schemas()}
    assert off_names == full
    assert on_names == full
    # Spot-check a few key tools so a future regression of the
    # available_schemas filter shape fails this test loudly.
    for name in (
        "list_catalogs",
        "check_uniqueness",
        "inspect_data_quality",
        "find_joinable_tables",
        "catalog_sync_status",
        "catalog_coverage_summary",
        "catalog_inventory",
        "describe_column",
    ):
        assert name in off_names, f"missing {name} in cache-only schema"


def test_is_live_only_tool_classifier() -> None:
    box = _CacheOnlyToolBox(allow=False)
    assert box._is_live_only_tool("list_catalogs") is True
    assert box._is_live_only_tool("catalog_sync_status") is False
    assert box._is_live_only_tool("nonexistent_tool") is False


# ── invoke() refuses live-only tools in cache-only mode ─────────────


def test_invoke_returns_needs_live_refresh_envelope(tmp_path) -> None:
    """End-to-end through the real ``ToolBox.invoke`` path. Live-only
    tool in cache-only mode must return ``needs_live_refresh: true``
    with the tool name, the user's arguments, the reason, and the
    user_action string — the SPA pulls these to render the retry
    button."""
    import json

    from amx.config import AMXConfig
    from amx.search.agent_tools import ToolBox
    from amx.search.catalog import SearchCatalog
    from amx.storage.sqlite_store import SQLiteHistoryStore

    db_path = tmp_path / "history.db"
    store = SQLiteHistoryStore(db_path)
    store.init()
    cat = SearchCatalog(db_path)
    cfg = AMXConfig()
    cfg.db.backend = "postgresql"
    box = ToolBox(cfg, cat, db_profiles=["prod-pg"], allow_live_refresh=False)

    # Sanity: live_only tool is still in the menu, the LLM sees it.
    assert any(e["function"]["name"] == "check_uniqueness" for e in box.available_schemas())

    # Dispatch — must refuse with the rich envelope.
    raw = box.invoke(
        "check_uniqueness",
        json.dumps({"table": "orders", "columns": ["order_id"]}),
    )
    decoded = json.loads(raw)
    assert decoded["needs_live_refresh"] is True
    assert decoded["tool"] == "check_uniqueness"
    assert decoded["arguments"] == {
        "table": "orders",
        "columns": ["order_id"],
    }
    assert "Live refresh" in decoded["user_action"]
    assert "cache" in decoded["reason"].lower()
    # Back-compat key for older frontends.
    assert decoded["error"] == "live_only_tool_disabled"


# ── _tool_catalog_sync_status shape ─────────────────────────────────


def test_tool_catalog_sync_status_returns_per_profile_freshness() -> None:
    """The new freshness tool returns one row per profile in scope
    with the documented keys, NEVER touches the live DB."""
    import time

    box = object.__new__(ToolBox)
    # Minimum surface the method needs.
    box._allow_live_refresh = False  # type: ignore[attr-defined]
    box.db_profiles = ["prod-pg", "dbr-oyk"]  # type: ignore[attr-defined]

    class _StubCatalog:
        def __init__(self) -> None:
            self.now = time.time()

        def get_profile_state(self, name: str) -> dict[str, object]:
            if name == "prod-pg":
                return {
                    "state": "done",
                    "total_tables": 100,
                    "processed_tables": 100,
                    "started_at": self.now - 1000,
                    "finished_at": self.now - 900,
                    "last_full_sync_at": self.now - 900,
                    "last_error": "",
                }
            return {
                "state": "syncing",
                "total_tables": 20,
                "processed_tables": 5,
                "started_at": self.now - 30,
                "finished_at": None,
                "last_full_sync_at": None,
                "last_error": "",
            }

    box.catalog = _StubCatalog()  # type: ignore[attr-defined]

    # ``_now`` is the SyncMixin clock-now helper on the real ToolBox;
    # we don't have the mixin attached on the bare object, so stub it.
    box._now = time.time  # type: ignore[attr-defined]

    result = ToolBox._tool_catalog_sync_status(box, db_profile="")
    assert isinstance(result, dict)
    assert result["scope"] == ["prod-pg", "dbr-oyk"]
    profiles = {row["db_profile"]: row for row in result["profiles"]}
    assert set(profiles) == {"prod-pg", "dbr-oyk"}
    # Done profile: fresh under 24h.
    prod = profiles["prod-pg"]
    assert prod["state"] == "done"
    assert prod["is_fresh_24h"] is True
    assert prod["last_synced_at"] is not None
    assert prod["age_seconds"] is not None
    # Syncing profile: no last_full_sync, both fresh flags False.
    dbr = profiles["dbr-oyk"]
    assert dbr["state"] == "syncing"
    assert dbr["last_synced_at"] is None
    assert dbr["age_seconds"] is None
    assert dbr["is_fresh_24h"] is False
    assert dbr["is_fresh_7d"] is False


def test_tool_catalog_sync_status_filters_to_named_profile() -> None:
    import time

    box = object.__new__(ToolBox)
    box._allow_live_refresh = False  # type: ignore[attr-defined]
    box.db_profiles = ["prod-pg", "dbr-oyk"]  # type: ignore[attr-defined]
    box._now = time.time  # type: ignore[attr-defined]

    class _StubCatalog:
        def get_profile_state(self, name: str) -> dict[str, object]:
            return {
                "state": "done",
                "total_tables": 0,
                "processed_tables": 0,
                "started_at": None,
                "finished_at": None,
                "last_full_sync_at": time.time(),
                "last_error": "",
            }

    box.catalog = _StubCatalog()  # type: ignore[attr-defined]
    result = ToolBox._tool_catalog_sync_status(box, db_profile="prod-pg")
    assert [row["db_profile"] for row in result["profiles"]] == ["prod-pg"]


def test_tool_catalog_coverage_summary_returns_per_schema_counts(
    tmp_path,
) -> None:
    """Seed two profiles × two schemas with a known documented /
    undocumented mix and assert the tool returns the right counts +
    totals + percentages. No live DB involvement."""
    from amx.config import AMXConfig
    from amx.search.agent_tools import ToolBox
    from amx.search.catalog import SearchCatalog
    from amx.storage.sqlite_store import SQLiteHistoryStore

    db_path = tmp_path / "history.db"
    store = SQLiteHistoryStore(db_path)
    store.init()
    cat = SearchCatalog(db_path)

    # prod-pg.public: 2 tables (1 documented), 4 columns (2 documented)
    cat.record_applied_description(
        db_profile="prod-pg",
        db_backend="postgresql",
        database_name="",
        schema_name="public",
        table_name="orders",
        column_name=None,
        entity_kind="table",
        asset_kind="table",
        description="Order header",
    )
    # Add an undocumented table by upserting via _upsert_entity through
    # a no-op description. Easiest: use record_applied_description with
    # description="" then null out the effective pointer manually.
    import sqlite3

    with sqlite3.connect(db_path) as raw:
        raw.row_factory = sqlite3.Row
        raw.execute(
            """INSERT INTO catalog_entities (
                  db_profile, db_backend, database_name, schema_name,
                  table_name, entity_kind, asset_kind, updated_at,
                  last_synced_at
              ) VALUES (
                  'prod-pg', 'postgresql', '', 'public',
                  'users', 'table', 'table', 0, 100
              )"""
        )
        # 4 columns under prod-pg.public: 2 documented, 2 undocumented
        for i, (table, col, described) in enumerate(
            [
                ("orders", "order_id", True),
                ("orders", "amount", False),
                ("users", "id", True),
                ("users", "email", False),
            ]
        ):
            raw.execute(
                """INSERT INTO catalog_entities (
                      db_profile, db_backend, database_name, schema_name,
                      table_name, column_name, entity_kind, asset_kind,
                      effective_description_id, updated_at, last_synced_at
                  ) VALUES (
                      'prod-pg', 'postgresql', '', 'public',
                      ?, ?, 'column', 'table',
                      ?, 0, 100
                  )""",
                (table, col, 1 if described else None),
            )
        # dbr-oyk.amx_test: 1 table, all documented
        raw.execute(
            """INSERT INTO catalog_entities (
                  db_profile, db_backend, database_name, schema_name,
                  table_name, entity_kind, asset_kind,
                  effective_description_id, updated_at, last_synced_at
              ) VALUES (
                  'dbr-oyk', 'databricks', 'amx_test', 'amx_test',
                  'adrc', 'table', 'table', 1, 0, 200
              )"""
        )
        raw.commit()

    cfg = AMXConfig()
    cfg.db.backend = "postgresql"
    box = ToolBox(cfg, cat, db_profiles=["prod-pg", "dbr-oyk"], allow_live_refresh=False)

    result = box._tool_catalog_coverage_summary()
    assert "error" not in result
    rows = {(r["db_profile"], r["schema"]): r for r in result["profiles"]}
    assert ("prod-pg", "public") in rows
    assert ("dbr-oyk", "amx_test") in rows
    public = rows[("prod-pg", "public")]
    assert public["total_tables"] == 2
    assert public["undocumented_tables"] == 1  # users has no description
    assert public["total_columns"] == 4
    assert public["undocumented_columns"] == 2
    assert public["table_coverage_pct"] == 50.0
    assert public["column_coverage_pct"] == 50.0
    dbr = rows[("dbr-oyk", "amx_test")]
    assert dbr["total_tables"] == 1
    assert dbr["undocumented_tables"] == 0
    # Totals across scope
    totals = result["totals"]
    assert totals["total_tables"] == 3
    assert totals["undocumented_tables"] == 1
    assert totals["total_columns"] == 4
    assert totals["undocumented_columns"] == 2


def test_tool_catalog_coverage_summary_rejects_out_of_scope_profile(
    tmp_path,
) -> None:
    from amx.config import AMXConfig
    from amx.search.agent_tools import ToolBox
    from amx.search.catalog import SearchCatalog
    from amx.storage.sqlite_store import SQLiteHistoryStore

    db_path = tmp_path / "history.db"
    store = SQLiteHistoryStore(db_path)
    store.init()
    cat = SearchCatalog(db_path)
    cfg = AMXConfig()
    cfg.db.backend = "postgresql"
    box = ToolBox(cfg, cat, db_profiles=["prod-pg"], allow_live_refresh=False)
    result = box._tool_catalog_coverage_summary(db_profile="someone-else")
    assert "error" in result
    assert "not in this Ask's scope" in result["error"]


def test_tool_catalog_sync_status_unknown_profile_returns_empty() -> None:
    import time

    box = object.__new__(ToolBox)
    box._allow_live_refresh = False  # type: ignore[attr-defined]
    box.db_profiles = ["prod-pg"]  # type: ignore[attr-defined]
    box._now = time.time  # type: ignore[attr-defined]
    box.catalog = type("C", (), {"get_profile_state": lambda self, n: {}})()  # type: ignore[attr-defined]
    result = ToolBox._tool_catalog_sync_status(box, db_profile="not-a-real-profile")
    assert result["profiles"] == []
