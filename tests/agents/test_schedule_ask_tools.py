"""Verify the Ask-agent ``list_schedules`` / ``get_schedule`` tools.

These confirm the JSON-schema entry exists for the LLM tool-list path
and that ``ToolBox.invoke('list_schedules', ...)`` /
``ToolBox.invoke('get_schedule', ...)`` go through the dispatcher and
return read-only payloads. Real LLM round-trip is out of scope for a
unit test; the tool registration + handler contract is what we care
about.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from amx.search.agent_tools import ToolBox
from amx.storage import sqlite_store as _store_module
from amx.storage.sqlite_store import SQLiteHistoryStore


@pytest.fixture
def store(tmp_path: Path) -> SQLiteHistoryStore:
    s = SQLiteHistoryStore(tmp_path / "history.db")
    s.init()
    _store_module._store = s
    yield s
    _store_module._store = None


@pytest.fixture
def toolbox(store: SQLiteHistoryStore) -> ToolBox:
    """Build a minimal ToolBox that does not touch the live DB.

    The ToolBox constructor wants a config + a catalog object. For the
    schedule tools we only need ``_history_store`` to resolve to the
    pinned singleton, so we patch ``__init__`` to skip the heavy setup.
    """
    box = ToolBox.__new__(ToolBox)  # type: ignore[call-arg]
    # Minimal attributes used by invoke() + cache.
    box._UNCACHED_TOOLS = frozenset()  # type: ignore[attr-defined]
    box._tool_cache = {}  # type: ignore[attr-defined]
    box._tool_cache_hits = 0  # type: ignore[attr-defined]
    box.db_profiles = ()  # type: ignore[attr-defined]
    box.cfg = MagicMock()
    return box


def _create_schedule(store: SQLiteHistoryStore, **kw):
    defaults = {
        "name": "test",
        "fire_at_utc": time.time() + 3600,
        "fire_at_tz": "Europe/Istanbul",
        "db_profile": "prod_sf",
        "scope_json": json.dumps({"mode": "all"}),
        "llm_profile": "claude",
        "review_strategy": "auto",
    }
    defaults.update(kw)
    return store.create_scheduled_run(**defaults)


def test_schemas_lists_list_schedules_and_get_schedule() -> None:
    schemas = ToolBox.schemas()
    names = {s["function"]["name"] for s in schemas}
    assert "list_schedules" in names
    assert "get_schedule" in names


def test_list_schedules_default_returns_active(toolbox: ToolBox, store: SQLiteHistoryStore) -> None:
    active = _create_schedule(store, name="upcoming")
    done = _create_schedule(store, name="finished")
    store.set_scheduled_run_status(done, "running")
    store.set_scheduled_run_status(done, "completed")

    result = json.loads(toolbox.invoke("list_schedules", "{}"))
    ids = [r["id"] for r in result["schedules"]]
    assert active in ids
    assert done not in ids


def test_list_schedules_past_filter(toolbox: ToolBox, store: SQLiteHistoryStore) -> None:
    _create_schedule(store, name="upcoming")
    done = _create_schedule(store, name="finished")
    store.set_scheduled_run_status(done, "running")
    store.set_scheduled_run_status(done, "completed")

    result = json.loads(toolbox.invoke("list_schedules", json.dumps({"filter": "past"})))
    ids = {r["id"] for r in result["schedules"]}
    assert ids == {done}


def test_list_schedules_filters_by_db_profile(toolbox: ToolBox, store: SQLiteHistoryStore) -> None:
    a = _create_schedule(store, db_profile="prod_sf")
    _create_schedule(store, db_profile="stg")
    result = json.loads(toolbox.invoke("list_schedules", json.dumps({"db_profile": "prod_sf"})))
    ids = {r["id"] for r in result["schedules"]}
    assert ids == {a}


def test_get_schedule_returns_full_record(toolbox: ToolBox, store: SQLiteHistoryStore) -> None:
    sid = _create_schedule(store, name="detailed")
    result = json.loads(toolbox.invoke("get_schedule", json.dumps({"schedule_id": sid})))
    assert "schedule" in result
    assert result["schedule"]["id"] == sid
    assert result["schedule"]["name"] == "detailed"


def test_get_schedule_returns_error_for_unknown_id(
    toolbox: ToolBox, store: SQLiteHistoryStore
) -> None:
    result = json.loads(toolbox.invoke("get_schedule", json.dumps({"schedule_id": 99999})))
    assert "error" in result
