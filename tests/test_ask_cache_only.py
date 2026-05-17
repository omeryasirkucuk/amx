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
    suppressed = [
        rec for rec in caplog.records if "force_fresh suppressed" in rec.getMessage()
    ]
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
    assert (
        adapter.list_tables(_RaisingEngine(), schema="", catalog="main") is None
    )


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
