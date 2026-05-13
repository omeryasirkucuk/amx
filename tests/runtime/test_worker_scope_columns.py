"""Tests for the ``columns`` scope mode (Phase 2c follow-up).

Verifies ``_parse_scope`` collapses column-level entries into the coarse
``{schema: [tables]}`` shape that ``analysis_runs.scope`` accepts while
still letting the future executor restrict its per-column work via the
saved scope_json payload.
"""

from __future__ import annotations

import json

from amx.runtime.worker import _parse_scope


def test_columns_mode_collapses_same_table() -> None:
    scope_json = json.dumps(
        {
            "mode": "columns",
            "columns": [
                {"schema": "public", "table": "users", "column": "id"},
                {"schema": "public", "table": "users", "column": "email"},
                {"schema": "public", "table": "orders", "column": "id"},
                {"schema": "staging", "table": "events", "column": "ts"},
            ],
        }
    )
    out = _parse_scope(scope_json)
    assert set(out.keys()) == {"public", "staging"}
    assert sorted(out["public"]) == sorted(["users", "orders"])
    assert out["staging"] == ["events"]


def test_columns_mode_ignores_malformed_entries() -> None:
    scope_json = json.dumps(
        {
            "mode": "columns",
            "columns": [
                {"schema": "public", "table": "users", "column": "id"},
                "not-a-dict",
                {"schema": "", "table": "users", "column": "x"},
                {"schema": "public", "column": "id"},  # missing table
                None,
            ],
        }
    )
    out = _parse_scope(scope_json)
    assert out == {"public": ["users"]}


def test_unknown_mode_returns_empty() -> None:
    out = _parse_scope(json.dumps({"mode": "wat", "schemas": ["x"]}))
    assert out == {}
