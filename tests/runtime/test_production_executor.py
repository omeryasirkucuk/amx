"""Setup-path tests for ``production_run_executor``.

The happy path of the executor calls a real LLM + DB, which a unit
test cannot drive. These tests cover the fail-fast guards that fire
before any expensive setup: missing payload fields, profile gone
from disk, scope expansion against a no-op DB.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import patch

import pytest

from amx.runtime.worker import _resolve_live_scope, production_run_executor


def test_executor_rejects_missing_db_profile_in_payload() -> None:
    with pytest.raises(ValueError, match="missing db_profile"):
        production_run_executor(
            1,
            {"id": 7, "db_profile": "", "llm_profile": "claude"},
        )


def test_executor_rejects_missing_llm_profile_in_payload() -> None:
    with pytest.raises(ValueError, match="missing db_profile or llm_profile"):
        production_run_executor(
            1,
            {"id": 7, "db_profile": "prod_sf", "llm_profile": ""},
        )


def test_executor_surfaces_keyerror_when_profile_deleted(tmp_path) -> None:
    """When the schedule was created against a profile that no longer
    exists on disk, the executor raises a clear KeyError naming the
    missing profile."""

    class _Cfg:
        db_profiles: dict[str, Any] = {}
        llm_profiles: dict[str, Any] = {}

    with patch("amx.config.AMXConfig.load", return_value=_Cfg()):
        with pytest.raises(KeyError, match="prod_sf"):
            production_run_executor(
                1,
                {
                    "id": 7,
                    "db_profile": "prod_sf",
                    "llm_profile": "claude",
                },
            )


# ── Scope resolution against a stub connector ────────────────────────


class _StubKind:
    """Stand in for the real AssetKind enum the connector returns."""

    def __init__(self, name: str) -> None:
        self.name = name


class _StubDB:
    def __init__(
        self,
        schemas: list[str],
        assets: dict[str, list[tuple[str, _StubKind]]],
    ) -> None:
        self._schemas = schemas
        self._assets = assets

    def list_schemas(self) -> list[str]:
        return self._schemas

    def list_assets(self, schema: str) -> list[tuple[str, _StubKind]]:
        return self._assets.get(schema, [])


def test_resolve_live_scope_mode_all_enumerates_everything() -> None:
    db = _StubDB(
        schemas=["public", "staging"],
        assets={
            "public": [("users", _StubKind("TABLE")), ("orders", _StubKind("VIEW"))],
            "staging": [("events", _StubKind("TABLE"))],
        },
    )
    out = _resolve_live_scope(json.dumps({"mode": "all"}), db)
    assert sorted(out.keys()) == ["public", "staging"]
    assert sorted(out["public"]) == sorted(["users", "orders"])
    assert out["staging"] == ["events"]


def test_resolve_live_scope_mode_schemas_filters() -> None:
    db = _StubDB(
        schemas=["public", "staging"],
        assets={
            "public": [("users", _StubKind("TABLE"))],
            "staging": [("events", _StubKind("TABLE"))],
        },
    )
    out = _resolve_live_scope(
        json.dumps({"mode": "schemas", "schemas": ["public"]}), db
    )
    assert out == {"public": ["users"]}


def test_resolve_live_scope_mode_schemas_skips_missing() -> None:
    db = _StubDB(
        schemas=["public"],
        assets={"public": [("users", _StubKind("TABLE"))]},
    )
    out = _resolve_live_scope(
        json.dumps({"mode": "schemas", "schemas": ["public", "nope"]}), db
    )
    assert out == {"public": ["users"]}


def test_resolve_live_scope_mode_tables_round_trips() -> None:
    db = _StubDB(schemas=[], assets={})
    out = _resolve_live_scope(
        json.dumps(
            {
                "mode": "tables",
                "tables": [
                    {"schema": "public", "table": "users"},
                    {"schema": "public", "table": "orders"},
                ],
            }
        ),
        db,
    )
    assert out == {"public": ["users", "orders"]}


def test_resolve_live_scope_mode_columns_collapses_to_tables() -> None:
    db = _StubDB(schemas=[], assets={})
    out = _resolve_live_scope(
        json.dumps(
            {
                "mode": "columns",
                "columns": [
                    {"schema": "public", "table": "users", "column": "id"},
                    {"schema": "public", "table": "users", "column": "email"},
                ],
            }
        ),
        db,
    )
    assert out == {"public": ["users"]}


def test_resolve_live_scope_filters_column_assets() -> None:
    """``list_assets`` returns columns alongside tables on some
    backends; the resolver must keep only TABLE / VIEW kinds."""
    db = _StubDB(
        schemas=["public"],
        assets={
            "public": [
                ("users", _StubKind("TABLE")),
                ("email", _StubKind("COLUMN")),
            ],
        },
    )
    out = _resolve_live_scope(json.dumps({"mode": "all"}), db)
    assert out == {"public": ["users"]}
