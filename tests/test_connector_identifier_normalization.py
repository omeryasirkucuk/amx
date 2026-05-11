"""Connector entry points fold user-supplied identifiers via the adapter.

A lowercase ``scott.employees`` typed into ``/db sample`` used to reach
the SQLAlchemy inspector unchanged, miss the upper-case Oracle/Snowflake
storage, and return empty results. The connector now calls
``adapter.normalize_identifier`` before any inspector call.
"""

from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from amx.config import DBConfig
from amx.db.adapters.base import BackendCapabilities
from amx.db.connector import DatabaseConnector


class _FakeInspector:
    def __init__(self) -> None:
        self.table_calls: list[tuple[str, ...]] = []
        self.view_calls: list[str] = []
        self.column_calls: list[tuple[str, str]] = []

    def get_table_names(self, schema: str) -> list[str]:  # noqa: D401
        self.table_calls.append(schema)
        return ["EMPLOYEES"] if schema == "SCOTT" else []

    def get_view_names(self, schema: str) -> list[str]:
        self.view_calls.append(schema)
        return []

    def get_columns(self, table: str, schema: str) -> list[dict]:
        self.column_calls.append((schema, table))
        return [{"name": "ID", "type": "INTEGER", "nullable": False, "comment": None}]

    def get_table_comment(self, table: str, schema: str) -> dict:
        return {"text": None}


class _UpperFoldAdapter:
    """Stub adapter that mimics Oracle's UPPER-folding behaviour."""

    name = "fake_oracle"
    capabilities = BackendCapabilities(column_comments=True)

    def normalize_identifier(self, value: str) -> str:
        if not value:
            return value
        if len(value) >= 2 and value.startswith('"') and value.endswith('"'):
            return value
        return value.upper()

    # Connector falls back to SQLAlchemy inspector when these return None.
    def list_tables(self, engine, schema, catalog=""):
        return None

    def list_views(self, engine, schema, catalog=""):
        return None


def _connector_with(adapter) -> DatabaseConnector:
    db = object.__new__(DatabaseConnector)
    db.cfg = DBConfig(backend="postgresql")
    db._engine = SimpleNamespace()
    db._adapter = adapter
    return db


class ConnectorNormalizationTests(unittest.TestCase):
    def test_list_tables_folds_schema_for_inspector(self) -> None:
        adapter = _UpperFoldAdapter()
        connector = _connector_with(adapter)
        insp = _FakeInspector()
        with patch("amx.db.connector.inspect", return_value=insp):
            result = connector.list_tables("scott")
        self.assertEqual(insp.table_calls, ["SCOTT"])
        self.assertEqual(result, ["EMPLOYEES"])

    def test_list_views_folds_schema(self) -> None:
        adapter = _UpperFoldAdapter()
        connector = _connector_with(adapter)
        insp = _FakeInspector()
        with patch("amx.db.connector.inspect", return_value=insp):
            connector.list_views("scott")
        self.assertEqual(insp.view_calls, ["SCOTT"])

    def test_get_column_comments_folds_both(self) -> None:
        adapter = _UpperFoldAdapter()
        connector = _connector_with(adapter)
        insp = _FakeInspector()
        with patch("amx.db.connector.inspect", return_value=insp):
            connector.get_column_comments("scott", "employees")
        self.assertEqual(insp.column_calls, [("SCOTT", "EMPLOYEES")])

    def test_missing_method_falls_back_to_identity(self) -> None:
        """Test fakes without ``normalize_identifier`` must not crash."""

        class _MinimalAdapter:
            name = "minimal"
            capabilities = BackendCapabilities()

            def list_tables(self, engine, schema, catalog=""):
                return None

        connector = _connector_with(_MinimalAdapter())
        insp = _FakeInspector()
        with patch("amx.db.connector.inspect", return_value=insp):
            connector.list_tables("public")
        self.assertEqual(insp.table_calls, ["public"])


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
