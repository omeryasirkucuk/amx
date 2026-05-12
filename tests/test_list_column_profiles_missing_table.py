"""Regression: ``list_column_profiles`` must not crash the code-analyze
worker when a table named in the codebase isn't present in the live DB.

Symptom (reproducer in the original report): a user connected a
PostgreSQL profile, linked an SAP codebase via GitHub, and clicked
Search. The code agent surfaced ``sap_s6p.vbrk`` (an SAP-specific
table name from the source files) and the analyze worker called
``connector.list_column_profiles("sap_s6p", "vbrk")``. PostgreSQL
introspection raised ``sqlalchemy.exc.NoSuchTableError`` and the
worker died, taking down the analyze job.

After the fix the call returns an empty list and the worker continues
processing the remaining suggestions.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from sqlalchemy.exc import NoSuchTableError


def test_list_column_profiles_returns_empty_when_table_missing():
    from amx.db.connector import DatabaseConnector

    conn = DatabaseConnector.__new__(DatabaseConnector)
    conn._engine = MagicMock()  # property getter reads from here
    conn._normalize_id = lambda v: v  # type: ignore[method-assign]

    fake_inspector = MagicMock()
    fake_inspector.get_columns.side_effect = NoSuchTableError("sap_s6p.vbrk")

    with patch("amx.db.connector.inspect", return_value=fake_inspector):
        out = DatabaseConnector.list_column_profiles(conn, "sap_s6p", "vbrk")
    assert out == []


def test_list_column_profiles_returns_columns_when_table_present():
    from amx.db.connector import DatabaseConnector

    conn = DatabaseConnector.__new__(DatabaseConnector)
    conn._engine = MagicMock()
    conn._normalize_id = lambda v: v  # type: ignore[method-assign]

    fake_inspector = MagicMock()
    fake_inspector.get_columns.return_value = [
        {"name": "id", "type": "INTEGER", "nullable": False},
        {"name": "label", "type": "TEXT", "nullable": True},
    ]

    with patch("amx.db.connector.inspect", return_value=fake_inspector):
        out = DatabaseConnector.list_column_profiles(conn, "public", "users")
    assert [c.name for c in out] == ["id", "label"]
    assert out[0].nullable is False
    assert out[1].nullable is True
