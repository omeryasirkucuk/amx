"""Snowflake adapter emits a 3-level FQN when the profile pins a database.

Background: ``fully_qualified_name`` returned ``"schema"."table"`` regardless
of whether the profile bound a database. Comment-write DDL
(``COMMENT ON TABLE``) silently targeted the connection's active database,
which broke once an agent / cross-database query needed to reference an
object outside the active DB. The fix emits a 3-level
``"db"."schema"."table"`` whenever ``cfg.database`` is non-empty.
"""

from __future__ import annotations

import unittest

from amx.config import DBConfig
from amx.db.adapters.snowflake import SnowflakeAdapter


class SnowflakeFullyQualifiedNameTests(unittest.TestCase):
    def test_three_level_when_database_set(self) -> None:
        adapter = SnowflakeAdapter(DBConfig(backend="snowflake", database="ANALYTICS"))
        self.assertEqual(
            adapter.fully_qualified_name("public", "orders"),
            '"ANALYTICS"."public"."orders"',
        )

    def test_two_level_when_database_blank(self) -> None:
        adapter = SnowflakeAdapter(DBConfig(backend="snowflake", database=""))
        self.assertEqual(
            adapter.fully_qualified_name("public", "orders"),
            '"public"."orders"',
        )

    def test_set_table_comment_sql_uses_three_level(self) -> None:
        adapter = SnowflakeAdapter(DBConfig(backend="snowflake", database="DW"))
        stmt = adapter.set_table_comment_sql("sales", "orders", "TABLE")
        self.assertIn('"DW"."sales"."orders"', stmt)
        self.assertIn(":cmt", stmt)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
