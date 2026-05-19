"""Hive adapter contract tests (no live Hive cluster required).

Covers the parts of the adapter that compile without `pyhive` installed
or a live HiveServer2 reachable:

* Capability flags (column comments OFF, table/view/schema/database ON).
* Comment writeback SQL templates compile with backtick quoting and
  inlined-literal escaping.
* ``set_column_comment_sql`` raises ``UnsupportedDatabaseOperation``
  with a clear explanation (the connector layer also gates this via
  the capability flag, but the adapter must raise cleanly when called
  directly).
* ``_parse_describe_formatted`` correctly extracts the table comment
  and column comments from a representative ``DESCRIBE FORMATTED``
  output captured against ``apache/hive:4.0.0``.
"""

from __future__ import annotations

import unittest

from amx.config import DBConfig
from amx.db.adapters.base import UnsupportedDatabaseOperation
from amx.db.adapters.hive import HiveAdapter


def _cfg(**overrides) -> DBConfig:
    base = {
        "backend": "hive",
        "host": "hive.example.com",
        "port": 10000,
        "user": "alice",
        "password": "secret",
        "database": "warehouse",
        "auth_mode": "PLAIN",
    }
    base.update(overrides)
    return DBConfig(**base)


class HiveCapabilityTests(unittest.TestCase):
    def test_column_comments_off(self) -> None:
        adapter = HiveAdapter(_cfg())
        self.assertFalse(adapter.capabilities.column_comments)

    def test_table_view_database_comments_on(self) -> None:
        caps = HiveAdapter(_cfg()).capabilities
        self.assertTrue(caps.table_comments)
        self.assertTrue(caps.view_comments)
        self.assertTrue(caps.database_comments)
        self.assertTrue(caps.schema_comments)
        self.assertFalse(caps.materialized_view_comments)

    def test_shared_history_off(self) -> None:
        # Hive cannot host AMX's run-history schema — row UPDATE is
        # partition-/transactional-table-only.
        self.assertFalse(HiveAdapter(_cfg()).capabilities.supports_shared_history)


class HiveCommentSqlTests(unittest.TestCase):
    def test_table_comment_uses_tblproperties(self) -> None:
        sql = HiveAdapter(_cfg()).set_table_comment_sql("warehouse", "orders", "TABLE")
        self.assertEqual(
            sql,
            "ALTER TABLE `warehouse`.`orders` SET TBLPROPERTIES ('comment' = :cmt)",
        )

    def test_view_comment_uses_alter_view(self) -> None:
        sql = HiveAdapter(_cfg()).set_table_comment_sql("warehouse", "v_orders", "VIEW")
        self.assertEqual(
            sql,
            "ALTER VIEW `warehouse`.`v_orders` SET TBLPROPERTIES ('comment' = :cmt)",
        )

    def test_materialized_view_rejected(self) -> None:
        with self.assertRaises(UnsupportedDatabaseOperation):
            HiveAdapter(_cfg()).set_table_comment_sql("warehouse", "mv_orders", "MATERIALIZED VIEW")

    def test_schema_comment_uses_dbproperties(self) -> None:
        sql = HiveAdapter(_cfg()).set_schema_comment_sql("warehouse")
        self.assertEqual(
            sql,
            "ALTER DATABASE `warehouse` SET DBPROPERTIES ('comment' = :cmt)",
        )

    def test_database_comment_uses_profile_db(self) -> None:
        sql = HiveAdapter(_cfg(database="analytics")).set_database_comment_sql()
        self.assertEqual(
            sql,
            "ALTER DATABASE `analytics` SET DBPROPERTIES ('comment' = :cmt)",
        )

    def test_database_comment_without_pinned_db_raises(self) -> None:
        with self.assertRaises(UnsupportedDatabaseOperation):
            HiveAdapter(_cfg(database="")).set_database_comment_sql()

    def test_column_comment_raises_with_rationale(self) -> None:
        with self.assertRaises(UnsupportedDatabaseOperation) as cm:
            HiveAdapter(_cfg()).set_column_comment_sql("warehouse", "orders", "o_id")
        msg = str(cm.exception)
        self.assertIn("Column comment write-back on Hive", msg)
        self.assertIn("re-declaring", msg.lower())

    def test_comment_literal_escaping(self) -> None:
        adapter = HiveAdapter(_cfg())
        template = adapter.set_table_comment_sql("warehouse", "orders", "TABLE")
        sql, params = adapter.comment_sql_with_params(
            template,
            "the customer's address",
        )
        # Single quotes doubled per ANSI rules, no bind params remain.
        self.assertIn("'the customer''s address'", sql)
        self.assertEqual(params, {})

    def test_multi_column_comments_returns_none(self) -> None:
        # Column comments are off — there is no bulk variant either.
        self.assertIsNone(
            HiveAdapter(_cfg()).set_multi_column_comments_sql(
                "warehouse", "orders", [("col1", "x"), ("col2", "y")]
            )
        )


class HiveDescribeFormattedParserTests(unittest.TestCase):
    """The fallback path for Hive 2.x parses ``DESCRIBE FORMATTED`` output.

    Hive 4.0.0 row shape is captured below for representative coverage.
    """

    def test_parse_typical_hive4_output(self) -> None:
        rows = [
            ("# col_name", "data_type", "comment"),
            ("id", "int", "Surrogate id"),
            ("ts", "timestamp", "Event time"),
            ("payload", "struct<k:string,v:string>", ""),
            ("", "", ""),
            ("# Detailed Table Information", "", ""),
            ("Database:", "warehouse", ""),
            ("Owner:", "amx", ""),
            ("", "", ""),
            ("# Table Parameters:", "", ""),
            ("", "comment", "Master events fact table"),
            ("", "transactional", "true"),
            ("", "", ""),
            ("# Storage Information", "", ""),
            ("SerDe Library:", "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe", ""),
        ]
        comment, columns = HiveAdapter._parse_describe_formatted(rows)
        self.assertEqual(comment, "Master events fact table")
        self.assertEqual(columns["id"], "Surrogate id")
        self.assertEqual(columns["ts"], "Event time")
        self.assertIsNone(columns["payload"])  # blank comment → None

    def test_parse_table_without_comment(self) -> None:
        rows = [
            ("# col_name", "data_type", "comment"),
            ("a", "string", "alpha"),
            ("", "", ""),
            ("# Detailed Table Information", "", ""),
            ("Database:", "warehouse", ""),
            ("", "", ""),
            ("# Table Parameters:", "", ""),
            ("", "transient_lastDdlTime", "1716081234"),
        ]
        comment, columns = HiveAdapter._parse_describe_formatted(rows)
        self.assertIsNone(comment)
        self.assertEqual(columns, {"a": "alpha"})

    def test_parse_real_hive4_output(self) -> None:
        # Captured verbatim from ``apache/hive:4.0.0`` after
        # ``ALTER TABLE … SET TBLPROPERTIES ('comment' = 'AMX writeback')``.
        # No ``# col_name`` header row, no ``#`` on ``Table Parameters:``.
        rows = [
            ("id", "int", "Surrogate id"),
            ("ts", "timestamp", "Event time"),
            ("", None, None),
            ("# Detailed Table Information", None, None),
            ("Database:           ", "amx_smoke           ", None),
            ("OwnerType:          ", "USER                ", None),
            ("Owner:              ", "hive                ", None),
            ("CreateTime:         ", "Tue May 19 09:18:54 UTC 2026", None),
            ("Retention:          ", "0                   ", None),
            ("Location:           ", "file:/opt/hive/data/warehouse/amx_smoke.db/events", None),
            ("Table Type:         ", "EXTERNAL_TABLE      ", None),
            ("Table Parameters:", None, None),
            ("", "EXTERNAL            ", "TRUE                "),
            ("", "TRANSLATED_TO_EXTERNAL", "TRUE                "),
            ("", "bucketing_version   ", "2                   "),
            ("", "comment             ", "AMX writeback comment"),
            ("", "external.table.purge", "TRUE                "),
            ("", "last_modified_by    ", "hive                "),
            ("", "last_modified_time  ", "1779182334          "),
            ("", "numFiles            ", "0                   "),
            ("", "transient_lastDdlTime", "1779182334          "),
            ("", None, None),
            ("# Storage Information", None, None),
            (
                "SerDe Library:      ",
                "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                None,
            ),
        ]
        comment, columns = HiveAdapter._parse_describe_formatted(rows)
        self.assertEqual(comment, "AMX writeback comment")
        self.assertEqual(columns, {"id": "Surrogate id", "ts": "Event time"})


class HiveAuthModeValidationTests(unittest.TestCase):
    def test_unknown_auth_mode_rejected_at_create_engine(self) -> None:
        # We can't reach the real create_engine without pyhive installed,
        # so test the validation gate via direct check.
        adapter = HiveAdapter(_cfg(auth_mode="MAGIC"))
        # If pyhive is unavailable the ImportError fires first; either
        # outcome is acceptable as long as the adapter never silently
        # ships a garbage auth_mode to the driver.
        with self.assertRaises((ValueError, ImportError)):
            adapter.create_engine()


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
