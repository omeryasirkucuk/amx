"""``profiling_approximate`` swaps COUNT(DISTINCT) for HLL on metered backends.

BigQuery, Snowflake, and Databricks all bill per row scanned. The
exact ``COUNT(DISTINCT col)`` aggregate hashes every sampled row
regardless of TABLESAMPLE / SAMPLE, which on wide / high-cardinality
columns racks up credits even with a 1 % slice. Setting
``DBConfig.profiling_approximate = True`` switches the per-column
and bulk profile queries to the backend's native HyperLogLog variant:

* BigQuery: ``APPROX_COUNT_DISTINCT(col)``
* Snowflake: ``APPROX_COUNT_DISTINCT(col)``
* Databricks Spark SQL: ``approx_count_distinct(col)``

The default is False so existing behaviour is preserved for users who
were happy with the exact-but-expensive path.
"""

from __future__ import annotations

import unittest

from amx.config import DBConfig
from amx.db.adapters.bigquery import BigQueryAdapter
from amx.db.adapters.databricks import DatabricksAdapter
from amx.db.adapters.snowflake import SnowflakeAdapter


class ApproximateProfilingDistinctSqlTests(unittest.TestCase):
    def test_bigquery_uses_approx_when_flag_on(self) -> None:
        cfg = DBConfig(backend="bigquery", project="p", profiling_approximate=True)
        adapter = BigQueryAdapter(cfg)
        col = adapter.quote_identifier("amount")

        per_col = adapter.column_stats_sql("`p`.`ds`.`orders`", col)
        bulk = adapter.column_stats_bulk_sql("`p`.`ds`.`orders`", [col])

        self.assertIn("APPROX_COUNT_DISTINCT", per_col)
        self.assertIn("APPROX_COUNT_DISTINCT", bulk)
        self.assertNotIn("COUNT(DISTINCT", per_col)
        self.assertNotIn("COUNT(DISTINCT", bulk)

    def test_bigquery_uses_exact_when_flag_off(self) -> None:
        cfg = DBConfig(backend="bigquery", project="p", profiling_approximate=False)
        adapter = BigQueryAdapter(cfg)
        col = adapter.quote_identifier("amount")
        per_col = adapter.column_stats_sql("`p`.`ds`.`orders`", col)
        bulk = adapter.column_stats_bulk_sql("`p`.`ds`.`orders`", [col])
        self.assertIn("COUNT(DISTINCT", per_col)
        self.assertIn("COUNT(DISTINCT", bulk)
        self.assertNotIn("APPROX_COUNT_DISTINCT", per_col)
        self.assertNotIn("APPROX_COUNT_DISTINCT", bulk)

    def test_snowflake_uses_approx_when_flag_on(self) -> None:
        cfg = DBConfig(backend="snowflake", account="a", profiling_approximate=True)
        adapter = SnowflakeAdapter(cfg)
        col = adapter.quote_identifier("amount")
        bulk = adapter.column_stats_bulk_sql('"db"."s"."t"', [col])
        per_col = adapter.column_stats_sql('"db"."s"."t"', col)
        self.assertIn("APPROX_COUNT_DISTINCT", per_col)
        self.assertIn("APPROX_COUNT_DISTINCT", bulk)

    def test_databricks_uses_approx_when_flag_on(self) -> None:
        cfg = DBConfig(
            backend="databricks",
            host="dbx",
            catalog="main",
            profiling_approximate=True,
        )
        adapter = DatabricksAdapter(cfg)
        col = adapter.quote_identifier("amount")
        bulk = adapter.column_stats_bulk_sql("`main`.`s`.`t`", [col])
        per_col = adapter.column_stats_sql("`main`.`s`.`t`", col)
        # Databricks Spark SQL exposes the function with lower-case
        # identifier — match exactly so we don't accidentally also pass
        # for the Snowflake / BigQuery upper-case name.
        self.assertIn("approx_count_distinct", per_col)
        self.assertIn("approx_count_distinct", bulk)

    def test_default_flag_value_is_false(self) -> None:
        # Existing profiles loaded from YAML without the new key must
        # keep the exact-count behaviour.
        self.assertFalse(DBConfig().profiling_approximate)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
