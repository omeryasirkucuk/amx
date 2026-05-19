"""Hive (HiveServer2) backend adapter — registration stub.

This file is intentionally a stub in PR-A. The Trino backend ships
first; the full Hive implementation lands in PR-B once the
registration plumbing here is verified end-to-end.

The class is registered in :data:`amx.db.adapters.SUPPORTED_BACKENDS`
so the wizard / Studio / tests can already enumerate it, but every
operation raises :class:`UnsupportedDatabaseOperation` with a clear
"coming in PR-B" message. The capability flags advertise the *intended*
final shape (table/view/schema/database comments YES, column comments
NO) so cache and connector code paths that branch on capability behave
the same way they will once the implementation lands — no
shape-of-the-world surprises between PR-A and PR-B.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy.engine import Engine

from amx.db.adapters.base import BackendCapabilities, DatabaseAdapter

_STUB_MESSAGE = (
    "Hive backend support is registered but not yet implemented in this "
    "release. The full HiveServer2 adapter lands in the next release. "
    "Track progress on GitHub issue #518."
)


class HiveAdapter(DatabaseAdapter):
    name = "hive"
    capabilities = BackendCapabilities(
        database_comments=True,
        schema_comments=True,
        table_comments=True,
        view_comments=True,
        materialized_view_comments=False,
        # Hive's only column-comment write path is a full
        # ``ALTER TABLE … CHANGE col col <type> COMMENT '…'`` which
        # requires re-declaring the original type. Mishandling complex
        # types (struct / map / array) can corrupt the schema, so the
        # capability ships OFF and the connector raises
        # ``UnsupportedDatabaseOperation`` cleanly.
        column_comments=False,
        materialized_views=False,
        relationships=False,
        row_count_stats=False,
        full_profiling=True,
        sampled_profiling=True,
        full_scan_when_row_count_unknown=False,
        external_tables=True,
        # Hive row-level UPDATE is partition-/transaction-table-only,
        # so it cannot safely host AMX's run-history schema.
        supports_shared_history=False,
        comment_asset_keywords=frozenset({"TABLE", "VIEW"}),
    )

    # ── Engine / connection ───────────────────────────────────────────────

    def create_engine(self) -> Engine:
        raise self.unsupported(_STUB_MESSAGE)

    def system_schemas(self) -> frozenset[str]:
        return frozenset({"information_schema", "sys"})

    # ── Profiling SQL — required abstracts ────────────────────────────────
    #
    # The stub returns HiveQL-shaped placeholder SQL so cache-fill and
    # introspection paths can compile the strings (the multi-backend
    # parametric tests assert ``isinstance(... , str)``). Execution is
    # guarded by ``create_engine`` raising, so the SQL is never sent to
    # a live cluster from this release.

    def column_stats_sql(self, fqn: str, quoted_col: str) -> str:
        return (
            f"SELECT "
            f"  SUM(CASE WHEN {quoted_col} IS NULL THEN 1 ELSE 0 END) AS null_cnt, "
            f"  COUNT(DISTINCT {quoted_col}) AS dist_cnt, "
            f"  CAST(MIN({quoted_col}) AS STRING) AS min_val, "
            f"  CAST(MAX({quoted_col}) AS STRING) AS max_val "
            f"FROM {fqn}"
        )

    def column_sample_sql(self, fqn: str, quoted_col: str) -> str:
        return (
            f"SELECT DISTINCT CAST({quoted_col} AS STRING) FROM {fqn} "
            f"WHERE {quoted_col} IS NOT NULL LIMIT :lim"
        )

    # ── Comment writeback — required abstracts ────────────────────────────
    #
    # The concrete SQL ships in PR-B; the stub returns templates that
    # match the intended capability flags so callers exercising the
    # "what SQL would you run" preview path get a meaningful (if
    # unexecuted) string. ``apply_comment`` itself blows up earlier in
    # ``create_engine`` so no faulty DDL ever reaches a live cluster.

    def set_table_comment_sql(self, schema: str, table: str, asset_keyword: str) -> str:
        if asset_keyword not in self.capabilities.comment_asset_keywords:
            raise self.unsupported(f"Comment write-back for {asset_keyword.lower()} assets")
        fqn = self.fully_qualified_name(schema, table)
        return f"ALTER {asset_keyword} {fqn} SET TBLPROPERTIES ('comment' = :cmt)"

    def set_column_comment_sql(self, schema: str, table: str, column: str) -> str:
        raise self.unsupported(
            "Column comment write-back on Hive — disabled in this release. "
            "Hive's only path requires full column re-declaration with type, "
            "which is unsafe for complex types. See issue #518 for status."
        )

    def set_schema_comment_sql(self, schema: str) -> str:
        return f"ALTER DATABASE {self.quote_identifier(schema)} SET DBPROPERTIES ('comment' = :cmt)"

    def set_database_comment_sql(self) -> str:
        return self.set_schema_comment_sql(getattr(self.cfg, "database", "") or "")

    def quote_identifier(self, name: str) -> str:
        # HiveQL uses backticks for identifiers.
        return "`" + str(name).replace("`", "``") + "`"

    def comment_sql_with_params(
        self,
        stmt_template: str,
        comment: str,
    ) -> tuple[str, dict[str, Any]]:
        # PR-B will inline the literal so the Thrift DDL path doesn't
        # fight named bind params; mirroring Databricks / Trino here
        # keeps the contract identical pre- and post-implementation.
        return stmt_template.replace(":cmt", self.quote_literal(comment)), {}
