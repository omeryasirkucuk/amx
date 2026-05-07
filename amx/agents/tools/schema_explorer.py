"""Schema-level structural exploration tool for agentic inventory questions."""

from __future__ import annotations

import re
from collections import Counter
from collections.abc import Callable
from typing import Any

from amx.config import AMXConfig
from amx.core.metadata import UniversalMetadataAdapter
from amx.db.connector import DatabaseConnector, ProfilingError


class SchemaExplorer:
    """Return macro-level structural facts instead of point search matches."""

    _STOPWORDS = {
        "the",
        "and",
        "for",
        "with",
        "from",
        "into",
        "table",
        "column",
        "field",
        "data",
        "record",
        "records",
        "used",
        "contains",
        "stores",
        "value",
        "values",
    }

    def __init__(
        self,
        cfg: AMXConfig,
        catalog,
        *,
        db_factory: Callable[[], DatabaseConnector] | None = None,
    ) -> None:
        self.cfg = cfg
        self.catalog = catalog
        self.db_profile = cfg.active_db_profile or "default"
        self._db_factory = db_factory or (lambda: DatabaseConnector(cfg.db))

    def explore(
        self,
        *,
        schema_name: str | None = None,
        database_name: str | None = None,
        limit: int = 500,
    ) -> dict[str, Any]:
        """Return table names, row counts, and column counts for a namespace."""
        schema_name = schema_name or self.cfg.current_schema or None
        database_name = (
            database_name
            or self.cfg.db.database
            or self.cfg.db.catalog
            or self.cfg.db.project
            or None
        )
        rows = self.catalog.schema_inventory(
            self.db_profile,
            schema_name=schema_name,
            database_name=database_name,
            limit=limit,
        )
        source = "search_catalog"
        gap_fills = 0
        if not rows:
            rows, gap_fills = self._live_inventory(
                schema_name=schema_name, database_name=database_name, limit=limit
            )
            source = "live_db"
        rows = self._with_semantic_clusters(rows)
        table_count = len(rows)
        total_columns = sum(int(row.get("column_count") or 0) for row in rows)
        known_row_count_tables = sum(1 for row in rows if int(row.get("row_count") or 0) > 0)
        return {
            "tool": "SchemaExplorer",
            "source": source,
            "scope": {
                "database_name": database_name or "",
                "schema_name": schema_name or "",
            },
            "summary": {
                "table_count": table_count,
                "total_columns": total_columns,
                "known_row_count_tables": known_row_count_tables,
                "gap_fill_operations": gap_fills,
            },
            "rows": rows,
        }

    def _live_inventory(
        self,
        *,
        schema_name: str | None,
        database_name: str | None,
        limit: int,
    ) -> tuple[list[dict[str, Any]], int]:
        db = self._db_factory()
        available_schemas = list(db.list_schemas())
        schema_lookup = {str(item).lower(): str(item) for item in available_schemas}
        if schema_name:
            normalized = str(schema_name).strip().lower()
            resolved = schema_lookup.get(normalized)
            schemas = [resolved] if resolved else available_schemas
        else:
            schemas = available_schemas
        rows: list[dict[str, Any]] = []
        gap_fills = 0
        for schema in schemas:
            try:
                tables = db.list_tables(schema)
            except Exception:
                continue
            for table in tables:
                if len(rows) >= limit:
                    return rows, gap_fills
                column_count = 0
                row_count = 0
                try:
                    profile = db.profile_table(schema, table, sample_size=0)
                    column_count = len(profile.columns)
                    row_count = int(profile.row_count or 0)
                    gap_fills += 1
                except ProfilingError:
                    pass
                except Exception:
                    pass
                rows.append(
                    {
                        "row_type": "schema_explorer_table",
                        "database_name": database_name or "",
                        "schema_name": schema,
                        "table_name": table,
                        "asset_kind": "table",
                        "row_count": row_count,
                        "column_count": column_count,
                        "effective_description": "",
                        "column_descriptions": "",
                        "source": "live_db",
                    }
                )
        return rows, gap_fills

    def _with_semantic_clusters(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        token_counts: Counter[str] = Counter()
        row_tokens: list[list[str]] = []
        for row in rows:
            tokens = self._tokens(
                " ".join(
                    [
                        str(row.get("effective_description") or ""),
                        str(row.get("column_descriptions") or ""),
                        str(row.get("table_name") or ""),
                    ]
                )
            )
            row_tokens.append(tokens)
            token_counts.update(set(tokens))
        out: list[dict[str, Any]] = []
        for row, tokens in zip(rows, row_tokens, strict=False):
            ranked = sorted(set(tokens), key=lambda token: (-token_counts[token], token))
            cluster = ranked[0].title() if ranked else "Unclustered"
            item = dict(row)
            item["row_type"] = "schema_explorer_table"
            item["source"] = item.get("source") or "search_catalog"
            item["semantic_cluster"] = cluster
            try:
                entity = UniversalMetadataAdapter.from_catalog_row(item)
                item["umi_kind"] = entity.kind
                item["umi_path"] = entity.path
            except Exception:
                item["umi_kind"] = "table"
                item["umi_path"] = ".".join(
                    part
                    for part in (
                        str(item.get("schema_name") or ""),
                        str(item.get("table_name") or ""),
                    )
                    if part
                )
            out.append(item)
        return out

    def _tokens(self, text: str) -> list[str]:
        tokens = []
        for token in re.findall(r"\w+", (text or "").lower()):
            if len(token) <= 2 or token in self._STOPWORDS:
                continue
            tokens.append(token)
        return tokens
