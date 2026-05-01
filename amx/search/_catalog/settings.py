"""Catalog settings + table-explain for ``SearchCatalog``.

Three small public methods bundled together because they don't fit
elsewhere:

* ``set_setting`` / ``get_settings`` — key-value configuration
  persisted in the catalog database (used for things like the
  active dedup language preference).
* ``explain_table`` — render a single-table summary block used by
  /search explain.
"""

from __future__ import annotations

import time
from typing import Any

from amx.search._catalog._constants import DEFAULT_SETTINGS
from amx.utils.logging import get_logger

log = get_logger("search.catalog.settings")


class SettingsMixin:
    """Settings + explain methods for ``SearchCatalog``."""

    def get_settings(self, db_profile: str) -> dict[str, str]:
        out = dict(DEFAULT_SETTINGS)
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT key_name, value_text FROM search_settings WHERE db_profile = ?",
                (db_profile,),
            ).fetchall()
        for row in rows:
            out[str(row["key_name"])] = str(row["value_text"])
        return out
    def set_setting(self, db_profile: str, key: str, value: str) -> None:
        now = time.time()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO search_settings (db_profile, key_name, value_text, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(db_profile, key_name) DO UPDATE SET
                    value_text = excluded.value_text,
                    updated_at = excluded.updated_at
                """,
                (db_profile, key, value, now),
            )
    def explain_table(self, db_profile: str, table_path: str) -> dict[str, Any] | None:
        parts = table_path.split(".")
        if len(parts) != 2:
            raise ValueError("Use schema.table format.")
        with self._connect() as conn:
            table = conn.execute(
                """
                SELECT ce.*, cd.description_text AS effective_description
                FROM catalog_entities ce
                LEFT JOIN catalog_descriptions cd ON cd.id = ce.effective_description_id
                WHERE ce.db_profile = ? AND ce.schema_name = ? AND ce.table_name = ? AND ce.entity_kind = 'table'
                """,
                (db_profile, parts[0], parts[1]),
            ).fetchone()
            if not table:
                return None
            cols = conn.execute(
                """
                SELECT ce.*, cd.description_text AS effective_description
                FROM catalog_entities ce
                LEFT JOIN catalog_descriptions cd ON cd.id = ce.effective_description_id
                WHERE ce.db_profile = ? AND ce.schema_name = ? AND ce.table_name = ? AND ce.entity_kind = 'column'
                ORDER BY ce.column_name
                """,
                (db_profile, parts[0], parts[1]),
            ).fetchall()
            rels = conn.execute(
                """
                SELECT cr.*, target.schema_name AS target_schema, target.table_name AS target_table
                FROM catalog_relationships cr
                LEFT JOIN catalog_entities target ON target.id = cr.to_entity_id
                WHERE cr.from_entity_id = ?
                ORDER BY cr.score DESC, cr.last_seen DESC
                """,
                (int(table["id"]),),
            ).fetchall()
        return {
            "table": dict(table),
            "columns": [dict(row) for row in cols],
            "relationships": [dict(row) for row in rels],
        }


__all__ = ["SettingsMixin"]
