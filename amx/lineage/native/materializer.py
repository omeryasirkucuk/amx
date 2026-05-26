"""Route a ``NativeLineageResult`` into the local catalog.

Every node a native lineage fetch discovers is sent to its correct
home — a table becomes a ``catalog_entities`` table row, an asset
(notebook / job / pipeline / dashboard / query / vector index) becomes
an ``__assets`` bridge row — and every relationship becomes a
``catalog_relationships`` edge. Two privilege tiers:

* **full** — the entity is already known to AMX (a synced table, or an
  ingested remote asset reconciled by its platform id). The edge points
  at the real, drillable row.
* **name_only** — native lineage named the entity but the user lacks
  the privileges to read its contents. A ghost row is recorded so the
  relationship and the name still appear on the canvas; nothing is
  dropped. (The bulk ``system.access.*`` extractor, by contrast,
  silently discards anything it cannot resolve to a 3-part table FQN.)

Unknown entity kinds are not dropped either — they land as ``external``
name-only nodes.

Idempotency is scoped to the **anchor**: a re-fetch of one table
refreshes only the edges that touch that table, leaving lineage fetched
for other tables intact.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass

from amx.lineage.native import provider as P
from amx.search.catalog import SearchCatalog
from amx.utils.logging import get_logger

log = get_logger("lineage.native.materializer")

SOURCE = "databricks_native_lineage"
REL_TABLE = "lineage_native_table"
REL_COLUMN = "lineage_native_column"
REL_ASSET = "lineage_native_asset"

# remote_<kind>s tables that back a "full" asset, keyed by node kind,
# with the column holding the platform external id. Kinds absent here
# (dashboard, vector_search_index, external) have no remote_* table and
# are always name-only ghosts.
_REMOTE_TABLE_BY_KIND: dict[str, tuple[str, str]] = {
    P.NOTEBOOK: ("remote_notebooks", "external_id"),
    P.QUERY: ("remote_queries", "external_id"),
    P.JOB: ("remote_jobs", "job_id"),
    P.PIPELINE: ("remote_pipelines", "pipeline_id"),
}


@dataclass
class MaterializeCounts:
    """Per-fetch summary the CLI / Studio surface to the user."""

    tables: int = 0
    assets: int = 0
    columns: int = 0
    name_only: int = 0
    edges: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "tables": self.tables,
            "assets": self.assets,
            "columns": self.columns,
            "name_only": self.name_only,
            "edges": self.edges,
        }


class LineageMaterializer:
    """Persist a :class:`NativeLineageResult` into the local catalog."""

    def __init__(self, catalog: SearchCatalog, *, profile_name: str, backend: str) -> None:
        self.catalog = catalog
        self.profile_name = profile_name
        self.backend = backend

    def materialize(self, result: P.NativeLineageResult) -> MaterializeCounts:
        counts = MaterializeCounts()
        with self.catalog._connect() as conn:
            # The anchor must resolve to a real, already-synced row — the
            # picker only offers cached tables — but tolerate a ghost if
            # somehow absent so the fetch still records its edges.
            anchor_id = self._resolve_or_ghost_table(conn, result.anchor, counts)
            if anchor_id is None:
                log.info("native lineage: could not resolve anchor %s", result.anchor.fqn)
                return counts

            # Anchor-scoped idempotency: drop this source's prior edges
            # that touch the anchor, then reinsert from the fresh fetch.
            conn.execute(
                """
                DELETE FROM catalog_relationships
                WHERE source = ?
                  AND relationship_type IN (?, ?, ?)
                  AND (from_entity_id = ? OR to_entity_id = ?)
                """,
                (SOURCE, REL_TABLE, REL_COLUMN, REL_ASSET, anchor_id, anchor_id),
            )

            seen_edges: set[tuple[int, int, str, str, str]] = set()
            for edge in result.edges:
                self._materialize_edge(conn, edge, anchor_id, counts, seen_edges)
        return counts

    # ── edge routing ─────────────────────────────────────────────────

    def _materialize_edge(
        self,
        conn,
        edge: P.NativeLineageEdge,
        anchor_id: int,
        counts: MaterializeCounts,
        seen: set[tuple[int, int, str, str, str]],
    ) -> None:
        src_id, src_kind = self._resolve_node(conn, edge.source, anchor_id, counts)
        tgt_id, tgt_kind = self._resolve_node(conn, edge.target, anchor_id, counts)
        if (
            src_id is None
            or tgt_id is None
            or src_id == tgt_id
            and not (edge.from_column and edge.to_column)
        ):
            return

        if edge.from_column and edge.to_column:
            rel = REL_COLUMN
        elif src_kind == P.TABLE and tgt_kind == P.TABLE:
            rel = REL_TABLE
        else:
            rel = REL_ASSET

        key = (
            src_id,
            tgt_id,
            rel,
            edge.from_column or "",
            edge.to_column or "",
        )
        if key in seen:
            return
        seen.add(key)

        details = {"direction": edge.direction}
        conn.execute(
            """
            INSERT INTO catalog_relationships (
                from_entity_id, to_entity_id, relationship_type, score,
                source, details_json, last_seen, from_entity_kind,
                to_entity_kind, from_column, to_column
            ) VALUES (?, ?, ?, 1.0, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                src_id,
                tgt_id,
                rel,
                SOURCE,
                json.dumps(details, sort_keys=True),
                time.time(),
                src_kind,
                tgt_kind,
                edge.from_column or "",
                edge.to_column or "",
            ),
        )
        counts.edges += 1
        if rel == REL_COLUMN:
            counts.columns += 1

    # ── node resolution ──────────────────────────────────────────────

    def _resolve_node(
        self, conn, node: P.NativeLineageNode, anchor_id: int, counts: MaterializeCounts
    ) -> tuple[int | None, str]:
        if node.kind == P.TABLE:
            entity_id = self._resolve_or_ghost_table(conn, node, counts)
            return entity_id, P.TABLE
        entity_id = self._resolve_or_ghost_asset(conn, node, counts)
        return entity_id, node.kind

    def _resolve_or_ghost_table(
        self, conn, node: P.NativeLineageNode, counts: MaterializeCounts
    ) -> int | None:
        fqn = node.fqn or node.name
        existing = self.catalog._resolve_catalog_entity_by_fqn(conn, self.profile_name, fqn)
        if existing is not None:
            counts.tables += 1
            return existing
        # Name-only ghost: register the table by FQN so the relationship
        # and name show on the canvas even without read access.
        parts = [p for p in (node.fqn or "").split(".") if p]
        if len(parts) == 3:
            database, schema, table = parts
        elif len(parts) == 2:
            database, schema, table = "", parts[0], parts[1]
        else:
            database, schema, table = "", "", node.name
        entity_id = self.catalog._upsert_entity(
            conn,
            db_profile=self.profile_name,
            db_backend=self.backend,
            database_name=database,
            schema_name=schema,
            table_name=table,
            column_name=None,
            entity_kind=P.TABLE,
            metadata_state="name_only",
        )
        counts.tables += 1
        counts.name_only += 1
        return entity_id

    def _resolve_or_ghost_asset(
        self, conn, node: P.NativeLineageNode, counts: MaterializeCounts
    ) -> int | None:
        remote = self._lookup_remote_asset(conn, node)
        if remote is not None:
            # Reconcile against an already-ingested asset → full, standard
            # bridge identity (matches the normal ingest path).
            entity_id = self.catalog._upsert_asset_entity(
                conn,
                profile_name=self.profile_name,
                kind=node.kind,
                remote_id=remote,
                display_name=node.name,
                backend=self.backend,
                metadata_state="full",
            )
            counts.assets += 1
            return entity_id
        # Name-only ghost bridge — no backing remote_* row.
        entity_id = self._upsert_ghost_asset(conn, node)
        counts.assets += 1
        counts.name_only += 1
        return entity_id

    def _lookup_remote_asset(self, conn, node: P.NativeLineageNode) -> int | None:
        """Return the remote_<kind>s.id matching this node, or ``None``."""
        if not node.external_id:
            return None
        spec = _REMOTE_TABLE_BY_KIND.get(node.kind)
        if spec is None:
            return None
        table, id_col = spec
        row = conn.execute(
            f"SELECT id FROM {table} WHERE profile_name = ? AND {id_col} = ?",  # noqa: S608
            (self.profile_name, node.external_id),
        ).fetchone()
        return int(row[0]) if row else None

    def _upsert_ghost_asset(self, conn, node: P.NativeLineageNode) -> int:
        """Create / refresh a name-only asset bridge row.

        Keyed by the platform external id (or a name slug when absent) so
        re-fetches are idempotent and the row cannot collide with a
        full bridge created by the normal ingest path, which keys on the
        remote_<kind>s.id.
        """
        ref = node.external_id or f"name:{node.name}"
        table_name = f"{node.kind}#ext:{ref}"
        now = time.time()
        existing = conn.execute(
            """SELECT id FROM catalog_entities
               WHERE db_profile = ? AND database_name = '' AND schema_name = '__assets'
                 AND table_name = ? AND column_name IS NULL AND entity_kind = ?""",
            (self.profile_name, table_name, node.kind),
        ).fetchone()
        if existing is not None:
            conn.execute(
                "UPDATE catalog_entities SET search_text = ?, db_backend = ?, "
                "updated_at = ?, last_synced_at = ? WHERE id = ?",
                (node.name, self.backend, now, now, int(existing[0])),
            )
            return int(existing[0])
        cur = conn.execute(
            """INSERT INTO catalog_entities
                   (db_profile, db_backend, database_name, schema_name, table_name,
                    column_name, entity_kind, asset_kind, search_text, source_remote_id,
                    updated_at, last_synced_at, metadata_state)
               VALUES (?, ?, '', '__assets', ?, NULL, ?, ?, ?, NULL, ?, ?, 'name_only')""",
            (
                self.profile_name,
                self.backend,
                table_name,
                node.kind,
                node.kind,
                node.name,
                now,
                now,
            ),
        )
        return int(cur.lastrowid or 0)


__all__ = [
    "LineageMaterializer",
    "MaterializeCounts",
    "SOURCE",
    "REL_TABLE",
    "REL_COLUMN",
    "REL_ASSET",
]
