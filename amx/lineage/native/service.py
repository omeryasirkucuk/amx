"""Orchestrate a per-table native lineage fetch.

Ties the pieces together: resolve the backend's provider, pull the
anchor's cached column names (for column-grain lineage), fetch, and
hand the result to :class:`LineageMaterializer`. A hard failure to read
the anchor's lineage (auth / network) is raised as
:class:`NativeLineageError` so the CLI / web layer can tell the user;
per-entity access gaps are handled inside the provider / materializer
as name-only nodes.
"""

from __future__ import annotations

from amx.db.adapters._databricks_workspace import DatabricksApiError, DatabricksAuthError
from amx.lineage.native import provider as P
from amx.lineage.native.materializer import LineageMaterializer, MaterializeCounts
from amx.search.catalog import SearchCatalog
from amx.utils.logging import get_logger

log = get_logger("lineage.native.service")


class NativeLineageError(RuntimeError):
    """A native lineage fetch could not complete (raised to the caller)."""


class LineageFetchService:
    """Fetch native lineage for one user-picked table."""

    def __init__(self, catalog: SearchCatalog) -> None:
        self.catalog = catalog

    def fetch(
        self,
        *,
        profile_name: str,
        backend: str,
        fqn: str,
        with_columns: bool = False,
    ) -> MaterializeCounts:
        provider = P.provider_for_profile(profile_name, backend)
        if provider is None:
            raise NativeLineageError(
                f"Native lineage fetch is not available for backend '{backend}'. "
                f"Supported: {', '.join(sorted(P.supported_backends())) or 'none'}."
            )

        anchor_columns: tuple[str, ...] = ()
        if with_columns:
            anchor_columns = self._anchor_columns(profile_name, fqn)

        try:
            result = provider.fetch_table_lineage(
                fqn, with_columns=with_columns, anchor_columns=anchor_columns
            )
        except DatabricksAuthError as exc:
            raise NativeLineageError(
                f"You don't have lineage access to {fqn} (the workspace rejected the request)."
            ) from exc
        except DatabricksApiError as exc:
            raise NativeLineageError(f"Lineage fetch for {fqn} failed: {exc}") from exc

        # Build a content-ingester so accessible notebooks / queries land
        # under Assets as full, drillable assets (their edges then point
        # at the real bridge, not a name-only ghost). Needs the provider's
        # workspace client; absent → assets stay name-only.
        ingester = None
        client = getattr(provider, "_client", None)
        if client is not None:
            from amx.lineage.native.ingest import build_asset_ingester

            ingester = build_asset_ingester(
                profile=profile_name, client=client, catalog=self.catalog
            )

        materializer = LineageMaterializer(
            self.catalog, profile_name=profile_name, backend=backend, ingester=ingester
        )
        counts = materializer.materialize(result)

        # Enrich discovered tables with their columns so the canvas shows
        # the column rail instead of "(no columns cached)". Best-effort,
        # via information_schema (PAT-accessible, catalog-qualified) — a
        # failure here never fails the fetch.
        try:
            self._enrich_columns(profile_name, backend, result)
        except Exception as exc:  # noqa: BLE001
            log.info("native lineage: column enrichment skipped: %s", exc)
        return counts

    def _enrich_columns(
        self, profile_name: str, backend: str, result: P.NativeLineageResult
    ) -> None:
        """Cache columns for every accessible table in the result.

        Reads ``<catalog>.information_schema.columns`` per table and
        writes ``catalog_entities`` column rows so the by-id canvas read
        renders the column rail. Tables the caller can't read return no
        rows and are simply skipped (they stay name-only).
        """
        # Collect distinct 3-part table FQNs (anchor + table endpoints).
        fqns: set[str] = set()
        if result.anchor.kind == P.TABLE and result.anchor.fqn:
            fqns.add(result.anchor.fqn)
        for edge in result.edges:
            for node in (edge.source, edge.target):
                if node.kind == P.TABLE and node.fqn:
                    fqns.add(node.fqn)
        triples = [tuple(f.split(".")) for f in fqns if len(f.split(".")) == 3]
        if not triples:
            return

        from sqlalchemy import text

        from amx.config import AMXConfig
        from amx.db.connector import DatabaseConnector

        db_cfg = AMXConfig.load().db_profiles.get(profile_name)
        if db_cfg is None:
            return
        connector = DatabaseConnector(db_cfg)

        for catalog, schema, table in triples:
            try:
                with connector.engine.connect() as bound:
                    rows = bound.execute(
                        text(
                            f"SELECT column_name, full_data_type FROM {catalog}.information_schema.columns "  # noqa: S608 — catalog is an identifier from the lineage graph
                            "WHERE table_schema = :s AND table_name = :t ORDER BY ordinal_position"
                        ),
                        {"s": schema, "t": table},
                    ).fetchall()
            except Exception as exc:  # noqa: BLE001 — no access / missing table → skip
                log.debug(
                    "native lineage: columns for %s.%s.%s skipped: %s", catalog, schema, table, exc
                )
                continue
            if not rows:
                continue
            with self.catalog._connect() as conn:
                for col_name, dtype in rows:
                    if not col_name:
                        continue
                    self.catalog._upsert_entity(
                        conn,
                        db_profile=profile_name,
                        db_backend=backend,
                        database_name=catalog,
                        schema_name=schema,
                        table_name=table,
                        column_name=str(col_name),
                        entity_kind="column",
                        dtype=str(dtype or ""),
                    )

    def _anchor_columns(self, profile_name: str, fqn: str) -> tuple[str, ...]:
        """Cached column names for the anchor table, for column lineage."""
        parts = [p for p in (fqn or "").split(".") if p]
        if len(parts) == 3:
            database, schema, table = parts
        elif len(parts) == 2:
            database, schema, table = "", parts[0], parts[1]
        else:
            return ()
        with self.catalog._connect() as conn:
            rows = conn.execute(
                """
                SELECT column_name FROM catalog_entities
                WHERE db_profile = ? AND entity_kind = 'column'
                  AND schema_name = ? AND table_name = ?
                  AND (database_name = ? OR ? = '')
                  AND column_name IS NOT NULL
                """,
                (profile_name, schema, table, database, database),
            ).fetchall()
        return tuple(str(r[0]) for r in rows if r[0])


__all__ = ["LineageFetchService", "NativeLineageError"]
