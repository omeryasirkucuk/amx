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
    ) -> MaterializeCounts:
        provider = P.provider_for_profile(profile_name, backend)
        if provider is None:
            raise NativeLineageError(
                f"Native lineage fetch is not available for backend '{backend}'. "
                f"Supported: {', '.join(sorted(P.supported_backends())) or 'none'}."
            )

        # The view is table-centric: table↔table lineage plus the anchor's
        # producer / consumer assets. Column-level lineage is intentionally
        # not requested (its REST shape mis-mapped columns onto table nodes).
        try:
            result = provider.fetch_table_lineage(fqn, with_columns=False, anchor_columns=())
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

            # Resolve notebook names from the persisted workspace index.
            # Unity Catalog hands back only a notebook ``object_id`` and
            # Databricks has no id→name reverse lookup, so names come from
            # a cached ``workspace/list`` scan. Reading the cache is cheap
            # and best-effort (cold cache → ids stay placeholders); a
            # cold/stale cache kicks off a background rebuild that never
            # blocks this fetch.
            from amx.lineage.native import notebook_index

            try:
                idx_path = notebook_index.cache_path(
                    self.catalog.db_path.parent, profile_name, getattr(client, "host", "")
                )
                notebook_index.resolve_names(result, idx_path)
                notebook_index.ensure_background_build(client, idx_path)
            except Exception as exc:  # noqa: BLE001 — naming is best-effort
                log.debug("notebook index resolve/build skipped: %s", exc)

        materializer = LineageMaterializer(
            self.catalog, profile_name=profile_name, backend=backend, ingester=ingester
        )
        counts = materializer.materialize(result)
        # Columns are intentionally NOT eagerly fetched here: pulling each
        # table's information_schema columns spins up the SQL warehouse
        # (cold-start = minutes) and the canvas shows tables collapsed by
        # design. Columns load when the user expands a table.
        return counts


__all__ = ["LineageFetchService", "NativeLineageError"]
