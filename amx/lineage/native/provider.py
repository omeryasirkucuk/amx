"""Cross-backend native-lineage provider seam.

A *native lineage provider* reads lineage straight from the database
platform's own lineage system — the same source that powers the
platform's lineage UI — for a single user-picked table. This is the
on-demand counterpart to the bulk ``system.access.*`` extractor in
:mod:`amx.lineage.extractors.system_tables`: it works per-table and
authorizes on ordinary table visibility rather than metastore-admin
grants, so an individual user running AMX with their own token can
still see "what feeds / consumes this table".

The protocol is deliberately backend-agnostic. Databricks lands first
(:mod:`amx.lineage.native.databricks`); Snowflake
(``SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES``) and BigQuery
(``INFORMATION_SCHEMA.JOBS_BY_PROJECT`` / Data Catalog) slot in behind
the same shape when they get built. Only the value objects and the
``backend -> factory`` registry live here; all REST / SQL coupling
stays inside each provider module so this file never needs to change
when a field name moves.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

# ── node kinds ───────────────────────────────────────────────────────
# The kinds a native lineage graph can surface. ``table`` is the only
# kind with a 3-part FQN; every other kind is an asset identified by a
# platform-native external id and a display name. ``external`` is the
# catch-all for any entity type a provider does not recognise — the
# materializer still records it by name rather than dropping it (the
# explicit contrast with the bulk extractor, which silently discards
# non-table endpoints).
TABLE = "table"
NOTEBOOK = "notebook"
JOB = "job"
PIPELINE = "pipeline"
QUERY = "query"
DASHBOARD = "dashboard"
VECTOR_SEARCH_INDEX = "vector_search_index"
EXTERNAL = "external"

# Asset kinds (everything that is not a plain table) — used by the
# materializer to decide between catalog-table routing and asset-bridge
# routing.
ASSET_KINDS = frozenset({NOTEBOOK, JOB, PIPELINE, QUERY, DASHBOARD, VECTOR_SEARCH_INDEX, EXTERNAL})

# Edge direction relative to the anchor table. ``upstream`` means the
# node feeds the anchor (a producer); ``downstream`` means the node
# reads from the anchor (a consumer). Mirrors the "Assets that write /
# read data" buckets in the Unity Catalog lineage UI.
UPSTREAM = "upstream"
DOWNSTREAM = "downstream"


@dataclass(frozen=True)
class NativeLineageNode:
    """One endpoint in a native lineage graph.

    ``name`` is always populated — it is the name-only guarantee: even
    when the caller lacks privileges to read the entity's contents, the
    provider still yields its name so the relationship is visible.
    ``fqn`` is the 3-part ``catalog.schema.table`` path for tables and
    ``None`` for assets. ``external_id`` is the platform-native handle
    (workspace object id, job id, vector index full name, …) used to
    reconcile against already-ingested assets. ``columns`` is populated
    only when column metadata is accessible.
    """

    kind: str
    name: str
    fqn: str | None = None
    external_id: str | None = None
    columns: tuple[str, ...] = ()


@dataclass(frozen=True)
class NativeLineageEdge:
    """A directed lineage edge ``source -> target`` relative to the anchor.

    ``direction`` records whether the non-anchor endpoint is an
    ``upstream`` producer or ``downstream`` consumer. ``from_column`` /
    ``to_column`` are set only for column-grain edges.
    """

    source: NativeLineageNode
    target: NativeLineageNode
    direction: str
    from_column: str | None = None
    to_column: str | None = None


@dataclass
class NativeLineageResult:
    """The full native lineage fetched for one anchor table."""

    anchor: NativeLineageNode
    edges: list[NativeLineageEdge] = field(default_factory=list)


@runtime_checkable
class NativeLineageProvider(Protocol):
    """Reads native lineage for a single table from one backend."""

    backend: str

    def fetch_table_lineage(
        self,
        fqn: str,
        *,
        with_columns: bool,
        anchor_columns: tuple[str, ...] = (),
    ) -> NativeLineageResult:
        """Return upstream/downstream lineage for the table at ``fqn``.

        ``anchor_columns`` lets the caller pass the anchor's known column
        names (from the local catalog) so the provider can fetch
        column-grain lineage without needing catalog access itself; it is
        consulted only when ``with_columns`` is true.

        Implementations must not raise on a related entity the caller
        cannot read — they yield it as a name-only node instead. They
        may raise on a hard failure to reach the anchor's lineage at
        all (auth against the anchor, network) so the service can report
        it to the user.
        """
        ...


# ── backend registry ─────────────────────────────────────────────────

ProviderFactory = Callable[[str], "NativeLineageProvider | None"]

_REGISTRY: dict[str, ProviderFactory] = {}


def register_provider(backend: str, factory: ProviderFactory) -> None:
    """Register a provider factory for a backend name (lower-cased)."""
    _REGISTRY[backend.strip().lower()] = factory


def provider_for_profile(profile_name: str, backend: str) -> NativeLineageProvider | None:
    """Resolve a profile to its native lineage provider, or ``None``.

    Returns ``None`` when the backend has no registered provider or the
    factory cannot build one (e.g. missing credentials). The caller
    treats ``None`` as "native lineage fetch is not available for this
    profile".
    """
    factory = _REGISTRY.get((backend or "").strip().lower())
    if factory is None:
        return None
    return factory(profile_name)


def supported_backends() -> frozenset[str]:
    """The set of backends with a registered native lineage provider."""
    return frozenset(_REGISTRY)


__all__ = [
    "TABLE",
    "NOTEBOOK",
    "JOB",
    "PIPELINE",
    "QUERY",
    "DASHBOARD",
    "VECTOR_SEARCH_INDEX",
    "EXTERNAL",
    "ASSET_KINDS",
    "UPSTREAM",
    "DOWNSTREAM",
    "NativeLineageNode",
    "NativeLineageEdge",
    "NativeLineageResult",
    "NativeLineageProvider",
    "ProviderFactory",
    "register_provider",
    "provider_for_profile",
    "supported_backends",
]
