"""Value objects and the ``LineageExtractor`` protocol used across the lineage package."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Protocol

CacheStatus = Literal["hit", "partial", "miss"]
ExtractMode = Literal["cache_only", "db_fill"]


@dataclass(frozen=True)
class ColumnRef:
    """Stable identifier for a column inside a single DB profile."""

    database: str
    schema: str
    table: str
    column: str

    def fqn(self) -> str:
        parts = [p for p in (self.database, self.schema, self.table, self.column) if p]
        return ".".join(parts)


@dataclass(frozen=True)
class Edge:
    """A directed lineage edge between two columns or tables.

    ``source`` feeds ``target``. Confidence is in [0, 1] with 1.0 reserved for
    deterministic extractors (FK, parsed view DDL); heuristics use < 1.0.
    """

    source: ColumnRef
    target: ColumnRef
    relationship_type: str  # 'lineage_fk' | 'lineage_view_ddl' | 'lineage_name_match'
    extractor: str  # 'fk' | 'view_ddl' | 'name_match'
    confidence: float
    evidence: str = ""  # short human-readable provenance hint


@dataclass(frozen=True)
class ScopeFragment:
    """A (database, schema) slice that an extractor flags as needing a DB fetch."""

    database: str
    schema: str
    estimated_objects: int = 0


@dataclass(frozen=True)
class CostHint:
    """Coarse estimate of what filling ``missing_scope`` would cost."""

    estimated_views: int = 0
    estimated_seconds: float = 0.0

    def __add__(self, other: CostHint) -> CostHint:
        return CostHint(
            estimated_views=self.estimated_views + other.estimated_views,
            estimated_seconds=self.estimated_seconds + other.estimated_seconds,
        )


@dataclass
class ExtractResult:
    """One extractor's contribution for a single ``Scope`` call."""

    edges: list[Edge] = field(default_factory=list)
    cache_status: CacheStatus = "hit"
    missing_scope: list[ScopeFragment] = field(default_factory=list)
    estimated_db_cost: CostHint = field(default_factory=CostHint)


@dataclass(frozen=True)
class Scope:
    """Anchor + radius for a single lineage extraction.

    ``anchor`` is the focal entity. ``depth_up`` / ``depth_down`` cap traversal
    so no extractor walks the full graph. ``database`` and ``schema`` scope
    the search to one slice of the profile when set.
    """

    profile: str
    anchor: ColumnRef
    depth_up: int = 1
    depth_down: int = 1
    database: str = ""
    schema: str = ""


class LineageExtractor(Protocol):
    """Pluggable lineage source. Implementations live in :mod:`amx.lineage.extractors`."""

    name: str

    def extract(
        self,
        *,
        hs: Any,
        scope: Scope,
        mode: ExtractMode = "cache_only",
    ) -> ExtractResult: ...


__all__ = [
    "CacheStatus",
    "ExtractMode",
    "ColumnRef",
    "Edge",
    "ScopeFragment",
    "CostHint",
    "ExtractResult",
    "Scope",
    "LineageExtractor",
]
