"""Canonical metadata abstractions for library-first AMX workflows.

The Universal Metadata Interface (UMI) keeps downstream agents focused on
observable signals instead of backend-specific naming conventions. Part of
the **public API** — see ``docs/PUBLIC_API.md`` for the stability contract.

Public names (re-exported via ``amx.core``): ``AbstractEntity``,
``UniversalMetadataAdapter``. ``LexicalSignal``, ``StructuralSignal``,
``StatisticalSignal``, ``SemanticSignal`` are part of the
``AbstractEntity`` shape and therefore implicitly public — additive
field changes only across minor versions.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from amx.db.connector import ColumnProfile, TableProfile

__all__ = [
    "AbstractEntity",
    "LexicalSignal",
    "SemanticSignal",
    "StatisticalSignal",
    "StructuralSignal",
    "UniversalMetadataAdapter",
]


@dataclass(frozen=True)
class LexicalSignal:
    """Technical labels available for an entity."""

    name: str
    path: str
    aliases: tuple[str, ...] = ()


@dataclass(frozen=True)
class StructuralSignal:
    """Shape and relationship facts available without interpreting names."""

    dtype: str = ""
    nullable: bool | None = None
    asset_kind: str = ""
    primary_key: tuple[str, ...] = ()
    foreign_keys: tuple[dict[str, Any], ...] = ()
    referenced_by: tuple[dict[str, Any], ...] = ()


@dataclass(frozen=True)
class StatisticalSignal:
    """Profiled data distribution facts."""

    row_count: int = 0
    null_count: int = 0
    distinct_count: int = 0
    cardinality_ratio: float = 0.0
    min_value: Any = None
    max_value: Any = None
    samples: tuple[Any, ...] = ()


@dataclass(frozen=True)
class SemanticSignal:
    """Human-authored or model-derived semantic evidence."""

    description: str = ""
    documentation: tuple[str, ...] = ()
    source: str = ""
    confidence: str = ""


@dataclass(frozen=True)
class AbstractEntity:
    """Backend-agnostic metadata object consumed by AMX agents."""

    entity_id: str
    kind: str
    lexical: LexicalSignal
    structural: StructuralSignal = field(default_factory=StructuralSignal)
    statistical: StatisticalSignal = field(default_factory=StatisticalSignal)
    semantic: SemanticSignal = field(default_factory=SemanticSignal)
    provenance: tuple[str, ...] = ()

    @property
    def path(self) -> str:
        return self.lexical.path


class UniversalMetadataAdapter:
    """Normalize concrete metadata sources into ``AbstractEntity`` objects."""

    @staticmethod
    def from_table_profile(profile: TableProfile) -> list[AbstractEntity]:
        table_path = f"{profile.schema}.{profile.name}"
        table = AbstractEntity(
            entity_id=f"table:{table_path}",
            kind="table",
            lexical=LexicalSignal(name=profile.name, path=table_path),
            structural=StructuralSignal(
                asset_kind=profile.asset_kind.value,
                primary_key=tuple(profile.primary_key),
                foreign_keys=tuple(dict(item) for item in profile.foreign_keys),
                referenced_by=tuple(dict(item) for item in profile.referenced_by),
            ),
            statistical=StatisticalSignal(row_count=int(profile.row_count or 0)),
            semantic=SemanticSignal(
                description=profile.existing_comment or "", source="database_comment"
            ),
            provenance=("table_profile",),
        )
        columns = [
            UniversalMetadataAdapter.from_column_profile(profile, column)
            for column in profile.columns
        ]
        return [table, *columns]

    @staticmethod
    def from_column_profile(profile: TableProfile, column: ColumnProfile) -> AbstractEntity:
        table_path = f"{profile.schema}.{profile.name}"
        column_path = f"{table_path}.{column.name}"
        return AbstractEntity(
            entity_id=f"column:{column_path}",
            kind="column",
            lexical=LexicalSignal(name=column.name, path=column_path),
            structural=StructuralSignal(
                dtype=column.dtype,
                nullable=bool(column.nullable),
                asset_kind=profile.asset_kind.value,
                primary_key=tuple(profile.primary_key),
                foreign_keys=tuple(dict(item) for item in profile.foreign_keys),
            ),
            statistical=StatisticalSignal(
                row_count=int(column.row_count or profile.row_count or 0),
                null_count=int(column.null_count or 0),
                distinct_count=int(column.distinct_count or 0),
                cardinality_ratio=float(column.cardinality_ratio or 0.0),
                min_value=column.min_val,
                max_value=column.max_val,
                samples=tuple(column.samples or ()),
            ),
            semantic=SemanticSignal(
                description=column.existing_comment or "", source="database_comment"
            ),
            provenance=("table_profile", "column_profile"),
        )

    @staticmethod
    def from_catalog_row(row: dict[str, Any]) -> AbstractEntity:
        schema = str(row.get("schema_name") or row.get("schema") or "")
        table = str(row.get("table_name") or row.get("table") or "")
        column = str(row.get("column_name") or row.get("column") or "")
        kind = str(row.get("entity_kind") or ("column" if column else "table"))
        path = ".".join(part for part in (schema, table, column) if part)
        return AbstractEntity(
            entity_id=f"{kind}:{path}",
            kind=kind,
            lexical=LexicalSignal(name=column or table or schema, path=path),
            structural=StructuralSignal(
                dtype=str(row.get("dtype") or ""),
                nullable=bool(row.get("nullable")) if row.get("nullable") is not None else None,
                asset_kind=str(row.get("asset_kind") or ""),
            ),
            statistical=StatisticalSignal(row_count=int(row.get("row_count") or 0)),
            semantic=SemanticSignal(
                description=str(row.get("effective_description") or row.get("description") or ""),
                source=str(row.get("effective_source_kind") or row.get("source") or ""),
                confidence=str(row.get("current_confidence") or row.get("confidence") or ""),
            ),
            provenance=("search_catalog",),
        )
