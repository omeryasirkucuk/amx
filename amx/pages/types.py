"""Typed values exchanged between the pages module's layers."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

AssetKind = Literal[
    "db_profile",
    "db_database",
    "db_schema",
    "db_table",
    "db_column",
    "doc_profile",
    "lineage_artifact",
]
SourceKind = Literal["upload", "email", "excel"]
PageStatus = Literal["draft", "published", "deleted"]


@dataclass(frozen=True)
class AssetRef:
    kind: AssetKind
    ref: str


@dataclass(frozen=True)
class SourceRef:
    kind: SourceKind
    path: str
    original_name: str


@dataclass
class PageDraft:
    id: str
    title: str
    slug: str
    intent: str
    assets: list[AssetRef] = field(default_factory=list)
    sources: list[SourceRef] = field(default_factory=list)
    markdown_body: str = ""
    status: PageStatus = "draft"


@dataclass
class PageContext:
    intent: str
    db_blocks: list[str] = field(default_factory=list)
    doc_blocks: list[str] = field(default_factory=list)
    lineage_blocks: list[str] = field(default_factory=list)
    source_blocks: list[str] = field(default_factory=list)

    def serialise(self) -> str:
        sections: list[str] = []
        if self.db_blocks:
            sections.append("# DATABASE ASSETS\n\n" + "\n\n".join(self.db_blocks))
        if self.doc_blocks:
            sections.append("# DOC SNIPPETS\n\n" + "\n\n".join(self.doc_blocks))
        if self.lineage_blocks:
            sections.append("# LINEAGE\n\n" + "\n\n".join(self.lineage_blocks))
        if self.source_blocks:
            sections.append("# SOURCES\n\n" + "\n\n".join(self.source_blocks))
        return "\n\n".join(sections)
