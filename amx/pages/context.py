"""Gathers the per-asset textual context fed to the LLM composer.

Each asset kind is routed to a resolver method on the injected
:class:`Resolver` so production code can wire the real DB/RAG/lineage
modules and tests can stub them. A simple greedy budget keeps the
serialised context under ``budget_bytes`` (default 60 KB) - DB DDLs
first, then doc snippets ranked by retrieval score, then lineage
neighbours, then uploaded sources.
"""

from __future__ import annotations

from typing import Protocol

from amx.pages.types import AssetRef, PageContext, SourceRef

DEFAULT_BUDGET = 60_000


class Resolver(Protocol):
    def resolve_db_asset(self, ref: str) -> str: ...
    def resolve_doc_profile(self, ref: str, intent: str, k: int = 5) -> list[str]: ...
    def resolve_lineage(self, ref: str) -> str: ...
    def resolve_asset(self, ref: str, kind: str, intent: str = "") -> str: ...
    def resolve_source(self, src: SourceRef) -> str: ...


def _fits(used: int, block: str, *, budget: int) -> bool:
    return used + len(block) <= budget


def gather(
    *,
    intent: str,
    assets: list[AssetRef],
    sources: list[SourceRef],
    resolver: Resolver,
    budget_bytes: int = DEFAULT_BUDGET,
) -> PageContext:
    ctx = PageContext(intent=intent)
    used = 0

    for a in assets:
        if a.kind.startswith("db_"):
            block = resolver.resolve_db_asset(a.ref)
            if _fits(used, block, budget=budget_bytes):
                ctx.db_blocks.append(block)
                used += len(block)

    for a in assets:
        if a.kind == "doc_profile":
            for snippet in resolver.resolve_doc_profile(a.ref, intent):
                if _fits(used, snippet, budget=budget_bytes):
                    ctx.doc_blocks.append(snippet)
                    used += len(snippet)

    for a in assets:
        if a.kind == "lineage_artifact":
            block = resolver.resolve_lineage(a.ref)
            if _fits(used, block, budget=budget_bytes):
                ctx.lineage_blocks.append(block)
                used += len(block)

    for a in assets:
        if a.kind.startswith("asset_"):
            block = resolver.resolve_asset(a.ref, a.kind, intent)
            if _fits(used, block, budget=budget_bytes):
                ctx.asset_blocks.append(block)
                used += len(block)

    for src in sources:
        block = resolver.resolve_source(src)
        if _fits(used, block, budget=budget_bytes):
            ctx.source_blocks.append(block)
            used += len(block)

    return ctx
