"""Tests for the pages context gatherer."""

from __future__ import annotations

from amx.pages.context import gather
from amx.pages.types import AssetRef, SourceRef


class StubResolver:
    def resolve_db_asset(self, ref: str) -> str:
        return f"DDL for {ref}"

    def resolve_doc_profile(self, ref: str, intent: str, k: int = 5) -> list[str]:
        return [f"snippet from {ref}: {intent}"]

    def resolve_lineage(self, ref: str) -> str:
        return f"lineage block for {ref}"

    def resolve_source(self, src: SourceRef) -> str:
        return f"source body {src.original_name}"


def test_gather_serialises_each_asset_kind() -> None:
    ctx = gather(
        intent="explain pipeline",
        assets=[
            AssetRef("db_table", "pg/sales/public/orders"),
            AssetRef("doc_profile", "doc:design"),
            AssetRef("lineage_artifact", "lineage:abc"),
        ],
        sources=[],
        resolver=StubResolver(),
        budget_bytes=10_000,
    )
    s = ctx.serialise()
    assert "DDL for pg/sales/public/orders" in s
    assert "snippet from doc:design" in s
    assert "lineage block for lineage:abc" in s


def test_gather_respects_budget() -> None:
    class BigResolver(StubResolver):
        def resolve_db_asset(self, ref: str) -> str:
            return "x" * 500

    ctx = gather(
        intent="i",
        assets=[AssetRef("db_table", f"r{i}") for i in range(10)],
        sources=[],
        resolver=BigResolver(),
        budget_bytes=1500,
    )
    # Should fit at most 3 blocks of 500 bytes each.
    assert len(ctx.db_blocks) == 3


def test_gather_includes_sources() -> None:
    ctx = gather(
        intent="i",
        assets=[],
        sources=[SourceRef("excel", "/tmp/x.xlsx", "x.xlsx")],
        resolver=StubResolver(),
    )
    assert "source body x.xlsx" in ctx.serialise()
