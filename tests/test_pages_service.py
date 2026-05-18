"""End-to-end tests for the pages orchestration service."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from amx.pages.service import PagesService
from amx.pages.store import PageStore
from amx.pages.types import AssetRef, SourceRef
from amx.storage.sqlite_store import SQLiteHistoryStore


class StubLLM:
    def chat(self, messages: list[dict[str, str]], **kw: Any) -> object:
        class R:
            content = "# Overview\n\nAuto body."

        return R()


class StubResolver:
    def resolve_db_asset(self, ref: str) -> str:
        return f"DDL {ref}"

    def resolve_doc_profile(self, ref: str, intent: str, k: int = 5) -> list[str]:
        return [f"snip {ref}"]

    def resolve_lineage(self, ref: str) -> str:
        return f"lineage {ref}"

    def resolve_source(self, src: SourceRef) -> str:
        return f"src {src.original_name}"


def _svc(tmp_path: Path) -> tuple[PagesService, PageStore]:
    store = PageStore(history=SQLiteHistoryStore(tmp_path / "a.db"))
    store.init_schema()
    svc = PagesService(store=store, llm=StubLLM(), resolver=StubResolver(), model_name="m")
    return svc, store


def test_create_generate_save_export(tmp_path: Path) -> None:
    svc, store = _svc(tmp_path)

    pid = svc.create_draft(
        title="X",
        intent="explain",
        assets=[AssetRef("db_table", "p/d/s/t")],
        sources=[],
        created_by="omer",
        now=datetime.now(timezone.utc),
    )
    svc.generate(pid, now=datetime.now(timezone.utc))
    page = store.get(pid)
    assert page is not None
    assert page["markdown_body"].startswith("# Overview")

    svc.save_revision(
        pid,
        markdown_body="# Edited",
        now=datetime.now(timezone.utc),
        saved_by="omer",
        note=None,
    )
    page = store.get(pid)
    assert page is not None
    assert page["markdown_body"] == "# Edited"

    md = svc.export(pid, "md")
    assert isinstance(md, str)
    assert md.startswith("# Edited")

    pdf = svc.export(pid, "pdf")
    assert isinstance(pdf, bytes)
    assert pdf.startswith(b"%PDF-")


def test_generate_raises_on_unknown_page(tmp_path: Path) -> None:
    svc, _ = _svc(tmp_path)
    import pytest

    with pytest.raises(KeyError):
        svc.generate("does-not-exist", now=datetime.now(timezone.utc))


def test_slug_falls_back_to_default_when_title_has_no_alnum(tmp_path: Path) -> None:
    svc, store = _svc(tmp_path)
    pid = svc.create_draft(
        title="!!!",
        intent="",
        assets=[],
        sources=[],
        created_by=None,
        now=datetime.now(timezone.utc),
    )
    page = store.get(pid)
    assert page is not None
    assert page["slug"] == "page"
