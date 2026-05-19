"""Tests for the documentation pages persistence facade."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from amx.pages.store import PageStore
from amx.pages.types import AssetRef, SourceRef
from amx.storage.sqlite_store import SQLiteHistoryStore


def _store(tmp_path: Path) -> PageStore:
    s = PageStore(history=SQLiteHistoryStore(tmp_path / "amx.db"))
    s.init_schema()
    return s


def test_create_get_list_delete(tmp_path: Path) -> None:
    s = _store(tmp_path)
    now = datetime.now(timezone.utc)
    pid = s.create(
        title="Orders overview",
        slug="orders-overview",
        intent="explain orders pipeline",
        assets=[AssetRef("db_table", "pg_prod/sales/public/orders")],
        sources=[],
        created_by="omer",
        now=now,
    )
    page = s.get(pid)
    assert page is not None
    assert page["title"] == "Orders overview"
    assert page["status"] == "draft"
    assert page["assets"][0]["ref"] == "pg_prod/sales/public/orders"
    assert page["assets"][0]["kind"] == "db_table"

    rows = s.list_active()
    assert len(rows) == 1

    s.soft_delete(pid, now=now)
    assert s.list_active() == []


def test_update_body_appends_version(tmp_path: Path) -> None:
    s = _store(tmp_path)
    now = datetime.now(timezone.utc)
    pid = s.create(
        title="Doc",
        slug="doc",
        intent="",
        assets=[],
        sources=[],
        created_by=None,
        now=now,
    )
    v1 = s.update_body(
        pid,
        markdown_body="# v1",
        rendered_html=None,
        now=now,
        saved_by="omer",
        note="first",
    )
    v2 = s.update_body(
        pid,
        markdown_body="# v2",
        rendered_html=None,
        now=now,
        saved_by="omer",
        note=None,
    )
    assert v1 == 1
    assert v2 == 2
    page = s.get(pid)
    assert page is not None
    assert page["markdown_body"] == "# v2"
    assert [v["version_no"] for v in page["versions"]] == [2, 1]


def test_create_attaches_sources(tmp_path: Path) -> None:
    s = _store(tmp_path)
    now = datetime.now(timezone.utc)
    pid = s.create(
        title="Doc",
        slug="doc-sources",
        intent="",
        assets=[],
        sources=[SourceRef("excel", "/tmp/x.xlsx", "x.xlsx")],
        created_by=None,
        now=now,
    )
    page = s.get(pid)
    assert page is not None
    assert page["sources"][0]["kind"] == "excel"
    assert page["sources"][0]["path"] == "/tmp/x.xlsx"
    assert page["sources"][0]["original_name"] == "x.xlsx"
