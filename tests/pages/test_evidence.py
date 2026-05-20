"""Tests for amx.pages.evidence — anchor-based published-page retrieval."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest

from amx.pages.evidence import PagesEvidence, build_pages_evidence
from amx.storage.sqlite_store import SQLiteHistoryStore


def _seed_published_page(
    store: SQLiteHistoryStore,
    *,
    page_id: str,
    title: str,
    body: str,
    asset_ref: str,
    asset_kind: str = "db_table",
) -> None:
    now = datetime.utcnow()
    store.create_documentation_page(
        page_id=page_id,
        title=title,
        slug=page_id,
        markdown_body=body,
        rendered_html=None,
        status="published",
        created_at=now,
        updated_at=now,
        created_by=None,
        generation_prompt=None,
        model_used=None,
        db_profile=None,
    )
    store.attach_documentation_page_asset(page_id, asset_kind=asset_kind, asset_ref=asset_ref)


def test_build_pages_evidence_returns_title_and_excerpt(tmp_path: Path) -> None:
    store = SQLiteHistoryStore(tmp_path / "history.db")
    store.init()
    _seed_published_page(
        store,
        page_id="p1",
        title="Customer revenue audit",
        body=(
            "This table is the canonical source after the 2026-Q1 migration. "
            "Daily refresh from S3. Connected to PBI dashboard 'CRO weekly'."
        ),
        asset_ref="p1:s:customers",
    )
    out = build_pages_evidence(
        store=store,
        asset_refs=["p1:s:customers"],
        question_terms=["customers", "table"],
        max_pages=3,
        max_excerpt_chars=400,
        enabled=True,
    )
    assert isinstance(out, PagesEvidence)
    assert len(out.items) == 1
    assert out.items[0].title == "Customer revenue audit"
    assert "canonical source" in out.items[0].excerpt
    assert len(out.items[0].excerpt) <= 400


def test_build_pages_evidence_excludes_drafts(tmp_path: Path) -> None:
    store = SQLiteHistoryStore(tmp_path / "history.db")
    store.init()
    now = datetime.utcnow()
    store.create_documentation_page(
        page_id="d1",
        title="Work in progress",
        slug="d1",
        markdown_body="not ready",
        rendered_html=None,
        status="draft",
        created_at=now,
        updated_at=now,
        created_by=None,
        generation_prompt=None,
        model_used=None,
        db_profile=None,
    )
    store.attach_documentation_page_asset("d1", asset_kind="db_table", asset_ref="p1:s:t")
    out = build_pages_evidence(
        store=store,
        asset_refs=["p1:s:t"],
        question_terms=["t"],
        enabled=True,
    )
    assert out.items == []


def test_build_pages_evidence_disabled_returns_empty(tmp_path: Path) -> None:
    store = SQLiteHistoryStore(tmp_path / "history.db")
    store.init()
    _seed_published_page(
        store, page_id="p1", title="X", body="anything", asset_ref="p1:s:t",
    )
    out = build_pages_evidence(
        store=store,
        asset_refs=["p1:s:t"],
        question_terms=["x"],
        enabled=False,
    )
    assert out.items == []


def test_build_pages_evidence_caps_at_max_pages(tmp_path: Path) -> None:
    store = SQLiteHistoryStore(tmp_path / "history.db")
    store.init()
    for i in range(5):
        _seed_published_page(
            store,
            page_id=f"p{i}",
            title=f"Page {i}",
            body="customers table notes",
            asset_ref="p1:s:customers",
        )
    out = build_pages_evidence(
        store=store,
        asset_refs=["p1:s:customers"],
        question_terms=["customers"],
        max_pages=3,
        enabled=True,
    )
    assert len(out.items) == 3
