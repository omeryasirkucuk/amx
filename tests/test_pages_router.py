"""FastAPI router tests for the documentation pages surface.

Uses ``app.dependency_overrides[get_pages_service]`` to inject a
service backed by a temp-dir SQLite history store plus stubbed LLM and
resolver so the tests stay fast and offline.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from amx.config import AMXConfig
from amx.pages.service import PagesService
from amx.pages.store import PageStore
from amx.pages.types import SourceRef
from amx.storage.sqlite_store import SQLiteHistoryStore
from amx.web.deps import get_pages_service
from amx.web.server import create_app

_TEST_TOKEN = "test-pages-token-xyz"


class _StubLLM:
    model_name = "stub-model"

    def chat(self, messages: list[dict[str, str]], **kw: Any) -> object:
        class R:
            content = "# Overview\n\nGenerated body."

        return R()


class _StubResolver:
    def __init__(self, cfg: AMXConfig) -> None:
        self.cfg = cfg

    def resolve_db_asset(self, ref: str) -> str:
        return f"DDL {ref}"

    def resolve_doc_profile(self, ref: str, intent: str, k: int = 5) -> list[str]:
        return [f"snip {ref}"]

    def resolve_lineage(self, ref: str) -> str:
        return f"lineage {ref}"

    def resolve_asset(self, ref: str, kind: str) -> str:
        return f"asset {kind} {ref}"

    def resolve_source(self, src: SourceRef) -> str:
        return f"src {src.original_name}"


@pytest.fixture()
def cfg() -> AMXConfig:
    cfg = AMXConfig()
    cfg.llm.provider = "openai"
    cfg.llm.model = "gpt-4"
    return cfg


@pytest.fixture()
def service(tmp_path: Path, cfg: AMXConfig) -> PagesService:
    store = PageStore(history=SQLiteHistoryStore(tmp_path / "history.db"))
    store.init_schema()
    return PagesService(
        store=store,
        llm=_StubLLM(),
        resolver=_StubResolver(cfg),
        model_name="stub-model",
    )


@pytest.fixture()
def client(cfg: AMXConfig, service: PagesService) -> TestClient:
    app = create_app(cfg, token=_TEST_TOKEN)
    app.dependency_overrides[get_pages_service] = lambda: service
    return TestClient(app)


@pytest.fixture()
def headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {_TEST_TOKEN}"}


def test_list_pages_empty(client: TestClient, headers: dict[str, str]) -> None:
    r = client.get("/api/pages", headers=headers)
    assert r.status_code == 200
    assert r.json() == []


def test_list_asset_options_for_notebook_kind(
    client: TestClient, headers: dict[str, str], service: PagesService
) -> None:
    """Studio's New-page wizard 'Ingested assets' tab hits this endpoint
    to populate the per-kind picker."""
    hs = service.store.history
    with hs._lock, hs._connect() as conn:  # type: ignore[attr-defined]
        conn.execute(
            "INSERT INTO remote_notebooks (profile_name, platform, external_id, "
            "name, workspace_path, language, source_text, source_hash, ingested_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "sf_prod",
                "snowflake",
                "e1",
                "nightly_etl",
                "/etl",
                "sql",
                "SELECT 1",
                "h",
                "2026-01-01",
            ),
        )

    r = client.get(
        "/api/pages/asset-options",
        params={"kind": "asset_notebook", "profile": "sf_prod"},
        headers=headers,
    )
    assert r.status_code == 200
    payload = r.json()
    assert isinstance(payload, list)
    assert len(payload) == 1
    item = payload[0]
    assert item["kind"] == "asset_notebook"
    assert item["name"] == "nightly_etl"
    assert item["ref"].startswith("sf_prod:")


def test_list_asset_options_rejects_unknown_kind(
    client: TestClient, headers: dict[str, str]
) -> None:
    r = client.get(
        "/api/pages/asset-options",
        params={"kind": "asset_bogus", "profile": "any"},
        headers=headers,
    )
    assert r.status_code == 400


def test_list_intent_templates(client: TestClient, headers: dict[str, str]) -> None:
    """Studio's New-page wizard fetches preset intent shapes from this
    endpoint to render the picker grid above the Intent textarea."""
    r = client.get("/api/pages/intent-templates", headers=headers)
    assert r.status_code == 200
    payload = r.json()
    assert isinstance(payload, list)
    slugs = {t["slug"] for t in payload}
    assert "single-table" in slugs
    assert "project-overview" in slugs
    one = next(t for t in payload if t["slug"] == "single-table")
    assert "label" in one
    assert "prompt_skeleton" in one
    assert "required_assets" in one


def test_create_then_list_then_get(client: TestClient, headers: dict[str, str]) -> None:
    r = client.post(
        "/api/pages",
        json={
            "title": "Orders",
            "intent": "explain",
            "assets": [{"kind": "db_table", "ref": "p/d/s/orders"}],
        },
        headers=headers,
    )
    assert r.status_code == 201
    pid = r.json()["id"]

    r = client.get("/api/pages", headers=headers)
    assert r.status_code == 200
    assert any(p["id"] == pid for p in r.json())

    r = client.get(f"/api/pages/{pid}", headers=headers)
    assert r.status_code == 200
    page = r.json()
    assert page["title"] == "Orders"
    assert page["status"] == "draft"
    assert len(page["assets"]) == 1


def test_generate_writes_body_via_stub_llm(client: TestClient, headers: dict[str, str]) -> None:
    r = client.post(
        "/api/pages",
        json={"title": "Pipeline", "intent": "explain", "assets": []},
        headers=headers,
    )
    pid = r.json()["id"]

    r = client.post(f"/api/pages/{pid}/generate", headers=headers)
    assert r.status_code == 200
    assert r.json()["markdown_body"].startswith("# Overview")


def test_generate_missing_page_returns_404(client: TestClient, headers: dict[str, str]) -> None:
    r = client.post("/api/pages/does-not-exist/generate", headers=headers)
    assert r.status_code == 404


def test_patch_updates_body_and_records_version(
    client: TestClient, headers: dict[str, str]
) -> None:
    pid = client.post(
        "/api/pages",
        json={"title": "Editable", "intent": ""},
        headers=headers,
    ).json()["id"]

    r = client.patch(
        f"/api/pages/{pid}",
        json={"markdown_body": "# Edited", "note": "manual edit"},
        headers=headers,
    )
    assert r.status_code == 200
    payload = r.json()
    assert payload["markdown_body"] == "# Edited"
    assert len(payload["versions"]) >= 1


def test_patch_missing_page_returns_404(client: TestClient, headers: dict[str, str]) -> None:
    r = client.patch(
        "/api/pages/ghost",
        json={"markdown_body": "# x"},
        headers=headers,
    )
    assert r.status_code == 404


def test_delete_soft_deletes_page(client: TestClient, headers: dict[str, str]) -> None:
    pid = client.post(
        "/api/pages",
        json={"title": "Trash"},
        headers=headers,
    ).json()["id"]

    r = client.delete(f"/api/pages/{pid}", headers=headers)
    assert r.status_code == 200
    assert r.json() == {"ok": True}

    listing = client.get("/api/pages", headers=headers).json()
    assert all(p["id"] != pid for p in listing)


def test_delete_missing_returns_404(client: TestClient, headers: dict[str, str]) -> None:
    r = client.delete("/api/pages/ghost", headers=headers)
    assert r.status_code == 404


def test_export_md_returns_markdown_attachment(client: TestClient, headers: dict[str, str]) -> None:
    pid = client.post(
        "/api/pages",
        json={"title": "Doc", "intent": ""},
        headers=headers,
    ).json()["id"]
    client.patch(
        f"/api/pages/{pid}",
        json={"markdown_body": "# Final"},
        headers=headers,
    )

    r = client.get(f"/api/pages/{pid}/export/md", headers=headers)
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/markdown")
    assert r.text == "# Final"
    assert "attachment" in r.headers.get("content-disposition", "")


def test_export_pdf_returns_pdf_bytes(client: TestClient, headers: dict[str, str]) -> None:
    # Same rationale as ``test_to_pdf_returns_bytes_starting_with_pdf_header``
    # in ``tests/test_pages_exporters.py``: PDF export needs the
    # optional ``[pages]`` extra (``xhtml2pdf`` + its native chain),
    # which is not installed on CI runners.
    pytest.importorskip(
        "xhtml2pdf",
        reason="xhtml2pdf is only installed with the [pages] extra",
    )
    pid = client.post(
        "/api/pages",
        json={"title": "PdfDoc", "intent": ""},
        headers=headers,
    ).json()["id"]
    client.patch(
        f"/api/pages/{pid}",
        json={"markdown_body": "# Hello\n\nbody"},
        headers=headers,
    )

    r = client.get(f"/api/pages/{pid}/export/pdf", headers=headers)
    assert r.status_code == 200
    assert r.headers["content-type"] == "application/pdf"
    assert r.content.startswith(b"%PDF-")


def test_export_md_missing_returns_404(client: TestClient, headers: dict[str, str]) -> None:
    r = client.get("/api/pages/ghost/export/md", headers=headers)
    assert r.status_code == 404


def test_upload_source_rejects_unknown_extension(
    client: TestClient, headers: dict[str, str]
) -> None:
    pid = client.post(
        "/api/pages",
        json={"title": "Files"},
        headers=headers,
    ).json()["id"]

    r = client.post(
        f"/api/pages/{pid}/sources",
        files={"file": ("evil.exe", b"\x00\x00", "application/octet-stream")},
        headers=headers,
    )
    assert r.status_code == 400


def test_upload_source_accepts_eml_and_persists(
    client: TestClient,
    headers: dict[str, str],
    service: PagesService,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AMX_CONFIG_DIR", str(tmp_path / "amx-home"))
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path / "home"))

    pid = client.post(
        "/api/pages",
        json={"title": "Mail"},
        headers=headers,
    ).json()["id"]

    eml_payload = b"From: a@example.com\r\nTo: b@example.com\r\nSubject: Hi\r\n\r\nHello there.\r\n"
    r = client.post(
        f"/api/pages/{pid}/sources",
        files={"file": ("note.eml", eml_payload, "message/rfc822")},
        headers=headers,
    )
    assert r.status_code == 201
    body = r.json()
    assert body["kind"] == "email"
    assert body["original_name"] == "note.eml"

    page = service.store.get(pid)
    assert page is not None
    assert len(page["sources"]) == 1
    assert page["sources"][0]["kind"] == "email"


def test_upload_source_missing_page_returns_404(
    client: TestClient, headers: dict[str, str]
) -> None:
    r = client.post(
        "/api/pages/ghost/sources",
        files={"file": ("a.md", b"hi", "text/markdown")},
        headers=headers,
    )
    assert r.status_code == 404
