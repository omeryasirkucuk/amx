"""Tests for the Studio MCP router (/api/mcp/*)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from amx.config import AMXConfig
from amx.web.server import create_app

_TOKEN = "test-mcp-token"
_AUTH = {"Authorization": f"Bearer {_TOKEN}"}


@pytest.fixture
def client(monkeypatch, tmp_path):
    # Isolate IDE config writes and skip the SDK install on connect.
    monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path))
    import amx.utils.optional_deps as optional_deps

    monkeypatch.setattr(optional_deps, "ensure", lambda *a, **k: None)
    cfg = AMXConfig()
    cfg.CONFIG_DIR = str(tmp_path)
    return TestClient(create_app(cfg, token=_TOKEN)), tmp_path


def test_status_endpoint(client):
    c, _ = client
    r = c.get("/api/mcp/status", headers=_AUTH)
    assert r.status_code == 200
    body = r.json()
    assert body["tool_count"] >= 10
    labels = {i["label"] for i in body["ides"]}
    assert {"Cursor", "Claude Desktop", "VS Code"} <= labels
    assert all(i["connected"] is False for i in body["ides"])


def test_tools_endpoint(client):
    c, _ = client
    r = c.get("/api/mcp/tools", headers=_AUTH)
    assert r.status_code == 200
    names = {t["name"] for t in r.json()["tools"]}
    assert "describe_column" in names
    assert "sample_column_values" not in names


def test_snippet_endpoint_and_bad_ide(client):
    c, _ = client
    r = c.get("/api/mcp/snippet", params={"ide": "vscode", "profiles": "sales"}, headers=_AUTH)
    assert r.status_code == 200
    assert '"servers"' in r.json()["snippet"]
    assert c.get("/api/mcp/snippet", params={"ide": "nope"}, headers=_AUTH).status_code == 404


def test_connect_and_disconnect_roundtrip(client):
    c, tmp_path = client
    r = c.post("/api/mcp/connect", json={"ide": "cursor", "profiles": ["sales"]}, headers=_AUTH)
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    assert body["status"]["connected"] is True
    assert body["post_connect_steps"]
    # File actually written.
    data = json.loads((tmp_path / ".cursor" / "mcp.json").read_text())
    assert data["mcpServers"]["amx"]["args"][-1] == "sales"

    r = c.post("/api/mcp/disconnect", json={"ide": "cursor"}, headers=_AUTH)
    assert r.status_code == 200
    assert r.json()["removed"] is True
    assert r.json()["status"]["connected"] is False


def test_connect_unknown_ide_404(client):
    c, _ = client
    r = c.post("/api/mcp/connect", json={"ide": "emacs"}, headers=_AUTH)
    assert r.status_code == 404
