from __future__ import annotations

import json

import pytest


@pytest.fixture()
def history_dir(tmp_path, monkeypatch, cfg):
    """Point cfg.CONFIG_DIR at an isolated dir and init history.db."""
    from amx.storage.sqlite_store import SQLiteHistoryStore

    monkeypatch.setattr(cfg, "CONFIG_DIR", str(tmp_path), raising=False)
    SQLiteHistoryStore(tmp_path / "history.db").init()

    cfg.llm_profiles = {"default": cfg.llm}  # ensure 'default' exists for tests
    return tmp_path


def _seed_row(history_dir, name="default", enabled=True):
    from amx.llm.style.profile import StyleProfile
    from amx.storage.style_store import StyleStore

    store = StyleStore(history_dir / "history.db")
    store.upsert(
        name,
        "a.b.c",
        "duckdb",
        StyleProfile(
            language="en-US",
            tone="x",
            avg_length_words=1,
            length_range=(1, 1),
            person="x",
            capitalization="x",
            ends_with_period=True,
            structural_patterns=[],
            vocabulary_register="x",
            redacted_examples=["Unique id of the <ENTITY>."],
        ),
        sample_count=3,
    )
    if not enabled:
        store.set_enabled(name, False)


def test_get_returns_404_when_missing(client, auth_headers, history_dir):
    r = client.get("/api/llm-profiles/default/style", headers=auth_headers)
    assert r.status_code == 404


def test_get_returns_serialized_row(client, auth_headers, history_dir):
    _seed_row(history_dir)
    r = client.get("/api/llm-profiles/default/style", headers=auth_headers)
    assert r.status_code == 200
    body = r.json()
    assert body["llm_profile"] == "default"
    assert body["enabled"] is True
    assert body["profile"]["language"] == "en-US"


def test_patch_toggles_enabled(client, auth_headers, history_dir):
    from amx.storage.style_store import StyleStore

    _seed_row(history_dir)
    r = client.patch(
        "/api/llm-profiles/default/style",
        json={"enabled": False},
        headers=auth_headers,
    )
    assert r.status_code == 200
    assert StyleStore(history_dir / "history.db").get("default").enabled is False


def test_delete_clears_row(client, auth_headers, history_dir):
    from amx.storage.style_store import StyleStore

    _seed_row(history_dir)
    r = client.delete("/api/llm-profiles/default/style", headers=auth_headers)
    assert r.status_code == 200
    assert StyleStore(history_dir / "history.db").get("default") is None


def test_unauthenticated_request_returns_401(client, history_dir):
    r = client.get("/api/llm-profiles/default/style")
    assert r.status_code == 401


def test_extract_with_mocked_helpers(client, auth_headers, history_dir, monkeypatch):
    """Patch the connector + LLM seams so the test stays I/O-free."""
    from amx.storage.style_store import StyleStore

    fake_comments = {f"col_{i}": f"Unique id of order {i}." for i in range(5)}
    fake_llm_resp = json.dumps(
        {
            "language": "en-US",
            "tone": "formal",
            "avg_length_words": 5,
            "length_range": [3, 7],
            "person": "impersonal",
            "capitalization": "sentence-case",
            "ends_with_period": True,
            "structural_patterns": ["noun + role"],
            "vocabulary_register": "business",
            "redacted_examples": ["Unique id of the <ENTITY>."],
        }
    )

    class FakeConn:
        backend = "snowflake"

        def use(self, db):
            pass

        def get_column_comments(self, schema, table):
            return fake_comments

    from amx.web.routers import style as style_router

    monkeypatch.setattr(style_router, "_open_connector", lambda c, p: FakeConn())
    monkeypatch.setattr(style_router, "_make_llm_caller", lambda c, p: lambda s, u: fake_llm_resp)

    from amx.config import DBConfig

    cfg = client.app.state.cfg
    cfg.db_profiles = {"warehouse": DBConfig()}
    cfg.active_db_profile = "warehouse"

    r = client.post(
        "/api/llm-profiles/default/style/extract",
        json={"source_ref": "warehouse.sales.orders"},
        headers=auth_headers,
    )
    assert r.status_code == 200, r.text
    assert r.json()["sample_count"] == 5
    row = StyleStore(history_dir / "history.db").get("default")
    assert row is not None
    assert row.source_ref == "warehouse.sales.orders"
