"""Per-question + sticky scope plumbing tests for /api/ask.

PR ask-A: AskRequest gains ``scope_profiles`` (per-question override),
chat_sessions gains a ``scope_profiles_json`` column for sticky scope,
and a new ``PATCH /api/ask/sessions/{id}`` lets the SPA's dropdown
update the sticky scope without flipping global config.
"""

from __future__ import annotations

import pytest

from amx.config import DBConfig
from amx.web.routers import ask as ask_router


@pytest.fixture()
def cfg_with_profiles(cfg):
    cfg.db_profiles["alpha"] = DBConfig(backend="postgresql", host="a", database="appa")
    cfg.db_profiles["beta"] = DBConfig(backend="postgresql", host="b", database="appb")
    cfg.db_profiles["gamma"] = DBConfig(backend="postgresql", host="c", database="appc")
    cfg.active_db_profile = "alpha"
    cfg.db = cfg.db_profiles["alpha"]
    return cfg


def test_resolve_ask_scope_dedupes_and_drops_ghosts(cfg_with_profiles) -> None:
    out = ask_router._resolve_ask_scope(
        cfg_with_profiles,
        body_scope=["alpha", "alpha", "beta", "ghost"],
        session_scope=None,
    )
    assert out == ["alpha", "beta"]


def test_resolve_ask_scope_body_overrides_session(cfg_with_profiles) -> None:
    out = ask_router._resolve_ask_scope(
        cfg_with_profiles,
        body_scope=["beta"],
        session_scope=["alpha"],
    )
    assert out == ["beta"]


def test_resolve_ask_scope_session_when_body_missing(cfg_with_profiles) -> None:
    out = ask_router._resolve_ask_scope(
        cfg_with_profiles,
        body_scope=None,
        session_scope=["beta", "gamma"],
    )
    assert out == ["beta", "gamma"]


def test_resolve_ask_scope_falls_back_to_all_profiles(cfg_with_profiles) -> None:
    out = ask_router._resolve_ask_scope(
        cfg_with_profiles,
        body_scope=None,
        session_scope=None,
    )
    assert sorted(out) == ["alpha", "beta", "gamma"]


def test_resolve_ask_scope_empty_body_distinct_from_none(cfg_with_profiles) -> None:
    """Body=[] (explicit empty list) means "scope to nothing valid"
    which collapses to []. Body=None falls back to session/default.
    """
    out_empty = ask_router._resolve_ask_scope(cfg_with_profiles, body_scope=[], session_scope=None)
    assert out_empty == []
    out_none = ask_router._resolve_ask_scope(cfg_with_profiles, body_scope=None, session_scope=None)
    assert sorted(out_none) == ["alpha", "beta", "gamma"]


def test_patch_session_scope_404_when_unknown(client, auth_headers, cfg_with_profiles) -> None:
    """PATCH /api/ask/sessions/{id} returns 404 if the session is
    missing or the history store isn't initialised yet."""
    # Without a history store, the 503 fast-path applies.
    response = client.patch(
        "/api/ask/sessions/9999",
        headers=auth_headers,
        json={"scope_profiles": ["alpha"]},
    )
    assert response.status_code in (404, 503)


def test_submit_ask_threads_scope_into_worker(
    client, auth_headers, cfg_with_profiles, monkeypatch
) -> None:
    """The worker spawn carries the resolved scope so run_tool_agent
    sees the per-question profiles."""
    captured: dict[str, object] = {}

    def fake_thread(*, target, args, name=None, daemon=None):
        # _ask_worker(cfg, job, question, session_id, db_profile, scope_profiles)
        captured["scope_profiles"] = list(args[5])

        class _T:
            def start(self_inner) -> None:
                return None

        return _T()

    import threading as _th

    monkeypatch.setattr(_th, "Thread", fake_thread)

    response = client.post(
        "/api/ask",
        headers=auth_headers,
        json={
            "question": "test",
            "scope_profiles": ["beta", "gamma"],
        },
    )
    assert response.status_code == 200
    assert response.json()["scope_profiles"] == ["beta", "gamma"]
    assert captured["scope_profiles"] == ["beta", "gamma"]


def test_submit_ask_falls_back_to_all_profiles(
    client, auth_headers, cfg_with_profiles, monkeypatch
) -> None:
    captured: dict[str, object] = {}

    def fake_thread(*, target, args, name=None, daemon=None):
        captured["scope_profiles"] = list(args[5])

        class _T:
            def start(self_inner) -> None:
                return None

        return _T()

    import threading as _th

    monkeypatch.setattr(_th, "Thread", fake_thread)

    response = client.post(
        "/api/ask",
        headers=auth_headers,
        json={"question": "no scope here"},
    )
    assert response.status_code == 200
    # Default = every saved DB profile.
    assert sorted(captured["scope_profiles"]) == ["alpha", "beta", "gamma"]
