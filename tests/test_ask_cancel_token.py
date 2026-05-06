"""CLI Ctrl-C cancellation reaches the ask agent loop.

Reported: pressing Ctrl-C in CLI ask mode doesn't terminate the
session — Python's default KeyboardInterrupt eventually fires but the
LLM HTTP call (or a chained tool-call sequence) holds the GIL long
enough that the user sees no response to their first press.

Fix: ``SearchAgent.ask`` accepts ``cancel_token`` and forwards it
through to ``run_tool_agent``. The CLI installs a temporary SIGINT
handler that sets the token (graceful) and raises on the second
press (force). On cancellation, the agent surfaces a clean
``intent='cancelled'`` SearchAnswer instead of bailing into the
legacy router or leaving the chat in an inconsistent state.

These tests pin the wiring at the SearchAgent layer; the SIGINT
handler in search.py is exercised manually since signal-based tests
are flaky in CI.
"""

from __future__ import annotations

import threading
from unittest.mock import MagicMock, patch

import pytest

from amx.agents.orchestrator import RunCancelled
from amx.config import AMXConfig, DBConfig, LLMConfig
from amx.search.agent import SearchAgent
from amx.search.catalog import SearchAnswer


@pytest.fixture()
def cfg_minimal() -> AMXConfig:
    cfg = AMXConfig()
    cfg.db_profiles = {"local": DBConfig(backend="postgresql", host="x", database="y")}
    cfg.active_db_profile = "local"
    cfg.llm_profiles = {"openai": LLMConfig(provider="openai", model="gpt-4o")}
    cfg.active_llm_profile = "openai"
    cfg.llm = LLMConfig(provider="openai", model="gpt-4o")
    return cfg


def _real_settings_catalog() -> MagicMock:
    """Build a catalog mock whose ``get_settings`` returns a real
    dict — the agent reads ``settings.get("use_tool_agent", "true")``
    and converts it via ``str().lower()``, so a default MagicMock
    answer would coerce to a junk string and skip the tool-agent
    dispatch we want to test.
    """
    catalog = MagicMock(name="SearchCatalog")
    catalog.get_settings.return_value = {"use_tool_agent": "true"}
    return catalog


def test_search_agent_ask_passes_cancel_token_to_tool_agent(cfg_minimal) -> None:
    """SearchAgent.ask threads its ``cancel_token`` kwarg into the
    short-circuit tool-agent dispatch so the loop can poll it."""
    catalog = _real_settings_catalog()
    agent = SearchAgent(cfg_minimal, catalog, db_profiles=["local"])
    token = threading.Event()
    captured: dict[str, object] = {}

    def fake_dispatch(*, cancel_token=None, **_):
        captured["cancel_token"] = cancel_token
        return SearchAnswer(
            intent="tool_agent",
            question="q",
            rows=[],
            confidence="high",
            summary="ok",
            provenance=[],
            details={},
        )

    # Bypass the upstream short-circuit chain that doesn't take the
    # token; the dispatch we care about is _answer_via_tool_agent.
    with patch.object(agent, "_handle_chitchat", return_value=None):
        with patch.object(agent, "_handle_meta_query", return_value=None):
            with patch.object(agent, "_handle_followup_reaffirmation", return_value=None):
                with patch.object(agent, "_llm_available", return_value=True):
                    with patch.object(agent, "_answer_via_tool_agent", side_effect=fake_dispatch):
                        with patch.object(agent, "_ensure_session_id", return_value=None):
                            answer = agent.ask("test question", cancel_token=token)
    assert captured["cancel_token"] is token
    assert answer.intent == "tool_agent"


def test_run_cancelled_returns_friendly_answer(cfg_minimal) -> None:
    """When the tool agent raises RunCancelled (the user pressed
    Ctrl-C and the cancel_token was set), SearchAgent.ask must turn
    it into a clean cancelled SearchAnswer rather than letting the
    exception escape into the REPL."""
    catalog = _real_settings_catalog()
    agent = SearchAgent(cfg_minimal, catalog, db_profiles=["local"])

    def boom(**_):
        raise RunCancelled("user cancel")

    with patch.object(agent, "_handle_chitchat", return_value=None):
        with patch.object(agent, "_handle_meta_query", return_value=None):
            with patch.object(agent, "_handle_followup_reaffirmation", return_value=None):
                with patch.object(agent, "_llm_available", return_value=True):
                    with patch.object(agent, "_answer_via_tool_agent", side_effect=boom):
                        with patch.object(agent, "_ensure_session_id", return_value=None):
                            answer = agent.ask("test", cancel_token=threading.Event())
    assert answer.intent == "cancelled"
    assert "cancel" in answer.summary.lower()
    assert answer.details.get("reason") == "cancelled_by_user"


def test_run_cancelled_message_in_turkish_for_turkish_question(cfg_minimal) -> None:
    """When the question was Turkish, the cancellation message
    matches so the user gets a consistent locale."""
    catalog = _real_settings_catalog()
    agent = SearchAgent(cfg_minimal, catalog, db_profiles=["local"])

    def boom(**_):
        raise RunCancelled("user cancel")

    with patch.object(agent, "_handle_chitchat", return_value=None):
        with patch.object(agent, "_handle_meta_query", return_value=None):
            with patch.object(agent, "_handle_followup_reaffirmation", return_value=None):
                with patch.object(agent, "_llm_available", return_value=True):
                    with patch.object(agent, "_answer_via_tool_agent", side_effect=boom):
                        with patch.object(agent, "_ensure_session_id", return_value=None):
                            answer = agent.ask(
                                "satışları olan tabloları göster",
                                cancel_token=threading.Event(),
                            )
    assert answer.intent == "cancelled"
    assert "iptal" in answer.summary.lower()


def test_search_agent_ask_works_without_cancel_token(cfg_minimal) -> None:
    """Backwards-compat: callers that don't pass cancel_token still
    work — the kwarg defaults to None and the agent skips the
    cancel-token-aware path."""
    catalog = _real_settings_catalog()
    agent = SearchAgent(cfg_minimal, catalog, db_profiles=["local"])

    def fake_dispatch(*, cancel_token=None, **_):
        assert cancel_token is None  # default from signature
        return SearchAnswer(
            intent="tool_agent",
            question="q",
            rows=[],
            confidence="high",
            summary="ok",
            provenance=[],
            details={},
        )

    with patch.object(agent, "_handle_chitchat", return_value=None):
        with patch.object(agent, "_handle_meta_query", return_value=None):
            with patch.object(agent, "_handle_followup_reaffirmation", return_value=None):
                with patch.object(agent, "_llm_available", return_value=True):
                    with patch.object(agent, "_answer_via_tool_agent", side_effect=fake_dispatch):
                        with patch.object(agent, "_ensure_session_id", return_value=None):
                            answer = agent.ask("test")
    assert answer.intent == "tool_agent"
