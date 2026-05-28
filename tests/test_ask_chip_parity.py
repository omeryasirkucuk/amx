"""CLI /ask forwards the lineage / pages / asset chips to the tool agent.

``SearchAgent.ask`` accepts ``lineage_profiles`` / ``pages_enabled`` /
``asset_kinds`` but used to drop them on the tool-agent path (they only
reached the legacy router). This pins the wiring so the CLI honours the
same chips Studio does, feeding both the SELECTION CONTEXT block and the
anchor-based lineage/pages enrichment.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

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
    catalog = MagicMock(name="SearchCatalog")
    catalog.get_settings.return_value = {"use_tool_agent": "true"}
    return catalog


def test_ask_forwards_chip_selections_to_tool_agent(cfg_minimal) -> None:
    catalog = _real_settings_catalog()
    agent = SearchAgent(cfg_minimal, catalog, db_profiles=["local"])
    captured: dict[str, object] = {}

    def fake_dispatch(*, lineage_profiles=None, pages_enabled=None, asset_kinds=None, **_):
        captured["lineage_profiles"] = lineage_profiles
        captured["pages_enabled"] = pages_enabled
        captured["asset_kinds"] = asset_kinds
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
                            answer = agent.ask(
                                "which lineage did I select?",
                                lineage_profiles=["customers-canvas"],
                                pages_enabled=False,
                                asset_kinds=["jobs"],
                            )

    assert captured["lineage_profiles"] == ["customers-canvas"]
    assert captured["pages_enabled"] is False
    assert captured["asset_kinds"] == ["jobs"]
    assert answer.intent == "tool_agent"
