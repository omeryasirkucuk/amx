"""Pin the rag_llm_profile override contract.

The user can run ``/use-rag-llm <name>`` to point the RAG agent at a
different LLM profile than the global ``active_llm_profile``. The
override must:

- round-trip through YAML save/load,
- fall back to the active profile when empty OR when pointed at a
  deleted profile (silent self-heal so a stale value never raises),
- be cleared automatically when the named profile is removed.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from amx.config import AMXConfig, LLMConfig


def _seeded_cfg() -> AMXConfig:
    cfg = AMXConfig()
    cfg.llm_profiles["big"] = LLMConfig(
        provider="anthropic", model="claude-sonnet-4-5", api_key="k1"
    )
    cfg.llm_profiles["small"] = LLMConfig(provider="openai", model="gpt-4o-mini", api_key="k2")
    cfg.set_active_llm_profile("big")
    return cfg


def test_effective_rag_llm_falls_back_to_active_when_unset() -> None:
    cfg = _seeded_cfg()
    assert cfg.rag_llm_profile == ""
    # Identity matters: callers gate on `rag_cfg is not cfg.llm` to
    # avoid building a redundant LLMProvider when there is no override.
    assert cfg.effective_rag_llm() is cfg.llm


def test_effective_rag_llm_returns_named_profile_when_set() -> None:
    cfg = _seeded_cfg()
    cfg.rag_llm_profile = "small"
    eff = cfg.effective_rag_llm()
    assert eff.model == "gpt-4o-mini"
    assert eff is not cfg.llm


def test_effective_rag_llm_falls_back_when_pointing_at_missing_profile() -> None:
    cfg = _seeded_cfg()
    cfg.rag_llm_profile = "ghost"
    # Silent self-heal: callers must never see a KeyError because the
    # YAML on disk lagged behind a profile deletion.
    assert cfg.effective_rag_llm() is cfg.llm


def test_remove_llm_profile_clears_rag_override_when_it_matches() -> None:
    cfg = _seeded_cfg()
    cfg.rag_llm_profile = "small"
    cfg.remove_llm_profile("small")
    assert cfg.rag_llm_profile == ""


def test_remove_llm_profile_leaves_rag_override_when_unrelated() -> None:
    cfg = _seeded_cfg()
    cfg.llm_profiles["other"] = LLMConfig(provider="openai", model="gpt-4o", api_key="k3")
    cfg.rag_llm_profile = "small"
    cfg.remove_llm_profile("other")
    assert cfg.rag_llm_profile == "small"


def test_rag_llm_profile_round_trips_through_yaml() -> None:
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "config.yml"
        cfg = _seeded_cfg()
        cfg.rag_llm_profile = "small"
        cfg.save(str(path))

        reloaded = AMXConfig.load(str(path))
        assert reloaded.rag_llm_profile == "small"
        assert reloaded.effective_rag_llm().model == "gpt-4o-mini"


def test_dangling_rag_llm_profile_is_dropped_on_load() -> None:
    """A YAML that names a profile no longer in llm_profiles must
    self-heal to an empty override on load — otherwise the orchestrator
    would silently fall back per call but the YAML would keep pointing
    at a ghost forever."""
    import yaml

    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "config.yml"
        cfg = _seeded_cfg()
        cfg.save(str(path))

        # Hand-edit the YAML to inject a dangling override.
        data = yaml.safe_load(path.read_text())
        data["rag_llm_profile"] = "ghost"
        path.write_text(yaml.dump(data, sort_keys=False))

        reloaded = AMXConfig.load(str(path))
        assert reloaded.rag_llm_profile == ""


def test_orchestrator_routes_rag_agent_to_override_llm() -> None:
    """The Orchestrator builds RAGAgent with rag_llm when supplied,
    falls back to the global llm otherwise. This guards the wiring at
    amx/agents/orchestrator.py:382."""
    from amx.agents.orchestrator import Orchestrator

    class _StubLLM:
        def __init__(self, tag: str) -> None:
            self.tag = tag

    class _StubDB:
        pass

    class _StubRAGStore:
        pass

    main = _StubLLM("global")
    rag = _StubLLM("rag-only")

    orch = Orchestrator(
        db=_StubDB(),
        llm=main,
        rag_store=_StubRAGStore(),
        rag_llm=rag,
    )
    assert orch.rag_agent is not None
    assert orch.rag_agent.llm is rag
    # The other agents (profile, code) keep using the global llm.
    assert orch.profile_agent.llm is main

    orch_default = Orchestrator(
        db=_StubDB(),
        llm=main,
        rag_store=_StubRAGStore(),
    )
    assert orch_default.rag_agent is not None
    assert orch_default.rag_agent.llm is main


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
