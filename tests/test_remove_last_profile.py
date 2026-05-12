"""Last / active profile deletion is allowed.

Reported: AMX refused to delete the active or last DB / LLM profile
("Cannot remove the last DB profile", "Cannot delete the active LLM
profile"). Forced a roundabout reset (add throwaway → activate →
delete → delete throwaway). The user wants the simpler "if I want to
clear everything, let me clear everything" semantics — Studio and
CLI both surface the empty-config state cleanly downstream.

These tests pin the new behaviour at the config layer (which the
web routers and CLI commands both compose).
"""

from __future__ import annotations

import pytest

from amx.config import AMXConfig, DBConfig, LLMConfig


@pytest.fixture()
def cfg_one_db_one_llm() -> AMXConfig:
    cfg = AMXConfig()
    cfg.db_profiles = {"only-db": DBConfig(backend="postgresql", host="x")}
    cfg.active_db_profile = "only-db"
    cfg.active_db_profiles = ["only-db"]
    cfg.llm_profiles = {"only-llm": LLMConfig(provider="openai", model="gpt-4o")}
    cfg.active_llm_profile = "only-llm"
    return cfg


def test_remove_last_db_profile_clears_active(cfg_one_db_one_llm) -> None:
    """The single DB profile can be deleted. cfg.db_profiles ends
    empty, active pointer cleared, cfg.db reset so callers don't see
    stale data from the deleted profile."""
    cfg_one_db_one_llm.remove_db_profile("only-db")
    assert cfg_one_db_one_llm.db_profiles == {}
    assert cfg_one_db_one_llm.active_db_profile == ""
    assert cfg_one_db_one_llm.active_db_profiles == []
    # cfg.db is a fresh empty DBConfig, not the deleted profile.
    assert cfg_one_db_one_llm.db.host != "x"


def test_remove_last_llm_profile_clears_active(cfg_one_db_one_llm) -> None:
    """The single LLM profile can be deleted. cfg.llm reset to an
    empty LLMConfig so /ask sees an unconfigured state and surfaces
    the friendly "configure LLM" prompt."""
    cfg_one_db_one_llm.remove_llm_profile("only-llm")
    assert cfg_one_db_one_llm.llm_profiles == {}
    assert cfg_one_db_one_llm.active_llm_profile == ""
    assert (cfg_one_db_one_llm.llm.provider or "") == ""
    assert (cfg_one_db_one_llm.llm.model or "") == ""


def test_remove_active_db_profile_promotes_next() -> None:
    """When others exist, removing the active profile promotes the
    next available one — no manual /use-db activation step needed."""
    cfg = AMXConfig()
    cfg.db_profiles = {
        "alpha": DBConfig(backend="postgresql", host="a"),
        "beta": DBConfig(backend="postgresql", host="b"),
    }
    cfg.active_db_profile = "alpha"
    cfg.active_db_profiles = ["alpha"]
    cfg.remove_db_profile("alpha")
    assert "alpha" not in cfg.db_profiles
    assert cfg.active_db_profile == "beta"


def test_remove_active_llm_profile_promotes_next() -> None:
    cfg = AMXConfig()
    cfg.llm_profiles = {
        "first": LLMConfig(provider="openai", model="gpt-4o"),
        "second": LLMConfig(provider="anthropic", model="claude-3"),
    }
    cfg.active_llm_profile = "first"
    cfg.remove_llm_profile("first")
    assert "first" not in cfg.llm_profiles
    assert cfg.active_llm_profile == "second"
    # cfg.llm reflects the newly-promoted profile so downstream
    # callers reading cfg.llm.provider / .model see the right thing.
    assert cfg.llm.provider == "anthropic"


def test_remove_inactive_db_profile_keeps_active_pointer() -> None:
    cfg = AMXConfig()
    cfg.db_profiles = {
        "alpha": DBConfig(backend="postgresql", host="a"),
        "beta": DBConfig(backend="postgresql", host="b"),
    }
    cfg.active_db_profile = "alpha"
    cfg.remove_db_profile("beta")
    assert cfg.active_db_profile == "alpha"


def test_llm_available_returns_false_after_last_delete(cfg_one_db_one_llm) -> None:
    """After wiping the last LLM profile, ``SearchAgent._llm_available``
    must return False so /ask shows the configure-llm prompt instead
    of trying to dispatch with an empty provider."""
    from amx.search._agent.session_memory import SessionMemoryMixin

    # Simulate the SearchAgent's check by reading the same fields.
    cfg_one_db_one_llm.remove_llm_profile("only-llm")

    # Mirror SessionMemoryMixin._llm_available's logic verbatim — the
    # method itself needs `self.settings`/`self.cfg` so we replay
    # the underlying read.
    has_provider = bool(getattr(cfg_one_db_one_llm.llm, "provider", ""))
    has_model = bool(getattr(cfg_one_db_one_llm.llm, "model", ""))
    assert not (has_provider and has_model)
    # Reference the mixin so the import is exercised by the test
    # (catches accidental rename/removal of the method).
    assert hasattr(SessionMemoryMixin, "_llm_available")


def test_remove_last_db_profile_through_loaded_config_persists(tmp_path) -> None:
    """Regression: deleting the last DB profile through a write-through
    ``AMXConfig.load()`` instance must actually drop the entry — both in
    memory and in the YAML. The pre-fix shape was: mid-removal,
    ``active_db_profiles = [...]`` reassignment tripped ``__setattr__`` ->
    autosave, and ``save()``'s "mirror active profile data into
    ``db_profiles[active]``" contract resurrected the just-popped row
    because ``active_db_profile`` was still pointing at it. The CLI then
    reported success while the profile silently came back."""
    import yaml

    cfg_path = tmp_path / "config.yml"
    cfg_path.write_text(
        "schema_version: 1\n"
        "db_profiles:\n"
        "  dbr:\n"
        "    backend: databricks\n"
        "    host: dbc-example.cloud.databricks.com\n"
        "active_db_profile: dbr\n"
        "active_db_profiles: [dbr]\n"
    )
    cfg = AMXConfig.load(str(cfg_path))
    assert "dbr" in cfg.db_profiles

    cfg.remove_db_profile("dbr")

    assert cfg.db_profiles == {}
    assert cfg.active_db_profile == ""
    assert cfg.active_db_profiles == []

    on_disk = yaml.safe_load(cfg_path.read_text()) or {}
    assert on_disk.get("db_profiles") in ({}, None)
    assert (on_disk.get("active_db_profile") or "") == ""


def test_remove_unknown_profile_still_raises() -> None:
    """Trying to remove a name that doesn't exist still raises
    KeyError — no silent failure that masks typos."""
    cfg = AMXConfig()
    cfg.db_profiles = {"a": DBConfig()}
    with pytest.raises(KeyError):
        cfg.remove_db_profile("nonexistent")
    cfg.llm_profiles = {"a": LLMConfig()}
    with pytest.raises(KeyError):
        cfg.remove_llm_profile("nonexistent")
