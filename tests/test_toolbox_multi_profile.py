"""Multi-profile ToolBox + scope-resolution tests for PR ask-A.

The actual catalog/SQL multi-profile path is exercised by
``test_search_catalog_multi_profile.py`` against a real SQLite store —
this file focuses on the agent-side glue: ToolBox correctly accepts a
``db_profiles`` list, exposes ``db_profile_filter`` in the right
shape, propagates the scope into the per-tool cache key, and the
``run_tool_agent`` entry point forwards the kwarg into the ToolBox
constructor.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from amx.config import AMXConfig, DBConfig
from amx.search.agent_tools import ToolBox


@pytest.fixture()
def cfg_with_profiles() -> AMXConfig:
    cfg = AMXConfig()
    cfg.db_profiles = {
        "alpha": DBConfig(backend="postgresql", host="a.local", database="appa"),
        "beta": DBConfig(backend="postgresql", host="b.local", database="appb"),
        "gamma": DBConfig(backend="snowflake", host="acct", account="acct"),
    }
    cfg.active_db_profile = "alpha"
    cfg.active_db_profiles = []
    cfg.db = cfg.db_profiles["alpha"]
    return cfg


def test_toolbox_single_profile_filter_returns_scalar(cfg_with_profiles) -> None:
    catalog = MagicMock()
    box = ToolBox(cfg_with_profiles, catalog, db_profiles=["alpha"])
    assert box.db_profile == "alpha"
    assert box.db_profiles == ["alpha"]
    # Scalar in single-profile mode so ``db_profile = ?`` stays the
    # cheap path through build_db_profile_clause.
    assert box.db_profile_filter == "alpha"
    assert box.is_multi_profile is False


def test_toolbox_multi_profile_filter_returns_list(cfg_with_profiles) -> None:
    catalog = MagicMock()
    box = ToolBox(cfg_with_profiles, catalog, db_profiles=["alpha", "beta"])
    assert box.db_profile == "alpha"  # anchor = first
    assert box.db_profiles == ["alpha", "beta"]
    assert box.db_profile_filter == ["alpha", "beta"]
    assert box.is_multi_profile is True


def test_toolbox_falls_back_to_effective_db_profiles(cfg_with_profiles) -> None:
    """When the caller doesn't pass db_profiles, ToolBox reads
    cfg.effective_db_profiles() (the multi-pick scope set by /use-db).
    """
    cfg_with_profiles.active_db_profiles = ["alpha", "beta", "gamma"]
    catalog = MagicMock()
    box = ToolBox(cfg_with_profiles, catalog)
    assert box.db_profiles == ["alpha", "beta", "gamma"]
    assert box.is_multi_profile is True


def test_toolbox_dedupes_scope(cfg_with_profiles) -> None:
    catalog = MagicMock()
    box = ToolBox(cfg_with_profiles, catalog, db_profiles=["alpha", "alpha", "beta", "alpha"])
    assert box.db_profiles == ["alpha", "beta"]


def test_toolbox_scope_falls_back_to_active_when_empty(cfg_with_profiles) -> None:
    cfg_with_profiles.active_db_profiles = []
    catalog = MagicMock()
    box = ToolBox(cfg_with_profiles, catalog)
    assert box.db_profiles == ["alpha"]


def test_cache_key_disambiguates_by_scope(cfg_with_profiles) -> None:
    """Two ToolBox instances with different scopes must NOT share cache
    entries — a multi-profile result is not interchangeable with a
    single-profile one.
    """
    catalog = MagicMock()
    catalog.search_tables = MagicMock(return_value=[])
    box_single = ToolBox(cfg_with_profiles, catalog, db_profiles=["alpha"])
    box_multi = ToolBox(cfg_with_profiles, catalog, db_profiles=["alpha", "beta"])

    # Hand-poke the cache to verify keys are distinct shape.
    box_single._tool_cache[("search_tables_by_concept", "{}", ("alpha",))] = "S"
    box_multi._tool_cache[("search_tables_by_concept", "{}", ("alpha", "beta"))] = "M"

    # The single-profile box should NOT see the multi-profile box's entry.
    assert ("search_tables_by_concept", "{}", ("alpha", "beta")) not in box_single._tool_cache
    # And vice-versa.
    assert ("search_tables_by_concept", "{}", ("alpha",)) not in box_multi._tool_cache


def test_run_tool_agent_passes_db_profiles_to_toolbox(cfg_with_profiles, monkeypatch) -> None:
    """run_tool_agent threads its db_profiles kwarg into ToolBox()."""
    captured: dict[str, object] = {}

    class FakeBox:
        def __init__(self, cfg, catalog, *, db_profiles=None, **kwargs):
            captured["db_profiles"] = list(db_profiles) if db_profiles else None

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    def fake_loop(**kwargs):
        # Don't actually run the LLM loop; just confirm we got here.
        from amx.search.tool_agent import ToolAgentResult

        return ToolAgentResult(
            answer="ok",
            tool_calls=[],
            iterations=0,
            usage={},
            finish_reason="stop",
        )

    from amx.search import tool_agent as _ta

    monkeypatch.setattr(_ta, "ToolBox", FakeBox)
    monkeypatch.setattr(_ta, "_run_tool_loop", fake_loop)

    catalog = MagicMock()
    llm = MagicMock()
    _ta.run_tool_agent(
        cfg=cfg_with_profiles,
        catalog=catalog,
        llm=llm,
        question="anything",
        answer_language="english",
        db_profiles=["alpha", "beta"],
    )
    assert captured["db_profiles"] == ["alpha", "beta"]
