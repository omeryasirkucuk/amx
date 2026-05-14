"""Round-trip the alternatives_mode field through YAML mapping helpers."""

from __future__ import annotations

import pytest

ALTERNATIVES_MODE_CHOICES = {"semantic", "lexical"}


def test_default_llm_config_picks_semantic_as_alternatives_mode():
    from amx.config import LLMConfig

    cfg = LLMConfig(provider="openai", model="gpt-4o-mini")
    assert cfg.alternatives_mode == "semantic"


def test_alternatives_mode_round_trips_through_mapping():
    from amx.config import _llm_from_mapping, _llm_to_mapping

    cfg = _llm_from_mapping(
        {
            "provider": "openai",
            "model": "gpt-4o-mini",
            "alternatives_mode": "lexical",
        }
    )
    assert cfg.alternatives_mode == "lexical"

    rt = _llm_to_mapping(cfg)
    assert rt["alternatives_mode"] == "lexical"


def test_missing_alternatives_mode_falls_back_to_default():
    from amx.config import _llm_from_mapping

    cfg = _llm_from_mapping({"provider": "openai", "model": "gpt-4o-mini"})
    assert cfg.alternatives_mode == "semantic"


@pytest.mark.parametrize("value", sorted(ALTERNATIVES_MODE_CHOICES))
def test_each_allowed_alternatives_mode_round_trips(value):
    from amx.config import _llm_from_mapping, _llm_to_mapping

    cfg = _llm_from_mapping(
        {"provider": "openai", "model": "gpt-4o-mini", "alternatives_mode": value}
    )
    assert cfg.alternatives_mode == value
    assert _llm_to_mapping(cfg)["alternatives_mode"] == value


def test_unknown_alternatives_mode_falls_back_to_default():
    """Hand-edited YAML with a typo must not load a broken config;
    the loader clamps it to the safe default."""
    from amx.config import _llm_from_mapping

    cfg = _llm_from_mapping(
        {"provider": "openai", "model": "gpt-4o-mini", "alternatives_mode": "semanitc"}
    )
    assert cfg.alternatives_mode == "semantic"


def test_alternatives_mode_normalises_case():
    """``Semantic`` / ``SEMANTIC`` from a hand-edited YAML are accepted."""
    from amx.config import _llm_from_mapping

    for raw in ("Semantic", "SEMANTIC", "Lexical", "LEXICAL"):
        cfg = _llm_from_mapping(
            {"provider": "openai", "model": "gpt-4o-mini", "alternatives_mode": raw}
        )
        assert cfg.alternatives_mode == raw.lower()


def test_alternatives_mode_choices_constant_exposed():
    from amx.config import (
        ALTERNATIVES_MODE_CHOICES as module_choices,
    )
    from amx.config import (
        DEFAULT_ALTERNATIVES_MODE as module_default,
    )

    assert set(module_choices) == ALTERNATIVES_MODE_CHOICES
    assert module_default == "semantic"
    assert module_default in module_choices
