"""Round-trip the confidence config block through YAML mapping helpers."""

from __future__ import annotations

import pytest

CONFIDENCE_SIGNAL_CHOICES = {"logprob", "self_consistency", "self_decl", "judge", "none"}


def test_default_llm_config_picks_self_consistency_as_active_signal():
    from amx.config import LLMConfig

    cfg = LLMConfig(provider="openai", model="gpt-4o-mini")
    assert cfg.confidence_signal == "self_consistency"
    assert cfg.confidence.enabled is True
    assert cfg.confidence.high == 0.75
    assert cfg.confidence.med == 0.50


def test_confidence_config_has_no_use_star_booleans():
    """Single-signal pivot drops the per-signal use_* gates."""
    from amx.config import ConfidenceConfig

    cfg = ConfidenceConfig()
    for legacy_field in ("use_logprob", "use_self_consistency", "use_self_decl", "use_judge"):
        assert not hasattr(cfg, legacy_field), (
            f"ConfidenceConfig still exposes legacy field {legacy_field!r}"
        )


def test_confidence_signal_round_trips_through_mapping():
    from amx.config import _llm_from_mapping, _llm_to_mapping

    cfg = _llm_from_mapping(
        {
            "provider": "openai",
            "model": "gpt-4o-mini",
            "confidence_signal": "logprob",
            "confidence": {"bands": {"high": 0.80, "med": 0.55}},
        }
    )
    assert cfg.confidence_signal == "logprob"
    assert cfg.confidence.high == 0.80
    assert cfg.confidence.med == 0.55

    rt = _llm_to_mapping(cfg)
    assert rt["confidence_signal"] == "logprob"
    assert rt["confidence"]["bands"]["high"] == 0.80


def test_missing_confidence_signal_falls_back_to_default():
    from amx.config import _llm_from_mapping

    cfg = _llm_from_mapping({"provider": "openai", "model": "gpt-4o-mini"})
    assert cfg.confidence_signal == "self_consistency"
    assert cfg.confidence.enabled is True


def test_legacy_confidence_block_with_use_star_keys_is_ignored_silently():
    """Old YAML configs that still carry ``use_*`` keys must not crash;
    the unknown keys are dropped on load and ``confidence_signal`` falls
    back to the default. Existing users do not need to edit their YAML
    by hand before upgrading."""
    from amx.config import _llm_from_mapping

    cfg = _llm_from_mapping(
        {
            "provider": "openai",
            "model": "gpt-4o-mini",
            "confidence": {
                "enabled": True,
                "use_logprob": False,
                "use_self_consistency": True,
                "use_self_decl": True,
                "use_judge": False,
            },
        }
    )
    assert cfg.confidence_signal == "self_consistency"
    assert cfg.confidence.enabled is True


@pytest.mark.parametrize("value", sorted(CONFIDENCE_SIGNAL_CHOICES))
def test_each_allowed_signal_value_round_trips(value):
    from amx.config import _llm_from_mapping, _llm_to_mapping

    cfg = _llm_from_mapping(
        {"provider": "openai", "model": "gpt-4o-mini", "confidence_signal": value}
    )
    assert cfg.confidence_signal == value
    assert _llm_to_mapping(cfg)["confidence_signal"] == value


def test_unknown_signal_value_falls_back_to_default():
    """Hand-edited YAML with a typo (e.g. ``judje``) must not load a
    broken config; the loader clamps it to the safe default and a
    later /confidence-signal command can fix it."""
    from amx.config import _llm_from_mapping

    cfg = _llm_from_mapping(
        {"provider": "openai", "model": "gpt-4o-mini", "confidence_signal": "judje"}
    )
    assert cfg.confidence_signal == "self_consistency"
