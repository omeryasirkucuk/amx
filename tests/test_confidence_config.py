"""Round-trip the new confidence config block through YAML mapping helpers."""

from __future__ import annotations


def test_default_confidence_config_enabled_with_both_phase1_signals():
    from amx.config import LLMConfig

    cfg = LLMConfig(provider="openai", model="gpt-4o-mini")
    assert cfg.confidence.enabled is True
    assert cfg.confidence.use_logprob is True
    assert cfg.confidence.use_self_consistency is True
    assert cfg.confidence.use_self_decl is False
    assert cfg.confidence.use_judge is False
    assert cfg.confidence.high == 0.75
    assert cfg.confidence.med == 0.50


def test_confidence_round_trips_through_mapping():
    from amx.config import _llm_from_mapping, _llm_to_mapping

    cfg = _llm_from_mapping(
        {
            "provider": "openai",
            "model": "gpt-4o-mini",
            "confidence": {
                "enabled": True,
                "use_logprob": False,
                "use_self_consistency": True,
                "bands": {"high": 0.80, "med": 0.55},
            },
        }
    )
    assert cfg.confidence.use_logprob is False
    assert cfg.confidence.use_self_consistency is True
    assert cfg.confidence.high == 0.80
    assert cfg.confidence.med == 0.55

    rt = _llm_to_mapping(cfg)
    assert rt["confidence"]["use_logprob"] is False
    assert rt["confidence"]["bands"]["high"] == 0.80


def test_missing_confidence_block_falls_back_to_defaults():
    from amx.config import _llm_from_mapping

    cfg = _llm_from_mapping({"provider": "openai", "model": "gpt-4o-mini"})
    assert cfg.confidence.enabled is True
    assert cfg.confidence.high == 0.75
