"""Profile-swap override path on ``LLMOverrides``.

When the per-run override carries ``profile=<saved-profile-name>``, the
named profile's full LLMConfig (provider/model/api_key/api_base) must
become the base for this run — per-knob overrides then layer on top.
Saved profiles on disk are never mutated. Unknown profile names
degrade safely (the active profile is used and the audit drops
``profile``).
"""

from amx.config import AMXConfig, LLMConfig
from amx.web.routers.runs import LLMOverrides, _apply_llm_overrides


def _cfg_with_two_profiles() -> AMXConfig:
    cfg = AMXConfig()
    cfg.llm = LLMConfig(
        provider="anthropic",
        model="claude-sonnet-4-5",
        api_key="active-key",
        api_base=None,
        temperature=0.2,
        n_alternatives=3,
    )
    cfg.llm_profiles = {
        "fast": LLMConfig(
            provider="openai",
            model="gpt-4o-mini",
            api_key="fast-key",
            api_base="https://api.openai.com/v1",
            temperature=0.9,
            n_alternatives=5,
        ),
        "active": cfg.llm,
    }
    cfg.active_llm_profile = "active"
    return cfg


class TestApplyLlmOverrides:
    def test_no_overrides_returns_cfg_unchanged(self) -> None:
        cfg = _cfg_with_two_profiles()
        derived, audit = _apply_llm_overrides(cfg, None)
        assert derived is cfg
        assert audit == {}

    def test_empty_overrides_returns_cfg_unchanged(self) -> None:
        cfg = _cfg_with_two_profiles()
        derived, audit = _apply_llm_overrides(cfg, LLMOverrides())
        assert derived is cfg
        assert audit == {}

    def test_profile_swap_loads_full_bundle(self) -> None:
        cfg = _cfg_with_two_profiles()
        derived, audit = _apply_llm_overrides(cfg, LLMOverrides(profile="fast"))
        assert derived.llm.provider == "openai"
        assert derived.llm.model == "gpt-4o-mini"
        assert derived.llm.api_key == "fast-key"
        assert derived.llm.api_base == "https://api.openai.com/v1"
        # Saved profile dict on cfg untouched.
        assert cfg.llm_profiles["fast"].temperature == 0.9
        assert audit == {"profile": "fast"}

    def test_profile_swap_with_per_knob_override(self) -> None:
        cfg = _cfg_with_two_profiles()
        derived, audit = _apply_llm_overrides(
            cfg, LLMOverrides(profile="fast", temperature=0.3)
        )
        # Profile's full bundle in effect …
        assert derived.llm.provider == "openai"
        assert derived.llm.api_key == "fast-key"
        # … with per-knob override layered on top.
        assert derived.llm.temperature == 0.3
        # Saved profile temperature still its original value.
        assert cfg.llm_profiles["fast"].temperature == 0.9
        assert audit == {"profile": "fast", "temperature": 0.3}

    def test_unknown_profile_falls_back_safely(self) -> None:
        cfg = _cfg_with_two_profiles()
        derived, audit = _apply_llm_overrides(
            cfg, LLMOverrides(profile="missing", temperature=0.5)
        )
        # Active profile (anthropic) stayed in place …
        assert derived.llm.provider == "anthropic"
        assert derived.llm.api_key == "active-key"
        # … the per-knob override still applied …
        assert derived.llm.temperature == 0.5
        # … and ``profile`` was dropped from the audit.
        assert "profile" not in audit
        assert audit == {"temperature": 0.5}

    def test_per_knob_override_only(self) -> None:
        """Existing pre-profile-feature behaviour still works."""
        cfg = _cfg_with_two_profiles()
        derived, audit = _apply_llm_overrides(cfg, LLMOverrides(temperature=0.7))
        assert derived.llm.provider == "anthropic"
        assert derived.llm.temperature == 0.7
        assert audit == {"temperature": 0.7}
