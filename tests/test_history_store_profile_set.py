"""Tests for the multi-profile history-store union helper + config round-trip."""

from __future__ import annotations

from amx.config import AMXConfig, history_store_profile_set


def test_singular_only_returns_primary() -> None:
    cfg = AMXConfig()
    cfg.history_store_profile = "prod_dwh"
    cfg.history_store_profiles = []
    assert history_store_profile_set(cfg) == ["prod_dwh"]


def test_extras_only_returns_extras() -> None:
    cfg = AMXConfig()
    cfg.history_store_profile = ""
    cfg.history_store_profiles = ["a", "b"]
    assert history_store_profile_set(cfg) == ["a", "b"]


def test_union_dedupes_primary_in_extras() -> None:
    cfg = AMXConfig()
    cfg.history_store_profile = "prod"
    cfg.history_store_profiles = ["prod", "dev", "staging"]
    assert history_store_profile_set(cfg) == ["prod", "dev", "staging"]


def test_union_strips_empty_strings_and_whitespace() -> None:
    cfg = AMXConfig()
    cfg.history_store_profile = "prod"
    cfg.history_store_profiles = ["", "  ", "dev", " staging "]
    assert history_store_profile_set(cfg) == ["prod", "dev", "staging"]


def test_empty_config_returns_empty_list() -> None:
    cfg = AMXConfig()
    cfg.history_store_profile = ""
    cfg.history_store_profiles = []
    assert history_store_profile_set(cfg) == []


def test_extras_preserve_insertion_order() -> None:
    cfg = AMXConfig()
    cfg.history_store_profile = ""
    cfg.history_store_profiles = ["z", "a", "m"]
    assert history_store_profile_set(cfg) == ["z", "a", "m"]


def test_legacy_config_loads_without_extras_key(tmp_path) -> None:
    """A YAML file written by AMX 0.16.x has no ``history_store_profiles``
    key. The loader must default to an empty list, not crash."""
    import yaml

    legacy_yaml = {
        "history_store_enabled": True,
        "history_store_profile": "prod",
        "history_store_schema": "AMX",
        "history_store_database": "",
    }
    path = tmp_path / "config.yml"
    path.write_text(yaml.safe_dump(legacy_yaml), encoding="utf-8")

    cfg = AMXConfig.load(str(path))
    assert cfg.history_store_profile == "prod"
    assert cfg.history_store_profiles == []
    assert history_store_profile_set(cfg) == ["prod"]


def test_new_config_round_trips_extras(tmp_path) -> None:
    """Setting extras, saving, and re-loading preserves the list."""
    path = tmp_path / "config.yml"
    cfg = AMXConfig.load(str(path))
    cfg.history_store_profile = "prod"
    cfg.history_store_profiles = ["dev", "staging"]
    cfg.save()

    reloaded = AMXConfig.load(str(path))
    assert reloaded.history_store_profile == "prod"
    assert reloaded.history_store_profiles == ["dev", "staging"]
    assert history_store_profile_set(reloaded) == ["prod", "dev", "staging"]
