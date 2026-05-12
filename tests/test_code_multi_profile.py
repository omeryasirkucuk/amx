"""PR δ — multi-profile code scope for ``/run``.

``AMXConfig.run_code_profiles`` is a list of code profile names. When
non-empty, ``effective_run_code_paths`` returns the **union** of every
named profile's paths so a single ``/run`` can pull retrieval context
from multiple code collections at once. Empty list falls back to the
single-active-profile contract — no migration needed.
"""

from __future__ import annotations

from amx.config import DISABLED_PROFILE, AMXConfig


def test_run_code_profiles_empty_falls_back_to_active():
    cfg = AMXConfig()
    cfg.code_profiles = {"backend": "/abs/backend"}
    cfg.active_code_profile = "backend"
    assert cfg.run_code_profiles == []
    assert cfg.effective_run_code_paths() == ["/abs/backend"]


def test_run_code_profiles_union():
    cfg = AMXConfig()
    cfg.code_profiles = {
        "backend": "/abs/backend",
        "etl": "/abs/etl",
    }
    cfg.run_code_profiles = ["backend", "etl"]
    assert cfg.effective_run_code_paths() == ["/abs/backend", "/abs/etl"]


def test_run_code_profiles_deduplicates_overlapping_paths():
    cfg = AMXConfig()
    cfg.code_profiles = {
        "a": "/abs/shared",
        "b": "/abs/shared",
        "c": "/abs/c_only",
    }
    cfg.run_code_profiles = ["a", "b", "c"]
    assert cfg.effective_run_code_paths() == ["/abs/shared", "/abs/c_only"]


def test_run_code_profiles_skips_disabled_sentinel():
    cfg = AMXConfig()
    cfg.code_profiles = {"backend": "/abs/backend"}
    cfg.run_code_profiles = [DISABLED_PROFILE, "backend"]
    assert cfg.effective_run_code_paths() == ["/abs/backend"]


def test_effective_code_paths_by_name_returns_just_that_profile():
    cfg = AMXConfig()
    cfg.code_profiles = {
        "backend": "/abs/backend",
        "etl": "/abs/etl",
    }
    assert cfg.effective_code_paths("etl") == ["/abs/etl"]
    assert cfg.effective_code_paths("nonexistent") == []


def test_effective_code_paths_no_name_uses_active():
    cfg = AMXConfig()
    cfg.code_profiles = {"backend": "/abs/backend", "etl": "/abs/etl"}
    cfg.active_code_profile = "etl"
    assert cfg.effective_code_paths() == ["/abs/etl"]


def test_record_code_profile_ingest_stamps_telemetry():
    cfg = AMXConfig()
    cfg.code_profiles = {"backend": "/abs/backend"}

    cfg.record_code_profile_ingest("backend")
    assert cfg.code_profile_last_indexed_at["backend"] > 0
    assert cfg.code_profile_last_error["backend"] == ""

    cfg.record_code_profile_ingest("backend", error="permission denied")
    assert cfg.code_profile_last_error["backend"] == "permission denied"
