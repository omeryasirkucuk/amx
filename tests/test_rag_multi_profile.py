"""PR D — multi-profile doc scope for ``/run``.

``AMXConfig.run_doc_profiles`` is a list of doc profile names. When
non-empty, ``effective_run_doc_paths`` returns the **union** of every
named profile's paths so a single ``/run`` can pull retrieval context
from multiple doc collections at once. Empty list falls back to the
single-active-profile contract — no migration needed.
"""

from __future__ import annotations

from amx.config import AMXConfig


def test_effective_run_doc_paths_falls_back_to_single_profile():
    cfg = AMXConfig()
    cfg.doc_profiles = {"handbook": ["/abs/handbook"]}
    cfg.active_doc_profile = "handbook"
    assert cfg.run_doc_profiles == []
    assert cfg.effective_run_doc_paths() == ["/abs/handbook"]


def test_effective_run_doc_paths_unions_multiple_profiles():
    cfg = AMXConfig()
    cfg.doc_profiles = {
        "handbook": ["/abs/handbook", "/abs/wiki"],
        "spec": ["/abs/spec"],
    }
    cfg.run_doc_profiles = ["handbook", "spec"]
    paths = cfg.effective_run_doc_paths()
    assert paths == ["/abs/handbook", "/abs/wiki", "/abs/spec"]


def test_effective_run_doc_paths_deduplicates_overlapping_paths():
    cfg = AMXConfig()
    cfg.doc_profiles = {
        "a": ["/abs/shared", "/abs/a_only"],
        "b": ["/abs/shared", "/abs/b_only"],
    }
    cfg.run_doc_profiles = ["a", "b"]
    paths = cfg.effective_run_doc_paths()
    assert paths == ["/abs/shared", "/abs/a_only", "/abs/b_only"]


def test_effective_run_doc_paths_skips_disabled_sentinel():
    from amx.config import DISABLED_PROFILE

    cfg = AMXConfig()
    cfg.doc_profiles = {"handbook": ["/abs/handbook"]}
    cfg.run_doc_profiles = [DISABLED_PROFILE, "handbook"]
    assert cfg.effective_run_doc_paths() == ["/abs/handbook"]


def test_effective_doc_paths_by_name_returns_just_that_profile():
    cfg = AMXConfig()
    cfg.doc_profiles = {
        "handbook": ["/abs/handbook"],
        "spec": ["/abs/spec"],
    }
    assert cfg.effective_doc_paths("spec") == ["/abs/spec"]
    assert cfg.effective_doc_paths("nonexistent") == []
