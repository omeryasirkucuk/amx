"""Phase 2 (0.11.0): persisted multi-pick DB scope + ProfileScope helper.

These tests pin the new contract introduced by the
``feat/multi-db-execution-and-optional-database`` branch:

* ``AMXConfig`` gains an ``active_db_profiles: list[str]`` field that
  is the source of truth for the multi-DB execution scope.
* ``set_active_db_profile(name)`` and ``set_active_db_profiles(names)``
  both keep the legacy ``active_db_profile`` scalar in sync as the
  first entry of the list (so the 99 existing call sites keep working
  unchanged).
* ``effective_db_profiles()`` returns the resolved scope, falling
  back to the legacy scalar when the list is empty (legacy YAML).
* ``ProfileScope`` is the per-command immutable scope object used by
  ``/ask``, ``/run`` and ``/sync``. It dedupes, preserves order,
  exposes ``is_multi``, and yields connectors lazily.
"""

from __future__ import annotations

from amx.config import AMXConfig, DBConfig
from amx.services.profile_scope import ProfileScope


def _build_cfg(
    *profile_specs: tuple[str, str, str],
) -> AMXConfig:
    """Build an in-memory AMXConfig with stub DB profiles.

    Each spec is ``(name, backend, host)`` — enough to satisfy
    ``is_connection_configured`` for PG so the legacy invariants
    around active profile tracking still hold.
    """
    cfg = AMXConfig()
    for name, backend, host in profile_specs:
        cfg.db_profiles[name] = DBConfig(
            backend=backend,
            host=host,
            user="alice",
            password="x",
        )
    if profile_specs:
        first = profile_specs[0][0]
        # Preload the default scope. Mirror what ``load()`` does on the
        # legacy single-active path.
        cfg.active_db_profile = first
        cfg.active_db_profiles = [first]
        cfg.db = cfg.db_profiles[first]
    return cfg


# ── AMXConfig: multi-pick setters ─────────────────────────────────────────


def test_set_active_db_profile_collapses_scope_to_single():
    cfg = _build_cfg(
        ("prod_pg", "postgresql", "prod.example.com"),
        ("analytics_pg", "postgresql", "analytics.example.com"),
    )
    cfg.active_db_profiles = ["prod_pg", "analytics_pg"]
    cfg.set_active_db_profile("analytics_pg")
    assert cfg.active_db_profile == "analytics_pg"
    assert cfg.active_db_profiles == ["analytics_pg"]
    assert cfg.db is cfg.db_profiles["analytics_pg"]


def test_set_active_db_profiles_persists_multi_scope():
    cfg = _build_cfg(
        ("prod_pg", "postgresql", "prod.example.com"),
        ("analytics_pg", "postgresql", "analytics.example.com"),
        ("warehouse_sf", "snowflake", "xy12345.us-east-1"),
    )
    cfg.set_active_db_profiles(["analytics_pg", "warehouse_sf"])
    assert cfg.active_db_profiles == ["analytics_pg", "warehouse_sf"]
    # Legacy scalar mirrors the first list entry for back-compat.
    assert cfg.active_db_profile == "analytics_pg"
    assert cfg.db is cfg.db_profiles["analytics_pg"]


def test_set_active_db_profiles_dedupes_preserving_order():
    cfg = _build_cfg(
        ("a", "postgresql", "a.example.com"),
        ("b", "postgresql", "b.example.com"),
    )
    cfg.set_active_db_profiles(["b", "a", "b", "  a  "])
    assert cfg.active_db_profiles == ["b", "a"]


def test_set_active_db_profiles_rejects_unknown():
    cfg = _build_cfg(("a", "postgresql", "a.example.com"))
    raised = False
    try:
        cfg.set_active_db_profiles(["a", "missing"])
    except KeyError:
        raised = True
    assert raised


def test_set_active_db_profiles_rejects_empty():
    cfg = _build_cfg(("a", "postgresql", "a.example.com"))
    raised = False
    try:
        cfg.set_active_db_profiles([])
    except ValueError:
        raised = True
    assert raised


def test_remove_db_profile_evicts_from_scope():
    cfg = _build_cfg(
        ("a", "postgresql", "a.example.com"),
        ("b", "postgresql", "b.example.com"),
        ("c", "postgresql", "c.example.com"),
    )
    cfg.set_active_db_profiles(["b", "c"])
    cfg.remove_db_profile("b")
    assert "b" not in cfg.active_db_profiles
    assert cfg.active_db_profiles == ["c"]


def test_effective_db_profiles_falls_back_to_scalar():
    """Legacy YAML has no active_db_profiles list — fall back to scalar."""
    cfg = _build_cfg(("only", "postgresql", "x.example.com"))
    cfg.active_db_profiles = []
    assert cfg.effective_db_profiles() == ["only"]


def test_effective_db_profiles_drops_unknown_entries():
    cfg = _build_cfg(("a", "postgresql", "a.example.com"))
    # Stale list (e.g. user removed 'b' externally then loaded the YAML).
    cfg.active_db_profiles = ["a", "ghost"]
    assert cfg.effective_db_profiles() == ["a"]


# ── ProfileScope ──────────────────────────────────────────────────────────


def test_profile_scope_from_config_single():
    cfg = _build_cfg(("only", "postgresql", "x.example.com"))
    scope = ProfileScope.from_config(cfg)
    assert scope.profiles == ("only",)
    assert scope.default == "only"
    assert scope.is_single is True
    assert scope.is_multi is False


def test_profile_scope_from_config_multi_preserves_order_and_default():
    cfg = _build_cfg(
        ("alpha", "postgresql", "a.example.com"),
        ("beta", "postgresql", "b.example.com"),
        ("gamma", "snowflake", "xy.us-east-1"),
    )
    cfg.set_active_db_profiles(["gamma", "alpha"])
    scope = ProfileScope.from_config(cfg)
    assert scope.profiles == ("gamma", "alpha")
    # default == first entry == legacy active scalar
    assert scope.default == "gamma"
    assert scope.is_multi is True


def test_profile_scope_from_names_dedupes_and_defaults():
    s = ProfileScope.from_names(["a", "b", "a", " ", "b"])
    assert s.profiles == ("a", "b")
    assert s.default == "a"


def test_profile_scope_from_names_with_explicit_default():
    s = ProfileScope.from_names(["a", "b", "c"], default="b")
    assert s.profiles == ("a", "b", "c")
    assert s.default == "b"


def test_profile_scope_from_names_default_falls_back_when_invalid():
    s = ProfileScope.from_names(["a", "b"], default="not-in-list")
    assert s.default == "a"


def test_profile_scope_empty():
    s = ProfileScope.empty()
    assert s.is_empty is True
    assert s.profiles == ()
    assert len(s) == 0


def test_profile_scope_iterates_in_order():
    s = ProfileScope.from_names(["x", "y", "z"])
    assert list(s) == ["x", "y", "z"]
    assert "y" in s
    assert "missing" not in s


def test_profile_scope_with_default_swaps_default_only():
    s = ProfileScope.from_names(["a", "b", "c"])
    s2 = s.with_default("c")
    assert s2.profiles == ("a", "b", "c")
    assert s2.default == "c"
    raised = False
    try:
        s.with_default("missing")
    except ValueError:
        raised = True
    assert raised


def test_profile_scope_configs_skips_missing():
    cfg = _build_cfg(("a", "postgresql", "a.example.com"))
    scope = ProfileScope.from_names(["a", "ghost"])
    cfgs = scope.configs(cfg)
    assert [n for n, _ in cfgs] == ["a"]


def test_profile_scope_str_human_readable():
    single = ProfileScope.from_names(["only"])
    multi = ProfileScope.from_names(["a", "b"])
    empty = ProfileScope.empty()
    assert str(single) == "only"
    assert str(multi) == "a, b (default=a)"
    assert "empty" in str(empty)
