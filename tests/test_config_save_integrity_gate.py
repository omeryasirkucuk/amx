"""Pre-save integrity gate: refuse to overwrite the on-disk config
when in-memory state has the autosave-race truncation signature.

After PR #360 (race fix) and PR #361 (rotating backup + restore), the
remaining recovery hole is: a future regression that re-introduces an
inconsistent state would have its bad payload copied through every
rotated backup over subsequent saves, gradually overwriting the
clean history. The gate stops that propagation at the source.
"""

from __future__ import annotations

import logging


def _make_cfg():
    from amx.config import AMXConfig

    return AMXConfig()


def test_save_refused_when_active_doc_profile_missing_from_dict(tmp_path, caplog):
    """``doc_profiles`` has no in-save auto-repair (unlike db/llm where
    ``save()`` re-inserts ``self.db`` / ``self.llm`` on the fly), so a
    state with ``active_doc_profile="ghost"`` and ``doc_profiles={}`` is
    the PR #351 truncation signature with nothing to fall back on. The
    gate must refuse rather than write a YAML that ratchets the loss
    into every rotated backup."""
    cfg = _make_cfg()
    cfg.active_doc_profile = "ghost-docs"
    cfg.doc_profiles.clear()

    cfg_path = tmp_path / "config.yml"
    pristine = (
        "active_doc_profile: ghost-docs\n"
        "doc_profiles:\n  ghost-docs:\n    - /tmp/something\n"
    )
    cfg_path.write_text(pristine)

    with caplog.at_level(logging.ERROR, logger="amx.config"):
        result = cfg.save(str(cfg_path))

    assert result == cfg_path
    assert cfg_path.read_text() == pristine, (
        "live config was overwritten — gate didn't fire and broken state is now on disk"
    )
    assert any(
        "refused" in r.message.lower() or "integrity" in r.message.lower()
        for r in caplog.records
    ), "expected a loud error log explaining the refusal"


def test_save_refused_when_active_code_profile_missing_from_dict(tmp_path):
    cfg = _make_cfg()
    cfg.active_code_profile = "ghost-code"
    cfg.code_profiles.clear()

    cfg_path = tmp_path / "config.yml"
    pristine = "active_code_profile: ghost-code\ncode_profiles:\n  ghost-code: {}\n"
    cfg_path.write_text(pristine)
    cfg.save(str(cfg_path))
    assert cfg_path.read_text() == pristine


def test_save_repairs_db_and_llm_inconsistency_then_writes(tmp_path):
    """For ``active_db_profile`` / ``active_llm_profile`` the save()
    contract has historically auto-inserted the in-memory dataclass
    into the dict before serialization. The gate must not break that
    repair path — it should only fire when no fallback exists."""
    cfg = _make_cfg()
    cfg.active_db_profile = "pg-prod"
    cfg.db_profiles.clear()  # would trip the raw integrity check

    cfg_path = tmp_path / "config.yml"
    cfg.save(str(cfg_path))

    assert cfg_path.exists()
    reloaded_text = cfg_path.read_text()
    assert "pg-prod" in reloaded_text
    assert "active_db_profile: pg-prod" in reloaded_text


def test_save_succeeds_when_active_matches_dict(tmp_path):
    """Counter-test: legitimate save with consistent state must still write."""
    from amx.config import LLMConfig

    cfg = _make_cfg()
    cfg.active_llm_profile = "real"
    cfg.llm_profiles["real"] = LLMConfig(provider="openai", model="gpt-4")

    cfg_path = tmp_path / "config.yml"
    cfg.save(str(cfg_path))

    body = cfg_path.read_text()
    assert "real" in body
    assert "active_llm_profile: real" in body


def test_save_succeeds_for_fresh_install_default_placeholder(tmp_path):
    """The loader injects ``active_db_profile: "default"`` on fresh
    installs with no profile dict. That state is legitimate (the user
    hasn't picked anything yet) and must not trigger the gate."""
    cfg = _make_cfg()
    cfg.active_db_profile = "default"
    cfg.db_profiles.clear()

    cfg_path = tmp_path / "config.yml"
    cfg.save(str(cfg_path))
    assert cfg_path.exists()
    assert "active_db_profile: default" in cfg_path.read_text()


def test_detect_silent_truncation_lists_all_broken_pairs():
    from amx.config import _detect_silent_truncation

    cfg = _make_cfg()
    cfg.active_db_profile = "x"
    cfg.active_llm_profile = "y"
    cfg.active_doc_profile = "z"
    cfg.active_code_profile = "w"
    cfg.db_profiles.clear()
    cfg.llm_profiles.clear()
    cfg.doc_profiles.clear()
    cfg.code_profiles.clear()

    problems = _detect_silent_truncation(cfg)
    assert len(problems) == 4
    joined = " ".join(problems)
    for ghost in ("x", "y", "z", "w"):
        assert f"'{ghost}'" in joined
