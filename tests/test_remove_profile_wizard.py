"""Wizard-first behaviour for the ``/remove-*-profile`` commands.

A bare invocation (no name) must open an interactive picker of the existing
profiles and confirm before deleting, instead of erroring with a usage
string. A name passed explicitly is the power-user shortcut and is validated
against the configured profiles. The picker/confirm primitives live in
``amx.utils.console`` (where ``resolve_removal_target`` calls them), so the
tests patch them there.
"""

from __future__ import annotations

from unittest.mock import patch

from amx.cli_support.commands.db import cmd_remove_profile
from amx.cli_support.commands.profiles import (
    cmd_remove_code_profile,
    cmd_remove_doc_profile,
    cmd_remove_llm_profile,
)
from amx.config import AMXConfig, DBConfig, LLMConfig

PICK = "amx.utils.console.ask_choice"
CONFIRM = "amx.utils.console.confirm"


def _cfg_with_llm_profiles() -> AMXConfig:
    cfg = AMXConfig()
    cfg.upsert_llm_profile("work", LLMConfig(provider="openai", model="gpt-5.4-mini"))
    cfg.upsert_llm_profile("home", LLMConfig(provider="anthropic", model="claude-haiku-4.5"))
    return cfg


# ── LLM profile ──────────────────────────────────────────────────────────────


def test_remove_llm_bare_opens_picker_and_removes_choice() -> None:
    cfg = _cfg_with_llm_profiles()
    with (
        patch(PICK, return_value="home") as pick,
        patch(CONFIRM, return_value=True) as conf,
    ):
        cmd_remove_llm_profile(cfg, [])
    pick.assert_called_once()
    conf.assert_called_once()
    assert "home" not in cfg.llm_profiles
    assert "work" in cfg.llm_profiles


def test_remove_llm_bare_declined_confirm_keeps_profile() -> None:
    cfg = _cfg_with_llm_profiles()
    with patch(PICK, return_value="home"), patch(CONFIRM, return_value=False):
        cmd_remove_llm_profile(cfg, [])
    assert "home" in cfg.llm_profiles  # declined the confirm, so nothing removed


def test_remove_llm_bare_no_selection_is_noop() -> None:
    cfg = _cfg_with_llm_profiles()
    with patch(PICK, return_value=""), patch(CONFIRM) as conf:
        cmd_remove_llm_profile(cfg, [])
    conf.assert_not_called()
    assert {"work", "home"} <= set(cfg.llm_profiles)


def test_remove_llm_explicit_name_skips_picker() -> None:
    cfg = _cfg_with_llm_profiles()
    with patch(PICK) as pick, patch(CONFIRM) as conf:
        cmd_remove_llm_profile(cfg, ["work"])
    pick.assert_not_called()
    conf.assert_not_called()
    assert "work" not in cfg.llm_profiles
    assert "home" in cfg.llm_profiles


def test_remove_llm_unknown_name_errors_and_keeps_all() -> None:
    cfg = _cfg_with_llm_profiles()
    with patch(PICK) as pick:
        cmd_remove_llm_profile(cfg, ["does-not-exist"])
    pick.assert_not_called()
    assert {"work", "home"} <= set(cfg.llm_profiles)


def test_remove_llm_with_no_profiles_is_noop() -> None:
    cfg = AMXConfig()
    with patch(PICK) as pick:
        cmd_remove_llm_profile(cfg, [])
    pick.assert_not_called()  # nothing to pick from


# ── document profile ───────────────────────────────────────────────────────


def test_remove_doc_bare_opens_picker_and_removes_choice() -> None:
    cfg = AMXConfig()
    cfg.doc_profiles["api"] = []
    cfg.doc_profiles["guides"] = []
    with patch(PICK, return_value="api"), patch(CONFIRM, return_value=True):
        cmd_remove_doc_profile(cfg, [])
    assert "api" not in cfg.doc_profiles
    assert "guides" in cfg.doc_profiles


# ── codebase profile ─────────────────────────────────────────────────────────


def test_remove_code_bare_opens_picker_and_removes_choice() -> None:
    cfg = AMXConfig()
    cfg.upsert_code_profile("repo", ".")
    cfg.upsert_code_profile("lib", ".")
    with patch(PICK, return_value="lib"), patch(CONFIRM, return_value=True):
        cmd_remove_code_profile(cfg, [])
    assert "lib" not in cfg.code_profiles
    assert "repo" in cfg.code_profiles


# ── DB profile (picker resolves the name; the destructive confirm lives in the
#    shared-run-history guard downstream, so the picker does not double-confirm)


def test_remove_db_bare_opens_picker_and_removes_choice() -> None:
    cfg = AMXConfig()
    cfg.upsert_db_profile("prod", DBConfig(backend="duckdb", database=":memory:"))
    cfg.upsert_db_profile("stage", DBConfig(backend="duckdb", database=":memory:"))
    with patch(PICK, return_value="stage") as pick, patch(CONFIRM) as conf:
        cmd_remove_profile(cfg, [])
    pick.assert_called_once()
    conf.assert_not_called()  # confirm_removal=False for the DB picker path
    assert "stage" not in cfg.db_profiles
    assert "prod" in cfg.db_profiles


def test_remove_db_unknown_name_errors_and_keeps_all() -> None:
    cfg = AMXConfig()
    cfg.upsert_db_profile("prod", DBConfig(backend="duckdb", database=":memory:"))
    cfg.upsert_db_profile("stage", DBConfig(backend="duckdb", database=":memory:"))
    with patch(PICK) as pick:
        cmd_remove_profile(cfg, ["ghost"])
    pick.assert_not_called()
    assert {"prod", "stage"} <= set(cfg.db_profiles)
