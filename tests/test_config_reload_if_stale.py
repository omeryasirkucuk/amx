"""Studio writes to ``~/.amx/config.yml`` must propagate into a
running CLI session without restart.

The fix lives in :meth:`AMXConfig.reload_if_stale`, called once per
prompt input from :func:`amx.cli_support.session.run_interactive_session`.
These tests cover the contract:

- a file unchanged on disk is a no-op (single stat, no reload)
- a file rewritten by another process is picked up
- a save() from this same instance does NOT cause a self-reload loop
- mutations apply in-place so existing references stay valid
"""

from __future__ import annotations

import os
import time
from pathlib import Path


def _write_yaml(path: Path, body: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body)


def test_reload_if_stale_returns_false_when_disk_unchanged(tmp_path, monkeypatch):
    monkeypatch.setenv("AMX_CONFIG_DIR", str(tmp_path))
    from amx.config import AMXConfig

    cfg_path = tmp_path / "config.yml"
    _write_yaml(cfg_path, "active_doc_profile: alpha\n")
    cfg = AMXConfig.load(str(cfg_path))
    assert cfg.active_doc_profile == "alpha"
    # No external write — reload should be a no-op
    assert cfg.reload_if_stale() is False


def test_reload_if_stale_picks_up_external_write(tmp_path, monkeypatch):
    """Simulates Studio writing while the CLI sits at its prompt."""
    monkeypatch.setenv("AMX_CONFIG_DIR", str(tmp_path))
    from amx.config import AMXConfig

    cfg_path = tmp_path / "config.yml"
    _write_yaml(cfg_path, "active_doc_profile: alpha\n")
    cfg = AMXConfig.load(str(cfg_path))
    assert cfg.active_doc_profile == "alpha"

    # Ensure the next write registers as newer; some filesystems quantise
    # mtime at 1s. Bump explicitly so the test is deterministic.
    new_mtime = time.time() + 5
    _write_yaml(cfg_path, "active_doc_profile: beta\n")
    os.utime(cfg_path, (new_mtime, new_mtime))

    assert cfg.reload_if_stale() is True
    assert cfg.active_doc_profile == "beta"


def test_save_does_not_trigger_self_reload(tmp_path, monkeypatch):
    """If save() didn't update _loaded_mtime, the very next call to
    reload_if_stale would see its own write as 'newer than load' and
    clobber any unsaved in-memory mutations made between save() and
    the next reload_if_stale call."""
    monkeypatch.setenv("AMX_CONFIG_DIR", str(tmp_path))
    from amx.config import AMXConfig

    cfg_path = tmp_path / "config.yml"
    _write_yaml(cfg_path, "active_doc_profile: alpha\n")
    cfg = AMXConfig.load(str(cfg_path))
    cfg.active_doc_profile = "beta"
    cfg.save(str(cfg_path))
    assert cfg.reload_if_stale() is False
    assert cfg.active_doc_profile == "beta"


def test_reload_keeps_same_instance_for_callers_holding_a_reference(tmp_path):
    """Other modules (history_store, embedding provider) keep a single
    reference to the AMXConfig instance. reload_if_stale mutates in
    place so those references stay valid."""
    from amx.config import AMXConfig

    cfg_path = tmp_path / "config.yml"
    _write_yaml(cfg_path, "active_doc_profile: alpha\n")
    cfg = AMXConfig.load(str(cfg_path))
    ref = cfg  # caller's hold

    new_mtime = time.time() + 5
    _write_yaml(cfg_path, "active_doc_profile: gamma\n")
    os.utime(cfg_path, (new_mtime, new_mtime))
    cfg.reload_if_stale()

    assert ref is cfg
    assert ref.active_doc_profile == "gamma"


def test_reload_handles_missing_path_gracefully(tmp_path):
    from amx.config import AMXConfig

    cfg = AMXConfig.load(str(tmp_path / "does_not_exist.yml"))
    assert cfg.reload_if_stale() is False


def test_reload_propagates_doc_profiles_dict_changes(tmp_path):
    """Critical for the original bug: a doc profile added in Studio
    must surface in the CLI's cfg.doc_profiles map after reload."""
    from amx.config import AMXConfig

    cfg_path = tmp_path / "config.yml"
    _write_yaml(cfg_path, "doc_profiles: {}\n")
    cfg = AMXConfig.load(str(cfg_path))
    assert cfg.doc_profiles == {}

    new_mtime = time.time() + 5
    _write_yaml(
        cfg_path,
        "doc_profiles:\n  test:\n    - /Users/example/.amx/uploads/test\n",
    )
    os.utime(cfg_path, (new_mtime, new_mtime))
    assert cfg.reload_if_stale() is True
    assert "test" in cfg.doc_profiles
    assert cfg.doc_profiles["test"] == ["/Users/example/.amx/uploads/test"]


def test_reload_propagates_llm_profile_field_changes(tmp_path):
    from amx.config import AMXConfig

    cfg_path = tmp_path / "config.yml"
    _write_yaml(
        cfg_path,
        "llm_profiles:\n  default:\n    provider: openai\n    model: gpt-4\nactive_llm_profile: default\n",
    )
    cfg = AMXConfig.load(str(cfg_path))
    assert cfg.llm.model == "gpt-4"

    new_mtime = time.time() + 5
    _write_yaml(
        cfg_path,
        "llm_profiles:\n  default:\n    provider: openai\n    model: gpt-4o\nactive_llm_profile: default\n",
    )
    os.utime(cfg_path, (new_mtime, new_mtime))
    cfg.reload_if_stale()
    assert cfg.llm_profiles["default"].model == "gpt-4o"
