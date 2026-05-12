"""``AMX_CONFIG_DIR`` env var overrides the default ``~/.amx`` config
directory so a dev install can be tested side-by-side with a
production AMX setup without sharing config.yml, history.db,
uploads, or chroma_db.

The doctor command at ``amx/cli_support/commands/doctor.py:155``
has been telling users about this variable for a while; this PR
ships the actual implementation.
"""

from __future__ import annotations

import os
from pathlib import Path


def test_default_config_dir_is_home_amx(monkeypatch):
    monkeypatch.delenv("AMX_CONFIG_DIR", raising=False)
    from amx.config import _resolve_config_dir

    assert _resolve_config_dir() == str(Path.home() / ".amx")


def test_env_var_overrides_default(monkeypatch, tmp_path):
    monkeypatch.setenv("AMX_CONFIG_DIR", str(tmp_path / "isolated"))
    from amx.config import _resolve_config_dir

    assert _resolve_config_dir() == str(tmp_path / "isolated")


def test_env_var_expands_tilde(monkeypatch):
    monkeypatch.setenv("AMX_CONFIG_DIR", "~/some-dev-dir")
    from amx.config import _resolve_config_dir

    resolved = _resolve_config_dir()
    # Use the same expanduser path the resolver uses so we don't fight
    # pytest's tmp-HOME fixture vs Path.home() resolution mismatches.
    assert resolved == str(Path("~/some-dev-dir").expanduser())
    assert "~" not in resolved


def test_empty_env_var_falls_back_to_default(monkeypatch):
    monkeypatch.setenv("AMX_CONFIG_DIR", "   ")
    from amx.config import _resolve_config_dir

    assert _resolve_config_dir() == str(Path.home() / ".amx")


def test_fresh_amxconfig_picks_up_env_var(monkeypatch, tmp_path):
    """Every newly-loaded :class:`AMXConfig` instance must honour the
    override — not just calls made after import."""
    monkeypatch.setenv("AMX_CONFIG_DIR", str(tmp_path / "dev"))
    # Force a fresh resolution; the dataclass uses a default_factory
    # so we don't need to reload the module.
    from amx.config import AMXConfig

    cfg = AMXConfig()
    assert cfg.CONFIG_DIR == str(tmp_path / "dev")


def test_studio_log_path_uses_override(monkeypatch, tmp_path):
    """Studio log file should land under the overridden dir, not
    under ``~/.amx`` — proves the override propagates through the
    real call sites, not just the resolver."""
    monkeypatch.setenv("AMX_CONFIG_DIR", str(tmp_path / "dev"))
    from amx.config import AMXConfig
    from amx.web.launcher import _studio_log_path

    cfg = AMXConfig()
    log_path = _studio_log_path(cfg, 47821)
    assert log_path == tmp_path / "dev" / "logs" / "studio-47821.log"
    # Resolver creates the dir on demand so a tail-f works immediately.
    assert log_path.parent.is_dir()


def test_subprocess_inherits_env_var(monkeypatch, tmp_path):
    """The Studio child process is spawned without an explicit env
    override; Python's subprocess.Popen inherits os.environ by
    default, so the child sees the same AMX_CONFIG_DIR the parent
    does. Just a smoke check that the resolver and os.environ agree."""
    monkeypatch.setenv("AMX_CONFIG_DIR", str(tmp_path / "dev"))
    assert os.environ["AMX_CONFIG_DIR"] == str(tmp_path / "dev")
    from amx.config import _resolve_config_dir

    assert _resolve_config_dir() == str(tmp_path / "dev")
