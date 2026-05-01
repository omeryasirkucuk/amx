"""Pytest fixtures shared across the AMX test suite.

The fixtures here keep tests away from real on-disk state on the
developer's machine. Two leaks they prevent:

1. **Keyring** — ``amx.config.save()`` writes secrets to the OS keyring
   (macOS Keychain, etc.) by default. Without isolation, every test
   would leak placeholder credentials into the developer's keyring.
2. **`~/.amx/`** — many tests construct ``AMXConfig()`` with no path
   override and then trigger a code path that calls ``cfg.save()``.
   Without isolation, those tests overwrite the developer's actual
   ``~/.amx/config.yml`` with synthetic test fixtures (the user-reported
   ghost-profile bug on 2026-05-02 was traced to exactly this — a test
   ran ``cmd_add_profile(cfg, ["databricks-default"])`` against an
   ``AMXConfig()`` whose ``CONFIG_DIR`` defaulted to ``~/.amx``,
   replacing the real config with synthetic test data).
"""

from __future__ import annotations

from collections.abc import Generator
from pathlib import Path

import pytest

from amx.storage.secrets import InMemorySecretStore, set_default_store


@pytest.fixture(autouse=True)
def _isolate_amx_home(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Generator[Path, None, None]:
    """Redirect ``Path.home()`` to a per-test temp dir.

    Applies to every test (unittest.TestCase included) so any code path
    that resolves ``~/.amx/...`` writes into the temp dir instead of the
    developer's real home. The directory is unique per test and is
    cleaned up automatically by pytest's ``tmp_path`` fixture.

    Pin point: ``AMXConfig.CONFIG_DIR`` is computed via
    ``str(Path.home() / ".amx")`` at instance creation time, so
    patching ``Path.home`` BEFORE the cfg is constructed is what makes
    every ``cfg.save()`` land in the temp dir. Tests that pass an
    explicit ``path=`` to ``cfg.save(...)`` are unaffected.
    """
    fake_home = tmp_path / "home"
    fake_home.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr("pathlib.Path.home", lambda: fake_home)
    yield fake_home


@pytest.fixture(autouse=True)
def _isolate_secret_store() -> None:
    """Swap in an in-memory secret store for every test.

    ``autouse=True`` applies this without each test having to opt in.
    The store is reset between tests so a profile written by one test
    does not leak credentials into another.
    """
    set_default_store(InMemorySecretStore())
    yield
    set_default_store(None)
