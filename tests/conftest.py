"""Pytest fixtures shared across the AMX test suite.

The most important responsibility of this file is keeping unit tests off
the user's real OS keyring. ``amx.config.save()`` writes secrets to the
keyring (macOS Keychain, etc.) by default, so without this fixture every
test run would leak placeholder credentials into the developer's keyring.
"""

from __future__ import annotations

import pytest

from amx.storage.secrets import InMemorySecretStore, set_default_store


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
