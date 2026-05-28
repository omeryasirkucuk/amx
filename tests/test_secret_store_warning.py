"""Secrets aren't silently downgraded to plaintext.

(1) The MotherDuck token is now in the DB secret fields, so it goes to
the keyring like every other credential instead of plaintext config.yml.
(2) When no keyring backend is reachable, AMX warns once instead of
silently storing plaintext (security.md promises the keyring).
"""

from __future__ import annotations

import pytest

import amx.config as config_mod
import amx.storage.secrets as secrets
import amx.utils.console as console


def test_motherduck_token_is_a_secret_field() -> None:
    assert "motherduck_token" in config_mod._DB_SECRET_FIELDS


def test_keyring_unavailable_warns_once(monkeypatch: pytest.MonkeyPatch) -> None:
    log_calls: list[str] = []
    monkeypatch.setattr(secrets.log, "warning", lambda m: log_calls.append(m))
    monkeypatch.setattr(console, "warn", lambda _m: None)
    monkeypatch.setattr(secrets, "_keyring_warning_emitted", False)

    secrets._warn_keyring_unavailable()
    secrets._warn_keyring_unavailable()  # second call is a no-op

    assert len(log_calls) == 1
    assert "PLAINTEXT" in log_calls[0]


def test_get_default_store_falls_back_with_warning(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(secrets.KeyringSecretStore, "is_available", lambda self: False)
    warned: list[bool] = []
    monkeypatch.setattr(secrets, "_warn_keyring_unavailable", lambda: warned.append(True))
    secrets.set_default_store(None)
    try:
        store = secrets.get_default_store()
        assert isinstance(store, secrets.NullSecretStore)
        assert warned == [True]
    finally:
        secrets.set_default_store(None)
