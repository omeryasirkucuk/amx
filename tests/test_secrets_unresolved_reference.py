"""Unresolved ``keyring:`` references must survive load → save round-trips.

Reported regression: after a Python reinstall (``pip install -e .``) the
keyring backend can briefly become unreachable on any platform —
macOS Keychain ACL miss, gnome-keyring / KWallet not running on
Linux, Credential Manager access denied on Windows. Without the fix
in this PR, that transient failure was *permanent*: the load step
overwrote the YAML's ``keyring:...`` reference with ``""`` in memory,
and the next ``cfg.save()`` then wrote the empty string to disk and
deleted the pointer for good. Users had to re-enter every secret.

These tests pin the new contract: the YAML reference is preserved
even when the backend is offline, so the very next process with a
healthy backend can resolve it.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from amx.config import AMXConfig
from amx.storage.secrets import (
    InMemorySecretStore,
    NullSecretStore,
    SecretStore,
    set_default_store,
)


@pytest.fixture
def healthy_store() -> InMemorySecretStore:
    """Backend that has the secret (the happy path)."""
    store = InMemorySecretStore()
    store.set("llm_profiles/default/api_key", "sk-real-key")
    set_default_store(store)
    yield store
    set_default_store(None)


@pytest.fixture
def offline_store() -> SecretStore:
    """Backend that's reachable but doesn't know the secret — the
    macOS-Keychain-after-binary-rebuild / gnome-keyring-not-running
    shape."""
    store = NullSecretStore()
    set_default_store(store)
    yield store
    set_default_store(None)


def _seed_yaml_with_reference(path: Path) -> None:
    path.write_text(
        """\
db_profiles:
  prod:
    backend: postgresql
    host: db.example.com
    database: prod
llm_profiles:
  default:
    provider: openai
    model: gpt-4o
    api_key: keyring:llm_profiles/default/api_key
active_db_profile: prod
active_llm_profile: default
""",
        encoding="utf-8",
    )


def test_offline_keyring_does_not_overwrite_reference_on_save(tmp_path, offline_store) -> None:
    """When the keyring is unavailable on load, the YAML must keep the
    original ``keyring:...`` pointer through every subsequent save —
    that's the only way a recovered backend can resolve the secret
    on the next run."""
    yaml_path = tmp_path / "config.yml"
    _seed_yaml_with_reference(yaml_path)

    cfg = AMXConfig.load(str(yaml_path))
    # In-memory the reference falls through unresolved (no plaintext).
    assert cfg.llm.api_key.startswith("keyring:")

    # Trigger every save path: explicit save() and an autosave via
    # mutating a tracked field. Both used to wipe the reference.
    cfg.save()
    cfg.active_llm_profile = "default"

    reloaded = yaml_path.read_text(encoding="utf-8")
    assert "keyring:llm_profiles/default/api_key" in reloaded, (
        "save() destroyed the keyring reference; the user would lose the "
        "credential permanently after a transient backend outage."
    )


def test_healthy_keyring_resolves_to_plaintext(tmp_path, healthy_store) -> None:
    """The happy path still works — load resolves the reference, save
    re-externalises it back to a reference."""
    yaml_path = tmp_path / "config.yml"
    _seed_yaml_with_reference(yaml_path)

    cfg = AMXConfig.load(str(yaml_path))
    assert cfg.llm.api_key == "sk-real-key"
    cfg.save()

    reloaded = yaml_path.read_text(encoding="utf-8")
    assert "keyring:llm_profiles/default/api_key" in reloaded
    assert "sk-real-key" not in reloaded


def test_provider_does_not_send_reference_as_bearer(monkeypatch) -> None:
    """``LLMProvider`` must never let a ``keyring:`` reference reach
    upstream. When the cfg arrives unresolved, the provider falls back
    to the ``AMX_LLM_API_KEY`` env var (or empty) for outgoing calls
    while leaving the cfg dataclass — and therefore the YAML — intact.
    """
    from amx.config import LLMConfig
    from amx.llm.provider import LLMProvider

    monkeypatch.setenv("AMX_LLM_API_KEY", "sk-env-fallback")
    cfg = LLMConfig(
        provider="openai",
        model="gpt-4o",
        api_key="keyring:llm_profiles/default/api_key",
    )
    provider = LLMProvider(cfg)

    # The dataclass must be untouched so save() preserves the YAML
    # pointer.
    assert cfg.api_key == "keyring:llm_profiles/default/api_key"
    # The provider's effective key is the env fallback.
    assert provider._effective_api_key == "sk-env-fallback"


def test_db_connector_does_not_send_reference_password(monkeypatch) -> None:
    """``DatabaseConnector`` must replace unresolved references with
    empty strings on a copy so an adapter never tries to use
    ``keyring:db_profiles/<x>/password`` as a real secret."""
    from dataclasses import dataclass

    from amx.config import DBConfig
    from amx.db.connector import DatabaseConnector

    @dataclass
    class _StubAdapter:
        name: str = "stub"
        capabilities: object = None

    monkeypatch.setattr(
        "amx.db.adapters.get_adapter",
        lambda cfg: _StubAdapter(),
    )
    monkeypatch.setattr(
        "amx.db.drivers.ensure_backend_driver",
        lambda backend: None,
    )

    original = DBConfig(
        backend="postgresql",
        host="x",
        database="d",
        password="keyring:db_profiles/prod/password",
    )
    db = DatabaseConnector(original)

    # The connector holds a copy with the reference scrubbed.
    assert db.cfg.password == ""
    # The original cfg dataclass — the one stored on the
    # AMXConfig.db_profiles map — is untouched, so a save() round-trip
    # still preserves the YAML pointer.
    assert original.password == "keyring:db_profiles/prod/password"
