"""Secret storage abstraction.

AMX keeps DB passwords, LLM API keys, and Databricks access tokens in
``~/.amx/config.yml``. Storing them in plaintext is not acceptable for a
package distributed via PyPI, so secret-bearing fields are externalised
to the OS keyring (macOS Keychain, Windows Credential Manager, Linux
Secret Service / D-Bus) and the YAML stores only opaque references like::

    keyring:db_profiles/default/password

Loaders resolve references back to their plaintext value transparently,
so the rest of AMX continues to see ``cfg.db.password`` etc. as strings.

When no keyring backend is available (rare — typically a headless Linux
server with no Secret Service running) the loader falls back to plaintext
behaviour and emits a one-time warning so the user can choose how to
proceed.

Tests should swap in :class:`InMemorySecretStore` via
:func:`set_default_store` to avoid touching the real OS keyring.
"""

from __future__ import annotations

from typing import Protocol

from amx.utils.logging import get_logger

log = get_logger("storage.secrets")

KEYRING_PREFIX = "keyring:"
SERVICE_NAME = "amx"


class SecretStore(Protocol):
    """Minimum interface required by :mod:`amx.config`."""

    def is_available(self) -> bool: ...
    def get(self, key: str) -> str | None: ...
    def set(self, key: str, value: str) -> None: ...
    def delete(self, key: str) -> None: ...


class KeyringSecretStore:
    """OS-keyring backed secret store using the ``keyring`` package."""

    def __init__(self, service: str = SERVICE_NAME) -> None:
        self._service = service
        self._kr: object | None = None
        self._available = False
        try:
            import keyring as _keyring

            self._kr = _keyring
            backend = _keyring.get_keyring()
            backend_name = backend.__class__.__name__.lower()
            # `fail.Keyring` is the sentinel returned when no real backend is
            # available; treat it as unavailable.
            self._available = bool(backend) and "fail" not in backend_name
        except Exception:
            self._kr = None
            self._available = False

    def is_available(self) -> bool:
        return self._available

    def get(self, key: str) -> str | None:
        if not self._available or self._kr is None:
            return None
        try:
            return self._kr.get_password(self._service, key)  # type: ignore[attr-defined]
        except Exception:
            return None

    def set(self, key: str, value: str) -> None:
        if not self._available or self._kr is None:
            return
        try:
            self._kr.set_password(self._service, key, value)  # type: ignore[attr-defined]
        except Exception:
            pass

    def delete(self, key: str) -> None:
        if not self._available or self._kr is None:
            return
        try:
            self._kr.delete_password(self._service, key)  # type: ignore[attr-defined]
        except Exception:
            pass


class InMemorySecretStore:
    """In-process store. Used by tests; not persistent across processes."""

    def __init__(self) -> None:
        self._data: dict[str, str] = {}

    def is_available(self) -> bool:
        return True

    def get(self, key: str) -> str | None:
        return self._data.get(key)

    def set(self, key: str, value: str) -> None:
        self._data[key] = value

    def delete(self, key: str) -> None:
        self._data.pop(key, None)


class NullSecretStore:
    """No-op store used when no keyring backend is reachable.

    All ``set`` calls are dropped; ``get`` always returns ``None``. The
    config loader falls back to leaving plaintext values in YAML in this
    case so users on backend-less systems are not silently locked out
    of their credentials.
    """

    def is_available(self) -> bool:
        return False

    def get(self, key: str) -> str | None:
        return None

    def set(self, key: str, value: str) -> None:
        return None

    def delete(self, key: str) -> None:
        return None


# ── Reference helpers ─────────────────────────────────────────────────


def is_secret_reference(value: object) -> bool:
    """True if *value* is a ``keyring:...`` reference string."""
    return isinstance(value, str) and value.startswith(KEYRING_PREFIX)


def make_reference(key: str) -> str:
    return f"{KEYRING_PREFIX}{key}"


def parse_reference(ref: str) -> str:
    if not is_secret_reference(ref):
        raise ValueError(f"Not a secret reference: {ref!r}")
    return ref[len(KEYRING_PREFIX) :]


# ── Default store singleton ───────────────────────────────────────────


_default_store: SecretStore | None = None
_keyring_warning_emitted = False


def _warn_keyring_unavailable() -> None:
    """Warn once when no keyring backend is reachable.

    Without this the fall back to plaintext storage in config.yml was
    silent — security.md and the masked prompts promise the OS keyring,
    so a headless-Linux / CI / Keychain-denied user was downgraded with
    no signal except a /doctor check they had no reason to run.
    """
    global _keyring_warning_emitted
    if _keyring_warning_emitted:
        return
    _keyring_warning_emitted = True
    msg = (
        "No OS keyring backend is available — database/LLM credentials will "
        "be stored as PLAINTEXT in config.yml (and its rotated backups). "
        "On headless Linux / CI install a keyring backend (e.g. SecretService "
        "or keyrings.alt); run /doctor for details."
    )
    log.warning(msg)
    try:
        from amx.utils.console import warn as _console_warn

        _console_warn(msg)
    except Exception:  # pragma: no cover - console is best-effort here
        pass


def get_default_store() -> SecretStore:
    global _default_store
    if _default_store is None:
        store: SecretStore = KeyringSecretStore()
        if not store.is_available():
            _warn_keyring_unavailable()
            store = NullSecretStore()
        _default_store = store
    return _default_store


def set_default_store(store: SecretStore | None) -> None:
    """Override the default store (tests). Pass ``None`` to reset."""
    global _default_store
    _default_store = store
