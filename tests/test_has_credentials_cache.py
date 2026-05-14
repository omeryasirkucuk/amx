"""Lazy + cached ``has_credentials`` resolution on ``GET /api/profiles/llm``.

The profiles router must NOT call the keyring on every read — macOS
Keychain will prompt the user on the first lookup, and Linux
secret-service availability is fragile. The cache is invalidated on
profile-credential mutations (PUT / DELETE) so the next read
re-resolves against the live store.
"""

from amx.config import LLMConfig
from amx.storage import secrets
from amx.web.routers import profiles


class _CountingStore:
    """Minimal :class:`SecretStore` shim that counts ``get`` calls."""

    def __init__(self, resolved: dict[str, str] | None = None) -> None:
        self._resolved = resolved or {}
        self.get_calls = 0

    def is_available(self) -> bool:
        return True

    def get(self, key: str) -> str | None:
        self.get_calls += 1
        return self._resolved.get(key)

    def set(self, key: str, value: str) -> None: ...
    def delete(self, key: str) -> None: ...


class _BrokenStore:
    def is_available(self) -> bool:
        return True

    def get(self, key: str) -> str | None:
        raise RuntimeError("keyring backend is locked")

    def set(self, key: str, value: str) -> None: ...
    def delete(self, key: str) -> None: ...


def _reset() -> None:
    profiles._CREDENTIAL_CACHE.clear()


class TestHasCredentialsCache:
    def setup_method(self) -> None:
        _reset()

    def teardown_method(self) -> None:
        _reset()
        secrets.set_default_store(None)

    def test_literal_key_fast_path_no_keyring_call(self) -> None:
        store = _CountingStore()
        secrets.set_default_store(store)
        llm = LLMConfig(provider="openai", model="gpt-5", api_key="sk-literal")
        assert profiles._check_credentials_cached("p_lit", llm) is True
        # Multiple reads still no keyring access.
        for _ in range(3):
            profiles._check_credentials_cached("p_lit", llm)
        assert store.get_calls == 0

    def test_keyring_ref_resolves_once_then_cached(self) -> None:
        store = _CountingStore(resolved={"amx/p1/api_key": "real-key"})
        secrets.set_default_store(store)
        llm = LLMConfig(
            provider="openai", model="gpt-5", api_key="keyring:amx/p1/api_key"
        )
        for _ in range(5):
            assert profiles._check_credentials_cached("p1", llm) is True
        assert store.get_calls == 1, (
            f"expected one keyring call across five reads, got {store.get_calls}"
        )

    def test_invalidation_drops_cache_and_re_resolves(self) -> None:
        store = _CountingStore(resolved={"amx/p1/api_key": "real-key"})
        secrets.set_default_store(store)
        llm = LLMConfig(
            provider="openai", model="gpt-5", api_key="keyring:amx/p1/api_key"
        )
        assert profiles._check_credentials_cached("p1", llm) is True
        assert store.get_calls == 1
        profiles._invalidate_credential_cache("p1")
        assert profiles._check_credentials_cached("p1", llm) is True
        assert store.get_calls == 2

    def test_resolver_failure_returns_false_no_propagation(self, caplog) -> None:
        secrets.set_default_store(_BrokenStore())
        llm = LLMConfig(
            provider="openai", model="gpt-5", api_key="keyring:amx/p1/api_key"
        )
        # Must not raise.
        assert profiles._check_credentials_cached("p1", llm) is False
        # And the failure is cached so subsequent reads don't keep
        # retrying a broken keyring.
        secrets.set_default_store(_CountingStore(resolved={"amx/p1/api_key": "now-up"}))
        assert profiles._check_credentials_cached("p1", llm) is False

    def test_empty_api_key_returns_false(self) -> None:
        llm = LLMConfig(provider="openai", model="gpt-5", api_key="")
        assert profiles._check_credentials_cached("p_empty", llm) is False
