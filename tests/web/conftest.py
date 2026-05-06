"""Shared fixtures for AMX Studio FastAPI tests.

Every test gets:

* a fresh :class:`AMXConfig` (no on-disk state, no profile leakage),
* a :class:`fastapi.testclient.TestClient` wired to a deterministic
  bearer token,
* helper builders for authenticated / unauthenticated requests.
"""

from __future__ import annotations

import pytest

from amx.config import AMXConfig
from amx.web.server import create_app

_TEST_TOKEN = "test-studio-token-abc123"


@pytest.fixture()
def cfg() -> AMXConfig:
    cfg = AMXConfig()
    # /api/ask now gates submission on a configured LLM provider/model
    # (PR fix/ask-llm-error-handling) so a fresh AMXConfig() returns 412
    # instead of spawning the worker. Seed sane defaults for the tests
    # that only care about session / scope plumbing — the LLM is
    # mocked at the worker boundary in tests that exercise it.
    cfg.llm.provider = "openai"
    cfg.llm.model = "gpt-4"
    return cfg


@pytest.fixture()
def token() -> str:
    return _TEST_TOKEN


@pytest.fixture()
def app(cfg: AMXConfig, token: str):
    return create_app(cfg, token=token)


@pytest.fixture()
def client(app):
    from fastapi.testclient import TestClient

    return TestClient(app)


@pytest.fixture()
def auth_headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}
