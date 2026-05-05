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
    return AMXConfig()


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
