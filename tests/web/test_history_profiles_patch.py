"""PATCH /api/history/profiles + GET /api/history/status surface tests."""

from __future__ import annotations

import pytest


@pytest.fixture()
def isolated_config(tmp_path, monkeypatch):
    """Point AMXConfig.load() at a throwaway YAML so the endpoint test
    does not mutate the developer's real config.yml."""
    cfg_path = tmp_path / "config.yml"
    cfg_path.write_text(
        "history_store_enabled: true\n"
        "history_store_profile: prod\n"
        "history_store_schema: AMX\n"
        "history_store_database: ''\n",
        encoding="utf-8",
    )

    from amx import config as cfg_module

    original_load = cfg_module.AMXConfig.load

    @classmethod
    def _scoped_load(cls, path=None):
        return original_load(str(cfg_path))

    monkeypatch.setattr(cfg_module.AMXConfig, "load", _scoped_load)
    yield cfg_path


def test_status_reports_singular_when_extras_empty(client, auth_headers, isolated_config) -> None:
    response = client.get("/api/history/status", headers=auth_headers)
    assert response.status_code in (200, 503)
    if response.status_code == 503:
        pytest.skip("history store not initialised in this test harness")
    payload = response.json()
    assert payload["shared_profile"] == "prod"
    assert payload["shared_profiles"] == ["prod"]


def test_patch_profiles_replaces_extras_and_dedupes_primary(
    client, auth_headers, isolated_config
) -> None:
    response = client.patch(
        "/api/history/profiles",
        json={"profiles": ["prod", "dev", "staging"]},
        headers=auth_headers,
    )
    assert response.status_code == 200
    payload = response.json()
    # Primary "prod" is dropped from the extras list because it lives
    # in the singular field; the union still surfaces it first.
    assert payload["shared_profile"] == "prod"
    assert payload["shared_profiles"] == ["prod", "dev", "staging"]


def test_patch_profiles_empty_list_clears_extras(client, auth_headers, isolated_config) -> None:
    # Seed.
    client.patch(
        "/api/history/profiles",
        json={"profiles": ["dev", "staging"]},
        headers=auth_headers,
    )
    # Now clear.
    response = client.patch(
        "/api/history/profiles",
        json={"profiles": []},
        headers=auth_headers,
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["shared_profiles"] == ["prod"]


def test_patch_profiles_strips_whitespace(client, auth_headers, isolated_config) -> None:
    response = client.patch(
        "/api/history/profiles",
        json={"profiles": ["  dev  ", "", "  ", "staging"]},
        headers=auth_headers,
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["shared_profiles"] == ["prod", "dev", "staging"]
