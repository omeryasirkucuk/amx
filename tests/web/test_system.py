"""System / context endpoints — what the SPA hits at boot."""

from __future__ import annotations

from amx import __version__ as AMX_VERSION


def test_health_returns_amx_version(client, auth_headers) -> None:
    response = client.get("/api/health", headers=auth_headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload == {"ok": True, "version": AMX_VERSION}


def test_version_reports_components(client, auth_headers) -> None:
    response = client.get("/api/version", headers=auth_headers)
    assert response.status_code == 200
    payload = response.json()
    # Pydantic with populate_by_name surfaces ``schema`` (the alias)
    # rather than the field name. The SPA reads the alias.
    assert payload["amx"] == AMX_VERSION
    assert isinstance(payload["schema"], int)
    assert payload["schema"] >= 1
    assert payload["web"] == "v1"


def test_context_reads_active_profile_state(client, auth_headers, cfg) -> None:
    """The /api/context handler must read straight from cfg —
    mutating cfg in-place after building the app should reflect on
    the next request."""
    cfg.active_db_profile = "prod"
    cfg.active_llm_profile = "claude"
    cfg.current_schema = "sales"
    cfg.current_table = "orders"

    response = client.get("/api/context", headers=auth_headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["active_db_profile"] == "prod"
    assert payload["active_llm_profile"] == "claude"
    assert payload["current_schema"] == "sales"
    assert payload["current_table"] == "orders"


def test_context_handles_blank_profile_state(client, auth_headers, cfg) -> None:
    """A fresh AMXConfig may have empty / sentinel profile fields.
    The endpoint must coerce those to JSON ``null`` rather than the
    literal string ``""``."""
    cfg.active_db_profile = ""
    cfg.active_llm_profile = ""
    cfg.current_schema = ""
    cfg.current_table = ""

    response = client.get("/api/context", headers=auth_headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["active_db_profile"] is None
    assert payload["active_llm_profile"] is None
    assert payload["current_schema"] is None
    assert payload["current_table"] is None
