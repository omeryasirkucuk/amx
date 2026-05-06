"""Profile CRUD tests — DB + LLM + activate + masking."""

from __future__ import annotations


def _set_db_profile(cfg, name: str, **fields) -> None:
    from amx.config import DBConfig

    cfg.db_profiles[name] = DBConfig(**fields)


def _set_llm_profile(cfg, name: str, **fields) -> None:
    from amx.config import LLMConfig

    cfg.llm_profiles[name] = LLMConfig(**fields)


def test_list_db_profiles_marks_active(client, auth_headers, cfg) -> None:
    _set_db_profile(cfg, "prod", backend="postgresql", host="db.example.com", database="app")
    _set_db_profile(cfg, "stage", backend="postgresql", host="stage.example.com")
    cfg.active_db_profile = "prod"

    response = client.get("/api/profiles/db", headers=auth_headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["active"] == "prod"
    by_name = {row["name"]: row for row in payload["profiles"]}
    assert by_name["prod"]["is_active"] is True
    assert by_name["stage"]["is_active"] is False


def test_get_db_profile_masks_secrets(client, auth_headers, cfg) -> None:
    _set_db_profile(
        cfg,
        "prod",
        backend="databricks",
        host="example.azuredatabricks.net",
        access_token="dapi-secret",
    )
    cfg.active_db_profile = "prod"

    response = client.get("/api/profiles/db/prod", headers=auth_headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["access_token"] == "********"
    assert payload["host"] == "example.azuredatabricks.net"


def test_put_db_profile_creates_then_round_trips(client, auth_headers, cfg) -> None:
    response = client.put(
        "/api/profiles/db/qa",
        headers=auth_headers,
        json={
            "backend": "postgresql",
            "host": "qa.example.com",
            "database": "qa_db",
            "password": "topsecret",
        },
    )
    assert response.status_code == 200
    assert response.json()["password"] == "********"
    # Round trip GET
    follow = client.get("/api/profiles/db/qa", headers=auth_headers).json()
    assert follow["host"] == "qa.example.com"
    assert follow["database"] == "qa_db"
    assert cfg.db_profiles["qa"].password == "topsecret"


def test_put_db_profile_placeholder_keeps_existing_secret(client, auth_headers, cfg) -> None:
    """The SPA reads the masked secret in the form; on save, it
    sends the placeholder back. The router treats that as 'no
    change' so the real secret never leaves localStorage but
    survives an edit on a different field."""
    _set_db_profile(cfg, "qa", backend="postgresql", host="old.example.com", password="real-secret")
    response = client.put(
        "/api/profiles/db/qa",
        headers=auth_headers,
        json={"host": "new.example.com", "password": "********"},
    )
    assert response.status_code == 200
    assert cfg.db_profiles["qa"].host == "new.example.com"
    assert cfg.db_profiles["qa"].password == "real-secret"


def test_put_db_profile_drops_unknown_fields(client, auth_headers) -> None:
    """Forwards-compat: a future SPA build could send a field this
    backend doesn't know about. The router must ignore it instead
    of 422-ing."""
    response = client.put(
        "/api/profiles/db/qa",
        headers=auth_headers,
        json={
            "backend": "postgresql",
            "host": "qa",
            "rocket_fuel": "high octane",  # unknown field
        },
    )
    assert response.status_code == 200


def test_delete_db_profile_active_promotes_next(client, auth_headers, cfg) -> None:
    """Deleting the active profile when others exist promotes the
    next remaining one. The user no longer has to manually activate
    a different profile before they can clean up."""
    _set_db_profile(cfg, "prod", backend="postgresql", host="x")
    _set_db_profile(cfg, "stage", backend="postgresql", host="y")
    cfg.active_db_profile = "prod"
    response = client.delete("/api/profiles/db/prod", headers=auth_headers)
    assert response.status_code == 200
    assert "prod" not in cfg.db_profiles
    # Active pointer migrated to the remaining profile.
    assert cfg.active_db_profile == "stage"
    body = response.json()
    assert body["active"] == "stage"
    assert body["remaining"] == 1


def test_delete_db_profile_last_one_clears_active(client, auth_headers, cfg) -> None:
    """User can wipe the only profile they have. The downstream
    surfaces (browse sidebar, /ask configure-llm flow, etc.) handle
    the empty-config state with friendly prompts."""
    _set_db_profile(cfg, "only", backend="postgresql", host="x")
    cfg.active_db_profile = "only"
    response = client.delete("/api/profiles/db/only", headers=auth_headers)
    assert response.status_code == 200
    assert cfg.db_profiles == {}
    assert cfg.active_db_profile == ""
    body = response.json()
    assert body["active"] is None
    assert body["remaining"] == 0


def test_delete_db_profile_removes_inactive(client, auth_headers, cfg) -> None:
    _set_db_profile(cfg, "prod", backend="postgresql", host="x")
    _set_db_profile(cfg, "stage", backend="postgresql", host="y")
    cfg.active_db_profile = "prod"
    response = client.delete("/api/profiles/db/stage", headers=auth_headers)
    assert response.status_code == 200
    assert "stage" not in cfg.db_profiles
    assert cfg.active_db_profile == "prod"


def test_activate_db_profile_404_for_unknown(client, auth_headers) -> None:
    response = client.post("/api/profiles/db/missing/activate", headers=auth_headers)
    assert response.status_code == 404


def test_activate_db_profile_flips_state(client, auth_headers, cfg) -> None:
    _set_db_profile(cfg, "prod", backend="postgresql", host="x")
    _set_db_profile(cfg, "stage", backend="postgresql", host="y")
    cfg.active_db_profile = "prod"

    response = client.post("/api/profiles/db/stage/activate", headers=auth_headers)
    assert response.status_code == 200
    assert cfg.active_db_profile == "stage"


def test_test_db_profile_invokes_connector(client, auth_headers, cfg, monkeypatch) -> None:
    from unittest.mock import MagicMock

    _set_db_profile(cfg, "prod", backend="postgresql", host="x")

    fake_result = MagicMock(ok=True, message="connected")
    fake_db = MagicMock(test_connection_result=MagicMock(return_value=fake_result))
    monkeypatch.setattr(
        "amx.web.routers.profiles.DatabaseConnector",
        lambda cfg: fake_db,
        raising=False,
    )
    # The router imports DatabaseConnector inline; monkeypatch the
    # actual location.
    import amx.db.connector as connector_module

    monkeypatch.setattr(connector_module, "DatabaseConnector", lambda cfg: fake_db)

    response = client.post("/api/profiles/db/prod/test", headers=auth_headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True


def test_llm_profile_round_trip(client, auth_headers, cfg) -> None:
    response = client.put(
        "/api/profiles/llm/gpt4",
        headers=auth_headers,
        json={"provider": "openai", "model": "gpt-4o", "api_key": "sk-real"},
    )
    assert response.status_code == 200
    assert response.json()["api_key"] == "********"
    assert cfg.llm_profiles["gpt4"].api_key == "sk-real"


def test_llm_profile_placeholder_preserves_key(client, auth_headers, cfg) -> None:
    _set_llm_profile(cfg, "gpt4", provider="openai", model="gpt-4o", api_key="sk-real")
    client.put(
        "/api/profiles/llm/gpt4",
        headers=auth_headers,
        json={"model": "gpt-5", "api_key": "********"},
    )
    assert cfg.llm_profiles["gpt4"].model == "gpt-5"
    assert cfg.llm_profiles["gpt4"].api_key == "sk-real"


def test_llm_profile_activate(client, auth_headers, cfg) -> None:
    _set_llm_profile(cfg, "gpt4", provider="openai", model="gpt-4o")
    cfg.active_llm_profile = "default"
    response = client.post("/api/profiles/llm/gpt4/activate", headers=auth_headers)
    assert response.status_code == 200
    assert cfg.active_llm_profile == "gpt4"


def test_delete_llm_profile_active_promotes_next(client, auth_headers, cfg) -> None:
    """Same as DB: deleting the active LLM with others present
    promotes the next remaining profile so the user can clean up
    without an extra activation roundtrip."""
    _set_llm_profile(cfg, "gpt4", provider="openai", model="gpt-4o")
    _set_llm_profile(cfg, "claude", provider="anthropic", model="claude-3")
    cfg.active_llm_profile = "gpt4"
    response = client.delete("/api/profiles/llm/gpt4", headers=auth_headers)
    assert response.status_code == 200
    assert "gpt4" not in cfg.llm_profiles
    assert cfg.active_llm_profile == "claude"


def test_delete_llm_profile_last_one_clears_active(client, auth_headers, cfg) -> None:
    """User can wipe the only LLM profile. The /ask 412 pre-flight
    (Studio) and ``_llm_available()`` check (CLI) both surface a
    "configure an LLM profile" prompt for the resulting empty
    state."""
    _set_llm_profile(cfg, "only", provider="openai", model="gpt-4o")
    cfg.active_llm_profile = "only"
    response = client.delete("/api/profiles/llm/only", headers=auth_headers)
    assert response.status_code == 200
    assert cfg.llm_profiles == {}
    assert cfg.active_llm_profile == ""
    # cfg.llm reset to a default empty LLMConfig — no stale provider
    # leaking from the just-deleted profile.
    assert (cfg.llm.provider or "") == ""
    body = response.json()
    assert body["active"] is None
    assert body["remaining"] == 0


def test_doc_profile_listing(client, auth_headers, cfg) -> None:
    cfg.doc_profiles["docs-prod"] = ["/docs/a", "/docs/b"]
    cfg.active_doc_profile = "docs-prod"
    response = client.get("/api/profiles/docs", headers=auth_headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["active"] == "docs-prod"
    assert payload["profiles"][0]["paths"] == ["/docs/a", "/docs/b"]


def test_code_profile_listing(client, auth_headers, cfg) -> None:
    cfg.code_profiles["main"] = "/src"
    cfg.active_code_profile = "main"
    response = client.get("/api/profiles/code", headers=auth_headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["active"] == "main"
    assert payload["profiles"][0]["path"] == "/src"


def test_db_backend_catalog_lists_supported_engines(client, auth_headers) -> None:
    """The Settings DB wizard fetches this catalog to render the
    backend dropdown + per-engine field list. Pin the contract so a
    silent dropout (e.g. Snowflake or Databricks vanishing from the
    wizard) shows up as a test failure."""
    response = client.get("/api/profiles/db/backends", headers=auth_headers)
    assert response.status_code == 200
    payload = response.json()
    ids = {b["id"] for b in payload["backends"]}
    expected = {
        "postgresql",
        "mysql",
        "snowflake",
        "databricks",
        "bigquery",
        "oracle",
        "mssql",
        "redshift",
        "clickhouse",
        "duckdb",
    }
    assert expected.issubset(ids)
    databricks = next(b for b in payload["backends"] if b["id"] == "databricks")
    assert databricks.get("supports_catalog") is True
    assert "http_path" in databricks["fields"]
    assert "access_token" in databricks["fields"]


def test_llm_provider_catalog_marks_needs_base_correctly(client, auth_headers) -> None:
    response = client.get("/api/profiles/llm/providers", headers=auth_headers)
    assert response.status_code == 200
    providers = {p["id"]: p for p in response.json()["providers"]}
    assert providers["openai"]["needs_key"] is True
    assert providers["openai"]["needs_base"] is False
    assert providers["ollama"]["needs_key"] is False
    assert providers["ollama"]["needs_base"] is True
    assert providers["databricks_serving"]["needs_key"] is True
    assert providers["databricks_serving"]["needs_base"] is True


def test_doc_profile_upsert_and_delete(client, auth_headers, cfg) -> None:
    response = client.put(
        "/api/profiles/docs/handbook",
        headers=auth_headers,
        json={"paths": ["/docs/a", " ", "https://example.com/x.pdf"]},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["paths"] == ["/docs/a", "https://example.com/x.pdf"]
    assert cfg.doc_profiles["handbook"] == ["/docs/a", "https://example.com/x.pdf"]

    response = client.delete("/api/profiles/docs/handbook", headers=auth_headers)
    assert response.status_code == 200
    assert "handbook" not in cfg.doc_profiles


def test_doc_profile_upsert_rejects_missing_paths(client, auth_headers) -> None:
    response = client.put(
        "/api/profiles/docs/x",
        headers=auth_headers,
        json={"name": "x"},
    )
    assert response.status_code == 400


def test_doc_profile_activate(client, auth_headers, cfg) -> None:
    cfg.doc_profiles["docs-prod"] = ["/d"]
    response = client.post("/api/profiles/docs/docs-prod/activate", headers=auth_headers)
    assert response.status_code == 200
    assert cfg.active_doc_profile == "docs-prod"


def test_code_profile_upsert_and_delete(client, auth_headers, cfg) -> None:
    response = client.put(
        "/api/profiles/code/main",
        headers=auth_headers,
        json={"path": "/src/repo"},
    )
    assert response.status_code == 200
    assert cfg.code_profiles["main"] == "/src/repo"

    response = client.delete("/api/profiles/code/main", headers=auth_headers)
    assert response.status_code == 200
    assert "main" not in cfg.code_profiles


def test_code_profile_upsert_rejects_empty_path(client, auth_headers) -> None:
    response = client.put(
        "/api/profiles/code/main",
        headers=auth_headers,
        json={"path": "   "},
    )
    assert response.status_code == 400


def test_code_profile_activate(client, auth_headers, cfg) -> None:
    cfg.code_profiles["repo"] = "/src"
    response = client.post("/api/profiles/code/repo/activate", headers=auth_headers)
    assert response.status_code == 200
    assert cfg.active_code_profile == "repo"
