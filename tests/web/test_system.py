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
    the next request.

    DB activation was retired in 0.13 (see ContextResponse), so the
    handler no longer surfaces ``active_db_profile`` /
    ``active_db_profiles``; LLM + doc + code activation continue to
    flow through unchanged.
    """
    cfg.active_db_profile = "prod"  # internal default-fallback pointer
    cfg.active_llm_profile = "claude"
    cfg.current_schema = "sales"
    cfg.current_table = "orders"

    response = client.get("/api/context", headers=auth_headers)
    assert response.status_code == 200
    payload = response.json()
    assert "active_db_profile" not in payload
    assert "active_db_profiles" not in payload
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
    # DB activation is no longer surfaced; the LLM / schema / table
    # fields still coerce blank strings to JSON null.
    assert "active_db_profile" not in payload
    assert payload["active_llm_profile"] is None
    assert payload["current_schema"] is None
    assert payload["current_table"] is None


def test_context_reports_batch_support_for_openai(client, auth_headers, cfg) -> None:
    """OpenAI is in amx.llm.batch._PROVIDER_MAP, so /api/context must
    advertise batch support — the SPA disables the RunNew "Batch mode"
    toggle when this is False."""
    cfg.llm.provider = "openai"
    response = client.get("/api/context", headers=auth_headers)
    assert response.status_code == 200
    assert response.json()["llm_supports_batch"] is True


def test_context_reports_no_batch_for_ollama(client, auth_headers, cfg) -> None:
    """Ollama has no Batch API implementation; the toggle stays disabled."""
    cfg.llm.provider = "ollama"
    response = client.get("/api/context", headers=auth_headers)
    assert response.status_code == 200
    assert response.json()["llm_supports_batch"] is False


def test_context_surfaces_llm_profile_defaults(client, auth_headers, cfg) -> None:
    """RunNew's "Advanced LLM settings" disclosure pre-fills from
    ``llm_profile_defaults`` on /api/context. The handler must mirror
    every tuning knob from ``cfg.llm`` so the SPA never has to fetch
    the profile twice."""
    cfg.llm.provider = "openai"
    cfg.llm.model = "gpt-4o"
    cfg.llm.temperature = 0.42
    cfg.llm.max_tokens = 8192
    cfg.llm.n_alternatives = 4
    cfg.llm.column_batch_size = 12
    cfg.llm.prompt_detail = "detailed"
    cfg.llm.description_verbosity = "comprehensive"
    cfg.llm.thinking_budget = 2048
    cfg.llm.logprob_high = 0.9
    cfg.llm.logprob_medium = 0.55
    cfg.llm.custom_input_cost_per_mtok = 1.25
    cfg.llm.custom_output_cost_per_mtok = 2.5

    response = client.get("/api/context", headers=auth_headers)
    assert response.status_code == 200
    defaults = response.json()["llm_profile_defaults"]
    assert defaults is not None
    assert defaults["temperature"] == 0.42
    assert defaults["max_tokens"] == 8192
    assert defaults["n_alternatives"] == 4
    assert defaults["column_batch_size"] == 12
    assert defaults["prompt_detail"] == "detailed"
    assert defaults["description_verbosity"] == "comprehensive"
    assert defaults["thinking_budget"] == 2048
    assert defaults["logprob_high"] == 0.9
    assert defaults["logprob_medium"] == 0.55
    assert defaults["custom_input_cost_per_mtok"] == 1.25
    assert defaults["custom_output_cost_per_mtok"] == 2.5


def test_context_omits_llm_profile_defaults_without_provider(
    client, auth_headers, cfg
) -> None:
    """Without a configured provider the handler returns ``null`` so
    the SPA can render an "LLM not configured" empty state instead of
    showing a row of fake defaults."""
    cfg.llm.provider = ""
    cfg.llm.model = ""
    response = client.get("/api/context", headers=auth_headers)
    assert response.status_code == 200
    assert response.json()["llm_profile_defaults"] is None
