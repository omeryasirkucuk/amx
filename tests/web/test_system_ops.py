"""System ops router tests — doctor + usage + catalog status.

Mocks the heavy paths (history store, search catalog) so the suite
never touches a real SQLite DB or Chroma instance — we only pin the
HTTP shape + CLI / web equivalence.
"""

from __future__ import annotations

import time
from unittest.mock import MagicMock


def test_doctor_returns_check_list(client, auth_headers, monkeypatch) -> None:
    """The endpoint should serialise every CheckResult the CLI's
    run_doctor would render. Pin the shape so a future check addition
    surfaces in both surfaces simultaneously."""
    from amx.cli_support.commands.doctor import CheckResult
    from amx.web.routers import system_ops

    monkeypatch.setattr(
        system_ops,
        "collect_doctor_checks",
        lambda cfg, *, skip_network: [
            CheckResult(name="amx version", ok=True, detail="0.99.0"),
            CheckResult(name="config file", ok=False, detail="missing", hint="run amx setup"),
        ],
    )
    response = client.get("/api/doctor", headers=auth_headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 2
    assert payload["failed"] == 1
    assert payload["ok"] is False
    assert payload["checks"][1]["hint"] == "run amx setup"


def test_doctor_skip_network_propagates(client, auth_headers, monkeypatch) -> None:
    """The query flag must reach collect_doctor_checks unchanged."""
    from amx.web.routers import system_ops

    captured: dict[str, bool] = {}

    def fake_collect(cfg, *, skip_network):
        captured["skip_network"] = skip_network
        return []

    monkeypatch.setattr(system_ops, "collect_doctor_checks", fake_collect)
    response = client.get("/api/doctor?skip_network=true", headers=auth_headers)
    assert response.status_code == 200
    assert captured["skip_network"] is True


def test_usage_with_no_history_returns_empty(client, auth_headers, monkeypatch) -> None:
    monkeypatch.setattr("amx.storage.sqlite_store.history_store", lambda: None)
    response = client.get("/api/usage?window=7d", headers=auth_headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["rows"] == []
    assert "History store" in (payload.get("message") or "")


def test_usage_aggregates_by_provider_model(client, auth_headers, monkeypatch) -> None:
    """Two runs from the same (provider, model) should collapse into
    one row with summed token counts. A second pair stays separate."""
    fake_runs = [
        {
            "started_at": time.time(),
            "llm_provider": "openai",
            "llm_model": "gpt-4o",
            "tokens_json": '{"records": [{"prompt_tokens": 100, "completion_tokens": 50}]}',
        },
        {
            "started_at": time.time(),
            "llm_provider": "openai",
            "llm_model": "gpt-4o",
            "tokens_json": '{"records": [{"prompt_tokens": 200, "completion_tokens": 75}]}',
        },
        {
            "started_at": time.time(),
            "llm_provider": "anthropic",
            "llm_model": "claude-haiku-4-5",
            "tokens_json": '{"records": [{"prompt_tokens": 10, "completion_tokens": 5}]}',
        },
    ]
    fake_hs = MagicMock(list_recent_runs=MagicMock(return_value=fake_runs))
    monkeypatch.setattr("amx.storage.sqlite_store.history_store", lambda: fake_hs)

    response = client.get("/api/usage?window=24h", headers=auth_headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["counted_runs"] == 3
    by_model = {(r["provider"], r["model"]): r for r in payload["rows"]}
    assert by_model[("openai", "gpt-4o")]["runs"] == 2
    assert by_model[("openai", "gpt-4o")]["input_tokens"] == 300
    assert by_model[("openai", "gpt-4o")]["output_tokens"] == 125
    assert by_model[("anthropic", "claude-haiku-4-5")]["runs"] == 1


def test_usage_pulls_all_command_kinds(client, auth_headers, monkeypatch) -> None:
    """The endpoint must request command_filter=None so re-run + apply
    rows contribute their tokens too. The default
    list_recent_runs(command_filter="analyze.run") used to drop those
    rows on the floor, leaving the Overview cards rendering "—" for
    users whose history was dominated by re-runs."""
    captured: dict[str, object] = {}

    def fake_list(*args, **kwargs):
        captured["kwargs"] = kwargs
        return []

    fake_hs = MagicMock(list_recent_runs=fake_list)
    monkeypatch.setattr("amx.storage.sqlite_store.history_store", lambda: fake_hs)

    response = client.get("/api/usage?window=all", headers=auth_headers)
    assert response.status_code == 200
    # Endpoint passes command_filter=None explicitly so every run kind
    # surfaces -- analyze, rerun, apply alike.
    assert captured["kwargs"].get("command_filter") is None


def test_usage_filters_by_window(client, auth_headers, monkeypatch) -> None:
    """Older runs must drop out when a finite window is set."""
    now = time.time()
    fake_runs = [
        {
            "started_at": now,
            "llm_provider": "openai",
            "llm_model": "gpt-4o",
            "tokens_json": '{"records": [{"prompt_tokens": 1, "completion_tokens": 1}]}',
        },
        {
            "started_at": now - (8 * 24 * 3600),  # 8 days ago — outside 7d
            "llm_provider": "openai",
            "llm_model": "gpt-4o",
            "tokens_json": '{"records": [{"prompt_tokens": 999, "completion_tokens": 999}]}',
        },
    ]
    fake_hs = MagicMock(list_recent_runs=MagicMock(return_value=fake_runs))
    monkeypatch.setattr("amx.storage.sqlite_store.history_store", lambda: fake_hs)

    response = client.get("/api/usage?window=7d", headers=auth_headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["counted_runs"] == 1
    assert payload["rows"][0]["input_tokens"] == 1


def test_catalog_status_when_uninitialised(client, auth_headers, monkeypatch) -> None:
    monkeypatch.setattr(
        "amx.search.catalog.SearchCatalog.from_history_store",
        classmethod(lambda cls: None),
    )
    response = client.get("/api/catalog/status", headers=auth_headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["ready"] is False
    assert "isn't initialised" in (payload.get("message") or "")


def test_history_store_status_when_disabled(client, auth_headers, cfg) -> None:
    cfg.history_store_enabled = False
    response = client.get("/api/admin/history-store-status", headers=auth_headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["enabled"] is False
    assert payload["outbox_pending"] == 0


def test_history_store_status_when_enabled(client, auth_headers, cfg, monkeypatch) -> None:
    cfg.history_store_enabled = True
    cfg.history_store_profile = "team-pg"
    cfg.history_store_schema = "amx_team"

    fake_shared = MagicMock()
    fake_shared.pending_count = MagicMock(return_value=4)
    fake_hs = MagicMock(shared=fake_shared)
    monkeypatch.setattr("amx.storage.sqlite_store.history_store", lambda: fake_hs)

    response = client.get("/api/admin/history-store-status", headers=auth_headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["enabled"] is True
    assert payload["profile"] == "team-pg"
    assert payload["schema"] == "amx_team"
    assert payload["outbox_pending"] == 4


def test_catalog_status_when_ready(client, auth_headers, monkeypatch) -> None:
    fake_status = {
        "entities": {"total_entities": 42, "effective_entities": 30, "last_synced_at": 0},
        "descriptions": {
            "total_descriptions": 100,
            "manual_count": 5,
            "reviewed_count": 80,
            "generated_count": 15,
            "rejected_count": 0,
        },
        "settings": {"context_detail": "standard"},
        "jobs": [],
    }
    fake_catalog = MagicMock()
    fake_catalog.sync_status = MagicMock(return_value=dict(fake_status))
    monkeypatch.setattr(
        "amx.search.catalog.SearchCatalog.from_history_store",
        classmethod(lambda cls: fake_catalog),
    )
    response = client.get("/api/catalog/status", headers=auth_headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["ready"] is True
    assert payload["entities"]["total_entities"] == 42
