"""POST /api/code/analyze — Studio Code Agent endpoint."""

from __future__ import annotations

from amx.config import DBConfig, LLMConfig


def _seed_minimal(cfg) -> None:
    cfg.db_profiles["prod_pg"] = DBConfig(backend="postgresql", host="x")
    cfg.active_db_profile = "prod_pg"
    cfg.llm_profiles["default"] = LLMConfig(provider="openai", model="gpt-4o-mini")
    cfg.active_llm_profile = "default"
    cfg.llm = cfg.llm_profiles["default"]


def test_analyze_requires_llm(client, auth_headers, cfg) -> None:
    cfg.db_profiles["prod_pg"] = DBConfig(backend="postgresql", host="x")
    cfg.code_profiles["etl"] = "/abs/etl"
    cfg.active_code_profile = "etl"
    # The web conftest pre-seeds cfg.llm with provider="openai" so the
    # configure-llm 412 only fires when we explicitly clear it.
    cfg.llm = LLMConfig()
    res = client.post(
        "/api/code/analyze",
        headers=auth_headers,
        json={"schema": "sales", "tables": ["orders"]},
    )
    assert res.status_code == 412
    assert "configure-llm" in res.text


def test_analyze_requires_code_profile(client, auth_headers, cfg) -> None:
    _seed_minimal(cfg)
    res = client.post(
        "/api/code/analyze",
        headers=auth_headers,
        json={"schema": "sales", "tables": ["orders"]},
    )
    assert res.status_code == 400
    assert "code profile" in res.text.lower()


def test_analyze_validates_tables_count(client, auth_headers, cfg) -> None:
    _seed_minimal(cfg)
    cfg.code_profiles["etl"] = "/abs/etl"
    cfg.active_code_profile = "etl"
    # Empty list should fail validation (min_length=1)
    res = client.post(
        "/api/code/analyze",
        headers=auth_headers,
        json={"schema": "sales", "tables": []},
    )
    assert res.status_code == 422


def test_analyze_validates_table_count_upper_bound(client, auth_headers, cfg) -> None:
    _seed_minimal(cfg)
    cfg.code_profiles["etl"] = "/abs/etl"
    cfg.active_code_profile = "etl"
    res = client.post(
        "/api/code/analyze",
        headers=auth_headers,
        json={
            "schema": "sales",
            "tables": [f"t{i}" for i in range(25)],  # > 20 max
        },
    )
    assert res.status_code == 422
