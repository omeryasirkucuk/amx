"""GET /api/ask/context — scope rozeti backend."""

from __future__ import annotations

from amx.config import DBConfig


def _seed_db(cfg, name: str = "prod_pg") -> None:
    cfg.db_profiles[name] = DBConfig(backend="postgresql", host="x")


def test_context_empty_when_nothing_configured(client, auth_headers, cfg) -> None:
    res = client.get("/api/ask/context", headers=auth_headers)
    assert res.status_code == 200
    body = res.json()
    assert body["doc_profiles"] == []
    assert body["code_profiles"] == []


def test_context_global_doc_profile_in_scope(client, auth_headers, cfg) -> None:
    _seed_db(cfg, "prod_pg")
    cfg.active_db_profile = "prod_pg"
    cfg.active_db_profiles = ["prod_pg"]
    cfg.doc_profiles["handbook"] = ["/abs/handbook"]

    res = client.get("/api/ask/context", headers=auth_headers)
    assert res.status_code == 200
    names = [p["name"] for p in res.json()["doc_profiles"]]
    assert names == ["handbook"]


def test_context_linked_profile_only_for_matching_scope(client, auth_headers, cfg) -> None:
    _seed_db(cfg, "prod_pg")
    _seed_db(cfg, "analytics_bq")
    cfg.doc_profiles["contracts"] = ["/abs/contracts"]
    cfg.set_doc_profile_linked_dbs("contracts", ["prod_pg"])

    res = client.get("/api/ask/context?scope_profiles=analytics_bq", headers=auth_headers)
    assert res.status_code == 200
    assert [p["name"] for p in res.json()["doc_profiles"]] == []

    res2 = client.get("/api/ask/context?scope_profiles=prod_pg", headers=auth_headers)
    assert [p["name"] for p in res2.json()["doc_profiles"]] == ["contracts"]


def test_context_returns_link_metadata(client, auth_headers, cfg) -> None:
    _seed_db(cfg, "prod_pg")
    cfg.doc_profiles["contracts"] = ["/abs/contracts"]
    cfg.set_doc_profile_linked_dbs("contracts", ["prod_pg"])

    res = client.get("/api/ask/context?scope_profiles=prod_pg", headers=auth_headers)
    body = res.json()
    assert body["doc_profiles"][0]["linked_db_profiles"] == ["prod_pg"]
    assert body["doc_profiles"][0]["paths"] == ["/abs/contracts"]
