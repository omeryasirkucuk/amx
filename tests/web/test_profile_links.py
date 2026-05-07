"""Studio /api/profiles/{docs,code} link round-trip + validation."""

from __future__ import annotations

from amx.config import DBConfig


def _seed_db(cfg, name: str = "prod_pg") -> None:
    cfg.db_profiles[name] = DBConfig(backend="postgresql", host="x")


def test_put_doc_profile_round_trips_links(client, auth_headers, cfg) -> None:
    _seed_db(cfg, "prod_pg")
    _seed_db(cfg, "analytics_bq")

    res = client.put(
        "/api/profiles/docs/contracts",
        headers=auth_headers,
        json={"paths": ["/abs/contracts"], "linked_db_profiles": ["prod_pg"]},
    )
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["linked_db_profiles"] == ["prod_pg"]

    listing = client.get("/api/profiles/docs", headers=auth_headers).json()
    by_name = {row["name"]: row for row in listing["profiles"]}
    assert by_name["contracts"]["linked_db_profiles"] == ["prod_pg"]


def test_put_code_profile_round_trips_links(client, auth_headers, cfg) -> None:
    _seed_db(cfg, "prod_pg")

    res = client.put(
        "/api/profiles/code/etl",
        headers=auth_headers,
        json={"path": "/abs/etl", "linked_db_profiles": ["prod_pg"]},
    )
    assert res.status_code == 200
    assert res.json()["linked_db_profiles"] == ["prod_pg"]


def test_put_rejects_unknown_db_link(client, auth_headers, cfg) -> None:
    res = client.put(
        "/api/profiles/docs/contracts",
        headers=auth_headers,
        json={"paths": ["/abs/contracts"], "linked_db_profiles": ["nope"]},
    )
    assert res.status_code == 400
    assert "nope" in res.text


def test_put_links_omitted_keeps_existing(client, auth_headers, cfg) -> None:
    """Body without ``linked_db_profiles`` must not nuke prior links."""
    _seed_db(cfg, "prod_pg")
    cfg.doc_profiles["contracts"] = ["/abs/contracts"]
    cfg.set_doc_profile_linked_dbs("contracts", ["prod_pg"])

    res = client.put(
        "/api/profiles/docs/contracts",
        headers=auth_headers,
        json={"paths": ["/abs/contracts/v2"]},  # no linked_db_profiles key
    )
    assert res.status_code == 200
    assert res.json()["linked_db_profiles"] == ["prod_pg"]


def test_put_empty_links_clears(client, auth_headers, cfg) -> None:
    _seed_db(cfg, "prod_pg")
    cfg.doc_profiles["contracts"] = ["/abs/contracts"]
    cfg.set_doc_profile_linked_dbs("contracts", ["prod_pg"])

    res = client.put(
        "/api/profiles/docs/contracts",
        headers=auth_headers,
        json={"paths": ["/abs/contracts"], "linked_db_profiles": []},
    )
    assert res.status_code == 200
    assert res.json()["linked_db_profiles"] == []
    assert cfg.doc_profile_linked_dbs.get("contracts", []) == []


def test_delete_cascade_drops_links(client, auth_headers, cfg) -> None:
    _seed_db(cfg, "prod_pg")
    cfg.doc_profiles["contracts"] = ["/abs/contracts"]
    cfg.set_doc_profile_linked_dbs("contracts", ["prod_pg"])

    res = client.delete("/api/profiles/docs/contracts", headers=auth_headers)
    assert res.status_code == 200
    assert "contracts" not in cfg.doc_profile_linked_dbs
