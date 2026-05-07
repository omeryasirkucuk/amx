"""Doc/Code profile ↔ DB link map: schema, validation, cascade cleanup."""

from __future__ import annotations

from amx.config import AMXConfig, DBConfig


def _seed(cfg: AMXConfig) -> None:
    cfg.db_profiles["prod_pg"] = DBConfig(backend="postgresql", host="prod")
    cfg.db_profiles["analytics_bq"] = DBConfig(backend="bigquery", host="bq")
    cfg.doc_profiles["contracts"] = ["/abs/contracts"]
    cfg.doc_profiles["misc"] = ["/abs/misc"]
    cfg.code_profiles["etl-jobs"] = "/abs/etl"


def test_set_doc_link_round_trips(tmp_path) -> None:
    cfg = AMXConfig.load(str(tmp_path / "config.yml"))
    _seed(cfg)
    cfg.set_doc_profile_linked_dbs("contracts", ["prod_pg"])
    cfg.save()

    reloaded = AMXConfig.load(str(tmp_path / "config.yml"))
    assert reloaded.doc_profile_linked_dbs.get("contracts") == ["prod_pg"]


def test_set_link_rejects_unknown_db(tmp_path) -> None:
    cfg = AMXConfig.load(str(tmp_path / "config.yml"))
    _seed(cfg)
    try:
        cfg.set_doc_profile_linked_dbs("contracts", ["ghost_db"])
    except KeyError as exc:
        assert "ghost_db" in str(exc)
    else:
        raise AssertionError("expected KeyError for unknown DB profile")


def test_set_link_dedupes_and_strips(tmp_path) -> None:
    cfg = AMXConfig.load(str(tmp_path / "config.yml"))
    _seed(cfg)
    cfg.set_doc_profile_linked_dbs("contracts", ["prod_pg", " ", "prod_pg", "analytics_bq"])
    assert cfg.doc_profile_linked_dbs["contracts"] == ["prod_pg", "analytics_bq"]


def test_clear_link_removes_entry(tmp_path) -> None:
    cfg = AMXConfig.load(str(tmp_path / "config.yml"))
    _seed(cfg)
    cfg.set_doc_profile_linked_dbs("contracts", ["prod_pg"])
    cfg.set_doc_profile_linked_dbs("contracts", [])
    assert "contracts" not in cfg.doc_profile_linked_dbs


def test_remove_db_profile_cascades_to_links(tmp_path) -> None:
    """Deleting a DB profile must drop it from every doc/code link list."""
    cfg = AMXConfig.load(str(tmp_path / "config.yml"))
    _seed(cfg)
    cfg.set_doc_profile_linked_dbs("contracts", ["prod_pg", "analytics_bq"])
    cfg.set_code_profile_linked_dbs("etl-jobs", ["prod_pg"])

    cfg.remove_db_profile("prod_pg")

    assert cfg.doc_profile_linked_dbs["contracts"] == ["analytics_bq"]
    assert cfg.code_profile_linked_dbs.get("etl-jobs", []) == []


def test_remove_doc_profile_drops_link_entry(tmp_path) -> None:
    cfg = AMXConfig.load(str(tmp_path / "config.yml"))
    _seed(cfg)
    cfg.set_doc_profile_linked_dbs("contracts", ["prod_pg"])
    cfg.remove_doc_profile("contracts")
    assert "contracts" not in cfg.doc_profile_linked_dbs


def test_load_prunes_ghost_db_references(tmp_path) -> None:
    """A YAML carrying links to deleted DB profiles must be cleaned at load."""
    yaml = tmp_path / "config.yml"
    yaml.write_text(
        """\
db_profiles:
  prod_pg:
    backend: postgresql
    host: x
doc_profiles:
  contracts:
    - /abs/contracts
doc_profile_linked_dbs:
  contracts: [prod_pg, removed_pg]
  ghost_doc: [prod_pg]
""",
        encoding="utf-8",
    )
    cfg = AMXConfig.load(str(yaml))
    # ghost_db dropped from list, ghost_doc key dropped entirely
    assert cfg.doc_profile_linked_dbs == {"contracts": ["prod_pg"]}
