"""Resolver behaviour for /ask doc/code scope from linked DB maps."""

from __future__ import annotations

from amx.config import AMXConfig, DBConfig
from amx.search._agent.scope import (
    resolve_code_profiles_for_scope,
    resolve_doc_profiles_for_scope,
)


def _seed(cfg: AMXConfig) -> None:
    cfg.db_profiles["prod_pg"] = DBConfig(backend="postgresql", host="p")
    cfg.db_profiles["analytics_bq"] = DBConfig(backend="bigquery", host="bq")
    cfg.doc_profiles["contracts"] = ["/abs/contracts"]
    cfg.doc_profiles["company_handbook"] = ["/abs/handbook"]
    cfg.code_profiles["etl"] = "/abs/etl"
    cfg.active_doc_profile = "company_handbook"
    cfg.active_code_profile = "etl"


def test_empty_scope_falls_back_to_active(tmp_path) -> None:
    cfg = AMXConfig.load(str(tmp_path / "config.yml"))
    _seed(cfg)
    assert resolve_doc_profiles_for_scope(cfg, []) == ["company_handbook"]
    assert resolve_code_profiles_for_scope(cfg, []) == ["etl"]


def test_global_profile_in_every_scope(tmp_path) -> None:
    """A doc profile with no link list is treated as global."""
    cfg = AMXConfig.load(str(tmp_path / "config.yml"))
    _seed(cfg)
    # No links anywhere — both docs return for every scope.
    assert set(resolve_doc_profiles_for_scope(cfg, ["prod_pg"])) == {
        "contracts",
        "company_handbook",
    }


def test_linked_profile_only_in_matching_scope(tmp_path) -> None:
    cfg = AMXConfig.load(str(tmp_path / "config.yml"))
    _seed(cfg)
    cfg.set_doc_profile_linked_dbs("contracts", ["prod_pg"])
    assert resolve_doc_profiles_for_scope(cfg, ["prod_pg"]) == [
        "contracts",
        "company_handbook",
    ]
    # ``contracts`` is linked only to prod_pg, not analytics_bq.
    assert resolve_doc_profiles_for_scope(cfg, ["analytics_bq"]) == ["company_handbook"]


def test_multi_db_scope_unions_links(tmp_path) -> None:
    cfg = AMXConfig.load(str(tmp_path / "config.yml"))
    _seed(cfg)
    cfg.set_doc_profile_linked_dbs("contracts", ["prod_pg"])
    cfg.set_doc_profile_linked_dbs("company_handbook", ["analytics_bq"])
    assert set(resolve_doc_profiles_for_scope(cfg, ["prod_pg", "analytics_bq"])) == {
        "contracts",
        "company_handbook",
    }
