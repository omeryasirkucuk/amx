"""Tests for the optional workspace_token field on DBConfig.

Covers:
* default value is empty string,
* explicit value is stored,
* field is in _DB_SECRET_FIELDS so the keyring externalises it,
* round-trip through _db_to_mapping / _db_from_mapping preserves the value,
* the Databricks profile wizard code path references the field.
"""

from __future__ import annotations


def test_dbconfig_workspace_token_optional_default_empty():
    from amx.config import DBConfig

    cfg = DBConfig(backend="databricks", host="https://x", access_token="t1")
    assert cfg.workspace_token == ""


def test_dbconfig_workspace_token_explicit():
    from amx.config import DBConfig

    cfg = DBConfig(
        backend="databricks",
        host="https://x",
        access_token="t1",
        workspace_token="ws-tok",
    )
    assert cfg.workspace_token == "ws-tok"


def test_workspace_token_in_secret_fields_tuple():
    from amx.config import _DB_SECRET_FIELDS

    assert "workspace_token" in _DB_SECRET_FIELDS


def test_dbconfig_roundtrip_through_dict_preserves_workspace_token():
    """workspace_token survives _db_to_mapping -> _db_from_mapping."""
    import tempfile
    from pathlib import Path

    from amx.config import AMXConfig, DBConfig

    cfg_obj = DBConfig(
        backend="databricks",
        host="https://x",
        access_token="t1",
        workspace_token="ws-tok",
    )

    tmp = Path(tempfile.mkdtemp())
    amx_cfg = AMXConfig()
    amx_cfg.db_profiles["test"] = cfg_obj
    amx_cfg.save(path=str(tmp / "config.yml"))
    fresh = AMXConfig.load(path=str(tmp / "config.yml"))
    rebuilt = fresh.db_profiles["test"]

    assert rebuilt.workspace_token == "ws-tok"


def test_databricks_wizard_source_collects_workspace_token():
    """Source-level check that the Databricks wizard references workspace_token.

    The full wizard is hard to drive in isolation due to interactive prompts;
    this asserts that the code path references the new field, which catches a
    wholly missed wiring.
    """
    import inspect

    import amx.cli_support.commands.db as db_mod

    src = inspect.getsource(db_mod)
    assert "workspace_token" in src, "Databricks profile wizard does not collect workspace_token"
