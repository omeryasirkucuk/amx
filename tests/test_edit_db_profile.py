"""Pin the /edit-db-profile contract.

User report (2026-05-04): the user accidentally pinned a Databricks
profile to a non-existent ``catalog="sap"`` and every subsequent
``amx`` startup tripped the SCHEMA_NOT_FOUND bootstrap warning. To
make that whole class of bug unreachable, AMX now ships:

- A dedicated ``/edit-db-profile`` (no more silent edit-on-collision
  via ``/add-db-profile``).
- An inline catalog/database picker in the wizard that lists what the
  live backend can actually see, with a "type custom value" + "save
  anyway?" gate for the rare permission-blocked-listing case.
- Removal of ``/schema`` and ``/table`` (those wrote
  ``cfg.current_schema`` / ``cfg.current_table`` blindly without any
  check). The fields stay; only the slash commands go.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from amx.cli_support.commands.db import (
    _ask_catalog_or_database_with_picker,
    cmd_add_profile,
    cmd_edit_profile,
    cmd_remove_profile,  # noqa: F401 — sanity import; keeps the module in sync
)
from amx.cli_support.slash_commands import cmd_heads_for_namespace
from amx.config import AMXConfig, DBConfig

# ── /add-db-profile and /edit-db-profile are non-overlapping ─────────────


def test_add_profile_refuses_when_name_already_exists() -> None:
    cfg = AMXConfig()
    cfg.db_profiles = {"foo": DBConfig(backend="postgresql", host="db.example.com")}
    cfg.active_db_profile = "foo"

    with patch("amx.cli_support.commands.db.interactive_db_block") as wizard:
        cmd_add_profile(cfg, ["foo"])
        # Crucial: the collision is rejected BEFORE the wizard runs.
        wizard.assert_not_called()


def test_add_profile_aborts_when_user_escs_on_name_prompt() -> None:
    """Regression: pre-0.12.9 ``ask`` swallowed Esc and returned ``""``,
    so the wizard kept walking with ``name=""``, called the engine
    picker, and on a second Esc happily saved a profile with an empty
    name AND empty backend (``Profile saved and activated: []``). The
    fix is to let ``PromptCancelled`` propagate so the dispatcher
    aborts the whole command before any save happens.
    """
    from amx.utils.console import PromptCancelled

    cfg = AMXConfig()
    cfg.db_profiles = {}
    cfg.active_db_profile = None

    with (
        patch("amx.cli_support.commands.db.ask", side_effect=PromptCancelled()),
        patch("amx.cli_support.commands.db.interactive_db_block") as wizard,
    ):
        with pytest.raises(PromptCancelled):
            cmd_add_profile(cfg, [])
        # The engine picker must never run, and nothing must be saved.
        wizard.assert_not_called()
    assert cfg.db_profiles == {}
    assert cfg.active_db_profile is None


def test_edit_profile_unknown_name_errors_without_wizard() -> None:
    cfg = AMXConfig()
    cfg.db_profiles = {"foo": DBConfig(backend="postgresql", host="db.example.com")}
    cfg.active_db_profile = "foo"

    with patch("amx.cli_support.commands.db.interactive_db_block") as wizard:
        cmd_edit_profile(cfg, ["nonexistent"])
        wizard.assert_not_called()


def test_edit_profile_walks_wizard_with_existing_as_defaults() -> None:
    cfg = AMXConfig()
    original = DBConfig(
        backend="postgresql", host="db.example.com", user="alice", database="orders"
    )
    cfg.db = original
    cfg.db_profiles = {"foo": original}
    cfg.active_db_profile = "foo"

    edited = DBConfig(backend="postgresql", host="db.example.com", user="alice", database="reports")
    captured: dict[str, object] = {}

    def spy(defaults):
        captured["defaults"] = defaults
        return edited

    with patch("amx.cli_support.commands.db.interactive_db_block", side_effect=spy):
        cmd_edit_profile(cfg, ["foo"])

    # Wizard saw the existing profile as defaults — Enter-to-keep can work.
    assert captured["defaults"] is original
    # The edit landed in db_profiles AND in cfg.db (because foo was active).
    assert cfg.db_profiles["foo"] is edited
    assert cfg.db.database == "reports"


def test_edit_profile_does_not_change_active_when_editing_inactive_profile() -> None:
    cfg = AMXConfig()
    active = DBConfig(backend="postgresql", host="active.example.com")
    other = DBConfig(backend="postgresql", host="other.example.com")
    cfg.db = active
    cfg.db_profiles = {"active": active, "other": other}
    cfg.active_db_profile = "active"

    edited = DBConfig(backend="postgresql", host="other-edited.example.com")
    with patch("amx.cli_support.commands.db.interactive_db_block", return_value=edited):
        cmd_edit_profile(cfg, ["other"])

    # The edit landed only in the other profile — active scope unchanged.
    assert cfg.db_profiles["other"] is edited
    assert cfg.active_db_profile == "active"
    assert cfg.db is active


# ── /schema and /table are gone from the registry ────────────────────────


def test_schema_and_table_slash_commands_are_gone() -> None:
    """They used to live under the /db namespace and silently wrote
    cfg.current_schema / cfg.current_table without any validation.
    They were the reason "olmayan bir şeye bağlanmak" was even
    possible. Make sure the heads stay gone — re-introducing them
    should be a deliberate, reviewed decision."""
    db_heads = cmd_heads_for_namespace("db")
    assert "schema" not in db_heads
    assert "table" not in db_heads
    # And the new edit head is wired up.
    assert "edit-db-profile" in db_heads


# ── Inline catalog/database picker — happy path & override ───────────────


class _FakeConnector:
    """Stand-in for DatabaseConnector that returns canned listings."""

    def __init__(
        self, catalogs: list[str] | None = None, databases: list[str] | None = None
    ) -> None:
        self._catalogs = catalogs or []
        self._databases = databases or []

    def list_catalogs(self) -> list[str]:
        return list(self._catalogs)

    def list_databases(self) -> list[str]:
        return list(self._databases)


def test_picker_falls_back_to_freeform_when_listing_is_empty() -> None:
    """Connection works but role lacks visibility — wizard must still
    let the user finish, just with a one-line warn."""
    cfg = DBConfig(backend="postgresql", host="db.example.com")
    with (
        patch(
            "amx.db.connector.DatabaseConnector",
            return_value=_FakeConnector(databases=[]),
        ),
        patch(
            "amx.cli_support.commands.db._ask_update_text",
            return_value="manual_typed_value",
        ),
    ):
        result = _ask_catalog_or_database_with_picker(
            label="database",
            current_value="",
            optional=True,
            probe_cfg=cfg,
            listing_kind="databases",
        )
    assert result == "manual_typed_value"


def test_picker_offers_listing_and_returns_selection() -> None:
    cfg = DBConfig(
        backend="databricks", host="ws.example.com", http_path="/sql/1.0/x", access_token="t"
    )
    with (
        patch(
            "amx.db.connector.DatabaseConnector",
            return_value=_FakeConnector(catalogs=["amx_test", "main", "system"]),
        ),
        patch(
            "amx.cli_support.commands.db.ask_choice",
            return_value="main",
        ),
    ):
        result = _ask_catalog_or_database_with_picker(
            label="Unity Catalog",
            current_value="",
            optional=True,
            probe_cfg=cfg,
            listing_kind="catalogs",
        )
    assert result == "main"


def test_picker_keep_current_returns_existing_value() -> None:
    cfg = DBConfig(
        backend="databricks", host="ws.example.com", http_path="/sql/1.0/x", access_token="t"
    )
    with (
        patch(
            "amx.db.connector.DatabaseConnector",
            return_value=_FakeConnector(catalogs=["amx_test", "main"]),
        ),
        patch(
            "amx.cli_support.commands.db.ask_choice",
            return_value="(keep current: amx_test)",
        ),
    ):
        result = _ask_catalog_or_database_with_picker(
            label="Unity Catalog",
            current_value="amx_test",
            optional=True,
            probe_cfg=cfg,
            listing_kind="catalogs",
        )
    assert result == "amx_test"


def test_picker_custom_value_in_listing_is_accepted_silently() -> None:
    """Custom value path that happens to match the listing — no warn,
    no extra confirm, just save the typed name."""
    cfg = DBConfig(
        backend="databricks", host="ws.example.com", http_path="/sql/1.0/x", access_token="t"
    )
    with (
        patch(
            "amx.db.connector.DatabaseConnector",
            return_value=_FakeConnector(catalogs=["amx_test", "main"]),
        ),
        patch(
            "amx.cli_support.commands.db.ask_choice",
            return_value="(type custom value)",
        ),
        patch(
            "amx.cli_support.commands.db._ask_update_text",
            return_value="amx_test",
        ),
        patch("amx.cli_support.commands.db.confirm") as confirm_mock,
    ):
        result = _ask_catalog_or_database_with_picker(
            label="Unity Catalog",
            current_value="",
            optional=True,
            probe_cfg=cfg,
            listing_kind="catalogs",
        )
    assert result == "amx_test"
    # The confirm should NOT have fired — the typed value was in the listing.
    confirm_mock.assert_not_called()


def test_picker_off_listing_value_blocks_unless_user_confirms_override() -> None:
    """Custom value not in the listing — that's the SCHEMA_NOT_FOUND
    footgun. The picker must ask 'Save anyway?' and re-open if the
    user says No, accept if they say Yes."""
    cfg = DBConfig(
        backend="databricks", host="ws.example.com", http_path="/sql/1.0/x", access_token="t"
    )
    with (
        patch(
            "amx.db.connector.DatabaseConnector",
            return_value=_FakeConnector(catalogs=["amx_test", "main"]),
        ),
        patch(
            "amx.cli_support.commands.db.ask_choice",
            return_value="(type custom value)",
        ),
        patch(
            "amx.cli_support.commands.db._ask_update_text",
            return_value="ghost_catalog",
        ),
        patch(
            "amx.cli_support.commands.db.confirm",
            return_value=True,
        ) as confirm_mock,
    ):
        result = _ask_catalog_or_database_with_picker(
            label="Unity Catalog",
            current_value="",
            optional=True,
            probe_cfg=cfg,
            listing_kind="catalogs",
        )
    assert result == "ghost_catalog"
    # The confirm fired — that's the override gate.
    confirm_mock.assert_called_once()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
