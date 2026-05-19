"""Tests for the universal runtime hierarchy picker added in 0.12.3.

Pre-0.12.3 only Databricks (catalog) and PostgreSQL (database) had a
runtime picker — every other 2-level backend silently produced empty
listings when the profile left the database field blank. The new
``ensure_hierarchy_resolved`` helper dispatches to the right picker
per backend and ``ensure_database_selected`` no longer hard-codes a
backend whitelist.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from unittest.mock import patch

import pytest

from amx.cli_support.catalog_picker import (
    ensure_database_selected,
    ensure_hierarchy_resolved,
)


@dataclass
class _FakeCfg:
    backend: str
    database: str = ""
    catalog: str = ""


@dataclass
class _FakeDB:
    """Minimal stand-in for :class:`DatabaseConnector` with just the
    surface ``ensure_database_selected`` and ``ensure_catalog_selected``
    touch."""

    cfg: _FakeCfg
    _databases: list[str] = field(default_factory=list)
    _catalogs: list[str] = field(default_factory=list)
    _supports_catalogs: bool = False
    _list_databases_raises: BaseException | None = None
    reconnects: int = 0

    def supports_catalogs(self) -> bool:
        return self._supports_catalogs

    def list_databases(self) -> list[str]:
        if self._list_databases_raises is not None:
            raise self._list_databases_raises
        return list(self._databases)

    def list_catalogs(self) -> list[str]:
        return list(self._catalogs)

    def reconnect(self) -> None:
        self.reconnects += 1


@pytest.fixture
def patch_picker_choice():
    """Patch the ``_ask_choice_or_cancel`` primitive so the picker
    returns a deterministic value without prompting."""

    def _patch(returned: str):
        return patch(
            "amx.cli_support.commands.manual._ask_choice_or_cancel",
            return_value=returned,
        )

    return _patch


@pytest.mark.parametrize(
    "backend",
    # 2-level backends — Hive joins this list because Hive treats
    # databases as schemas (no catalog above). Trino is intentionally
    # excluded: it's 3-level (catalog > schema > table) and uses the
    # catalog picker, not the database picker.
    ["postgresql", "snowflake", "mysql", "oracle", "mssql", "redshift", "clickhouse", "hive"],
)
def test_database_picker_runs_for_every_2level_backend(backend: str, patch_picker_choice) -> None:
    """The pre-0.12.3 whitelist locked out 5 backends (mysql, oracle,
    mssql, redshift, clickhouse). Verify every 2-level backend now gets
    the picker when ``cfg.database`` is blank and the adapter returns
    a non-empty database list."""
    db = _FakeDB(cfg=_FakeCfg(backend=backend), _databases=["analytics", "warehouse"])
    with patch_picker_choice("warehouse"):
        chosen = ensure_database_selected(db)
    assert chosen == "warehouse"
    assert db.cfg.database == "warehouse"
    assert db.reconnects == 1


def test_picker_skips_when_database_already_pinned(patch_picker_choice) -> None:
    """The picker is for the unpinned case only — pinned profiles must
    not be re-prompted."""
    db = _FakeDB(cfg=_FakeCfg(backend="mysql", database="prod"), _databases=["prod", "staging"])
    with patch_picker_choice("staging"):
        chosen = ensure_database_selected(db)
    # No picker fired; existing pin is returned untouched.
    assert chosen == "prod"
    assert db.cfg.database == "prod"
    assert db.reconnects == 0


def test_picker_silent_when_adapter_returns_no_databases(patch_picker_choice) -> None:
    """A 1-database server (e.g. MSSQL with one user DB) must not spam
    the user with a "no databases visible" warning — the 0.12.2
    behaviour for non-postgres backends. Silent return is the contract."""
    db = _FakeDB(cfg=_FakeCfg(backend="mssql"), _databases=[])
    with patch_picker_choice("anything"):
        chosen = ensure_database_selected(db)
    assert chosen == ""
    assert db.reconnects == 0


def test_picker_surfaces_missing_driver_as_warning() -> None:
    """When ``list_databases`` raises ``ImportError`` (missing optional
    driver), the picker must show that ImportError's message verbatim
    (it carries the actionable ``pip install 'amx-cli[<extra>]'`` hint)
    instead of silently returning empty."""
    db = _FakeDB(
        cfg=_FakeCfg(backend="mysql"),
        _list_databases_raises=ImportError(
            "pymysql is required for the MySQL backend. "
            "Install the extra: pip install 'amx-cli[mysql]'"
        ),
    )
    with patch("amx.cli_support.catalog_picker.warn") as warn_mock:
        chosen = ensure_database_selected(db)
    assert chosen == ""
    assert warn_mock.called
    msg = warn_mock.call_args[0][0]
    assert "pip install 'amx-cli[mysql]'" in msg


def test_ensure_hierarchy_resolved_dispatches_to_catalog_picker(patch_picker_choice) -> None:
    """3-level backends (databricks) hit the catalog picker, not the
    database picker — verify the unified entry point dispatches correctly."""
    db = _FakeDB(
        cfg=_FakeCfg(backend="databricks"),
        _catalogs=["main", "analytics"],
        _supports_catalogs=True,
    )
    # `ensure_catalog_selected` shows the picker via _ask_choice_or_cancel
    # (imported lazily inside the helper from manual.py); the picker
    # writes ``cfg.catalog`` not ``cfg.database``.
    with patch_picker_choice("analytics"):
        chosen = ensure_hierarchy_resolved(db)
    assert chosen == "analytics"
    assert db.cfg.catalog == "analytics"
    # Catalog picker is NOT supposed to set database.
    assert db.cfg.database == ""


def test_ensure_hierarchy_resolved_falls_through_to_database_picker(patch_picker_choice) -> None:
    """2-level backends route through the database picker."""
    db = _FakeDB(cfg=_FakeCfg(backend="postgresql"), _databases=["app", "metrics"])
    with patch_picker_choice("app"):
        chosen = ensure_hierarchy_resolved(db)
    assert chosen == "app"
    assert db.cfg.database == "app"


def test_pinned_catalog_is_used_silently_on_every_run(patch_picker_choice) -> None:
    """Profile with ``cfg.catalog`` already set must not re-prompt on
    every ``/run``. Mirrors what the 2-level database picker already
    does for ``cfg.database`` — pinned == use directly, no re-ask.

    Regression guard for the user-reported friction: a Databricks
    profile with ``catalog=main`` was showing the picker on every
    run, forcing the user to press Enter even though they had already
    pinned a catalog at profile-creation time.
    """
    db = _FakeDB(
        cfg=_FakeCfg(backend="databricks", catalog="main"),
        _catalogs=["main", "analytics"],
        _supports_catalogs=True,
    )
    # The picker primitive must NOT be invoked at all when the
    # catalog is pinned. Asserting the choice fn was never called is
    # the strongest form of "no prompt shown to the user".
    with patch("amx.cli_support.commands.manual._ask_choice_or_cancel") as choice_mock:
        chosen = ensure_hierarchy_resolved(db)
    assert chosen == "main"
    assert db.cfg.catalog == "main"
    choice_mock.assert_not_called()


def test_pinned_catalog_no_longer_visible_falls_back_to_picker(patch_picker_choice) -> None:
    """Defence: if the pinned catalog has been dropped on the server
    (or the role lost access), the silent path can't honour the pin
    — show the picker so the user can choose a still-valid catalog
    instead of returning a name the next list_schemas would 404 on."""
    db = _FakeDB(
        cfg=_FakeCfg(backend="databricks", catalog="archived"),
        _catalogs=["main", "analytics"],
        _supports_catalogs=True,
    )
    with patch_picker_choice("main"):
        chosen = ensure_hierarchy_resolved(db)
    assert chosen == "main"
    assert db.cfg.catalog == "main"
