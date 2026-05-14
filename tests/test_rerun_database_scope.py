"""Regression: ``_connector_for_db_profile`` must honor the parent
run's database / catalog scope.

The Re-Run + Variations executors call ``_connector_for_db_profile``
to open a fresh connector for the original asset's profile. The
profile on disk often has a blank ``database`` field (the user types
``/use-db <profile> <database>`` at REPL time to set the active
scope; the saved profile retains the bare connection cfg). Without
the override, the connector falls back to the engine default —
Postgres' ``postgres`` system database — and the inspector
immediately raises ``NoSuchTableError`` for a table that
demonstrably exists in the database the parent /run found it.

Live deploy failure captured before this fix:

    sqlalchemy.exc.NoSuchTableError: cars.data
    → ProfilingError: Profiling failed for cars.data: cars.data [NoSuchTableError]

The table ``cars.data`` lives in the ``bird_train`` database; the
profile's ``database`` field is blank; the connector connected to
``postgres`` instead and 100% of Re-Runs failed.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from unittest.mock import MagicMock, patch

import pytest

from amx.agents.rerun_context import _connector_for_db_profile


@dataclass
class _StubDBConfig:
    """Minimal stand-in for ``amx.config.DBConfig``. Only carries the
    fields ``_connector_for_db_profile`` reads / replaces — backend,
    database, catalog. Comparable via ``dataclasses.replace`` because
    it's a real dataclass."""

    backend: str = "postgresql"
    database: str = ""
    catalog: str = ""


@dataclass
class _StubCfg:
    """Minimal AMXConfig stand-in carrying ``db_profiles`` only."""

    db_profiles: dict[str, _StubDBConfig] = field(default_factory=dict)


@pytest.fixture
def cfg_with_blank_db() -> _StubCfg:
    """A DB profile whose ``database`` field is blank — the production
    bug-trigger shape."""
    return _StubCfg(
        db_profiles={
            "local-postgre": _StubDBConfig(
                backend="postgresql",
                database="",
                catalog="",
            )
        }
    )


class TestConnectorRespectsParentScope:
    """Pin the override pipeline so a blank-database profile + a
    parent run targeting a real database hands the connector the
    real database name."""

    def test_database_override_reaches_connector(self, cfg_with_blank_db: _StubCfg) -> None:
        """When the caller passes ``database="bird_train"``, the
        underlying connector's cfg has database=bird_train — even
        though the saved profile carries an empty string."""
        with patch("amx.agents.rerun_context.DatabaseConnector") as DC:
            DC.return_value = MagicMock()
            _connector_for_db_profile(
                cfg_with_blank_db,
                "local-postgre",
                database="bird_train",
            )
            # The connector was instantiated exactly once.
            assert DC.call_count == 1
            (passed_cfg,) = DC.call_args.args
            assert passed_cfg.database == "bird_train", (
                f"connector was handed cfg.database={passed_cfg.database!r}; "
                "expected 'bird_train'. Without this override the connector "
                "would fall back to the engine default and NoSuchTableError "
                "every re-run."
            )

    def test_catalog_override_reaches_connector(self, cfg_with_blank_db: _StubCfg) -> None:
        """Databricks / BigQuery use ``catalog`` (or its project alias)
        — same override pipeline must work for them."""
        with patch("amx.agents.rerun_context.DatabaseConnector") as DC:
            DC.return_value = MagicMock()
            _connector_for_db_profile(
                cfg_with_blank_db,
                "local-postgre",
                catalog="prod_warehouse",
            )
            (passed_cfg,) = DC.call_args.args
            assert passed_cfg.catalog == "prod_warehouse"

    def test_no_override_leaves_profile_untouched(self, cfg_with_blank_db: _StubCfg) -> None:
        """When neither database nor catalog is provided, the
        connector receives the bare profile object — same behaviour
        as before the regression-fix landed, so the no-scope path
        does not drift."""
        with patch("amx.agents.rerun_context.DatabaseConnector") as DC:
            DC.return_value = MagicMock()
            _connector_for_db_profile(cfg_with_blank_db, "local-postgre")
            (passed_cfg,) = DC.call_args.args
            # Same instance — no dataclass.replace was applied.
            assert passed_cfg is cfg_with_blank_db.db_profiles["local-postgre"]

    def test_overrides_do_not_mutate_saved_profile(self, cfg_with_blank_db: _StubCfg) -> None:
        """``dataclasses.replace`` returns a new instance — the
        original profile on ``cfg.db_profiles`` MUST stay blank so a
        subsequent /run that resolves the same profile doesn't
        accidentally inherit the re-run's scope override."""
        with patch("amx.agents.rerun_context.DatabaseConnector") as DC:
            DC.return_value = MagicMock()
            _connector_for_db_profile(
                cfg_with_blank_db,
                "local-postgre",
                database="bird_train",
            )
        # The saved profile is untouched.
        assert cfg_with_blank_db.db_profiles["local-postgre"].database == ""

    def test_unknown_profile_raises_rerun_context_error(self, cfg_with_blank_db: _StubCfg) -> None:
        """Existing safety behaviour preserved — picking a profile
        that no longer exists surfaces a friendly error."""
        from amx.agents.rerun_context import RerunContextError

        with pytest.raises(RerunContextError):
            _connector_for_db_profile(cfg_with_blank_db, "missing")
