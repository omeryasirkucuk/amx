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


# ── End-to-end: build_context_snapshot reads database from settings_json ──


class TestBuildContextSnapshotResolvesDatabase:
    """Pin the end-to-end resolution path: a parent run whose
    ``analysis_runs.database`` column is blank (the production shape —
    that column doesn't exist on the table) but whose ``settings_json``
    carries ``database='bird_train'`` must surface 'bird_train' to the
    connector. The previous fix's tests called
    ``_connector_for_db_profile`` directly with the kwarg
    pre-supplied — they couldn't catch the upstream "we never read the
    right field" bug that this test would have prevented."""

    def test_database_resolved_from_settings_json(
        self, tmp_path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from pathlib import Path

        from amx.agents import rerun_context as rc
        from amx.storage.sqlite_store import SQLiteHistoryStore

        hs = SQLiteHistoryStore(Path(tmp_path) / "history.db")
        hs.init()
        monkeypatch.setattr(rc, "history_store", lambda: hs)

        # Seed a parent run whose settings_json carries the database
        # while the top-level column is blank — production shape.
        run_id = hs.create_run(
            command="analyze.run",
            mode="chat",
            db_backend="postgresql",
            db_profile="local-postgre",
            llm_provider="openai",
            llm_model="gpt-x",
            scope={"cars": ["data"]},
            settings={"database": "bird_train", "catalog": None},
        )
        [target_id] = hs.save_run_results(
            run_id,
            [
                {
                    "schema": "cars",
                    "table": "data",
                    "column": "ID",
                    "asset_kind": "column",
                    "source": "llm",
                    "confidence": "medium",
                    "alternatives": ["a"],
                }
            ],
        )

        # Build a real cfg whose db_profile has the bug-trigger shape
        # (blank database). Stub the DatabaseConnector so we capture
        # what database value reaches it without making a live
        # connection.
        from amx.config import AMXConfig, DBConfig

        cfg = AMXConfig()
        cfg.db_profiles = {
            "local-postgre": DBConfig(backend="postgresql", database=""),
        }
        cfg.active_db_profile = "local-postgre"

        captured: dict[str, str] = {}

        class _StubConnector:
            def __init__(self, cfg_in) -> None:
                captured["database"] = cfg_in.database or ""
                captured["catalog"] = getattr(cfg_in, "catalog", "") or ""
                self.cfg = cfg_in
                self.backend = "postgresql"
                self.stats_label = "stub"

            def profile_table(self, schema, table, **kwargs):
                # Surface enough fields so _table_profile_to_dicts
                # doesn't crash. The actual contents don't matter —
                # we're testing the connector-cfg wiring, not the
                # profiling output.
                from amx.db.connector import TableProfile

                return TableProfile(
                    schema=schema,
                    name=table,
                    existing_comment=None,
                    schema_comment=None,
                    database_comment=None,
                )

            def close(self):
                pass

        monkeypatch.setattr(rc, "DatabaseConnector", _StubConnector)

        rc.build_context_snapshot(cfg, target_result_id=int(target_id), job_id="probe-1")

        assert captured["database"] == "bird_train", (
            f"Connector was handed database={captured.get('database')!r}; "
            "expected 'bird_train' from the parent run's settings_json. "
            "If this returns '' or None, the parent_database read is "
            "still missing the settings_json fallback and 100% of "
            "Re-Runs / Variations will fail in production."
        )
