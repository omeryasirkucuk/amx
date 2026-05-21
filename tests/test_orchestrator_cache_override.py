"""Bulk worker honours ``cache_override_assets`` and surfaces a
better remediation message when the live DB can't reflect a table.

When Studio's reachability pre-flight (``POST /api/runs/preflight``)
finds a table the live DB can't read and the user picks "Use cached
schema" in the dialog, the SPA submits the run with
``cache_override_assets=["sap_s6p.adrt"]``. The orchestrator must
then skip ``profile_table`` for that asset and synthesize a
metadata-only :class:`TableProfile` from the catalog cache.

When the override is missing (CLI submit, direct API caller), the
underlying ``NoSuchTableError`` from SQLAlchemy must propagate as
itself out of ``_column_profiler.profile_table`` so the worker can
narrow-catch and surface an actionable "table not reachable" message.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from amx.db.connector import AssetKind, TableProfile


class _StubDB:
    """Minimal connector stand-in that records every ``profile_table``
    call. Tests assert the override path NEVER reaches this stub."""

    def __init__(self) -> None:
        self.profile_calls: list[tuple[str, str]] = []
        self.cfg = MagicMock(active_db_profile="demo", db=MagicMock(database="appdb", catalog=None))

    def profile_table(
        self,
        schema: str,
        table: str,
        *,
        asset_kind: AssetKind | None = None,
    ) -> TableProfile:
        self.profile_calls.append((schema, table))
        return TableProfile(schema=schema, name=table, columns=[])


def _orchestrator_with_stub_db(stub: _StubDB):
    """Build a bare Orchestrator instance with the helper attached;
    we don't need the full agent chain to exercise the override path.
    """
    from amx.agents.orchestrator import Orchestrator

    orch = Orchestrator.__new__(Orchestrator)
    orch.db = stub  # type: ignore[assignment]
    return orch


def test_synthesize_uses_search_catalog(monkeypatch) -> None:
    """``_synthesize_profile_from_cache`` reads from the SearchCatalog
    helper and returns a TableProfile populated with column names,
    dtypes, nullable, and existing comments — no PK/FK/samples/stats.
    """
    stub = _StubDB()
    orch = _orchestrator_with_stub_db(stub)

    captured: dict[str, object] = {}

    class _FakeCatalog:
        @classmethod
        def from_history_store(cls):
            return cls()

        def fetch_columns_for_table(
            self,
            profile,
            *,
            schema_name,
            table_name,
            database_name=None,
        ):
            captured["profile"] = profile
            captured["schema"] = schema_name
            captured["table"] = table_name
            captured["database"] = database_name
            return [
                {"name": "order_id", "dtype": "BIGINT", "nullable": False, "comment": "PK"},
                {"name": "total", "dtype": "DECIMAL", "nullable": True, "comment": ""},
            ]

    monkeypatch.setattr("amx.search.catalog.SearchCatalog", _FakeCatalog)

    profile = orch._synthesize_profile_from_cache("sales", "orders", AssetKind.TABLE)

    assert captured["profile"] == "demo"
    assert captured["schema"] == "sales"
    assert captured["table"] == "orders"
    assert captured["database"] == "appdb"
    assert profile.schema == "sales"
    assert profile.name == "orders"
    assert profile.asset_kind == AssetKind.TABLE
    assert len(profile.columns) == 2
    assert profile.columns[0].name == "order_id"
    assert profile.columns[0].dtype == "BIGINT"
    assert profile.columns[0].nullable is False
    assert profile.columns[0].existing_comment == "PK"
    assert profile.columns[1].existing_comment is None  # empty string normalized
    # Defensive: every "live signal" stays empty.
    assert profile.primary_key == []
    assert profile.foreign_keys == []
    assert profile.row_count == 0


def test_synthesize_returns_empty_profile_when_catalog_unavailable(monkeypatch) -> None:
    """SearchCatalog import or instantiation failure must not blow up
    the bulk worker — we fall through to an empty TableProfile so the
    run can still complete (even if the LLM has nothing to work with).
    """
    stub = _StubDB()
    orch = _orchestrator_with_stub_db(stub)

    def _boom(*_a, **_kw):
        raise RuntimeError("no history store")

    monkeypatch.setattr("amx.search.catalog.SearchCatalog.from_history_store", _boom)

    profile = orch._synthesize_profile_from_cache("sales", "orders", AssetKind.TABLE)
    assert profile.columns == []
    assert profile.schema == "sales"
    assert profile.name == "orders"


def test_column_profiler_re_raises_no_such_table_error() -> None:
    """``_column_profiler.profile_table`` must re-raise
    ``NoSuchTableError`` as itself (not wrap it in ``ProfilingError``)
    so the bulk worker can narrow-catch and surface the new
    "table_not_reachable" remediation message. Wrapping it would lose
    the class signal and force string parsing in the caller."""
    from sqlalchemy.exc import NoSuchTableError

    from amx.db._column_profiler import profile_table

    fake_insp = MagicMock()
    fake_insp.get_columns = MagicMock(side_effect=NoSuchTableError("sap_s6p.adrt"))
    # Other inspector calls are best-effort; return empty so we reach
    # the ``get_columns`` call where the NoSuchTableError is raised.
    fake_insp.get_pk_constraint = MagicMock(return_value={"constrained_columns": []})
    fake_insp.get_foreign_keys = MagicMock(return_value=[])
    fake_insp.get_unique_constraints = MagicMock(return_value=[])
    fake_insp.get_check_constraints = MagicMock(return_value=[])

    fake_adapter = MagicMock()
    fake_adapter.fully_qualified_name = MagicMock(return_value="sap_s6p.adrt")
    fake_adapter.get_table_stats = MagicMock(return_value={})
    fake_adapter.actionable_profile_error = MagicMock(return_value=None)
    fake_adapter.get_analytics_metadata = MagicMock(return_value=MagicMock())

    fake_db = MagicMock(backend="postgresql")
    fake_db._adapter = fake_adapter
    fake_db._normalize_id = lambda s: s
    fake_db._get_inspector = MagicMock(return_value=fake_insp)
    fake_db.engine = MagicMock()
    fake_db.get_table_comment = MagicMock(return_value=None)
    fake_db.get_schema_comment = MagicMock(return_value=None)
    fake_db.get_database_comment = MagicMock(return_value=None)
    fake_db.get_incoming_foreign_keys = MagicMock(return_value=[])
    fake_db.get_related_table_comments = MagicMock(return_value=[])

    with pytest.raises(NoSuchTableError):
        profile_table(fake_db, "sap_s6p", "adrt")

    fake_insp.get_columns.assert_called_once()
