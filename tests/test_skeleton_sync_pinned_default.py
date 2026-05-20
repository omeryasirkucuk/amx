"""sync_profile_skeleton must walk only the pinned container."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from amx.search import _skeleton_jobs


def setup_function() -> None:
    _skeleton_jobs._jobs.clear()


@pytest.fixture()
def fake_connector():
    conn = MagicMock()
    conn.list_catalogs.return_value = ["prod", "dev", "scratch"]
    conn.list_databases.return_value = ["prod", "dev", "scratch"]
    conn.list_schemas.return_value = ["public"]
    conn.list_assets.return_value = [("orders", "table")]
    return conn


def _make_catalog() -> MagicMock:
    catalog = MagicMock()
    conn = MagicMock()
    catalog._connect.return_value.__enter__ = MagicMock(return_value=conn)
    catalog._connect.return_value.__exit__ = MagicMock(return_value=False)
    catalog.history_store = None
    return catalog


def test_pinned_catalog_short_circuits_enumeration(fake_connector) -> None:
    from amx.search import drift

    cfg = SimpleNamespace(
        db_profiles={
            "prof": SimpleNamespace(
                backend="databricks", catalog="prod", database="", dataset=""
            ),
        }
    )
    catalog = _make_catalog()

    with patch.object(drift, "_scoped_connector", return_value=fake_connector):
        summary = drift.sync_profile_skeleton(cfg, "prof", catalog)

    assert summary["containers"] == ["prod"]
    fake_connector.list_catalogs.assert_not_called()
    fake_connector.list_databases.assert_not_called()


def test_pinned_database_short_circuits_enumeration(fake_connector) -> None:
    from amx.search import drift

    cfg = SimpleNamespace(
        db_profiles={
            "prof": SimpleNamespace(
                backend="postgres", catalog="", database="app", dataset=""
            ),
        }
    )
    catalog = _make_catalog()

    with patch.object(drift, "_scoped_connector", return_value=fake_connector):
        summary = drift.sync_profile_skeleton(cfg, "prof", catalog)

    assert summary["containers"] == ["app"]
    fake_connector.list_databases.assert_not_called()


def test_unpinned_profile_falls_back_to_enumeration(fake_connector) -> None:
    from amx.search import drift

    cfg = SimpleNamespace(
        db_profiles={
            "prof": SimpleNamespace(
                backend="databricks", catalog="", database="", dataset=""
            ),
        }
    )
    catalog = _make_catalog()

    with patch.object(drift, "_scoped_connector", return_value=fake_connector):
        summary = drift.sync_profile_skeleton(cfg, "prof", catalog)

    assert set(summary["containers"]) == {"prod", "dev", "scratch"}
    fake_connector.list_catalogs.assert_called()
