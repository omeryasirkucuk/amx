"""sync_profile_skeleton must observe a cancel event at its loop heads."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from amx.search import _skeleton_jobs, drift


def setup_function() -> None:
    _skeleton_jobs._jobs.clear()


def _make_catalog() -> MagicMock:
    catalog = MagicMock()
    conn = MagicMock()
    catalog._connect.return_value.__enter__ = MagicMock(return_value=conn)
    catalog._connect.return_value.__exit__ = MagicMock(return_value=False)
    catalog.history_store = None
    return catalog


def _fake_connector():
    fake = MagicMock()
    fake.list_schemas.return_value = ["public"]
    fake.list_assets.return_value = [("orders", "table")]
    return fake


def test_pre_set_cancel_exits_without_completing() -> None:
    event = _skeleton_jobs.register("prof")
    event.set()

    cfg = SimpleNamespace(
        db_profiles={
            "prof": SimpleNamespace(
                backend="postgres", catalog="", database="app", dataset=""
            ),
        }
    )
    catalog = _make_catalog()

    with patch.object(drift, "_scoped_connector", return_value=_fake_connector()):
        summary = drift.sync_profile_skeleton(cfg, "prof", catalog)

    assert summary["state"] == "cancelled"
    catalog.finish_skeleton_sync.assert_called_with(
        "prof", ok=False, error="cancelled"
    )


def test_normal_run_finishes_with_ok_true() -> None:
    cfg = SimpleNamespace(
        db_profiles={
            "prof": SimpleNamespace(
                backend="postgres", catalog="", database="app", dataset=""
            ),
        }
    )
    catalog = _make_catalog()

    with patch.object(drift, "_scoped_connector", return_value=_fake_connector()):
        summary = drift.sync_profile_skeleton(cfg, "prof", catalog)

    assert summary["state"] == "done"
    final_calls = [
        call for call in catalog.finish_skeleton_sync.call_args_list
        if call.kwargs.get("ok") is True
    ]
    assert final_calls, "expected at least one finish_skeleton_sync(..., ok=True)"


def test_unregister_is_called_after_normal_run() -> None:
    cfg = SimpleNamespace(
        db_profiles={
            "prof": SimpleNamespace(
                backend="postgres", catalog="", database="app", dataset=""
            ),
        }
    )
    catalog = _make_catalog()

    with patch.object(drift, "_scoped_connector", return_value=_fake_connector()):
        drift.sync_profile_skeleton(cfg, "prof", catalog)

    assert "prof" not in _skeleton_jobs.running_profiles()
