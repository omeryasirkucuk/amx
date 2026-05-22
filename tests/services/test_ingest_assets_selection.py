"""PR-A: IngestRequest.selection threads through to the connector.

When the user picks specific external_ids in the Studio /
CLI browse-and-pick wizard, the service must forward that subset
through the connector's ``external_id_filter`` kwarg instead of
re-listing everything from the platform. ``queries`` and
``task_dependencies`` are intentionally outside the selection
contract — they're time-windowed aggregates filtered by
``history_days`` / ``query_history_limit``.
"""

from unittest.mock import MagicMock


def _stub_catalog() -> MagicMock:
    catalog = MagicMock()
    catalog.sync_remote_assets.return_value = {"notebooks": 0, "jobs": 0}
    catalog.rebuild_remote_asset_lineage.return_value = {
        "notebooks": 0,
        "queries": 0,
        "streams": 0,
        "pipelines": 0,
    }
    return catalog


def test_selection_passes_external_id_filter_to_connector():
    from amx.services.ingest_assets import IngestAssetsService, IngestRequest

    connector = MagicMock()
    connector.list_remote_notebooks.return_value = iter([])
    svc = IngestAssetsService(connector=connector, catalog=_stub_catalog())
    svc.run(
        IngestRequest(
            profile_name="prod",
            types=["notebooks"],
            selection={"notebooks": ["ext-1", "ext-3"]},
        ),
        progress=lambda _e: None,
    )
    connector.list_remote_notebooks.assert_called_once_with(external_id_filter=["ext-1", "ext-3"])


def test_missing_kind_in_selection_keeps_default_behaviour():
    """A kind absent from the dict should still get ``external_id_filter=None``.

    The pre-PR-A path was "ingest everything"; omitting a kind from
    the user's picks must keep that contract.
    """
    from amx.services.ingest_assets import IngestAssetsService, IngestRequest

    connector = MagicMock()
    connector.list_remote_notebooks.return_value = iter([])
    connector.list_remote_jobs.return_value = iter([])
    svc = IngestAssetsService(connector=connector, catalog=_stub_catalog())
    svc.run(
        IngestRequest(
            profile_name="prod",
            types=["notebooks", "jobs"],
            selection={"notebooks": ["ext-1"]},
        ),
        progress=lambda _e: None,
    )
    connector.list_remote_notebooks.assert_called_once_with(external_id_filter=["ext-1"])
    connector.list_remote_jobs.assert_called_once_with(runs_per_job=20, external_id_filter=None)


def test_no_selection_means_no_filter():
    """``selection=None`` is the explicit ``ingest all`` opt-out."""
    from amx.services.ingest_assets import IngestAssetsService, IngestRequest

    connector = MagicMock()
    connector.list_remote_notebooks.return_value = iter([])
    svc = IngestAssetsService(connector=connector, catalog=_stub_catalog())
    svc.run(
        IngestRequest(profile_name="prod", types=["notebooks"], selection=None),
        progress=lambda _e: None,
    )
    connector.list_remote_notebooks.assert_called_once_with(external_id_filter=None)


def test_queries_ignore_selection():
    """Selections targeted at ``queries`` must not reach the connector.

    The query iterator is time-windowed; the connector doesn't
    accept ``external_id_filter`` for it, so silently dropping the
    selection key for that kind keeps the contract.
    """
    from amx.services.ingest_assets import IngestAssetsService, IngestRequest

    connector = MagicMock()
    connector.list_remote_queries.return_value = iter([])
    svc = IngestAssetsService(connector=connector, catalog=_stub_catalog())
    svc.run(
        IngestRequest(
            profile_name="prod",
            types=["queries"],
            selection={"queries": ["irrelevant"]},
            history_days=14,
            query_history_limit=500,
        ),
        progress=lambda _e: None,
    )
    connector.list_remote_queries.assert_called_once_with(history_days=14, limit=500)
