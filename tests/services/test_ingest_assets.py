from unittest.mock import MagicMock


def test_ingest_assets_service_runs_per_type():
    from amx.services.ingest_assets import (
        IngestAssetsService,
        IngestRequest,
        IngestResult,
    )

    connector = MagicMock()
    connector.list_remote_notebooks.return_value = iter([])
    connector.list_remote_jobs.return_value = iter([])
    catalog = MagicMock()
    catalog.sync_remote_assets.return_value = {"notebooks": 0, "jobs": 0}
    catalog.rebuild_remote_asset_lineage.return_value = {
        "notebooks": 0,
        "queries": 0,
        "streams": 0,
        "pipelines": 0,
    }

    svc = IngestAssetsService(connector=connector, catalog=catalog)
    req = IngestRequest(
        profile_name="prod",
        types=["notebooks", "jobs"],
        history_days=7,
        runs_per_job=20,
    )
    result: IngestResult = svc.run(req, progress=lambda evt: None)
    assert result.counts["notebooks"] == 0
    assert "lineage" in result.counts
    catalog.sync_remote_assets.assert_called_once()
    catalog.rebuild_remote_asset_lineage.assert_called_once_with(profile_name="prod")


def test_ingest_assets_service_emits_progress():
    from amx.services.ingest_assets import (
        IngestAssetsService,
        IngestProgressEvent,
        IngestRequest,
    )

    connector = MagicMock()
    connector.list_remote_notebooks.return_value = iter([object(), object()])
    catalog = MagicMock()
    catalog.sync_remote_assets.return_value = {"notebooks": 2}
    catalog.rebuild_remote_asset_lineage.return_value = {
        "notebooks": 0,
        "queries": 0,
        "streams": 0,
        "pipelines": 0,
    }

    svc = IngestAssetsService(connector=connector, catalog=catalog)
    events: list[IngestProgressEvent] = []
    svc.run(
        IngestRequest(profile_name="prod", types=["notebooks"], history_days=7, runs_per_job=20),
        progress=events.append,
    )
    states = [(e.asset_type, e.state) for e in events]
    assert ("notebooks", "started") in states
    assert ("notebooks", "completed") in states
    # The orchestrator also emits storage + lineage + indexing
    # completion events. ``indexing`` lands as "completed" when the
    # asset RAG store is reachable, "failed" otherwise (e.g. CI
    # environments without chromadb). Either way the event MUST be
    # emitted so the Studio SSE stream knows the phase ran.
    assert any(e.asset_type == "storage" and e.state == "completed" for e in events)
    assert any(e.asset_type == "lineage" and e.state == "completed" for e in events)
    assert any(e.asset_type == "indexing" and e.state in {"completed", "failed"} for e in events)


def test_ingest_assets_service_reports_per_type_failure():
    from amx.services.ingest_assets import (
        IngestAssetsService,
        IngestRequest,
    )

    connector = MagicMock()

    def raise_on_iter(**_kwargs):
        raise PermissionError("ACCOUNT_USAGE denied")

    connector.list_remote_queries.side_effect = raise_on_iter
    catalog = MagicMock()
    catalog.sync_remote_assets.return_value = {"queries": 0}
    catalog.rebuild_remote_asset_lineage.return_value = {
        "notebooks": 0,
        "queries": 0,
        "streams": 0,
        "pipelines": 0,
    }

    svc = IngestAssetsService(connector=connector, catalog=catalog)
    req = IngestRequest(profile_name="prod", types=["queries"], history_days=7, runs_per_job=20)
    result = svc.run(req, progress=lambda e: None)
    assert "queries" in result.failures
    assert "ACCOUNT_USAGE" in result.failures["queries"]


def test_ingest_assets_service_passes_history_kwargs_to_queries():
    from amx.services.ingest_assets import (
        IngestAssetsService,
        IngestRequest,
    )

    connector = MagicMock()
    connector.list_remote_queries.return_value = iter([])
    catalog = MagicMock()
    catalog.sync_remote_assets.return_value = {"queries": 0}
    catalog.rebuild_remote_asset_lineage.return_value = {
        "notebooks": 0,
        "queries": 0,
        "streams": 0,
        "pipelines": 0,
    }

    svc = IngestAssetsService(connector=connector, catalog=catalog)
    req = IngestRequest(
        profile_name="prod",
        types=["queries"],
        history_days=30,
        runs_per_job=20,
        query_history_limit=500,
    )
    svc.run(req, progress=lambda e: None)
    connector.list_remote_queries.assert_called_once_with(history_days=30, limit=500)
