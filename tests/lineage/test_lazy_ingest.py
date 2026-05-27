"""Single-asset lazy ingest — selection mapping, notebook path ingest, cold index."""

from __future__ import annotations

import json
import time
from unittest.mock import MagicMock

from amx.lineage.native import lazy_ingest, notebook_index

# ── selection requests (job / pipeline only) ─────────────────────────────


def test_job_and_pipeline_build_selection_requests():
    job = lazy_ingest.selection_request_for(profile="db", kind="job", external_id="9")
    assert job is not None and job.types == ["jobs"] and job.selection == {"jobs": ["9"]}
    pipe = lazy_ingest.selection_request_for(profile="db", kind="pipeline", external_id="7")
    assert pipe is not None and pipe.types == ["pipelines"]


def test_notebook_and_query_are_not_selection_ingestable():
    # notebooks ingest by path, queries by query_definition — not via selection.
    assert lazy_ingest.selection_request_for(profile="db", kind="notebook", external_id="1") is None
    assert lazy_ingest.selection_request_for(profile="db", kind="query", external_id="1") is None


def test_unknown_kind_or_blank_id_returns_none():
    assert (
        lazy_ingest.selection_request_for(profile="db", kind="dashboard", external_id="1") is None
    )
    assert lazy_ingest.selection_request_for(profile="db", kind="job", external_id="") is None


# ── notebook ingest via the persisted index path ─────────────────────────


def _catalog(tmp_path, fetched_id):
    catalog = MagicMock()
    catalog.db_path = tmp_path / "history.db"
    conn = MagicMock()
    conn.execute.return_value.fetchone.return_value = (fetched_id,) if fetched_id else None
    catalog._connect.return_value.__enter__.return_value = conn
    catalog._connect.return_value.__exit__.return_value = False
    return catalog


def test_notebook_ingest_uses_index_path(tmp_path):
    catalog = _catalog(tmp_path, fetched_id=42)
    idx = notebook_index.cache_path(tmp_path, "db", "h")
    idx.write_text(
        json.dumps(
            {
                "version": notebook_index.CACHE_VERSION,
                "built_at": time.time(),
                "names": {"123": "My NB"},
                "paths": {"123": "/A/My NB"},
            }
        ),
        encoding="utf-8",
    )
    connector = MagicMock()
    connector.workspace_client.host = "h"
    connector.list_remote_notebooks_by_specs.return_value = [MagicMock()]  # one DTO

    out = lazy_ingest.ingest_one_asset(
        connector=connector, catalog=catalog, profile="db", kind="notebook", external_id="123"
    )
    assert out.status == "ok"
    assert out.remote_id == 42
    connector.list_remote_notebooks_by_specs.assert_called_once_with([("123", "/A/My NB")])


def test_notebook_cold_index_returns_indexing(tmp_path, monkeypatch):
    catalog = _catalog(tmp_path, fetched_id=None)
    connector = MagicMock()
    connector.workspace_client.host = "h"
    built = {}
    monkeypatch.setattr(
        notebook_index,
        "ensure_background_build",
        lambda client, path, **kw: built.update({"called": True}),
    )
    out = lazy_ingest.ingest_one_asset(
        connector=connector, catalog=catalog, profile="db", kind="notebook", external_id="999"
    )
    assert out.status == "indexing"
    assert built.get("called")  # background build kicked off
    connector.list_remote_notebooks_by_specs.assert_not_called()
