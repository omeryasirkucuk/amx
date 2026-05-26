"""Single-asset lazy ingest — selection-request mapping."""

from __future__ import annotations

from amx.lineage.native.lazy_ingest import selection_request_for


def test_notebook_selection_request():
    req = selection_request_for(profile="db", kind="notebook", external_id="123")
    assert req is not None
    assert req.profile_name == "db"
    assert req.types == ["notebooks"]
    assert req.selection == {"notebooks": ["123"]}


def test_job_selection_request():
    req = selection_request_for(profile="db", kind="job", external_id="9")
    assert req.types == ["jobs"]
    assert req.selection == {"jobs": ["9"]}


def test_unknown_kind_or_blank_id_returns_none():
    assert selection_request_for(profile="db", kind="dashboard", external_id="1") is None
    assert selection_request_for(profile="db", kind="notebook", external_id="") is None
