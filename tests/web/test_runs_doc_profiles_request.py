"""PR E: ``POST /api/runs`` accepts ``doc_profiles`` as a one-shot
override of ``cfg.run_doc_profiles``.

These tests pin the body-shape contract for the new field. The
worker-side apply / revert behaviour is exercised by the existing
``test_runs_*`` suites through ``_run_worker_body``; we add a unit-
level check that the body parses cleanly and the field defaults to
``None`` so legacy callers (CLI bridges, older Studio bundles) keep
submitting valid payloads.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from amx.web.routers.runs import RunRequest


def test_run_request_accepts_doc_profiles_list() -> None:
    body = RunRequest.model_validate(
        {
            "scope": {"sales": ["orders"]},
            "doc_profiles": ["onboarding", "compliance"],
        }
    )
    assert body.doc_profiles == ["onboarding", "compliance"]


def test_run_request_doc_profiles_defaults_to_none() -> None:
    body = RunRequest.model_validate({"scope": {"sales": ["orders"]}})
    assert body.doc_profiles is None


def test_run_request_doc_profiles_empty_list_is_legal() -> None:
    """Empty list means "no docs for this run" — a valid override that
    differs semantically from ``None`` ("fall back to config")."""
    body = RunRequest.model_validate({"scope": {"sales": ["orders"]}, "doc_profiles": []})
    assert body.doc_profiles == []


def test_run_request_doc_profiles_rejects_non_string_entries() -> None:
    """Pydantic strict mode rejects a malformed payload before it
    reaches the worker so the in-memory cfg mutation is always safe."""
    with pytest.raises(ValidationError):
        RunRequest.model_validate({"scope": {"sales": ["orders"]}, "doc_profiles": [{"name": "x"}]})
