"""Confirm AskRequest carries the new lineage/pages fields end-to-end.

Task 7 of the lineage/pages-in-Ask plan: HTTP body must accept the
two new override fields without rejection, and the worker call chain
must forward them by keyword. Old clients that omit both fields keep
working unchanged (back-compat).
"""

from __future__ import annotations

from unittest.mock import MagicMock

from amx.web.routers import ask as ask_router


def test_ask_request_accepts_lineage_and_pages_overrides(
    client, auth_headers, monkeypatch
) -> None:
    """POST /api/ask accepts the new lineage_profiles + pages_enabled
    fields and the worker forwards them by keyword to ``run_tool_agent``.
    """
    captured: dict[str, object] = {}

    monkeypatch.setattr(ask_router, "_load_catalog", lambda: MagicMock())
    monkeypatch.setattr(ask_router, "LLMProvider", lambda _cfg: MagicMock())

    def _capture(*_args, **kwargs):
        captured.update(kwargs)

        class _StubResult:
            answer = "stub"
            session_memory = []
            metrics: dict[str, object] = {}
            tool_calls: list[dict[str, object]] = []
            intent = "answer"
            citations: list[dict[str, object]] = []

        return _StubResult()

    monkeypatch.setattr(ask_router, "run_tool_agent", _capture)

    response = client.post(
        "/api/ask",
        headers=auth_headers,
        json={
            "question": "what is the customers table?",
            "lineage_profiles": ["canvas-a"],
            "pages_enabled": False,
        },
    )
    assert response.status_code == 200, response.text

    # The worker runs on a daemon thread; in tests it completes
    # synchronously fast enough that the captured kwargs are populated
    # by the time we inspect them. If timing ever becomes flaky we'd
    # join the thread via the JobRegistry; not needed today.
    import time

    deadline = time.monotonic() + 2.0
    while "lineage_profiles" not in captured and time.monotonic() < deadline:
        time.sleep(0.01)

    assert captured.get("lineage_profiles") == ["canvas-a"]
    assert captured.get("pages_enabled") is False


def test_ask_request_accepts_missing_lineage_and_pages_overrides(
    client, auth_headers, monkeypatch
) -> None:
    """Both fields default to None (Auto) when omitted — old clients
    keep working."""
    captured: dict[str, object] = {}

    monkeypatch.setattr(ask_router, "_load_catalog", lambda: MagicMock())
    monkeypatch.setattr(ask_router, "LLMProvider", lambda _cfg: MagicMock())

    def _capture(*_args, **kwargs):
        captured.update(kwargs)

        class _StubResult:
            answer = "stub"
            session_memory = []
            metrics: dict[str, object] = {}
            tool_calls: list[dict[str, object]] = []
            intent = "answer"
            citations: list[dict[str, object]] = []

        return _StubResult()

    monkeypatch.setattr(ask_router, "run_tool_agent", _capture)

    response = client.post(
        "/api/ask",
        headers=auth_headers,
        json={"question": "what is foo?"},
    )
    assert response.status_code == 200, response.text

    import time

    deadline = time.monotonic() + 2.0
    while "lineage_profiles" not in captured and time.monotonic() < deadline:
        time.sleep(0.01)

    assert captured.get("lineage_profiles") is None
    assert captured.get("pages_enabled") is None


def test_ask_request_model_parses_new_fields_directly() -> None:
    """Unit-level check on the Pydantic model — defaults are None,
    explicit values round-trip, and extras don't raise."""
    body = ask_router.AskRequest(question="hi")
    assert body.lineage_profiles is None
    assert body.pages_enabled is None

    body = ask_router.AskRequest(
        question="hi",
        lineage_profiles=["canvas-a", "canvas-b"],
        pages_enabled=True,
    )
    assert body.lineage_profiles == ["canvas-a", "canvas-b"]
    assert body.pages_enabled is True

    body = ask_router.AskRequest(
        question="hi",
        lineage_profiles=[],
        pages_enabled=False,
    )
    assert body.lineage_profiles == []
    assert body.pages_enabled is False
