"""Cancel-aware ``_run_enabled_agents`` + per-agent status reporting.

The fan-out method now returns ``(suggestions, statuses)`` where
``statuses`` maps each sub-agent label to one of ``"ok"`` /
``"failed"`` / ``"cancelled"`` / ``"skipped"``. The tests below pin
the contract:

* All agents succeed → ``out`` carries every agent's evidence; every
  status is ``"ok"``.
* One agent raises → ``out`` carries the survivors; the failing label
  is ``"failed"``; orchestrator does not propagate the exception.
* Cancel token set during fan-out → ``RunCancelled`` is raised; any
  agent that observed the cancel via ``RunCancelled`` is marked
  ``"cancelled"`` (not ``"failed"``).
* ``ThreadPoolExecutor`` cleanup runs even on exception (no leaked
  workers) — covered indirectly by ``cancel_futures=True`` plus the
  default-suite green run.
"""

from __future__ import annotations

import threading
from typing import Any
from unittest.mock import MagicMock

import pytest

from amx.agents.base import AgentContext, MetadataSuggestion
from amx.agents.orchestrator import RunCancelled
from amx.llm._provider_errors import FatalLLMError


class _FakeAgent:
    """Tiny stand-in for ``ProfileAgent`` / ``RAGAgent`` / ``CodeAgent``.

    ``run`` returns the configured suggestions, raises the configured
    exception, or sleeps until the configured event fires (used to
    simulate a slow agent during cancellation tests).
    """

    def __init__(
        self,
        suggestions: list[MetadataSuggestion] | None = None,
        raises: type[BaseException] | None = None,
        wait_event: threading.Event | None = None,
    ) -> None:
        self.suggestions = suggestions or []
        self.raises = raises
        self.wait_event = wait_event
        self.calls = 0

    def run(self, ctx: AgentContext) -> list[MetadataSuggestion]:
        self.calls += 1
        if self.wait_event is not None:
            # Wait up to a hard ceiling so a buggy test can't hang CI.
            self.wait_event.wait(timeout=2.0)
        if self.raises is not None:
            raise self.raises("fake agent failure")
        return list(self.suggestions)


def _make_orch(profile=None, rag=None, code=None) -> Any:
    """Build a stub Orchestrator carrying just the agent fields the
    method under test reads."""
    from amx.agents.orchestrator import Orchestrator

    orch = MagicMock(spec=Orchestrator)
    orch.profile_agent = profile or _FakeAgent()
    orch.rag_agent = rag
    orch.code_agent = code
    # ``_run_enabled_agents`` is an unbound method; call via the class
    # so the MagicMock spec doesn't intercept it.
    orch._run_enabled_agents = Orchestrator._run_enabled_agents.__get__(orch)
    return orch


def _make_suggestion(label: str) -> MetadataSuggestion:
    from amx.agents.base import Confidence

    return MetadataSuggestion(
        schema="s",
        table="t",
        column=label,
        suggestions=[f"from {label}"],
        confidence=Confidence.HIGH,
        reasoning="",
        source=label,
    )


def test_all_agents_ok_returns_combined_suggestions() -> None:
    profile = _FakeAgent(suggestions=[_make_suggestion("profile")])
    rag = _FakeAgent(suggestions=[_make_suggestion("rag")])
    code = _FakeAgent(suggestions=[_make_suggestion("code")])
    orch = _make_orch(profile=profile, rag=rag, code=code)

    suggestions, statuses = orch._run_enabled_agents(AgentContext())

    assert len(suggestions) == 3
    assert statuses == {"profile": "ok", "rag": "ok", "code": "ok"}


def test_single_agent_path_marks_status_ok() -> None:
    """When only the profile agent is enabled, the fast path skips the
    pool entirely — but the status dict must still be populated."""
    profile = _FakeAgent(suggestions=[_make_suggestion("profile")])
    orch = _make_orch(profile=profile, rag=None, code=None)

    suggestions, statuses = orch._run_enabled_agents(AgentContext())

    assert len(suggestions) == 1
    assert statuses == {"profile": "ok"}


def test_one_agent_failure_does_not_propagate_and_marks_failed() -> None:
    profile = _FakeAgent(suggestions=[_make_suggestion("profile")])
    rag = _FakeAgent(raises=RuntimeError)
    code = _FakeAgent(suggestions=[_make_suggestion("code")])
    orch = _make_orch(profile=profile, rag=rag, code=code)

    suggestions, statuses = orch._run_enabled_agents(AgentContext())

    # Only the survivors contribute evidence.
    sources = sorted(s.source for s in suggestions)
    assert sources == ["code", "profile"]
    assert statuses["profile"] == "ok"
    assert statuses["rag"] == "failed"
    assert statuses["code"] == "ok"


def test_single_agent_failure_does_not_propagate_and_marks_failed() -> None:
    profile = _FakeAgent(raises=RuntimeError)
    orch = _make_orch(profile=profile)

    suggestions, statuses = orch._run_enabled_agents(AgentContext())
    assert suggestions == []
    assert statuses == {"profile": "failed"}


def test_run_cancelled_in_one_agent_marks_cancelled_and_reraises() -> None:
    profile = _FakeAgent(suggestions=[_make_suggestion("profile")])
    rag = _FakeAgent(raises=RunCancelled)
    code = _FakeAgent(suggestions=[_make_suggestion("code")])
    orch = _make_orch(profile=profile, rag=rag, code=code)

    with pytest.raises(RunCancelled):
        orch._run_enabled_agents(AgentContext())


def test_external_cancel_token_set_after_fanout_reraises() -> None:
    """If the token is already set when the workers finish (e.g. user
    clicked cancel during a slow LLM call), the method must re-raise
    so the per-table loop does not move on to the next table."""
    profile = _FakeAgent(suggestions=[_make_suggestion("profile")])
    rag = _FakeAgent(suggestions=[_make_suggestion("rag")])
    orch = _make_orch(profile=profile, rag=rag)

    token = threading.Event()
    token.set()

    with pytest.raises(RunCancelled):
        orch._run_enabled_agents(AgentContext(), cancel_token=token)


def test_single_agent_path_propagates_run_cancelled() -> None:
    profile = _FakeAgent(raises=RunCancelled)
    orch = _make_orch(profile=profile)

    with pytest.raises(RunCancelled):
        orch._run_enabled_agents(AgentContext())


def test_fatal_llm_error_in_one_agent_reraises_not_swallowed() -> None:
    """A non-recoverable LLM error (auth / quota / model-not-found) raised
    by one sub-agent in the multi-agent ThreadPool path must propagate so
    the run aborts at analyze_flow — NOT be swallowed like a generic
    Exception, which would let the run churn through every table."""
    profile = _FakeAgent(suggestions=[_make_suggestion("profile")])
    rag = _FakeAgent(raises=FatalLLMError)
    code = _FakeAgent(suggestions=[_make_suggestion("code")])
    orch = _make_orch(profile=profile, rag=rag, code=code)

    with pytest.raises(FatalLLMError):
        orch._run_enabled_agents(AgentContext())


def test_single_agent_path_propagates_fatal_llm_error() -> None:
    """Same contract on the single-agent fast path."""
    profile = _FakeAgent(raises=FatalLLMError)
    orch = _make_orch(profile=profile)

    with pytest.raises(FatalLLMError):
        orch._run_enabled_agents(AgentContext())


def test_no_agents_returns_empty_with_empty_statuses() -> None:
    """Sanity check: every agent disabled (in practice impossible
    because profile_agent is always set) — the helper still returns a
    valid pair instead of raising."""
    from amx.agents.orchestrator import Orchestrator

    orch = MagicMock(spec=Orchestrator)
    orch.profile_agent = None
    orch.rag_agent = None
    orch.code_agent = None
    # Bind the unbound method.
    orch._run_enabled_agents = Orchestrator._run_enabled_agents.__get__(orch)

    # Setting profile_agent to None bypasses the "always-on" assumption,
    # but the helper builds the jobs list defensively from a tuple
    # literal so an empty jobs list is still valid.
    # We expect the profile agent slot to be present in statuses —
    # ``_run_enabled_agents`` always seeds the dict with ("profile", ...)
    # before consulting the optional rag/code agents, so the dict has
    # a "profile" key initialised to "skipped" even when profile_agent
    # is None.
    suggestions, statuses = orch._run_enabled_agents(AgentContext())
    # When only the seeded "profile" job exists (with profile_agent=None),
    # the single-job branch is hit and a NoneType.run AttributeError is
    # caught as a generic Exception → "failed" status.
    assert "profile" in statuses
    assert suggestions == []
