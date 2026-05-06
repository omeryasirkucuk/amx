"""Resumed chat sessions replay history into the tool agent.

The bug report: opening an old /ask chat in Studio and asking a
follow-up makes the agent forget all prior turns — references like
"that table" or "the second one" never resolve. Root cause: the
``_ask_worker`` had ``session_memory=None`` hardcoded with a TODO.

These tests pin the contract: when ``session_id`` is supplied,
``_ask_worker`` loads the recent Q/A pairs from ChatSessionStore and
forwards them to ``run_tool_agent`` so the model sees prior context.
"""

from __future__ import annotations

from amx.web.routers import ask as ask_router


def test_load_session_memory_returns_none_when_no_session() -> None:
    assert ask_router._load_session_memory(None) is None
    assert ask_router._load_session_memory(0) is None


def test_load_session_memory_returns_none_when_store_unavailable(monkeypatch) -> None:
    """No history store (fresh CLI session) → no memory, agent runs
    without prior context. Falls into the same code path as a brand-
    new session."""
    monkeypatch.setattr(ask_router, "_session_store_or_none", lambda: None)
    assert ask_router._load_session_memory(42) is None


def test_load_session_memory_drops_trailing_user_row(monkeypatch) -> None:
    """The current user question was already inserted via
    ``append_user_turn`` before the worker spawned. Forwarding it
    back into ``run_tool_agent`` would post the question twice. The
    loader trims trailing user rows to prevent that."""

    class StubStore:
        def recent_turns(self, *_args, **_kwargs):
            return [
                {"role": "user", "question": "what tables sell things?"},
                {
                    "role": "assistant",
                    "answer_summary": "sales.orders, sales.invoices, sales.returns",
                },
                {"role": "user", "question": "show me the first one"},  # current
            ]

    monkeypatch.setattr(ask_router, "_session_store_or_none", lambda: StubStore())
    memory = ask_router._load_session_memory(99)
    assert memory == [
        {"role": "user", "content": "what tables sell things?"},
        {
            "role": "assistant",
            "content": "sales.orders, sales.invoices, sales.returns",
        },
    ]


def test_load_session_memory_surfaces_summary_as_synthetic_user(monkeypatch) -> None:
    """Compaction inserts ``role='summary'`` rows that carry forward
    older context. We surface them as ``user`` messages tagged
    ``(prior conversation summary)`` so the model treats them as
    context, not as a real user turn."""

    class StubStore:
        def recent_turns(self, *_args, **_kwargs):
            return [
                {
                    "role": "summary",
                    "answer_summary": "User asked about SAP tables; we listed VBRK, VBAK, ADRC.",
                },
                {"role": "user", "question": "and the customers table?"},
                {"role": "assistant", "answer_summary": "KNA1 holds master records."},
                {"role": "user", "question": "any more?"},  # current — trimmed
            ]

    monkeypatch.setattr(ask_router, "_session_store_or_none", lambda: StubStore())
    memory = ask_router._load_session_memory(7)
    assert memory is not None
    assert memory[0]["role"] == "user"
    assert memory[0]["content"].startswith("(prior conversation summary)")
    assert memory[1] == {"role": "user", "content": "and the customers table?"}
    assert memory[2] == {
        "role": "assistant",
        "content": "KNA1 holds master records.",
    }
    assert len(memory) == 3  # trailing user trimmed


def test_load_session_memory_skips_empty_content_rows(monkeypatch) -> None:
    """Defensive — DB rows with NULL question/answer_summary don't
    show up as empty bubbles in the LLM's context."""

    class StubStore:
        def recent_turns(self, *_args, **_kwargs):
            return [
                {"role": "user", "question": ""},  # empty user
                {"role": "assistant", "answer_summary": "real answer"},
                {"role": "assistant", "answer_summary": ""},  # empty assistant
                {"role": "user", "question": "current"},  # trimmed
            ]

    monkeypatch.setattr(ask_router, "_session_store_or_none", lambda: StubStore())
    memory = ask_router._load_session_memory(1)
    assert memory == [{"role": "assistant", "content": "real answer"}]


def test_load_session_memory_returns_none_when_only_current_user_turn(
    monkeypatch,
) -> None:
    """First message of a brand-new session: the only row is the
    current user question, which gets trimmed → no history → return
    None so the agent loop skips the empty-memory branch."""

    class StubStore:
        def recent_turns(self, *_args, **_kwargs):
            return [{"role": "user", "question": "first question"}]

    monkeypatch.setattr(ask_router, "_session_store_or_none", lambda: StubStore())
    assert ask_router._load_session_memory(1) is None


def test_load_session_memory_handles_store_exception(monkeypatch) -> None:
    """recent_turns() raising (corrupt DB, transient lock, …) doesn't
    crash the worker — we return None and the agent runs without
    prior context."""

    class StubStore:
        def recent_turns(self, *_args, **_kwargs):
            raise RuntimeError("database is locked")

    monkeypatch.setattr(ask_router, "_session_store_or_none", lambda: StubStore())
    assert ask_router._load_session_memory(1) is None
