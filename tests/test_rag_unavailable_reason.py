"""``RAGStore`` init failure during /run is surfaced, not swallowed.

Before PR A the analyze flow and the headless inference entrypoint
both wrapped the ``RAGStore(...)`` constructor in
``except Exception: pass``. The run then proceeded with no document
context and the user was never told.

PR A replaces both ``except: pass`` blocks with an ``error(...)``
console line + a ``rag_unavailable_reason`` key on the run record's
``metrics_json`` dict so post-run summaries can render
"No RAG context used (reason: ...)" instead of nothing.
"""

from __future__ import annotations


def test_rag_unavailable_reason_helper_records_message_on_metrics() -> None:
    """The helper that owns the reason-recording side effect can be
    called directly so the analyze-flow integration doesn't need a
    full orchestrator stand-up to be tested."""
    from amx.cli_support.commands.analyze_flow import _record_rag_unavailable_reason

    sink: dict[str, str] = {}
    _record_rag_unavailable_reason(
        sink,
        RuntimeError("collection corrupt"),
    )
    assert "rag_unavailable_reason" in sink
    assert "RuntimeError" in sink["rag_unavailable_reason"]
    assert "collection corrupt" in sink["rag_unavailable_reason"]


def test_inference_helper_records_reason_too() -> None:
    """The headless library entrypoint uses the same helper so the
    two code paths can never drift on what they record."""
    from amx.core.inference import _format_rag_unavailable_reason

    msg = _format_rag_unavailable_reason(ValueError("not a directory"))
    assert "ValueError" in msg
    assert "not a directory" in msg
