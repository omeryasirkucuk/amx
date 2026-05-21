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


def _raise_typeerror() -> None:
    """Helper that raises a TypeError so the traceback frame points at
    ``_raise_typeerror`` and a known line in this file. Used by both
    formatters to assert the captured location.
    """
    bad: str = "oops"
    bad()  # type: ignore[operator]  # the canonical "'str' object is not callable"


def test_record_helper_appends_traceback_location() -> None:
    """A vague ``TypeError: 'str' object is not callable`` is useless
    without a file/line — capture the last traceback frame so the run
    detail page surfaces ``at <basename>:<lineno> in <symbol>``.
    """
    from amx.cli_support.commands.analyze_flow import _record_rag_unavailable_reason

    sink: dict[str, str] = {}
    try:
        _raise_typeerror()
    except TypeError as exc:
        _record_rag_unavailable_reason(sink, exc)

    reason = sink["rag_unavailable_reason"]
    assert reason.startswith("TypeError: ")
    assert "'str' object is not callable" in reason
    # Location suffix: filename, line, symbol all present.
    assert "test_rag_unavailable_reason.py" in reason
    assert ":" in reason.split(" at ")[-1]  # has ``<basename>:<lineno>``
    assert "_raise_typeerror" in reason


def test_inference_helper_appends_traceback_location() -> None:
    """Parity with the analyze-flow helper — the headless library
    entrypoint must also append the traceback frame so /history and
    Studio render the same diagnostic for both call sites."""
    from amx.core.inference import _format_rag_unavailable_reason

    try:
        _raise_typeerror()
    except TypeError as exc:
        msg = _format_rag_unavailable_reason(exc)

    assert msg.startswith("TypeError: ")
    assert "test_rag_unavailable_reason.py" in msg
    assert "_raise_typeerror" in msg


def test_helpers_no_location_when_no_traceback() -> None:
    """A bare exception built in user code (no ``raise`` yet) has no
    traceback; the helpers must not crash trying to extract one and the
    recorded reason simply has no location suffix."""
    from amx.cli_support.commands.analyze_flow import _record_rag_unavailable_reason
    from amx.core.inference import _format_rag_unavailable_reason

    bare = TypeError("never raised")
    sink: dict[str, str] = {}
    _record_rag_unavailable_reason(sink, bare)
    assert sink["rag_unavailable_reason"] == "TypeError: never raised"
    assert _format_rag_unavailable_reason(bare) == "TypeError: never raised"
