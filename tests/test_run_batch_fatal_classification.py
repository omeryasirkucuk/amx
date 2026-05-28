"""``run_batch`` maps fatal provider errors into ``FatalLLMError``.

Auth / quota / model-not-found surfaced by a batch ``submit`` are just
as non-recoverable as in chat mode. ``run_batch`` must classify them so
the ``analyze_flow`` handler aborts the run with one actionable message,
instead of leaking a raw provider error past the ``FatalLLMError``-only
catch (where it would crash with a stack trace and no guidance).
"""

from __future__ import annotations

import pytest

import amx.llm.batch as batch
from amx.llm._provider_errors import FatalLLMError


class _FakeProvider:
    """Batch provider whose ``submit`` always raises the given error."""

    def __init__(self, exc: BaseException) -> None:
        self._exc = exc

    def submit(self, requests: list[batch.BatchRequest]) -> dict:
        raise self._exc


def _requests() -> list[batch.BatchRequest]:
    return [batch.BatchRequest(custom_id="c1", messages=[{"role": "user", "content": "hi"}])]


def test_run_batch_maps_auth_error_to_fatal(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        batch,
        "get_batch_provider",
        lambda cfg: _FakeProvider(Exception("Error code: 401 - invalid api key")),
    )
    with pytest.raises(FatalLLMError) as excinfo:
        batch.run_batch(_requests(), object())  # type: ignore[arg-type]
    # The user-facing message must be actionable (point at /llm).
    assert "/llm" in excinfo.value.user_message


def test_run_batch_passes_through_already_classified_fatal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sentinel = FatalLLMError("already classified")
    monkeypatch.setattr(batch, "get_batch_provider", lambda cfg: _FakeProvider(sentinel))
    with pytest.raises(FatalLLMError) as excinfo:
        batch.run_batch(_requests(), object())  # type: ignore[arg-type]
    assert excinfo.value is sentinel


def test_run_batch_reraises_non_fatal_runtime_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Operational batch failures (job status, parse errors) are NOT fatal
    # LLM errors and must propagate unchanged, not be relabelled.
    err = RuntimeError("Batch job ended with status 'failed'.")
    monkeypatch.setattr(batch, "get_batch_provider", lambda cfg: _FakeProvider(err))
    with pytest.raises(RuntimeError) as excinfo:
        batch.run_batch(_requests(), object())  # type: ignore[arg-type]
    assert not isinstance(excinfo.value, FatalLLMError)
    assert excinfo.value is err
