"""Pin the ``_tool_describe_run`` payload shape for applied-state surfaces.

When the chat assistant calls ``describe_run`` after a partial-apply run,
the response must expose enough structure for the LLM to answer "which
columns applied?" without guessing — a regression here caused run #66 to
list six columns and leave the user without an actionable answer.

Two payload additions are pinned:

* per-row ``applied`` bool and ``applied_at`` epoch on every ``results`` entry
* top-level ``applied_columns`` pre-filtered list (the LLM quotes this
  verbatim for partial-apply runs)
"""

from __future__ import annotations

from typing import Any

import pytest

import amx.storage.sqlite_store as sqlite_store_module
from amx.search.agent_tools import ToolBox


class _FakeHistoryStore:
    """Minimal stub that satisfies ``_tool_describe_run`` reads."""

    def __init__(self, run: dict[str, Any], results: list[dict[str, Any]]) -> None:
        self._run = run
        self._results = results

    def get_run(self, run_id: int) -> dict[str, Any] | None:  # noqa: ARG002
        return self._run

    def get_run_results(self, run_id: int) -> list[dict[str, Any]]:  # noqa: ARG002
        return self._results


def _run_row(status: str, applied_count: int) -> dict[str, Any]:
    return {
        "status": status,
        "applied_count": applied_count,
        "processed_count": 6,
        "selected_count": 6,
        "planned_count": 6,
        "command": "analyze.run",
        "mode": "auto",
        "scope_json": {},
        "db_profile": "local-postgre",
        "db_backend": "postgresql",
        "llm_profile": "gpt-5",
        "llm_provider": "openai",
        "llm_model": "gpt-5",
        "doc_profile": "",
        "code_profile": "",
        "settings_json": {},
        "metrics_json": {},
        "tokens_json": {},
        "review_strategy": "interactive",
        "error_text": "",
        "started_at": 1747225000.0,
        "ended_at": 1747225036.0,
        "duration_sec": 36.0,
    }


def _result_row(
    column: str,
    *,
    applied: bool,
    chosen_description: str,
) -> dict[str, Any]:
    return {
        "schema_name": "app_store",
        "table_name": "user_reviews",
        "column_name": column,
        "asset_kind": "column",
        "source": "llm",
        "confidence": "high",
        "logprob_score": -0.123,
        "token_count": 42,
        "model_version": "gpt-5",
        "chosen_description": chosen_description,
        "evaluation": "accepted" if applied else "",
        "alternatives_json": ["alt 1", "alt 2"],
        "db_applied_status": "applied" if applied else "",
        "applied_at": 1747225056.7 if applied else None,
    }


@pytest.fixture
def patched_history_store(monkeypatch: pytest.MonkeyPatch) -> Any:
    """Install a fake ``history_store()`` factory and return a setter."""

    holder: dict[str, _FakeHistoryStore] = {}

    def _factory() -> _FakeHistoryStore | None:
        return holder.get("store")

    monkeypatch.setattr(sqlite_store_module, "history_store", _factory)

    def _install(store: _FakeHistoryStore) -> None:
        holder["store"] = store

    return _install


def _toolbox() -> ToolBox:
    # The describe_run path only touches ``self`` for ``_tool_describe_run``
    # bookkeeping (no other attributes used), so an empty instance suffices.
    return ToolBox.__new__(ToolBox)


def test_describe_run_adds_applied_flag_and_at(patched_history_store: Any) -> None:
    patched_history_store(
        _FakeHistoryStore(
            run=_run_row("applied_partial", applied_count=1),
            results=[
                _result_row("App", applied=False, chosen_description=""),
                _result_row(
                    "Sentiment_Confidence",
                    applied=True,
                    chosen_description="A numerical score representing model confidence.",
                ),
            ],
        )
    )

    out = _toolbox()._tool_describe_run(run_id=66, include_results=True)

    assert out["status"] == "applied_partial"
    assert out["applied_count"] == 1
    assert out["results_count"] == 2

    by_column = {row["column"]: row for row in out["results"]}
    assert by_column["App"]["applied"] is False
    assert by_column["App"]["applied_at"] is None
    assert by_column["Sentiment_Confidence"]["applied"] is True
    assert by_column["Sentiment_Confidence"]["applied_at"] == 1747225056.7


def test_describe_run_precomputes_applied_columns_summary(
    patched_history_store: Any,
) -> None:
    patched_history_store(
        _FakeHistoryStore(
            run=_run_row("applied_partial", applied_count=1),
            results=[
                _result_row("App", applied=False, chosen_description=""),
                _result_row("Translated_Review", applied=False, chosen_description=""),
                _result_row(
                    "Sentiment_Confidence",
                    applied=True,
                    chosen_description="A numerical score representing model confidence.",
                ),
            ],
        )
    )

    out = _toolbox()._tool_describe_run(run_id=66, include_results=True)

    assert out["applied_columns"] == [
        {
            "schema": "app_store",
            "table": "user_reviews",
            "column": "Sentiment_Confidence",
            "chosen_description": "A numerical score representing model confidence.",
            "applied_at": 1747225056.7,
        }
    ]


def test_describe_run_applied_columns_is_empty_when_no_applies(
    patched_history_store: Any,
) -> None:
    patched_history_store(
        _FakeHistoryStore(
            run=_run_row("ready_for_review", applied_count=0),
            results=[
                _result_row("App", applied=False, chosen_description=""),
                _result_row("Sentiment", applied=False, chosen_description=""),
            ],
        )
    )

    out = _toolbox()._tool_describe_run(run_id=66, include_results=True)

    assert out["applied_columns"] == []
    assert all(row["applied"] is False for row in out["results"])
