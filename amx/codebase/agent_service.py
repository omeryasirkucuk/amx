"""Run the Code Agent across a list of tables and stream progress.

This module is the shared core both the CLI's ``/code-analyze`` and
the Studio ``POST /api/code/analyze`` endpoint dispatch into. The CLI
formerly drove the loop itself; pulling it here lets the FastAPI
worker run the same logic against the same caches without forking a
sub-process or duplicating the orchestration.

The function is intentionally callback-driven (``on_table_start``,
``on_table_done``) instead of returning a generator so the SSE event
bus can plumb status updates without us inventing a new streaming
contract.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from amx.agents.base import AgentSuggestion
    from amx.codebase.analyzer import CodebaseReport
    from amx.config import AMXConfig
    from amx.db.connector import DatabaseConnector
    from amx.llm.provider import LLMProvider


@dataclass
class CodeAnalyzeRequest:
    """One ``/code-analyze`` invocation, regardless of caller."""

    schema: str
    tables: list[str]
    code_profile: str
    code_report: CodebaseReport


@dataclass
class CodeAnalyzeResult:
    """Aggregate output from one analyze run."""

    suggestions: list[AgentSuggestion]
    by_table: dict[str, int]


TableStartCb = Callable[[str, int], None]
TableDoneCb = Callable[[str, int], None]


def run_code_analysis(
    cfg: AMXConfig,
    db: DatabaseConnector,
    llm: LLMProvider,
    request: CodeAnalyzeRequest,
    *,
    on_table_start: TableStartCb | None = None,
    on_table_done: TableDoneCb | None = None,
) -> CodeAnalyzeResult:
    """Per-table Code Agent fan-out, with progress callbacks.

    Lives here (not in the CLI) so the FastAPI ``/code/analyze`` worker
    can reuse the loop. The previous home was a Click command body
    where the Studio worker couldn't reach.
    """
    from amx.agents.base import AgentContext
    from amx.agents.code_agent import CodeAgent

    agent = CodeAgent(llm, request.code_report)
    suggestions: list[Any] = []
    by_table: dict[str, int] = {}
    for table_name in request.tables:
        columns = db.list_column_profiles(request.schema, table_name)
        if on_table_start is not None:
            on_table_start(table_name, len(columns))
        ctx = AgentContext(
            schema=request.schema,
            table=table_name,
            db_profile={
                "row_count": 0,
                "columns": [{"name": c.name, "dtype": c.dtype} for c in columns],
            },
            existing_metadata={},
        )
        produced = agent.run(ctx)
        suggestions.extend(produced)
        by_table[table_name] = len(produced)
        if on_table_done is not None:
            on_table_done(table_name, len(produced))
    # Cast back to the AgentSuggestion type for callers/typing — kept
    # as ``Any`` inside the function so the import lives under
    # TYPE_CHECKING in callers that don't need the concrete class.
    return CodeAnalyzeResult(suggestions=list(suggestions), by_table=by_table)


def serialize_suggestions(suggestions: list[Any]) -> list[dict[str, Any]]:
    """Render Code Agent suggestions as JSON-serialisable dicts.

    Same shape the CLI writes to ``~/.amx/code_agent_results.json`` so
    the Studio response stays in sync with the on-disk cache the next
    ``/run`` consumes.
    """
    return [
        {
            "schema": s.schema,
            "table": s.table,
            "column": s.column,
            "suggestions": list(s.suggestions or []),
            "confidence": s.confidence.value
            if hasattr(s.confidence, "value")
            else str(s.confidence),
            "reasoning": s.reasoning,
            "source": s.source,
        }
        for s in suggestions
    ]
