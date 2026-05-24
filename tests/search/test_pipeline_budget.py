"""Canonical home for token-budget enforcement.

The helper used to live in ``amx/search/tool_agent.py`` so other
modules importing it had to pull in the entire tool-loop. PR 4 of the
/ask refactor moves it to ``amx/search/pipeline/budget.py`` and
re-exports it from ``tool_agent`` for backward compatibility. This
suite exercises the new canonical location directly so a future
removal of the re-export (planned for PR 7) does not silently lose
coverage.
"""

from __future__ import annotations

import json
from typing import Any

from amx.search.pipeline.budget import (
    TRUNCATED_TOOL_PAYLOAD,
    enforce_input_token_budget,
)


def test_truncated_payload_is_parseable_json() -> None:
    """The model treats the sentinel like any other tool result, so
    it must parse as JSON without special handling."""
    parsed = json.loads(TRUNCATED_TOOL_PAYLOAD)
    assert parsed == {"truncated": True, "reason": "context budget reached"}


def test_enforce_no_op_when_under_budget() -> None:
    messages: list[dict[str, Any]] = [
        {"role": "system", "content": "system"},
        {"role": "user", "content": "hi"},
        {"role": "tool", "tool_call_id": "c1", "content": '{"matches": []}'},
    ]
    snapshot = [dict(m) for m in messages]
    assert enforce_input_token_budget(messages, budget=1_000_000) is False
    assert messages == snapshot


def test_enforce_truncates_oldest_first() -> None:
    big = json.dumps({"rows": ["x" * 100 for _ in range(2000)]})
    messages: list[dict[str, Any]] = [
        {"role": "tool", "tool_call_id": "c1", "content": big},
        {"role": "tool", "tool_call_id": "c2", "content": '{"small": true}'},
    ]
    assert enforce_input_token_budget(messages, budget=100) is True
    assert messages[0]["content"] == TRUNCATED_TOOL_PAYLOAD


def test_tool_agent_reexports_keep_back_compat() -> None:
    """The module-level aliases in `tool_agent.py` are still exposed
    so call sites (and existing tests) that read them via the
    ``amx.search.tool_agent`` namespace keep working until the
    re-export is removed in PR 7."""
    import amx.search.tool_agent as ta

    assert ta._TRUNCATED_TOOL_PAYLOAD == TRUNCATED_TOOL_PAYLOAD
    assert ta._enforce_input_token_budget is enforce_input_token_budget
