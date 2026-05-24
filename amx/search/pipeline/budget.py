"""Token-budget enforcement for the /ask tool loop.

Lives in its own module so future pipeline stages
(``plan.py``, ``retrieve.py``, ``synthesize.py``) can call the same
helpers without importing the entire ``tool_agent.py`` module.

The truncation policy is: replace the oldest tool-result message
contents in place with a fixed JSON sentinel until the estimated
token cost falls under the budget. Newer tool results stay intact so
the model still sees its latest evidence.

The sentinel (``TRUNCATED_TOOL_PAYLOAD``) is plain JSON so the model
parses it identically to any normal result envelope — no special
handling required at the model side.
"""

from __future__ import annotations

from typing import Any

from amx.utils.token_tracker import estimate_tokens

# Sentinel content that replaces a truncated tool result. Plain JSON
# so the model parses it identically to any normal result envelope.
TRUNCATED_TOOL_PAYLOAD = '{"truncated": true, "reason": "context budget reached"}'


def enforce_input_token_budget(
    messages: list[dict[str, Any]],
    *,
    budget: int,
) -> bool:
    """Truncate oldest tool-result contents in-place until the estimated
    token cost of ``messages`` falls under ``budget``. Returns ``True``
    when any truncation occurred, ``False`` when the prompt already fit.

    Iteration order is oldest-first so the model retains its most
    recent evidence. Re-running on an already-truncated message list
    is a no-op (the sentinel content is recognised and skipped).
    """
    if estimate_tokens(messages) <= budget:
        return False
    truncated_any = False
    for msg in messages:
        if msg.get("role") != "tool":
            continue
        if msg.get("content") == TRUNCATED_TOOL_PAYLOAD:
            continue
        msg["content"] = TRUNCATED_TOOL_PAYLOAD
        truncated_any = True
        if estimate_tokens(messages) <= budget:
            return True
    return truncated_any
