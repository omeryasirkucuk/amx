"""Detect the conversation's focus profile from prior assistant turns.

Pure module — no AMX-internal dependencies, no I/O. Lifted from
``amx/search/tool_agent.py`` so future pipeline stages can call the
same heuristic without importing the tool-loop module.

Heuristic: scan the last ~3 assistant turns' answer text for
``db_profile=NAME`` or ``profile NAME`` mentions. If one profile
accounts for ≥60% of mentions, return it; otherwise return ``None``
and let the LLM pick. Lightweight on purpose — we don't re-parse
tool_call traces (those aren't carried in session_memory) so the
heuristic operates on what the LLM has already said.

Single-profile scopes skip the calculation entirely (focus is
implicit).
"""

from __future__ import annotations

from typing import Any


def compute_focus_profile(
    session_memory: list[dict[str, Any]] | None,
    scope: list[str],
) -> str | None:
    """Return the profile name that dominates recent assistant turns,
    or ``None`` when there is no clear focus.

    Arguments:
        session_memory: list of ``{role, content, ...}`` chat turns
            from the active session. ``None`` or empty → no focus.
        scope: every profile currently in retrieval scope. Single-
            profile scopes return ``None`` (focus would be redundant).
    """
    if not scope or len(scope) < 2 or not session_memory:
        return None
    last_turns = [t for t in session_memory if t.get("role") == "assistant"][-3:]
    if not last_turns:
        return None
    counts: dict[str, int] = dict.fromkeys(scope, 0)
    text_blob = " ".join(str(t.get("content") or "") for t in last_turns).lower()
    for name in scope:
        # Word-boundary-ish: name surrounded by whitespace, punctuation, or quotes.
        # Cheap substring count is correct enough for short profile names.
        counts[name] = text_blob.count(name.lower())
    total = sum(counts.values())
    if total < 2:  # too few mentions → don't bias
        return None
    top_name, top_count = max(counts.items(), key=lambda kv: kv[1])
    if top_count == 0 or top_count / total < 0.60:
        return None
    return top_name
