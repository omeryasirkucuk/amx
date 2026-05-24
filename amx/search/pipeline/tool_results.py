"""Tool-result inspection helpers for the /ask agent.

Pure functions that examine the raw string a tool returned and pull
out flags the loop needs (e.g. ``"partial": true`` from the catalog
tools' envelope). Lives here so future pipeline stages and tests can
reach the helpers without importing the entire ``tool_agent`` module.
"""

from __future__ import annotations


def looks_partial(tool_result: str | None) -> bool:
    """Cheap textual check for the ``"partial": true`` marker on a
    tool result.

    The result is the JSON string the catalog tools return; parsing
    every result would be wasteful when only a minority carry the
    flag. ``"partial": true`` (with single or double quotes, with or
    without internal whitespace) is unambiguous enough that a
    substring match is sufficient.

    Returns ``False`` for ``None`` and empty strings so callers don't
    have to guard.
    """
    if not tool_result:
        return False
    if '"partial": true' in tool_result or '"partial":true' in tool_result:
        return True
    return "'partial': True" in tool_result or "'partial':True" in tool_result
