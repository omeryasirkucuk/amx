"""Explicit registry binding LLM tool schemas to ToolBox methods.

Replaces the implicit ``getattr(self, f"_tool_{name}")`` dispatch in
:meth:`amx.search.agent_tools.ToolBox.invoke` with a declared mapping
so schema↔implementation drift surfaces as a deterministic test
failure (``test_tool_registry.py``) instead of a runtime
``{"error": "Unknown tool: …"}`` envelope the LLM has to puzzle out.

The registry also carries a ``side_effect`` flag per tool — the only
non-pure tool today is ``describe_table`` (writes table metadata to
the history cache with a 24-hour TTL). Future PRs in the /ask refactor
(see ``moonlit-snacking-quokka.md`` plan) thread this flag through the
agent prompt so the LLM knows when calling a tool has consequences
beyond returning data.
"""

from __future__ import annotations

from typing import NamedTuple


class ToolBinding(NamedTuple):
    """One tool's schema/handler binding.

    ``handler_method`` is the attribute name looked up on
    :class:`amx.search.agent_tools.ToolBox` (or one of its mixins).
    Today every tool follows the ``_tool_{schema_name}`` convention;
    storing the resolved name lets a future PR rename either side
    independently without breaking the dispatch.
    """

    handler_method: str
    side_effect: bool = False


# Tools that mutate persistent state. Everything else is read-only
# and defaults to ``side_effect=False``. Keep this set small — when
# in doubt, audit the implementation and add a regression test before
# flipping the flag.
_SIDE_EFFECT_TOOLS: frozenset[str] = frozenset(
    {
        # `_tool_describe_table` calls `_writeback_table_metadata`
        # which persists the live-probe result into the history store
        # with a 24h TTL (see agent_tools.py:482-530).
        "describe_table",
    }
)


def _build_tools() -> dict[str, ToolBinding]:
    """Build the registry from the canonical schema list.

    Iterating ``tool_schemas()`` means every schema entry
    automatically participates; missing or renamed schemas are caught
    by ``test_every_schema_is_registered``.
    """
    from amx.search._tool_schemas import tool_schemas

    out: dict[str, ToolBinding] = {}
    for entry in tool_schemas():
        name = str(entry.get("function", {}).get("name") or "")
        if not name:
            continue
        out[name] = ToolBinding(
            handler_method=f"_tool_{name}",
            side_effect=name in _SIDE_EFFECT_TOOLS,
        )
    return out


TOOLS: dict[str, ToolBinding] = _build_tools()


def get_binding(name: str) -> ToolBinding | None:
    """Look up a tool's binding by schema name; returns ``None`` for
    unknown names so the caller can return its own structured error
    envelope rather than raising."""
    return TOOLS.get(name)
