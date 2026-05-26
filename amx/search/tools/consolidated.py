"""Consolidated table-discovery functions for the /ask agent.

Today the LLM sees four overlapping tools when it wants to find a
table:

* ``find_table_by_name`` — exact name match across catalog + live DB.
* ``search_tables_by_concept`` — semantic search over table descriptions.
* ``list_tables_in_schema`` — exhaustive enumeration in a single schema.
* ``search_assets`` — keyword search across the whole asset catalog.

Each call has its own argument shape and result envelope. The
decision overhead bloats the LLM's tool menu (verbose schema
descriptions explaining when to prefer which) and forces the prompt
to carry ~50 lines of routing rules.

This module is the **internal target** for consolidation. The
public-LLM-facing unified tool (``find_table(name, strategy, scope)``)
is intentionally NOT yet registered in
``amx/search/_tool_schemas.py`` — wiring it would change the LLM's
menu and routing in production, which the per-PR smoke-test gate
cannot verify in the current refactor pass. Once the gate is back
(or the unified tool is verified end-to-end), a follow-on PR flips
the schema list and removes the legacy tools.

For now the function below exists so:

1. Parity tests (``test_consolidated_find_table.py``) prove the
   dispatch produces byte-identical output to the legacy tools given
   matching arguments — the contract the future schema flip needs.
2. Internal callers (future pipeline stages, debug tooling) can use
   one entry point instead of branching on strategy.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from amx.search.agent_tools import ToolBox


Strategy = Literal["exact", "semantic", "list_in_schema"]


def find_table(
    toolbox: ToolBox,
    *,
    name: str,
    strategy: Strategy = "exact",
    scope: str = "catalog",
    limit: int = 10,
    force_fresh: bool = False,
) -> dict[str, Any]:
    """Unified table discovery — dispatches to the legacy tool that
    owns the requested strategy.

    Arguments mirror the union of the legacy tools' kwargs so a future
    schema flip is a one-line description swap rather than a
    parameter-shape migration:

    * ``strategy="exact"`` → :meth:`ToolBox._tool_find_table_by_name`.
      ``name`` is the table name to look up; ``force_fresh`` skips
      the cache layer. ``scope`` is currently advisory (legacy tool
      always sweeps both catalog and live DB).
    * ``strategy="semantic"`` →
      :meth:`ToolBox._tool_search_tables_by_concept`. ``name`` is
      treated as the concept query; ``limit`` caps results.
    * ``strategy="list_in_schema"`` →
      :meth:`ToolBox._tool_list_tables_in_schema`. ``name`` is the
      schema name; ``scope`` may carry the catalog/database hint.

    Behaviour parity is enforced by
    ``tests/search/test_consolidated_find_table.py`` so the dispatch
    can be swapped for a direct LLM-callable entry point without
    surprising the legacy callers.
    """
    target = (name or "").strip()
    if not target:
        return {"error": "Argument 'name' is required."}

    if strategy == "exact":
        return toolbox._tool_find_table_by_name(  # noqa: SLF001
            name=target, force_fresh=force_fresh
        )
    if strategy == "semantic":
        return toolbox._tool_search_tables_by_concept(  # noqa: SLF001
            concept=target, limit=limit
        )
    if strategy == "list_in_schema":
        return toolbox._tool_list_tables_in_schema(  # noqa: SLF001
            schema=target,
            catalog=scope or "",
            force_fresh=force_fresh,
        )
    return {
        "error": (f"Unknown strategy {strategy!r}. Use 'exact', 'semantic', or 'list_in_schema'."),
    }
