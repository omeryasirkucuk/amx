"""Registry-vs-schemas integrity for the /ask tool catalog.

Locks in three contracts the implicit ``getattr`` dispatch never
enforced:

1. Every schema entry in ``amx/search/_tool_schemas.py`` has a binding
   in ``amx/search/tools/registry.py:TOOLS`` — a new tool added to the
   schema list but missed by the registry fails immediately rather
   than at runtime when the LLM picks it.
2. Every registered tool has a matching handler method on
   ``ToolBox`` (or one of its mixins) — drift in the other direction
   surfaces as a clean assertion instead of an
   ``{"error": "handler not found"}`` envelope.
3. The ``side_effect`` flag is declared correctly: ``describe_table``
   (writes to history cache) is True; representative pure tools
   stay False.
"""

from __future__ import annotations

from amx.search._tool_schemas import tool_schemas
from amx.search.agent_tools import ToolBox
from amx.search.tools.registry import TOOLS, ToolBinding, get_binding


def test_every_schema_has_a_registry_binding() -> None:
    """Schemas drive the LLM's tool menu; every entry must have a
    binding so dispatch never falls through to ``Unknown tool``."""
    schema_names = {entry["function"]["name"] for entry in tool_schemas()}
    missing = schema_names - set(TOOLS)
    assert not missing, (
        f"Tools declared in schemas but missing from registry: {sorted(missing)}"
    )


def test_registry_only_contains_real_schemas() -> None:
    """A binding without a backing schema would never be invoked by
    the LLM — flag the stale entry rather than carry dead weight."""
    schema_names = {entry["function"]["name"] for entry in tool_schemas()}
    stale = set(TOOLS) - schema_names
    assert not stale, (
        f"Tools in registry but no schema declares them: {sorted(stale)}"
    )


def test_every_registered_tool_has_handler_method() -> None:
    """The dispatch path in ``ToolBox.invoke`` does
    ``getattr(self, binding.handler_method)``; a missing method would
    return an error envelope rather than execute. Lock the binding in
    place at import time."""
    missing: list[tuple[str, str]] = []
    for name, binding in TOOLS.items():
        if not hasattr(ToolBox, binding.handler_method):
            missing.append((name, binding.handler_method))
    assert not missing, (
        "Tools in registry with no handler on ToolBox: "
        + ", ".join(f"{n}→{m}" for n, m in missing)
    )


def test_describe_table_declared_side_effect() -> None:
    """``_tool_describe_table`` writes the live-probe result into the
    history cache (24h TTL). The registry must surface this so future
    PRs can warn the LLM before invocation."""
    binding = get_binding("describe_table")
    assert binding is not None
    assert binding.side_effect is True


def test_pure_read_tools_default_to_no_side_effect() -> None:
    """Representative read-only tools must not be flagged side-effect
    — a false positive would force the LLM to surface a misleading
    'this tool mutates state' notice on every catalog lookup."""
    for name in ("list_schemas", "find_table_by_name", "search_assets", "describe_column"):
        binding = get_binding(name)
        assert binding is not None, f"{name} missing from registry"
        assert binding.side_effect is False, (
            f"Pure read tool {name} should default to side_effect=False"
        )


def test_get_binding_returns_none_for_unknown_tool() -> None:
    """The dispatch path treats ``None`` from ``get_binding`` as a
    clean 'unknown tool' signal — exercise it directly so refactors
    that change the lookup contract are caught."""
    assert get_binding("__definitely_not_a_real_tool__") is None


def test_tool_binding_handler_method_follows_convention() -> None:
    """Today every binding follows the ``_tool_{name}`` convention;
    storing the resolved string is what lets a future PR rename the
    method without changing the schema. Exercise the invariant so
    accidental departures are spotted."""
    for name, binding in TOOLS.items():
        assert isinstance(binding, ToolBinding)
        assert binding.handler_method == f"_tool_{name}", (
            f"{name}: handler_method {binding.handler_method!r} does not "
            f"match _tool_{{name}} convention"
        )
