"""Tool registry + helpers for the /ask tool-calling loop.

Split from `amx/search/agent_tools.py` so the schema↔implementation
binding has a single declared source of truth instead of relying on
implicit `getattr` dispatch on a god-object.
"""

from amx.search.tools.registry import TOOLS, ToolBinding, get_binding

__all__ = ["TOOLS", "ToolBinding", "get_binding"]
