"""Core AMX APIs that can be used without the interactive CLI shell."""

from __future__ import annotations

__all__ = [
    "AMXApplication",
    "AbstractEntity",
    "AskToolbox",
    "LoopBasedAskAgent",
    "StateManager",
    "ToolAskResponse",
    "UniversalMetadataAdapter",
    "infer_table_metadata",
]


def __getattr__(name: str):
    if name == "AMXApplication":
        from amx.core.application import AMXApplication

        return AMXApplication
    if name in {"AskToolbox", "LoopBasedAskAgent", "ToolAskResponse"}:
        from amx.core.ask_agent import AskToolbox, LoopBasedAskAgent, ToolAskResponse

        return {
            "AskToolbox": AskToolbox,
            "LoopBasedAskAgent": LoopBasedAskAgent,
            "ToolAskResponse": ToolAskResponse,
        }[name]
    if name in {"AbstractEntity", "UniversalMetadataAdapter"}:
        from amx.core.metadata import AbstractEntity, UniversalMetadataAdapter

        return {
            "AbstractEntity": AbstractEntity,
            "UniversalMetadataAdapter": UniversalMetadataAdapter,
        }[name]
    if name == "StateManager":
        from amx.core.state import StateManager

        return StateManager
    if name == "infer_table_metadata":
        from amx.core.inference import infer_table_metadata

        return infer_table_metadata
    raise AttributeError(name)
