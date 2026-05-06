"""Core AMX APIs that can be used without the interactive CLI shell."""

from __future__ import annotations

__all__ = [
    "AMXApplication",
    "AbstractEntity",
    "InferenceResult",
    "StateManager",
    "UniversalMetadataAdapter",
]


def __getattr__(name: str):
    if name == "AMXApplication":
        from amx.core.application import AMXApplication

        return AMXApplication
    if name in {"AbstractEntity", "UniversalMetadataAdapter"}:
        from amx.core.metadata import AbstractEntity, UniversalMetadataAdapter

        return {
            "AbstractEntity": AbstractEntity,
            "UniversalMetadataAdapter": UniversalMetadataAdapter,
        }[name]
    if name == "StateManager":
        from amx.core.state import StateManager

        return StateManager
    if name == "InferenceResult":
        from amx.core.inference import InferenceResult

        return InferenceResult
    raise AttributeError(name)
