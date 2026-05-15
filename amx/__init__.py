"""AMX — Agentic Metadata Extractor."""

__version__ = "0.15.0"

__all__ = [
    "AMXApplication",
    "AbstractEntity",
    "UniversalMetadataAdapter",
    "__version__",
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
    raise AttributeError(name)
