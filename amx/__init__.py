"""AMX — Agentic Metadata Extractor."""

__version__ = "0.11.0a1"

__all__ = [
    "AMXApplication",
    "AbstractEntity",
    "UniversalMetadataAdapter",
    "__version__",
    "init",
]


def init(config_path: str | None = None):
    """Initialize AMX as a headless library application."""
    from amx.core.application import AMXApplication

    return AMXApplication.load(config_path)


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
