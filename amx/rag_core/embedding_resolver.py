"""Single resolution authority for embedding models across all sides.

AMX embeds text in several subsystems — docs (which also powers catalog
search), code, and ingested assets — each with its own
``embedding_{side}`` config field and its own Chroma collection. Before
this module each side had its own ``_resolve_*_embedding`` function that
independently read config, built the embedding function, and **silently**
fell back to MiniLM when the configured model couldn't be built (e.g.
``sentence-transformers`` not installed). That silent substitution meant
a user could configure gte-small and unknowingly run MiniLM.

``resolve_embedding`` centralises that logic and — crucially — reports
whether a fallback happened (``fell_back`` + ``fallback_reason``) so
callers and the UI can surface the truth instead of a silent swap. The
per-side *default* target still differs (docs/assets fall back to plain
MiniLM; code prefers a code-specialised encoder), so each side passes
its own ``default_resolver``; the explicit-provider path and the
fallback-signalling are shared here.

The three legacy ``_resolve_*_embedding`` functions are thin wrappers
that call this and return the historical ``(provider, model,
embedding_function)`` tuple, so existing callers are unchanged.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

# A side's default/fallback resolver returns the legacy tuple
# ``(provider, model, embedding_function)``. MiniLM is represented by
# ``embedding_function=None`` (Chroma's bundled default).
DefaultResolver = Callable[[], tuple[str, str, Any | None]]

# config ``kind`` values that mean "the bundled MiniLM default".
_DEFAULT_KINDS = {"", "minilm", "default", "minilm-l6-v2"}


@dataclass(frozen=True)
class ResolvedEmbedding:
    """The outcome of resolving an embedding side, with honest provenance.

    ``configured_*`` is what the config asked for; ``active_*`` is what
    is actually used after any fallback. When they differ because the
    configured model could not be built, ``fell_back`` is True and
    ``fallback_reason`` explains why (so the UI can say "configured
    gte-small but running minilm — sentence-transformers not installed"
    instead of silently substituting).
    """

    side: str
    configured_provider: str
    configured_model: str
    active_provider: str
    active_model: str
    embedding_function: Any | None
    fell_back: bool
    fallback_reason: str | None
    dependency_available: bool

    def as_tuple(self) -> tuple[str, str, Any | None]:
        """Legacy ``(provider, model, embedding_function)`` shape that
        the historical ``_resolve_*_embedding`` callers expect — always
        the ACTIVE (post-fallback) identity, matching prior behaviour."""
        return (self.active_provider, self.active_model, self.embedding_function)


def _load_cfg(cfg: Any | None) -> Any | None:
    if cfg is not None:
        return cfg
    try:
        from amx.config import AMXConfig

        return AMXConfig.load()
    except Exception:
        return None


def resolve_embedding(
    side: str,
    cfg: Any | None,
    *,
    default_resolver: DefaultResolver,
) -> ResolvedEmbedding:
    """Resolve the embedding for ``side`` ("docs" | "code" | "assets").

    The explicit-provider path (a non-default ``kind`` with a model id)
    is built via ``make_embedding_function``; on a build failure it
    falls back to ``default_resolver()`` AND records ``fell_back`` +
    ``fallback_reason``. The default path (no config, default kind, or
    no model id) delegates to ``default_resolver()`` and is NOT a
    fallback — it's the configured intent.
    """
    from amx.search.embeddings import make_embedding_function

    cfg = _load_cfg(cfg)
    embedding = getattr(cfg, f"embedding_{side}", None) if cfg is not None else None

    def _default(reason: str | None, dep_ok: bool) -> ResolvedEmbedding:
        provider, model, ef = default_resolver()
        configured_provider = (
            (getattr(embedding, "kind", "") or "minilm").lower().strip()
            if embedding is not None
            else provider
        )
        configured_model = (getattr(embedding, "model", "") or "") if embedding is not None else model
        fell = reason is not None
        return ResolvedEmbedding(
            side=side,
            configured_provider=configured_provider or provider,
            configured_model=configured_model or model,
            active_provider=provider,
            active_model=model,
            embedding_function=ef,
            fell_back=fell,
            fallback_reason=reason,
            dependency_available=dep_ok,
        )

    if embedding is None:
        return _default(None, True)

    kind = (getattr(embedding, "kind", "") or "minilm").lower().strip()
    model = getattr(embedding, "model", "") or ""
    api_key = getattr(embedding, "api_key", "") or ""
    base_url = getattr(embedding, "base_url", "") or ""

    # Default kind, or a non-default kind with no model id picked yet →
    # the side's default. Not a fallback; this is the configured intent.
    if kind in _DEFAULT_KINDS or not model:
        return _default(None, True)

    try:
        ef = make_embedding_function(kind, model=model, api_key=api_key, base_url=base_url)
    except Exception as exc:
        # The configured model could not be built (missing optional
        # dependency, bad model id, unreachable endpoint). Fall back so
        # the feature still works, but record it loudly.
        return _default(str(exc) or exc.__class__.__name__, False)

    return ResolvedEmbedding(
        side=side,
        configured_provider=kind,
        configured_model=model,
        active_provider=kind,
        active_model=model,
        embedding_function=ef,
        fell_back=False,
        fallback_reason=None,
        dependency_available=True,
    )


def _minilm_default() -> tuple[str, str, Any | None]:
    return ("minilm", "minilm-l6-v2", None)


def resolve_side(side: str, cfg: Any | None = None) -> ResolvedEmbedding:
    """Resolve a side by name, applying that side's own default target.

    A convenience dispatcher so callers (status endpoint, health panel,
    CLI) get a full :class:`ResolvedEmbedding` — configured vs active +
    fell_back — without knowing each side's default resolver. docs and
    assets default to plain MiniLM; code defers to its jina-or-MiniLM
    default. Unknown sides default to MiniLM.
    """
    if side == "code":
        from amx.codebase.code_rag import _default_code_embedding

        return resolve_embedding("code", cfg, default_resolver=_default_code_embedding)
    return resolve_embedding(side, cfg, default_resolver=_minilm_default)
