"""Chroma collection identity: what was used to build the vectors.

Every persistent Chroma collection AMX writes records three facts on
its metadata:

- ``embedding_provider`` — ``"minilm"``, ``"openai_compatible"``,
  ``"sentence_transformers"``.
- ``embedding_model`` — the model id within that provider.
- ``embedding_dim`` — the vector dimensionality, when we know it
  ahead of time. ``0`` means \"unknown / legacy collection\" and
  disables the dim half of the mismatch check (provider+model still
  apply). Recording the dimension catches the silent-corruption case
  where two providers happen to share a model name string but emit
  different-sized vectors — a real failure mode that the original
  provider+model-only check missed.

The mismatch policy:

- If the active config matches all three recorded fields → pass.
- If recorded dim is ``0`` (legacy) but provider+model match → pass
  AND backfill the dim onto the collection so future reopens get the
  stronger check.
- If recorded provider/model are missing entirely (pre-PR-B
  collection) → silently backfill all three. The grandfather rule
  from the original docs-RAG implementation.
- Anything else → raise :class:`CollectionIdentityMismatch` with a
  pipeline-specific recovery hint (``/docs reindex``,
  ``/code-refresh``, ``/search rebuild``).

The shared module exists so Catalog Search can adopt the same
identity contract — historically it recorded nothing and silently
re-embedded on provider swap (https://github.com/omeryasirkucuk/amx/issues/454
PR-B). Document RAG and Code RAG keep their own exception types as
deprecated aliases (see ``amx.docs.rag.EmbeddingProviderMismatch``,
``amx.codebase.code_rag.CodeEmbeddingMismatch``) for any external
caller that imports them by name; new code should raise the unified
:class:`CollectionIdentityMismatch` instead.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from amx.utils.logging import get_logger

log = get_logger("rag_core.collection_identity")


# Well-known embedding dimensions. The dispatch is intentionally tiny:
# the only provider whose model dimension AMX can predict offline
# without instantiating the embedding function is MiniLM. Everything
# else is reported as 0 (unknown) until the embedding function exposes
# a dimension attribute or until a future PR adds a probe call.
_KNOWN_DIMENSIONS: dict[tuple[str, str], int] = {
    ("minilm", "minilm-l6-v2"): 384,
}


@dataclass(frozen=True)
class CollectionIdentity:
    """Triple that uniquely identifies the vector space a Chroma
    collection lives in.

    The dim field is optional in the sense that ``0`` means "I don't
    know yet"; the mismatch check degrades to provider+model-only when
    either side reports ``0``.
    """

    embedding_provider: str
    embedding_model: str
    embedding_dim: int = 0

    @classmethod
    def from_active(
        cls,
        provider: str,
        model: str,
        embedding_function: Any | None = None,
    ) -> CollectionIdentity:
        """Build an identity from the active config + (optional)
        live embedding function.

        Looks up the dimension via :func:`infer_dimension`, which uses
        a static dispatch first and falls back to a ``dim`` attribute
        on the embedding function (``MiniLMEmbedding.dim``, custom
        sentence-transformers wrappers, …).
        """
        return cls(
            embedding_provider=provider,
            embedding_model=model,
            embedding_dim=infer_dimension(provider, model, embedding_function),
        )

    def to_metadata(self) -> dict[str, Any]:
        """Project the identity onto the dict shape Chroma persists in
        collection metadata. Keys are stable; renaming any of them is
        a breaking change requiring a schema bump."""
        return {
            "embedding_provider": self.embedding_provider,
            "embedding_model": self.embedding_model,
            "embedding_dim": int(self.embedding_dim),
        }


def infer_dimension(
    provider: str,
    model: str,
    embedding_function: Any | None = None,
) -> int:
    """Return the embedding dimension if known, else ``0``.

    Resolution order:

    1. Static dispatch on ``(provider, model)`` for well-known combos
       (just MiniLM today; future PRs may add cross-encoder probes).
    2. ``embedding_function.dim`` attribute (the
       :class:`amx.search.embeddings.MiniLMEmbedding` wrapper exposes
       it; custom wrappers may too).
    3. ``embedding_function._st.get_sentence_embedding_dimension()``
       for ``SentenceTransformer``-backed wrappers — the model object
       always knows its own output dim once instantiated.

    Returning ``0`` rather than raising lets the mismatch check fall
    back to provider+model comparison without breaking offline-only
    OpenAI-compat setups that would otherwise need a network probe.
    """
    static = _KNOWN_DIMENSIONS.get((provider.lower(), model.lower()))
    if static:
        return int(static)
    if embedding_function is not None:
        attr = getattr(embedding_function, "dim", None)
        if isinstance(attr, int) and attr > 0:
            return int(attr)
        inner = getattr(embedding_function, "_st", None)
        if inner is not None:
            try:
                d = int(inner.get_sentence_embedding_dimension())
            except Exception:
                d = 0
            if d > 0:
                return d
    return 0


class CollectionIdentityMismatch(RuntimeError):
    """Raised when a Chroma collection's recorded identity disagrees
    with the active config.

    The exception fields expose both the recorded and the active
    triple so callers (CLI doctor, error formatters) can render the
    diff cleanly. The ``recovery_hint`` carries the pipeline-specific
    \"how to fix\" string (`/docs reindex`, `/code-refresh`,
    `/search rebuild`).
    """

    def __init__(
        self,
        *,
        recorded: CollectionIdentity,
        active: CollectionIdentity,
        recovery_hint: str,
    ) -> None:
        self.recorded = recorded
        self.active = active
        self.recovery_hint = recovery_hint
        diff_parts: list[str] = []
        if recorded.embedding_provider != active.embedding_provider:
            diff_parts.append(
                f"provider: {recorded.embedding_provider!r} → {active.embedding_provider!r}"
            )
        if recorded.embedding_model != active.embedding_model:
            diff_parts.append(f"model: {recorded.embedding_model!r} → {active.embedding_model!r}")
        if (
            recorded.embedding_dim
            and active.embedding_dim
            and recorded.embedding_dim != active.embedding_dim
        ):
            diff_parts.append(f"dim: {recorded.embedding_dim} → {active.embedding_dim}")
        diff = "; ".join(diff_parts) or "identity changed"
        super().__init__(
            f"Vector collection was indexed with a different embedding identity "
            f"({diff}). {recovery_hint}"
        )


def _read_identity(meta: dict[str, Any]) -> CollectionIdentity | None:
    """Reconstruct a :class:`CollectionIdentity` from collection
    metadata. Returns ``None`` when provider/model are missing — the
    \"legacy collection\" case that should be backfilled, not raised
    on."""
    provider = meta.get("embedding_provider")
    model = meta.get("embedding_model")
    if not provider or not model:
        return None
    dim_raw = meta.get("embedding_dim", 0)
    try:
        dim = int(dim_raw) if dim_raw is not None else 0
    except (TypeError, ValueError):
        dim = 0
    return CollectionIdentity(
        embedding_provider=str(provider),
        embedding_model=str(model),
        embedding_dim=dim,
    )


def reconcile_identity(
    collection: Any,
    active: CollectionIdentity,
    *,
    schema_version: int,
    schema_version_key: str = "amx_schema_version",
    recovery_hint: str,
) -> None:
    """Write or verify the identity on ``collection``.

    Called once per ``RAGStore`` / ``CodeIndex`` / ``SearchIndex``
    construction, after ``get_or_create_collection``.

    Behaviour:

    - Existing collection with full identity matching active → no-op.
    - Existing collection with provider/model match but dim mismatch
      → raise.
    - Existing collection with provider/model match and dim missing
      (legacy) → backfill the dim and schema version silently.
    - Existing collection with no identity at all → backfill all three
      + the schema version.
    - First create (no metadata yet) → metadata was already set by
      ``get_or_create_collection``; nothing to do here.

    Raises :class:`CollectionIdentityMismatch` on a real conflict.
    """
    # Defensive: some tests inject fake Chroma collection objects that
    # do not expose a ``metadata`` attribute. Treat that as
    # "no identity recorded, nothing we can do" and bail without
    # raising — the rest of the test wires what it needs to.
    raw_meta = getattr(collection, "metadata", None)
    if raw_meta is None and not hasattr(collection, "modify"):
        return
    meta = dict(raw_meta or {})
    recorded = _read_identity(meta)

    if recorded is None:
        # Legacy / first-open collection: stamp identity now so future
        # reopens have something to compare against.
        _backfill(collection, meta, active, schema_version, schema_version_key)
        return

    provider_or_model_changed = (
        recorded.embedding_provider != active.embedding_provider
        or recorded.embedding_model != active.embedding_model
    )
    dim_changed = (
        recorded.embedding_dim > 0
        and active.embedding_dim > 0
        and recorded.embedding_dim != active.embedding_dim
    )
    if provider_or_model_changed or dim_changed:
        raise CollectionIdentityMismatch(
            recorded=recorded,
            active=active,
            recovery_hint=recovery_hint,
        )

    # Same provider+model, but the recorded dim is unknown (0) while
    # the active dim is now knowable — upgrade the metadata so the
    # check tightens on next reopen.
    if recorded.embedding_dim == 0 and active.embedding_dim > 0:
        _backfill(collection, meta, active, schema_version, schema_version_key)


def _backfill(
    collection: Any,
    existing_meta: dict[str, Any],
    active: CollectionIdentity,
    schema_version: int,
    schema_version_key: str,
) -> None:
    """Write the active identity + schema version onto the collection.

    Strips ``hnsw:*`` keys before calling ``collection.modify`` —
    Chroma rejects construction-time parameters in ``modify(metadata=)``
    even when the value is unchanged.
    """
    merged = {k: v for k, v in existing_meta.items() if not str(k).startswith("hnsw:")}
    merged.update(active.to_metadata())
    merged[schema_version_key] = schema_version
    modify = getattr(collection, "modify", None)
    if modify is None:
        return
    try:
        modify(metadata=merged)
    except Exception as exc:  # noqa: BLE001 — best-effort backfill
        log.warning("Could not backfill collection identity metadata: %s", exc)
