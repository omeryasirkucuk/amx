"""Pluggable embedding providers for the AMX search index.

Chroma ships a default sentence-transformers model (``all-MiniLM-L6-v2``,
6M parameters, 384 dimensions) that AMX has been using implicitly. It is
fast and offline but its semantic precision is the weakest of the three
options users have asked for. This module exposes three swap-in
embedding providers wrapped as Chroma ``EmbeddingFunction`` instances:

* :class:`MiniLMEmbedding` — the Chroma default, kept here so it is
  named explicitly and can be replaced without code changes.
* :class:`OpenAICompatibleEmbedding` — points at any OpenAI-compatible
  ``/embeddings`` endpoint (real OpenAI, Azure OpenAI, Together,
  Mistral, vLLM, LM Studio, llama.cpp server, etc.). Quality is
  governed by the chosen ``model``; cost is per-token.
* :class:`SentenceTransformerEmbedding` — accepts any HuggingFace
  sentence-transformers model id (``BAAI/bge-large-en-v1.5``,
  ``intfloat/e5-large-v2``, …). Stays offline; first run downloads
  the weights via ``sentence-transformers``.

The ``sentence-transformers`` dependency is optional and only required
for :class:`SentenceTransformerEmbedding`. Install via the
``local-embeddings`` extra (``pip install 'amx[local-embeddings]'``).

Use :func:`make_embedding_function` to build a provider from a config
dict; it returns ``None`` for the MiniLM default so callers can pass
``None`` directly to Chroma and get the historical behaviour.
"""

from __future__ import annotations

from typing import Any, Callable

from chromadb.api.types import Documents, EmbeddingFunction, Embeddings


SUPPORTED_KINDS = ("minilm", "openai_compatible", "sentence_transformers")
DEFAULT_KIND = "minilm"
DEFAULT_OPENAI_BASE_URL = "https://api.openai.com/v1"


# ── Default-provider singleton ────────────────────────────────────────
#
# ``SearchIndex`` is constructed deep in the codebase (e.g. inside
# ``SearchCatalog.from_history_store()``) where the live ``AMXConfig``
# is not in scope. To avoid plumbing ``cfg`` through every caller we
# expose a process-wide factory that the CLI installs at startup based
# on ``cfg.embedding``. ``SearchIndex.__init__`` falls back to this
# factory when no explicit ``embedding_function`` is passed, preserving
# the previous default-MiniLM behaviour for direct constructors that
# do not provide one (notably the test suite).

_default_factory: Callable[[], EmbeddingFunction | None] | None = None


def set_default_embedding_function(
    factory: Callable[[], EmbeddingFunction | None] | None,
) -> None:
    """Install (or clear) the process-wide default embedding factory.

    The CLI calls this once at startup with a closure that builds the
    provider configured in ``cfg.embedding``. Tests can install a stub
    factory and reset to ``None`` in tearDown.
    """
    global _default_factory
    _default_factory = factory


def get_default_embedding_function() -> EmbeddingFunction | None:
    """Return the configured default provider, or ``None`` for MiniLM."""
    factory = _default_factory
    if factory is None:
        return None
    try:
        return factory()
    except Exception:
        # Swallow factory failures (bad model id, missing dep, network
        # unreachable for OpenAI etc.) — the caller will see Chroma's
        # bundled MiniLM and a separate themed error from the CLI hook.
        return None


def _openai_client_factory(
    *, api_key: str, base_url: str, timeout: float | None
) -> Any:
    """Build a real OpenAI-compatible client. Indirected so tests can patch."""
    try:
        from openai import OpenAI
    except ImportError as exc:  # pragma: no cover — openai is a hard dep
        raise RuntimeError(
            "The `openai` package is required for OpenAI-compatible embeddings."
        ) from exc
    return OpenAI(api_key=api_key, base_url=base_url, timeout=timeout)


class MiniLMEmbedding(EmbeddingFunction):
    """Explicit wrapper for Chroma's default ``all-MiniLM-L6-v2`` model.

    Equivalent to passing ``embedding_function=None`` to a Chroma
    collection but exposes the choice in the codebase rather than
    relying on a silent default.
    """

    name = "minilm-l6-v2"
    dim = 384

    def __init__(self) -> None:
        try:
            from chromadb.utils import embedding_functions

            self._inner: EmbeddingFunction = (
                embedding_functions.DefaultEmbeddingFunction()  # type: ignore[assignment]
            )
        except Exception as exc:  # pragma: no cover — chroma is a hard dep
            raise RuntimeError(
                "Chroma's default embedding function could not be loaded. "
                "Reinstall amx, or pin chromadb>=0.5."
            ) from exc

    def __call__(self, input: Documents) -> Embeddings:
        return self._inner(input)


class OpenAICompatibleEmbedding(EmbeddingFunction):
    """Embeddings via any OpenAI-compatible ``/embeddings`` endpoint.

    Accepts ``base_url`` so users can plug in:

    * OpenAI proper (``https://api.openai.com/v1`` — the default);
    * Azure OpenAI (e.g. ``https://<resource>.openai.azure.com/...``);
    * Hosted compat layers (Together, Mistral, OpenRouter, …);
    * Local OpenAI-compatible servers (LM Studio, llama.cpp ``server``,
      vLLM, Text Generation Inference).

    Cost and quality are determined entirely by the chosen ``model``.
    Falls back to a clear ``RuntimeError`` if the ``openai`` SDK is
    missing or the request fails, so the caller can route to a usable
    provider rather than opaque retrieval failures.
    """

    name = "openai-compatible"

    def __init__(
        self,
        *,
        model: str,
        api_key: str = "",
        base_url: str = DEFAULT_OPENAI_BASE_URL,
        timeout: float | None = 60.0,
    ) -> None:
        if not model:
            raise ValueError("OpenAICompatibleEmbedding requires a non-empty model name")
        # Indirected through ``_openai_client_factory`` so tests can patch the
        # constructor (`amx.search.embeddings._openai_client_factory`) without
        # having to wrestle with the openai SDK or the network.
        self._model = model
        self._client = _openai_client_factory(
            api_key=api_key or "no-api-key-required-for-local",
            base_url=base_url or DEFAULT_OPENAI_BASE_URL,
            timeout=timeout,
        )

    def __call__(self, input: Documents) -> Embeddings:
        texts = list(input)
        if not texts:
            return []
        response = self._client.embeddings.create(
            model=self._model,
            input=texts,
        )
        return [item.embedding for item in response.data]


class SentenceTransformerEmbedding(EmbeddingFunction):
    """Embeddings via a local ``sentence-transformers`` model.

    Accepts any HuggingFace model id understood by
    ``SentenceTransformer(...)``. Loads the weights into memory on
    first call; subsequent calls reuse the cached model. Designed for
    users who want stronger embeddings than MiniLM without making
    external API calls.

    Requires the ``local-embeddings`` extra::

        pip install "amx[local-embeddings]"
    """

    name = "sentence-transformers"

    def __init__(self, *, model: str) -> None:
        if not model:
            raise ValueError(
                "SentenceTransformerEmbedding requires a model id "
                "(e.g. 'BAAI/bge-large-en-v1.5')"
            )
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError as exc:
            raise RuntimeError(
                "sentence-transformers is not installed. "
                'Install via `pip install "amx[local-embeddings]"`.'
            ) from exc
        self._model_id = model
        self._st = SentenceTransformer(model)

    def __call__(self, input: Documents) -> Embeddings:
        texts = list(input)
        if not texts:
            return []
        # convert_to_numpy then .tolist() keeps the return type as plain
        # python lists, which Chroma stores without re-encoding.
        return self._st.encode(texts, convert_to_numpy=True).tolist()


def make_embedding_function(
    kind: str = DEFAULT_KIND,
    *,
    model: str = "",
    api_key: str = "",
    base_url: str = "",
    **_: Any,
) -> EmbeddingFunction | None:
    """Build a Chroma ``EmbeddingFunction`` for *kind*.

    Returns ``None`` for the MiniLM default so callers can hand the
    result straight to ``Chroma.get_or_create_collection`` without
    having to special-case the default path::

        ef = make_embedding_function(cfg.embedding.kind, model=cfg.embedding.model, ...)
        kwargs = {"embedding_function": ef} if ef is not None else {}
        collection = client.get_or_create_collection(name="amx_search", **kwargs)

    Unknown kinds raise :class:`ValueError`; unsupported configurations
    (missing model id, missing optional dep) raise :class:`RuntimeError`
    with a remediation hint, so the calling layer can show a themed
    error rather than a stack trace.
    """
    normalised = (kind or DEFAULT_KIND).lower().strip()
    if normalised in {"", "minilm", "default", "minilm-l6-v2"}:
        return None  # Chroma's bundled default is the desired behaviour.
    if normalised == "openai_compatible":
        return OpenAICompatibleEmbedding(
            model=model,
            api_key=api_key,
            base_url=base_url or DEFAULT_OPENAI_BASE_URL,
        )
    if normalised == "sentence_transformers":
        return SentenceTransformerEmbedding(model=model)
    raise ValueError(
        f"Unknown embedding kind: {kind!r}. Expected one of {SUPPORTED_KINDS}."
    )
