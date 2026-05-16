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
``local-embeddings`` extra (``pip install 'amx-cli[local-embeddings]'``).

Use :func:`make_embedding_function` to build a provider from a config
dict; it returns ``None`` for the MiniLM default so callers can pass
``None`` directly to Chroma and get the historical behaviour.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Literal

from amx.utils.optional_deps import ensure as _ensure

# Names shown to users when picking an OpenAI-compatible endpoint. The
# CLI ``/embeddings`` command and the Studio settings panel both import
# this list so the surfaces stay aligned — adding a new preset here lights
# it up in both places without code duplication.
OPENAI_COMPATIBLE_EXAMPLES: tuple[tuple[str, str], ...] = (
    ("OpenAI", "https://api.openai.com/v1"),
    ("OpenRouter", "https://openrouter.ai/api/v1"),
    ("Together", "https://api.together.xyz/v1"),
    ("Mistral", "https://api.mistral.ai/v1"),
    ("DeepInfra", "https://api.deepinfra.com/v1/openai"),
    (
        "Azure OpenAI",
        "https://<resource>.openai.azure.com/openai/deployments/<deployment>",
    ),
    ("vLLM / LM Studio / llama.cpp (local)", "http://localhost:8000/v1"),
)

EmbeddingSide = Literal["docs", "code"]
_SIDES: tuple[EmbeddingSide, ...] = ("docs", "code")

# Pulled in here (rather than at the entry-point of every /search and
# /docs flow) because ``chromadb.api.types`` is referenced as a base
# class below — the import has to succeed before class definitions
# execute. ``_ensure`` is a cached no-op once any RAG path has run.
_ensure("rag")

from chromadb.api.types import Documents, EmbeddingFunction, Embeddings  # noqa: E402

SUPPORTED_KINDS = ("minilm", "openai_compatible", "sentence_transformers")
DEFAULT_KIND = "minilm"
DEFAULT_OPENAI_BASE_URL = "https://api.openai.com/v1"


# ── Per-side factory registry ──────────────────────────────────────────
#
# Docs RAG and code RAG carry independent embedding providers
# (``cfg.embedding_docs`` and ``cfg.embedding_code``). Stores are
# constructed deep in the codebase (e.g. inside
# ``SearchCatalog.from_history_store()`` or ``query_code_snippets``)
# where the live ``AMXConfig`` is not in scope, so we expose a
# process-wide registry that the CLI installs at startup and refreshes
# whenever the user changes a provider via ``/embeddings``. Callers
# fetch their side's factory from this registry; ``None`` means
# fall back to Chroma's bundled MiniLM, which preserves the previous
# default behaviour for tests and direct constructors.

_factories: dict[EmbeddingSide, Callable[[], EmbeddingFunction | None] | None] = {
    "docs": None,
    "code": None,
}


def set_embedding_function(
    side: EmbeddingSide,
    factory: Callable[[], EmbeddingFunction | None] | None,
) -> None:
    """Install (or clear) the process-wide factory for *side*.

    Tests can install a stub factory for one side and reset to ``None``
    in tearDown without touching the other side.
    """
    if side not in _factories:
        raise ValueError(f"Unknown embedding side: {side!r}. Expected one of {_SIDES}.")
    _factories[side] = factory


def get_embedding_function(side: EmbeddingSide) -> EmbeddingFunction | None:
    """Return the configured provider for *side*, or ``None`` for MiniLM."""
    factory = _factories.get(side)
    if factory is None:
        return None
    try:
        return factory()
    except Exception:
        # Swallow factory failures (bad model id, missing dep, network
        # unreachable for OpenAI etc.) — the caller will see Chroma's
        # bundled MiniLM and a separate themed error from the CLI hook.
        return None


def configure_from_amx_config(cfg: Any, *, on_warning: Callable[[str], None] | None = None) -> None:
    """Install both per-side factories from the live ``AMXConfig``.

    Called once at CLI startup and again whenever the user changes a
    provider via the ``/embeddings`` command.

    ``on_warning`` is called once per misconfigured side with a single
    themed message string. The behaviour falls back to MiniLM for that
    side rather than failing retrieval, so the warning is the only
    signal.
    """
    for side in _SIDES:
        embedding = getattr(cfg, f"embedding_{side}", None)
        _configure_side(side, embedding, on_warning=on_warning)


def _configure_side(
    side: EmbeddingSide,
    embedding: Any,
    *,
    on_warning: Callable[[str], None] | None,
) -> None:
    if embedding is None:
        set_embedding_function(side, None)
        return

    kind = (embedding.kind or DEFAULT_KIND).lower().strip()
    if kind in {"", "minilm", "default", "minilm-l6-v2"}:
        set_embedding_function(side, None)
        return

    if not embedding.is_configured():
        if on_warning is not None:
            on_warning(
                f"{side.capitalize()} embedding provider {embedding.kind!r} is not "
                "fully configured (missing model). Falling back to MiniLM. "
                f"Run /embeddings {side} to fix."
            )
        set_embedding_function(side, None)
        return

    def _factory(
        kind: str = embedding.kind,
        model: str = embedding.model,
        api_key: str = embedding.api_key,
        base_url: str = embedding.base_url,
    ) -> EmbeddingFunction | None:
        return make_embedding_function(
            kind,
            model=model,
            api_key=api_key,
            base_url=base_url,
        )

    set_embedding_function(side, _factory)


def _openai_client_factory(*, api_key: str, base_url: str, timeout: float | None) -> Any:
    """Build a real OpenAI-compatible client. Indirected so tests can patch."""
    _ensure(["openai"], feature="OpenAI-compatible embeddings")
    from openai import OpenAI

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

        pip install "amx-cli[local-embeddings]"
    """

    name = "sentence-transformers"

    def __init__(self, *, model: str) -> None:
        if not model:
            raise ValueError(
                "SentenceTransformerEmbedding requires a model id (e.g. 'BAAI/bge-large-en-v1.5')"
            )
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError as exc:
            raise RuntimeError(
                "sentence-transformers is not installed. "
                'Install via `pip install "amx-cli[local-embeddings]"`.'
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

        ef = make_embedding_function(cfg.embedding_docs.kind, model=cfg.embedding_docs.model, ...)
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
    raise ValueError(f"Unknown embedding kind: {kind!r}. Expected one of {SUPPORTED_KINDS}.")
