"""``/embeddings`` slash command for the AMX interactive CLI.

Lets users inspect and change the search-index embedding provider
(``cfg.embedding``) without hand-editing ``~/.amx/config.yml``. The
underlying providers are defined in :mod:`amx.search.embeddings`; this
module is the thin user-facing layer that updates ``cfg.embedding``,
re-installs the default factory so subsequent ``/search`` queries use
the new provider, and prints the next step for the user (``/search
rebuild`` because vectors from the previous model are not reusable).
"""

from __future__ import annotations

from amx.config import (
    DEFAULT_EMBEDDING_KIND,
    SUPPORTED_EMBEDDING_KINDS,
    AMXConfig,
    EmbeddingConfig,
)
from amx.search.embeddings import (
    DEFAULT_OPENAI_BASE_URL,
    configure_from_amx_config,
)
from amx.utils.console import (
    ask,
    ask_choice,
    ask_password,
    error,
    heading,
    info,
    success,
    warn,
)


def _print_current(cfg: AMXConfig) -> None:
    heading("Current search-index embedding provider")
    emb = cfg.embedding
    info(f"Kind:    {emb.kind}")
    info(f"Model:   {emb.model or '(none — using provider default)'}")
    if emb.kind == "openai_compatible":
        info(f"Base URL: {emb.base_url or DEFAULT_OPENAI_BASE_URL}")
        info(f"API key:  {'(set, stored in OS keyring)' if emb.api_key else '(unset)'}")
    if emb.kind == "sentence_transformers":
        info("Loads via the `local-embeddings` extra (sentence-transformers).")
    if emb.kind in {"minilm", "default", "minilm-l6-v2", ""}:
        info("MiniLM is Chroma's bundled default — no setup required.")


def _set_minilm(cfg: AMXConfig) -> None:
    cfg.embedding = EmbeddingConfig(kind="minilm")
    configure_from_amx_config(cfg, on_warning=warn)
    success("Embeddings switched to MiniLM (Chroma's bundled default).")
    info("Run /search rebuild to re-embed the catalog with the new provider.")


def _set_openai_compatible(cfg: AMXConfig, rest: list[str]) -> None:
    model = rest[0] if rest else ask(
        "Embedding model (e.g. text-embedding-3-small, text-embedding-3-large)"
    )
    if not model:
        error("A model id is required for openai_compatible embeddings.")
        return
    base_url = ask(
        "Base URL (Enter for the default OpenAI endpoint)",
        default=DEFAULT_OPENAI_BASE_URL,
    ) or DEFAULT_OPENAI_BASE_URL
    api_key = ask_password("API key (stored in your OS keyring, not the YAML)")

    cfg.embedding = EmbeddingConfig(
        kind="openai_compatible",
        model=model,
        api_key=api_key,
        base_url=base_url,
    )
    configure_from_amx_config(cfg, on_warning=warn)
    success(f"Embeddings switched to openai_compatible / {model}.")
    info(
        f"Endpoint: {base_url}. The API key is stored in the OS keyring under "
        "amx:embedding/api_key."
    )
    info("Run /search rebuild to re-embed the catalog with the new provider.")


def _set_sentence_transformers(cfg: AMXConfig, rest: list[str]) -> None:
    model = rest[0] if rest else ask(
        "HuggingFace model id (e.g. BAAI/bge-large-en-v1.5, intfloat/e5-large-v2)"
    )
    if not model:
        error("A model id is required for sentence_transformers embeddings.")
        return
    cfg.embedding = EmbeddingConfig(kind="sentence_transformers", model=model)
    configure_from_amx_config(cfg, on_warning=warn)
    success(f"Embeddings switched to sentence_transformers / {model}.")
    info(
        "Requires `pip install \"amx[local-embeddings]\"` if you have not "
        "already done so. The model will be downloaded on first /search use."
    )
    info("Run /search rebuild to re-embed the catalog with the new provider.")


def cmd_embeddings(cfg: AMXConfig, rest: list[str]) -> None:
    """Show or change the search-index embedding provider.

    Usage::

        /embeddings                        # show current provider
        /embeddings minilm                 # switch to MiniLM (default)
        /embeddings openai [model]         # switch to OpenAI-compatible
        /embeddings local [model]          # switch to sentence-transformers

    With no arguments, an interactive picker is shown. With a kind
    argument and missing details, the user is prompted for the
    remaining fields.
    """
    if not rest:
        _print_current(cfg)
        info("")
        choice = ask_choice(
            "Switch provider?",
            choices=["keep", "minilm", "openai", "local"],
            default="keep",
            descriptions={
                "keep": "leave the current provider unchanged",
                "minilm": "Chroma default (offline, fastest, lowest quality)",
                "openai": "OpenAI-compatible /embeddings endpoint",
                "local": "sentence-transformers (offline, stronger than MiniLM)",
            },
        )
        if choice in {"", "keep"}:
            return
        rest = [choice]

    head = (rest[0] or "").lower().strip()
    if head in {"minilm", "default"}:
        _set_minilm(cfg)
        return
    if head in {"openai", "openai_compatible", "openai-compatible"}:
        _set_openai_compatible(cfg, rest[1:])
        return
    if head in {"local", "sentence_transformers", "sentence-transformers", "st"}:
        _set_sentence_transformers(cfg, rest[1:])
        return
    error(
        f"Unknown embedding kind: {rest[0]!r}. Expected one of "
        f"{SUPPORTED_EMBEDDING_KINDS} (or aliases: minilm, openai, local)."
    )


# Re-export so session.py can import via the same pattern as profiles.py.
__all__ = ["cmd_embeddings", "DEFAULT_EMBEDDING_KIND"]
