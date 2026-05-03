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

_LABEL_MINILM = "MiniLM"
_LABEL_OPENAI = "OpenAI-compatible"
_LABEL_LOCAL = "Local sentence-transformers"
_LABEL_CANCEL = "Cancel"

# Common OpenAI-compatible providers shown to the user as examples when they
# pick "OpenAI-compatible". The OpenAI-compatible kind already covers all of
# these — they only differ in base_url and the API-key value, which is why
# we surface the URLs here rather than adding bespoke kinds for each.
_OPENAI_COMPATIBLE_EXAMPLES: list[tuple[str, str]] = [
    ("OpenAI", "https://api.openai.com/v1"),
    ("OpenRouter", "https://openrouter.ai/api/v1"),
    ("Together", "https://api.together.xyz/v1"),
    ("Mistral", "https://api.mistral.ai/v1"),
    ("DeepInfra", "https://api.deepinfra.com/v1/openai"),
    ("Azure OpenAI", "https://<resource>.openai.azure.com/openai/deployments/<deployment>"),
    ("vLLM / LM Studio / llama.cpp (local)", "http://localhost:8000/v1"),
]


def _current_label(cfg: AMXConfig) -> str:
    emb = cfg.embedding
    if emb.kind in {"minilm", "default", "minilm-l6-v2", ""}:
        return f"{_LABEL_MINILM} — default (current)"
    if emb.kind == "openai_compatible":
        model = emb.model or "no model set"
        return f"{_LABEL_OPENAI} — {model} (current)"
    if emb.kind == "sentence_transformers":
        model = emb.model or "no model set"
        return f"{_LABEL_LOCAL} — {model} (current)"
    return f"{emb.kind} (current)"


def _print_current(cfg: AMXConfig) -> None:
    heading("Current search-index embedding provider")
    emb = cfg.embedding
    if emb.kind in {"minilm", "default", "minilm-l6-v2", ""}:
        info(
            "MiniLM (--default) — Chroma's bundled all-MiniLM-L6-v2; offline, fastest, lowest quality."
        )
        info("No setup required; this is what every fresh install starts with.")
        return
    if emb.kind == "openai_compatible":
        info("OpenAI-compatible /embeddings endpoint.")
        info(f"  Model:   {emb.model or '(unset — required)'}")
        info(f"  Base URL: {emb.base_url or DEFAULT_OPENAI_BASE_URL}")
        info(
            f"  API key:  {'(set, stored in OS keyring)' if emb.api_key else '(unset — required)'}"
        )
        return
    if emb.kind == "sentence_transformers":
        info("Local sentence-transformers (offline, stronger than MiniLM).")
        info(f"  Model: {emb.model or '(unset — required)'}")
        info('Requires `pip install "amx-cli[local-embeddings]"`.')
        return
    info(f"Kind: {emb.kind}")
    info(f"Model: {emb.model or '(unset)'}")


def _set_minilm(cfg: AMXConfig) -> None:
    cfg.embedding = EmbeddingConfig(kind="minilm")
    configure_from_amx_config(cfg, on_warning=warn)
    success("Embeddings switched to MiniLM (Chroma's bundled default).")
    info("Run /search rebuild to re-embed the catalog with the new provider.")


def _set_openai_compatible(cfg: AMXConfig, rest: list[str]) -> None:
    info(
        "OpenAI-compatible mode covers many providers — they all expose the "
        "same /embeddings shape, only the base URL and API key differ."
    )
    info("Examples:")
    for label, url in _OPENAI_COMPATIBLE_EXAMPLES:
        info(f"  • {label}: {url}")

    model = (
        rest[0]
        if rest
        else ask(
            "Embedding model id (e.g. text-embedding-3-small for OpenAI, "
            "openai/text-embedding-3-small for OpenRouter)"
        )
    )
    if not model:
        error("A model id is required for OpenAI-compatible embeddings.")
        return
    base_url = (
        ask(
            "Base URL (Enter for the default OpenAI endpoint)",
            default=DEFAULT_OPENAI_BASE_URL,
        )
        or DEFAULT_OPENAI_BASE_URL
    )
    api_key = ask_password("API key (stored in your OS keyring, not the YAML)")

    cfg.embedding = EmbeddingConfig(
        kind="openai_compatible",
        model=model,
        api_key=api_key,
        base_url=base_url,
    )
    configure_from_amx_config(cfg, on_warning=warn)
    success(f"Embeddings switched to OpenAI-compatible / {model}.")
    info(
        f"Endpoint: {base_url}. The API key is stored in the OS keyring under "
        "amx:embedding/api_key."
    )
    info("Run /rebuild (inside /search) to re-embed the catalog with the new provider.")


def _set_sentence_transformers(cfg: AMXConfig, rest: list[str]) -> None:
    info("Recommended HuggingFace embedding models (offline, stronger than MiniLM):")
    info("  • BAAI/bge-large-en-v1.5         English, 1024-dim, top-tier on MTEB")
    info("  • BAAI/bge-m3                    Multilingual, dense + sparse + ColBERT")
    info("  • intfloat/e5-large-v2           English, 1024-dim, fast")
    info("  • intfloat/multilingual-e5-large Multilingual, 1024-dim")

    model = rest[0] if rest else ask("HuggingFace model id (e.g. BAAI/bge-large-en-v1.5)")
    if not model:
        error("A model id is required for local sentence-transformers embeddings.")
        return
    cfg.embedding = EmbeddingConfig(kind="sentence_transformers", model=model)
    configure_from_amx_config(cfg, on_warning=warn)
    success(f"Embeddings switched to local sentence-transformers / {model}.")
    info(
        'Requires `pip install "amx-cli[local-embeddings]"` if you have not '
        "already done so. The model will be downloaded on first /search use."
    )
    info("Run /rebuild (inside /search) to re-embed the catalog with the new provider.")


def cmd_embeddings(cfg: AMXConfig, rest: list[str]) -> None:
    """Show or change the search-index embedding provider.

    Usage (inside /search namespace)::

        /embeddings                        # show current provider + interactive picker
        /embeddings minilm                 # switch to MiniLM (Chroma default)
        /embeddings openai [model]         # switch to any OpenAI-compatible /embeddings endpoint
        /embeddings local [model]          # switch to local sentence-transformers

    The OpenAI-compatible path covers OpenAI, OpenRouter, Together,
    Mistral, Azure OpenAI, vLLM, LM Studio, llama.cpp server, and any
    other provider that exposes the OpenAI ``/embeddings`` shape — the
    user just plugs in the matching ``base_url`` and ``api_key``.
    """
    if not rest:
        _print_current(cfg)
        info("")

        # Build the picker so the current provider is the default option,
        # labelled "(current)", and the cancel option is explicit instead of
        # the previous ambiguous "keep".
        emb_kind = (cfg.embedding.kind or "minilm").lower()
        if emb_kind in {"minilm", "default", "minilm-l6-v2", ""}:
            current_label = f"{_LABEL_MINILM} (--default, current)"
        elif emb_kind == "openai_compatible":
            current_label = f"{_LABEL_OPENAI} (current)"
        elif emb_kind == "sentence_transformers":
            current_label = f"{_LABEL_LOCAL} (current)"
        else:
            current_label = f"{cfg.embedding.kind} (current)"

        # Show the current option only if it is one of the standard kinds;
        # otherwise the user is on something custom and we still let them
        # cancel without a no-op "switch to current" entry.
        choices = [current_label, _LABEL_MINILM, _LABEL_OPENAI, _LABEL_LOCAL, _LABEL_CANCEL]
        # De-duplicate: if current is already MiniLM, drop the second MiniLM row.
        seen: set[str] = set()
        deduped: list[str] = []
        for item in choices:
            base = item.split(" (")[0]
            if base in seen and "current" not in item:
                continue
            seen.add(base)
            deduped.append(item)
        choices = deduped

        descriptions = {
            current_label: "leave the current provider unchanged",
            _LABEL_MINILM: "Chroma's bundled all-MiniLM-L6-v2 (offline, fastest, lowest quality)",
            _LABEL_OPENAI: "OpenAI / OpenRouter / Together / Mistral / Azure / vLLM / LM Studio / …",
            _LABEL_LOCAL: "any HuggingFace sentence-transformers model (offline, stronger)",
            _LABEL_CANCEL: "exit without changing the provider",
        }
        choice = ask_choice(
            "Switch provider?",
            choices=choices,
            default=current_label,
            descriptions=descriptions,
        )
        # Map the verbose label back to a kind alias.
        if not choice or choice in {current_label, _LABEL_CANCEL}:
            return
        if choice == _LABEL_MINILM:
            rest = ["minilm"]
        elif choice == _LABEL_OPENAI:
            rest = ["openai"]
        elif choice == _LABEL_LOCAL:
            rest = ["local"]
        else:
            return

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
