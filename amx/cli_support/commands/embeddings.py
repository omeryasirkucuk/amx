"""``/embeddings`` slash command for the AMX interactive CLI.

Lets users inspect and change the search-index embedding providers
(``cfg.embedding_docs`` for docs RAG, ``cfg.embedding_code`` for code
RAG) without hand-editing ``~/.amx/config.yml``. The underlying
providers are defined in :mod:`amx.search.embeddings`; this module is
the thin user-facing layer that updates the per-side config, re-installs
the process-wide factories so subsequent ``/search`` and code-RAG
queries use the new provider, and prints the next step for the user
(re-embed the affected catalog/collection because vectors from the
previous model are not reusable).
"""

from __future__ import annotations

from typing import Literal

from amx.config import (
    DEFAULT_EMBEDDING_KIND,
    SUPPORTED_EMBEDDING_KINDS,
    AMXConfig,
    EmbeddingConfig,
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

Side = Literal["docs", "code"]

# ``amx.search.embeddings`` pulls in chromadb at import time, which
# we don't want to load on every CLI launch. Mirror the constant
# here as a tiny duplicate (it never changes) and lazy-import the
# function inside the wrapper below so handlers can call it without
# the chromadb tax landing on the boot path.
DEFAULT_OPENAI_BASE_URL = "https://api.openai.com/v1"


def _configure_from_amx_config(*args, **kwargs):  # type: ignore[no-untyped-def]
    from amx.search.embeddings import configure_from_amx_config

    return configure_from_amx_config(*args, **kwargs)


_LABEL_MINILM = "MiniLM"
_LABEL_OPENAI = "OpenAI-compatible"
_LABEL_LOCAL = "Local sentence-transformers"
_LABEL_CANCEL = "Cancel"
_LABEL_DOCS = "Docs RAG"
_LABEL_CODE = "Code RAG"

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


def _side_field(side: Side) -> str:
    return f"embedding_{side}"


def _get_side(cfg: AMXConfig, side: Side) -> EmbeddingConfig:
    return getattr(cfg, _side_field(side))


def _set_side(cfg: AMXConfig, side: Side, value: EmbeddingConfig) -> None:
    setattr(cfg, _side_field(side), value)


def _rebuild_hint(side: Side) -> str:
    if side == "docs":
        return "Run /search rebuild to re-embed the docs catalog with the new provider."
    return "Run /code refresh to re-index the code RAG collection with the new provider."


def _side_title(side: Side) -> str:
    return _LABEL_DOCS if side == "docs" else _LABEL_CODE


def _print_current(cfg: AMXConfig, side: Side) -> None:
    heading(f"Current {_side_title(side)} embedding provider")
    emb = _get_side(cfg, side)
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


def _set_minilm(cfg: AMXConfig, side: Side) -> None:
    _set_side(cfg, side, EmbeddingConfig(kind="minilm"))
    _configure_from_amx_config(cfg, on_warning=warn)
    success(f"{_side_title(side)} embeddings switched to MiniLM (Chroma's bundled default).")
    info(_rebuild_hint(side))


def _set_openai_compatible(cfg: AMXConfig, side: Side, rest: list[str]) -> None:
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

    _set_side(
        cfg,
        side,
        EmbeddingConfig(
            kind="openai_compatible",
            model=model,
            api_key=api_key,
            base_url=base_url,
        ),
    )
    _configure_from_amx_config(cfg, on_warning=warn)
    success(f"{_side_title(side)} embeddings switched to OpenAI-compatible / {model}.")
    info(
        f"Endpoint: {base_url}. The API key is stored in the OS keyring under "
        f"amx:embedding_{side}/api_key."
    )
    info(_rebuild_hint(side))


def _set_sentence_transformers(cfg: AMXConfig, side: Side, rest: list[str]) -> None:
    info("Recommended HuggingFace embedding models (offline, stronger than MiniLM):")
    info("  • BAAI/bge-large-en-v1.5         English, 1024-dim, top-tier on MTEB")
    info("  • BAAI/bge-m3                    Multilingual, dense + sparse + ColBERT")
    info("  • intfloat/e5-large-v2           English, 1024-dim, fast")
    info("  • intfloat/multilingual-e5-large Multilingual, 1024-dim")

    model = rest[0] if rest else ask("HuggingFace model id (e.g. BAAI/bge-large-en-v1.5)")
    if not model:
        error("A model id is required for local sentence-transformers embeddings.")
        return
    _set_side(cfg, side, EmbeddingConfig(kind="sentence_transformers", model=model))
    _configure_from_amx_config(cfg, on_warning=warn)
    success(f"{_side_title(side)} embeddings switched to local sentence-transformers / {model}.")
    info(
        'Requires `pip install "amx-cli[local-embeddings]"` if you have not '
        "already done so. The model will be downloaded on first use."
    )
    info(_rebuild_hint(side))


def _pick_side(rest: list[str]) -> tuple[Side | None, list[str]]:
    """Pull the side argument from ``rest`` or prompt the user.

    Returns ``(side, remaining_args)``. ``side`` is ``None`` when the
    user cancels at the prompt.
    """
    if rest:
        head = (rest[0] or "").lower().strip()
        if head in {"docs", "doc", "rag"}:
            return ("docs", rest[1:])
        if head in {"code"}:
            return ("code", rest[1:])

    choice = ask_choice(
        "Which side?",
        choices=[_LABEL_DOCS, _LABEL_CODE, _LABEL_CANCEL],
        default=_LABEL_DOCS,
        descriptions={
            _LABEL_DOCS: "controls embeddings for the docs RAG store (/search, /ask)",
            _LABEL_CODE: "controls embeddings for the code RAG store (/code search, /code-refresh)",
            _LABEL_CANCEL: "exit without changing anything",
        },
    )
    if not choice or choice == _LABEL_CANCEL:
        return (None, rest)
    if choice == _LABEL_DOCS:
        return ("docs", rest)
    return ("code", rest)


def _pick_kind(cfg: AMXConfig, side: Side) -> str | None:
    """Show the picker for a side and return the chosen kind alias.

    ``None`` when the user cancels.
    """
    emb = _get_side(cfg, side)
    emb_kind = (emb.kind or "minilm").lower()
    if emb_kind in {"minilm", "default", "minilm-l6-v2", ""}:
        current_label = f"{_LABEL_MINILM} (--default, current)"
    elif emb_kind == "openai_compatible":
        current_label = f"{_LABEL_OPENAI} (current)"
    elif emb_kind == "sentence_transformers":
        current_label = f"{_LABEL_LOCAL} (current)"
    else:
        current_label = f"{emb.kind} (current)"

    choices = [current_label, _LABEL_MINILM, _LABEL_OPENAI, _LABEL_LOCAL, _LABEL_CANCEL]
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
        f"Switch {_side_title(side)} provider?",
        choices=choices,
        default=current_label,
        descriptions=descriptions,
    )
    if not choice or choice in {current_label, _LABEL_CANCEL}:
        return None
    if choice == _LABEL_MINILM:
        return "minilm"
    if choice == _LABEL_OPENAI:
        return "openai"
    if choice == _LABEL_LOCAL:
        return "local"
    return None


def _apply_kind(cfg: AMXConfig, side: Side, head: str, rest: list[str]) -> None:
    kind = (head or "").lower().strip()
    if kind in {"minilm", "default"}:
        _set_minilm(cfg, side)
        return
    if kind in {"openai", "openai_compatible", "openai-compatible"}:
        _set_openai_compatible(cfg, side, rest)
        return
    if kind in {"local", "sentence_transformers", "sentence-transformers", "st"}:
        _set_sentence_transformers(cfg, side, rest)
        return
    error(
        f"Unknown embedding kind: {head!r}. Expected one of "
        f"{SUPPORTED_EMBEDDING_KINDS} (or aliases: minilm, openai, local)."
    )


def cmd_embeddings(cfg: AMXConfig, rest: list[str]) -> None:
    """Show or change the search-index embedding providers.

    Usage (inside /search namespace)::

        /embeddings                              # show both sides + interactive picker
        /embeddings docs                         # show docs side + interactive picker
        /embeddings code                         # show code side + interactive picker
        /embeddings docs minilm                  # switch docs to MiniLM (Chroma default)
        /embeddings docs openai [model]          # switch docs to any OpenAI-compatible endpoint
        /embeddings docs local [model]           # switch docs to local sentence-transformers
        /embeddings code minilm                  # ditto for code RAG
        /embeddings code openai [model]
        /embeddings code local [model]

    Docs RAG and code RAG carry independent embedding providers — the
    ``side`` argument selects which one this invocation operates on.

    The OpenAI-compatible path covers OpenAI, OpenRouter, Together,
    Mistral, Azure OpenAI, vLLM, LM Studio, llama.cpp server, and any
    other provider that exposes the OpenAI ``/embeddings`` shape — the
    user just plugs in the matching ``base_url`` and ``api_key``.
    """
    side, rest = _pick_side(rest)
    if side is None:
        return

    if not rest:
        _print_current(cfg, side)
        info("")
        kind_alias = _pick_kind(cfg, side)
        if kind_alias is None:
            return
        _apply_kind(cfg, side, kind_alias, [])
        return

    _apply_kind(cfg, side, rest[0], rest[1:])


# Re-export so session.py can import via the same pattern as profiles.py.
__all__ = ["cmd_embeddings", "DEFAULT_EMBEDDING_KIND"]
