"""Regression: ``amx`` REPL open must not pip-install chromadb.

The pricing fetcher (PR #298) and the install-flow refactor (PR #296)
both relied on the principle that a fresh ``pip install amx-cli``
followed by a bare ``amx`` should complete without a second wave of
pip subprocesses. ``amx/cli.py`` then re-broke that contract by
calling ``_install_embedding_provider(cfg)`` during the
:func:`amx.cli.main` callback, which imports
``amx.search.embeddings`` — a module that runs a module-level
``_ensure("rag")`` (chromadb + langchain-text-splitters + tiktoken)
before its class bodies can execute. Default-MiniLM users (the
overwhelming majority on day one) therefore paid a multi-minute
chromadb install at the moment they ran ``amx`` for the first time,
even when they only intended to touch ``/db``, ``/llm``, ``/ask``.

The fix is to short-circuit ``_install_embedding_provider`` for the
default-kind case: no custom factory is needed (Chroma's bundled
MiniLM is the same provider AMX would have configured anyway), so
the embeddings module can stay un-imported until the user actually
reaches a RAG entry point.

These tests pin that boundary against an innocent-looking edit
re-introducing an eager import.
"""

from __future__ import annotations

import sys

import pytest

from amx.cli import _install_embedding_provider
from amx.config import AMXConfig


def _drop_embeddings_module() -> None:
    """Force a fresh import check by evicting the cached module."""
    sys.modules.pop("amx.search.embeddings", None)


def test_default_embedding_kind_does_not_import_search_embeddings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A default-kind ``AMXConfig`` (both ``embedding_docs.kind`` and
    ``embedding_code.kind`` default to ``"minilm"``) must take the
    no-op shortcut. Importing ``amx.search.embeddings`` here
    would re-trigger the chromadb-pulling ``_ensure("rag")`` on every
    fresh-install REPL open, even though Chroma's bundled MiniLM is
    the exact provider AMX would have configured.
    """
    _drop_embeddings_module()
    cfg = AMXConfig()  # default kind = "minilm"
    assert cfg.embedding_docs.kind == "minilm"

    _install_embedding_provider(cfg)

    assert "amx.search.embeddings" not in sys.modules, (
        "regression: default-kind embedding triggered an eager import of "
        "amx.search.embeddings, which would chain into chromadb pip install "
        "on a fresh ``amx`` open."
    )


@pytest.mark.parametrize("default_alias", ["", "default", "minilm-l6-v2", "MINILM"])
def test_default_kind_aliases_all_take_the_shortcut(
    default_alias: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The shortcut accepts the common spellings users (and historical
    configs) might leave in ``cfg.embedding_docs.kind``: empty string,
    ``default``, ``minilm-l6-v2``, and capitalisation variants. Without
    this normalisation a tiny typo flips the user into the
    eager-import path and tanks first-launch UX.
    """
    _drop_embeddings_module()
    cfg = AMXConfig()
    cfg.embedding_docs.kind = default_alias

    _install_embedding_provider(cfg)

    assert "amx.search.embeddings" not in sys.modules, (
        f"regression: kind={default_alias!r} did not take the shortcut"
    )


def test_custom_embedding_kind_still_imports_search_embeddings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Custom-embedding users opt in to a non-default factory and are
    by definition heading for a RAG flow that would have required
    chromadb anyway. The shortcut must not silently swallow their
    custom config — the embeddings module is imported (so
    ``configure_from_amx_config`` writes the user's preferred
    factory) when the kind is anything other than the recognised
    default aliases.
    """
    _drop_embeddings_module()
    cfg = AMXConfig()
    cfg.embedding_docs.kind = "openai_compatible"

    _install_embedding_provider(cfg)

    assert "amx.search.embeddings" in sys.modules, (
        "regression: custom-kind embedding config silently skipped the "
        "configure_from_amx_config call; the user's factory would never "
        "be installed and every RAG operation would silently fall back "
        "to MiniLM."
    )
