"""Single-authority embedding resolution + honest fallback signal.

Before this module each side (docs/code/assets) silently fell back to
MiniLM when the configured model couldn't be built, so a user could
configure gte-small and unknowingly run MiniLM. ``resolve_embedding``
centralises the logic and reports ``fell_back`` + ``fallback_reason``
so the substitution is never silent. These tests pin that contract and
verify the three legacy wrappers still return the historical tuple.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from amx.rag_core.embedding_resolver import ResolvedEmbedding, resolve_embedding

_MINILM = lambda: ("minilm", "minilm-l6-v2", None)  # noqa: E731


def _cfg(side: str, *, kind: str, model: str = "") -> SimpleNamespace:
    field = f"embedding_{side}"
    return SimpleNamespace(
        **{field: SimpleNamespace(kind=kind, model=model, api_key="", base_url="")}
    )


def test_default_kind_is_not_a_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg("docs", kind="minilm")
    r = resolve_embedding("docs", cfg, default_resolver=_MINILM)
    assert r.active_provider == "minilm"
    assert r.fell_back is False
    assert r.fallback_reason is None
    assert r.dependency_available is True


def test_none_config_is_not_a_fallback() -> None:
    r = resolve_embedding("docs", SimpleNamespace(), default_resolver=_MINILM)
    assert r.active_provider == "minilm"
    assert r.fell_back is False


def test_configured_model_that_loads(monkeypatch: pytest.MonkeyPatch) -> None:
    import amx.search.embeddings as emb

    sentinel = object()
    monkeypatch.setattr(emb, "make_embedding_function", lambda *a, **k: sentinel)
    cfg = _cfg("docs", kind="sentence_transformers", model="thenlper/gte-small")

    r = resolve_embedding("docs", cfg, default_resolver=_MINILM)

    assert r.configured_provider == "sentence_transformers"
    assert r.configured_model == "thenlper/gte-small"
    assert r.active_provider == "sentence_transformers"
    assert r.active_model == "thenlper/gte-small"
    assert r.embedding_function is sentinel
    assert r.fell_back is False
    assert r.dependency_available is True


def test_configured_model_that_fails_falls_back_loudly(monkeypatch: pytest.MonkeyPatch) -> None:
    import amx.search.embeddings as emb

    def _boom(*_a: Any, **_k: Any):
        raise RuntimeError("sentence-transformers is not installed")

    monkeypatch.setattr(emb, "make_embedding_function", _boom)
    cfg = _cfg("docs", kind="sentence_transformers", model="thenlper/gte-small")

    r = resolve_embedding("docs", cfg, default_resolver=_MINILM)

    # Configured intent preserved, active reflects the fallback.
    assert r.configured_provider == "sentence_transformers"
    assert r.configured_model == "thenlper/gte-small"
    assert r.active_provider == "minilm"
    assert r.active_model == "minilm-l6-v2"
    # The substitution is NOT silent.
    assert r.fell_back is True
    assert "sentence-transformers is not installed" in (r.fallback_reason or "")
    assert r.dependency_available is False


def test_explicit_kind_without_model_uses_default(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg("assets", kind="openai_compatible", model="")
    r = resolve_embedding("assets", cfg, default_resolver=_MINILM)
    assert r.active_provider == "minilm"
    assert r.fell_back is False


def test_as_tuple_returns_active_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    import amx.search.embeddings as emb

    monkeypatch.setattr(emb, "make_embedding_function", lambda *a, **k: "EF")
    cfg = _cfg("code", kind="openai_compatible", model="text-embedding-3-small")
    r = resolve_embedding("code", cfg, default_resolver=_MINILM)
    assert r.as_tuple() == ("openai_compatible", "text-embedding-3-small", "EF")


def test_code_default_resolver_is_used_for_default_kind() -> None:
    """Code's default target is its own (jina/minilm) resolver, not the
    plain MiniLM one — the side-specific default is honoured."""
    called = {"n": 0}

    def _code_default():
        called["n"] += 1
        return ("sentence_transformers", "jinaai/jina-embeddings-v2-base-code", "JINA_EF")

    cfg = _cfg("code", kind="minilm")
    r = resolve_embedding("code", cfg, default_resolver=_code_default)
    assert called["n"] == 1
    assert r.active_model == "jinaai/jina-embeddings-v2-base-code"
    assert r.fell_back is False


# ── back-compat: the three legacy wrappers return the historical tuple ──


def test_docs_wrapper_back_compat(monkeypatch: pytest.MonkeyPatch) -> None:
    from amx.docs.rag import _resolve_docs_embedding

    out = _resolve_docs_embedding(SimpleNamespace())
    assert out == ("minilm", "minilm-l6-v2", None)


def test_assets_wrapper_back_compat() -> None:
    from amx.assets.rag import _resolve_assets_embedding

    out = _resolve_assets_embedding(SimpleNamespace())
    assert out[0] == "minilm" and out[1] == "minilm-l6-v2"


def test_resolve_side_docs_uses_minilm_default(monkeypatch: pytest.MonkeyPatch) -> None:
    """The side dispatcher applies the right default without the caller
    knowing it — docs/assets default to plain MiniLM."""
    from amx.rag_core.embedding_resolver import resolve_side

    r = resolve_side("docs", _cfg("docs", kind="minilm"))
    assert r.active_provider == "minilm"
    assert r.fell_back is False


def test_resolve_side_reports_fallback_honestly(monkeypatch: pytest.MonkeyPatch) -> None:
    """The headline contract: a configured model that can't build is
    reported as fell_back with active != configured — never a silent
    swap."""
    import amx.search.embeddings as emb
    from amx.rag_core.embedding_resolver import resolve_side

    monkeypatch.setattr(
        emb,
        "make_embedding_function",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("dep missing")),
    )
    r = resolve_side("assets", _cfg("assets", kind="sentence_transformers", model="x/y"))
    assert r.configured_model == "x/y"
    assert r.active_provider == "minilm"
    assert r.fell_back is True
    assert r.dependency_available is False


def test_resolved_embedding_is_frozen() -> None:
    r = ResolvedEmbedding(
        side="docs",
        configured_provider="minilm",
        configured_model="minilm-l6-v2",
        active_provider="minilm",
        active_model="minilm-l6-v2",
        embedding_function=None,
        fell_back=False,
        fallback_reason=None,
        dependency_available=True,
    )
    with pytest.raises(Exception):
        r.fell_back = True  # type: ignore[misc]
