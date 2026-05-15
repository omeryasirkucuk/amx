"""PR-C: Code RAG default embedder resolution tests.

Pins the graceful-fallback policy:

- When ``sentence-transformers`` is installed AND the jina code model
  loads cleanly → ``(sentence_transformers, jinaai/jina-embeddings-v2-base-code, ef)``.
- When sentence-transformers is missing OR the model fails to load
  for any reason (offline install, HF cache issue, …) → MiniLM with
  a one-time WARNING logged to the ``codebase.code_rag`` logger.
- Explicit non-default provider configs are honoured unchanged.

These tests mock the jina-construction path so CI doesn't trigger a
~161 MB model download every run. The fallback path is the one CI
hits by default — the smoke effect is that ``CodeIndex`` instances
still build successfully when the local-embeddings extra isn't
installed.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from amx.codebase import code_rag


@pytest.fixture(autouse=True)
def _reset_warning_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure each test starts with a clean ``_jina_fallback_warned``
    state so the one-time WARNING assertion is reliable."""
    monkeypatch.setattr(code_rag, "_jina_fallback_warned", False, raising=False)


# ── happy path: jina is available ─────────────────────────────────────


def test_default_returns_jina_when_sentence_transformers_available(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the local-embeddings extra is installed and the jina model
    loads cleanly, the default resolves to jina (not MiniLM)."""
    fake_ef = object()  # stand-in for the real EmbeddingFunction
    monkeypatch.setattr(
        code_rag,
        "_try_jina_code_embedder",
        lambda: (fake_ef, None),
    )

    provider, model, ef = code_rag._resolve_active_embedding(cfg=None)
    assert provider == "sentence_transformers"
    assert model == "jinaai/jina-embeddings-v2-base-code"
    assert ef is fake_ef


# ── fallback: sentence-transformers missing ───────────────────────────


def test_default_falls_back_to_minilm_when_st_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When jina construction fails (no sentence-transformers), the
    default falls back to MiniLM. The user-facing identity is MiniLM,
    not a half-broken jina."""
    monkeypatch.setattr(
        code_rag,
        "_try_jina_code_embedder",
        lambda: (None, "sentence-transformers not installed"),
    )

    provider, model, ef = code_rag._resolve_active_embedding(cfg=None)
    assert provider == "minilm"
    assert model == "minilm-l6-v2"
    assert ef is None


def test_fallback_logs_one_time_warning(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """First fallback logs a WARNING that names the install command;
    subsequent fallbacks within the same process stay silent."""
    monkeypatch.setattr(
        code_rag,
        "_try_jina_code_embedder",
        lambda: (None, "test reason"),
    )

    with caplog.at_level(logging.WARNING, logger="codebase.code_rag"):
        code_rag._resolve_active_embedding(cfg=None)
        code_rag._resolve_active_embedding(cfg=None)
        code_rag._resolve_active_embedding(cfg=None)

    warning_records = [
        r for r in caplog.records if r.levelno == logging.WARNING and "MiniLM" in r.getMessage()
    ]
    assert len(warning_records) == 1, (
        f"Expected exactly one fallback WARNING per process, got {len(warning_records)}"
    )
    msg = warning_records[0].getMessage()
    assert "amx-cli[local-embeddings]" in msg
    assert "jinaai/jina-embeddings-v2-base-code" in msg
    assert "test reason" in msg


# ── explicit provider override ────────────────────────────────────────


def test_explicit_openai_compatible_config_is_honoured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An explicit ``/embeddings openai_compatible <model>`` choice
    bypasses the jina-default upgrade entirely — user picked their
    provider, we honour it."""
    fake_ef = object()

    def _fake_make(kind: str, **_kwargs: object) -> object:
        return fake_ef

    monkeypatch.setattr(
        "amx.search.embeddings.make_embedding_function",
        _fake_make,
    )

    cfg = SimpleNamespace(
        embedding=SimpleNamespace(
            kind="openai_compatible",
            model="text-embedding-3-small",
            api_key="sk-test",
            base_url="",
        )
    )
    provider, model, ef = code_rag._resolve_active_embedding(cfg=cfg)
    assert provider == "openai_compatible"
    assert model == "text-embedding-3-small"
    assert ef is fake_ef


# ── _try_jina_code_embedder direct ────────────────────────────────────


def test_try_jina_reports_runtime_error_on_missing_st() -> None:
    """When ``SentenceTransformerEmbedding`` raises RuntimeError (the
    branch it uses when ``sentence_transformers`` import fails),
    ``_try_jina_code_embedder`` returns (None, reason) rather than
    propagating."""

    class _FakeST:
        def __init__(self, *, model: str) -> None:  # noqa: ARG002
            raise RuntimeError("sentence-transformers is not installed")

    with patch("amx.search.embeddings.SentenceTransformerEmbedding", _FakeST):
        ef, reason = code_rag._try_jina_code_embedder()
    assert ef is None
    assert reason and "sentence-transformers" in reason


def test_try_jina_catches_model_load_failures() -> None:
    """A broader failure during ``SentenceTransformer(model)`` — e.g.
    HF Hub unreachable, disk-cache permission denied — also degrades
    to ``(None, reason)`` without crashing the CodeIndex bootstrap."""

    class _FakeST:
        def __init__(self, *, model: str) -> None:  # noqa: ARG002
            raise OSError("HF Hub unreachable: offline mode")

    with patch("amx.search.embeddings.SentenceTransformerEmbedding", _FakeST):
        ef, reason = code_rag._try_jina_code_embedder()
    assert ef is None
    assert reason and "OSError" in reason and "offline" in reason


def test_try_jina_returns_ef_on_success() -> None:
    """Success path — ``_try_jina_code_embedder`` returns the
    constructed EmbeddingFunction and ``None`` for the error slot."""

    class _FakeST:
        def __init__(self, *, model: str) -> None:
            self.model = model

    with patch("amx.search.embeddings.SentenceTransformerEmbedding", _FakeST):
        ef, reason = code_rag._try_jina_code_embedder()
    assert ef is not None
    assert reason is None
    assert getattr(ef, "model", None) == code_rag.JINA_CODE_MODEL
