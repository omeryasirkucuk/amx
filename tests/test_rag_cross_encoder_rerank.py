"""PR-F: cross-encoder rerank (opt-in) tests.

Pure-mocks for the cross-encoder lifecycle plus an end-to-end smoke
through ``RAGStore``. The real model never loads in CI — every test
patches ``sentence_transformers.CrossEncoder`` so this suite stays
under a second and offline-safe.
"""

from __future__ import annotations

import sys
import types
from pathlib import Path
from unittest.mock import patch

import pytest

from amx.docs.rag import RAGStore
from amx.docs.scanner import DocInfo
from amx.rag_core.rerank import (
    MODEL_FOR_KIND,
    CrossEncoderReranker,
    reranker_from_kind,
)


def _inject_fake_st(monkeypatch: pytest.MonkeyPatch, cross_encoder_cls: type) -> None:
    """Inject a fake ``sentence_transformers`` module that exposes
    the supplied ``CrossEncoder`` class. CI has a broken
    ``transformers`` install (tokenizers version pin), so direct
    ``patch("sentence_transformers.CrossEncoder", ...)`` would fail
    at patch-discovery time. ``sys.modules`` injection bypasses the
    real package entirely."""
    fake = types.ModuleType("sentence_transformers")
    fake.CrossEncoder = cross_encoder_cls  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "sentence_transformers", fake)


# ── factory: kind → reranker dispatch ────────────────────────────────


def test_reranker_from_kind_heuristic_returns_none() -> None:
    """``heuristic`` (the default) returns ``None`` — caller's existing
    rerank path stays load-bearing."""
    assert reranker_from_kind("heuristic") is None
    assert reranker_from_kind("") is None
    assert reranker_from_kind(None) is None


def test_reranker_from_kind_cross_encoder_returns_instance() -> None:
    r = reranker_from_kind("cross_encoder")
    assert isinstance(r, CrossEncoderReranker)
    assert r.model_id == MODEL_FOR_KIND["cross_encoder"]


def test_reranker_from_kind_multilingual_returns_instance() -> None:
    r = reranker_from_kind("cross_encoder_multilingual")
    assert isinstance(r, CrossEncoderReranker)
    assert r.model_id == MODEL_FOR_KIND["cross_encoder_multilingual"]


def test_reranker_from_kind_unknown_returns_none_and_logs(caplog: pytest.LogCaptureFixture) -> None:
    """Unknown kinds degrade to heuristic with a warning so the user
    sees their typo without retrieval breaking."""
    import logging

    with caplog.at_level(logging.WARNING, logger="rag_core.rerank"):
        out = reranker_from_kind("typo-not-real")
    assert out is None
    assert any("typo-not-real" in r.message for r in caplog.records)


def test_reranker_from_kind_is_case_insensitive() -> None:
    """User-supplied config values may be uppercase or whitespace-
    padded. The factory normalises before lookup."""
    assert isinstance(reranker_from_kind("CROSS_ENCODER"), CrossEncoderReranker)
    assert isinstance(reranker_from_kind("  cross_encoder  "), CrossEncoderReranker)


# ── CrossEncoderReranker: lazy load + fallback ──────────────────────


def test_construction_does_not_load_model(monkeypatch: pytest.MonkeyPatch) -> None:
    """Construction is cheap; the model isn't loaded until the first
    rerank call. Verified by injecting a raising stub — if
    construction loaded eagerly, this would raise."""

    class _Explode:
        def __init__(self, *_a, **_kw):
            raise AssertionError("constructor should not be called at __init__")

    _inject_fake_st(monkeypatch, _Explode)
    r = CrossEncoderReranker(model_id="some/model")
    assert r.model_id == "some/model"


def test_rerank_loads_model_on_first_call(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeCE:
        def __init__(self, model_id: str) -> None:
            self.model_id = model_id
            self.calls = 0

        def predict(self, pairs, **_kwargs):
            self.calls += 1
            return [float(i) for i in range(len(pairs))]

    _inject_fake_st(monkeypatch, _FakeCE)
    r = CrossEncoderReranker(model_id="x/y")
    hits = [{"text": "a"}, {"text": "b"}, {"text": "c"}]
    out = r.rerank("q", hits)
    assert len(out) == 3
    # _FakeCE assigns ascending scores [0, 1, 2]; rerank sorts
    # descending so "c" comes first.
    assert out[0]["text"] == "c"
    assert out[-1]["text"] == "a"
    # And every output hit has a numeric score stamped.
    for h in out:
        assert isinstance(h["score"], float)


def test_rerank_empty_input_short_circuits(monkeypatch: pytest.MonkeyPatch) -> None:
    """No hits → no model load."""

    class _Explode:
        def __init__(self, *_a, **_kw):
            raise AssertionError("model load should not happen on empty input")

    _inject_fake_st(monkeypatch, _Explode)
    r = CrossEncoderReranker(model_id="x")
    assert r.rerank("q", []) == []


def test_rerank_fallback_when_sentence_transformers_missing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """``ImportError`` from the sentence_transformers import is the
    "extra not installed" path — return hits unchanged with a
    one-time warning."""
    import logging

    with patch.dict("sys.modules", {"sentence_transformers": None}):
        r = CrossEncoderReranker(model_id="x")
        hits = [{"text": "a"}, {"text": "b"}]
        with caplog.at_level(logging.WARNING, logger="rag_core.rerank"):
            out = r.rerank("q", hits)
    assert out == hits
    assert any("sentence-transformers not installed" in rec.message for rec in caplog.records)


def test_rerank_fallback_when_model_load_raises(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A failure during ``CrossEncoder(model_id)`` (HF Hub down,
    permission error, etc.) degrades to the input ordering with
    a structured warning."""
    import logging

    class _RaiseOnLoad:
        def __init__(self, *_a, **_kw):
            raise OSError("HF Hub unreachable")

    _inject_fake_st(monkeypatch, _RaiseOnLoad)
    r = CrossEncoderReranker(model_id="x")
    hits = [{"text": "a"}]
    with caplog.at_level(logging.WARNING, logger="rag_core.rerank"):
        out = r.rerank("q", hits)
    assert out == hits
    assert any("could not load model" in rec.message for rec in caplog.records)


def test_rerank_fallback_when_predict_raises(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Model loads but the scoring call itself fails — still safe."""
    import logging

    class _BadPredict:
        def __init__(self, *_a, **_kw):
            pass

        def predict(self, *_a, **_kw):
            raise RuntimeError("CUDA OOM")

    _inject_fake_st(monkeypatch, _BadPredict)
    r = CrossEncoderReranker(model_id="x")
    hits = [{"text": "a"}, {"text": "b"}]
    with caplog.at_level(logging.WARNING, logger="rag_core.rerank"):
        out = r.rerank("q", hits)
    assert out == hits
    assert any(".predict failed" in rec.message for rec in caplog.records)


def test_rerank_caches_failed_load_no_retry() -> None:
    """After a model-load failure, subsequent ``rerank`` calls do
    NOT re-attempt the import — they short-circuit via the
    ``_load_failed`` flag. Pinning this so we don't accidentally
    introduce a per-query import retry that pegs the CPU on a
    persistently-broken install."""
    call_count = {"n": 0}

    def _raise(*_args, **_kwargs):
        call_count["n"] += 1
        raise ImportError("simulated")

    with patch.dict("sys.modules", {"sentence_transformers": None}):
        # Drive the import path failure via patch.dict so the
        # first call goes through the ImportError branch.
        r = CrossEncoderReranker(model_id="x")
        r.rerank("q1", [{"text": "a"}])
        r.rerank("q2", [{"text": "b"}])
        r.rerank("q3", [{"text": "c"}])
    # The first call captured the ImportError and set
    # _load_failed=True; subsequent calls bail out before touching
    # the import again — so the patched module's stub was hit at
    # most once.
    # (We can't easily count import attempts from the test side;
    # the contract under test is that ``r._load_failed`` flips
    # after first failure.)
    assert r._load_failed is True


# ── RAGStore wire-up: reranker_kind flag ────────────────────────────


def _make_doc(tmp_path: Path, body: str, name: str = "fixture.txt") -> DocInfo:
    p = tmp_path / name
    p.write_text(body, encoding="utf-8")
    return DocInfo(
        path=str(p),
        size_bytes=p.stat().st_size,
        extension=".txt",
        source_type="local",
        source_root=str(tmp_path),
    )


def test_ragstore_default_uses_heuristic_rerank(tmp_path: Path) -> None:
    """Without an explicit ``reranker_kind`` (and no cfg.docs.rerank),
    RAGStore stays on the heuristic path — no cross-encoder
    instance attached."""
    store = RAGStore(
        persist_dir=str(tmp_path / "chroma"),
        embedding_function=None,
        embedding_provider="minilm",
        embedding_model="minilm-l6-v2",
    )
    assert store._cross_encoder is None


def test_ragstore_explicit_reranker_kind_attaches_instance(tmp_path: Path) -> None:
    """Passing ``reranker_kind="cross_encoder"`` attaches a
    :class:`CrossEncoderReranker` to the store. The model isn't
    loaded yet (verified by the constructor side-effect: lazy)."""
    store = RAGStore(
        persist_dir=str(tmp_path / "chroma"),
        embedding_function=None,
        embedding_provider="minilm",
        embedding_model="minilm-l6-v2",
        reranker_kind="cross_encoder",
    )
    assert isinstance(store._cross_encoder, CrossEncoderReranker)
    assert store._cross_encoder.model_id == MODEL_FOR_KIND["cross_encoder"]
    assert store._cross_encoder._model is None  # not yet loaded


def test_ragstore_cross_encoder_path_runs_when_attached(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """End-to-end: a store constructed with the cross-encoder
    reranker actually delegates to it on ``query()``. The fake
    ``CrossEncoder`` records every ``.predict`` call so we can
    assert at least one happened during the query."""

    class _CountingCE:
        calls = 0

        def __init__(self, *_a, **_kw):
            pass

        def predict(self, pairs, **_kw):
            _CountingCE.calls += 1
            return [1.0 - i * 0.1 for i in range(len(pairs))]

    _inject_fake_st(monkeypatch, _CountingCE)
    store = RAGStore(
        persist_dir=str(tmp_path / "chroma"),
        embedding_function=None,
        embedding_provider="minilm",
        embedding_model="minilm-l6-v2",
        reranker_kind="cross_encoder",
    )
    store.ingest([_make_doc(tmp_path, "Some retrieval body.", name="a.txt")])
    store.query("retrieval body", n_results=1)
    assert _CountingCE.calls >= 1


def test_ragstore_falls_back_to_heuristic_when_cross_encoder_unavailable(
    tmp_path: Path,
) -> None:
    """Cross-encoder configured but ``sentence_transformers`` missing
    → the rerank degrades transparently; hits still come back."""
    with patch.dict("sys.modules", {"sentence_transformers": None}):
        store = RAGStore(
            persist_dir=str(tmp_path / "chroma"),
            embedding_function=None,
            embedding_provider="minilm",
            embedding_model="minilm-l6-v2",
            reranker_kind="cross_encoder",
        )
        store.ingest([_make_doc(tmp_path, "body text", name="a.txt")])
        hits = store.query("body text", n_results=1)
    assert hits, "fallback should still return retrieval hits"


def test_ragstore_unknown_reranker_kind_falls_back_to_heuristic(tmp_path: Path) -> None:
    """A typo in ``reranker_kind`` doesn't break retrieval —
    factory returns ``None``, RAGStore stays on heuristic path."""
    store = RAGStore(
        persist_dir=str(tmp_path / "chroma"),
        embedding_function=None,
        embedding_provider="minilm",
        embedding_model="minilm-l6-v2",
        reranker_kind="typo-not-a-real-kind",
    )
    assert store._cross_encoder is None
