"""PR δ — doctor adds a ``Code RAG store`` check.

Three branches matter to the user:

* fresh persist_dir (collection opens, 0 chunks) → ``ok=True`` with a
  "0 chunks indexed" detail and a "run /code scan" hint;
* mismatched embedding metadata → ``ok=False`` with the recorded
  provider/model in the detail and a remediation hint;
* arbitrary open failure → ``ok=False`` with the exception name.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from amx.cli_support.commands.doctor import _check_code_rag


def _wire_chroma(monkeypatch, *, coll):
    # The doctor helper imports ``chromadb`` lazily inside the function
    # body, so patching the top-level module's ``PersistentClient`` is
    # enough to redirect the construction.
    fake_client = SimpleNamespace(get_or_create_collection=lambda **_kw: coll)
    import chromadb

    monkeypatch.setattr(chromadb, "PersistentClient", lambda **_kw: fake_client)


def test_doctor_passes_for_healthy_code_collection(monkeypatch):
    coll = MagicMock()
    coll.metadata = {
        "embedding_provider": "minilm",
        "embedding_model": "minilm-l6-v2",
    }
    coll.count = MagicMock(return_value=42)
    _wire_chroma(monkeypatch, coll=coll)

    monkeypatch.setattr("amx.codebase.code_rag.query_code_snippets", lambda *_a, **_kw: [])
    monkeypatch.setattr(
        "amx.codebase.code_rag._resolve_code_embedding",
        lambda *_a, **_kw: ("minilm", "minilm-l6-v2", None),
    )

    cfg = MagicMock()
    result = _check_code_rag(cfg)
    assert result.ok is True
    assert "42 chunks" in result.detail
    assert "minilm" in result.detail


def test_doctor_warns_for_empty_collection(monkeypatch):
    coll = MagicMock()
    coll.metadata = {
        "embedding_provider": "minilm",
        "embedding_model": "minilm-l6-v2",
    }
    coll.count = MagicMock(return_value=0)
    _wire_chroma(monkeypatch, coll=coll)
    monkeypatch.setattr("amx.codebase.code_rag.query_code_snippets", lambda *_a, **_kw: [])
    monkeypatch.setattr(
        "amx.codebase.code_rag._resolve_code_embedding",
        lambda *_a, **_kw: ("minilm", "minilm-l6-v2", None),
    )

    cfg = MagicMock()
    result = _check_code_rag(cfg)
    assert result.ok is True  # empty is a warning-level pass, not a fail
    assert "0 chunks" in result.detail
    assert result.hint  # should suggest running /code scan


def test_doctor_fails_for_embedding_mismatch(monkeypatch):
    coll = MagicMock()
    coll.metadata = {
        "embedding_provider": "openai_compatible",
        "embedding_model": "text-embedding-3-small",
    }
    coll.count = MagicMock(return_value=0)
    _wire_chroma(monkeypatch, coll=coll)

    monkeypatch.setattr(
        "amx.codebase.code_rag._resolve_code_embedding",
        lambda *_a, **_kw: ("minilm", "minilm-l6-v2", None),
    )
    monkeypatch.setattr("amx.codebase.code_rag.query_code_snippets", lambda *_a, **_kw: [])

    cfg = MagicMock()
    result = _check_code_rag(cfg)
    assert result.ok is False
    assert "text-embedding-3-small" in result.detail
    assert "code-refresh" in (result.hint or "").lower()


def test_doctor_fails_for_open_error(monkeypatch):
    import chromadb

    def _boom(**_kw):
        raise RuntimeError("chroma_db permission denied")

    monkeypatch.setattr(chromadb, "PersistentClient", _boom)

    cfg = MagicMock()
    result = _check_code_rag(cfg)
    assert result.ok is False
    assert "permission denied" in result.detail
