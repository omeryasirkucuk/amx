"""Shared embedding health + rebuild authority.

These pin the transport-free contract that both the web router and the
``/embeddings`` CLI consume, so the two surfaces can never drift: the
rebuild-all aggregate, the per-side failure capture, and the HTTP-status
hint carried by :class:`EmbeddingBackendUnavailable`.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

import amx.rag_core.embedding_health as eh
from amx.rag_core.embedding_health import (
    EMBEDDING_SIDES,
    EmbeddingBackendUnavailable,
    rebuild_all,
)


def test_sides_cover_docs_code_assets() -> None:
    assert set(EMBEDDING_SIDES) == {"docs", "code", "assets"}


def test_backend_unavailable_carries_status_hint() -> None:
    assert EmbeddingBackendUnavailable("x").status_code == 500
    assert EmbeddingBackendUnavailable("y", status_code=503).status_code == 503


def test_rebuild_all_runs_every_side_when_healthy(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[str] = []

    def _fake(side: str, cfg: Any, profile: str | None) -> dict[str, Any]:
        seen.append(side)
        return {"ok": True, "side": side, "message": f"rebuilt {side}"}

    monkeypatch.setattr(eh, "rebuild_side", _fake)
    out = rebuild_all(SimpleNamespace())
    assert seen == list(EMBEDDING_SIDES)
    assert out["ok"] is True
    assert out["failed"] == []
    assert {r["side"] for r in out["results"]} == set(EMBEDDING_SIDES)


def test_rebuild_all_records_failure_and_keeps_going(monkeypatch: pytest.MonkeyPatch) -> None:
    def _fake(side: str, cfg: Any, profile: str | None) -> dict[str, Any]:
        if side == "code":
            raise EmbeddingBackendUnavailable("Code RAG unavailable")
        return {"ok": True, "side": side}

    monkeypatch.setattr(eh, "rebuild_side", _fake)
    out = rebuild_all(SimpleNamespace())
    # One side failed but the others still ran.
    assert out["ok"] is False
    assert out["failed"] == ["code"]
    assert {r["side"] for r in out["results"]} == set(EMBEDDING_SIDES)
    code_row = next(r for r in out["results"] if r["side"] == "code")
    assert code_row["ok"] is False
    assert "Code RAG unavailable" in code_row["error"]


def test_collection_status_handles_missing_chromadb(monkeypatch: pytest.MonkeyPatch) -> None:
    # When chromadb can't be imported the status is reported as an error
    # rather than raising — every surface must still render.
    import builtins

    real_import = builtins.__import__

    def _no_chromadb(name: str, *a: Any, **k: Any):
        if name == "chromadb":
            raise ImportError("no chromadb")
        return real_import(name, *a, **k)

    monkeypatch.setattr(builtins, "__import__", _no_chromadb)
    out = eh.collection_status("docs", SimpleNamespace())
    assert out["stale"] is False
    assert out["collections"] == []
    assert "chromadb" in out["error"]
