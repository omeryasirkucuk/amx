"""Storage correctness for the code RAG (PR beta).

Mirrors :mod:`tests.test_rag_storage_correctness` for the code path:

1. The ``amx_code`` collection records ``embedding_provider`` /
   ``embedding_model`` on first create so a later reopen can detect a
   provider switch.
2. Reopening with a different provider raises
   :class:`CodeEmbeddingMismatch`; a pre-PR-beta collection without
   those metadata keys is grandfathered in and gets the metadata
   back-filled silently.
3. Re-indexing a file deletes orphan chunks (shrink / function rename /
   file removal) before upserting the new ones.
4. ``analyze_flow`` tags the mismatch with an ``embedding_mismatch:``
   prefix on ``code_unavailable_reason``.
"""

from __future__ import annotations

from pathlib import Path

import chromadb
import pytest

from amx.codebase import code_rag
from amx.codebase.code_rag import (
    COLLECTION,
    CodeEmbeddingMismatch,
    index_codebase_tree,
)


@pytest.fixture(autouse=True)
def _stub_embedding_resolver(monkeypatch):
    """Bypass the real :func:`make_embedding_function` so tests can
    pretend to use ``openai_compatible``/``sentence_transformers``
    without hitting the network or requiring the optional deps.

    The mismatch check operates on the recorded ``embedding_provider`` /
    ``embedding_model`` metadata strings; the actual ``ef`` object is
    irrelevant to it. Returning ``None`` means Chroma falls back to its
    bundled default which is fine for the test's purposes (we never
    enqueue real vectors to compare).
    """
    current: dict[str, tuple[str, str]] = {"value": ("minilm", "minilm-l6-v2")}

    def _fake_resolve(cfg=None):
        emb = getattr(cfg, "embedding_code", None)
        if emb is not None:
            return (
                getattr(emb, "kind", "minilm"),
                getattr(emb, "model", "minilm-l6-v2"),
                None,
            )
        return (*current["value"], None)

    monkeypatch.setattr(code_rag, "_resolve_code_embedding", _fake_resolve)
    yield current


def _seed_collection(
    persist: Path,
    *,
    provider: str = "minilm",
    model: str = "minilm-l6-v2",
) -> None:
    """Force-create the collection with explicit provider metadata.

    Uses a tiny throwaway repo so :func:`index_codebase_tree` does the
    create-path work (and stamps metadata) for us; keeps the test
    aligned with the production helper rather than poking Chroma
    directly.
    """
    repo = persist.parent / f"seed_{provider}"
    repo.mkdir(parents=True, exist_ok=True)
    (repo / "x.py").write_text("def x():\n    return 1\n", encoding="utf-8")

    class _FakeEmbedding:
        def __init__(self, k: str, m: str) -> None:
            self.kind = k
            self.model = m
            self.api_key = ""
            self.base_url = ""

    class _FakeCfg:
        def __init__(self, k: str, m: str) -> None:
            self.embedding_code = _FakeEmbedding(k, m)

    index_codebase_tree(
        repo,
        persist_dir=str(persist),
        source_root=str(repo),
        cfg=_FakeCfg(provider, model),
    )


# ── metadata recorded on first create ────────────────────────────────


def test_collection_records_embedding_metadata_on_create(tmp_path: Path) -> None:
    persist = tmp_path / "chroma"
    _seed_collection(persist, provider="minilm", model="minilm-l6-v2")
    client = chromadb.PersistentClient(path=str(persist))
    coll = client.get_collection(COLLECTION)
    meta = dict(coll.metadata or {})
    assert meta.get("embedding_provider") == "minilm"
    assert meta.get("embedding_model") == "minilm-l6-v2"
    # PR-B: schema bumped to v2 across docs + code RAG; ``embedding_dim``
    # added for MiniLM (well-known = 384). Older v1 metadata gets
    # upgraded silently on reopen — see test_legacy_dim_backfilled below.
    assert meta.get("amx_schema_version") == 2
    assert meta.get("embedding_dim") == 384


def test_legacy_v1_code_collection_dim_backfilled_on_reopen(tmp_path: Path) -> None:
    """v1 metadata (no ``embedding_dim``) reopens cleanly and the dim
    is back-filled silently on next open — mirrors the docs-RAG test
    in test_rag_storage_correctness.py."""
    persist = tmp_path / "chroma"
    persist.mkdir(parents=True, exist_ok=True)
    client = chromadb.PersistentClient(path=str(persist))
    client.get_or_create_collection(
        name=COLLECTION,
        metadata={
            "hnsw:space": "cosine",
            "embedding_provider": "minilm",
            "embedding_model": "minilm-l6-v2",
            "amx_schema_version": 1,
        },
    )
    _seed_collection(persist, provider="minilm", model="minilm-l6-v2")
    coll = client.get_collection(COLLECTION)
    meta = dict(coll.metadata or {})
    assert meta.get("embedding_dim") == 384
    assert meta.get("amx_schema_version") == 2


def test_dim_mismatch_raises_code_embedding_mismatch(tmp_path: Path) -> None:
    """Same provider/model strings but a recorded dim that disagrees
    with the active dim raises. Catches the silent-corruption case
    where two providers expose the same model id with different
    vector dimensions."""
    persist = tmp_path / "chroma"
    persist.mkdir(parents=True, exist_ok=True)
    client = chromadb.PersistentClient(path=str(persist))
    client.get_or_create_collection(
        name=COLLECTION,
        metadata={
            "hnsw:space": "cosine",
            "embedding_provider": "minilm",
            "embedding_model": "minilm-l6-v2",
            "embedding_dim": 768,
            "amx_schema_version": 2,
        },
    )
    with pytest.raises(CodeEmbeddingMismatch) as excinfo:
        _seed_collection(persist, provider="minilm", model="minilm-l6-v2")
    msg = str(excinfo.value)
    assert "dim 768 -> 384" in msg
    assert excinfo.value.recorded_dim == 768
    assert excinfo.value.active_dim == 384


# ── mismatch raises ──────────────────────────────────────────────────


def test_mismatch_raises_code_embedding_mismatch(tmp_path: Path) -> None:
    persist = tmp_path / "chroma"
    _seed_collection(persist, provider="openai_compatible", model="text-embedding-3-small")
    # Reopen with provider B; expect the structured mismatch error.
    with pytest.raises(CodeEmbeddingMismatch) as excinfo:
        _seed_collection(persist, provider="minilm", model="minilm-l6-v2")
    msg = str(excinfo.value)
    assert "openai_compatible" in msg
    assert "text-embedding-3-small" in msg
    assert "minilm" in msg
    assert "/code-refresh" in msg


# ── grandfather rule for pre-PR-beta collections ─────────────────────


def test_pre_existing_collection_without_metadata_is_grandfathered(
    tmp_path: Path,
) -> None:
    persist = tmp_path / "chroma"
    persist.mkdir(parents=True, exist_ok=True)
    # Simulate a pre-PR-beta collection: only the ``hnsw:space`` key.
    client = chromadb.PersistentClient(path=str(persist))
    client.get_or_create_collection(name=COLLECTION, metadata={"hnsw:space": "cosine"})

    _seed_collection(persist, provider="minilm", model="minilm-l6-v2")

    coll = client.get_collection(COLLECTION)
    backfilled = dict(coll.metadata or {})
    assert backfilled.get("embedding_provider") == "minilm"
    assert backfilled.get("embedding_model") == "minilm-l6-v2"


# ── idempotency ──────────────────────────────────────────────────────


def _ids_for_rel(persist: Path, rel: str) -> list[str]:
    client = chromadb.PersistentClient(path=str(persist))
    coll = client.get_collection(COLLECTION)
    return list(coll.get(where={"rel_path": rel}, include=[]).get("ids") or [])


def test_idempotent_ingest_deletes_orphan_chunks_on_shrink(tmp_path: Path) -> None:
    persist = tmp_path / "chroma"
    repo = tmp_path / "repo"
    repo.mkdir()
    src = repo / "lots.py"
    # Three functions → three AST chunks. The chunker drops segments
    # shorter than 40 chars, so each body is padded with a long
    # docstring that lives inside the function (counts toward the
    # source segment, unlike a trailing comment which AST excludes).
    pad = "x" * 200
    src.write_text(
        "\n\n".join([f'def fn_{i}(x):\n    """{pad}"""\n    return x + {i}\n' for i in range(3)]),
        encoding="utf-8",
    )
    index_codebase_tree(repo, persist_dir=str(persist), source_root=str(repo))
    initial = _ids_for_rel(persist, "lots.py")
    assert len(initial) == 3

    # Shrink to a single function — expect only one chunk to remain.
    src.write_text(
        f'def only_one(x):\n    """{pad}"""\n    return x\n',
        encoding="utf-8",
    )
    index_codebase_tree(repo, persist_dir=str(persist), source_root=str(repo))
    final = _ids_for_rel(persist, "lots.py")
    assert len(final) == 1


def test_idempotent_ingest_deletes_orphans_on_file_removal(tmp_path: Path) -> None:
    persist = tmp_path / "chroma"
    repo = tmp_path / "repo"
    repo.mkdir()
    keep = repo / "keep.py"
    drop = repo / "drop.py"
    pad = "p" * 200
    keep.write_text(f'def keep_me():\n    """{pad}"""\n    return 1\n', encoding="utf-8")
    drop.write_text(f'def drop_me():\n    """{pad}"""\n    return 2\n', encoding="utf-8")
    index_codebase_tree(repo, persist_dir=str(persist), source_root=str(repo))
    assert _ids_for_rel(persist, "drop.py")

    drop.unlink()
    index_codebase_tree(repo, persist_dir=str(persist), source_root=str(repo))
    assert _ids_for_rel(persist, "drop.py") == []
    assert _ids_for_rel(persist, "keep.py")


def test_idempotent_ingest_handles_function_rename(tmp_path: Path) -> None:
    persist = tmp_path / "chroma"
    repo = tmp_path / "repo"
    repo.mkdir()
    src = repo / "rename.py"
    pad = "r" * 200
    src.write_text(
        f'def original_name(x):\n    """{pad}"""\n    return x + 1\n',
        encoding="utf-8",
    )
    index_codebase_tree(repo, persist_dir=str(persist), source_root=str(repo))
    before = sorted(_ids_for_rel(persist, "rename.py"))
    assert before  # at least one chunk exists

    # Rename the function — chunk id suffix changes (function name +
    # lineno), so the doc_id hash changes too. The old id must be
    # deleted by the per-file pre-delete step.
    src.write_text(
        f'def renamed_name(x):\n    """{pad}"""\n    return x + 1\n',
        encoding="utf-8",
    )
    index_codebase_tree(repo, persist_dir=str(persist), source_root=str(repo))
    after = sorted(_ids_for_rel(persist, "rename.py"))
    assert after
    assert set(after).isdisjoint(set(before))


# ── caller surfaces mismatch as a run-record reason ──────────────────


def test_record_code_unavailable_reason_tags_mismatch() -> None:
    from amx.cli_support.commands.analyze_flow import _record_code_unavailable_reason

    exc = CodeEmbeddingMismatch(
        recorded_provider="openai_compatible",
        recorded_model="text-embedding-3-small",
        active_provider="minilm",
        active_model="minilm-l6-v2",
    )
    sink: dict[str, str] = {}
    _record_code_unavailable_reason(sink, exc)
    assert sink["code_unavailable_reason"].startswith("embedding_mismatch:")
    assert "openai_compatible" in sink["code_unavailable_reason"]


def test_record_code_unavailable_reason_tags_generic_failure() -> None:
    from amx.cli_support.commands.analyze_flow import _record_code_unavailable_reason

    sink: dict[str, str] = {}
    _record_code_unavailable_reason(sink, RuntimeError("boom"))
    # PR δ (I13): generic failures are tagged with the
    # ``index_error:`` prefix so downstream consumers can distinguish
    # them from the ``embedding_mismatch`` and ``query_timeout`` cases.
    assert sink["code_unavailable_reason"].startswith("index_error: RuntimeError")
    assert "boom" in sink["code_unavailable_reason"]
