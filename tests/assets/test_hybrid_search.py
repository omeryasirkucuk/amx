"""Tests for :mod:`amx.assets.search` — hybrid keyword + semantic search.

The hybrid path replaces the pure-semantic ``AssetRAGStore.query``
call that the Studio search box used to issue. The two regressions it
fixes are pinned here:

* Searching ``trips`` in Queries used to return SQL with no ``trips``
  token because MiniLM's embedding clustered the query close to
  ``count(*) FROM _amx_users``. With FTS5 as the candidate gate, an
  asset must contain the keyword to be eligible for return.
* The FTS5 mirror tables stay in sync with the source ``remote_*``
  tables through ``AFTER INSERT/UPDATE/DELETE`` triggers installed by
  ``_ensure_fts_tables``; the test below pins that contract.

The dense rerank step is mocked out with a stub store so these tests
stay free of the optional ChromaDB dependency.
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from amx.assets.search import HybridAssetSearch
from amx.assets.types import AssetQueryHit
from amx.storage.sqlite_store import SQLiteHistoryStore


class _StubRAGStore:
    """In-memory rerank that scores by 'longest substring match'.

    Mirrors the contract of :class:`AssetRAGStore.rerank` /
    :meth:`AssetRAGStore.query` without the heavy embedding stack.
    Scoring is intentionally deterministic so tests can assert on
    ordering.
    """

    def __init__(self, name_by_id: dict[tuple[str, int], str]) -> None:
        # (kind, remote_id) -> display name
        self.name_by_id = name_by_id
        self.calls: list[dict] = []

    def rerank(
        self,
        candidate_remote_ids: list[int],
        text: str,
        *,
        profile: str,
        kind: str,
        top_k: int = 20,
    ) -> list[AssetQueryHit]:
        self.calls.append(
            {
                "fn": "rerank",
                "candidates": list(candidate_remote_ids),
                "text": text,
                "profile": profile,
                "kind": kind,
                "top_k": top_k,
            }
        )
        out: list[AssetQueryHit] = []
        for rid in candidate_remote_ids[:top_k]:
            name = self.name_by_id.get((kind, int(rid)), f"asset-{rid}")
            score = 0.5 + (0.1 if text.lower() in name.lower() else 0.0)
            out.append(
                AssetQueryHit(
                    chunk_id=f"{profile}::{kind}::{rid}::0",
                    kind=kind,
                    profile=profile,
                    remote_id=int(rid),
                    name=name,
                    text=name,
                    score=score,
                    metadata={},
                )
            )
        return out

    def query(
        self,
        text: str,
        *,
        top_k: int = 5,
        profile: str | None = None,
        kind: str | None = None,
        remote_ids: list[int] | None = None,
    ) -> list[AssetQueryHit]:
        self.calls.append(
            {
                "fn": "query",
                "text": text,
                "top_k": top_k,
                "profile": profile,
                "kind": kind,
                "remote_ids": list(remote_ids or []),
            }
        )
        return []


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteHistoryStore:
    db = SQLiteHistoryStore(tmp_path / "history.db")
    db.init()
    return db


def _insert_notebook(
    store: SQLiteHistoryStore,
    *,
    rid_external: str,
    name: str,
    source: str,
    workspace: str = "/Workspace/Foo",
    profile: str = "prof",
) -> int:
    with store._connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO remote_notebooks (
                profile_name, platform, external_id, name, workspace_path,
                qualified_name, language, source_text, source_hash,
                last_modified_at, last_modified_by, owner, cell_count,
                ingested_at
            ) VALUES (?, 'databricks', ?, ?, ?, NULL, 'python', ?, 'h', NULL, NULL, NULL, 1, ?)
            """,
            (profile, rid_external, name, workspace, source, time.time()),
        )
        return int(cur.lastrowid or 0)


def _insert_query(
    store: SQLiteHistoryStore,
    *,
    rid_external: str,
    name: str | None,
    sql_text: str,
    warehouse: str = "wh1",
    profile: str = "prof",
) -> int:
    with store._connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO remote_queries (
                profile_name, platform, kind, external_id, name, sql_text, sql_hash,
                warehouse, user_name, executed_at, duration_ms, ingested_at
            ) VALUES (?, 'databricks', 'history', ?, ?, ?, 'h', ?, 'u', NULL, NULL, ?)
            """,
            (profile, rid_external, name, sql_text, warehouse, time.time()),
        )
        return int(cur.lastrowid or 0)


def test_fts_sync_triggers_keep_inserts_in_view(store: SQLiteHistoryStore) -> None:
    """An INSERT on remote_notebooks must materialise in fts_notebooks."""
    rid = _insert_notebook(
        store, rid_external="n1", name="orders_ingest", source="SELECT 1 FROM orders"
    )
    with store._connect() as conn:
        rows = conn.execute(
            "SELECT remote_id FROM fts_notebooks WHERE fts_notebooks MATCH 'orders' "
            "AND profile_name = ?",
            ("prof",),
        ).fetchall()
    assert [int(r[0]) for r in rows] == [rid]


def test_fts_sync_triggers_keep_deletes_in_view(store: SQLiteHistoryStore) -> None:
    """A DELETE on the source table must remove the matching FTS row."""
    rid = _insert_notebook(store, rid_external="n1", name="orders_ingest", source="SELECT 1")
    with store._connect() as conn:
        conn.execute("DELETE FROM remote_notebooks WHERE id = ?", (rid,))
        conn.commit()
        remaining = conn.execute(
            "SELECT COUNT(*) FROM fts_notebooks WHERE remote_id = ? AND profile_name = ?",
            (rid, "prof"),
        ).fetchone()[0]
    assert remaining == 0


def test_keyword_strict_excludes_results_without_token(store: SQLiteHistoryStore) -> None:
    """The 'trips' regression: SQL without the token must not be returned."""
    rid_yes = _insert_query(
        store,
        rid_external="q1",
        name="trips_count",
        sql_text="SELECT COUNT(*) FROM trips WHERE dt > '2020-01-01'",
    )
    _insert_query(
        store,
        rid_external="q2",
        name="users_count",
        sql_text="SELECT COUNT(*) FROM _amx_users",
    )
    rag = _StubRAGStore(
        {
            ("query", rid_yes): "trips_count",
        }
    )
    with store._connect() as conn:
        search = HybridAssetSearch(conn, rag)  # type: ignore[arg-type]
        hits = search.search("trips", kind="query", profile="prof", limit=10)
    assert [h.remote_id for h in hits] == [rid_yes]
    assert all(h.metadata.get("match_type") == "keyword_strict" for h in hits)


def test_keyword_strict_returns_empty_when_no_keyword_hit(store: SQLiteHistoryStore) -> None:
    """Zero FTS5 candidates in keyword_strict mode means zero hits."""
    _insert_query(store, rid_external="q1", name="users_count", sql_text="SELECT * FROM _amx_users")
    rag = _StubRAGStore({})
    with store._connect() as conn:
        search = HybridAssetSearch(conn, rag)  # type: ignore[arg-type]
        hits = search.search("trips", kind="query", profile="prof", limit=10)
    assert hits == []
    # No rerank call should have been issued — FTS gave us nothing.
    assert all(call["fn"] != "rerank" for call in rag.calls)


def test_auto_mode_falls_back_to_semantic_when_keyword_empty(
    store: SQLiteHistoryStore,
) -> None:
    """``auto`` mode runs FTS first, then semantic on zero matches."""
    _insert_query(store, rid_external="q1", name="users", sql_text="SELECT 1")
    rag = _StubRAGStore({})
    with store._connect() as conn:
        search = HybridAssetSearch(conn, rag)  # type: ignore[arg-type]
        search.search("trips", kind="query", profile="prof", limit=5, mode="auto")
    fn_calls = [c["fn"] for c in rag.calls]
    assert "query" in fn_calls, "auto mode should have fallen back to semantic query"


def test_semantic_only_bypasses_fts(store: SQLiteHistoryStore) -> None:
    """``semantic_only`` mode never touches FTS5."""
    rag = _StubRAGStore({})
    with store._connect() as conn:
        search = HybridAssetSearch(conn, rag)  # type: ignore[arg-type]
        search.search(
            "anything",
            kind="query",
            profile="prof",
            limit=5,
            mode="semantic_only",
        )
    fn_calls = [c["fn"] for c in rag.calls]
    assert fn_calls == ["query"]


def test_prefix_tokenisation_allows_short_prefix_match(store: SQLiteHistoryStore) -> None:
    """Search for ``trip`` should surface ``trips`` (FTS5 prefix)."""
    rid = _insert_query(
        store,
        rid_external="q1",
        name="trips_count",
        sql_text="SELECT COUNT(*) FROM trips",
    )
    rag = _StubRAGStore({("query", rid): "trips_count"})
    with store._connect() as conn:
        search = HybridAssetSearch(conn, rag)  # type: ignore[arg-type]
        hits = search.search("trip", kind="query", profile="prof", limit=5)
    assert [h.remote_id for h in hits] == [rid]
