"""SQLite FTS5 sidecar for the Document RAG store (PR-E).

The Document RAG store historically did pure dense-vector retrieval
via Chroma. The audit's failure-mode 2 (\"insufficient vector-only\")
called for a BM25 lexical channel alongside the dense one, fused
with Reciprocal Rank Fusion. This module owns the lexical channel:
a SQLite FTS5 virtual table that mirrors every Chroma chunk by
``chunk_id``, indexed by content, supporting BM25-ranked MATCH
queries.

Why FTS5 instead of a separate process (Elasticsearch, OpenSearch,
Tantivy)?

- Zero new runtime dep — SQLite ships with Python; FTS5 is enabled
  by default on every distribution AMX supports.
- Same persistence dir as Chroma (``~/.amx/chroma_db/docs_fts.sqlite``)
  so backup / restore lifts both together.
- BM25 built in (``bm25()`` function on the virtual table).

Schema:

    CREATE VIRTUAL TABLE chunks_fts USING fts5(
        chunk_id UNINDEXED,
        source UNINDEXED,
        content,
        tokenize='porter unicode61'
    );

``chunk_id`` is the same string Chroma uses
(``f\"{doc.path}::{i}\"``) so a fused result-set is trivial to look
up back in Chroma metadata for citation. ``UNINDEXED`` means FTS5
keeps the column readable but does not tokenise it.

The schema also stores the LangChain Document metadata fields the
caller needs at retrieval time (``source``) so a fused result-set
can be filtered by ``source_filters`` before fusion without a
second round-trip into Chroma.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any

from amx.utils.logging import get_logger

log = get_logger("docs.fts5_sidecar")

# Bump when the on-disk schema changes incompatibly. The sidecar
# stores this in a one-row meta table so an older AMX opening a
# newer file (or vice-versa) can bail cleanly instead of producing
# corrupt fused rankings.
_SCHEMA_VERSION = 1

_TABLE_NAME = "chunks_fts"
_META_TABLE_NAME = "fts_meta"


class FTS5Sidecar:
    """SQLite FTS5 mirror of the Document RAG chunks.

    Caller-owned lifecycle: ``RAGStore`` constructs one in
    ``__init__``, calls ``upsert``/``delete_by_*`` from
    ``ingest``/``delete_chunks_for_sources``, calls ``query`` from
    ``RAGStore.query``, and never closes the connection explicitly
    (the SQLite connection is kept open for the life of the
    process; AMX is a single-process CLI / Studio server).

    All public methods are best-effort — a SQLite failure logs and
    degrades gracefully (returns empty results / silently drops the
    write) rather than raising. The vector path is the load-bearing
    one; the FTS5 channel is supplementary.
    """

    def __init__(self, persist_dir: Path | str) -> None:
        self.persist_dir = Path(persist_dir)
        self.persist_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.persist_dir / "docs_fts.sqlite"
        try:
            self._conn: sqlite3.Connection | None = sqlite3.connect(
                str(self.db_path),
                isolation_level=None,  # autocommit; we manage transactions
                check_same_thread=False,  # AMX threads queries via the timeout executor
            )
            self._ensure_schema()
        except sqlite3.Error as exc:  # pragma: no cover — FTS5 is bundled
            log.warning("Could not open FTS5 sidecar at %s: %s", self.db_path, exc)
            self._conn = None

    # ── schema ─────────────────────────────────────────────────────

    def _ensure_schema(self) -> None:
        assert self._conn is not None
        # FTS5 virtual table. ``tokenize='porter unicode61'`` does
        # Porter stemming on top of Unicode-aware tokenisation —
        # ``order`` matches ``ordering``, ``ordered``, etc., which is
        # the right default for English prose AMX corpora.
        self._conn.execute(
            f"""
            CREATE VIRTUAL TABLE IF NOT EXISTS {_TABLE_NAME} USING fts5(
                chunk_id UNINDEXED,
                source UNINDEXED,
                content,
                tokenize='porter unicode61'
            )
            """
        )
        # Meta table for schema-version stamping.
        self._conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {_META_TABLE_NAME} (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        self._conn.execute(
            f"INSERT OR REPLACE INTO {_META_TABLE_NAME}(key, value) VALUES (?, ?)",
            ("schema_version", str(_SCHEMA_VERSION)),
        )

    # ── writes ─────────────────────────────────────────────────────

    def upsert(self, rows: Iterable[tuple[str, str, str]]) -> int:
        """Insert / replace chunks. ``rows`` is an iterable of
        ``(chunk_id, source, content)`` triples.

        FTS5 has no UPSERT primitive, so we do a transactional
        DELETE-then-INSERT. ``rowid`` is auto-assigned; we never
        expose it because callers identify chunks by ``chunk_id``.
        Returns the number of rows actually inserted (skipping rows
        with empty content, which would otherwise pollute the
        index with zero-length tokens).
        """
        if self._conn is None:
            return 0
        materialised = [
            (cid, src, content)
            for cid, src, content in rows
            if cid and isinstance(content, str) and content.strip()
        ]
        if not materialised:
            return 0
        ids = [row[0] for row in materialised]
        placeholders = ",".join("?" for _ in ids)
        try:
            self._conn.execute("BEGIN")
            self._conn.execute(
                f"DELETE FROM {_TABLE_NAME} WHERE chunk_id IN ({placeholders})",
                ids,
            )
            self._conn.executemany(
                f"INSERT INTO {_TABLE_NAME}(chunk_id, source, content) VALUES (?, ?, ?)",
                materialised,
            )
            self._conn.execute("COMMIT")
        except sqlite3.Error as exc:  # pragma: no cover — rare
            log.warning("FTS5 sidecar upsert failed: %s", exc)
            try:
                self._conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            return 0
        return len(materialised)

    def delete_by_ids(self, ids: Sequence[str]) -> int:
        """Delete rows by chunk_id. Returns rows deleted (or 0 on
        any error)."""
        if self._conn is None or not ids:
            return 0
        placeholders = ",".join("?" for _ in ids)
        try:
            cursor = self._conn.execute(
                f"DELETE FROM {_TABLE_NAME} WHERE chunk_id IN ({placeholders})",
                list(ids),
            )
            return cursor.rowcount or 0
        except sqlite3.Error as exc:  # pragma: no cover — rare
            log.warning("FTS5 sidecar delete_by_ids failed: %s", exc)
            return 0

    def delete_by_source(self, source: str) -> int:
        """Delete all rows for the given ``source`` path. Used by
        ``RAGStore.delete_chunks_for_sources`` and by the orphan-
        chunk reaper in ``ingest``."""
        if self._conn is None or not source:
            return 0
        try:
            cursor = self._conn.execute(
                f"DELETE FROM {_TABLE_NAME} WHERE source = ?",
                (source,),
            )
            return cursor.rowcount or 0
        except sqlite3.Error as exc:  # pragma: no cover — rare
            log.warning("FTS5 sidecar delete_by_source failed: %s", exc)
            return 0

    # ── reads ──────────────────────────────────────────────────────

    def query(
        self,
        text: str,
        *,
        k: int = 20,
        source_filters: Sequence[str] | None = None,
    ) -> list[tuple[str, float]]:
        """BM25-ranked top-k chunk_ids for the query ``text``.

        Returns a list of ``(chunk_id, score)`` tuples in best-first
        order. The score is ``-bm25(table)`` so higher is better
        (FTS5's bm25 returns negative numbers where closer-to-zero
        means more relevant; we flip the sign so callers don't have
        to remember).

        ``source_filters`` is an optional list of source paths the
        caller wants to restrict to — passed through as an
        ``IN (...)`` clause so the lexical channel respects the
        same scoping the vector channel does.

        Returns ``[]`` on any error, empty input, or empty result —
        callers fall back to vector-only retrieval.
        """
        if self._conn is None or not text or not text.strip():
            return []
        match_query = _sanitise_match(text)
        if not match_query:
            return []
        params: list[Any] = [match_query]
        where_extra = ""
        if source_filters:
            placeholders = ",".join("?" for _ in source_filters)
            where_extra = f" AND source IN ({placeholders})"
            params.extend(source_filters)
        params.append(int(k))
        try:
            rows = self._conn.execute(
                f"""
                SELECT chunk_id, -bm25({_TABLE_NAME}) AS score
                FROM {_TABLE_NAME}
                WHERE {_TABLE_NAME} MATCH ?{where_extra}
                ORDER BY bm25({_TABLE_NAME})
                LIMIT ?
                """,
                params,
            ).fetchall()
        except sqlite3.Error as exc:
            log.warning("FTS5 sidecar query failed (q=%r): %s", text, exc)
            return []
        return [(str(chunk_id), float(score)) for chunk_id, score in rows]

    def count(self) -> int:
        """Total rows in the FTS table. Used by ``RAGStore`` to
        detect a stale sidecar (Chroma populated but FTS empty) on
        first open under PR-E."""
        if self._conn is None:
            return 0
        try:
            row = self._conn.execute(f"SELECT COUNT(*) FROM {_TABLE_NAME}").fetchone()
        except sqlite3.Error:
            return 0
        return int(row[0]) if row else 0


# ── helpers ────────────────────────────────────────────────────────────


def _sanitise_match(text: str) -> str:
    """Turn a user query string into a safe FTS5 ``MATCH`` clause.

    FTS5's MATCH parser recognises operators (``AND``, ``OR``,
    ``NOT``, ``NEAR``, column qualifiers, quoted phrases) that would
    raise ``no such column`` / ``syntax error`` on natural-language
    questions. We extract alphanumeric tokens (Unicode-aware via the
    Python regex), drop the very-short ones (< 2 chars; would match
    almost everything), and join with implicit AND-of-OR semantics:
    ``token1 OR token2 OR ...`` with each token double-quoted to
    suppress operator interpretation.

    Empty input or no usable tokens → empty string (caller treats
    that as \"no FTS results\").
    """
    import re

    tokens = re.findall(r"[A-Za-z0-9_]+", str(text or ""), flags=re.UNICODE)
    safe = [t for t in tokens if len(t) >= 2]
    if not safe:
        return ""
    # Quote each token to make sure FTS5 treats it as a literal, not
    # an operator. ``"and"`` is a token, not the AND operator.
    return " OR ".join(f'"{token}"' for token in safe)


__all__ = ["FTS5Sidecar"]
