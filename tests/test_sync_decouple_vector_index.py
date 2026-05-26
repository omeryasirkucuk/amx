"""Vector indexing must never block catalog metadata writes.

A user changed their catalog embedding profile (minilm → gte-small)
after the vector collection was built. Every subsequent `/search sync`
crashed at the indexing step with `CollectionIdentityMismatch`, and
because the structured-metadata write shared the call path, NO columns
or row counts were persisted for the affected tables — the catalog was
stuck at table-level skeleton data.

The structured metadata (columns, dtypes, row counts, FKs) lives in
SQLite and is independent of the embedding model; only semantic search
needs the vector index. These tests pin the decoupling: a vector
identity mismatch degrades semantic search (skips the index, sets a
flag) but the metadata still commits.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from amx.db.connector import AssetKind, ColumnProfile, TableProfile
from amx.rag_core.collection_identity import (
    CollectionIdentity,
    CollectionIdentityMismatch,
)
from amx.search.catalog import SearchCatalog
from amx.storage.sqlite_store import SQLiteHistoryStore


def _identity_mismatch() -> CollectionIdentityMismatch:
    """Build a realistic mismatch (minilm → gte-small) like the one a
    user hits after swapping the catalog embedding profile."""
    return CollectionIdentityMismatch(
        recorded=CollectionIdentity(embedding_provider="minilm", embedding_model="minilm-l6-v2"),
        active=CollectionIdentity(
            embedding_provider="sentence_transformers",
            embedding_model="thenlper/gte-small",
        ),
        recovery_hint="Run /search rebuild.",
    )


@pytest.fixture()
def fresh_catalog(tmp_path: Path) -> SearchCatalog:
    db = tmp_path / "history.db"
    store = SQLiteHistoryStore(db)
    store.init()
    return SearchCatalog(db)


def _profile() -> TableProfile:
    return TableProfile(
        schema="airline",
        name="Airports",
        asset_kind=AssetKind.TABLE,
        row_count=6510,
        existing_comment="Airport reference data",
        columns=[
            ColumnProfile(name="Code", dtype="TEXT", nullable=False, existing_comment="IATA code"),
            ColumnProfile(
                name="Description", dtype="TEXT", nullable=True, existing_comment="Airport name"
            ),
        ],
    )


def _column_names(catalog: SearchCatalog) -> list[str]:
    with catalog._connect() as conn:  # noqa: SLF001
        return [
            str(r["column_name"])
            for r in conn.execute(
                "SELECT column_name FROM catalog_entities "
                "WHERE entity_kind = 'column' ORDER BY column_name"
            )
        ]


def _table_row_count(catalog: SearchCatalog) -> int | None:
    with catalog._connect() as conn:  # noqa: SLF001
        row = conn.execute(
            "SELECT row_count FROM catalog_entities WHERE entity_kind = 'table' LIMIT 1"
        ).fetchone()
        return None if row is None else row["row_count"]


def test_sync_persists_metadata_when_indexing_identity_mismatch(
    fresh_catalog: SearchCatalog, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The headline regression: a CollectionIdentityMismatch during
    indexing must NOT abort the metadata write. Columns + row count
    land; the degradation flag is set so the caller can prompt a
    rebuild."""

    def _boom(_entities: list[dict[str, object]]) -> int:
        raise _identity_mismatch()

    monkeypatch.setattr(fresh_catalog.index, "upsert_entities", _boom)

    # Must NOT raise.
    fresh_catalog.sync_table_profile(
        db_profile="local-postgre",
        db_backend="postgresql",
        database_name="bird_train",
        profile=_profile(),
        query_usage={},
    )

    # Structured metadata persisted despite the indexing failure.
    assert _column_names(fresh_catalog) == ["Code", "Description"]
    assert _table_row_count(fresh_catalog) == 6510
    # The degradation flag is set so /search sync can tell the user to
    # run /search rebuild.
    assert getattr(fresh_catalog, "_index_degraded", False) is True


def test_sync_indexes_normally_when_no_mismatch(
    fresh_catalog: SearchCatalog, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Happy path: when indexing succeeds, the degradation flag stays
    unset and the indexed=1 stamp is written (no silent degradation)."""
    calls: list[int] = []

    def _ok(entities: list[dict[str, object]]) -> int:
        calls.append(len(entities))
        return len(entities)

    monkeypatch.setattr(fresh_catalog.index, "upsert_entities", _ok)

    fresh_catalog.sync_table_profile(
        db_profile="local-postgre",
        db_backend="postgresql",
        database_name="bird_train",
        profile=_profile(),
        query_usage={},
    )

    assert _column_names(fresh_catalog) == ["Code", "Description"]
    assert getattr(fresh_catalog, "_index_degraded", False) is False
    # Indexing actually ran (table + 2 columns → at least one upsert).
    assert calls


def test_indexing_warning_logged_once_not_per_entity(
    fresh_catalog: SearchCatalog, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A wide table must not spam one warning per column — the
    degradation hint logs at most once per sync run via the flag."""

    def _boom(_entities: list[dict[str, object]]) -> int:
        raise _identity_mismatch()

    monkeypatch.setattr(fresh_catalog.index, "upsert_entities", _boom)

    import logging

    with caplog.at_level(logging.WARNING, logger="search.catalog.entity_crud"):
        fresh_catalog.sync_table_profile(
            db_profile="p",
            db_backend="postgresql",
            database_name="d",
            profile=_profile(),
            query_usage={},
        )

    skip_warnings = [r for r in caplog.records if "Semantic indexing skipped" in r.getMessage()]
    # One sync_table_profile call → exactly one degradation warning,
    # even though the table has multiple column entities to index.
    assert len(skip_warnings) == 1
