"""Smoke test for the SSE streaming lineage suggest pipeline."""

from __future__ import annotations

from pathlib import Path

import pytest

from amx.config import AMXConfig
from amx.lineage import service
from amx.lineage.types import ColumnRef, Scope
from amx.storage.sqlite_store import SQLiteHistoryStore
from tests.lineage.conftest import seed_table_entity


@pytest.fixture
def store_with_anchor(tmp_path: Path) -> tuple[SQLiteHistoryStore, str]:
    store = SQLiteHistoryStore(tmp_path / "history.db")
    store.init()
    seed_table_entity(
        store,
        profile="p1",
        backend="postgresql",
        database="db1",
        schema="public",
        table="anchor",
    )
    return store, "p1"


def test_stream_emits_done_event(store_with_anchor: tuple[SQLiteHistoryStore, str]):
    """Smoke: the generator yields a terminal `done` event even when
    no extractor produces anything. The cache-only run on an empty
    catalog returns zero edges; SSE protocol still requires the
    `done` chunk so the consumer cleanly closes the channel."""
    store, profile = store_with_anchor
    scope = Scope(
        profile=profile,
        anchor=ColumnRef(database="db1", schema="public", table="anchor", column=""),
        depth_up=1,
        depth_down=1,
        database="db1",
        schema="public",
    )
    cfg = AMXConfig()
    chunks = list(service.stream_suggest_lineage(store, scope, cfg))
    assert chunks, "stream should emit at least one chunk"
    joined = "".join(chunks)
    assert "event: done" in joined
    assert "data:" in joined


def test_stream_event_format(store_with_anchor: tuple[SQLiteHistoryStore, str]):
    """Each yielded chunk is a well-formed SSE message: it has an
    ``event:`` line and a ``data:`` line separated from the next chunk
    by a blank line. Consumer (useStreamingAI) relies on this shape."""
    store, profile = store_with_anchor
    scope = Scope(
        profile=profile,
        anchor=ColumnRef(database="db1", schema="public", table="anchor", column=""),
        depth_up=1,
        depth_down=1,
        database="db1",
        schema="public",
    )
    chunks = list(service.stream_suggest_lineage(store, scope, AMXConfig()))
    for chunk in chunks:
        assert chunk.startswith("event:"), f"chunk must start with event:: {chunk!r}"
        assert "\ndata:" in chunk
        assert chunk.endswith("\n\n"), "chunk must terminate with blank line"
