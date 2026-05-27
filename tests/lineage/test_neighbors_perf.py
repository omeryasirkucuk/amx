"""Sanity bound: the shared core stays fast on a wide graph.

Not a micro-benchmark — a generous ceiling that catches accidental
N+1 / O(n^2) regressions without being timing-flaky.
"""

from __future__ import annotations

import time
from pathlib import Path

from amx.lineage.neighbors import lineage_neighbors
from amx.storage.sqlite_store import SQLiteHistoryStore


def test_wide_graph_under_generous_bound(tmp_path: Path) -> None:
    hs = SQLiteHistoryStore(tmp_path / "history.db")
    hs.init()
    with hs._connect() as conn:
        cur = conn.execute(
            "INSERT INTO catalog_entities (db_profile, db_backend, database_name, "
            "schema_name, table_name, entity_kind, asset_kind, search_text, metadata_state) "
            "VALUES ('dbr','databricks','wh','s','anchor','table','table','','full')"
        )
        anchor = int(cur.lastrowid)
        now = time.time()
        for i in range(200):
            cur = conn.execute(
                "INSERT INTO catalog_entities (db_profile, db_backend, database_name, "
                "schema_name, table_name, entity_kind, asset_kind, search_text, metadata_state) "
                "VALUES ('dbr','databricks','wh','s',?,'table','table','','full')",
                (f"p{i}",),
            )
            pid = int(cur.lastrowid)
            conn.execute(
                "INSERT INTO catalog_relationships (from_entity_id, to_entity_id, "
                "relationship_type, score, source, details_json, last_seen, "
                "from_entity_kind, to_entity_kind) VALUES (?,?,?,1.0,'native','{}',?, 'table','table')",
                (pid, anchor, "lineage_native_table", now),
            )

    with hs._connect() as conn:
        start = time.perf_counter()
        out = lineage_neighbors(conn, anchor_entity_ids=[anchor])
        elapsed = time.perf_counter() - start

    # Capped output (DEFAULT_FANOUT) and a comfortable time ceiling.
    assert len(out[anchor]) == 12
    assert elapsed < 0.5
