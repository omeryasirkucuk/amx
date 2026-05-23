"""GET /api/assets/by-table?profile=...&schema=...&table=... — reverse asset lookup."""

from __future__ import annotations

import sqlite3
import time

from amx.config import AMXConfig
from amx.storage.sqlite_store import SQLiteHistoryStore
from amx.web.server import create_app

_TEST_TOKEN = "test-table-assets-token"
_AUTH = {"Authorization": f"Bearer {_TEST_TOKEN}"}


def _make_client(tmp_path):
    from fastapi.testclient import TestClient

    cfg = AMXConfig()
    cfg.CONFIG_DIR = str(tmp_path)
    db_path = tmp_path / "history.db"
    SQLiteHistoryStore(db_path).init()
    app = create_app(cfg, token=_TEST_TOKEN)
    return TestClient(app), db_path


def _seed_table(db_path, *, profile="prod", catalog="prod", schema="sales", name="orders") -> int:
    with sqlite3.connect(db_path) as conn:
        cur = conn.execute(
            """
            INSERT INTO catalog_entities
                (db_profile, db_backend, database_name, schema_name, table_name,
                 entity_kind)
            VALUES (?, 'databricks', ?, ?, ?, 'table')
            """,
            (profile, catalog, schema, name),
        )
        conn.commit()
        return int(cur.lastrowid)


def _seed_query(
    db_path,
    *,
    profile="prod",
    external_id="q1",
    name="ingest_orders",
) -> int:
    with sqlite3.connect(db_path) as conn:
        cur = conn.execute(
            """
            INSERT INTO remote_queries
                (profile_name, platform, kind, external_id, name, sql_text,
                 sql_hash, ingested_at)
            VALUES (?, 'databricks', 'saved', ?, ?, 'SELECT 1', 'h-' || ?, ?)
            """,
            (profile, external_id, name, external_id, time.time()),
        )
        conn.commit()
        return int(cur.lastrowid)


def _seed_notebook(
    db_path,
    *,
    profile="prod",
    external_id="n1",
    name="loader_nb",
    path="/Workspace/loader",
) -> int:
    with sqlite3.connect(db_path) as conn:
        cur = conn.execute(
            """
            INSERT INTO remote_notebooks
                (profile_name, platform, external_id, name, workspace_path,
                 language, source_text, source_hash, ingested_at)
            VALUES (?, 'databricks', ?, ?, ?, 'sql', '{}', 'h-' || ?, ?)
            """,
            (profile, external_id, name, path, external_id, time.time()),
        )
        conn.commit()
        return int(cur.lastrowid)


def _seed_edge(
    db_path,
    *,
    profile: str,
    from_kind: str,
    from_id: int,
    to_id: int,
    edge_type: str,
    direction: str | None = None,
    last_used_at: float | None = None,
    last_user: str | None = None,
    discovered_at: float | None = None,
) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO asset_lineage_edges
                (profile_name, from_kind, from_id, to_kind, to_id, edge_type,
                 raw_ref, discovered_at, direction, last_used_at, last_user)
            VALUES (?, ?, ?, 'table', ?, ?, '{}', ?, ?, ?, ?)
            """,
            (
                profile,
                from_kind,
                from_id,
                to_id,
                edge_type,
                discovered_at if discovered_at is not None else time.time(),
                direction,
                last_used_at,
                last_user,
            ),
        )
        conn.commit()


def test_returns_404_for_missing_table(tmp_path):
    client, _db = _make_client(tmp_path)
    resp = client.get(
        "/api/assets/by-table?profile=prod&schema=sales&table=ghost",
        headers=_AUTH,
    )
    assert resp.status_code == 404


def test_groups_reads_and_writes_by_direction(tmp_path):
    client, db_path = _make_client(tmp_path)
    table_id = _seed_table(db_path, name="orders")
    nb_id = _seed_notebook(db_path)
    q_id = _seed_query(db_path)
    _seed_edge(
        db_path,
        profile="prod",
        from_kind="notebook",
        from_id=nb_id,
        to_id=table_id,
        edge_type="notebook_reads_table",
        direction="read",
        last_used_at=time.time(),
        last_user="alice@example.com",
    )
    _seed_edge(
        db_path,
        profile="prod",
        from_kind="query",
        from_id=q_id,
        to_id=table_id,
        edge_type="query_writes_table",
        direction="write",
    )

    resp = client.get(
        "/api/assets/by-table?profile=prod&schema=sales&table=orders&database=prod",
        headers=_AUTH,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["table"]["fqn"] == "prod.sales.orders"
    assert [r["kind"] for r in body["reads"]] == ["notebook"]
    assert body["reads"][0]["last_user"] == "alice@example.com"
    assert [w["kind"] for w in body["writes"]] == ["query"]
    assert body["counts"]["notebook"] == 1
    assert body["counts"]["query"] == 1


def test_legacy_edge_without_direction_falls_back_to_edge_type(tmp_path):
    """Rows written before the direction column existed still classify
    correctly via the edge_type → side mapping."""
    client, db_path = _make_client(tmp_path)
    table_id = _seed_table(db_path)
    nb_id = _seed_notebook(db_path)
    _seed_edge(
        db_path,
        profile="prod",
        from_kind="notebook",
        from_id=nb_id,
        to_id=table_id,
        edge_type="task_runs_notebook",
        direction=None,
    )
    resp = client.get(
        "/api/assets/by-table?profile=prod&schema=sales&table=orders&database=prod",
        headers=_AUTH,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    # task_runs_notebook is a read in the fallback mapping.
    assert len(body["reads"]) == 1
    assert body["reads"][0]["direction"] == "read"


def test_unknown_direction_surfaces_on_both_sides(tmp_path):
    """An edge with direction='both' (or an unrecognised edge_type) shows
    up under both reads and writes so the UI can render it neutrally."""
    client, db_path = _make_client(tmp_path)
    table_id = _seed_table(db_path)
    q_id = _seed_query(db_path)
    _seed_edge(
        db_path,
        profile="prod",
        from_kind="query",
        from_id=q_id,
        to_id=table_id,
        edge_type="something_custom",
        direction="both",
    )
    body = client.get(
        "/api/assets/by-table?profile=prod&schema=sales&table=orders&database=prod",
        headers=_AUTH,
    ).json()
    assert len(body["reads"]) == 1 and body["reads"][0]["direction"] == "unknown"
    assert len(body["writes"]) == 1 and body["writes"][0]["direction"] == "unknown"


def test_direction_filter_narrows_lists_without_changing_counts(tmp_path):
    client, db_path = _make_client(tmp_path)
    table_id = _seed_table(db_path)
    nb_id = _seed_notebook(db_path)
    q_id = _seed_query(db_path)
    _seed_edge(
        db_path,
        profile="prod",
        from_kind="notebook",
        from_id=nb_id,
        to_id=table_id,
        edge_type="notebook_reads_table",
        direction="read",
    )
    _seed_edge(
        db_path,
        profile="prod",
        from_kind="query",
        from_id=q_id,
        to_id=table_id,
        edge_type="query_writes_table",
        direction="write",
    )

    reads_only = client.get(
        "/api/assets/by-table?profile=prod&schema=sales&table=orders&database=prod&direction=read",
        headers=_AUTH,
    ).json()
    assert reads_only["reads"] and not reads_only["writes"]
    # Counts mirror the full edge set, not the filtered view, so the
    # UI badges can show "12 notebooks · 47 queries" regardless of
    # which tab the user clicked.
    assert reads_only["counts"]["query"] == 1


def test_since_days_window_drops_stale_edges(tmp_path):
    """An edge whose last activity is older than the window is omitted."""
    client, db_path = _make_client(tmp_path)
    table_id = _seed_table(db_path)
    nb_id = _seed_notebook(db_path)
    old = time.time() - 200 * 86400
    _seed_edge(
        db_path,
        profile="prod",
        from_kind="notebook",
        from_id=nb_id,
        to_id=table_id,
        edge_type="notebook_reads_table",
        direction="read",
        last_used_at=old,
        discovered_at=old,
    )
    body = client.get(
        "/api/assets/by-table?profile=prod&schema=sales&table=orders&database=prod&since_days=30",
        headers=_AUTH,
    ).json()
    assert body["reads"] == []
    # Setting since_days=0 disables the window and the edge resurfaces.
    body_all = client.get(
        "/api/assets/by-table?profile=prod&schema=sales&table=orders&database=prod&since_days=0",
        headers=_AUTH,
    ).json()
    assert len(body_all["reads"]) == 1
