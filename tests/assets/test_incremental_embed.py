"""PR-D: incremental-embed gate driven by ``last_embedded_hash``.

The actual Chroma round-trip lives behind an optional dep
(``sentence_transformers``), so these tests target the per-row hash
helpers directly. They cover:

* the migration adds ``last_embedded_hash`` to legacy DBs,
* every schema-description entry is non-empty (CI schema gate),
* ``_current_hashes_for_kinds`` reads the right column per kind,
* ``_last_embedded_hashes_for_kinds`` reads the new column,
* ``_update_last_embedded_hashes`` stamps the column after embed,
* ``_clear_last_embedded_hashes`` NULLs the column on force-reindex,
* ``_clear_last_embedded_hash_for_row`` NULLs a single row on the
  chunking-override PUT/DELETE path.
"""

from __future__ import annotations

import sqlite3

import pytest

from amx.assets.rag import (
    _clear_last_embedded_hash_for_row,
    _clear_last_embedded_hashes,
    _current_hashes_for_kinds,
    _hashable_for_pipeline,
    _last_embedded_hashes_for_kinds,
    _update_last_embedded_hashes,
)
from amx.assets.types import AssetDocument
from amx.storage.schema_descriptions import SCHEMA_DESCRIPTIONS
from amx.storage.sqlite_store import SQLiteHistoryStore


def _make_store(tmp_path):
    db_path = tmp_path / "history.db"
    SQLiteHistoryStore(db_path).init()
    return db_path


def _seed_notebook(conn, *, name="nb1", source_hash="h-source", last_embedded_hash=None):
    conn.execute(
        """
        INSERT INTO remote_notebooks
            (profile_name, platform, external_id, name, workspace_path,
             qualified_name, language, source_text, source_hash,
             last_modified_at, last_modified_by, owner, cell_count,
             last_embedded_hash, ingested_at)
        VALUES ('prod', 'databricks', ?, ?, '/n', NULL, 'python',
                '{}', ?, NULL, NULL, NULL, 1, ?, '2026-05-22')
        """,
        (f"ext-{name}", name, source_hash, last_embedded_hash),
    )
    return conn.execute("SELECT last_insert_rowid()").fetchone()[0]


def _seed_query(conn, *, name="q1", sql_hash="h-sql", last_embedded_hash=None):
    conn.execute(
        """
        INSERT INTO remote_queries
            (profile_name, platform, kind, external_id, name, sql_text,
             sql_hash, warehouse, user_name, executed_at, duration_ms,
             last_embedded_hash, ingested_at)
        VALUES ('prod', 'databricks', 'saved', ?, ?, 'SELECT 1', ?,
                NULL, NULL, NULL, NULL, ?, '2026-05-22')
        """,
        (f"ext-{name}", name, sql_hash, last_embedded_hash),
    )
    return conn.execute("SELECT last_insert_rowid()").fetchone()[0]


def _seed_pipeline(conn, *, name="p1", libraries='["a"]', state="RUNNING", last_embedded_hash=None):
    conn.execute(
        """
        INSERT INTO remote_pipelines
            (profile_name, pipeline_id, name, target_schema, edition,
             continuous, photon, libraries_json, latest_update_state,
             latest_update_creation_time, last_embedded_hash, ingested_at)
        VALUES ('prod', ?, ?, 'analytics', 'CORE', 0, 0, ?, ?, NULL, ?, '2026-05-22')
        """,
        (f"ext-{name}", name, libraries, state, last_embedded_hash),
    )
    return conn.execute("SELECT last_insert_rowid()").fetchone()[0]


# ── schema migration + description coverage ──────────────────────────────


def test_migration_adds_last_embedded_hash_to_legacy_db(tmp_path):
    """A pre-PR-D history.db without the column gains it on init()."""
    db_path = tmp_path / "history.db"
    # Build a minimal legacy schema by hand — no last_embedded_hash columns.
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "CREATE TABLE remote_notebooks ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, profile_name TEXT NOT NULL, "
            "platform TEXT NOT NULL, external_id TEXT NOT NULL, name TEXT NOT NULL, "
            "workspace_path TEXT, qualified_name TEXT, language TEXT NOT NULL, "
            "source_text TEXT NOT NULL, source_hash TEXT NOT NULL, "
            "last_modified_at TIMESTAMP, last_modified_by TEXT, owner TEXT, "
            "cell_count INTEGER, ingested_at TIMESTAMP NOT NULL)"
        )
        conn.execute(
            "CREATE TABLE remote_queries ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, profile_name TEXT NOT NULL, "
            "platform TEXT NOT NULL, kind TEXT NOT NULL, external_id TEXT NOT NULL, "
            "name TEXT, sql_text TEXT NOT NULL, sql_hash TEXT NOT NULL, "
            "warehouse TEXT, user_name TEXT, executed_at TIMESTAMP, "
            "duration_ms INTEGER, ingested_at TIMESTAMP NOT NULL)"
        )
        conn.execute(
            "CREATE TABLE remote_pipelines ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, profile_name TEXT NOT NULL, "
            "pipeline_id TEXT NOT NULL, name TEXT NOT NULL, target_schema TEXT, "
            "edition TEXT, continuous INTEGER NOT NULL, photon INTEGER NOT NULL, "
            "libraries_json TEXT NOT NULL, latest_update_state TEXT, "
            "latest_update_creation_time TIMESTAMP, ingested_at TIMESTAMP NOT NULL)"
        )
        conn.commit()
    SQLiteHistoryStore(db_path).init()
    with sqlite3.connect(db_path) as conn:
        for table in ("remote_notebooks", "remote_queries", "remote_pipelines"):
            cols = {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
            assert "last_embedded_hash" in cols, f"{table} migration didn't add last_embedded_hash"


def test_schema_descriptions_cover_last_embedded_hash():
    """House rule §5: every internal column ships with a description."""
    for table in ("remote_notebooks", "remote_queries", "remote_pipelines"):
        desc = SCHEMA_DESCRIPTIONS.get(table, {})
        assert desc.get("last_embedded_hash"), (
            f"{table}.last_embedded_hash missing schema description"
        )


# ── hash helpers ──────────────────────────────────────────────────────────


def test_current_hashes_reads_source_hash_for_notebooks(tmp_path):
    db_path = _make_store(tmp_path)
    with sqlite3.connect(db_path) as conn:
        rid = _seed_notebook(conn, source_hash="abc123")
        conn.commit()
        docs = [
            AssetDocument(
                kind="notebook",
                profile="prod",
                remote_id=rid,
                chunk_index=0,
                text="...",
            )
        ]
        hashes = _current_hashes_for_kinds(conn, "prod", docs)
    assert hashes == {("notebook", rid): "abc123"}


def test_current_hashes_reads_sql_hash_for_queries(tmp_path):
    db_path = _make_store(tmp_path)
    with sqlite3.connect(db_path) as conn:
        rid = _seed_query(conn, sql_hash="def456")
        conn.commit()
        docs = [
            AssetDocument(kind="query", profile="prod", remote_id=rid, chunk_index=0, text="...")
        ]
        hashes = _current_hashes_for_kinds(conn, "prod", docs)
    assert hashes == {("query", rid): "def456"}


def test_current_hashes_synthesizes_pipeline_hash(tmp_path):
    """Pipelines have no native hash; the helper derives one from
    the libraries_json + latest_update_state envelope.
    """
    db_path = _make_store(tmp_path)
    expected = _hashable_for_pipeline('["a"]::RUNNING')
    with sqlite3.connect(db_path) as conn:
        rid = _seed_pipeline(conn, libraries='["a"]', state="RUNNING")
        conn.commit()
        docs = [
            AssetDocument(
                kind="pipeline",
                profile="prod",
                remote_id=rid,
                chunk_index=0,
                text="...",
            )
        ]
        hashes = _current_hashes_for_kinds(conn, "prod", docs)
    assert hashes == {("pipeline", rid): expected}


def test_current_hashes_ignores_unhashable_kinds(tmp_path):
    """Jobs / streams / streamlit_apps return no hash — they always
    re-embed in the incremental path.
    """
    db_path = _make_store(tmp_path)
    with sqlite3.connect(db_path) as conn:
        docs = [
            AssetDocument(kind="job", profile="prod", remote_id=99, chunk_index=0, text="..."),
            AssetDocument(kind="stream", profile="prod", remote_id=99, chunk_index=0, text="..."),
        ]
        hashes = _current_hashes_for_kinds(conn, "prod", docs)
    assert hashes == {}


def test_last_embedded_returns_stored_value(tmp_path):
    db_path = _make_store(tmp_path)
    with sqlite3.connect(db_path) as conn:
        rid = _seed_notebook(conn, source_hash="curr", last_embedded_hash="prev")
        conn.commit()
        docs = [
            AssetDocument(
                kind="notebook",
                profile="prod",
                remote_id=rid,
                chunk_index=0,
                text="...",
            )
        ]
        out = _last_embedded_hashes_for_kinds(conn, "prod", docs)
    assert out == {("notebook", rid): "prev"}


def test_last_embedded_returns_none_for_fresh_row(tmp_path):
    db_path = _make_store(tmp_path)
    with sqlite3.connect(db_path) as conn:
        rid = _seed_notebook(conn, source_hash="curr", last_embedded_hash=None)
        conn.commit()
        docs = [
            AssetDocument(
                kind="notebook",
                profile="prod",
                remote_id=rid,
                chunk_index=0,
                text="...",
            )
        ]
        out = _last_embedded_hashes_for_kinds(conn, "prod", docs)
    assert out == {("notebook", rid): None}


def test_update_last_embedded_stamps_current_hash(tmp_path):
    db_path = _make_store(tmp_path)
    with sqlite3.connect(db_path) as conn:
        rid = _seed_notebook(conn, source_hash="new-hash", last_embedded_hash=None)
        conn.commit()
        docs = [
            AssetDocument(
                kind="notebook",
                profile="prod",
                remote_id=rid,
                chunk_index=0,
                text="...",
            )
        ]
        _update_last_embedded_hashes(conn, "prod", docs)
        stored = conn.execute(
            "SELECT last_embedded_hash FROM remote_notebooks WHERE id = ?", (rid,)
        ).fetchone()[0]
    assert stored == "new-hash"


def test_clear_last_embedded_hashes_nulls_the_column(tmp_path):
    db_path = _make_store(tmp_path)
    with sqlite3.connect(db_path) as conn:
        rid_nb = _seed_notebook(conn, last_embedded_hash="stamped")
        rid_q = _seed_query(conn, last_embedded_hash="stamped")
        rid_p = _seed_pipeline(conn, last_embedded_hash="stamped")
        conn.commit()
        _clear_last_embedded_hashes(conn, "prod")
        nb_h = conn.execute(
            "SELECT last_embedded_hash FROM remote_notebooks WHERE id = ?", (rid_nb,)
        ).fetchone()[0]
        q_h = conn.execute(
            "SELECT last_embedded_hash FROM remote_queries WHERE id = ?", (rid_q,)
        ).fetchone()[0]
        p_h = conn.execute(
            "SELECT last_embedded_hash FROM remote_pipelines WHERE id = ?", (rid_p,)
        ).fetchone()[0]
    assert nb_h is None and q_h is None and p_h is None


def test_clear_last_embedded_hash_for_row_scoped(tmp_path):
    """The single-row helper must not affect siblings — that's the
    whole point of the per-asset chunking-override hook.
    """
    db_path = _make_store(tmp_path)
    with sqlite3.connect(db_path) as conn:
        rid_a = _seed_notebook(conn, name="a", last_embedded_hash="stamped")
        rid_b = _seed_notebook(conn, name="b", last_embedded_hash="stamped")
        conn.commit()
        _clear_last_embedded_hash_for_row(conn, "prod", "notebook", rid_a)
        a = conn.execute(
            "SELECT last_embedded_hash FROM remote_notebooks WHERE id = ?", (rid_a,)
        ).fetchone()[0]
        b = conn.execute(
            "SELECT last_embedded_hash FROM remote_notebooks WHERE id = ?", (rid_b,)
        ).fetchone()[0]
    assert a is None
    assert b == "stamped"


def test_clear_last_embedded_hash_for_row_ignores_unhashable_kind(tmp_path):
    """Jobs aren't tracked; the helper is a no-op on them rather than
    blowing up.
    """
    db_path = _make_store(tmp_path)
    with sqlite3.connect(db_path) as conn:
        # No remote_jobs table change — helper should silently noop.
        _clear_last_embedded_hash_for_row(conn, "prod", "job", 1)


# ── reindex --force CLI surface ──────────────────────────────────────────


@pytest.mark.parametrize("force_flag", [True, False])
def test_reindex_command_accepts_force_flag(force_flag, monkeypatch):
    """Ensure the new --force flag exists on /db assets reindex.

    Doesn't exercise the embed path (no chromadb in CI); just
    verifies argparse + the forward to ``run_reindex``.
    """
    from amx.cli_support.commands import db_assets_impl as impl

    captured = {}

    def fake_run_reindex(cfg, *, profile, skip_confirm, force=False):
        captured["force"] = force
        captured["profile"] = profile
        captured["skip_confirm"] = skip_confirm

    monkeypatch.setattr(impl, "run_reindex", fake_run_reindex)
    import click
    from click.testing import CliRunner

    from amx.cli_support.commands.db_assets import register_db_assets_commands

    pass_config = click.make_pass_decorator(object, ensure=True)

    @click.group()
    @click.pass_context
    def root(ctx):
        if ctx.obj is None:
            from amx.config import AMXConfig

            ctx.obj = AMXConfig()

    @root.group()
    def db():
        pass

    register_db_assets_commands(db, pass_config=pass_config)
    args = ["db", "assets", "reindex", "--profile", "prod", "-y"]
    if force_flag:
        args.append("--force")
    result = CliRunner().invoke(root, args, catch_exceptions=False)
    assert result.exit_code == 0, result.output
    assert captured["force"] is force_flag
