"""Non-negotiable performance guards for ``/lineage create``.

Per the slice-1 contract: a warm-cache run on a 5,000-table / 500-view
catalog must complete in under 5 seconds. Anything slower means a
regression in the cache-first plumbing.
"""

from __future__ import annotations

import json
import time
from unittest.mock import patch

from amx.lineage import service
from amx.lineage import store as lineage_store
from amx.lineage.extractors.view_ddl import ConnectorHandle
from amx.lineage.types import ColumnRef, Scope

from .conftest import (
    seed_foreign_key_relationship,
    seed_table_entity,
)

PERF_BUDGET_SECONDS = 5.0


def _seed_large_catalog(hs, *, tables: int = 5000, views: int = 500) -> int:
    """Insert N table entities + view-cache rows so the warm-cache path can read them all."""
    anchor_id = seed_table_entity(hs, schema="public", table="orders")
    for i in range(20):  # a handful of FK edges so FK extractor has work
        other_id = seed_table_entity(hs, schema="public", table=f"t{i}")
        seed_foreign_key_relationship(
            hs,
            from_table_id=anchor_id,
            to_table_id=other_id,
            constrained_columns=[f"c{i}"],
            referred_columns=["id"],
            referred_table=f"t{i}",
        )
    # bulk-insert column_comments_cache so name-match has many candidates
    now = time.time()
    with hs._connect() as conn:
        rows = [
            (
                f"p||public|t{i}",
                "p",
                "",
                "public",
                f"t{i}",
                None,
                json.dumps({"id": {"type": "integer"}, "name": {"type": "text"}}),
                "TABLE",
                now,
                now + 3600,
                1,
            )
            for i in range(tables)
        ]
        conn.executemany(
            "INSERT OR IGNORE INTO column_comments_cache (cache_key, db_profile, database_name, "
            "schema_name, table_name, table_comment, columns_json, kind, fetched_at, expires_at, "
            "bulk_filled) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
    # view-cache entries
    lineage_store.upsert_view_definitions(
        hs,
        db_profile="p",
        database="",
        schema="public",
        entries=[
            {
                "view_name": f"v{i}",
                "ddl_text": f"SELECT id FROM t{i}",
                "dialect": "duckdb",
                "parsed_lineage": [
                    {"target": "id", "sources": [{"table": f"t{i}", "column": "id"}]}
                ],
                "parse_status": "ok",
                "parse_error": "",
            }
            for i in range(views)
        ],
    )
    return anchor_id


def test_warm_cache_create_under_perf_budget(hs, tmp_path):
    """5k tables / 500 views — warm cache — should finish well under 5s."""
    _seed_large_catalog(hs, tables=5000, views=500)

    scope = Scope(profile="p", anchor=ColumnRef("", "public", "orders", ""))
    out_path = tmp_path / "perf.svg"

    started = time.perf_counter()
    with patch("amx.lineage.service.render_lineage_image", return_value=out_path):
        result = service.create_lineage(
            hs=hs,
            scope=scope,
            name="perf-budget",
            output_path=out_path,
            fmt="svg",
            fill_decision="skip",
            force_scale=True,  # synthetic catalog exceeds the soft guard; perf, not scale
        )
    elapsed = time.perf_counter() - started

    assert not result.aborted, result.abort_reason
    assert elapsed < PERF_BUDGET_SECONDS, (
        f"warm-cache /lineage create took {elapsed:.2f}s (budget {PERF_BUDGET_SECONDS}s)"
    )


def test_create_with_default_skip_does_not_open_connector(hs, tmp_path):
    """The most-important UX guarantee: nothing reaches the wire unless the user opts in."""
    seed_table_entity(hs, schema="public", table="orders")
    factory_calls = []

    def boom_factory(profile):
        factory_calls.append(profile)
        raise AssertionError("default flow must not call the connector factory")

    scope = Scope(profile="p", anchor=ColumnRef("", "public", "orders", ""))
    with patch("amx.lineage.service.render_lineage_image", return_value=tmp_path / "x.svg"):
        result = service.create_lineage(
            hs=hs,
            scope=scope,
            name="no-connector",
            output_path=tmp_path / "x.svg",
            fmt="svg",
            fill_decision="skip",
            connector_factory=boom_factory,
        )
    assert factory_calls == []
    assert not result.aborted


def test_db_fill_only_fires_after_explicit_decision(hs, tmp_path):
    """When the caller passes fill_decision='skip', adapter must not be called."""

    class _BoomAdapter:
        def list_views_with_definitions(self, engine, schema):
            raise AssertionError("skip decision must not run adapter")

    def factory(profile):
        return ConnectorHandle(engine=object(), adapter=_BoomAdapter(), backend="duckdb")

    seed_table_entity(hs, schema="public", table="orders")
    scope = Scope(profile="p", anchor=ColumnRef("", "public", "orders", ""))
    with patch("amx.lineage.service.render_lineage_image", return_value=tmp_path / "x.svg"):
        result = service.create_lineage(
            hs=hs,
            scope=scope,
            name="never-fills",
            output_path=tmp_path / "x.svg",
            fmt="svg",
            fill_decision="skip",
            connector_factory=factory,
        )
    assert not result.aborted
    assert result.extractors_partial is True
