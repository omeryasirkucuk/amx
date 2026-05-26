"""Native lineage fetch — provider mapping, materializer routing, service."""

from __future__ import annotations

import time
from typing import Any

import pytest

from amx.lineage.native import provider as P
from amx.lineage.native.databricks import DatabricksLineageProvider
from amx.lineage.native.materializer import (
    REL_ASSET,
    REL_TABLE,
    SOURCE,
    LineageMaterializer,
)
from amx.lineage.native.service import LineageFetchService, NativeLineageError
from amx.search.catalog import SearchCatalog

from .conftest import seed_table_entity

ANCHOR_FQN = "workspace.new_schema.dummy_table"


# ── provider mapping ──────────────────────────────────────────────────


class _FakeClient:
    def __init__(self, table_resp: dict[str, Any], column_resp: dict[str, Any] | None = None):
        self._table = table_resp
        self._column = column_resp or {}
        self.column_calls: list[tuple[str, str]] = []

    def table_lineage(self, *, table_name: str, include_entity_lineage: bool = True):
        assert include_entity_lineage is True
        return self._table

    def column_lineage(self, *, table_name: str, column_name: str):
        self.column_calls.append((table_name, column_name))
        return self._column

    def resolve_entity_name(self, *, kind: str, external_id: str):
        # Mirror the real client; return a name so the provider's
        # name-resolution post-pass has something to apply.
        return f"{kind}-{external_id}-name"


def test_provider_maps_tables_and_producer_consumer_assets() -> None:
    resp = {
        "upstreams": [
            {
                "tableInfo": {
                    "catalog_name": "workspace",
                    "schema_name": "new_schema",
                    "name": "src_tbl",
                },
                "notebookInfos": [{"notebook_id": "n1", "name": "ETL nb"}],
            }
        ],
        "downstreams": [
            {
                "tableInfo": {
                    "catalog_name": "workspace",
                    "schema_name": "new_schema",
                    "name": "report_tbl",
                },
                "jobInfos": [{"job_id": "j9", "name": "nightly"}],
            }
        ],
    }
    provider = DatabricksLineageProvider(_FakeClient(resp))
    result = provider.fetch_table_lineage(ANCHOR_FQN, with_columns=False)

    assert result.anchor.fqn == ANCHOR_FQN
    ups = [e for e in result.edges if e.direction == P.UPSTREAM]
    downs = [e for e in result.edges if e.direction == P.DOWNSTREAM]
    # upstream: src table feeds anchor, producer notebook feeds anchor
    assert any(e.source.kind == P.TABLE and e.source.name == "src_tbl" for e in ups)
    assert any(e.source.kind == P.NOTEBOOK and e.source.external_id == "n1" for e in ups)
    # downstream: anchor feeds report table + consumer job
    assert any(e.target.kind == P.TABLE and e.target.name == "report_tbl" for e in downs)
    assert any(e.target.kind == P.JOB and e.target.external_id == "j9" for e in downs)


def test_provider_tolerates_camelcase_and_missing_fields() -> None:
    resp = {
        "upstreams": [
            {"tableInfo": {"catalogName": "c", "schemaName": "s", "tableName": "t"}},
            {"notebookInfos": [{}]},  # no id, no name → still named, not dropped
        ]
    }
    provider = DatabricksLineageProvider(_FakeClient(resp))
    result = provider.fetch_table_lineage(ANCHOR_FQN, with_columns=False)
    assert any(e.source.fqn == "c.s.t" for e in result.edges)
    assert any(e.source.kind == P.NOTEBOOK for e in result.edges)


def test_provider_never_fetches_column_lineage() -> None:
    """Column-level lineage is intentionally disabled: its REST shape is
    unverified and mis-mapped columns onto table nodes ("every column
    looked like its own table"). ``with_columns`` is accepted for the
    protocol but must be a no-op — no ``column_lineage`` calls, no
    column-grained edges — regardless of the flag."""
    client = _FakeClient(
        {
            "upstreams": [
                {
                    "tableInfo": {
                        "catalog_name": "workspace",
                        "schema_name": "new_schema",
                        "name": "src_tbl",
                    }
                }
            ]
        },
    )
    provider = DatabricksLineageProvider(client)

    # Even when the caller asks for columns, the provider ignores it.
    result = provider.fetch_table_lineage(ANCHOR_FQN, with_columns=True, anchor_columns=("id",))
    assert client.column_calls == []
    assert all(not e.from_column and not e.to_column for e in result.edges)
    # The table-level lineage still comes through unaffected.
    assert any(e.source.fqn == "workspace.new_schema.src_tbl" for e in result.edges)


# ── materializer routing ──────────────────────────────────────────────


def _anchor_node() -> P.NativeLineageNode:
    return P.NativeLineageNode(kind=P.TABLE, name="dummy_table", fqn=ANCHOR_FQN)


def _materializer(hs) -> LineageMaterializer:
    catalog = SearchCatalog(hs.db_path)
    return LineageMaterializer(catalog, profile_name="dbr", backend="databricks")


def _rels(hs) -> list[dict[str, Any]]:
    with hs._connect() as conn:
        rows = conn.execute(
            "SELECT from_entity_id, to_entity_id, relationship_type, source, "
            "from_entity_kind, to_entity_kind, from_column, to_column, details_json "
            "FROM catalog_relationships WHERE source = ?",
            (SOURCE,),
        ).fetchall()
    return [dict(r) for r in rows]


def _seed_anchor(hs) -> int:
    return seed_table_entity(
        hs,
        profile="dbr",
        backend="databricks",
        database="workspace",
        schema="new_schema",
        table="dummy_table",
    )


def test_table_edge_full_when_both_resolved(hs) -> None:
    _seed_anchor(hs)
    seed_table_entity(
        hs,
        profile="dbr",
        backend="databricks",
        database="workspace",
        schema="new_schema",
        table="src_tbl",
    )
    result = P.NativeLineageResult(anchor=_anchor_node())
    result.edges.append(
        P.NativeLineageEdge(
            source=P.NativeLineageNode(
                kind=P.TABLE, name="src_tbl", fqn="workspace.new_schema.src_tbl"
            ),
            target=_anchor_node(),
            direction=P.UPSTREAM,
        )
    )
    counts = _materializer(hs).materialize(result)
    assert counts.edges == 1
    assert counts.name_only == 0
    rels = _rels(hs)
    assert len(rels) == 1
    assert rels[0]["relationship_type"] == REL_TABLE
    # both endpoints are full (no name_only rows created)
    with hs._connect() as conn:
        states = {
            r[0] for r in conn.execute("SELECT metadata_state FROM catalog_entities").fetchall()
        }
    assert states == {"full"}


def test_unresolved_table_becomes_name_only_ghost(hs) -> None:
    _seed_anchor(hs)
    result = P.NativeLineageResult(anchor=_anchor_node())
    result.edges.append(
        P.NativeLineageEdge(
            source=P.NativeLineageNode(kind=P.TABLE, name="ghost", fqn="workspace.other.ghost"),
            target=_anchor_node(),
            direction=P.UPSTREAM,
        )
    )
    counts = _materializer(hs).materialize(result)
    assert counts.name_only == 1
    with hs._connect() as conn:
        ghost = conn.execute(
            "SELECT metadata_state FROM catalog_entities WHERE table_name = 'ghost'"
        ).fetchone()
    assert ghost is not None and ghost[0] == "name_only"


def test_asset_full_when_remote_row_exists_else_ghost(hs) -> None:
    _seed_anchor(hs)
    # seed an ingested notebook so the producer reconciles to full
    with hs._connect() as conn:
        conn.execute(
            """INSERT INTO remote_notebooks
               (profile_name, platform, external_id, name, language, source_text,
                source_hash, ingested_at)
               VALUES ('dbr','databricks','n1','ETL nb','python','...','h', ?)""",
            (time.time(),),
        )
    result = P.NativeLineageResult(anchor=_anchor_node())
    result.edges.append(
        P.NativeLineageEdge(
            source=P.NativeLineageNode(kind=P.NOTEBOOK, name="ETL nb", external_id="n1"),
            target=_anchor_node(),
            direction=P.UPSTREAM,
        )
    )
    result.edges.append(
        P.NativeLineageEdge(
            source=_anchor_node(),
            target=P.NativeLineageNode(kind=P.DASHBOARD, name="Sales board", external_id="d5"),
            direction=P.DOWNSTREAM,
        )
    )
    counts = _materializer(hs).materialize(result)
    assert counts.assets == 2
    assert counts.name_only == 1  # dashboard has no remote_* table → ghost
    rels = _rels(hs)
    assert all(r["relationship_type"] == REL_ASSET for r in rels)
    # producer notebook reconciled to full bridge (source_remote_id set)
    with hs._connect() as conn:
        nb = conn.execute(
            "SELECT metadata_state, source_remote_id FROM catalog_entities "
            "WHERE entity_kind = 'notebook'"
        ).fetchone()
        dash = conn.execute(
            "SELECT metadata_state FROM catalog_entities WHERE entity_kind = 'dashboard'"
        ).fetchone()
    assert nb[0] == "full" and nb[1] is not None
    assert dash[0] == "name_only"


def test_unknown_kind_is_recorded_as_external_not_dropped(hs) -> None:
    _seed_anchor(hs)
    result = P.NativeLineageResult(anchor=_anchor_node())
    result.edges.append(
        P.NativeLineageEdge(
            source=P.NativeLineageNode(kind=P.EXTERNAL, name="mystery asset", external_id="x1"),
            target=_anchor_node(),
            direction=P.UPSTREAM,
        )
    )
    counts = _materializer(hs).materialize(result)
    assert counts.edges == 1
    with hs._connect() as conn:
        ext = conn.execute(
            "SELECT metadata_state FROM catalog_entities WHERE entity_kind = 'external'"
        ).fetchone()
    assert ext is not None and ext[0] == "name_only"


def test_refetch_is_idempotent_and_anchor_scoped(hs) -> None:
    anchor_id = _seed_anchor(hs)
    other_anchor = seed_table_entity(
        hs,
        profile="dbr",
        backend="databricks",
        database="workspace",
        schema="new_schema",
        table="other_anchor",
    )
    src = seed_table_entity(
        hs,
        profile="dbr",
        backend="databricks",
        database="workspace",
        schema="new_schema",
        table="src_tbl",
    )
    # Pre-existing native edge for a DIFFERENT anchor must survive a
    # re-fetch of dummy_table.
    with hs._connect() as conn:
        conn.execute(
            "INSERT INTO catalog_relationships (from_entity_id, to_entity_id, "
            "relationship_type, score, source, details_json, last_seen, "
            "from_entity_kind, to_entity_kind) VALUES (?, ?, ?, 1.0, ?, '{}', ?, 'table', 'table')",
            (src, other_anchor, REL_TABLE, SOURCE, time.time()),
        )

    result = P.NativeLineageResult(anchor=_anchor_node())
    result.edges.append(
        P.NativeLineageEdge(
            source=P.NativeLineageNode(
                kind=P.TABLE, name="src_tbl", fqn="workspace.new_schema.src_tbl"
            ),
            target=_anchor_node(),
            direction=P.UPSTREAM,
        )
    )
    mat = _materializer(hs)
    mat.materialize(result)
    mat.materialize(result)  # twice

    rels = _rels(hs)
    # one edge for the other anchor (preserved) + exactly one for dummy_table
    touching_anchor = [r for r in rels if anchor_id in (r["from_entity_id"], r["to_entity_id"])]
    touching_other = [r for r in rels if other_anchor in (r["from_entity_id"], r["to_entity_id"])]
    assert len(touching_anchor) == 1
    assert len(touching_other) == 1


# ── service ───────────────────────────────────────────────────────────


def test_native_lineage_surfaces_in_studio_payload(hs) -> None:
    """Native edges (asset kinds + name_only) merge into lineage_for_studio."""
    from amx.lineage.service import lineage_for_studio
    from amx.lineage.types import ColumnRef, Scope

    _seed_anchor(hs)
    result = P.NativeLineageResult(anchor=_anchor_node())
    # an upstream name-only table + a downstream dashboard (ghost asset)
    result.edges.append(
        P.NativeLineageEdge(
            source=P.NativeLineageNode(
                kind=P.TABLE, name="ghost", fqn="workspace.new_schema.ghost"
            ),
            target=_anchor_node(),
            direction=P.UPSTREAM,
        )
    )
    result.edges.append(
        P.NativeLineageEdge(
            source=_anchor_node(),
            target=P.NativeLineageNode(kind=P.DASHBOARD, name="Sales board", external_id="d5"),
            direction=P.DOWNSTREAM,
        )
    )
    _materializer(hs).materialize(result)

    scope = Scope(
        profile="dbr",
        anchor=ColumnRef(database="workspace", schema="new_schema", table="dummy_table", column=""),
    )
    payload = lineage_for_studio(hs, scope=scope)
    kinds = {n["kind"] for n in payload["nodes"]}
    assert "dashboard" in kinds
    labels = {n["label"] for n in payload["nodes"]}
    assert "Sales board" in labels
    # the unresolved upstream table is greyed as name_only
    ghost = next(n for n in payload["nodes"] if n["label"].endswith("ghost"))
    assert ghost["metadataState"] == "name_only"
    assert any(e["extractor"] == "native" for e in payload["edges"])


def test_service_raises_for_unsupported_backend(hs) -> None:
    svc = LineageFetchService(SearchCatalog(hs.db_path))
    with pytest.raises(NativeLineageError):
        svc.fetch(profile_name="pg", backend="postgresql", fqn="a.b.c")


def test_service_materializes_via_registered_provider(hs, monkeypatch) -> None:
    _seed_anchor(hs)

    class _StubProvider:
        backend = "databricks"

        def fetch_table_lineage(self, fqn, *, with_columns, anchor_columns=()):
            r = P.NativeLineageResult(
                anchor=P.NativeLineageNode(kind=P.TABLE, name="dummy_table", fqn=fqn)
            )
            r.edges.append(
                P.NativeLineageEdge(
                    source=P.NativeLineageNode(kind=P.NOTEBOOK, name="nb", external_id="z1"),
                    target=r.anchor,
                    direction=P.UPSTREAM,
                )
            )
            return r

    monkeypatch.setattr(P, "provider_for_profile", lambda profile, backend: _StubProvider())
    svc = LineageFetchService(SearchCatalog(hs.db_path))
    counts = svc.fetch(profile_name="dbr", backend="databricks", fqn=ANCHOR_FQN)
    assert counts.edges == 1
    assert counts.name_only == 1
