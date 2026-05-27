"""Persisted notebook index — build, lookup, staleness, name resolution."""

from __future__ import annotations

import json
import time

from amx.lineage.native import notebook_index as ni
from amx.lineage.native import provider as P


class _FakeClient:
    """Yields a small workspace tree like ``list_workspace_objects`` does."""

    def list_workspace_objects(self):
        yield {"object_type": "DIRECTORY", "object_id": 1, "path": "/Users/me/Folder"}
        yield {"object_type": "NOTEBOOK", "object_id": 123, "path": "/Users/me/Folder/My NB"}
        yield {"object_type": "NOTEBOOK", "object_id": 456, "path": "/A/B/Other"}
        yield {"object_type": "FILE", "object_id": 2, "path": "/Users/me/data.csv"}


def test_build_index_writes_only_notebooks(tmp_path):
    path = ni.cache_path(tmp_path, "db", "host.example.com")
    count = ni.build_index(_FakeClient(), path)
    assert count == 2
    names = ni.load_names(path)
    assert names == {"123": "My NB", "456": "Other"}


def test_build_index_persists_paths(tmp_path):
    path = ni.cache_path(tmp_path, "db", "h")
    ni.build_index(_FakeClient(), path)
    assert ni.lookup_path(path, "123") == "/Users/me/Folder/My NB"
    assert ni.lookup_path(path, "456") == "/A/B/Other"
    assert ni.lookup_path(path, "999") is None  # unknown id


def test_v1_name_only_cache_is_stale(tmp_path):
    """A pre-paths cache (no version/paths) rebuilds even if recent."""
    path = tmp_path / "old.json"
    path.write_text(
        json.dumps({"built_at": time.time(), "names": {"123": "My NB"}}), encoding="utf-8"
    )
    assert ni.is_stale(path, ttl_s=3600)  # v1 shape → stale despite being fresh
    assert ni.lookup_path(path, "123") is None  # no paths in v1


def test_load_names_missing_returns_empty(tmp_path):
    assert ni.load_names(tmp_path / "nope.json") == {}


def test_is_stale(tmp_path):
    path = tmp_path / "idx.json"
    assert ni.is_stale(path)  # missing → stale
    fresh = {"version": ni.CACHE_VERSION, "built_at": time.time(), "names": {}, "paths": {}}
    path.write_text(json.dumps(fresh), encoding="utf-8")
    assert not ni.is_stale(path, ttl_s=3600)
    old = {"version": ni.CACHE_VERSION, "built_at": time.time() - 10_000, "names": {}, "paths": {}}
    path.write_text(json.dumps(old), encoding="utf-8")
    assert ni.is_stale(path, ttl_s=3600)


def test_resolve_names_rewrites_known_ids_only(tmp_path):
    path = ni.cache_path(tmp_path, "db", "h")
    path.write_text(
        json.dumps({"built_at": time.time(), "names": {"123": "My NB"}}), encoding="utf-8"
    )

    anchor = P.NativeLineageNode(kind=P.TABLE, name="t", fqn="c.s.t")
    known = P.NativeLineageNode(kind=P.NOTEBOOK, name="notebook 123", external_id="123")
    unknown = P.NativeLineageNode(kind=P.NOTEBOOK, name="notebook 999", external_id="999")
    res = P.NativeLineageResult(anchor=anchor)
    res.edges = [
        P.NativeLineageEdge(source=known, target=anchor, direction=P.UPSTREAM),
        P.NativeLineageEdge(source=unknown, target=anchor, direction=P.UPSTREAM),
    ]
    ni.resolve_names(res, path)
    names = {n.name for e in res.edges for n in (e.source, e.target)}
    assert "My NB" in names  # known id resolved
    assert "notebook 999" in names  # unknown id left as placeholder
