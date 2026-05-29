"""Tests for the Runs page's server-side pagination / search / filter /
sort path and its full-dataset facet counts.

Covers:
* ``command_bucket`` / ``kind_bucket_sql`` (the Python mirror of the
  frontend ``commandKind`` used to group runs).
* ``list_recent_runs`` with ``offset`` / ``q`` / ``status`` / ``kind`` /
  ``sort_by`` — the new server-driven knobs — plus the back-compat path.
* ``runs_facets`` total + per-kind + per-status counts over a seeded store.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from amx.storage._history_runs import finish_run
from amx.storage.run_kinds import command_bucket, kind_bucket_sql
from amx.storage.sqlite_store import SQLiteHistoryStore


def _fresh_store(tmp: str) -> SQLiteHistoryStore:
    s = SQLiteHistoryStore(Path(tmp) / "history.db")
    s.init()
    return s


def _seed(
    s: SQLiteHistoryStore,
    *,
    command: str = "analyze.run",
    status: str = "success",
    schema: str = "sales",
    table: str = "orders",
    finish: bool = True,
) -> int:
    rid = s.create_run(
        command=command,
        mode="batch",
        db_backend="postgres",
        db_profile="pg",
        llm_provider="openai",
        llm_model="gpt-4o",
        scope={schema: [table]},
    )
    # ``create_run`` leaves the row ``running``; ``finish`` flips it to the
    # requested terminal status. Skip for runs we want to leave in-flight.
    if finish:
        finish_run(s, rid, status=status, metrics={}, tokens={}, results={})
    return rid


class CommandBucketTests(unittest.TestCase):
    def test_command_bucket_truth_table(self) -> None:
        cases = {
            "analyze.run": "analyze",
            "analyze.apply": "analyze",
            "rerun": "rerun",
            "generate.table": "generate",
            "generate.column": "generate",
            "search.ask": "ask",
            "ask.run": "ask",
            "schedule": "schedule",
            "search.sync": "other",
            "": "other",
            None: "other",
        }
        for cmd, bucket in cases.items():
            self.assertEqual(command_bucket(cmd), bucket, cmd)

    def test_kind_bucket_sql_shapes(self) -> None:
        frag, params = kind_bucket_sql("generate")
        self.assertIn("LIKE", frag)
        self.assertEqual(params, ["generate.%"])
        frag, params = kind_bucket_sql("ask")
        self.assertIn("IN", frag)
        self.assertEqual(params, ["search.ask", "ask.run"])
        # "all"/None -> no clause.
        self.assertEqual(kind_bucket_sql("all"), ("", []))
        self.assertEqual(kind_bucket_sql(None), ("", []))
        # "other" -> negation of every known bucket.
        frag, _ = kind_bucket_sql("other")
        self.assertIn("NOT IN", frag)
        self.assertIn("NOT LIKE", frag)


class ListRecentRunsServerTests(unittest.TestCase):
    def _seed_mixed(self, s: SQLiteHistoryStore) -> None:
        for _ in range(7):
            _seed(s, command="analyze.run")
        for _ in range(3):
            _seed(s, command="generate.table", status="failed")
        for _ in range(5):
            _seed(s, command="search.ask")
        _seed(s, command="rerun")
        _seed(s, command="schedule")

    def test_offset_pages_are_disjoint(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            s = _fresh_store(tmp)
            self._seed_mixed(s)
            page1 = s.list_recent_runs(limit=5, offset=0, command_filter=None)
            page2 = s.list_recent_runs(limit=5, offset=5, command_filter=None)
            self.assertEqual(len(page1), 5)
            self.assertEqual(len(page2), 5)
            self.assertTrue({r["id"] for r in page1}.isdisjoint({r["id"] for r in page2}))

    def test_kind_filter(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            s = _fresh_store(tmp)
            self._seed_mixed(s)
            gen = s.list_recent_runs(limit=50, command_filter=None, kind="generate")
            self.assertEqual(len(gen), 3)
            self.assertTrue(all(r["command"] == "generate.table" for r in gen))
            ask = s.list_recent_runs(limit=50, command_filter=None, kind="ask")
            self.assertEqual(len(ask), 5)

    def test_search_over_command_and_scope(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            s = _fresh_store(tmp)
            self._seed_mixed(s)
            by_command = s.list_recent_runs(limit=50, command_filter=None, q="generate")
            self.assertEqual(len(by_command), 3)
            # Every seeded run touches sales.orders, so a scope search hits all.
            by_scope = s.list_recent_runs(limit=50, command_filter=None, q="orders")
            self.assertEqual(len(by_scope), 17)

    def test_status_filter(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            s = _fresh_store(tmp)
            self._seed_mixed(s)
            failed = s.list_recent_runs(limit=50, command_filter=None, status="failed")
            self.assertEqual(len(failed), 3)
            self.assertTrue(all(r["status"] == "failed" for r in failed))

    def test_sort_by_id_ascending(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            s = _fresh_store(tmp)
            self._seed_mixed(s)
            asc = s.list_recent_runs(limit=3, command_filter=None, sort_by="id", sort_dir="asc")
            ids = [r["id"] for r in asc]
            self.assertEqual(ids, sorted(ids))

    def test_unknown_sort_column_falls_back(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            s = _fresh_store(tmp)
            self._seed_mixed(s)
            # A bogus sort_by must not raise (no SQL injection) — it falls
            # back to started_at DESC.
            rows = s.list_recent_runs(
                limit=5, command_filter=None, sort_by="; DROP TABLE analysis_runs;--"
            )
            self.assertEqual(len(rows), 5)

    def test_backcompat_default_call(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            s = _fresh_store(tmp)
            self._seed_mixed(s)
            # Legacy call (default command_filter="analyze.run") still returns
            # only analyze runs, newest-first.
            rows = s.list_recent_runs(limit=50)
            self.assertEqual(len(rows), 7)
            self.assertTrue(all(r["command"] == "analyze.run" for r in rows))


class RunsFacetsTests(unittest.TestCase):
    def _seed_mixed(self, s: SQLiteHistoryStore) -> None:
        for _ in range(7):
            _seed(s, command="analyze.run")
        for _ in range(3):
            _seed(s, command="generate.table", status="failed")
        for _ in range(5):
            _seed(s, command="search.ask")
        _seed(s, command="rerun")
        _seed(s, command="schedule")

    def test_facets_total_and_counts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            s = _fresh_store(tmp)
            self._seed_mixed(s)
            f = s.runs_facets()
            self.assertEqual(f["total"], 17)
            self.assertEqual(f["kind_counts"]["analyze"], 7)
            self.assertEqual(f["kind_counts"]["generate"], 3)
            self.assertEqual(f["kind_counts"]["ask"], 5)
            self.assertEqual(f["kind_counts"]["rerun"], 1)
            self.assertEqual(f["kind_counts"]["schedule"], 1)
            self.assertEqual(f["kind_counts"]["all"], 17)
            self.assertEqual(f["status_counts"]["success"], 14)
            self.assertEqual(f["status_counts"]["failed"], 3)

    def test_facets_respect_kind_for_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            s = _fresh_store(tmp)
            self._seed_mixed(s)
            # Status counts reflect the active kind: generate runs are all
            # failed in the seed.
            f = s.runs_facets(kind="generate")
            self.assertEqual(f["total"], 3)
            self.assertEqual(f["status_counts"]["failed"], 3)
            self.assertEqual(f["status_counts"]["success"], 0)

    def test_facets_respect_search(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            s = _fresh_store(tmp)
            self._seed_mixed(s)
            f = s.runs_facets(q="generate")
            self.assertEqual(f["total"], 3)
            self.assertEqual(f["kind_counts"]["generate"], 3)
            self.assertEqual(f["kind_counts"]["all"], 3)


if __name__ == "__main__":
    unittest.main()
