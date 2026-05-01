"""Tests for the /compare slash command and its supporting store hooks.

Covers three slices:

* History store migration adds the new ``llm_profile`` / ``doc_profile`` /
  ``code_profile`` columns idempotently, including on a database that
  already has rows from before the upgrade.
* ``find_runs_for_scope`` returns the right rows when filtering by
  schema, table, and command.
* The end-to-end ``/search compare`` Click command renders without
  crashing on a seeded store and surfaces per-run aggregate metrics.
"""

from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from click.testing import CliRunner

from amx.cli import main
from amx.cli_support.commands.compare import (
    _aggregate_for_run,
    _detect_by,
    _resolve_runs,
    _top_alternative,
)
from amx.config import AMXConfig
from amx.storage.sqlite_store import SQLiteHistoryStore


def _fresh_store(tmp: str) -> SQLiteHistoryStore:
    s = SQLiteHistoryStore(Path(tmp) / "history.db")
    s.init()
    return s


def _seed_run(
    s: SQLiteHistoryStore,
    *,
    schema: str = "sales",
    table: str = "orders",
    llm_profile: str = "gpt4o",
    doc_profile: str | None = "docs-prod",
    code_profile: str | None = "code-main",
    llm_model: str = "gpt-4o",
    command: str = "analyze.run",
    suggestions: list[dict] | None = None,
) -> int:
    rid = s.create_run(
        command=command,
        mode="batch",
        db_backend="postgres",
        db_profile="pg",
        llm_provider="openai",
        llm_model=llm_model,
        scope={schema: [table]},
        llm_profile=llm_profile,
        doc_profile=doc_profile,
        code_profile=code_profile,
    )
    if suggestions:
        s.save_run_results(rid, suggestions)
    return rid


class HistoryMigrationTests(unittest.TestCase):
    def test_migration_adds_profile_columns_on_fresh_db(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            s = _fresh_store(tmp)
            with sqlite3.connect(s.db_path) as conn:
                cols = {r[1] for r in conn.execute("PRAGMA table_info(analysis_runs)")}
            for new_col in ("llm_profile", "doc_profile", "code_profile"):
                self.assertIn(new_col, cols)

    def test_migration_is_idempotent_on_existing_db(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "history.db"
            # Simulate an old database that predates the new columns.
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    """
                    CREATE TABLE analysis_runs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        started_at REAL NOT NULL,
                        ended_at REAL,
                        duration_sec REAL,
                        status TEXT NOT NULL,
                        command TEXT NOT NULL,
                        mode TEXT,
                        db_backend TEXT,
                        db_profile TEXT,
                        llm_provider TEXT,
                        llm_model TEXT,
                        scope_json TEXT,
                        metrics_json TEXT,
                        tokens_json TEXT,
                        results_json TEXT,
                        error_text TEXT
                    )
                    """
                )
                conn.execute(
                    "INSERT INTO analysis_runs (started_at, status, command) VALUES (?, ?, ?)",
                    (1.0, "success", "analyze.run"),
                )
            # First init runs full migration.
            s = SQLiteHistoryStore(db_path)
            s.init()
            # Second init must not crash on duplicate ALTER TABLEs.
            s.init()
            with sqlite3.connect(db_path) as conn:
                cols = {r[1] for r in conn.execute("PRAGMA table_info(analysis_runs)")}
            for new_col in (
                "llm_profile", "doc_profile", "code_profile",
                "selected_count", "planned_count", "processed_count",
                "applied_count", "review_strategy",
            ):
                self.assertIn(new_col, cols)

    def test_create_run_persists_profiles(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            s = _fresh_store(tmp)
            rid = _seed_run(s)
            row = s.get_run(rid)
            assert row is not None
            self.assertEqual(row["llm_profile"], "gpt4o")
            self.assertEqual(row["doc_profile"], "docs-prod")
            self.assertEqual(row["code_profile"], "code-main")

    def test_legacy_run_renders_null_profiles(self) -> None:
        """Old rows (NULL profiles) must round-trip without crashing comparisons."""
        with tempfile.TemporaryDirectory() as tmp:
            s = _fresh_store(tmp)
            # Insert directly with NULL profile fields to simulate pre-migration data.
            with sqlite3.connect(s.db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO analysis_runs (
                        started_at, status, command, mode, db_backend, db_profile,
                        llm_provider, llm_model, scope_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (1.0, "success", "analyze.run", "batch", "postgres", "pg",
                     "openai", "gpt-4o", '{"sales": ["orders"]}'),
                )
            runs = s.list_recent_runs()
            self.assertEqual(len(runs), 1)
            self.assertIsNone(runs[0].get("llm_profile"))


class FindRunsForScopeTests(unittest.TestCase):
    def test_filters_by_schema_and_command(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            s = _fresh_store(tmp)
            _seed_run(s, schema="sales", table="orders", command="analyze.run")
            _seed_run(s, schema="sales", table="orders", command="search.ask",
                      llm_profile="claude")
            _seed_run(s, schema="hr", table="employees", command="analyze.run")
            sales_only = s.find_runs_for_scope(schema="sales", limit=10)
            self.assertEqual(len(sales_only), 2)
            ask_only = s.find_runs_for_scope(
                schema="sales", command_filter="search.ask", limit=10,
            )
            self.assertEqual(len(ask_only), 1)
            self.assertEqual(ask_only[0]["llm_profile"], "claude")


class CompareHelpersTests(unittest.TestCase):
    def test_detect_by_picks_first_varying_dimension(self) -> None:
        runs = [
            {"llm_profile": "a", "doc_profile": "x", "code_profile": "k", "llm_model": "gpt"},
            {"llm_profile": "b", "doc_profile": "x", "code_profile": "k", "llm_model": "gpt"},
        ]
        self.assertEqual(_detect_by(runs), "llm_profile")

    def test_detect_by_falls_back_to_run(self) -> None:
        runs = [
            {"llm_profile": "a", "doc_profile": "x", "code_profile": "k",
             "llm_model": "gpt", "db_profile": "pg"},
            {"llm_profile": "a", "doc_profile": "x", "code_profile": "k",
             "llm_model": "gpt", "db_profile": "pg"},
        ]
        self.assertEqual(_detect_by(runs), "run")

    def test_top_alternative_prefers_chosen(self) -> None:
        row = {
            "chosen_description": "Picked by user",
            "alternatives_json": ["LLM first guess", "Second"],
        }
        self.assertEqual(_top_alternative(row), "Picked by user")

    def test_top_alternative_falls_back_to_first_alternative(self) -> None:
        row = {"chosen_description": "", "alternatives_json": ["LLM first guess"]}
        self.assertEqual(_top_alternative(row), "LLM first guess")

    def test_top_alternative_handles_json_string_alternatives(self) -> None:
        row = {"chosen_description": "", "alternatives_json": '["from-json"]'}
        self.assertEqual(_top_alternative(row), "from-json")

    def test_aggregate_collects_logprob_and_band_distribution(self) -> None:
        run = {
            "duration_sec": 10.0,
            "metrics_json": {"model_processing_sec": 8.0},
            "tokens_json": {"summary": [["plan", 100, 200, 300]]},
            "processed_count": 5,
            "applied_count": 4,
        }
        results = [
            {"logprob_score": 0.9, "confidence": "high"},
            {"logprob_score": 0.5, "confidence": "medium"},
            {"logprob_score": 0.3, "confidence": "low"},
        ]
        agg = _aggregate_for_run(run, results)
        self.assertAlmostEqual(agg["avg_logprob"], (0.9 + 0.5 + 0.3) / 3.0)
        self.assertAlmostEqual(agg["high_pct"], 100.0 / 3.0)
        self.assertAlmostEqual(agg["approval_rate"], 0.8)
        self.assertEqual(agg["total_tokens"], 300)


class CompareCommandTests(unittest.TestCase):
    """End-to-end click invocation of /search compare with a seeded store."""

    def _seed_two_runs(self, s: SQLiteHistoryStore) -> tuple[int, int]:
        common_suggestions = [
            {
                "schema": "sales",
                "table": "orders",
                "column": "id",
                "asset_kind": "table",
                "source": "code",
                "confidence": "high",
                "logprob_score": 0.91,
                "raw_logprob": 0.91,
                "token_count": 28,
                "model_version": "gpt-4o",
                "reasoning": "primary key",
                "alternatives": ["Primary key for orders"],
            }
        ]
        rid1 = _seed_run(
            s, llm_profile="gpt4o-prof", llm_model="gpt-4o",
            suggestions=common_suggestions,
        )
        # Vary the LLM profile so auto-detect should pick "llm_profile".
        rid2 = _seed_run(
            s, llm_profile="sonnet-prof", llm_model="claude-sonnet-4-6",
            suggestions=[{**common_suggestions[0], "logprob_score": 0.71,
                          "confidence": "medium"}],
        )
        return rid1, rid2

    def test_compare_renders_with_two_runs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cfg = AMXConfig()
            cfg.CONFIG_DIR = tmp  # type: ignore[misc]
            store = SQLiteHistoryStore(Path(tmp) / "history.db")
            store.init()
            rid1, rid2 = self._seed_two_runs(store)

            runner = CliRunner()
            with (
                patch("amx.config.AMXConfig.load", return_value=cfg),
                # Both the CLI bootstrap path and the compare command read
                # ``history_store`` from their respective modules. Patch
                # both so the runner sees our pre-seeded store.
                patch("amx.cli.history_store", return_value=store),
                patch("amx.cli.init_history_store", return_value=store),
                patch(
                    "amx.cli_support.commands.compare.history_store",
                    return_value=store,
                ),
            ):
                result = runner.invoke(
                    main,
                    ["--config", "test-config.yml", "search", "compare",
                     str(rid1), str(rid2)],
                    env={"AMX_SESSION_CHILD": "1"},
                    catch_exceptions=False,
                )

            self.assertEqual(result.exit_code, 0, msg=result.output)
            # Header line plus the three table titles must all appear.
            self.assertIn("Comparing 2 runs", result.output)
            self.assertIn("varying dimension: llm_profile", result.output)
            self.assertIn("Run summary", result.output)
            self.assertIn("Per-column results", result.output)
            self.assertIn("Aggregate metrics", result.output)
            # Rich truncates long cells in narrow terminals — assert on
            # fragments that survive truncation rather than full names.
            self.assertIn("gpt4", result.output)
            self.assertIn("sonn", result.output)
            # Per-column pivot row must show the description and logprob.
            self.assertIn("Primary key for orders", result.output)
            self.assertIn("0.91", result.output)
            self.assertIn("0.71", result.output)

    def test_compare_refuses_single_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cfg = AMXConfig()
            cfg.CONFIG_DIR = tmp  # type: ignore[misc]
            store = SQLiteHistoryStore(Path(tmp) / "history.db")
            store.init()
            rid = _seed_run(store)

            runner = CliRunner()
            with (
                patch("amx.config.AMXConfig.load", return_value=cfg),
                patch("amx.cli.history_store", return_value=store),
                patch("amx.cli.init_history_store", return_value=store),
                patch(
                    "amx.cli_support.commands.compare.history_store",
                    return_value=store,
                ),
            ):
                result = runner.invoke(
                    main,
                    ["--config", "test-config.yml", "search", "compare", str(rid)],
                    env={"AMX_SESSION_CHILD": "1"},
                    catch_exceptions=False,
                )
            self.assertEqual(result.exit_code, 0, msg=result.output)
            self.assertIn("Only one run resolved", result.output)


class CompareResolveTests(unittest.TestCase):
    """Direct exercise of _resolve_runs to keep coverage tight on the resolution rules."""

    def test_resolve_by_explicit_run_ids(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = SQLiteHistoryStore(Path(tmp) / "history.db")
            store.init()
            rid1 = _seed_run(store, llm_profile="a")
            rid2 = _seed_run(store, llm_profile="b")
            cfg = AMXConfig()
            with patch(
                "amx.cli_support.commands.compare.history_store",
                return_value=store,
            ):
                runs = _resolve_runs(
                    cfg=cfg,
                    run_ids=(str(rid1), str(rid2)),
                    schema="",
                    table="",
                    last_n=5,
                    command_filter="all",
                )
            self.assertEqual(
                {int(r["id"]) for r in runs},
                {rid1, rid2},
            )

    def test_resolve_requires_scope_when_no_ids(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = SQLiteHistoryStore(Path(tmp) / "history.db")
            store.init()
            cfg = AMXConfig()
            with patch(
                "amx.cli_support.commands.compare.history_store",
                return_value=store,
            ):
                runs = _resolve_runs(
                    cfg=cfg,
                    run_ids=(),
                    schema="",
                    table="",
                    last_n=5,
                    command_filter="all",
                )
            self.assertEqual(runs, [])


if __name__ == "__main__":
    unittest.main()
