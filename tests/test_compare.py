"""Tests for the /compare slash command and its supporting store hooks.

Covers three slices:

* History store migration adds the new ``llm_profile`` / ``doc_profile`` /
  ``code_profile`` columns idempotently, including on a database that
  already has rows from before the upgrade.
* ``find_runs_for_scope`` returns the right rows when filtering by
  schema, table, and command.
* The end-to-end ``/history compare`` Click command renders without
  crashing on a seeded store and surfaces per-run aggregate metrics.
"""

from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from click.testing import CliRunner

from amx.cli import main
from amx.cli_support.commands.compare import (
    _aggregate_for_run,
    _band_prefix,
    _collect_per_column_long,
    _collect_run_summary_rows,
    _detect_by,
    _export_csv,
    _export_json,
    _export_markdown,
    _resolve_runs,
    _top_alternative,
    _word_diff,
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
                "llm_profile",
                "doc_profile",
                "code_profile",
                "selected_count",
                "planned_count",
                "processed_count",
                "applied_count",
                "review_strategy",
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
                    (
                        1.0,
                        "success",
                        "analyze.run",
                        "batch",
                        "postgres",
                        "pg",
                        "openai",
                        "gpt-4o",
                        '{"sales": ["orders"]}',
                    ),
                )
            runs = s.list_recent_runs()
            self.assertEqual(len(runs), 1)
            self.assertIsNone(runs[0].get("llm_profile"))


class FindRunsForScopeTests(unittest.TestCase):
    def test_filters_by_schema_and_command(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            s = _fresh_store(tmp)
            _seed_run(s, schema="sales", table="orders", command="analyze.run")
            _seed_run(s, schema="sales", table="orders", command="search.ask", llm_profile="claude")
            _seed_run(s, schema="hr", table="employees", command="analyze.run")
            sales_only = s.find_runs_for_scope(schema="sales", limit=10)
            self.assertEqual(len(sales_only), 2)
            ask_only = s.find_runs_for_scope(
                schema="sales",
                command_filter="search.ask",
                limit=10,
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
            {
                "llm_profile": "a",
                "doc_profile": "x",
                "code_profile": "k",
                "llm_model": "gpt",
                "db_profile": "pg",
            },
            {
                "llm_profile": "a",
                "doc_profile": "x",
                "code_profile": "k",
                "llm_model": "gpt",
                "db_profile": "pg",
            },
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
    """End-to-end click invocation of /history compare with a seeded store."""

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
            s,
            llm_profile="gpt4o-prof",
            llm_model="gpt-4o",
            suggestions=common_suggestions,
        )
        # Vary the LLM profile so auto-detect should pick "llm_profile".
        rid2 = _seed_run(
            s,
            llm_profile="sonnet-prof",
            llm_model="claude-sonnet-4-6",
            suggestions=[{**common_suggestions[0], "logprob_score": 0.71, "confidence": "medium"}],
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
                    ["--config", "test-config.yml", "history", "compare", str(rid1), str(rid2)],
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
                    ["--config", "test-config.yml", "history", "compare", str(rid)],
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


class WordDiffTests(unittest.TestCase):
    def test_identical_strings_return_plain_text_no_styling(self) -> None:
        out = _word_diff("the quick brown fox", "the quick brown fox")
        self.assertEqual(out.plain, "the quick brown fox")
        self.assertEqual(out.spans, [])

    def test_replacement_strikes_baseline_and_greens_replacement(self) -> None:
        out = _word_diff("the quick brown fox", "the slow brown fox")
        # The composed text contains both baseline ("quick") and current ("slow")
        # with different styles so reviewers can see what changed at a glance.
        self.assertIn("quick", out.plain)
        self.assertIn("slow", out.plain)
        styles = {span.style for span in out.spans}
        self.assertIn("strike red", styles)
        self.assertIn("bold green", styles)

    def test_pure_insertion_only_emits_green(self) -> None:
        out = _word_diff("the brown fox", "the brown fox jumps")
        self.assertIn("jumps", out.plain)
        self.assertTrue(any(span.style == "bold green" for span in out.spans))
        self.assertFalse(any(span.style == "strike red" for span in out.spans))

    def test_pure_deletion_only_emits_strike_red(self) -> None:
        out = _word_diff("the brown lazy fox", "the brown fox")
        self.assertIn("lazy", out.plain)
        self.assertTrue(any(span.style == "strike red" for span in out.spans))
        self.assertFalse(any(span.style == "bold green" for span in out.spans))

    def test_empty_strings_do_not_crash(self) -> None:
        self.assertEqual(_word_diff("", "").plain, "")
        out_insert = _word_diff("", "new")
        self.assertEqual(out_insert.plain, "new")
        out_delete = _word_diff("old", "")
        self.assertEqual(out_delete.plain, "old")


def _seed_two_runs_for_export(s: SQLiteHistoryStore) -> tuple[int, int]:
    base_suggestion = {
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
    rid1 = _seed_run(
        s,
        llm_profile="gpt4o",
        llm_model="gpt-4o",
        suggestions=[base_suggestion],
    )
    rid2 = _seed_run(
        s,
        llm_profile="claude",
        llm_model="claude-sonnet-4-6",
        suggestions=[
            {
                **base_suggestion,
                "logprob_score": 0.71,
                "confidence": "medium",
                "alternatives": ["Order identifier"],
            }
        ],
    )
    return rid1, rid2


class ExportCSVTests(unittest.TestCase):
    def test_csv_round_trip_contains_all_three_sections(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = SQLiteHistoryStore(Path(tmp) / "history.db")
            store.init()
            rid1, rid2 = _seed_two_runs_for_export(store)
            runs = [store.get_run(rid2), store.get_run(rid1)]
            results_by_run = {
                rid1: store.get_run_results(rid1),
                rid2: store.get_run_results(rid2),
            }
            out_path = Path(tmp) / "compare.csv"
            _export_csv(out_path, runs, results_by_run)
            self.assertTrue(out_path.exists())
            text = out_path.read_text()
            # Section markers
            self.assertIn("# section: run_summary", text)
            self.assertIn("# section: per_column", text)
            self.assertIn("# section: aggregate_metrics", text)
            # Data presence
            self.assertIn("gpt4o", text)
            self.assertIn("claude", text)
            self.assertIn("Primary key for orders", text)
            self.assertIn("Order identifier", text)
            # Aggregate row labels
            self.assertIn("avg_logprob_score", text)
            self.assertIn("approval_rate", text)


class ExportMarkdownTests(unittest.TestCase):
    def test_markdown_renders_three_sections(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = SQLiteHistoryStore(Path(tmp) / "history.db")
            store.init()
            rid1, rid2 = _seed_two_runs_for_export(store)
            runs = [store.get_run(rid2), store.get_run(rid1)]
            results_by_run = {
                rid1: store.get_run_results(rid1),
                rid2: store.get_run_results(rid2),
            }
            out_path = Path(tmp) / "compare.md"
            _export_markdown(out_path, runs, results_by_run)
            self.assertTrue(out_path.exists())
            text = out_path.read_text()
            self.assertIn("# AMX run comparison", text)
            self.assertIn("## Run summary", text)
            self.assertIn("## Per-column results", text)
            self.assertIn("## Aggregate metrics", text)
            # Wide-format header includes per-run columns
            self.assertIn(f"Run #{rid1}", text)
            self.assertIn(f"Run #{rid2}", text)
            # Profile names survive
            self.assertIn("gpt4o", text)
            self.assertIn("claude", text)
            # Description survives
            self.assertIn("Primary key for orders", text)


class CollectorTests(unittest.TestCase):
    """Direct exercise of the long-form collectors so export shapes stay stable."""

    def test_run_summary_collector_shape(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = SQLiteHistoryStore(Path(tmp) / "history.db")
            store.init()
            rid = _seed_run(store, llm_profile="x", doc_profile="d", code_profile="c")
            rows = _collect_run_summary_rows([store.get_run(rid)])
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["llm_profile"], "x")
            self.assertEqual(rows[0]["doc_profile"], "d")
            self.assertEqual(rows[0]["code_profile"], "c")

    def test_per_column_long_emits_one_row_per_run_per_asset(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = SQLiteHistoryStore(Path(tmp) / "history.db")
            store.init()
            rid1, rid2 = _seed_two_runs_for_export(store)
            runs = [store.get_run(rid1), store.get_run(rid2)]
            results_by_run = {
                rid1: store.get_run_results(rid1),
                rid2: store.get_run_results(rid2),
            }
            rows = _collect_per_column_long(runs, results_by_run)
            self.assertEqual(len(rows), 2)
            self.assertEqual({r["run_id"] for r in rows}, {rid1, rid2})
            self.assertEqual({r["column"] for r in rows}, {"id"})


class ExportJSONTests(unittest.TestCase):
    def test_json_round_trip_has_expected_keys(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = SQLiteHistoryStore(Path(tmp) / "history.db")
            store.init()
            rid1, rid2 = _seed_two_runs_for_export(store)
            runs = [store.get_run(rid2), store.get_run(rid1)]
            results_by_run = {
                rid1: store.get_run_results(rid1),
                rid2: store.get_run_results(rid2),
            }
            out_path = Path(tmp) / "compare.json"
            _export_json(out_path, runs, results_by_run)
            self.assertTrue(out_path.exists())
            import json as _json

            payload = _json.loads(out_path.read_text())
            for key in (
                "schema_version",
                "generated_at",
                "amx_version",
                "run_count",
                "run_summary",
                "per_column",
                "aggregate_metrics",
            ):
                self.assertIn(key, payload)
            self.assertEqual(payload["run_count"], 2)
            self.assertEqual(len(payload["run_summary"]), 2)
            self.assertEqual({r["run_id"] for r in payload["run_summary"]}, {rid1, rid2})
            # per_column rows are long-format → 2 (assets) × 2 (runs) but
            # the asset is shared, so 2 total.
            self.assertEqual(len(payload["per_column"]), 2)
            self.assertEqual(
                {r["description"] for r in payload["per_column"]},
                {"Primary key for orders", "Order identifier"},
            )

    def test_json_long_format_aggregate_one_row_per_metric_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = SQLiteHistoryStore(Path(tmp) / "history.db")
            store.init()
            rid1, rid2 = _seed_two_runs_for_export(store)
            runs = [store.get_run(rid1), store.get_run(rid2)]
            results_by_run = {
                rid1: store.get_run_results(rid1),
                rid2: store.get_run_results(rid2),
            }
            out_path = Path(tmp) / "compare.json"
            _export_json(out_path, runs, results_by_run)
            import json as _json

            payload = _json.loads(out_path.read_text())
            # 12 metric rows (cost_usd added in 0.13) x 2 runs = 24 entries.
            self.assertEqual(len(payload["aggregate_metrics"]), 12 * 2)
            metrics_seen = {r["metric"] for r in payload["aggregate_metrics"]}
            self.assertIn("avg_logprob_score", metrics_seen)
            self.assertIn("approval_rate", metrics_seen)
            self.assertIn("total_tokens", metrics_seen)
            self.assertIn("cost_usd", metrics_seen)


class ComparePerColumnContractTests(unittest.TestCase):
    """Pin the long-format ``per_column`` shape the SPA's
    ``RunsCompare.tsx`` (PerColumnPivot) depends on. The page
    renders ``confidence`` + ``logprob_score`` + ``token_count`` per
    cell on top of the description, plus the ``run_id`` is what
    drives the per-asset winner-ring computation. Drift on any of
    these four fields would silently break the Compare UX without
    a visible runtime error -- the cells would just lose their
    badges -- so pin the contract here.
    """

    def test_per_column_long_carries_confidence_logprob_tokens(self) -> None:
        from amx.cli_support.commands.compare import _collect_per_column_long

        with tempfile.TemporaryDirectory() as tmp:
            store = SQLiteHistoryStore(Path(tmp) / "history.db")
            store.init()
            rid1, rid2 = _seed_two_runs_for_export(store)
            runs = [store.get_run(rid1), store.get_run(rid2)]
            results_by_run = {
                rid1: store.get_run_results(rid1),
                rid2: store.get_run_results(rid2),
            }
            rows = _collect_per_column_long(runs, results_by_run)
            self.assertGreater(len(rows), 0)
            for row in rows:
                # SPA reads exactly these keys -- removing any of
                # them silently degrades the Compare cell footer.
                for key in (
                    "schema",
                    "table",
                    "column",
                    "run_id",
                    "description",
                    "confidence",
                    "logprob_score",
                    "token_count",
                ):
                    self.assertIn(key, row)


class CompareAggregateMetricSetTests(unittest.TestCase):
    """Pin the per-metric ``_AGGREGATE_METRICS`` table the SPA's
    AGGREGATE_DIRECTION map mirrors. A new metric added on the
    backend without a matching frontend entry would render with a
    raw key label and no winner highlight; pinning the set here
    forces both sides to update together.
    """

    def test_aggregate_metric_names_match_spa_direction_map(self) -> None:
        from amx.cli_support.commands.compare import _AGGREGATE_METRICS

        # Mirror of frontend/src/routes/RunsCompare.tsx
        # ``AGGREGATE_DIRECTION``. If you add a metric on either
        # side, update this set + the SPA together.
        spa_direction_map = {
            "wall_duration_sec",
            "model_processing_sec",
            "prompt_tokens",
            "completion_tokens",
            "total_tokens",
            "cost_usd",
            "avg_logprob_score",
            "pct_high_confidence",
            "pct_medium_confidence",
            "pct_low_confidence",
            "approval_rate",
            "saved_results",
        }
        backend_names = {export_name for export_name, _agg_key in _AGGREGATE_METRICS}
        self.assertEqual(
            spa_direction_map,
            backend_names,
            "SPA's AGGREGATE_DIRECTION must list exactly the metrics "
            "the backend's _AGGREGATE_METRICS emits.",
        )


class ComparePdfRenderingTests(unittest.TestCase):
    """PDF report rendering — pivots, winner highlights, density tuning,
    and a smoke test that WeasyPrint actually emits a valid PDF blob
    when the system has Pango/Cairo available.
    """

    def _payload(self, run_ids: list[int]) -> dict:
        """Build a synthetic ``compare_runs`` payload covering all the
        shapes the template depends on: summary rows, long-format
        aggregates with min / max / neutral metrics, and per-column
        rows with overlapping + non-overlapping assets across runs.
        """
        runs = [{"id": rid} for rid in run_ids]
        summary_rows = [
            {
                "run_id": rid,
                "started_at": "2026-05-08 10:00",
                "status": "success",
                "command": "analyze.run",
                "db_profile": "pg",
                "llm_profile": f"prof-{rid}",
                "llm_model": "gpt-4o",
                "doc_profile": "docs-prod",
                "code_profile": "code-main",
                "duration_sec": 10.0 + rid,
                "processed_count": 5,
                "applied_count": 4,
                "settings": {},
            }
            for rid in run_ids
        ]
        # Aggregates: total_tokens (min), avg_logprob_score (max),
        # pct_medium_confidence (neutral), saved_results (neutral).
        aggregates: list[dict] = []
        for i, rid in enumerate(run_ids):
            aggregates.append({"metric": "total_tokens", "run_id": rid, "value": 1000 - i * 10})
            aggregates.append(
                {"metric": "avg_logprob_score", "run_id": rid, "value": -0.5 - i * 0.1}
            )
            aggregates.append({"metric": "pct_medium_confidence", "run_id": rid, "value": 33.3})
            aggregates.append({"metric": "saved_results", "run_id": rid, "value": 7})
        per_column: list[dict] = []
        for i, rid in enumerate(run_ids):
            per_column.append(
                {
                    "schema": "sales",
                    "table": "orders",
                    "column": "id",
                    "run_id": rid,
                    "description": f"Primary key v{i}",
                    "confidence": ["high", "medium", "low"][i % 3],
                    "logprob_score": -0.2 - i * 0.05,
                    "token_count": 28,
                }
            )
        # Asset present only in the first run — exercises the
        # "missing cell" path.
        per_column.append(
            {
                "schema": "sales",
                "table": "orders",
                "column": "customer_id",
                "run_id": run_ids[0],
                "description": "FK to customers",
                "confidence": "high",
                "logprob_score": -0.1,
                "token_count": 14,
            }
        )
        return {
            "runs": runs,
            "summary_rows": summary_rows,
            "per_column": per_column,
            "aggregates": aggregates,
            "missing": [],
        }

    def test_aggregate_pivot_picks_min_winner_for_total_tokens(self) -> None:
        from amx.cli_support.commands.compare import _build_pdf_context

        ctx = _build_pdf_context(self._payload([10, 20, 30]))
        tokens_row = next(r for r in ctx["aggregate_rows"] if r["metric"] == "total_tokens")
        winners = {rid for rid, cell in tokens_row["cells"].items() if cell["is_winner"]}
        # Synthetic payload makes run 30 the lowest (1000 - 2*10 = 980).
        self.assertEqual(winners, {30})

    def test_aggregate_pivot_picks_max_winner_for_avg_logprob(self) -> None:
        from amx.cli_support.commands.compare import _build_pdf_context

        ctx = _build_pdf_context(self._payload([10, 20, 30]))
        logprob_row = next(r for r in ctx["aggregate_rows"] if r["metric"] == "avg_logprob_score")
        winners = {rid for rid, cell in logprob_row["cells"].items() if cell["is_winner"]}
        # Run 10's logprob is -0.5 (closest to 0 = best).
        self.assertEqual(winners, {10})

    def test_aggregate_pivot_marks_no_winner_for_neutral_metrics(self) -> None:
        from amx.cli_support.commands.compare import _build_pdf_context

        ctx = _build_pdf_context(self._payload([10, 20, 30]))
        for metric in ("pct_medium_confidence", "saved_results"):
            row = next(r for r in ctx["aggregate_rows"] if r["metric"] == metric)
            winners = [rid for rid, cell in row["cells"].items() if cell["is_winner"]]
            self.assertEqual(winners, [], f"{metric} must never highlight a winner")

    def test_per_column_pivot_picks_highest_logprob_per_asset(self) -> None:
        from amx.cli_support.commands.compare import _build_pdf_context

        ctx = _build_pdf_context(self._payload([10, 20, 30]))
        # Asset present in all 3 runs sorts first (overlap=3 > overlap=1).
        first = ctx["percol_rows"][0]
        self.assertEqual(first["label"], "sales.orders.id")
        winners = {rid for rid, cell in first["cells"].items() if cell and cell["is_winner"]}
        # Run 10 has the highest (least negative) logprob: -0.2.
        self.assertEqual(winners, {10})
        # Asset only in run 10 has its single cell as winner by default.
        second = ctx["percol_rows"][1]
        self.assertEqual(second["label"], "sales.orders.customer_id")
        self.assertIsNotNone(second["cells"][10])
        self.assertIsNone(second["cells"][20])
        self.assertIsNone(second["cells"][30])

    def test_density_scales_with_run_count(self) -> None:
        from amx.cli_support.commands.compare import _build_pdf_context

        small = _build_pdf_context(self._payload([1, 2]))
        big = _build_pdf_context(self._payload(list(range(1, 9))))  # 8 runs
        # More runs → smaller cell font (down to a 7pt floor).
        self.assertGreater(float(small["cell_font_pt"]), float(big["cell_font_pt"]))
        self.assertGreaterEqual(float(big["cell_font_pt"]), 7.0)

    def test_aggregate_cell_formatting(self) -> None:
        from amx.cli_support.commands.compare import _format_aggregate_cell

        self.assertEqual(_format_aggregate_cell("cost_usd", 0), "$0.00")
        self.assertEqual(_format_aggregate_cell("cost_usd", 0.005), "<$0.01")
        self.assertEqual(_format_aggregate_cell("cost_usd", 1.2345), "$1.2345")
        self.assertEqual(_format_aggregate_cell("pct_high_confidence", 67.4), "67%")
        self.assertEqual(_format_aggregate_cell("approval_rate", 0.5), "50%")
        self.assertEqual(_format_aggregate_cell("wall_duration_sec", 3.456), "3.46s")
        self.assertEqual(_format_aggregate_cell("avg_logprob_score", -0.7321), "-0.732")
        self.assertEqual(_format_aggregate_cell("total_tokens", None), "—")
        self.assertEqual(_format_aggregate_cell("total_tokens", 12345), "12,345")

    def test_pdf_direction_map_matches_aggregate_metric_set(self) -> None:
        """Mirror-test for the PDF-side direction table. Adding a new
        aggregate metric without a direction entry would silently land
        in the PDF as a neutral row; pin both sides together.
        """
        from amx.cli_support.commands.compare import (
            _AGGREGATE_METRICS,
            _PDF_AGGREGATE_DIRECTION,
        )

        backend_names = {export_name for export_name, _agg_key in _AGGREGATE_METRICS}
        self.assertEqual(
            backend_names,
            set(_PDF_AGGREGATE_DIRECTION.keys()),
            "_PDF_AGGREGATE_DIRECTION must cover exactly the metrics "
            "_AGGREGATE_METRICS emits — otherwise PDF rows render with "
            "no winner highlight.",
        )

    def test_bootstrap_appends_brew_prefix_on_macos(self) -> None:
        """The dyld bootstrap is what made the first user-facing
        report work — without it the ``brew install pango`` prefix
        ``/opt/homebrew/lib`` never enters dyld's search path and
        WeasyPrint's first dlopen fails with a 500 in production.
        Pin the env-var augmentation so a future refactor can't
        silently drop it.
        """
        import sys as _sys

        if _sys.platform != "darwin":
            self.skipTest("macOS-only behaviour")

        from amx.cli_support.commands import compare as compare_mod

        # Pretend the brew prefix exists even on hosts where it
        # doesn't, so the test doesn't depend on whether the dev
        # has actually run ``brew install pango``.
        before = os.environ.get("DYLD_FALLBACK_LIBRARY_PATH", "")
        os.environ.pop("DYLD_FALLBACK_LIBRARY_PATH", None)
        with patch("amx.cli_support.commands.compare.Path") as MockPath:
            instance = MockPath.return_value
            instance.is_dir.return_value = True
            try:
                compare_mod._bootstrap_weasyprint_native_libs()
                got = os.environ.get("DYLD_FALLBACK_LIBRARY_PATH", "")
            finally:
                if before:
                    os.environ["DYLD_FALLBACK_LIBRARY_PATH"] = before
                else:
                    os.environ.pop("DYLD_FALLBACK_LIBRARY_PATH", None)
        self.assertIn("/opt/homebrew/lib", got)

    def test_amx_logo_embeds_as_data_url(self) -> None:
        """The PDF running header pulls the AMX favicon from
        ``amx/web/static/favicon.png`` and inlines it as a base64
        data URL. The static dir is shipped inside the wheel
        (``[tool.setuptools.package-data]``), so the lookup must
        succeed both for an editable install and for a built wheel.
        """
        from amx.cli_support.commands.compare import _amx_logo_data_url

        data_url = _amx_logo_data_url()
        self.assertTrue(
            data_url.startswith("data:image/png;base64,"),
            "Favicon must encode as a PNG data URL; got "
            + (data_url[:40] if data_url else "(empty)"),
        )
        # Sanity: real favicon is ~13 KB; the encoded payload ought
        # to be at least an order of magnitude larger than the
        # ``data:image/png;base64,`` prefix.
        self.assertGreater(len(data_url), 1000)

    def test_logo_url_threaded_into_template_context(self) -> None:
        from amx.cli_support.commands.compare import _build_pdf_context

        ctx = _build_pdf_context(self._payload([1, 2]))
        self.assertIn("logo_data_url", ctx)
        self.assertTrue(
            ctx["logo_data_url"].startswith("data:image/png;base64,"),
            "Context must carry the logo as a data URL so the @page "
            "@top-right rule can render it on every page.",
        )

    def test_render_compare_pdf_returns_pdf_bytes(self) -> None:
        """Smoke test: WeasyPrint produces a valid PDF blob for an
        8-run payload (the dense layout case the user explicitly
        called out). Skipped when Pango/Cairo aren't installed on
        the test host — CI image installs them; local devs can
        ``brew install pango cairo`` to opt in.
        """
        # Check the pip packages are present *without* importing
        # WeasyPrint — that would trigger the native dlopen too
        # early, before render_compare_pdf's bootstrap helper has
        # had a chance to point dyld at the brew prefix.
        import importlib.util

        for module_name in ("jinja2", "weasyprint"):
            if importlib.util.find_spec(module_name) is None:
                self.skipTest(f"PDF deps not installed: {module_name}")

        from amx.cli_support.commands.compare import render_compare_pdf

        payload = self._payload(list(range(1, 9)))  # 8 runs
        try:
            pdf_bytes = render_compare_pdf(payload)
        except OSError as exc:
            # render_compare_pdf augments DYLD_FALLBACK_LIBRARY_PATH /
            # LD_LIBRARY_PATH with the standard brew / distro prefixes
            # before importing WeasyPrint. If the dlopen still fails
            # the host genuinely has no Pango installed — skip the
            # smoke test instead of failing the suite.
            self.skipTest(f"WeasyPrint native libs missing: {exc}")
        self.assertTrue(pdf_bytes.startswith(b"%PDF-"), "Output must be a PDF blob")
        self.assertGreater(len(pdf_bytes), 1000, "PDF should not be a stub")


class CompareAskToolTests(unittest.TestCase):
    """The Ask LLM tool registry now exposes ``compare_runs`` so
    natural-language requests like "compare runs 58, 59" route through
    the same pure helper the CLI ``/history compare`` and the Studio
    Compare modal already use. These tests pin the dispatcher contract
    — argument validation, summary-by-default payload shape, the
    ``include_per_column`` and ``column_filter`` drill-down knobs — so
    a refactor that breaks the LLM-facing surface trips a red test.
    """

    def _build_toolbox(self):
        from unittest.mock import MagicMock

        from amx.search.agent_tools import ToolBox

        cfg = AMXConfig()
        catalog = MagicMock()
        return ToolBox(cfg, catalog, db_factory=lambda: MagicMock())

    def _seed_two_overlapping_runs(self, store: SQLiteHistoryStore) -> tuple[int, int]:
        suggestion = {
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
        rid_a = _seed_run(store, llm_profile="prof-a", suggestions=[suggestion])
        rid_b = _seed_run(
            store,
            llm_profile="prof-b",
            suggestions=[{**suggestion, "logprob_score": 0.78, "confidence": "medium"}],
        )
        return rid_a, rid_b

    def test_rejects_fewer_than_two_run_ids(self) -> None:
        from unittest.mock import patch

        with tempfile.TemporaryDirectory() as tmp:
            store = SQLiteHistoryStore(Path(tmp) / "history.db")
            store.init()
            toolbox = self._build_toolbox()
            with patch("amx.cli_support.commands.compare.history_store", return_value=store):
                empty = toolbox._tool_compare_runs(run_ids=[])
                single = toolbox._tool_compare_runs(run_ids=[42])
        self.assertIn("error", empty)
        self.assertIn("at least 2", empty["error"])
        self.assertIn("error", single)
        self.assertIn("at least 2", single["error"])

    def test_default_returns_summary_with_sample_not_full_pivot(self) -> None:
        """Default invocation must keep the per-column pivot OUT of the
        payload — only a 3-row sample plus a total count — so the LLM
        doesn't blow its context window on an 8-run × 200-column
        comparison. Drill-down is the LLM's call via include_per_column.
        """
        from unittest.mock import patch

        with tempfile.TemporaryDirectory() as tmp:
            store = SQLiteHistoryStore(Path(tmp) / "history.db")
            store.init()
            rid_a, rid_b = self._seed_two_overlapping_runs(store)
            toolbox = self._build_toolbox()
            with patch("amx.cli_support.commands.compare.history_store", return_value=store):
                payload = toolbox._tool_compare_runs(run_ids=[rid_a, rid_b])

        self.assertNotIn("per_column", payload)
        self.assertIn("per_column_sample", payload)
        self.assertIn("per_column_count", payload)
        self.assertLessEqual(len(payload["per_column_sample"]), 3)
        # Summary shape — one row per run, both runs visible.
        self.assertEqual(len(payload["summary_rows"]), 2)
        self.assertEqual({r["run_id"] for r in payload["summary_rows"]}, {rid_a, rid_b})
        self.assertGreater(len(payload["aggregates"]), 0)

    def test_include_per_column_returns_full_pivot(self) -> None:
        from unittest.mock import patch

        with tempfile.TemporaryDirectory() as tmp:
            store = SQLiteHistoryStore(Path(tmp) / "history.db")
            store.init()
            rid_a, rid_b = self._seed_two_overlapping_runs(store)
            toolbox = self._build_toolbox()
            with patch("amx.cli_support.commands.compare.history_store", return_value=store):
                payload = toolbox._tool_compare_runs(
                    run_ids=[rid_a, rid_b],
                    include_per_column=True,
                )
        self.assertIn("per_column", payload)
        self.assertNotIn("per_column_sample", payload)
        self.assertEqual(payload["per_column_count"], len(payload["per_column"]))

    def test_column_filter_restricts_per_column_rows(self) -> None:
        """When the user is asking about one specific column the LLM
        should pass column_filter — much cheaper than the full pivot.
        The response must include only that column's rows and surface
        the filter back so the model can confirm what it received.
        """
        from unittest.mock import patch

        with tempfile.TemporaryDirectory() as tmp:
            store = SQLiteHistoryStore(Path(tmp) / "history.db")
            store.init()
            rid_a, rid_b = self._seed_two_overlapping_runs(store)
            toolbox = self._build_toolbox()
            with patch("amx.cli_support.commands.compare.history_store", return_value=store):
                payload = toolbox._tool_compare_runs(
                    run_ids=[rid_a, rid_b],
                    column_filter="id",
                )
        self.assertEqual(payload.get("column_filter"), "id")
        self.assertIn("per_column", payload)
        for row in payload["per_column"]:
            self.assertEqual(row.get("column"), "id")

    def test_invalid_run_id_returns_clean_error(self) -> None:
        from unittest.mock import patch

        with tempfile.TemporaryDirectory() as tmp:
            store = SQLiteHistoryStore(Path(tmp) / "history.db")
            store.init()
            toolbox = self._build_toolbox()
            with patch("amx.cli_support.commands.compare.history_store", return_value=store):
                payload = toolbox._tool_compare_runs(
                    run_ids=["not-a-number", "also-bad"],  # type: ignore[list-item]
                )
        self.assertIn("error", payload)
        self.assertIn("Invalid", payload["error"])

    def test_compare_runs_schema_required_run_ids(self) -> None:
        from amx.search.agent_tools import ToolBox

        entry = next(
            (s for s in ToolBox.schemas() if s.get("function", {}).get("name") == "compare_runs"),
            None,
        )
        self.assertIsNotNone(entry, "compare_runs must appear in ToolBox.schemas()")
        params = entry["function"]["parameters"]
        self.assertIn("run_ids", params["required"])
        self.assertIn("include_per_column", params["properties"])
        self.assertIn("column_filter", params["properties"])


class CompareQualityTests(unittest.TestCase):
    """Quality framework tests — Tier 0 / Tier 1 / Tier 2 metrics plus
    the reference-resolution waterfall and the academic citation list.

    These pin the contract between the CLI / Studio / Ask AMX surfaces
    and the quality module: a future refactor that drops chrF or
    breaks the citation registry trips a red test instead of silently
    shipping unattributed metrics.
    """

    def test_schema_grounding_rewards_descriptions_that_cite_column_metadata(
        self,
    ) -> None:
        from amx.cli_support.quality import schema_grounding_score

        # Description that names the column + dtype scores high.
        good = schema_grounding_score(
            "Primary key for the orders table, integer.",
            schema="sales",
            table="orders",
            column="id",
            dtype="integer",
        )
        # Generic description (no column/table/dtype mention) scores low.
        bad = schema_grounding_score(
            "A primary key value used internally by the system.",
            schema="sales",
            table="orders",
            column="id",
            dtype="integer",
        )
        self.assertGreater(good, bad)
        self.assertGreaterEqual(good, 0.5)

    def test_chrf_and_rouge_l_drop_when_no_reference(self) -> None:
        from amx.cli_support.quality import chrf_score, rouge_l_score

        self.assertIsNone(chrf_score("anything", ""))
        self.assertIsNone(rouge_l_score("anything", ""))

    def test_chrf_and_rouge_l_correlate_with_reference_overlap(self) -> None:
        from amx.cli_support.quality import chrf_score, rouge_l_score

        ref = "Primary key uniquely identifying each order record."
        close = "Primary key uniquely identifying each order."
        far = "A surrogate identifier on the table."
        # Closer paraphrase scores higher on both metrics.
        self.assertGreater(chrf_score(close, ref), chrf_score(far, ref))
        self.assertGreater(rouge_l_score(close, ref), rouge_l_score(far, ref))

    def test_reference_waterfall_user_pin_overrides_db_and_catalog(self) -> None:
        from unittest.mock import MagicMock

        from amx.cli_support.quality import resolve_reference_for_asset

        store = MagicMock()
        # Even if the live DB has a comment, the user pin should win.
        db = MagicMock()
        db.get_column_comments.return_value = {"id": "DB COMMENT TEXT"}
        store.get_run_results.return_value = [
            {
                "schema_name": "sales",
                "table_name": "orders",
                "column_name": "id",
                "chosen_description": "PIN TEXT",
            }
        ]
        store.list_apply_events.return_value = []
        ref = resolve_reference_for_asset(
            schema="sales",
            table="orders",
            column="id",
            runs=[{"id": 7}, {"id": 8}],
            db_connector=db,
            history_store=store,
            ground_truth_run_id=7,
        )
        self.assertEqual(ref.source, "user_pinned")
        self.assertEqual(ref.text, "PIN TEXT")
        self.assertEqual(ref.run_id, 7)

    def test_reference_waterfall_falls_back_to_db_then_catalog_then_none(
        self,
    ) -> None:
        from unittest.mock import MagicMock

        from amx.cli_support.quality import resolve_reference_for_asset

        # (1) Live DB comment present → wins over catalog history.
        db = MagicMock()
        db.get_column_comments.return_value = {"id": "DB COMMENT"}
        store = MagicMock()
        store.list_apply_events.return_value = [
            {
                "schema_name": "sales",
                "table_name": "orders",
                "column_name": "id",
                "new_comment": "OLD APPLIED",
            }
        ]
        ref = resolve_reference_for_asset(
            schema="sales",
            table="orders",
            column="id",
            runs=[],
            db_connector=db,
            history_store=store,
        )
        self.assertEqual(ref.source, "db_comment")
        self.assertEqual(ref.text, "DB COMMENT")

        # (2) DB silent → catalog history wins.
        db.get_column_comments.return_value = {"id": None}
        ref = resolve_reference_for_asset(
            schema="sales",
            table="orders",
            column="id",
            runs=[],
            db_connector=db,
            history_store=store,
        )
        self.assertEqual(ref.source, "catalog_applied")
        self.assertEqual(ref.text, "OLD APPLIED")

        # (3) Both silent → none.
        store.list_apply_events.return_value = []
        ref = resolve_reference_for_asset(
            schema="sales",
            table="orders",
            column="id",
            runs=[],
            db_connector=db,
            history_store=store,
        )
        self.assertEqual(ref.source, "none")
        self.assertEqual(ref.text, "")

    def test_compute_quality_metrics_emits_per_run_and_citations(self) -> None:
        from amx.cli_support.quality import compute_quality_metrics

        # Synthetic compare payload — two runs, one shared column.
        payload = {
            "runs": [{"id": 1}, {"id": 2}],
            "summary_rows": [{"run_id": 1}, {"run_id": 2}],
            "aggregates": [],
            "per_column": [
                {
                    "schema": "sales",
                    "table": "orders",
                    "column": "id",
                    "run_id": 1,
                    "description": "Primary key for the orders table.",
                    "dtype": "integer",
                },
                {
                    "schema": "sales",
                    "table": "orders",
                    "column": "id",
                    "run_id": 2,
                    "description": "Generic identifier.",
                    "dtype": "integer",
                },
            ],
            "missing": [],
        }
        out = compute_quality_metrics(payload, tier=0)
        self.assertEqual(len(out["per_run"]), 2)
        # Schema grounding is reference-free, so it must always show up.
        for row in out["per_run"]:
            self.assertIn("schema_grounding", row)
        # No reference resolved → reference-based citations are still
        # listed even with zero hits, because the user might enable
        # them later via --ground-truth-run.
        labels = [c["label"] for c in out["citations"]]
        self.assertIn("Type-token ratio", labels)
        self.assertIn("Jaccard similarity (schema grounding)", labels)

    def test_academic_references_registry_covers_every_metric(self) -> None:
        """Pin the citation registry: every metric advertised in the
        UI / PDF must have an entry, otherwise the methods footer
        renders without attribution."""
        from amx.cli_support.quality import ACADEMIC_REFERENCES

        for key in (
            "chrf",
            "rouge_l",
            "bertscore",
            "g_eval",
            "prometheus",
            "type_token_ratio",
            "levenshtein",
            "jaccard",
        ):
            self.assertIn(key, ACADEMIC_REFERENCES)
            self.assertTrue(ACADEMIC_REFERENCES[key]["citation"])
            self.assertTrue(ACADEMIC_REFERENCES[key]["label"])

    def test_bert_score_returns_none_without_reference(self) -> None:
        from amx.cli_support.quality import bert_score_for_pair

        # BERTScore is reference-based — empty reference must short-
        # circuit cleanly so callers can fall through gracefully on
        # assets that have no resolved ground truth.
        self.assertIsNone(bert_score_for_pair("anything", ""))
        self.assertIsNone(bert_score_for_pair("", "reference"))

    def test_judge_cost_logged_as_app_event(self) -> None:
        """When Tier 2 judge calls actually fire, the aggregate cost
        rolls into the ``app_events`` audit trail so it shows up in
        /usage / Studio Audit alongside other compare events. We do
        NOT mutate the compared runs' ``tokens_json`` — those rows
        are closed historical records of the analyze runs.
        """
        from unittest.mock import MagicMock

        from amx.cli_support.quality import compute_quality_metrics

        store = MagicMock()
        # Fake out enough surface that compute_quality_metrics can
        # find the per-asset rows. The judge tournament needs the
        # llm_provider to be present and tier=2 to fire.
        llm = MagicMock()
        llm.chat.return_value = MagicMock(
            content='{"winner": "A", "reasoning": "fake", "confidence": 0.9}',
            usage={"prompt_tokens": 50, "completion_tokens": 10},
        )
        payload = {
            "runs": [{"id": 1}, {"id": 2}],
            "summary_rows": [{"run_id": 1}, {"run_id": 2}],
            "aggregates": [],
            "per_column": [
                {
                    "schema": "sales",
                    "table": "orders",
                    "column": "id",
                    "run_id": 1,
                    "description": "Primary key.",
                },
                {
                    "schema": "sales",
                    "table": "orders",
                    "column": "id",
                    "run_id": 2,
                    "description": "Order id.",
                },
            ],
            "missing": [],
        }
        out = compute_quality_metrics(
            payload,
            tier=2,
            history_store=store,
            llm_provider=llm,
        )
        # Cost is captured in the response.
        self.assertEqual(out["cost"]["prompt_tokens"], 50)
        self.assertEqual(out["cost"]["completion_tokens"], 10)
        # AND audited via log_event on the app_events trail.
        store.log_event.assert_called_once()
        kwargs = store.log_event.call_args.kwargs
        self.assertEqual(kwargs.get("event_type"), "quality_judge")
        self.assertEqual(kwargs.get("command"), "search.compare")
        self.assertIn("prompt_tokens", kwargs.get("details") or {})

    def test_compare_runs_payload_omits_quality_when_tier_zero_and_no_pin(
        self,
    ) -> None:
        """The default ``compare_runs(run_ids)`` call (tier=0, no pin)
        must NOT compute quality metrics — the modal auto-load stays
        cheap. Quality only kicks in when explicitly requested.
        """
        from unittest.mock import patch

        from amx.cli_support.commands.compare import compare_runs

        with tempfile.TemporaryDirectory() as tmp:
            store = SQLiteHistoryStore(Path(tmp) / "history.db")
            store.init()
            _seed_run(store)
            _seed_run(store, llm_profile="alt")
            with patch(
                "amx.cli_support.commands.compare.history_store",
                return_value=store,
            ):
                payload = compare_runs([1, 2])
        self.assertNotIn("quality_metrics", payload)


class CompareDispatchUnderHistoryNamespaceTests(unittest.TestCase):
    """User feedback 2026-05-02: ``/compare`` belongs under ``/history``,
    not ``/search`` — comparing past runs is fundamentally an audit
    operation. The slash registry now lists it under
    ``_HISTORY_COMMANDS`` and the session shortcut_map routes the bare
    verb to ``["history", "compare"]`` from any namespace.
    """

    def test_compare_dispatches_to_history_from_root(self) -> None:
        from amx.cli_support.session import session_to_click_args

        self.assertEqual(
            session_to_click_args("", ["compare"]),
            ["history", "compare"],
        )

    def test_compare_dispatches_to_history_from_search_namespace(self) -> None:
        """Pre-fix the verb was registered under search and a bare
        ``/compare`` from /search would fall through to ``["search",
        "ask", "compare"]``. After moving it to /history the same
        bare invocation must resolve to ``["history", "compare"]``."""
        from amx.cli_support.session import session_to_click_args

        self.assertEqual(
            session_to_click_args("search", ["compare"]),
            ["history", "compare"],
        )

    def test_compare_listed_under_history_namespace_in_registry(self) -> None:
        from amx.cli_support.slash_commands import (
            cmd_heads_for_namespace,
            find_command,
        )

        compare = find_command("/compare")
        self.assertIsNotNone(compare)
        self.assertEqual(compare.namespace, "history")
        self.assertIn("compare", cmd_heads_for_namespace("history"))
        # And NOT under /search anymore
        self.assertNotIn("compare", cmd_heads_for_namespace("search"))


class RunSettingsSnapshotTests(unittest.TestCase):
    """The user-reported gap: ``/compare`` showed profiles + tokens but
    not the actual knobs (prompt_detail, language, batch_size, …) that
    they varied between runs. Now ``analysis_runs.settings_json``
    captures every LLM/run config field at run-start time, and
    ``Run settings`` renders them.
    """

    def test_create_run_persists_settings_dict_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = SQLiteHistoryStore(Path(tmp) / "history.db")
            store.init()
            settings = {
                "prompt_detail": "detailed",
                "language": "english",
                "column_batch_size": 8,
                "n_alternatives": 5,
                "completion_mode": "chat_completions",
                "description_verbosity": "brief",
                "temperature": 0.3,
                "force_logprobs": True,
                "dedup_used": True,
                "missing_only": False,
                "review_strategy": "auto-apply",
                "use_batch": False,
            }
            rid = store.create_run(
                command="analyze.run",
                mode="chat",
                db_backend="postgresql",
                db_profile="pg",
                llm_provider="openai",
                llm_model="gpt-4o",
                scope={"sales": ["orders"]},
                llm_profile="default",
                settings=settings,
            )
            row = store.get_run(rid)
            assert row is not None
            self.assertEqual(row["settings_json"], settings)

            # Also exposed through the scope-filter query path used by
            # /history compare's --last resolution.
            scope_runs = store.find_runs_for_scope(schema="sales", limit=5)
            self.assertEqual(len(scope_runs), 1)
            self.assertEqual(scope_runs[0]["settings_json"], settings)

    def test_legacy_runs_with_no_settings_json_round_trip_as_none(self) -> None:
        """Pre-migration runs (settings_json column added in this PR)
        must continue to round-trip without crashing — the table
        renders ``—`` for them, but the run is otherwise valid history.
        """
        with tempfile.TemporaryDirectory() as tmp:
            store = SQLiteHistoryStore(Path(tmp) / "history.db")
            store.init()
            rid = store.create_run(
                command="analyze.run",
                mode="chat",
                db_backend="postgresql",
                db_profile="pg",
                llm_provider="openai",
                llm_model="gpt-4o",
                scope={"sales": ["orders"]},
                # No settings= passed
            )
            row = store.get_run(rid)
            self.assertIsNone(row.get("settings_json"))

    def test_settings_for_run_returns_empty_dict_for_legacy_or_missing(self) -> None:
        from amx.cli_support.commands.compare import _settings_for_run

        # Missing key
        self.assertEqual(_settings_for_run({"id": 1}), {})
        # JSON string (storage layer hasn't deserialised — defensive)
        self.assertEqual(
            _settings_for_run({"settings_json": '{"prompt_detail": "minimal"}'}),
            {"prompt_detail": "minimal"},
        )
        # Already a dict (the typical post-deserialisation shape)
        d = {"prompt_detail": "detailed", "n_alternatives": 5}
        self.assertEqual(_settings_for_run({"settings_json": d}), d)
        # Garbage JSON degrades gracefully
        self.assertEqual(_settings_for_run({"settings_json": "{not json"}), {})


class AskHistoryToolsTests(unittest.TestCase):
    """User report 2026-05-02: /ask said "I don't have access to your past
    runs". Wired ``list_past_runs`` and ``describe_run`` into the search
    agent's tool registry so the LLM can introspect the same data
    ``/history compare`` reads. These tests exercise the tool-handler
    layer directly — mocking the LLM is out of scope here, but verifying
    the tools return the right shape catches every regression that
    matters."""

    def _build_toolbox(self, store: SQLiteHistoryStore):
        from unittest.mock import MagicMock

        from amx.search.agent_tools import ToolBox

        cfg = AMXConfig()
        catalog = MagicMock()
        # The toolbox lazily creates a DatabaseConnector via db_factory;
        # past-runs tools don't touch the live DB so any factory works.
        toolbox = ToolBox(cfg, catalog, db_factory=lambda: MagicMock())
        # Replace the global history_store accessor for the duration of
        # the test so the tool sees our seeded store.
        return toolbox

    def test_list_past_runs_returns_compact_settings_aware_payload(self) -> None:
        from unittest.mock import patch

        with tempfile.TemporaryDirectory() as tmp:
            store = SQLiteHistoryStore(Path(tmp) / "history.db")
            store.init()
            rid_a = store.create_run(
                command="analyze.run",
                mode="chat",
                db_backend="postgresql",
                db_profile="pg",
                llm_provider="openai",
                llm_model="gpt-4o",
                scope={"sales": ["orders"]},
                llm_profile="default",
                doc_profile="docs-prod",
                code_profile="code-main",
                settings={
                    "prompt_detail": "detailed",
                    "n_alternatives": 5,
                    "column_batch_size": 10,
                    "dedup_used": True,
                },
            )
            rid_b = store.create_run(
                command="analyze.run",
                mode="chat",
                db_backend="postgresql",
                db_profile="pg",
                llm_provider="anthropic",
                llm_model="claude-sonnet-4-6",
                scope={"sales": ["orders"]},
                llm_profile="claude",
                settings={
                    "prompt_detail": "minimal",
                    "n_alternatives": 3,
                    "column_batch_size": 8,
                    "dedup_used": False,
                },
            )

            toolbox = self._build_toolbox(store)
            with patch("amx.storage.sqlite_store.history_store", return_value=store):
                payload = toolbox._tool_list_past_runs(schema="sales", limit=10)

            self.assertEqual(payload["count"], 2)
            self.assertEqual(payload["filter"]["schema"], "sales")
            run_ids = {r["run_id"] for r in payload["runs"]}
            self.assertEqual(run_ids, {rid_a, rid_b})
            # Settings ride along so the LLM can compare profiles directly.
            for run in payload["runs"]:
                self.assertIn("settings", run)
                self.assertIn("prompt_detail", run["settings"])
                self.assertIn("n_alternatives", run["settings"])

    def test_list_past_runs_command_filter_validates(self) -> None:
        from unittest.mock import patch

        with tempfile.TemporaryDirectory() as tmp:
            store = SQLiteHistoryStore(Path(tmp) / "history.db")
            store.init()
            toolbox = self._build_toolbox(store)
            with patch("amx.storage.sqlite_store.history_store", return_value=store):
                bogus = toolbox._tool_list_past_runs(command="bogus")
            self.assertIn("error", bogus)
            self.assertIn("Invalid 'command'", bogus["error"])

    def test_list_past_runs_handles_missing_store(self) -> None:
        from unittest.mock import patch

        with tempfile.TemporaryDirectory() as tmp:
            store = SQLiteHistoryStore(Path(tmp) / "history.db")
            store.init()
            toolbox = self._build_toolbox(store)
            with patch("amx.storage.sqlite_store.history_store", return_value=None):
                payload = toolbox._tool_list_past_runs(limit=5)
            self.assertEqual(payload["count"], 0)
            self.assertIn("note", payload)
            self.assertIn("history store is initialised", payload["note"])

    def test_describe_run_returns_settings_metrics_and_results(self) -> None:
        from unittest.mock import patch

        with tempfile.TemporaryDirectory() as tmp:
            store = SQLiteHistoryStore(Path(tmp) / "history.db")
            store.init()
            rid = store.create_run(
                command="analyze.run",
                mode="chat",
                db_backend="postgresql",
                db_profile="pg",
                llm_provider="openai",
                llm_model="gpt-4o",
                scope={"sales": ["orders"]},
                llm_profile="default",
                settings={"prompt_detail": "standard"},
            )
            store.save_run_results(
                rid,
                [
                    {
                        "schema": "sales",
                        "table": "orders",
                        "column": "id",
                        "asset_kind": "table",
                        "source": "code",
                        "confidence": "high",
                        "logprob_score": 0.92,
                        "raw_logprob": 0.92,
                        "token_count": 28,
                        "model_version": "gpt-4o",
                        "reasoning": "primary key",
                        "alternatives": ["Primary key for orders"],
                    }
                ],
            )

            toolbox = self._build_toolbox(store)
            with patch("amx.storage.sqlite_store.history_store", return_value=store):
                row = toolbox._tool_describe_run(run_id=rid, include_results=True)

            self.assertEqual(row["run_id"], rid)
            self.assertEqual(row["settings"], {"prompt_detail": "standard"})
            self.assertEqual(row["llm_model"], "gpt-4o")
            self.assertEqual(row["results_count"], 1)
            self.assertEqual(row["results"][0]["column"], "id")
            self.assertEqual(row["results"][0]["confidence"], "high")
            self.assertAlmostEqual(row["results"][0]["logprob_score"] or 0, 0.92, places=4)

    def test_describe_run_invalid_id_returns_error(self) -> None:
        from unittest.mock import patch

        with tempfile.TemporaryDirectory() as tmp:
            store = SQLiteHistoryStore(Path(tmp) / "history.db")
            store.init()
            toolbox = self._build_toolbox(store)
            with patch("amx.storage.sqlite_store.history_store", return_value=store):
                row = toolbox._tool_describe_run(run_id="not-a-number")  # type: ignore[arg-type]
            self.assertIn("error", row)
            self.assertIn("Invalid run_id", row["error"])

    def test_past_runs_tools_are_in_the_llm_tool_schema(self) -> None:
        """The new tools must show up in ``ToolBox.schemas()`` so LiteLLM
        forwards them to the LLM. Otherwise the system prompt's
        instruction to "call list_past_runs" produces a hallucinated
        function-call name the agent loop can't dispatch."""
        from amx.search.agent_tools import ToolBox

        names = {entry["function"]["name"] for entry in ToolBox.schemas()}
        self.assertIn("list_past_runs", names)
        self.assertIn("describe_run", names)
        self.assertIn("list_chat_sessions", names)
        self.assertIn("compare_runs", names)

    def test_list_past_runs_default_filters_to_analyze_run(self) -> None:
        """User report 2026-05-02 (image 2): /ask sessions polluted the
        run history listing. Default filter is now ``analyze.run`` so
        questions like "which amx runs do you see" return /run history,
        not search.ask audit entries.
        """
        from unittest.mock import patch

        with tempfile.TemporaryDirectory() as tmp:
            store = SQLiteHistoryStore(Path(tmp) / "history.db")
            store.init()
            _seed_run(store, schema="sales", table="orders", command="analyze.run")
            _seed_run(store, schema="sales", table="orders", command="search.ask")
            _seed_run(store, schema="sales", table="orders", command="search.ask")

            toolbox = self._build_toolbox(store)
            with patch("amx.storage.sqlite_store.history_store", return_value=store):
                default = toolbox._tool_list_past_runs()
                explicit_all = toolbox._tool_list_past_runs(command="all")
                ask_only = toolbox._tool_list_past_runs(command="search.ask")

        self.assertEqual(
            {r["command"] for r in default["runs"]},
            {"analyze.run"},
            "Default filter must exclude search.ask — chat sessions belong in "
            "list_chat_sessions, not list_past_runs.",
        )
        self.assertEqual(len(explicit_all["runs"]), 3)
        self.assertEqual({r["command"] for r in ask_only["runs"]}, {"search.ask"})

    def test_list_past_runs_returns_human_readable_timestamps(self) -> None:
        """User report 2026-05-02 (image 1): the LLM rendered raw epoch
        timestamps and raw float seconds (``1777675166.705911``,
        ``60.27061319351196``) when asked for a table. Tool output now
        includes formatted ``started_at`` (ISO) and ``duration_human``
        fields, plus a ``presentation_hint`` telling the LLM how to
        render compact tables.
        """
        from unittest.mock import patch

        with tempfile.TemporaryDirectory() as tmp:
            store = SQLiteHistoryStore(Path(tmp) / "history.db")
            store.init()
            _seed_run(store)

            toolbox = self._build_toolbox(store)
            with patch("amx.storage.sqlite_store.history_store", return_value=store):
                payload = toolbox._tool_list_past_runs()

        self.assertEqual(payload["count"], 1)
        row = payload["runs"][0]
        # Both raw + human-readable fields are present so the LLM picks
        # the right one and the tool stays machine-consumable.
        self.assertIn("started_at", row)
        self.assertIn("started_at_epoch", row)
        self.assertIn("duration_human", row)
        self.assertIn("duration_sec", row)
        self.assertIn("model_processing_human", row)
        # The ISO timestamp doesn't begin with the year 1970 (epoch 0),
        # so it actually formatted something.
        self.assertNotEqual(row["started_at"], "—")
        self.assertNotIn("1970", row["started_at"])
        # presentation_hint nudges the LLM toward a 6-column compact table.
        self.assertIn("presentation_hint", payload)
        self.assertIn("compact table", payload["presentation_hint"].lower())

    def test_history_list_runs_wizard_when_no_flags_passed(self) -> None:
        """User feedback 2026-05-02: /history list should ask the user
        (like /run) when there are options to choose from, instead of
        forcing flag syntax. Bare ``/history list`` now prompts for
        limit + include-asks; explicit flags skip the matching prompt.
        """
        from amx.cli import main

        prompts: list[tuple[str, str]] = []

        def fake_ask(question: str, default: str = "") -> str:
            prompts.append(("ask", question))
            return default

        def fake_choice(question, choices, default="", descriptions=None):
            prompts.append(("choice", question))
            return default

        with tempfile.TemporaryDirectory() as tmp:
            runner = CliRunner()

            # Bare /history list — both prompts must fire.
            with (
                patch("amx.utils.console.ask", side_effect=fake_ask),
                patch("amx.utils.console.ask_choice", side_effect=fake_choice),
            ):
                runner.invoke(
                    main,
                    ["--config", str(Path(tmp) / "noconfig.yml"), "history", "list"],
                    env={"AMX_SESSION_CHILD": "1"},
                    catch_exceptions=False,
                )
            self.assertEqual(
                [k for k, _ in prompts],
                ["ask", "choice"],
                "Bare /list must ask 'how many runs' THEN 'include /ask sessions'.",
            )

            # -n 5 explicit → only the asks prompt fires.
            prompts.clear()
            with (
                patch("amx.utils.console.ask", side_effect=fake_ask),
                patch("amx.utils.console.ask_choice", side_effect=fake_choice),
            ):
                runner.invoke(
                    main,
                    ["--config", str(Path(tmp) / "noconfig.yml"), "history", "list", "-n", "5"],
                    env={"AMX_SESSION_CHILD": "1"},
                    catch_exceptions=False,
                )
            self.assertEqual([k for k, _ in prompts], ["choice"])

            # Both flags explicit → zero prompts (power user / scripts).
            prompts.clear()
            with (
                patch("amx.utils.console.ask", side_effect=fake_ask),
                patch("amx.utils.console.ask_choice", side_effect=fake_choice),
            ):
                runner.invoke(
                    main,
                    [
                        "--config",
                        str(Path(tmp) / "noconfig.yml"),
                        "history",
                        "list",
                        "-n",
                        "5",
                        "--include-asks",
                    ],
                    env={"AMX_SESSION_CHILD": "1"},
                    catch_exceptions=False,
                )
            self.assertEqual(
                prompts, [], "Power-user invocation with both flags must skip the wizard."
            )

    def test_list_chat_sessions_returns_resumable_threads(self) -> None:
        """``/ask`` chat sessions live in chat_sessions / chat_turns and
        are resumable via ``/session resume <id>`` — surface them via
        their own tool so the LLM doesn't conflate per-turn audit-log
        rows with full conversation threads.
        """
        from unittest.mock import patch

        from amx.search.session_store import ChatSessionStore

        with tempfile.TemporaryDirectory() as tmp:
            store = SQLiteHistoryStore(Path(tmp) / "history.db")
            store.init()
            session_store = ChatSessionStore(store)
            sid = session_store.start_session(
                db_profile="pg", llm_profile="default", title="Test thread"
            )
            session_store.append_user_turn(sid, question="What schemas exist?")

            toolbox = self._build_toolbox(store)
            with patch("amx.storage.sqlite_store.history_store", return_value=store):
                payload = toolbox._tool_list_chat_sessions()

        self.assertEqual(payload["count"], 1)
        self.assertEqual(payload["sessions"][0]["session_id"], sid)
        self.assertEqual(payload["sessions"][0]["first_question"], "What schemas exist?")
        self.assertTrue(payload["sessions"][0]["is_active"])
        self.assertIn("/session resume", payload["note"])


class CatalogDiscoveryToolsTests(unittest.TestCase):
    """User report 2026-05-04: defining a Databricks DB profile without
    pinning a catalog made ``/ask`` fail with NO_SUCH_CATALOG_EXCEPTION on
    the first listing call. The agent now has dedicated catalog-discovery
    tools (``list_catalogs`` / ``list_server_databases``) and the schema-
    listing tools accept an optional ``catalog`` argument so the LLM can
    drill in without mutating the saved profile."""

    def _toolbox(self, fake_db, *, pinned_catalog: str = "", pinned_database: str = ""):
        from unittest.mock import MagicMock

        from amx.search.agent_tools import ToolBox

        cfg = AMXConfig()
        cfg.db.backend = "databricks"
        cfg.db.catalog = pinned_catalog
        cfg.db.database = pinned_database
        catalog = MagicMock()
        return ToolBox(cfg, catalog, db_factory=lambda: fake_db)

    def test_list_schemas_returns_catalog_list_when_multiple_user_catalogs(self) -> None:
        """When a 3-level backend has no catalog pinned and the workspace
        exposes multiple user catalogs, list_schemas must surface the
        filtered list (system catalogs dropped) plus a ``needs_catalog``
        flag so the LLM can recurse with the chosen catalog."""

        class FakeDB:
            def supports_catalogs(self) -> bool:
                return True

            def list_catalogs(self) -> list[str]:
                # Two user catalogs (analytics, sap) plus the standard
                # Databricks system catalogs that must NOT show up in
                # the surfaced list.
                return ["analytics", "sap", "samples", "system", "workspace"]

            cfg = type("DBCfg", (), {"catalog": ""})()

        toolbox = self._toolbox(FakeDB())
        payload = toolbox._tool_list_schemas()
        self.assertTrue(payload["needs_catalog"])
        # System catalogs (samples / system / workspace) filtered out.
        self.assertEqual(payload["catalogs"], ["analytics", "sap"])
        # Full list surfaced as a fallback so the LLM can override.
        self.assertEqual(
            payload["all_catalogs"], ["analytics", "sap", "samples", "system", "workspace"]
        )
        self.assertIn("no catalog pinned", payload["message"])

    def test_list_schemas_auto_picks_single_user_catalog(self) -> None:
        """User report 2026-05-04: kimi-thinking entered a degenerate
        loop ('I see 4 catalogs, let me check amx_test first…') instead
        of recursing because the previous behaviour always punted to
        the LLM. When exactly one non-system catalog is visible we now
        auto-pick it and return the schemas in a single round-trip,
        with ``auto_picked_catalog`` set so the LLM can mention it."""

        class FakeDB:
            def __init__(self) -> None:
                self.cfg = type("DBCfg", (), {"catalog": ""})()
                self.observed_catalog: str = ""

            def supports_catalogs(self) -> bool:
                return True

            def list_catalogs(self) -> list[str]:
                # The user's reported case: one user catalog
                # (``amx_test``) plus three Databricks system catalogs.
                return ["amx_test", "samples", "system", "workspace"]

            def list_schemas(self) -> list[str]:
                self.observed_catalog = self.cfg.catalog
                return ["sales", "ops"]

        fake = FakeDB()
        toolbox = self._toolbox(fake)
        payload = toolbox._tool_list_schemas()
        # Auto-picked the single user catalog and listed its schemas.
        self.assertEqual(payload["schemas"], ["sales", "ops"])
        self.assertEqual(payload["catalog"], "amx_test")
        self.assertEqual(payload["auto_picked_catalog"], "amx_test")
        self.assertNotIn("needs_catalog", payload)
        # Connector saw the auto-pinned catalog while listing.
        self.assertEqual(fake.observed_catalog, "amx_test")
        # And the cfg was restored.
        self.assertEqual(fake.cfg.catalog, "")

    def test_list_tables_in_schema_auto_picks_catalog_when_unpinned(self) -> None:
        """User report 2026-05-04 (third loop): list_schemas correctly
        auto-picked ``amx_test`` and returned the schema list, but
        list_tables_in_schema(schema='amx_test_schema') did NOT
        auto-pick — cfg.catalog stayed empty, the inspector emitted
        ``SHOW TABLES FROM None.amx_test_schema``, and the LLM
        answered with the warehouse error instead of the table list.
        Auto-pick now applies at every catalog-scoped tool, not only
        list_schemas."""

        class FakeDB:
            def __init__(self) -> None:
                self.cfg = type("DBCfg", (), {"catalog": ""})()
                self.observed_catalog: str = ""

            def supports_catalogs(self) -> bool:
                return True

            def list_catalogs(self) -> list[str]:
                return ["amx_test", "samples", "system", "workspace"]

            def list_schemas(self) -> list[str]:
                self.observed_catalog = self.cfg.catalog
                return ["amx_test_schema"]

            def list_tables(self, schema: str) -> list[str]:
                self.observed_catalog = self.cfg.catalog
                return ["customers", "orders"]

        fake = FakeDB()
        toolbox = self._toolbox(fake)
        payload = toolbox._tool_list_tables_in_schema(schema="amx_test_schema")
        self.assertTrue(payload["found"])
        self.assertEqual(payload["catalog"], "amx_test")
        self.assertEqual(payload["auto_picked_catalog"], "amx_test")
        self.assertEqual([t["name"] for t in payload["tables"]], ["customers", "orders"])
        # Connector saw the temporary pin while resolving + listing.
        self.assertEqual(fake.observed_catalog, "amx_test")
        # And cfg was restored after the call.
        self.assertEqual(fake.cfg.catalog, "")

    def test_describe_table_auto_picks_catalog_when_unpinned(self) -> None:
        """describe_table also has to auto-pick — otherwise it emits
        DESCRIBE None.<schema>.<table> on Databricks UC and returns
        a confusing 'found=false, catalog 'none' was not found' shape
        that the LLM then narrates back at the user."""

        class FakeProfile:
            existing_comment = ""
            row_count = 42
            columns: list = []
            analytics = type("A", (), {})()

        class FakeDB:
            def __init__(self) -> None:
                self.cfg = type("DBCfg", (), {"catalog": ""})()
                self.observed_catalog: str = ""

            def supports_catalogs(self) -> bool:
                return True

            def list_catalogs(self) -> list[str]:
                return ["amx_test", "samples", "system", "workspace"]

            def profile_table(self, schema: str, table: str, **_kw):
                self.observed_catalog = self.cfg.catalog
                return FakeProfile()

        fake = FakeDB()
        toolbox = self._toolbox(fake)
        payload = toolbox._tool_describe_table(schema="amx_test_schema", table="customers")
        self.assertTrue(payload["found"])
        self.assertEqual(payload["catalog"], "amx_test")
        self.assertEqual(fake.observed_catalog, "amx_test")
        self.assertEqual(fake.cfg.catalog, "")

    def test_list_schemas_with_catalog_argument_scopes_listing(self) -> None:
        """Passing ``catalog=X`` must temporarily pin cfg.catalog so the
        connector emits ``SHOW SCHEMAS IN X`` instead of failing on the
        SQLAlchemy default. The pin must be restored afterwards."""

        class FakeDB:
            def __init__(self) -> None:
                self.cfg = type("DBCfg", (), {"catalog": ""})()
                self.observed_catalog: str = ""

            def supports_catalogs(self) -> bool:
                return True

            def list_schemas(self) -> list[str]:
                self.observed_catalog = self.cfg.catalog
                return ["sales", "ops"]

        fake = FakeDB()
        toolbox = self._toolbox(fake)
        payload = toolbox._tool_list_schemas(catalog="main")
        self.assertEqual(payload["schemas"], ["sales", "ops"])
        self.assertEqual(payload["database"], "main")
        # Connector saw the temporary pin while listing.
        self.assertEqual(fake.observed_catalog, "main")
        # And the pin was restored afterwards (no leak into cfg).
        self.assertEqual(fake.cfg.catalog, "")

    def test_list_catalogs_tool_returns_show_catalogs_result(self) -> None:
        class FakeDB:
            def supports_catalogs(self) -> bool:
                return True

            def list_catalogs(self) -> list[str]:
                return ["main", "samples"]

            cfg = type("DBCfg", (), {"catalog": "main"})()

        toolbox = self._toolbox(FakeDB(), pinned_catalog="main")
        payload = toolbox._tool_list_catalogs()
        self.assertTrue(payload["supports_catalogs"])
        self.assertEqual(payload["catalogs"], ["main", "samples"])
        self.assertEqual(payload["active_catalog"], "main")

    def test_list_catalogs_tool_auto_resolves_single_user_catalog(self) -> None:
        """User report 2026-05-04 (second loop): kimi-thinking still
        looped because the system prompt's no-catalog hint pushed it
        toward list_catalogs first; then it narrated the 4-entry list
        instead of calling list_schemas. list_catalogs now eagerly
        attaches the schemas of the single user catalog when the
        workspace exposes one — the next agent iteration has no
        decision to make and no list to enumerate at the user."""

        class FakeDB:
            def __init__(self) -> None:
                self.cfg = type("DBCfg", (), {"catalog": ""})()

            def supports_catalogs(self) -> bool:
                return True

            def list_catalogs(self) -> list[str]:
                return ["amx_test", "samples", "system", "workspace"]

            def list_schemas(self) -> list[str]:
                return ["sales", "ops"]

        toolbox = self._toolbox(FakeDB())
        payload = toolbox._tool_list_catalogs()
        self.assertEqual(payload["catalogs"], ["amx_test", "samples", "system", "workspace"])
        self.assertEqual(payload["user_catalogs"], ["amx_test"])
        self.assertEqual(payload["auto_picked_catalog"], "amx_test")
        self.assertEqual(payload["schemas_in_auto_picked_catalog"], ["sales", "ops"])
        # Instruction nudges the LLM to answer directly instead of
        # narrating the catalog list.
        self.assertIn("schemas_in_auto_picked_catalog", payload["instruction"])
        self.assertIn("not enumerate", payload["instruction"].lower())

    def test_list_catalogs_tool_signals_2_level_backend(self) -> None:
        class FakeDB:
            def supports_catalogs(self) -> bool:
                return False

        toolbox = self._toolbox(FakeDB())
        payload = toolbox._tool_list_catalogs()
        self.assertFalse(payload["supports_catalogs"])
        self.assertEqual(payload["catalogs"], [])
        self.assertIn("list_server_databases", payload["message"])

    def test_list_server_databases_returns_databases_with_active_marker(self) -> None:
        class FakeDB:
            def list_databases(self) -> list[str]:
                return ["app", "analytics"]

            cfg = type("DBCfg", (), {"database": "app"})()

        toolbox = self._toolbox(FakeDB(), pinned_database="app")
        payload = toolbox._tool_list_server_databases()
        self.assertEqual(payload["databases"], ["app", "analytics"])
        self.assertEqual(payload["active_database"], "app")

    def test_new_discovery_tools_appear_in_tool_schemas(self) -> None:
        """The new tools must show up in ``ToolBox.schemas()`` so LiteLLM
        forwards them to the LLM. Otherwise the system prompt's
        instructions to call them produce a hallucinated function-call
        name the agent loop can't dispatch."""
        from amx.search.agent_tools import ToolBox

        names = {entry["function"]["name"] for entry in ToolBox.schemas()}
        self.assertIn("list_catalogs", names)
        self.assertIn("list_server_databases", names)
        self.assertIn("list_volumes", names)
        # list_schemas / list_tables_in_schema both accept the optional
        # catalog argument now.
        for entry in ToolBox.schemas():
            fn = entry["function"]
            if fn["name"] in {"list_schemas", "list_tables_in_schema"}:
                self.assertIn(
                    "catalog",
                    fn["parameters"]["properties"],
                    f"{fn['name']} should advertise an optional `catalog` argument so "
                    "the LLM can drill into a Unity-Catalog catalog without mutating "
                    "the saved profile.",
                )

    def test_list_volumes_iterates_schemas_when_schema_omitted(self) -> None:
        """User report 2026-05-04: /ask answered 'I can't see volumes' on
        Databricks because no tool exposed SHOW VOLUMES. The new
        list_volumes tool runs SHOW VOLUMES across every schema in the
        auto-picked catalog when the LLM doesn't name one, surfacing
        managed and external volumes for the LLM to summarise."""

        class FakeCaps:
            volumes = True

        class FakeDB:
            def __init__(self) -> None:
                self.cfg = type("DBCfg", (), {"catalog": ""})()
                self.observed_catalog: str = ""
                self.capabilities = FakeCaps()

            def supports_catalogs(self) -> bool:
                return True

            def list_catalogs(self) -> list[str]:
                # Single user catalog — auto-picked.
                return ["amx_test", "samples", "system", "workspace"]

            def list_schemas(self) -> list[str]:
                self.observed_catalog = self.cfg.catalog
                return ["sales", "raw"]

            def list_volumes(self, schema: str, catalog: str):
                if schema == "sales":
                    return [
                        {"name": "raw_files", "type": "managed", "comment": "ETL inbox"},
                    ]
                if schema == "raw":
                    return [
                        {"name": "uploads", "type": "external", "comment": ""},
                    ]
                return []

        toolbox = self._toolbox(FakeDB())
        payload = toolbox._tool_list_volumes()
        self.assertTrue(payload["supported"])
        self.assertEqual(payload["catalog"], "amx_test")
        self.assertEqual(payload["auto_picked_catalog"], "amx_test")
        self.assertEqual(payload["count"], 2)
        names = {row["name"] for row in payload["volumes"]}
        self.assertEqual(names, {"raw_files", "uploads"})
        kinds = {row["kind"] for row in payload["volumes"]}
        self.assertEqual(kinds, {"managed", "external"})

    def test_list_volumes_returns_unsupported_for_non_databricks_backend(self) -> None:
        """For PG/Snowflake/etc. there is no Volume concept — the tool
        must surface ``supported=false`` so the LLM doesn't invent a
        SHOW VOLUMES query against an unsupported backend."""

        class FakeCaps:
            volumes = False

        class FakeDB:
            capabilities = FakeCaps()

        toolbox = self._toolbox(FakeDB())
        payload = toolbox._tool_list_volumes()
        self.assertFalse(payload["supported"])
        self.assertEqual(payload["volumes"], [])
        self.assertIn("Databricks", payload["message"])


def test_band_prefix_renders_glyph_for_high_band():
    assert "[H]" in _band_prefix({"band": "HIGH"})


def test_band_prefix_renders_glyph_for_med_band():
    assert "[M]" in _band_prefix({"band": "MED"})


def test_band_prefix_renders_glyph_for_low_band():
    assert "[L]" in _band_prefix({"band": "LOW"})


def test_band_prefix_empty_for_legacy_string_entry():
    assert _band_prefix("plain string") == ""


def test_band_prefix_empty_for_dict_without_band():
    assert _band_prefix({"text": "no band here"}) == ""


def test_band_prefix_empty_for_unknown_band():
    assert _band_prefix({"band": "WAT"}) == ""


if __name__ == "__main__":
    unittest.main()
