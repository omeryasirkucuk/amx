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

import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from click.testing import CliRunner

from amx.cli import main
from amx.cli_support.commands.compare import (
    _aggregate_for_run,
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
            # 11 metric rows × 2 runs = 22 entries.
            self.assertEqual(len(payload["aggregate_metrics"]), 11 * 2)
            metrics_seen = {r["metric"] for r in payload["aggregate_metrics"]}
            self.assertIn("avg_logprob_score", metrics_seen)
            self.assertIn("approval_rate", metrics_seen)
            self.assertIn("total_tokens", metrics_seen)


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


if __name__ == "__main__":
    unittest.main()
