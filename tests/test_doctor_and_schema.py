"""Tests for `amx doctor` + config schema versioning.

Together they kill the version-skew bug class that hit on
2026-05-01: two ``amx`` binaries on PATH writing the same
``~/.amx/config.yml`` made profiles silently disappear when the older
binary couldn't parse keys the newer one had written.

Schema versioning makes the second binary refuse the read instead of
mangling the file. ``amx doctor`` lists every binary on PATH so the
user can spot the situation before debugging anything else.
"""

from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from click.testing import CliRunner

from amx.cli import main
from amx.cli_support.commands.doctor import (
    CheckResult,
    _check_amx_on_path,
    _check_amx_version,
    _check_optional_deps,
    _check_python_version,
    _render_results,
    run_doctor,
)
from amx.config import (
    CONFIG_SCHEMA_VERSION,
    AMXConfig,
    ConfigSchemaTooNewError,
)


class ConfigSchemaVersioningTests(unittest.TestCase):
    def test_save_stamps_schema_version(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cfg = AMXConfig()
            cfg.CONFIG_DIR = tmp  # type: ignore[misc]
            path = Path(tmp) / "config.yml"
            cfg.save(str(path))
            text = path.read_text()
            self.assertIn(f"schema_version: {CONFIG_SCHEMA_VERSION}", text)

    def test_load_accepts_current_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cfg = AMXConfig()
            cfg.CONFIG_DIR = tmp  # type: ignore[misc]
            cfg.active_db_profile = "pg-prod"
            path = Path(tmp) / "config.yml"
            cfg.save(str(path))
            # Round-trip read
            reloaded = AMXConfig.load(str(path))
            self.assertEqual(reloaded.active_db_profile, "pg-prod")

    def test_load_accepts_legacy_unversioned_config(self) -> None:
        """Configs from before schema versioning was introduced have no
        ``schema_version`` key. Doctor must treat that as v0 — older —
        and load happily so existing users aren't kicked out on upgrade.
        """
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.yml"
            path.write_text(
                # Minimal pre-versioning YAML shape.
                "db:\n  backend: postgres\n  database: prod\n"
                "active_db_profile: legacy\n"
                "db_profiles:\n  legacy:\n    backend: postgres\n"
            )
            cfg = AMXConfig.load(str(path))
            self.assertEqual(cfg.active_db_profile, "legacy")

    def test_load_refuses_future_schema(self) -> None:
        """A config written by a NEWER AMX must be refused with a clear
        error rather than read silently (which would strip unknown keys
        on the next save — the ghost-profile incident)."""
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.yml"
            future = CONFIG_SCHEMA_VERSION + 5
            path.write_text(
                f"schema_version: {future}\ndb:\n  backend: postgres\nactive_db_profile: default\n"
            )
            with self.assertRaises(ConfigSchemaTooNewError) as cm:
                AMXConfig.load(str(path))
            self.assertEqual(cm.exception.file_version, future)
            self.assertEqual(cm.exception.supported_version, CONFIG_SCHEMA_VERSION)

    def test_cli_renders_actionable_message_on_future_schema(self) -> None:
        """The CLI top-level catches ``ConfigSchemaTooNewError`` and
        renders an actionable message instead of a stack trace.
        """
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.yml"
            future = CONFIG_SCHEMA_VERSION + 5
            path.write_text(f"schema_version: {future}\ndb:\n  backend: postgres\n")
            runner = CliRunner()
            result = runner.invoke(
                main,
                ["--config", str(path), "doctor", "--skip-network"],
                catch_exceptions=False,
            )
            self.assertNotEqual(result.exit_code, 0)
            self.assertIn(f"schema_version={future}", result.output)
            self.assertIn("Upgrade AMX", result.output)


class DoctorPathConflictTests(unittest.TestCase):
    """The hero check: detect multiple `amx` binaries on PATH."""

    def test_single_amx_passes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            amx_path = Path(tmp) / "amx"
            amx_path.write_text("#!/bin/sh\necho amx\n")
            amx_path.chmod(0o755)
            with patch.dict(os.environ, {"PATH": tmp}, clear=True):
                result = _check_amx_on_path()
            self.assertTrue(result.ok)
            self.assertIn("amx", result.detail)

    def test_multiple_amx_fails_with_helpful_hint(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_a, tempfile.TemporaryDirectory() as tmp_b:
            for d in (tmp_a, tmp_b):
                p = Path(d) / "amx"
                p.write_text("#!/bin/sh\necho amx\n")
                p.chmod(0o755)
            with patch.dict(os.environ, {"PATH": f"{tmp_a}{os.pathsep}{tmp_b}"}, clear=True):
                result = _check_amx_on_path()
            self.assertFalse(result.ok)
            self.assertIn("Multiple `amx` installs detected", result.hint)
            self.assertIn(tmp_a, result.detail)
            self.assertIn(tmp_b, result.detail)

    def test_no_amx_on_path_returns_not_ok(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict(os.environ, {"PATH": tmp}, clear=True):
                result = _check_amx_on_path()
            self.assertFalse(result.ok)
            self.assertIn("Reinstall", result.hint)
            # The hint must reference the live PyPI distribution name,
            # not the legacy ``amx`` import name.
            self.assertIn("amx-cli", result.hint)

    def test_falls_back_to_shutil_which_when_walk_misses(self) -> None:
        """``shutil.which`` is the canonical PATH resolver; when the
        manual walk misses a hit (e.g. on Windows where binaries are
        named ``amx.exe``) the result must still report success
        instead of the false "(not found)" the pre-0.12.2 doctor
        printed on a healthy Windows install."""
        with tempfile.TemporaryDirectory() as tmp:
            fake_amx = str(Path(tmp) / "amx.exe")
            with (
                patch.dict(os.environ, {"PATH": tmp}, clear=True),
                patch("amx.cli_support.commands.doctor.shutil.which", return_value=fake_amx),
            ):
                result = _check_amx_on_path()
            self.assertTrue(result.ok, msg=result.detail)
            self.assertIn("amx.exe", result.detail)


class DoctorOptionalDepsActiveBackendGateTests(unittest.TestCase):
    """The active DB profile's driver must be reported as REQUIRED,
    not as a green ``optional`` line, so a fresh ``pip install
    amx-cli`` user who set up Databricks doesn't see ``✓ Databricks
    driver — not installed (optional)`` while the actual workflow
    crashes the moment they try to use it."""

    def test_databricks_driver_marked_required_when_active(self) -> None:
        results = _check_optional_deps(active_backend="databricks")
        databricks_check = next(r for r in results if r.name == "Databricks driver")
        try:
            import databricks.sql  # noqa: F401
        except ImportError:
            self.assertFalse(databricks_check.ok)
            self.assertIn("required for active 'databricks' profile", databricks_check.detail)
            self.assertIn("amx-cli[databricks]", databricks_check.hint)
        else:
            # Driver happens to be installed in the test environment —
            # the line still gets the "required" tag instead of the
            # plain "installed" wording.
            self.assertTrue(databricks_check.ok)
            self.assertIn("required", databricks_check.detail)

    def test_other_backends_stay_optional(self) -> None:
        results = _check_optional_deps(active_backend="postgresql")
        # Postgres isn't in the optional-deps probe list (driver is
        # plain ``psycopg2``), so the only thing we care about is that
        # Databricks/Snowflake/BigQuery DON'T get demoted to required.
        for r in results:
            if r.name in {"Snowflake driver", "BigQuery driver", "Databricks driver"}:
                self.assertNotIn("required for active", r.detail)


class DoctorVersionAndPythonChecksTests(unittest.TestCase):
    def test_amx_version_check_reports_schema_version(self) -> None:
        result = _check_amx_version()
        self.assertTrue(result.ok)
        self.assertIn(f"schema v{CONFIG_SCHEMA_VERSION}", result.detail)

    def test_python_version_check_passes_on_modern_python(self) -> None:
        result = _check_python_version()
        # Test only runs on Python 3.10+ anyway; just verify shape.
        self.assertTrue(result.ok)
        self.assertIn(".", result.detail)


class DoctorRendererTests(unittest.TestCase):
    def test_renderer_returns_zero_on_all_pass(self) -> None:
        results = [
            CheckResult(name="X", ok=True, detail="d"),
            CheckResult(name="Y", ok=True, detail="d"),
        ]
        self.assertEqual(_render_results(results), 0)

    def test_renderer_returns_one_on_any_failure(self) -> None:
        results = [
            CheckResult(name="X", ok=True, detail="d"),
            CheckResult(name="Y", ok=False, detail="bad", hint="fix it"),
        ]
        self.assertEqual(_render_results(results), 1)


class DoctorEndToEndTests(unittest.TestCase):
    def test_run_doctor_skip_network_completes_on_fresh_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cfg = AMXConfig()
            cfg.CONFIG_DIR = tmp  # type: ignore[misc]
            # No DB / LLM configured → those checks short-circuit. With
            # --skip-network the run should always be 0 regardless of
            # network reachability.
            exit_code = run_doctor(cfg, skip_network=True)
            self.assertEqual(exit_code, 0)

    def test_doctor_command_renders_report(self) -> None:
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.yml"
            # Pre-create a minimal valid config so load() succeeds.
            path.write_text("db:\n  backend: postgres\nactive_db_profile: ''\n")
            result = runner.invoke(
                main,
                ["--config", str(path), "doctor", "--skip-network"],
                catch_exceptions=False,
            )
            self.assertEqual(result.exit_code, 0, msg=result.output)
            self.assertIn("AMX doctor report", result.output)
            self.assertIn("AMX version", result.output)
            self.assertIn("Python runtime", result.output)
            self.assertIn("amx on PATH", result.output)


class DoctorCrossNamespaceDispatchTests(unittest.TestCase):
    """`/doctor` must work from every namespace tab, not just /search.

    Regression guard: registering ``cross_namespace=True`` in the slash
    registry was not enough — without an entry in
    ``session.shortcut_map``, ``/doctor`` from /search fell through to
    ``["search", "ask", "doctor"]`` (sent the literal "doctor" string to
    the search agent as a question), and from any other namespace fell
    through to ``[namespace, "doctor"]`` which Click rejected.
    """

    def test_dispatch_from_root_namespace(self) -> None:
        from amx.cli_support.session import session_to_click_args

        self.assertEqual(session_to_click_args("", ["doctor"]), ["doctor"])

    def test_dispatch_from_search_namespace(self) -> None:
        from amx.cli_support.session import session_to_click_args

        # The bug specifically masqueraded as "works under search"
        # because /search swallows unknown verbs as questions.
        self.assertEqual(session_to_click_args("search", ["doctor"]), ["doctor"])

    def test_dispatch_from_other_namespaces(self) -> None:
        from amx.cli_support.session import session_to_click_args

        for ns in ("db", "metadata", "docs", "llm", "code", "analyze", "history"):
            with self.subTest(namespace=ns):
                self.assertEqual(
                    session_to_click_args(ns, ["doctor"]),
                    ["doctor"],
                    f"/doctor must dispatch to top-level click "
                    f"`doctor` from /{ns}, not get swallowed.",
                )

    def test_skip_network_flag_passed_through(self) -> None:
        from amx.cli_support.session import session_to_click_args

        self.assertEqual(
            session_to_click_args("llm", ["doctor", "--skip-network"]),
            ["doctor", "--skip-network"],
        )


if __name__ == "__main__":
    unittest.main()
