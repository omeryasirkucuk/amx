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


if __name__ == "__main__":
    unittest.main()
