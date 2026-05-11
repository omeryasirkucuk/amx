"""The ``/profiling`` CLI command persists the new ``approximate`` flag.

PR 3 extends the four-arg form to ``/profiling <mode> [max_rows]
[sample_size] [approximate]`` so users can flip the metered-backend
billing toggle without editing YAML.
"""

from __future__ import annotations

import unittest
from unittest.mock import patch

from amx.cli_support.commands.db import cmd_profiling
from amx.config import AMXConfig, DBConfig


class ProfilingCommandApproximateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.cfg = AMXConfig()
        self.cfg.db = DBConfig(backend="bigquery", project="p")
        self.cfg.active_db_profile = "default"
        self.cfg.db_profiles = {"default": self.cfg.db}

    def test_four_args_set_approximate(self) -> None:
        with patch.object(self.cfg, "save"):
            cmd_profiling(self.cfg, ["sampled", "5000000", "10", "on"])
        self.assertTrue(self.cfg.db.profiling_approximate)
        self.assertEqual(self.cfg.db.profiling_mode, "sampled")
        self.assertEqual(self.cfg.db.profiling_max_rows, 5_000_000)
        self.assertEqual(self.cfg.db.profiling_sample_size, 10)

    def test_three_args_leaves_approximate_alone(self) -> None:
        self.cfg.db.profiling_approximate = True
        with patch.object(self.cfg, "save"):
            cmd_profiling(self.cfg, ["full", "1000000", "5"])
        self.assertTrue(self.cfg.db.profiling_approximate)

    def test_off_keyword_disables_approximate(self) -> None:
        self.cfg.db.profiling_approximate = True
        with patch.object(self.cfg, "save"):
            cmd_profiling(self.cfg, ["full", "1000000", "5", "off"])
        self.assertFalse(self.cfg.db.profiling_approximate)

    def test_bad_approximate_arg_errors(self) -> None:
        errors: list[str] = []
        with (
            patch.object(self.cfg, "save"),
            patch(
                "amx.cli_support.commands.db.error",
                side_effect=lambda msg, *a, **kw: errors.append(str(msg)),
            ),
        ):
            cmd_profiling(self.cfg, ["full", "1000000", "5", "maybe"])
        self.assertTrue(
            any("approximate" in e.lower() for e in errors),
            f"Expected an approximate-flag error, got {errors!r}",
        )


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
