"""``/add-db-profile`` for MSSQL prints the platform-specific ODBC hint.

pyodbc installs cleanly with ``pip install amx-cli[mssql]`` but the
SQL Server ODBC driver is a separate system package (``msodbcsql18``).
Without it the wizard's connection test fails with ``Can't open lib
'ODBC Driver 18 for SQL Server'`` — the support-friction fix is to
print the right install command for the user's OS before they hit the
test.
"""

from __future__ import annotations

import unittest
from unittest.mock import patch

from amx.cli_support.commands.db import _print_system_prereq_hint


class MssqlSystemPrereqHintTests(unittest.TestCase):
    def test_macos_prints_brew_hint(self) -> None:
        infos: list[str] = []
        with (
            patch("platform.system", return_value="Darwin"),
            patch(
                "amx.cli_support._db_diagnostics.info",
                side_effect=lambda msg, *a, **kw: infos.append(str(msg)),
            ),
        ):
            _print_system_prereq_hint("mssql")
        self.assertTrue(any("brew" in m and "msodbcsql18" in m for m in infos))

    def test_linux_prints_distro_pointer(self) -> None:
        infos: list[str] = []
        with (
            patch("platform.system", return_value="Linux"),
            patch(
                "amx.cli_support._db_diagnostics.info",
                side_effect=lambda msg, *a, **kw: infos.append(str(msg)),
            ),
        ):
            _print_system_prereq_hint("mssql")
        self.assertTrue(any("msodbcsql18" in m for m in infos))

    def test_other_backends_print_nothing(self) -> None:
        infos: list[str] = []
        with patch(
            "amx.cli_support._db_diagnostics.info",
            side_effect=lambda msg, *a, **kw: infos.append(str(msg)),
        ):
            _print_system_prereq_hint("postgresql")
            _print_system_prereq_hint("duckdb")
        self.assertEqual(infos, [])


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
