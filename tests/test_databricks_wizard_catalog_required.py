"""Databricks Unity Catalog must be set when the wizard saves a new profile.

The adapter's catalog-less ``SHOW SCHEMAS`` path falls back to
SQLAlchemy's inspector, which on Unity Catalog returns empty results —
the user filed a "fresh profile, listing returns nothing" report. The
wizard now requires the catalog field; legacy hive_metastore-only
workspaces must type ``hive_metastore`` explicitly so the choice is
visible in the saved profile.
"""

from __future__ import annotations

import unittest
from unittest.mock import patch

from amx.cli_support.commands.db import interactive_db_block
from amx.config import DBConfig


class DatabricksWizardCatalogRequiredTests(unittest.TestCase):
    def test_blank_catalog_is_reprompted_until_filled(self) -> None:
        defaults = DBConfig(
            backend="databricks",
            host="",
            http_path="",
            access_token="",
            catalog="",
            database="",
            tls_no_verify=False,
            tls_trusted_ca_file="",
        )

        # Probe-gate answers "no", so the wizard falls through to the
        # free-form ``Unity Catalog (required; ...)`` prompt. We answer
        # blank once (the wizard must reject and re-prompt), then with
        # ``hive_metastore`` to escape.
        ask_values = iter(
            [
                "host.example",  # Workspace host
                "/sql/x",  # SQL warehouse HTTP path
                "",  # Trusted CA bundle path (empty is fine)
                "",  # Unity Catalog — first attempt: blank
                "hive_metastore",  # Unity Catalog — second attempt: legacy literal
                "",  # Schema / database optional, keep blank
            ]
        )
        choice_values = iter(
            [
                "databricks",  # Select backend
                "no",  # Disable TLS verify? no
                "no",  # Probe catalogs? no
            ]
        )

        warns: list[str] = []

        with (
            patch(
                "amx.cli_support.commands.db.ask_choice",
                side_effect=lambda *a, **kw: next(choice_values),
            ),
            patch(
                "amx.cli_support.commands.db.ask",
                side_effect=lambda *a, **kw: next(ask_values),
            ),
            patch(
                "amx.cli_support.commands.db.ask_password",
                side_effect=lambda *a, **kw: "tok",
            ),
            patch(
                "amx.cli_support.commands.db.warn",
                side_effect=lambda msg, *a, **kw: warns.append(str(msg)),
            ),
        ):
            updated = interactive_db_block(defaults)

        self.assertEqual(updated.catalog, "hive_metastore")
        self.assertTrue(
            any("Unity Catalog" in w and "required" in w for w in warns),
            f"Expected a 'Unity Catalog is required' warning, got {warns!r}",
        )


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
