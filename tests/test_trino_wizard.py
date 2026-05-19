"""Trino profile wizard end-to-end mock test.

Mirrors the structure of ``test_databricks_wizard_catalog_required.py`` —
patches the four prompt primitives (ask, ask_password, ask_choice,
warn) so the wizard runs without a terminal, then asserts the saved
``DBConfig`` reflects the picker choices. Covers the two main auth
modes (Basic and JWT) and confirms the TLS-verify branch only
fires for ``https``.
"""

from __future__ import annotations

import unittest
from unittest.mock import patch

from amx.cli_support.commands.db import interactive_db_block
from amx.config import DBConfig


class TrinoWizardTests(unittest.TestCase):
    def test_basic_auth_https_with_catalog(self) -> None:
        defaults = DBConfig(backend="trino")

        ask_values = iter(
            [
                "trino.example.com",  # host
                "443",  # port
                "alice",  # username
                "hive",  # catalog
                "default",  # schema
                "",  # tls CA bundle path (blank)
            ]
        )
        choice_values = iter(
            [
                "trino",  # backend picker
                "https",  # http_scheme
                "basic",  # auth mode
                "yes",  # verify TLS? -> _ask_update_bool returns True
            ]
        )

        with (
            patch(
                "amx.cli_support.commands.db._offer_to_install_backend_driver",
                return_value=None,
            ),
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
                side_effect=lambda *a, **kw: "secret-password",
            ),
            patch(
                "amx.cli_support.commands.db.warn",
                side_effect=lambda *a, **kw: None,
            ),
        ):
            updated = interactive_db_block(defaults)

        self.assertEqual(updated.backend, "trino")
        self.assertEqual(updated.host, "trino.example.com")
        self.assertEqual(updated.port, 443)
        self.assertEqual(updated.user, "alice")
        self.assertEqual(updated.password, "secret-password")
        self.assertEqual(updated.jwt_token, "")
        self.assertEqual(updated.catalog, "hive")
        self.assertEqual(updated.database, "default")
        self.assertEqual(updated.http_scheme, "https")
        self.assertTrue(updated.verify)

    def test_jwt_auth_collects_token_and_skips_password(self) -> None:
        defaults = DBConfig(backend="trino")

        ask_values = iter(
            [
                "galaxy.starburst.io",  # host
                "443",  # port
                "alice",  # username
                "",  # catalog (skipped at command time)
                "",  # schema (skipped at command time)
                "",  # tls CA bundle path
            ]
        )
        choice_values = iter(
            [
                "trino",
                "https",
                "jwt",
                "yes",  # verify TLS
            ]
        )

        with (
            patch(
                "amx.cli_support.commands.db._offer_to_install_backend_driver",
                return_value=None,
            ),
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
                side_effect=lambda *a, **kw: "ey.jwt.token",
            ),
            patch(
                "amx.cli_support.commands.db.warn",
                side_effect=lambda *a, **kw: None,
            ),
        ):
            updated = interactive_db_block(defaults)

        self.assertEqual(updated.jwt_token, "ey.jwt.token")
        # JWT path leaves password blank — they are mutually exclusive
        # at wizard time even though ``DBConfig`` stores both.
        self.assertEqual(updated.password, "")
        self.assertEqual(updated.catalog, "")
        self.assertEqual(updated.database, "")

    def test_http_skips_tls_prompts(self) -> None:
        """Local dev clusters over http must not prompt for TLS verification."""
        defaults = DBConfig(backend="trino")

        ask_values = iter(
            [
                "localhost",
                "8080",
                "amx",
                "memory",  # catalog
                "",  # schema
                # NO TLS path entry — http branch skips both verify
                # and CA bundle path prompts.
            ]
        )
        choice_values = iter(
            [
                "trino",
                "http",
                "basic",
                # NO verify prompt — http branch skips it.
            ]
        )

        with (
            patch(
                "amx.cli_support.commands.db._offer_to_install_backend_driver",
                return_value=None,
            ),
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
                side_effect=lambda *a, **kw: "",
            ),
            patch(
                "amx.cli_support.commands.db.warn",
                side_effect=lambda *a, **kw: None,
            ),
        ):
            updated = interactive_db_block(defaults)

        self.assertEqual(updated.http_scheme, "http")
        self.assertEqual(updated.port, 8080)
        self.assertEqual(updated.catalog, "memory")


class HiveWizardTests(unittest.TestCase):
    def test_plain_auth_collects_password(self) -> None:
        defaults = DBConfig(backend="hive")

        ask_values = iter(
            [
                "hive.example.com",  # host
                "10000",  # port
                "alice",  # user
                "warehouse",  # database
            ]
        )
        choice_values = iter(
            [
                "hive",  # backend
                "PLAIN",  # auth mode
            ]
        )

        info_messages: list[str] = []
        with (
            patch(
                "amx.cli_support.commands.db._offer_to_install_backend_driver",
                return_value=None,
            ),
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
                side_effect=lambda *a, **kw: "hive-secret",
            ),
            patch(
                "amx.cli_support.commands.db.info",
                side_effect=lambda msg, *a, **kw: info_messages.append(str(msg)),
            ),
            patch(
                "amx.cli_support.commands.db.warn",
                side_effect=lambda *a, **kw: None,
            ),
        ):
            updated = interactive_db_block(defaults)

        self.assertEqual(updated.backend, "hive")
        self.assertEqual(updated.host, "hive.example.com")
        self.assertEqual(updated.port, 10000)
        self.assertEqual(updated.user, "alice")
        self.assertEqual(updated.password, "hive-secret")
        self.assertEqual(updated.auth_mode, "PLAIN")
        self.assertEqual(updated.database, "warehouse")
        # Wizard surfaces the deployment-disambiguation hint so users
        # configuring a Databricks legacy hive_metastore catalog don't
        # accidentally configure the wrong backend.
        self.assertTrue(
            any("Databricks" in m and "hive_metastore" in m for m in info_messages),
            f"Expected a Databricks/hive_metastore disambiguation hint, got {info_messages!r}",
        )

    def test_nosasl_skips_password_prompt(self) -> None:
        defaults = DBConfig(backend="hive")

        ask_values = iter(
            [
                "localhost",
                "10000",
                "amx",
                "",  # database (skipped)
            ]
        )
        choice_values = iter(
            [
                "hive",
                "NOSASL",
            ]
        )

        with (
            patch(
                "amx.cli_support.commands.db._offer_to_install_backend_driver",
                return_value=None,
            ),
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
                side_effect=lambda *a, **kw: "",
            ),
            patch(
                "amx.cli_support.commands.db.info",
                side_effect=lambda *a, **kw: None,
            ),
            patch(
                "amx.cli_support.commands.db.warn",
                side_effect=lambda *a, **kw: None,
            ),
        ):
            updated = interactive_db_block(defaults)

        self.assertEqual(updated.auth_mode, "NOSASL")
        self.assertEqual(updated.password, "")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
