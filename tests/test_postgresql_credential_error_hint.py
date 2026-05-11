"""PostgreSQL adapter surfaces a friendly hint on credential failure.

Without this branch, ``OperationalError: FATAL: password authentication
failed for user "amx"`` bubbled raw through the connector, prompting
support tickets that looked like ``/db test`` was broken when really the
profile just had a wrong password.
"""

from __future__ import annotations

import unittest

from amx.config import DBConfig
from amx.db.adapters.postgresql import PostgreSQLAdapter


class PostgreSQLCredentialErrorHintTests(unittest.TestCase):
    def setUp(self) -> None:
        self.adapter = PostgreSQLAdapter(DBConfig(backend="postgresql"))

    def test_password_auth_failed_returns_hint(self) -> None:
        exc = Exception('FATAL:  password authentication failed for user "amx"')
        hint = self.adapter.actionable_profile_error(exc)
        self.assertIsNotNone(hint)
        assert hint is not None  # for type-checkers
        self.assertIn("credentials", hint.lower())
        self.assertIn("/edit", hint)

    def test_sqlstate_28p01_returns_hint(self) -> None:
        exc = Exception("connection failed: SQLSTATE 28P01")
        self.assertIsNotNone(self.adapter.actionable_profile_error(exc))

    def test_no_password_supplied_returns_hint(self) -> None:
        exc = Exception("fe_sendauth: no password supplied")
        self.assertIsNotNone(self.adapter.actionable_profile_error(exc))

    def test_unrelated_error_returns_none(self) -> None:
        exc = Exception('relation "orders" already exists')
        self.assertIsNone(self.adapter.actionable_profile_error(exc))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
