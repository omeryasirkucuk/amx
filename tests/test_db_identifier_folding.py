"""Tests for ``DatabaseAdapter.normalize_identifier``.

Background: Oracle and Snowflake fold *unquoted* identifiers to UPPER and
then store them that way. A user typing ``scott.employees`` into the
wizard or ``/db sample`` used to silently receive an empty result on
those backends because the SQLAlchemy inspector and adapter metadata
queries were called with the raw lowercase value. The connector now
normalizes user-supplied schema/table names via the adapter hook before
any inspector call.
"""

from __future__ import annotations

import unittest

from amx.config import DBConfig
from amx.db.adapters.base import DatabaseAdapter
from amx.db.adapters.oracle import OracleAdapter
from amx.db.adapters.postgresql import PostgreSQLAdapter
from amx.db.adapters.snowflake import SnowflakeAdapter


def _make(adapter_cls: type[DatabaseAdapter], **cfg_kwargs: object) -> DatabaseAdapter:
    cfg = DBConfig(backend=adapter_cls.name, **cfg_kwargs)  # type: ignore[arg-type]
    return adapter_cls(cfg)


class IdentifierFoldingTests(unittest.TestCase):
    def test_oracle_folds_unquoted_to_upper(self) -> None:
        adapter = _make(OracleAdapter)
        self.assertEqual(adapter.normalize_identifier("scott"), "SCOTT")
        self.assertEqual(adapter.normalize_identifier("employees"), "EMPLOYEES")
        self.assertEqual(adapter.normalize_identifier("MixedCase"), "MIXEDCASE")

    def test_oracle_preserves_quoted_identifier(self) -> None:
        adapter = _make(OracleAdapter)
        self.assertEqual(adapter.normalize_identifier('"scott"'), '"scott"')
        self.assertEqual(adapter.normalize_identifier('"MixedCase"'), '"MixedCase"')

    def test_oracle_passes_through_empty(self) -> None:
        adapter = _make(OracleAdapter)
        self.assertEqual(adapter.normalize_identifier(""), "")

    def test_snowflake_folds_unquoted_to_upper(self) -> None:
        adapter = _make(SnowflakeAdapter)
        self.assertEqual(adapter.normalize_identifier("analytics"), "ANALYTICS")
        self.assertEqual(adapter.normalize_identifier('"analytics"'), '"analytics"')

    def test_postgres_passes_value_through(self) -> None:
        adapter = _make(PostgreSQLAdapter)
        self.assertEqual(adapter.normalize_identifier("public"), "public")
        self.assertEqual(adapter.normalize_identifier("MixedCase"), "MixedCase")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
