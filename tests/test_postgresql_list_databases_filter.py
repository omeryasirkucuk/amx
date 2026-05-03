"""PostgreSQLAdapter.list_databases hides the maintenance ``postgres`` DB.

The /history-store enable picker passes the result of
``adapter.list_databases(engine)`` to a numbered picker. The PG
maintenance database (``postgres``) is empty by default and offering
it as a numbered choice next to a real user database (e.g. ``SAP``)
has caused users to pick it by mistake — the AMX schema then lands in
the empty system DB and is invisible from the user's main connection.

This test pins the filter: when other DBs exist, ``postgres`` is
dropped; on a fresh server with only ``postgres``, it is still
returned (otherwise bootstrap would have nothing to pick).
"""

from __future__ import annotations

from unittest.mock import MagicMock

from amx.config import DBConfig
from amx.db.adapters.postgresql import PostgreSQLAdapter


def _adapter_with_db_listing(rows: list[str]) -> PostgreSQLAdapter:
    """Build a PostgreSQLAdapter whose engine returns *rows* from list_databases."""
    cfg = DBConfig(
        backend="postgresql",
        host="localhost",
        port=5432,
        database="test",
        user="test",
        password="test",
    )
    adapter = PostgreSQLAdapter(cfg)
    fake_engine = MagicMock()
    cm = fake_engine.connect.return_value.__enter__.return_value
    cm.execute.return_value.fetchall.return_value = [(name,) for name in rows]
    fake_engine.connect.return_value.__exit__.return_value = False
    return adapter, fake_engine


def test_postgres_dropped_when_other_dbs_exist() -> None:
    adapter, engine = _adapter_with_db_listing(["postgres", "SAP", "analytics"])
    assert adapter.list_databases(engine) == ["SAP", "analytics"]


def test_postgres_kept_when_it_is_the_only_db() -> None:
    """Fresh install fallback — bootstrap can't pick from an empty list."""
    adapter, engine = _adapter_with_db_listing(["postgres"])
    assert adapter.list_databases(engine) == ["postgres"]


def test_user_dbs_returned_unchanged_when_postgres_absent() -> None:
    adapter, engine = _adapter_with_db_listing(["SAP", "analytics", "warehouse_prod"])
    assert adapter.list_databases(engine) == ["SAP", "analytics", "warehouse_prod"]
