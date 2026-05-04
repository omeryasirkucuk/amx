"""Regression: startup bootstrap must pre-create the AMX schema.

User report (2026-05-04 against 0.12.5): on every ``amx`` startup,

    [WARNING] Shared run-history disabled this session: …
    SCHEMA_NOT_FOUND The schema `sap.amx` cannot be found.

The asymmetry: ``/history-store enable`` already calls
``adapter.create_history_schema(engine, schema_name)`` (idempotent
``CREATE SCHEMA IF NOT EXISTS``) before running ``MetaData.create_all``.
The startup path inside ``_build_shared_store`` did NOT, so any user
whose schema disappeared (or was never created on this host) saw
SCHEMA_NOT_FOUND on every startup.

This test pins the ordering: ``create_history_schema`` runs before
``SQLAlchemyHistoryStore`` is built. Failures from the pre-create
step are downgraded to debug logs (the schema may already exist —
``MetaData.create_all`` will succeed regardless).
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from amx.storage import factory


@dataclass
class _StubCapabilities:
    supports_shared_history: bool = True
    schema_comments: bool = False


class _StubAdapter:
    """Records the call order of ``create_engine`` / ``create_history_schema``
    so tests can assert ordering without a real SQLAlchemy engine."""

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.capabilities = _StubCapabilities()

    def create_engine(self):
        self.calls.append("create_engine")
        return object()

    def create_history_schema(self, engine, schema_name: str) -> None:
        self.calls.append(f"create_history_schema:{schema_name}")

    def create_history_schema_ddl(self, schema_name: str) -> str:
        return f"CREATE SCHEMA IF NOT EXISTS {schema_name}"


@dataclass
class _StubDBProfile:
    backend: str = "postgresql"


class _StubAMXConfig:
    history_store_enabled = True
    history_store_profile = "team_pg"
    history_store_schema = "AMX"
    history_store_database = ""
    db_profiles = {"team_pg": _StubDBProfile()}


@pytest.fixture
def patched_factory(monkeypatch: pytest.MonkeyPatch) -> _StubAdapter:
    """Wire ``_build_shared_store`` against a stub adapter and a no-op
    ``SQLAlchemyHistoryStore`` so we can assert the call order without
    a real backend."""
    adapter = _StubAdapter()
    monkeypatch.setattr("amx.db.adapters.get_adapter", lambda _db_cfg: adapter)

    class _NoopStore:
        def __init__(self, *_args, **_kwargs) -> None:
            pass

        def init(self) -> None:
            adapter.calls.append("SQLAlchemyHistoryStore.init")

    monkeypatch.setattr(
        "amx.storage.sqlalchemy_store.SQLAlchemyHistoryStore",
        _NoopStore,
    )
    return adapter


def test_build_shared_store_pre_creates_schema_before_init(
    patched_factory: _StubAdapter,
) -> None:
    cfg = _StubAMXConfig()
    store = factory._build_shared_store(cfg)
    assert store is not None
    # Ordering contract: create_engine → create_history_schema → store ctor.
    # ``store.init()`` is invoked by the caller (init_history_store /
    # _LazyDualWriteStore), not by _build_shared_store itself.
    assert patched_factory.calls == [
        "create_engine",
        "create_history_schema:AMX",
    ]


def test_pre_create_schema_failure_is_swallowed_to_debug(
    patched_factory: _StubAdapter,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """If the pre-create step fails (e.g. user lacks CREATE SCHEMA but
    the schema already exists), we must NOT abort. ``MetaData.create_all``
    will get its chance to succeed; only a real SCHEMA_NOT_FOUND from
    that path should reach the user."""

    def _raise(_engine, _schema):
        raise RuntimeError("permission denied: cannot CREATE SCHEMA")

    monkeypatch.setattr(patched_factory, "create_history_schema", _raise)

    cfg = _StubAMXConfig()
    caplog.set_level("DEBUG", logger="amx.storage.factory")
    store = factory._build_shared_store(cfg)
    # Build still succeeded — the failure was logged at debug level so
    # MetaData.create_all is given the chance to run.
    assert store is not None
    debug_messages = [record.message for record in caplog.records if record.levelname == "DEBUG"]
    assert any("Pre-create schema" in msg for msg in debug_messages)
