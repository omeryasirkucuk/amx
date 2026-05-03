"""History-store factory.

Single entry point that decides whether the running session gets a
local-only :class:`SQLiteHistoryStore` or a dual-write store wrapping
the local SQLite + a shared warehouse-backed
:class:`SQLAlchemyHistoryStore`. Inspects ``cfg.history_store_enabled``
plus a couple of guard rails (the named profile must exist, its
backend must declare ``supports_shared_history``, the engine must be
buildable).

Replaces the original ``init_history_store(config_dir: str)`` helper in
:mod:`amx.storage.sqlite_store` for callers that have an
:class:`AMXConfig` in hand. The legacy function is preserved as a
back-compat shim that defaults to local-only.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from amx.storage import sqlite_store
from amx.storage.sqlite_store import SQLiteHistoryStore
from amx.utils.logging import get_logger

if TYPE_CHECKING:
    from amx.config import AMXConfig

log = get_logger("storage.factory")


class HistoryStoreBootstrapError(RuntimeError):
    """Raised when shared-history setup fails in a recoverable way.

    Carries the DDL the user can hand to a DBA so the failure surfaces
    with a clear remediation path (typical case: the active connection
    lacks ``CREATE SCHEMA`` privileges).
    """

    def __init__(self, message: str, *, ddl: str | None = None) -> None:
        super().__init__(message)
        self.ddl = ddl or ""


def _build_shared_store(cfg: AMXConfig):
    """Construct a SQLAlchemyHistoryStore from *cfg*'s history profile.

    Returns ``None`` (and logs at INFO) if shared mode is disabled or
    the named profile is missing. Raises :class:`HistoryStoreBootstrapError`
    when shared mode is requested but the backend cannot host it.
    """
    if not getattr(cfg, "history_store_enabled", False):
        return None

    profile_name = (getattr(cfg, "history_store_profile", "") or "").strip()
    if not profile_name:
        log.warning(
            "history_store_enabled=True but history_store_profile is empty — "
            "falling back to local-only history."
        )
        return None
    if profile_name not in cfg.db_profiles:
        log.warning(
            "history_store_profile=%r is not in cfg.db_profiles — "
            "falling back to local-only history.",
            profile_name,
        )
        return None

    db_cfg = cfg.db_profiles[profile_name]

    # Local imports keep the SQLAlchemy + adapter surface out of the
    # main amx import path. ``init_history_store`` is hot on every
    # ``amx`` invocation; importing SQLAlchemy adapters on a code path
    # most users never hit would cost ~150ms of startup time.
    from amx.db.adapters import get_adapter
    from amx.storage.sqlalchemy_store import SQLAlchemyHistoryStore

    adapter = get_adapter(db_cfg)
    if not getattr(adapter.capabilities, "supports_shared_history", False):
        raise HistoryStoreBootstrapError(
            f"The {db_cfg.backend!r} backend does not support shared run-history "
            f"(profile {profile_name!r}). Pick a profile whose backend supports "
            "it, or run `/history-store disable`."
        )

    schema_name = (getattr(cfg, "history_store_schema", "") or "AMX").strip() or "AMX"
    engine = adapter.create_engine()
    store = SQLAlchemyHistoryStore(engine=engine, schema=schema_name)
    return store


def init_history_store(cfg: AMXConfig):
    """Build the singleton history store and attach it to the legacy global.

    The returned object always implements :class:`amx.storage.protocol.IHistoryStore`.
    When shared mode is enabled, the result is a
    :class:`amx.storage.dual_write.DualWriteHistoryStore` wrapping local
    SQLite + the shared SQLAlchemy store; otherwise it is a vanilla
    :class:`SQLiteHistoryStore`. Both are pin-compatible with every
    existing call site that uses ``history_store()`` from
    :mod:`amx.storage.sqlite_store`.

    On bootstrap failure (network unreachable, schema lacks DDL
    permissions, etc.) we DO NOT raise — we fall back to local-only and
    log a warning. The user can fix the issue and re-run
    ``/history-store enable`` without losing the active session.
    """
    config_dir = getattr(cfg, "CONFIG_DIR", str(Path.home() / ".amx"))
    db_path = Path(config_dir) / "history.db"
    local = SQLiteHistoryStore(db_path)
    try:
        local.init()
    except Exception as exc:
        log.warning("Could not initialise local SQLite history: %s", exc)

    shared = None
    try:
        shared = _build_shared_store(cfg)
    except HistoryStoreBootstrapError as exc:
        # Surface as a warning + DDL hint; keep going with local-only
        # so the user's session is never blocked.
        log.warning(
            "Shared history bootstrap failed: %s\n"
            "Falling back to local-only history.\n"
            "Run `/history-store enable` after fixing.",
            exc,
        )
    except Exception as exc:
        log.warning(
            "Could not connect to shared history store; falling back "
            "to local-only. Underlying error: %s",
            exc,
        )

    if shared is not None:
        try:
            shared.init()
        except Exception as exc:
            log.warning(
                "Shared history schema not initialised (%s). Run "
                "`/history-store enable` to bootstrap the AMX schema.",
                exc,
            )
            shared = None

    if shared is None:
        sqlite_store._store = local  # type: ignore[assignment]
        return local

    from amx.storage.dual_write import DualWriteHistoryStore

    dual = DualWriteHistoryStore(local=local, shared=shared)
    sqlite_store._store = dual  # type: ignore[assignment]
    return dual


def history_store():
    """Return the active history store singleton, or ``None``."""
    return sqlite_store._store


__all__ = [
    "HistoryStoreBootstrapError",
    "history_store",
    "init_history_store",
]
