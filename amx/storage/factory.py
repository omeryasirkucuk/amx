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

from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING

from amx.storage import sqlite_store
from amx.storage.sqlite_store import SQLiteHistoryStore
from amx.utils.logging import get_logger

if TYPE_CHECKING:
    from amx.config import AMXConfig, DBConfig

log = get_logger("storage.factory")


# Process-level cache so the bootstrap warning fires AT MOST ONCE per
# (profile, schema) per process. Without this, every slash command in
# an interactive session re-enters init_history_store and re-emits a
# multi-page warning; the user-reported regression on 2026-05-03 made
# the terminal unusable.
_BOOTSTRAP_FAILURE_CACHE: set[tuple[str, str]] = set()


def _short_error(exc: BaseException) -> str:
    """Return the first non-empty line of *exc*'s message.

    SQLAlchemy errors include the offending SQL in the body — useful
    in debug logs but a 70-line wall-of-text in user-facing warnings.
    """
    msg = str(exc).strip()
    if not msg:
        return exc.__class__.__name__
    first = msg.split("\n", 1)[0].strip()
    # Trim verbose SQLAlchemy boilerplate suffixes when present.
    return first.split("(Background on this error", 1)[0].rstrip(" .")


def apply_history_db_override(db_cfg: DBConfig, override: str) -> DBConfig:
    """Return a copy of *db_cfg* with *override* applied to the right field.

    Different backends use different fields to identify the container
    that holds the AMX schema:

    * Databricks Unity Catalog → ``catalog`` (the FQN is
      ``catalog.schema.table``).
    * BigQuery → ``project`` (datasets are scoped to a project).
    * Everything else (PG, MySQL, MSSQL, Oracle, Redshift, Snowflake) →
      ``database``.

    Empty *override* is a no-op — the profile keeps whatever it had
    pinned originally.
    """
    if not override:
        return db_cfg
    if db_cfg.backend == "databricks":
        return replace(db_cfg, catalog=override)
    if db_cfg.backend == "bigquery":
        return replace(db_cfg, project=override)
    return replace(db_cfg, database=override)


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

    # Apply the user's "where do you want the AMX schema?" choice
    # (set during /history-store enable). For PG/MySQL/MSSQL/Oracle/
    # Redshift/Snowflake this overrides ``database``; for Databricks
    # ``catalog``; for BigQuery ``project``. ``replace`` returns a
    # copy so the user's saved profile in ``cfg.db_profiles`` is
    # untouched.
    override = (getattr(cfg, "history_store_database", "") or "").strip()
    if override:
        db_cfg = apply_history_db_override(db_cfg, override)

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


def _warn_bootstrap_failure_once(
    cache_key: tuple[str, str], short: str, exc: BaseException
) -> None:
    """Emit a single one-line warning per (profile, schema) per process.

    The full exception (including the multi-line CREATE TABLE DDL that
    SQLAlchemy attaches to errors) goes to the debug log only — that
    multi-page wall-of-text was the user-reported regression on
    2026-05-03 that turned every slash command into a screenful of SQL.
    Subsequent calls in the same process for the same target are silent
    so an interactive session that re-enters ``init_history_store`` per
    slash command stops spamming the same warning.
    """
    if cache_key in _BOOTSTRAP_FAILURE_CACHE:
        log.debug("Shared history bootstrap retry suppressed for %r: %s", cache_key, short)
        return
    _BOOTSTRAP_FAILURE_CACHE.add(cache_key)
    log.warning(
        "Shared run-history disabled this session: %s "
        "(run `/history-store disable` to silence permanently, "
        "or `/history-store enable` to re-bootstrap).",
        short,
    )
    log.debug("Shared history bootstrap full traceback:", exc_info=exc)


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
    log a single one-line warning per process. The user can fix the
    issue and re-run ``/history-store enable`` without losing the
    active session, or run ``/history-store disable`` to stop AMX from
    even trying.
    """
    config_dir = getattr(cfg, "CONFIG_DIR", str(Path.home() / ".amx"))
    db_path = Path(config_dir) / "history.db"
    local = SQLiteHistoryStore(db_path)
    try:
        local.init()
    except Exception as exc:
        log.warning("Could not initialise local SQLite history: %s", exc)

    profile_key = (
        str(getattr(cfg, "history_store_profile", "") or ""),
        str(getattr(cfg, "history_store_schema", "") or ""),
    )

    shared = None
    try:
        shared = _build_shared_store(cfg)
    except HistoryStoreBootstrapError as exc:
        _warn_bootstrap_failure_once(profile_key, _short_error(exc), exc)
    except Exception as exc:
        _warn_bootstrap_failure_once(profile_key, _short_error(exc), exc)

    if shared is not None:
        try:
            shared.init()
        except Exception as exc:
            _warn_bootstrap_failure_once(profile_key, _short_error(exc), exc)
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
    "apply_history_db_override",
    "history_store",
    "init_history_store",
]
