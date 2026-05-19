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
            "history_store_enabled=True but history_store_profile is empty  --  "
            "falling back to local-only history."
        )
        return None
    if profile_name not in cfg.db_profiles:
        log.warning(
            "history_store_profile=%r is not in cfg.db_profiles  --  "
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
    # Pre-create the AMX schema before MetaData.create_all reaches for
    # CREATE TABLE. The DDL is `CREATE SCHEMA IF NOT EXISTS …` so it
    # is a no-op when the schema already exists; without this call,
    # users whose schema was dropped (or never created on this host)
    # see a SCHEMA_NOT_FOUND on every startup because create_all
    # assumes the parent schema exists. Symmetric with the call that
    # `/history-store enable` already issues.
    #
    # Best-effort: a permission failure here is downgraded to debug
    # log because in practice the schema may already exist and
    # create_all will succeed regardless. A genuinely missing schema +
    # missing CREATE SCHEMA grant still surfaces via the create_all
    # error, with the original phrasing preserved.
    try:
        adapter.create_history_schema(engine, schema_name)
    except Exception as exc:
        log.debug(
            "Pre-create schema %r failed (will let create_all decide): %s",
            schema_name,
            exc,
        )
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


def _bootstrap_dual_or_local(local: SQLiteHistoryStore, cfg: AMXConfig):
    """Build the dual-write store, or fall back to *local* on failure.

    Network-bound — runs the SQLAlchemy engine creation, schema bootstrap,
    and table create_all on the remote backend. Errors are caught and
    downgraded to a one-line warning (cached per profile/schema so the
    same failure is not announced twice in one process).

    Used by :class:`_LazyDualWriteStore` to defer all of the above out
    of the ``amx`` startup path.
    """
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
        return local

    from amx.storage.dual_write import DualWriteHistoryStore

    dual = DualWriteHistoryStore(local=local, shared=shared)

    from amx.storage.backfill import start_background_backfill

    try:
        start_background_backfill(
            local,
            shared,
            shared_profile=str(getattr(cfg, "history_store_profile", "") or ""),
            shared_schema=str(getattr(cfg, "history_store_schema", "") or "AMX"),
        )
    except Exception:  # noqa: BLE001
        log.debug("backfill bootstrap failed; continuing", exc_info=True)

    return dual


class _LazyDualWriteStore:
    """Defer shared-history bootstrap until first method call.

    Building the shared backend (Databricks, Postgres, Snowflake, …)
    means an SQLAlchemy engine + a schema-create + a CREATE TABLE
    round-trip — easily 2-3 seconds on a remote warehouse. Most ``amx``
    invocations never write to history (they list profiles, run /help,
    /db-profiles, …) so blocking the welcome banner on that work is
    pure user-perceived latency.

    This wrapper holds the local SQLite store eagerly (already cheap
    to build) and constructs the real
    :class:`~amx.storage.dual_write.DualWriteHistoryStore` on demand —
    typically the first ``/run`` or any other call that hits a write
    method. After the first call the wrapper proxies straight through
    to the real store with no extra overhead.

    On bootstrap failure the wrapper transparently falls back to the
    local-only :class:`SQLiteHistoryStore`, mirroring the historical
    eager path but without the startup tax.
    """

    def __init__(self, local: SQLiteHistoryStore, cfg: AMXConfig) -> None:
        # Stored under leading-underscore names so __getattr__ can
        # cleanly distinguish "wrapper internals" (never proxied) from
        # delegate attributes (always proxied).
        self._local = local
        self._cfg = cfg
        self._wrapped: object | None = None
        # Public ``db_path`` is read by callers that expect the
        # IHistoryStore-style local path attribute (notably the outbox
        # bookkeeping in DualWriteHistoryStore tests). Mirror it from
        # the local store so it works without forcing a build.
        self.db_path = local.db_path

    @property
    def local(self) -> SQLiteHistoryStore:
        """Always-available local store; reading this never bootstraps."""
        return self._local

    # ── Local SQLite delegates (zero-cost; never bootstrap shared) ────────
    # ``_lock`` and ``_connect`` are part of the SQLite-level API that
    # ``ChatSessionStore`` and the migration helpers reach into directly
    # via ``self._history._lock`` / ``self._history._connect()``. They
    # are inherently LOCAL operations (the chat-session table lives in
    # ``~/.amx/history.db``, not on the team backend), so we route them
    # straight to the eager local store. Without this, ``/ask`` raised
    # ``Ask failed: _lock`` because ``__getattr__`` rejects underscore
    # names by design.
    @property
    def _lock(self):  # noqa: ANN201 — returned object is a threading.Lock
        return self._local._lock

    def _connect(self):  # noqa: ANN201 — returns sqlite3.Connection ctxmgr
        return self._local._connect()

    @property
    def shared(self):  # noqa: ANN201 — duck-typed for `hasattr(store, "shared")` callers
        """Real shared store, or ``None`` when bootstrap failed.

        Reading this property triggers the lazy build because every
        existing ``hasattr(hs, "shared")`` consumer is gating a real
        write/read against the team backend; that work was always
        going to be the moment we absorb the bootstrap cost anyway.
        """
        target = self._ensure_built()
        return getattr(target, "shared", None)

    def pending_count(self) -> int:
        target = self._ensure_built()
        method = getattr(target, "pending_count", None)
        return int(method()) if callable(method) else 0

    def flush_pending(self) -> tuple[int, int]:
        target = self._ensure_built()
        method = getattr(target, "flush_pending", None)
        return method() if callable(method) else (0, 0)

    def _ensure_built(self) -> object:
        if self._wrapped is None:
            self._wrapped = _bootstrap_dual_or_local(self._local, self._cfg)
        return self._wrapped

    def __getattr__(self, name: str) -> object:
        # Only fires for attrs not in the instance __dict__ and not
        # defined on the class above. Keeps wrapper internals
        # (``_local`` / ``_cfg`` / ``_wrapped``) out of the lazy-proxy
        # path so reading them never triggers a bootstrap.
        if name.startswith("_"):
            raise AttributeError(name)
        target = self._ensure_built()
        return getattr(target, name)


def init_history_store(cfg: AMXConfig):
    """Build the singleton history store and attach it to the legacy global.

    The returned object always implements :class:`amx.storage.protocol.IHistoryStore`.
    When shared mode is enabled, the result is a
    :class:`_LazyDualWriteStore` that wraps a local SQLite store
    eagerly and bootstraps the shared SQLAlchemy backend on first use;
    otherwise it is a plain :class:`SQLiteHistoryStore`. Both are
    pin-compatible with every existing call site that reads from
    ``history_store()``.

    The deferred build keeps ``amx`` startup local-only — the welcome
    banner appears in <100ms even on Databricks/PG-backed shared
    history. Bootstrap failures are still announced (one-line warning,
    cached per profile/schema) but only the first time a call actually
    needs the shared backend.
    """
    config_dir = getattr(cfg, "CONFIG_DIR", str(Path.home() / ".amx"))
    db_path = Path(config_dir) / "history.db"
    local = SQLiteHistoryStore(db_path)
    try:
        local.init()
    except Exception as exc:
        log.warning("Could not initialise local SQLite history: %s", exc)

    if not getattr(cfg, "history_store_enabled", False):
        sqlite_store._store = local  # type: ignore[assignment]
        return local

    lazy = _LazyDualWriteStore(local, cfg)
    sqlite_store._store = lazy  # type: ignore[assignment]
    return lazy


def history_store():
    """Return the active history store singleton, or ``None``."""
    return sqlite_store._store


__all__ = [
    "HistoryStoreBootstrapError",
    "_LazyDualWriteStore",
    "apply_history_db_override",
    "history_store",
    "init_history_store",
]
