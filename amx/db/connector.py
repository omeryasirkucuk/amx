"""Database introspection and metadata extraction.

Supports multiple backends via the adapter layer in ``amx.db.adapters``.
"""

from __future__ import annotations

import time
from typing import Any

from sqlalchemy import inspect, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import NoSuchTableError

from amx.config import DBConfig
from amx.core.errors import actionable_error_message
from amx.db._connector_types import (
    _NON_TRANSIENT_DB_PATTERNS as _NON_TRANSIENT_DB_PATTERNS,
)
from amx.db._connector_types import (
    _TRANSIENT_DB_PATTERNS as _TRANSIENT_DB_PATTERNS,
)
from amx.db._connector_types import (  # noqa: PLC0414
    CONNECTION_RETRY_BACKOFF_SEC as CONNECTION_RETRY_BACKOFF_SEC,
)
from amx.db._connector_types import (
    MAX_CONNECTION_RETRIES as MAX_CONNECTION_RETRIES,
)
from amx.db._connector_types import (
    AnalyticsMetadata as AnalyticsMetadata,
)
from amx.db._connector_types import (
    AssetKind as AssetKind,
)
from amx.db._connector_types import (
    ColumnProfile as ColumnProfile,
)
from amx.db._connector_types import (
    ConnectionTestResult as ConnectionTestResult,
)
from amx.db._connector_types import (
    ProfilingError as ProfilingError,
)
from amx.db._connector_types import (
    TableProfile as TableProfile,
)
from amx.db._connector_types import (
    _is_transient_db_connection_error as _is_transient_db_connection_error,
)
from amx.db.adapters.base import BackendCapabilities, UnsupportedDatabaseOperation
from amx.utils.logging import get_logger

log = get_logger("db.connector")


# Process-wide hit / miss tallies for the in-memory wizard memo. Exposed
# through ``/db cache stats`` so the user can verify the wizard fix is
# serving repeat picker invocations from memo rather than the live DB.
# Counters reset on process restart (no persistence).
_LISTING_MEMO_COUNTERS: dict[str, dict[str, int]] = {
    "catalogs": {"hit": 0, "miss": 0},
    "databases": {"hit": 0, "miss": 0},
}


def _bump_listing_memo_counter(kind: str, outcome: str) -> None:
    """Increment the wizard-memo counter for ``kind`` / ``outcome``.

    ``kind`` is ``"catalogs"`` or ``"databases"``; ``outcome`` is
    ``"hit"`` or ``"miss"``. The dict is initialized with both keys so
    no thread-safety dance is needed for simple incremements — the GIL
    serializes the read-modify-write of the int.
    """
    try:
        _LISTING_MEMO_COUNTERS[kind][outcome] += 1
    except KeyError:
        pass


def get_listing_memo_counters() -> dict[str, dict[str, int]]:
    """Snapshot the wizard-memo hit / miss tallies.

    Consumed by ``amx.storage.cache_ops.cache_stats`` so ``/db cache
    stats`` can surface wizard cache effectiveness alongside the
    persistent cache counters.
    """
    return {kind: dict(counts) for kind, counts in _LISTING_MEMO_COUNTERS.items()}


class DatabaseConnector:
    """Unified database connector that delegates backend-specific work to adapters."""

    def __init__(self, cfg: DBConfig, *, profile_name: str = ""):
        # ``profile_name`` is the AMXConfig key (e.g. "prod-snowflake")
        # this connector was opened for. Used as the cache key for the
        # column-comments cache (:mod:`amx.storage.sqlite_store`) so an
        # invalidation triggered from the web router or a CLI write hits
        # the exact rows the connector populated. Empty string is the
        # legacy single-profile fallback — callers that have a profile
        # name should always pass it.
        self.profile_name = str(profile_name or "")

        # Sanitize unresolved keyring references on a COPY so the
        # adapter never tries to use ``keyring:db_profiles/<x>/password``
        # as a real secret (which would produce a confusing
        # backend-specific auth error). Mutating the caller's cfg
        # would propagate through ``AMXConfig.save()`` and erase the
        # YAML reference — keep the original intact so the next
        # process with a healthy keyring backend (any platform:
        # macOS Keychain, Linux Secret Service, Windows Credential
        # Manager) can resolve it.
        from dataclasses import replace as _replace

        from amx.storage.secrets import is_secret_reference

        sanitized: dict[str, str] = {}
        for fld in ("password", "access_token"):
            current = getattr(cfg, fld, "") or ""
            if is_secret_reference(current):
                sanitized[fld] = ""
                log.warning(
                    "DB profile %s field is an unresolved keyring "
                    "reference — backend keyring may be unavailable. "
                    "Connection attempts will fail until the backend "
                    "recovers or the credential is re-entered via /db.",
                    fld,
                )
        if sanitized:
            self.cfg = _replace(cfg, **sanitized)
        else:
            self.cfg = cfg
        self._engine: Engine | None = None

        # Auto-install the backend's driver(s) before constructing the
        # adapter so a user with a saved profile (e.g. ``local-postgre``
        # backed by Postgres) on a fresh slim install never sees the
        # raw ``ModuleNotFoundError: No module named 'psycopg2'`` —
        # they see the same one-time pip-progress UX every other
        # feature uses. Idempotent / cached after first hit.
        from amx.db.drivers import ensure_backend_driver

        ensure_backend_driver(cfg.backend)

        from amx.db.adapters import get_adapter

        self._adapter = get_adapter(self.cfg)

        # In-process memo for wizard-driven catalog / database pickers.
        # These two listings have no persistent cache table (unlike
        # schemas_cache / column_comments_cache) so without a memo the
        # wizard fires a fresh ``SHOW CATALOGS`` / ``SHOW DATABASES`` on
        # every /run, /edit, /search sync invocation. The memo lasts for
        # ``_listing_memo_ttl_seconds`` and is cleared on ``reconnect()``
        # and on explicit ``invalidate_listing_memo()`` calls.
        self._catalogs_memo: tuple[float, list[str]] | None = None
        self._databases_memo: tuple[float, list[str]] | None = None
        self._listing_memo_ttl_seconds: float = 300.0

    @property
    def engine(self) -> Engine:
        if self._engine is None:
            self._engine = self._adapter.create_engine()
            url_tail = self.cfg.url.split("@")[-1] if "@" in self.cfg.url else self.cfg.url
            log.info("Connected via %s to %s", self._adapter.name, url_tail)
        return self._engine

    @property
    def backend(self) -> str:
        return self._adapter.name

    @property
    def capabilities(self) -> BackendCapabilities:
        return getattr(self._adapter, "capabilities", BackendCapabilities())

    def _get_inspector(self):
        """Return a SQLAlchemy inspector for the live engine.

        Indirection point so tests can monkey-patch
        ``amx.db.connector.inspect`` (the single source of truth) and
        affect every call site — including the profiler module which
        delegates here instead of importing :mod:`sqlalchemy.inspect`
        independently.
        """
        return inspect(self.engine)

    def _normalize_id(self, value: str) -> str:
        """Fold a user-supplied identifier into the form the backend stores.

        Delegated to ``adapter.normalize_identifier`` (pass-through on
        most backends; UPPER on Oracle and Snowflake). Test fakes that
        skip the method get a safe identity fallback — defensive because
        the connector is exercised heavily with stub adapters.
        """
        normaliser = getattr(self._adapter, "normalize_identifier", None)
        if normaliser is None:
            return value
        try:
            return normaliser(value)
        except Exception:
            return value

    def test_connection_result(self) -> ConnectionTestResult:
        """Test the active connection, retrying once on transient failures.

        Mirrors :func:`amx.llm.provider._is_transient_llm_error` — DNS
        glitches, connection resets, and timeouts are retried once with
        a short backoff; auth / permission / missing-DB / SSL-trust
        errors propagate immediately so the user sees the categorised
        actionable message from :class:`amx.core.errors.ErrorMapper`
        without an artificial delay.
        """
        last_exc: Exception | None = None
        for attempt in range(MAX_CONNECTION_RETRIES + 1):
            try:
                self._adapter.test_connection(self._engine)
                return ConnectionTestResult(ok=True)
            except Exception as exc:
                last_exc = exc
                if attempt < MAX_CONNECTION_RETRIES and _is_transient_db_connection_error(exc):
                    wait = CONNECTION_RETRY_BACKOFF_SEC * (2**attempt)
                    log.warning(
                        "DB connection failed (attempt %d/%d)  --  retrying in %.1fs: %s",
                        attempt + 1,
                        MAX_CONNECTION_RETRIES + 1,
                        wait,
                        exc,
                    )
                    time.sleep(wait)
                    continue
                break

        assert last_exc is not None  # the loop must have raised at least once
        actionable = self._adapter.actionable_profile_error(last_exc) or actionable_error_message(
            last_exc, backend=self.backend
        )
        log.error("Connection failed: %s", actionable)
        return ConnectionTestResult(ok=False, message=actionable, exception=last_exc)

    def test_connection(self) -> bool:
        return self.test_connection_result().ok

    # ── Schema / asset listing ────────────────────────────────────────────

    def list_catalogs(self) -> list[str]:
        """Catalogs visible to the active connection.

        Empty list for backends without a 3-level
        catalog/schema/table hierarchy. Used by the manual-edit
        wizard on Databricks Unity Catalog so the user picks a
        catalog before the schema picker fires.

        ``ImportError`` propagates so the missing-driver case (user
        ran ``pip install amx-cli`` without ``[databricks]``) reaches
        the catalog picker as an actionable hint instead of
        masquerading as "empty workspace".

        Reads from the in-process memo when fresh — wizards that fire
        catalog pickers on every /run / /edit / /search sync used to
        re-issue ``SHOW CATALOGS`` each time. The memo is cleared by
        ``reconnect()`` and ``invalidate_listing_memo()``.
        """
        now = time.time()
        if self._catalogs_memo is not None:
            ts, cached = self._catalogs_memo
            if (now - ts) < self._listing_memo_ttl_seconds:
                _bump_listing_memo_counter("catalogs", "hit")
                return list(cached)
        try:
            result = list(self._adapter.list_catalogs(self.engine))
        except ImportError:
            raise
        except Exception:
            _bump_listing_memo_counter("catalogs", "miss")
            return []
        self._catalogs_memo = (now, result)
        _bump_listing_memo_counter("catalogs", "miss")
        return result

    def supports_catalogs(self) -> bool:
        """True when ``list_catalogs`` is meaningful for this adapter."""
        try:
            return bool(self._adapter.supports_catalogs())
        except Exception:
            return False

    def list_databases(self) -> list[str]:
        """User-visible databases on this server (2-level backends only).

        Used by the runtime database picker when the profile has
        ``database=""``. Returns ``[]`` for backends that don't expose a
        multi-database server (Databricks catalogs, BigQuery datasets —
        those use ``list_catalogs``).

        ``ImportError`` propagates so the missing-driver case (a fresh
        ``pip install amx-cli`` without the right extra) reaches the
        runtime database picker as an actionable hint instead of
        masquerading as "no databases visible". Mirrors the symmetric
        behaviour of :meth:`list_catalogs`, including the in-process
        memo so picker re-prompts don't hit the server.
        """
        now = time.time()
        if self._databases_memo is not None:
            ts, cached = self._databases_memo
            if (now - ts) < self._listing_memo_ttl_seconds:
                _bump_listing_memo_counter("databases", "hit")
                return list(cached)
        try:
            result = list(self._adapter.list_databases(self.engine))
        except ImportError:
            raise
        except Exception as exc:
            log.debug("list_databases failed: %s", exc)
            _bump_listing_memo_counter("databases", "miss")
            return []
        self._databases_memo = (now, result)
        _bump_listing_memo_counter("databases", "miss")
        return result

    def invalidate_listing_memo(self) -> None:
        """Drop the in-process catalog/database memo.

        Called by reconnect-aware paths after a config edit so the next
        picker sees the live server state. The persistent caches
        (``schemas_cache``, ``column_comments_cache``) are unaffected —
        their invalidations happen via the storage-layer helpers.
        """
        self._catalogs_memo = None
        self._databases_memo = None

    def reconnect(self) -> None:
        """Dispose the active engine so the next ``self.engine`` access
        rebuilds it from the current ``self.cfg``.

        The runtime database picker mutates ``self.cfg.database``
        in-memory; without a reconnect, ``self._engine`` would still be
        bound to the old database and every subsequent listing query
        would target the wrong DB.

        Also drops the catalogs/databases memo — the picker just changed
        the active scope, so the cached listing may not apply any more.
        """
        if self._engine is not None:
            try:
                self._engine.dispose()
            except Exception as exc:
                log.debug("engine.dispose() raised during reconnect: %s", exc)
        self._engine = None
        self.invalidate_listing_memo()

    def list_schemas(self) -> list[str]:
        # Cache fast path: the schemas_cache holds (schema, comment)
        # pairs per (profile, database, catalog). Reading here lets the
        # sidebar's catalog-expand short-circuit BOTH the schema
        # enumeration AND the per-schema get_schema_comment loop the
        # router fires right after — see _populate_catalogs_cache for
        # the bulk fill that warms this state.
        catalog = getattr(self.cfg, "catalog", "") or ""
        live: list[str] | None = None
        if self._catalog_bulk_cache_is_fresh(catalog):
            cached = self._list_schemas_from_cache(catalog)
            if cached:
                live = [name for name, _ in cached]
        if live is None and self._populate_catalogs_cache(catalog):
            cached = self._list_schemas_from_cache(catalog)
            if cached:
                live = [name for name, _ in cached]
        if live is None:
            # Adapter-specific override (e.g. Databricks ``SHOW SCHEMAS
            # IN <catalog>``) takes precedence so catalog-scoped
            # backends don't fall through to the SQLAlchemy inspector —
            # which ignores catalog and returns ambiguous results.
            try:
                adapter_result = self._adapter.list_schemas(self.engine, catalog)
            except Exception:
                adapter_result = None
            if adapter_result is not None:
                live = list(adapter_result)
            else:
                insp = inspect(self.engine)
                system = self._adapter.system_schemas()
                live = [s for s in insp.get_schema_names() if s not in system]
        return self._apply_pinned_schema_filter(live)

    def _apply_pinned_schema_filter(self, schemas: list[str]) -> list[str]:
        """Narrow the live schema list to the pinned value when set.

        Implements the wizard-driven scope rule for the third level of
        the hierarchy: Databricks's ``cfg.database`` is a SCHEMA pin
        (the wizard prompt literally reads "Schema / database
        (optional)"); BigQuery's ``cfg.dataset`` is the same idea.
        Other backends have no schema-level pin so the list passes
        through unchanged.

        Fallback (mirrors the catalog picker from PR #318): when the
        pinned value is no longer in the live list (schema dropped,
        permissions lost), return the full live list so the sidebar's
        pinned-but-missing warning can surface — never fabricate a
        phantom row.
        """
        backend = (getattr(self._adapter, "name", "") or "").lower()
        if backend == "databricks":
            pinned = str(getattr(self.cfg, "database", "") or "").strip()
        elif backend == "bigquery":
            pinned = str(getattr(self.cfg, "dataset", "") or "").strip()
        else:
            return schemas
        if not pinned:
            return schemas
        if pinned in schemas:
            return [pinned]
        return schemas

    def list_tables(self, schema: str) -> list[str]:
        # Adapter override path for catalog-scoped backends (Databricks
        # Unity Catalog ``SHOW TABLES IN <catalog>.<schema>``). When
        # the override returns None we fall back to the SQLAlchemy
        # inspector — same contract as ``list_schemas``.
        schema = self._normalize_id(schema)
        catalog = getattr(self.cfg, "catalog", "") or ""
        try:
            adapter_result = self._adapter.list_tables(
                self.engine,
                schema,
                catalog,
            )
        except Exception:
            adapter_result = None
        if adapter_result is not None:
            return list(adapter_result)
        insp = inspect(self.engine)
        return insp.get_table_names(schema=schema)

    def list_views(self, schema: str) -> list[str]:
        schema = self._normalize_id(schema)
        catalog = getattr(self.cfg, "catalog", "") or ""
        try:
            adapter_result = self._adapter.list_views(
                self.engine,
                schema,
                catalog,
            )
        except Exception:
            adapter_result = None
        if adapter_result is not None:
            return list(adapter_result)
        insp = inspect(self.engine)
        return insp.get_view_names(schema=schema)

    def list_materialized_views(self, schema: str) -> list[str]:
        if not self.capabilities.materialized_views:
            return []
        schema = self._normalize_id(schema)
        return self._adapter.list_materialized_views(self.engine, schema)

    # ── Extended object types ─────────────────────────────────────────────
    #
    # Each accessor is gated by the matching ``BackendCapabilities`` flag
    # so unsupported backends short-circuit without firing a query.
    # Adapter exceptions degrade to ``[]`` with a debug-level log entry,
    # mirroring the ``list_databases`` resilience pattern — a single
    # permission failure should never tank a wider listing operation.

    def _list_extended(
        self,
        flag_name: str,
        method_name: str,
        *args: Any,
    ) -> list[dict[str, Any]]:
        if not getattr(self.capabilities, flag_name, False):
            return []
        try:
            return list(getattr(self._adapter, method_name)(self.engine, *args))
        except Exception as exc:
            log.debug("%s failed: %s", method_name, exc)
            return []

    def list_stored_procedures(self, schema: str) -> list[dict[str, Any]]:
        return self._list_extended("stored_procedures", "list_stored_procedures", schema)

    def list_functions(self, schema: str) -> list[dict[str, Any]]:
        return self._list_extended("functions", "list_functions", schema)

    def list_sequences(self, schema: str) -> list[dict[str, Any]]:
        return self._list_extended("sequences", "list_sequences", schema)

    def list_triggers(self, schema: str, table: str | None = None) -> list[dict[str, Any]]:
        return self._list_extended("triggers", "list_triggers", schema, table)

    def list_events(self, schema: str) -> list[dict[str, Any]]:
        return self._list_extended("events", "list_events", schema)

    def list_packages(self, schema: str) -> list[dict[str, Any]]:
        return self._list_extended("packages", "list_packages", schema)

    def list_synonyms(self, schema: str) -> list[dict[str, Any]]:
        return self._list_extended("synonyms", "list_synonyms", schema)

    def list_user_defined_types(self, schema: str) -> list[dict[str, Any]]:
        return self._list_extended("user_defined_types", "list_user_defined_types", schema)

    def list_dictionaries(self, database: str | None = None) -> list[dict[str, Any]]:
        # ClickHouse exposes dictionaries by *database* — defaults to the
        # connection's current database when not passed.
        db = database if database is not None else getattr(self.cfg, "database", "") or ""
        return self._list_extended("dictionaries", "list_dictionaries", db)

    def list_macros(self, schema: str) -> list[dict[str, Any]]:
        return self._list_extended("macros", "list_macros", schema)

    def list_volumes(self, schema: str, catalog: str | None = None) -> list[dict[str, Any]]:
        cat = catalog if catalog is not None else getattr(self.cfg, "catalog", "") or ""
        return self._list_extended("volumes", "list_volumes", cat, schema)

    def list_volumes_bulk(
        self,
        catalog: str | None = None,
    ) -> list[dict[str, Any]] | None:
        """One INFORMATION_SCHEMA query for every volume in the catalog.

        Returns ``None`` when the active backend has no bulk implementation
        (caller falls back to a per-schema loop) or when the
        ``volumes`` capability is off. Callers must handle the
        ``None`` case explicitly.
        """
        if not self.capabilities.volumes:
            return None
        cat = catalog if catalog is not None else getattr(self.cfg, "catalog", "") or ""
        if not cat:
            return None
        try:
            return self._adapter.list_volumes_bulk(self.engine, cat)
        except Exception as exc:
            log.debug("list_volumes_bulk failed for %s: %s", cat, exc)
            return None

    def list_assets_bulk(
        self,
        catalog: str | None = None,
    ) -> list[tuple[str, str, AssetKind]] | None:
        """Bulk asset enumeration across every schema in ``catalog``.

        Returns triples ``(schema, name, AssetKind)`` or ``None`` when the
        active backend has no bulk implementation.
        """
        cat = catalog if catalog is not None else getattr(self.cfg, "catalog", "") or ""
        if not cat:
            return None
        try:
            raw = self._adapter.list_assets_bulk(self.engine, cat)
        except Exception as exc:
            log.debug("list_assets_bulk failed for %s: %s", cat, exc)
            return None
        if raw is None:
            return None
        # Normalise the backend's raw asset-kind string to AssetKind.
        out: list[tuple[str, str, AssetKind]] = []
        for sch, name, raw_kind in raw:
            kind_norm = (raw_kind or "").strip().upper()
            if kind_norm == "VIEW":
                kind = AssetKind.VIEW
            elif kind_norm in {"MATERIALIZED VIEW", "MATERIALIZED_VIEW"}:
                kind = AssetKind.MATERIALIZED_VIEW
            else:
                kind = AssetKind.TABLE
            out.append((sch, name, kind))
        return out

    def list_datashares(self) -> list[dict[str, Any]]:
        # No schema / catalog argument — datashares live at the cluster /
        # account level on every backend that supports them.
        if not self.capabilities.datashares:
            return []
        try:
            return list(self._adapter.list_datashares(self.engine))
        except Exception as exc:
            log.debug("list_datashares failed: %s", exc)
            return []

    def list_external_tables(self, schema: str) -> list[dict[str, Any]]:
        return self._list_extended("external_tables", "list_external_tables", schema)

    def list_assets(self, schema: str) -> list[tuple[str, AssetKind]]:
        """All analyzable assets (tables, views, materialized views) in a schema.

        Cache fast path: when the column-comments cache for this
        schema is bulk-filled (the adapter's ``bulk_schema_metadata``
        ran for it within the TTL window), we already know every table
        + view + MV name and their kinds — return them without
        re-issuing SHOW TABLES / SHOW VIEWS. This is the difference
        the user observed between "expand schema" hitting the DB
        every time and a true warm cache.

        Cold path: try the bulk fetch via
        ``_populate_schema_metadata_cache`` to also fill the cache for
        every sibling read in the same session. If the backend has no
        bulk source (or it fails), fall through to the existing
        per-list inspector calls and DO NOT pretend the result is
        bulk-filled — that flag is reserved for the adapter path that
        promises an exhaustive enumeration.
        """
        normalised = self._normalize_id(schema)
        # Fast path — cache covers the schema; derive list from rows.
        if self._schema_bulk_cache_is_fresh(normalised):
            cached = self._lookup_column_comments_cache_bulk(normalised)
            if cached:
                kind_map = {
                    "TABLE": AssetKind.TABLE,
                    "VIEW": AssetKind.VIEW,
                    "MATERIALIZED VIEW": AssetKind.MATERIALIZED_VIEW,
                }
                assets = [
                    (
                        name,
                        kind_map.get(
                            str(entry.get("kind", "TABLE")).upper(),
                            AssetKind.TABLE,
                        ),
                    )
                    for name, entry in cached.items()
                ]
                assets.sort(key=lambda x: x[0])
                return assets
        # Cold path — try the bulk adapter so the next call is warm.
        if self._populate_schema_metadata_cache(normalised):
            cached = self._lookup_column_comments_cache_bulk(normalised)
            if cached:
                kind_map = {
                    "TABLE": AssetKind.TABLE,
                    "VIEW": AssetKind.VIEW,
                    "MATERIALIZED VIEW": AssetKind.MATERIALIZED_VIEW,
                }
                assets = [
                    (
                        name,
                        kind_map.get(
                            str(entry.get("kind", "TABLE")).upper(),
                            AssetKind.TABLE,
                        ),
                    )
                    for name, entry in cached.items()
                ]
                assets.sort(key=lambda x: x[0])
                return assets
        # Fallback — inspector-driven enumeration (legacy path). The
        # variable name is reused across the cache-fast / cache-cold /
        # legacy branches but each branch returns, so the apparent
        # rebind here is dead code from the type-checker's POV.
        fallback_assets: list[tuple[str, AssetKind]] = []
        for t in self.list_tables(schema):
            fallback_assets.append((t, AssetKind.TABLE))
        for v in self.list_views(schema):
            fallback_assets.append((v, AssetKind.VIEW))
        for mv in self.list_materialized_views(schema):
            fallback_assets.append((mv, AssetKind.MATERIALIZED_VIEW))
        fallback_assets.sort(key=lambda x: x[0])
        return fallback_assets

    def list_column_profiles(self, schema: str, table: str) -> list[ColumnProfile]:
        """Return column names/types/nullability without scanning table data."""
        schema = self._normalize_id(schema)
        table = self._normalize_id(table)
        insp = inspect(self.engine)
        try:
            raw_cols = insp.get_columns(table, schema=schema)
        except NoSuchTableError:
            # Code analysis routinely surfaces table names (e.g. SAP-style
            # ``sap_s6p.vbrk``) that exist in the referenced codebase but
            # not in the database AMX is connected to. Returning an empty
            # column list lets the analyze worker degrade gracefully
            # instead of crashing the whole code agent run.
            log.debug("list_column_profiles: %s.%s not present in live DB", schema, table)
            return []
        return [
            ColumnProfile(
                name=str(c["name"]),
                dtype=str(c["type"]),
                nullable=bool(c.get("nullable", True)),
            )
            for c in raw_cols
        ]

    def resolve_asset_kind(self, schema: str, name: str) -> AssetKind:
        """Determine whether *name* is a table, view, or materialized view."""
        schema = self._normalize_id(schema)
        name = self._normalize_id(name)
        tables = set(self.list_tables(schema))
        if name in tables:
            return AssetKind.TABLE
        views = set(self.list_views(schema))
        if name in views:
            return AssetKind.VIEW
        matviews = set(self.list_materialized_views(schema))
        if name in matviews:
            return AssetKind.MATERIALIZED_VIEW
        return AssetKind.TABLE

    # ── Comments (read) ───────────────────────────────────────────────────

    @property
    def _cache_profile_key(self) -> str:
        """Stable identifier the column-comments cache keys off.

        Prefers the AMX profile name when the caller supplied one
        (every CLI / web router code path does in v0.14+). Falls back
        to a short hash of the connection identity so anonymous
        connectors — e.g. tests that build a connector directly from a
        ``DBConfig`` — still get a deterministic, collision-free key.
        """
        if self.profile_name:
            return self.profile_name
        import hashlib

        sig = (
            f"{getattr(self.cfg, 'url', '') or ''}|"
            f"{getattr(self.cfg, 'database', '') or ''}|"
            f"{getattr(self.cfg, 'catalog', '') or ''}"
        )
        return "anon:" + hashlib.sha1(sig.encode("utf-8")).hexdigest()[:12]

    def _cache_database_key(self) -> str:
        """Database scope the cache narrows on within a profile."""
        return str(getattr(self.cfg, "database", "") or getattr(self.cfg, "catalog", "") or "")

    def _lookup_column_comments_cache(self, schema: str, table: str) -> dict[str, Any] | None:
        """Return a cached ``{table_comment, columns, kind, ...}`` or None."""
        try:
            from amx.storage.sqlite_store import history_store
        except Exception:
            return None
        store = history_store()
        if store is None:
            return None
        try:
            return store.lookup_column_comments_cache(
                db_profile=self._cache_profile_key,
                database=self._cache_database_key(),
                schema=schema,
                table=table,
            )
        except Exception as exc:
            log.debug("column-comments cache lookup failed: %s", exc)
            return None

    def _save_column_comments_cache(
        self,
        schema: str,
        entries: dict[str, dict[str, Any]],
        *,
        bulk_filled: bool = False,
    ) -> None:
        """Persist a per-schema metadata dict to the cache.

        ``bulk_filled`` is ``True`` only when the entries dict came
        from the adapter's bulk source — i.e. covers every table in
        the schema. Per-table fallback writes pass ``False`` so the
        cache row is usable for column-comment short-circuits but not
        for ``list_assets``.
        """
        if not entries:
            return
        try:
            from amx.storage.sqlite_store import history_store
        except Exception:
            return
        store = history_store()
        if store is None:
            return
        try:
            store.save_column_comments_cache(
                db_profile=self._cache_profile_key,
                database=self._cache_database_key(),
                schema=schema,
                entries=entries,
                bulk_filled=bulk_filled,
            )
        except Exception as exc:
            log.debug("column-comments cache save failed: %s", exc)

    def _schema_bulk_cache_is_fresh(self, schema: str) -> bool:
        """Cheap check used by ``list_assets`` before re-running SHOW TABLES."""
        try:
            from amx.storage.sqlite_store import history_store
        except Exception:
            return False
        store = history_store()
        if store is None:
            return False
        try:
            return store.schema_has_bulk_filled_cache(
                db_profile=self._cache_profile_key,
                database=self._cache_database_key(),
                schema=schema,
            )
        except Exception:
            return False

    def _lookup_column_comments_cache_bulk(self, schema: str) -> dict[str, dict[str, Any]]:
        try:
            from amx.storage.sqlite_store import history_store
        except Exception:
            return {}
        store = history_store()
        if store is None:
            return {}
        try:
            return store.lookup_column_comments_cache_bulk(
                db_profile=self._cache_profile_key,
                database=self._cache_database_key(),
                schema=schema,
            )
        except Exception:
            return {}

    def invalidate_column_comments_cache(
        self,
        schema: str | None = None,
        table: str | None = None,
        *,
        match_any_database: bool = False,
    ) -> None:
        """Drop cached comment rows for this connector.

        - ``schema`` + ``table`` → single row (one table COMMENT write).
        - ``schema`` only → whole schema (schema COMMENT write).
        - Both None → whole profile (database COMMENT or wholesale reset).

        Schema-level invalidations also wipe the matching
        ``schemas_cache`` row so a re-read of ``get_schema_comment``
        sees the fresh value, not the pre-write copy.

        ``match_any_database=True`` widens the cache delete to ignore
        ``database_name``. Used by the apply path because pending
        entries don't carry the originating ``database`` / ``catalog``
        scope: the apply worker falls back to active-profile defaults
        whose cache key can differ from the one Studio's snapshot
        endpoint populated when the user navigated to a non-pinned DB.
        """
        try:
            from amx.storage.sqlite_store import history_store
        except Exception:
            return
        store = history_store()
        if store is None:
            return
        try:
            store.invalidate_column_comments_cache(
                db_profile=self._cache_profile_key,
                database=self._cache_database_key(),
                schema=schema,
                table=table,
                match_any_database=match_any_database,
            )
        except Exception as exc:
            log.debug("column-comments cache invalidate failed: %s", exc)
        # Mirror the invalidation into the schemas_cache. Granularity
        # mapping: ``table`` writes don't touch schema metadata so we
        # skip them; ``schema`` writes wipe that one schema row; whole-
        # profile invalidations (both None) wipe everything in the
        # catalog cache too.
        if table is not None:
            return
        try:
            store.invalidate_schemas_cache(
                db_profile=self._cache_profile_key,
                database=self._cache_database_key(),
                catalog=str(getattr(self.cfg, "catalog", "") or ""),
                schema=schema,
            ) if schema is not None else store.invalidate_schemas_cache(
                db_profile=self._cache_profile_key,
            )
        except Exception as exc:
            log.debug("schemas cache invalidate failed: %s", exc)

    # ── schemas_cache helpers ─────────────────────────────────────────────

    def _lookup_schemas_cache(self, catalog: str, schema: str) -> dict[str, Any] | None:
        try:
            from amx.storage.sqlite_store import history_store
        except Exception:
            return None
        store = history_store()
        if store is None:
            return None
        try:
            return store.lookup_schemas_cache(
                db_profile=self._cache_profile_key,
                database=self._cache_database_key(),
                catalog=catalog,
                schema=schema,
            )
        except Exception:
            return None

    def _catalog_bulk_cache_is_fresh(self, catalog: str) -> bool:
        try:
            from amx.storage.sqlite_store import history_store
        except Exception:
            return False
        store = history_store()
        if store is None:
            return False
        try:
            return store.catalog_has_bulk_filled_cache(
                db_profile=self._cache_profile_key,
                database=self._cache_database_key(),
                catalog=catalog,
            )
        except Exception:
            return False

    def _list_schemas_from_cache(self, catalog: str) -> list[tuple[str, str | None]]:
        try:
            from amx.storage.sqlite_store import history_store
        except Exception:
            return []
        store = history_store()
        if store is None:
            return []
        try:
            return store.list_schemas_from_cache(
                db_profile=self._cache_profile_key,
                database=self._cache_database_key(),
                catalog=catalog,
            )
        except Exception:
            return []

    def _save_schemas_cache(
        self,
        catalog: str,
        entries: dict[str, str | None],
        *,
        bulk_filled: bool = False,
    ) -> None:
        if not entries:
            return
        try:
            from amx.storage.sqlite_store import history_store
        except Exception:
            return
        store = history_store()
        if store is None:
            return
        try:
            store.save_schemas_cache(
                db_profile=self._cache_profile_key,
                database=self._cache_database_key(),
                catalog=catalog,
                entries=entries,
                bulk_filled=bulk_filled,
            )
        except Exception as exc:
            log.debug("schemas cache save failed: %s", exc)

    def _populate_catalogs_cache(self, catalog: str) -> bool:
        """Run the adapter's ``bulk_catalog_metadata`` and cache it.

        Returns ``True`` when the adapter delivered a non-empty dict
        (every reachable schema in the catalog) and ``False`` when the
        backend has no bulk source — the caller then falls back to
        per-schema enumeration + per-schema comment lookups.
        """
        try:
            payload = self._adapter.bulk_catalog_metadata(self.engine, catalog)
        except Exception as exc:
            log.debug(
                "bulk_catalog_metadata raised on %s for catalog %r: %s",
                self._adapter.name,
                catalog,
                exc,
            )
            return False
        if not payload:
            return False
        self._save_schemas_cache(catalog, payload, bulk_filled=True)
        return True

    def _populate_schema_metadata_cache(self, schema: str) -> bool:
        """Run the adapter's bulk source for ``schema`` and cache results.

        Returns ``True`` when the adapter populated the cache (any
        backend that overrides :meth:`bulk_schema_metadata`) and
        ``False`` when there is no bulk source — the caller then falls
        back to per-table inspector calls. The fallback path also
        caches its results entry-by-entry, so subsequent reads of any
        cached table short-circuit either way.

        Long-first-fetch CLI affordance: when stdout is a TTY and no
        Rich Live region is already painting (so we are in an
        interactive command, not the middle of a ``/run`` orchestrator
        pipeline), the bulk query is wrapped in ``step_spinner`` so the
        user sees ``Fetching column descriptions for {schema}…  Xs``
        rather than a frozen prompt during the cold pull. Subsequent
        cache hits stay silent — there is no spinner because there is
        no DB hit.
        """
        catalog = str(getattr(self.cfg, "catalog", "") or "")
        spinner_ctx = None
        try:
            import sys as _sys
            import threading as _threading

            from amx.utils.console import is_quiet
            from amx.utils.live_display import get_display

            display = get_display()
            display_active = display is not None and getattr(display, "_live", None) is not None
            # Only paint a spinner when this call comes from the CLI's
            # foreground REPL — i.e. main thread, TTY, no Rich Live
            # region already active, and the thread-local quiet flag
            # the Studio worker installs is off. Without these guards
            # a Studio sidebar expand (which runs on a uvicorn worker
            # thread) was bleeding "Cached column descriptions for X"
            # lines into the user's CLI shell.
            on_main_thread = _threading.current_thread() is _threading.main_thread()
            if _sys.stdout.isatty() and not display_active and on_main_thread and not is_quiet():
                from amx.utils.console import step_spinner

                spinner_ctx = step_spinner(
                    f"Fetching column descriptions for {schema}",
                    done_message=f"Cached column descriptions for {schema}",
                )
        except Exception:
            spinner_ctx = None

        try:
            if spinner_ctx is not None:
                with spinner_ctx:
                    payload = self._adapter.bulk_schema_metadata(
                        self.engine, schema, catalog=catalog
                    )
            else:
                payload = self._adapter.bulk_schema_metadata(self.engine, schema, catalog=catalog)
        except Exception as exc:
            log.debug(
                "bulk_schema_metadata raised on %s for schema %s: %s",
                self._adapter.name,
                schema,
                exc,
            )
            return False
        if not payload:
            return False
        self._save_column_comments_cache(schema, payload, bulk_filled=True)
        return True

    def get_table_comment(self, schema: str, table: str) -> str | None:
        if not self.capabilities.table_comments and not self.capabilities.view_comments:
            return None
        schema = self._normalize_id(schema)
        table = self._normalize_id(table)
        cached = self._lookup_column_comments_cache(schema, table)
        if cached is not None:
            return cached.get("table_comment")
        # Try a single round-trip bulk source for the whole schema; on
        # success every sibling table is now warm in the cache too. If
        # the backend has no bulk source, drop to the per-table
        # inspector call below and cache its result entry-by-entry.
        if self._populate_schema_metadata_cache(schema):
            cached = self._lookup_column_comments_cache(schema, table)
            if cached is not None:
                return cached.get("table_comment")
        insp = inspect(self.engine)
        try:
            info = insp.get_table_comment(table, schema=schema)
            value = info.get("text")
        except Exception:
            value = None
        # Even on the per-table path we cache the result so the next
        # call within the TTL window skips the round-trip.
        self._save_column_comments_cache(
            schema,
            {table: {"table_comment": value, "columns": {}, "kind": "TABLE"}},
        )
        return value

    def get_column_comments(self, schema: str, table: str) -> dict[str, str | None]:
        if not self.capabilities.column_comments:
            return {}
        schema = self._normalize_id(schema)
        table = self._normalize_id(table)
        cached = self._lookup_column_comments_cache(schema, table)
        if cached is not None and cached.get("columns"):
            return dict(cached["columns"])
        if self._populate_schema_metadata_cache(schema):
            cached = self._lookup_column_comments_cache(schema, table)
            if cached is not None and cached.get("columns"):
                return dict(cached["columns"])
        insp = inspect(self.engine)
        cols = insp.get_columns(table, schema=schema)
        out = {c["name"]: c.get("comment") for c in cols}
        # Preserve any table-level comment we already cached for this
        # row so the columns fill doesn't blank it out.
        existing_tc = cached.get("table_comment") if cached else None
        self._save_column_comments_cache(
            schema,
            {table: {"table_comment": existing_tc, "columns": out, "kind": "TABLE"}},
        )
        return out

    def column_comments_probe_query(self, schema: str, table: str) -> str:
        return self._adapter.column_comments_probe_query(schema, table)

    def table_metadata_probe_query(self, schema: str, table: str) -> str:
        return self._adapter.table_metadata_probe_query(schema, table)

    def get_table_metadata_snapshot(self, schema: str, table: str) -> dict[str, Any]:
        columns = self.list_column_profiles(schema, table)
        comments = self.get_column_comments(schema, table)
        return {
            "schema": schema,
            "table": table,
            "table_comment": self.get_table_comment(schema, table) or "",
            "columns": [
                {
                    "name": column.name,
                    "dtype": column.dtype,
                    "nullable": column.nullable,
                    "comment": comments.get(column.name) or "",
                }
                for column in columns
            ],
        }

    def get_schema_comment(self, schema: str) -> str | None:
        if not self.capabilities.schema_comments:
            return None
        catalog = str(getattr(self.cfg, "catalog", "") or "")
        # Cache fast path: hit the schemas_cache before any DB call.
        cached = self._lookup_schemas_cache(catalog, schema)
        if cached is not None:
            return cached.get("schema_comment")
        # Cold path: bulk-fill the whole catalog in one shot so the
        # sidebar's per-schema loop becomes O(1) cache hits after the
        # first call. If the adapter has no bulk source, drop to the
        # legacy per-schema call and cache its single-row result.
        if self._populate_catalogs_cache(catalog):
            cached = self._lookup_schemas_cache(catalog, schema)
            if cached is not None:
                return cached.get("schema_comment")
        value = self._adapter.get_schema_comment(self.engine, schema)
        self._save_schemas_cache(catalog, {schema: value}, bulk_filled=False)
        return value

    def get_database_comment(self) -> str | None:
        if not self.capabilities.database_comments:
            return None
        return self._adapter.get_database_comment(self.engine)

    # ── Profiling ─────────────────────────────────────────────────────────
    def profile_table(
        self,
        schema: str,
        table: str,
        sample_size: int | None = None,
        asset_kind: AssetKind | None = None,
    ) -> TableProfile:
        from amx.db._column_profiler import profile_table

        return profile_table(self, schema, table, sample_size=sample_size, asset_kind=asset_kind)

    # The five ``_collect_*`` profiling helpers live in
    # :mod:`amx.db._column_profiler`. Tests that exercised them as
    # methods on ``DatabaseConnector`` keep working through these
    # thin delegators; ``profile_table`` itself calls the module-level
    # functions directly to avoid one hop per call inside the inner loop.

    def _collect_column_stats_and_samples(self, *args, **kwargs):
        from amx.db._column_profiler import collect_column_stats_and_samples

        return collect_column_stats_and_samples(self, *args, **kwargs)

    def _collect_bulk_stats(self, *args, **kwargs):
        from amx.db._column_profiler import collect_bulk_stats

        return collect_bulk_stats(self, *args, **kwargs)

    def _collect_per_column_stats_fallback(self, *args, **kwargs):
        from amx.db._column_profiler import collect_per_column_stats_fallback

        return collect_per_column_stats_fallback(self, *args, **kwargs)

    def _collect_bulk_samples(self, *args, **kwargs):
        from amx.db._column_profiler import collect_bulk_samples

        return collect_bulk_samples(self, *args, **kwargs)

    def _collect_per_column_samples(self, *args, **kwargs):
        from amx.db._column_profiler import collect_per_column_samples

        return collect_per_column_samples(self, *args, **kwargs)

    def profile_entities(
        self,
        schema: str,
        table: str,
        sample_size: int | None = None,
        asset_kind: AssetKind | None = None,
    ):
        """Return profiled metadata normalized to Universal Metadata Interface objects."""
        from amx.core.metadata import UniversalMetadataAdapter

        return UniversalMetadataAdapter.from_table_profile(
            self.profile_table(schema, table, sample_size=sample_size, asset_kind=asset_kind)
        )

    # ── Relationships ─────────────────────────────────────────────────────

    def get_incoming_foreign_keys(self, schema: str, table: str) -> list[dict[str, Any]]:
        if not self.capabilities.relationships:
            return []
        schema = self._normalize_id(schema)
        table = self._normalize_id(table)
        try:
            return self._adapter.get_incoming_foreign_keys(self.engine, schema, table)
        except Exception as exc:
            actionable = self._adapter.actionable_profile_error(exc) or actionable_error_message(
                exc, backend=self.backend
            )
            log.warning(
                "Incoming foreign key introspection failed for %s.%s via %s: %s",
                schema,
                table,
                self.backend,
                actionable or exc,
            )
            return []

    def get_related_table_comments(
        self,
        outgoing_fks: list[dict[str, Any]],
        incoming_fks: list[dict[str, Any]],
    ) -> list[dict[str, str]]:
        """Fetch comments for tables connected through FK relationships.

        Uses :meth:`amx.db.adapters.base.BaseAdapter.batch_get_table_comments`
        when the active adapter implements the hook (PostgreSQL today)
        so a tabel with N foreign-key neighbours costs one round-trip
        instead of N. Adapters that have not overridden the hook fall
        back to the per-table ``get_table_comment`` path; behaviour is
        unchanged for them.
        """
        related: set[tuple[str, str]] = set()
        for fk in outgoing_fks:
            rs = str(fk.get("referred_schema") or "")
            rt = str(fk.get("referred_table") or "")
            if rs and rt:
                related.add((rs, rt))
        for fk in incoming_fks:
            rs = str(fk.get("source_schema") or "")
            rt = str(fk.get("source_table") or "")
            if rs and rt:
                related.add((rs, rt))

        if not related:
            return []

        ordered_pairs = sorted(related)

        # Try the batch path first. ``batch_get_table_comments`` returns
        # ``None`` on adapters that have not opted in; a dict (possibly
        # empty) means the adapter handled the request and we should
        # not fall back per-table.
        comments_by_pair: dict[tuple[str, str], str | None] | None = None
        try:
            comments_by_pair = self._adapter.batch_get_table_comments(self.engine, ordered_pairs)
        except Exception as exc:
            # A misbehaving batch implementation must not break callers
            # — log and fall back to the historical per-table path.
            log.warning(
                "Adapter %s batch_get_table_comments failed (%s); falling back to per-table fetch.",
                type(self._adapter).__name__,
                exc,
            )
            comments_by_pair = None

        out: list[dict[str, str]] = []
        for rs, rt in ordered_pairs:
            if comments_by_pair is not None:
                cmt = comments_by_pair.get((rs, rt)) or ""
            else:
                cmt = self.get_table_comment(rs, rt) or ""
            out.append({"schema": rs, "table": rt, "comment": cmt})
        return out

    # ── Comments (write) ──────────────────────────────────────────────────

    def _execute_comment_sql(self, conn: Connection, stmt: str, comment: str) -> None:
        final_sql, params = self._adapter.comment_sql_with_params(stmt, comment)
        conn.execute(text(final_sql), params)

    def preview_comment_sql(
        self,
        *,
        schema: str,
        table: str = "",
        column: str | None = None,
        asset_kind: AssetKind = AssetKind.TABLE,
    ) -> str | None:
        """Return the SQL template that ``apply_comment`` would execute.

        Used by dry-run flows to show users what would be written
        without touching the database. Returns ``None`` when the
        active backend cannot accept a comment for the requested
        asset kind (mirrors the ``UnsupportedDatabaseOperation``
        branches in :meth:`apply_comment`).

        The returned string still contains the ``:cmt`` parameter
        placeholder — the comment text is intentionally not inlined
        so dry-run preview never has to escape user-provided strings
        into a SQL literal.
        """
        if asset_kind == AssetKind.SCHEMA:
            if not self.capabilities.schema_comments:
                return None
            return self._adapter.set_schema_comment_sql(schema)
        if asset_kind == AssetKind.DATABASE:
            if not self.capabilities.database_comments:
                return None
            return self._adapter.set_database_comment_sql()
        if column is None:
            keyword = asset_kind.comment_keyword
            if keyword not in self.capabilities.comment_asset_keywords:
                return None
            if asset_kind == AssetKind.VIEW and not self.capabilities.view_comments:
                return None
            if (
                asset_kind == AssetKind.MATERIALIZED_VIEW
                and not self.capabilities.materialized_view_comments
            ):
                return None
            return self._adapter.set_table_comment_sql(schema, table, keyword)
        # Column comment
        if not self.capabilities.column_comments:
            return None
        return self._adapter.set_column_comment_sql(schema, table, column)

    def apply_column_comments_batch(
        self,
        schema: str,
        table: str,
        comments: list[tuple[str, str]],
        *,
        conn: Connection | None = None,
    ) -> bool:
        if not self.capabilities.column_comments:
            raise UnsupportedDatabaseOperation(f"{self.backend} does not support column comments.")
        stmt = self._adapter.set_multi_column_comments_sql(schema, table, comments)
        if not stmt:
            return False
        if conn is None:
            with self.engine.begin() as local_conn:
                local_conn.execute(text(stmt))
        else:
            conn.execute(text(stmt))
        log.info("Set %d column comments on %s.%s", len(comments), schema, table)
        return True

    def apply_comment(
        self,
        *,
        schema: str,
        table: str = "",
        column: str | None = None,
        comment: str,
        asset_kind: AssetKind = AssetKind.TABLE,
        conn: Connection | None = None,
    ) -> None:
        if asset_kind == AssetKind.SCHEMA:
            if not self.capabilities.schema_comments:
                raise UnsupportedDatabaseOperation(
                    f"{self.backend} does not support schema comments."
                )
            stmt = self._adapter.set_schema_comment_sql(schema)
            if conn is None:
                with self.engine.begin() as local_conn:
                    self._execute_comment_sql(local_conn, stmt, comment)
            else:
                self._execute_comment_sql(conn, stmt, comment)
            log.info("Set comment on schema %s", schema)
            return

        if asset_kind == AssetKind.DATABASE:
            if not self.capabilities.database_comments:
                raise UnsupportedDatabaseOperation(
                    f"{self.backend} does not support database comments."
                )
            stmt = self._adapter.set_database_comment_sql()
            if conn is None:
                with self.engine.begin() as local_conn:
                    self._execute_comment_sql(local_conn, stmt, comment)
            else:
                self._execute_comment_sql(conn, stmt, comment)
            log.info("Set comment on database")
            return

        if column is None:
            keyword = asset_kind.comment_keyword
            if keyword not in self.capabilities.comment_asset_keywords:
                raise UnsupportedDatabaseOperation(
                    f"{self.backend} does not support comment write-back for {asset_kind.label} assets."
                )
            if asset_kind == AssetKind.VIEW and not self.capabilities.view_comments:
                raise UnsupportedDatabaseOperation(
                    f"{self.backend} does not support view comments."
                )
            if (
                asset_kind == AssetKind.MATERIALIZED_VIEW
                and not self.capabilities.materialized_view_comments
            ):
                raise UnsupportedDatabaseOperation(
                    f"{self.backend} does not support materialized view comments."
                )
            if asset_kind == AssetKind.TABLE and not self.capabilities.table_comments:
                raise UnsupportedDatabaseOperation(
                    f"{self.backend} does not support table comments."
                )
            stmt = self._adapter.set_table_comment_sql(schema, table, keyword)
            if conn is None:
                with self.engine.begin() as local_conn:
                    self._execute_comment_sql(local_conn, stmt, comment)
            else:
                self._execute_comment_sql(conn, stmt, comment)
            log.info("Set comment on %s.%s (%s)", schema, table, asset_kind.label)
            return

        if not self.capabilities.column_comments:
            raise UnsupportedDatabaseOperation(f"{self.backend} does not support column comments.")
        stmt = self._adapter.set_column_comment_sql(schema, table, column)
        if conn is None:
            with self.engine.begin() as local_conn:
                self._execute_comment_sql(local_conn, stmt, comment)
        else:
            self._execute_comment_sql(conn, stmt, comment)
        log.info("Set comment on %s.%s.%s", schema, table, column)

    def set_table_comment(
        self,
        schema: str,
        table: str,
        comment: str,
        asset_kind: AssetKind = AssetKind.TABLE,
    ) -> None:
        self.apply_comment(schema=schema, table=table, comment=comment, asset_kind=asset_kind)

    def set_column_comment(self, schema: str, table: str, column: str, comment: str) -> None:
        self.apply_comment(schema=schema, table=table, column=column, comment=comment)

    def set_schema_comment(self, schema: str, comment: str) -> None:
        self.apply_comment(schema=schema, comment=comment, asset_kind=AssetKind.SCHEMA)

    def set_database_comment(self, comment: str) -> None:
        self.apply_comment(schema="", comment=comment, asset_kind=AssetKind.DATABASE)

    # ── Remote executable assets ──────────────────────────────────────────
    #
    # Adapter methods take ``engine`` as the first positional argument for
    # signature uniformity (matches ``list_tables(engine, schema, catalog)``).
    # Snowflake's implementations issue SQL via ``engine``; Databricks' use
    # an HTTP Workspace client and ignore it. The passthrough hands the
    # connector's live engine to both so neither variant raises ``TypeError``.

    def list_remote_notebooks(self, *, external_id_filter=None):
        return self._adapter.list_remote_notebooks(
            self.engine, external_id_filter=external_id_filter
        )

    def fetch_remote_notebook_source(self, external_id: str) -> str:
        return self._adapter.fetch_remote_notebook_source(self.engine, external_id)

    def list_remote_jobs(self, *, runs_per_job: int = 20, external_id_filter=None):
        return self._adapter.list_remote_jobs(
            self.engine,
            runs_per_job=runs_per_job,
            external_id_filter=external_id_filter,
        )

    def list_remote_pipelines(self, *, external_id_filter=None):
        return self._adapter.list_remote_pipelines(
            self.engine, external_id_filter=external_id_filter
        )

    def list_remote_streamlit_apps(self, *, external_id_filter=None):
        return self._adapter.list_remote_streamlit_apps(
            self.engine, external_id_filter=external_id_filter
        )

    def list_remote_streams(self, *, external_id_filter=None):
        return self._adapter.list_remote_streams(self.engine, external_id_filter=external_id_filter)

    def list_remote_task_dependencies(self):
        return self._adapter.list_remote_task_dependencies(self.engine)

    def list_remote_queries(self, *, history_days: int = 7, limit: int = 1000):
        return self._adapter.list_remote_queries(
            self.engine, history_days=history_days, limit=limit
        )

    # PR-A: metadata-only listings power the Studio / CLI
    # "browse and pick" wizard. Adapters that don't implement a
    # given kind raise ``AttributeError`` — callers should check
    # ``capabilities`` (or catch) before invoking. Each method
    # yields ``AssetMetadata`` rows: cheap identity only, no
    # content fetch.

    def list_workspace_children(self, *, parent_path: str, kind: str):
        """PR-E lazy discover — yield immediate children of ``parent_path``."""
        return self._adapter.list_workspace_children(
            self.engine, parent_path=parent_path, kind=kind
        )

    def list_remote_notebooks_metadata(self):
        return self._adapter.list_remote_notebooks_metadata(self.engine)

    def list_remote_jobs_metadata(self):
        return self._adapter.list_remote_jobs_metadata(self.engine)

    def list_remote_pipelines_metadata(self):
        return self._adapter.list_remote_pipelines_metadata(self.engine)

    def list_remote_streamlit_apps_metadata(self):
        return self._adapter.list_remote_streamlit_apps_metadata(self.engine)

    def list_remote_streams_metadata(self):
        return self._adapter.list_remote_streams_metadata(self.engine)

    # ── Adapter metadata ──────────────────────────────────────────────────

    @property
    def stats_label(self) -> str:
        """Human-readable label for the stats source (passed to LLM prompts)."""
        return self._adapter.stats_label()

    # ── Cleanup ───────────────────────────────────────────────────────────

    def close(self) -> None:
        if self._engine:
            self._engine.dispose()
