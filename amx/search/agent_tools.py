"""Tool definitions for the tool-calling ``/ask`` agent.

The tool-calling agent (``amx/search/tool_agent.py``) hands the LLM a small
fixed set of tools that wrap the existing catalog / live-DB / SchemaExplorer
infrastructure. The LLM picks which tool to call (and with what arguments)
to answer the user's question — instead of us trying to classify the
question via regex up-front.

Each tool is described by:
* ``name``      — JSON-schema friendly identifier (snake_case) the LLM emits.
* ``schema``    — OpenAI-compatible function-calling JSON schema.
* ``run(args)`` — Python callable invoked when the LLM picks this tool.

The ``ToolBox`` class holds the catalog/DB references and exposes ``schemas``
(passed to the LLM) plus ``invoke(name, json_args_string)``.
"""

from __future__ import annotations

import contextlib
import json
from collections.abc import Callable
from dataclasses import replace as _dc_replace
from difflib import SequenceMatcher
from typing import Any

from amx.config import AMXConfig
from amx.db.connector import DatabaseConnector, ProfilingError
from amx.search._agent_tools_helpers import (
    _description_proximity,
    _dtype_compat_score,
    _name_overlap_score,
    _safe_json,
    _sample_distinct_values,
    _ToolError,
)
from amx.search._tool_history import _HistoryToolsMixin
from amx.search._tool_join_inference import _JoinInferenceMixin
from amx.search._tool_rag import _RagToolsMixin
from amx.search._tool_scd_and_role import _ScdAndRoleMixin
from amx.search._tool_schemas import tool_schemas as _tool_schemas
from amx.search.catalog import SearchCatalog
from amx.utils.logging import get_logger

log = get_logger("search.agent_tools")

# ``json`` and ``SequenceMatcher`` stay imported above because the
# ``ToolBox`` methods below still reach for them directly. The pure
# helpers (``_name_overlap_score``, ``_dtype_compat_score``,
# ``_description_proximity``, ``_safe_json``, and the ``_ToolError``
# sentinel) live in ``_agent_tools_helpers`` and are re-exported here
# so historical imports — ``from amx.search.agent_tools import
# _name_overlap_score`` — keep working without forcing every caller
# to chase the internal split.
__all__ = (
    "_ToolError",
    "_description_proximity",
    "_dtype_compat_score",
    "_name_overlap_score",
    "_safe_json",
    "ToolBox",
)


class _CacheBackedTableProfile:
    """Minimal ``TableProfile`` stand-in for cache-served describe_table.

    The catalog / 24h cache stores ``(column list, row_count,
    table_comment)`` — enough to reconstruct the response shape
    ``_tool_describe_table`` builds. We DON'T have analytics
    metadata (partition keys, indexes, storage_bytes, …) because
    those come from a live backend probe; the response surface
    leaves the ``analytics`` block empty in that case, which the
    LLM already knows means "this backend / cache doesn't carry
    that signal".
    """

    __slots__ = ("columns", "row_count", "existing_comment", "analytics")

    def __init__(self, *, columns: list[dict], row_count: int, table_comment: str) -> None:
        # Recreate the lightweight ``ColumnProfile`` shape the live
        # path produces: each entry exposes ``.name``, ``.dtype``,
        # ``.nullable``, ``.existing_comment``.
        from types import SimpleNamespace as _NS

        self.columns = [
            _NS(
                name=str(c.get("name", "")),
                dtype=str(c.get("type") or c.get("dtype") or ""),
                nullable=bool(c.get("nullable", True)),
                existing_comment=str(c.get("comment") or ""),
            )
            for c in (columns or [])
        ]
        self.row_count = int(row_count or 0)
        self.existing_comment = table_comment or ""
        self.analytics = None


class ToolBox(_HistoryToolsMixin, _JoinInferenceMixin, _RagToolsMixin, _ScdAndRoleMixin):
    """Concrete tool implementations the agent loop dispatches into."""

    # Tools that must never be served from the in-question cache.
    # Empty by default — every visible tool is a pure read (no DDL, no
    # side-effecting writes), and the LLM agent loop runs all of them
    # against the same point-in-time database state for the duration
    # of a single /ask question. Listed names are skipped at the
    # ``invoke()`` cache layer; subclasses or future tools that mutate
    # state should add their name here.
    _UNCACHED_TOOLS: frozenset[str] = frozenset()

    #: Default for ``_allow_live_refresh`` — class-level so test
    #: harnesses that ``object.__new__(ToolBox)`` past ``__init__``
    #: still see the cache-only contract instead of an AttributeError.
    _allow_live_refresh: bool = False

    #: Per-profile live-DB tool fan-out timeout. If a single profile
    #: takes longer than this to respond, its result is dropped from
    #: the union and the LLM is told the profile timed out — other
    #: profiles still come back. Picked deliberately well under the
    #: 12s hard ceiling for the whole question.
    _LIVE_FANOUT_TIMEOUT_SEC: float = 8.0
    #: Cap on parallel workers for the fan-out. Caps at 8 so a user
    #: with 30 profiles doesn't spawn 30 SQLAlchemy engines at once.
    _LIVE_FANOUT_MAX_WORKERS: int = 8

    def __init__(
        self,
        cfg: AMXConfig,
        catalog: SearchCatalog,
        *,
        db_factory: Callable[[], DatabaseConnector] | None = None,
        db_profiles: list[str] | tuple[str, ...] | None = None,
        db_connectors: dict[str, DatabaseConnector] | None = None,
        doc_profiles: list[str] | tuple[str, ...] | None = None,
        code_profiles: list[str] | tuple[str, ...] | None = None,
        allow_live_refresh: bool = False,
    ) -> None:
        self.cfg = cfg
        self.catalog = catalog
        # Per-question cache-only gate. When False (default), every tool
        # that exposes a ``force_fresh`` argument silently coerces it to
        # False so the LLM cannot bypass the catalog cache and trigger
        # an unauthorised live-DB read. Flipped to True from the Ask UI
        # "Live refresh" toggle.
        self._allow_live_refresh: bool = bool(allow_live_refresh)
        # Resolve the multi-profile retrieval scope.
        # ``db_profiles`` (caller-supplied) > ``cfg.active_db_profiles`` (the
        # 0.11.0 multi-pick scope) > legacy single-active fallback. Anchor
        # ``self.db_profile`` to the first entry so legacy single-profile
        # call-sites that read it still resolve to a valid name; full
        # multi-profile callers use ``self.db_profile_filter`` instead.
        configured: list[str] = []
        if db_profiles:
            configured = [str(name).strip() for name in db_profiles if str(name).strip()]
        elif callable(getattr(cfg, "effective_db_profiles", None)):
            try:
                configured = [
                    str(name).strip() for name in cfg.effective_db_profiles() if str(name).strip()
                ]
            except Exception:
                configured = []
        if not configured:
            fallback = (cfg.active_db_profile or "default").strip() or "default"
            configured = [fallback]
        # Dedupe while preserving order.
        seen: set[str] = set()
        scope: list[str] = []
        for name in configured:
            if name in seen:
                continue
            seen.add(name)
            scope.append(name)
        self.db_profiles: list[str] = scope
        self.db_profile: str = scope[0]  # anchor for legacy single-profile reads
        # Explicit doc/code profile overrides. ``None`` (default) keeps
        # the historic auto-resolution from the DB scope via the link
        # maps; an explicit list (incl. ``[]``) lets Studio + the CLI
        # decouple "what docs apply" from the DB choice — the user can
        # opt into 2 doc profiles regardless of which DBs are in scope,
        # or opt OUT of doc retrieval entirely while still asking
        # DB-grounded questions.
        self._doc_profiles_override: list[str] | None = (
            [str(name).strip() for name in doc_profiles if str(name).strip()]
            if doc_profiles is not None
            else None
        )
        self._code_profiles_override: list[str] | None = (
            [str(name).strip() for name in code_profiles if str(name).strip()]
            if code_profiles is not None
            else None
        )
        self._db_factory = db_factory or (lambda: DatabaseConnector(cfg.db))
        # Only build the live DB connector lazily — many tools never need it.
        self._db: DatabaseConnector | None = None
        # Multi-profile live-DB fan-out: per-profile connectors keyed by
        # name. Populated lazily on first tool call that targets a
        # specific profile. Caller can prime the dict via
        # ``db_connectors`` (Studio's _CONNECTOR_CACHE bridges through
        # this so the SPA's browse-side warm engines are reused). All
        # connectors are disposed in ``close()`` regardless of source.
        self._connectors: dict[str, DatabaseConnector] = dict(db_connectors or {})
        self._owned_connectors: set[str] = set()
        # In-question tool memoization. The 6-iteration LLM loop can
        # call ``describe_table(foo)`` three times in one question
        # (LLM thinks → calls describe_table → reads response → thinks
        # → forgets it already had it → calls describe_table again).
        # Caching by (tool_name, args) inside one ToolBox lifetime
        # collapses the 2nd and 3rd call to a free memory lookup.
        # ToolBox is instantiated per /ask question so the cache never
        # outlives a single point-in-time view of the database.
        # The cache key includes the tuple of profiles — a multi-profile
        # call's results are NOT interchangeable with a single-profile
        # call's results, so they must miss when the scope changes.
        self._tool_cache: dict[tuple[str, str, tuple[str, ...]], str] = {}
        self._tool_cache_hits: int = 0

    @property
    def db_profile_filter(self) -> str | list[str]:
        """Return the scope in the form catalog tools accept.

        Catalog methods type their ``db_profile`` parameter as
        ``DBProfileFilter = str | Sequence[str]``. We hand back a scalar
        for the single-profile case (so the SQL stays ``db_profile = ?``,
        cheap path) and a list for multi-profile (which expands to
        ``db_profile IN (?, ?, ?)``).
        """
        if len(self.db_profiles) <= 1:
            return self.db_profile
        return list(self.db_profiles)

    @property
    def is_multi_profile(self) -> bool:
        """``True`` when the scope spans 2+ profiles."""
        return len(self.db_profiles) > 1

    # ------------------------------------------------------------------ helpers
    def _gate_force_fresh(self, requested: bool) -> bool:
        """Honour ``force_fresh`` only when the user enabled "Live refresh".

        Ask is cache-only by default — the per-question UI toggle is the
        single source of truth for whether a tool call is permitted to
        bypass the catalog cache. When the toggle is OFF, the LLM may
        still pass ``force_fresh=true`` (it's documented in the tool
        schema for back-compat) but we silently coerce it to ``False``
        and log the suppression so the trace is auditable.
        """
        if not requested:
            return False
        if self._allow_live_refresh:
            return True
        try:
            log.debug(
                "force_fresh suppressed by cache-only Ask mode "
                "(allow_live_refresh=False)"
            )
        except Exception:
            pass
        return False

    def _live_db(self) -> DatabaseConnector:
        if self._db is None:
            self._db = self._db_factory()
        return self._db

    def _databases_to_sweep(self) -> list[str | None]:
        """Return the list of database names the live-DB tools should
        walk for the current profile.

        ``[None]`` (a single anonymous entry) means "use the active
        connector as-is" — the legacy single-database path. For an
        unpinned 2-level backend (PostgreSQL / MySQL / SQL Server with
        ``cfg.db.database == ''``) we enumerate every database the
        server exposes so a question like ``find_table_by_name vbrk``
        catches tables in ANY database, not just whatever the JDBC
        URL happened to default to.

        Three-level backends (Databricks, BigQuery) are unaffected:
        they use ``catalog`` and the catalog-level catalog tools
        already fan out via the catalog SQL clause.
        """
        # If the profile pins a database, the legacy single-DB path is
        # correct — nothing to fan out.
        pinned = str(getattr(self.cfg.db, "database", "") or "").strip()
        if pinned:
            return [None]
        # Catalog-style backends (3-level) keep the legacy path.
        try:
            if self._live_db().supports_catalogs():
                return [None]
        except Exception:
            return [None]
        # Unpinned 2-level backend: ask the connector for every visible
        # database. Empty list → caller falls back to the single
        # connection-time DB (preserves the old behaviour rather than
        # hiding the connector when ``list_databases`` is unsupported).
        # Reuse the 24h-TTL cache so a second /ask question within the
        # same day doesn't pay the live ``list_databases`` round-trip
        # again. Cold path writes back.
        cached = self._cached_databases_for_profile(self.db_profile)
        if cached:
            return list(cached)
        try:
            dbs = [d for d in self._live_db().list_databases() if d]
        except Exception:
            dbs = []
        if dbs:
            self._save_databases_for_profile(self.db_profile, dbs)
        return dbs or [None]

    def _connector_for_database(self, database: str | None) -> DatabaseConnector:
        """Return a connector overlaying ``database`` onto the active
        profile's DBConfig.

        ``database=None`` returns the legacy single-DB connector. A
        named database produces a fresh ``DBConfig`` via
        :func:`dataclasses.replace` so the original profile record
        stays untouched (no race against concurrent tool calls). The
        connector is cached in ``self._connectors`` under
        ``"<profile>::<database>"`` for the lifetime of the ToolBox so
        a per-DB fanout doesn't pay N engine builds per question.
        """
        if not database:
            return self._live_db()
        from dataclasses import replace as _replace

        key = f"{self.db_profile}::{database}"
        cached = self._connectors.get(key)
        if cached is not None:
            return cached
        base = self.cfg.db_profiles.get(self.db_profile)
        if base is None:
            return self._live_db()
        try:
            scoped_cfg = _replace(base, database=database)
        except Exception:
            return self._live_db()
        connector = DatabaseConnector(scoped_cfg, profile_name=self.db_profile)
        self._connectors[key] = connector
        self._owned_connectors.add(key)
        return connector

    # ── Cache-first metadata lookup ─────────────────────────────────────
    # Three layers, cheapest first. Every read tool consults this helper
    # before falling through to a live ``profile_table`` / ``list_*``
    # call. The user reported that an unpinned PostgreSQL profile incurred
    # ~500 live round-trips per ``/ask`` question (cross-DB fanout);
    # this helper closes that gap by reusing the catalog/cache work AMX
    # already does for sidebar exploration and ``/search sync``.

    _CACHED_DATABASE_LIST_TABLE = "__amx_database_list__"

    def _now(self) -> float:
        import time as _t

        return _t.time()

    def _history_store(self):
        """Return the history-store singleton or ``None`` when the
        process hasn't bootstrapped one. Both reads and writes degrade
        to "no cache" rather than raising — the agent must keep
        working on a fresh CLI session that never opened the store."""
        try:
            from amx.storage.factory import history_store as _hs

            return _hs()
        except Exception:
            return None

    def _resolve_table_metadata(
        self,
        *,
        db_profile: str,
        database: str,
        schema: str,
        table: str,
        force_fresh: bool = False,
    ) -> tuple[dict | None, str, float]:
        """Cheap-to-expensive lookup for table+column metadata.

        Returns ``(payload, source, age_seconds)``. ``payload`` matches
        the shape the live ``profile_table`` path produces:

            {
                "table_comment": str,
                "row_count": int,
                "columns": [{name, dtype, nullable, comment}, ...],
            }

        Plus an extra ``last_synced_at`` (ISO timestamp) when the
        catalog supplied it. ``source`` is one of:

            * ``"catalog"`` — read from ``catalog_entities`` (filled by
              ``/search sync``). No TTL; ``age_seconds`` is the time
              since the last sync so the LLM can hedge.
            * ``"live_cache"`` — read from ``run_context_cache`` (24h
              TTL). Filled by an earlier ``/run-apply`` or by a prior
              ``/ask`` write-back via :meth:`_writeback_table_metadata`.
            * ``"live"`` — no payload here; the caller will issue a
              live ``profile_table`` and call
              :meth:`_writeback_table_metadata` on success.

        ``force_fresh=True`` skips both caches and signals the caller
        to go live. Used when the user explicitly asks for the
        current state (post-apply, post-DDL, etc.).
        """
        if force_fresh:
            return (None, "live", 0.0)
        # ── Layer 1: catalog ──
        try:
            data = self.catalog.fetch_table_metadata(db_profile, schema, table)
        except Exception:
            data = None
        # Strict shape check — tests stub ``self.catalog`` as a bare
        # MagicMock which makes ``fetch_table_metadata`` return another
        # MagicMock (truthy but not a dict). Same hardening protects
        # against catalog rows with missing fields.
        if isinstance(data, dict) and isinstance(data.get("columns"), list):
            last_synced = float(data.get("last_synced_at") or 0.0)
            age = max(0.0, self._now() - last_synced) if last_synced > 0.0 else 0.0
            return (
                {
                    "table_comment": data.get("table_comment", ""),
                    "row_count": data.get("row_count", 0),
                    "columns": data.get("columns", []),
                    "last_synced_at": last_synced or None,
                },
                "catalog",
                age,
            )
        # ── Layer 2: 24h live cache ──
        # When the caller passes ``database=""`` (unpinned profile, no
        # cross-DB resolution yet), try BOTH the empty-db key AND the
        # cached database list. The write-back path keys rows under the
        # resolved database, so a follow-up unpinned lookup needs to
        # iterate the cached database names to find a hit.
        hs = self._history_store()
        cached = None
        if hs is not None:
            lookup_dbs: list[str] = [database or ""]
            if not (database or "").strip():
                extra = self._cached_databases_for_profile(db_profile) or []
                for d in extra:
                    if d and d not in lookup_dbs:
                        lookup_dbs.append(d)
            for cand_db in lookup_dbs:
                try:
                    cached = hs.lookup_run_context_cache(
                        db_profile=db_profile,
                        database=cand_db,
                        schema=schema,
                        table=table,
                    )
                except Exception:
                    cached = None
                if cached:
                    break
            if cached and isinstance(cached.get("payload"), dict):
                payload = cached["payload"]
                # Only reuse rows that carry a NON-EMPTY column list.
                # ``find_table_by_name`` writes ``{kind: "discovery",
                # columns: []}`` purely to remember the database the
                # table lives in — those rows should never satisfy a
                # describe_table request. The 24h cache slot for the
                # server-wide database list (``database_list_cache``)
                # is keyed under a different table name sentinel so it
                # never collides with real lookups.
                kind = str(payload.get("kind") or "")
                cols = payload.get("columns")
                if kind != "discovery" and isinstance(cols, list) and len(cols) > 0:
                    created_at = float(cached.get("created_at") or 0.0)
                    age = max(0.0, self._now() - created_at)
                    return (payload, "live_cache", age)
        return (None, "live", 0.0)

    def _writeback_table_metadata(
        self,
        *,
        db_profile: str,
        database: str,
        schema: str,
        table: str,
        payload: dict,
    ) -> None:
        """Persist a live ``profile_table`` result so the next /ask
        call lands on the 24h cache instead of paying the live round-
        trip again. /run-apply reads the same key shape, so the
        write-back is double-duty: an /ask describe_table primes a
        future re-run as well.

        When the agent ran against an unpinned profile (no explicit
        database from the LLM), we ALSO write a mirror row keyed under
        ``database=""``. Reason: a follow-up /ask question doesn't know
        which database the table lives in either, so its lookup uses
        ``database=""``. /run-apply still writes under the explicit
        database when it has one, so the legacy keying is preserved
        for that surface.
        """
        hs = self._history_store()
        if hs is None:
            return
        resolved_db = (database or "").strip()
        keys_to_write = {resolved_db}
        # Mirror under the empty-db key only when the profile is
        # unpinned — otherwise the explicit-db key IS the canonical
        # one and a duplicate would waste a row without helping any
        # lookup pattern.
        pinned_active_db = str(getattr(self.cfg.db, "database", "") or "").strip()
        if not pinned_active_db:
            keys_to_write.add("")
        for key in keys_to_write:
            try:
                hs.save_run_context_cache(
                    db_profile=db_profile,
                    database=key,
                    schema=schema,
                    table=table,
                    payload=payload,
                    source_run_id=None,
                    ttl_seconds=86400.0,
                )
            except Exception:
                # Cache writes are best-effort — never break the tool path.
                continue

    def _cached_databases_for_profile(self, db_profile: str) -> list[str] | None:
        """Return the cached server-wide database list for *db_profile*
        when fresh, otherwise ``None``. Cache slot is keyed under
        ``run_context_cache`` with the sentinel
        ``database='', schema='', table=<__amx_database_list__>`` —
        existing schema, no migration needed."""
        hs = self._history_store()
        if hs is None:
            return None
        try:
            cached = hs.lookup_run_context_cache(
                db_profile=db_profile,
                database="",
                schema="",
                table=self._CACHED_DATABASE_LIST_TABLE,
            )
        except Exception:
            return None
        if not cached or not isinstance(cached.get("payload"), dict):
            return None
        databases = cached["payload"].get("databases")
        if not isinstance(databases, list):
            return None
        out = [str(d) for d in databases if isinstance(d, str) and d]
        return out or None

    def _save_databases_for_profile(self, db_profile: str, databases: list[str]) -> None:
        hs = self._history_store()
        if hs is None or not databases:
            return
        try:
            hs.save_run_context_cache(
                db_profile=db_profile,
                database="",
                schema="",
                table=self._CACHED_DATABASE_LIST_TABLE,
                payload={"kind": "database_list", "databases": list(databases)},
                source_run_id=None,
                ttl_seconds=86400.0,
            )
        except Exception:
            pass

    def _connector_for_profile(self, profile: str) -> DatabaseConnector:
        """Return a (lazy) live-DB connector bound to *profile*'s DBConfig.

        Used by the multi-profile live-DB fan-out paths so each
        profile's `list_schemas()` / `list_tables()` call goes against
        its own SQLAlchemy engine. Connectors are cached for the
        lifetime of this ToolBox (one ``/ask`` question) and disposed
        in :meth:`close`.

        Falls back to the anchor connector when *profile* matches the
        anchor — keeps the cache small in the common single-profile
        case.
        """
        name = (profile or "").strip()
        if not name:
            return self._live_db()
        cached = self._connectors.get(name)
        if cached is not None:
            return cached
        if name == self.db_profile:
            # Anchor profile reuses the legacy ``self._db_factory``-built
            # connector so existing single-profile tools and the
            # multi-profile fan-out share one engine for the active row.
            connector = self._live_db()
            self._connectors[name] = connector
            return connector
        # Build a fresh DBConfig-bound connector for the requested
        # profile. The catalog-side multi-profile clause expansion
        # (PR-A) handled index queries; live-DB queries need a real
        # connection per backend, so each profile gets its own.
        base = self.cfg.db_profiles.get(name)
        if base is None:
            raise _ToolError(
                f"Unknown DB profile {name!r}; configured profiles: "
                f"{', '.join(sorted(self.cfg.db_profiles)) or '(none)'}"
            )
        connector = DatabaseConnector(base)
        self._connectors[name] = connector
        self._owned_connectors.add(name)
        return connector

    def _live_fanout(
        self,
        op: Callable[[DatabaseConnector], Any],
        *,
        profiles: list[str] | None = None,
    ) -> dict[str, dict[str, Any]]:
        """Run *op* against every profile in scope in parallel.

        Returns a dict keyed by profile name where each value is one
        of ``{"ok": True, "value": <op-result>}``,
        ``{"ok": False, "error": "<message>"}``, or
        ``{"ok": False, "timeout": True}``.

        The fan-out caps concurrency at
        :attr:`_LIVE_FANOUT_MAX_WORKERS` and times out individual
        per-profile calls at :attr:`_LIVE_FANOUT_TIMEOUT_SEC`. A slow
        profile NEVER blocks the others — its slot returns timeout
        and the caller surfaces a partial-results note for the LLM.
        """
        from concurrent.futures import ThreadPoolExecutor, wait
        from concurrent.futures import TimeoutError as _Timeout

        targets = list(profiles) if profiles else list(self.db_profiles)
        if not targets:
            return {}
        max_workers = min(self._LIVE_FANOUT_MAX_WORKERS, max(1, len(targets)))
        results: dict[str, dict[str, Any]] = {}

        def _runner(profile_name: str) -> dict[str, Any]:
            try:
                conn = self._connector_for_profile(profile_name)
            except _ToolError as exc:
                return {"ok": False, "error": str(exc)}
            try:
                value = op(conn)
            except Exception as exc:
                return {"ok": False, "error": f"{exc.__class__.__name__}: {exc}"}
            return {"ok": True, "value": value}

        with ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="amx-toolbox-fanout",
        ) as pool:
            future_map = {pool.submit(_runner, name): name for name in targets}
            done, not_done = wait(future_map, timeout=self._LIVE_FANOUT_TIMEOUT_SEC)
            for future in done:
                name = future_map[future]
                try:
                    results[name] = future.result(timeout=0)
                except _Timeout:
                    results[name] = {"ok": False, "timeout": True}
                except Exception as exc:
                    results[name] = {"ok": False, "error": str(exc)}
            for future in not_done:
                name = future_map[future]
                future.cancel()
                results[name] = {"ok": False, "timeout": True}
        return results

    def close(self) -> None:
        """Dispose the live DB connector. Each ``/ask`` question instantiates a
        fresh ``ToolBox``; without this call the SQLAlchemy engine + connection
        pool stay alive across REPL turns, leaking file descriptors until
        macOS / Linux ulimit kicks in (the user-reported
        ``OSError: [Errno 24] Too many open files`` after several turns).

        Multi-profile fan-out: every connector ToolBox owned (i.e.
        opened on demand for a non-anchor profile) is closed too. Caller-
        supplied connectors via ``db_connectors=`` are NOT closed —
        their lifetime belongs to the caller (Studio's connector cache).
        """
        if self._db is not None:
            with contextlib.suppress(Exception):
                self._db.close()
            self._db = None
        for name in list(self._owned_connectors):
            connector = self._connectors.pop(name, None)
            if connector is None:
                continue
            with contextlib.suppress(Exception):
                connector.close()
        self._owned_connectors.clear()

    def __enter__(self) -> ToolBox:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    # ------------------------------------------------------------------ schemas
    @staticmethod
    def schemas() -> list[dict[str, Any]]:
        """JSON schemas for every tool — passed to the LLM as the ``tools``
        parameter. Names are stable; argument names match the Python kwargs
        of the corresponding ``ToolBox`` method. Data lives in
        ``amx/search/_tool_schemas.py``; this method delegates so callers
        keep their existing call site (``ToolBox.schemas()``).

        Static method, returns the full unfiltered list. Use
        :meth:`available_schemas` from instance call sites so the
        cache-only ``allow_live_refresh`` filter is applied — that is
        the path ``run_tool_agent`` takes.
        """
        return _tool_schemas()

    def available_schemas(self) -> list[dict[str, Any]]:
        """Return the full schema list — live-only tools STAY visible.

        Previous revisions filtered ``freshness="live_only"`` entries
        when ``allow_live_refresh`` was False; that hid useful tool
        names from the LLM and forced it to invent vague fallback
        prose ("I don't have a direct tool that can…"). The current
        contract instead keeps every tool visible AND has
        :meth:`invoke` short-circuit cache-only mode with a
        structured ``needs_live_refresh`` envelope. The LLM sees the
        tool name + arguments it would have run, and can quote them
        verbatim to the user along with the one-click retry hint.
        """
        return self.schemas()

    def _is_live_only_tool(self, name: str) -> bool:
        """``True`` when the named tool is annotated ``live_only`` in
        the schema list — used by :meth:`invoke` as a belt-and-braces
        guard in case the LLM tries to call a tool it could not have
        seen in :meth:`available_schemas`."""
        for entry in _tool_schemas():
            if entry.get("function", {}).get("name") == name:
                return entry.get("freshness", "cache_ok") == "live_only"
        return False

    # ------------------------------------------------------------------ invoke
    def invoke(self, name: str, raw_arguments: str) -> str:
        """Dispatch a tool by name; return the result as a JSON string for the
        LLM. All tools return a string for direct embedding in the next
        ``role=tool`` message.

        Within one ToolBox lifetime (one /ask question) the same tool +
        arguments returns the cached result without re-running the
        handler — the LLM frequently re-asks for the same data across
        loop iterations and that's strictly wasteful. Errors are not
        cached so transient failures (network blip, transient
        permission denial) can be retried by the next LLM iteration.
        """
        try:
            args = json.loads(raw_arguments) if raw_arguments else {}
        except json.JSONDecodeError as exc:
            return _safe_json({"error": f"Invalid arguments JSON: {exc}"})

        # Cache-only mode: live-only tools stay in the menu but the
        # body never runs. Return a structured ``needs_live_refresh``
        # envelope so the LLM can surface the exact tool + arguments
        # back to the user along with the one-click retry hint. The
        # SPA reads ``needs_live_refresh: true`` from the tool.call
        # payload and renders an "Enable Live refresh & retry" button.
        if not self._allow_live_refresh and self._is_live_only_tool(name):
            log.info(
                "live-only tool %s blocked: allow_live_refresh=False — "
                "returning needs_live_refresh envelope",
                name,
            )
            return _safe_json(
                {
                    "needs_live_refresh": True,
                    "tool": name,
                    "arguments": args,
                    "reason": (
                        "This tool samples the live database; the "
                        "catalog cache cannot answer."
                    ),
                    "user_action": (
                        "Enable the 'Live refresh' toggle in the Ask "
                        "composer and ask the same question again — "
                        "the assistant will then call this tool."
                    ),
                    # Back-compat: tests pre-dating the envelope still
                    # check for ``error == "live_only_tool_disabled"``;
                    # keep the key so the freshness contract test and
                    # the CHANGELOG-era harness keep passing.
                    "error": "live_only_tool_disabled",
                }
            )

        # Cache lookup. JSON-stringify the args dict with sorted keys
        # so semantically-equivalent arg permutations
        # (e.g. {"a":1,"b":2} vs {"b":2,"a":1}) hash to the same key.
        # Falls through to the handler on TypeError if some arg isn't
        # JSON-serialisable (no caching, but no crash either).
        # The third element of the key is the (sorted) tuple of profiles
        # in scope; a multi-profile result MUST NOT be served to a
        # single-profile call or vice versa, so the scope tuple
        # disambiguates them.
        cache_key: tuple[str, str, tuple[str, ...]] | None = None
        if name not in self._UNCACHED_TOOLS:
            try:
                profile_key = tuple(sorted(self.db_profiles))
                cache_key = (
                    name,
                    json.dumps(args, sort_keys=True, default=str),
                    profile_key,
                )
                cached = self._tool_cache.get(cache_key)
                if cached is not None:
                    self._tool_cache_hits += 1
                    return cached
            except (TypeError, ValueError):
                cache_key = None  # un-cacheable args; just dispatch normally

        try:
            handler = getattr(self, f"_tool_{name}", None)
            if handler is None:
                return _safe_json({"error": f"Unknown tool: {name}"})
            payload = handler(**args)
            result = _safe_json(payload)
        except _ToolError as exc:
            # Errors are not cached — the next iteration may succeed.
            return _safe_json({"error": str(exc)})
        except Exception as exc:  # surface to LLM but don't crash
            return _safe_json({"error": f"Tool {name} failed: {exc}"})

        # Cache the success path only.
        is_error_payload = isinstance(payload, dict) and "error" in payload
        if cache_key is not None and not is_error_payload:
            self._tool_cache[cache_key] = result
        return result

    # ------------------------------------------------------------------ implementations
    @contextlib.contextmanager
    def _scoped_catalog(self, db: DatabaseConnector, catalog: str | None):
        """Temporarily pin ``cfg.catalog`` so connector methods route by it.

        Used by ``list_schemas`` / ``list_tables_in_schema`` when the LLM
        passes a catalog argument to drill into a Unity-Catalog catalog
        the active profile has not pinned. Empty / None ``catalog`` is a
        no-op so callers can pass through unconditionally.
        """
        cat = (catalog or "").strip()
        if not cat:
            yield
            return
        cfg = getattr(db, "cfg", None)
        if cfg is None:
            yield
            return
        previous = getattr(cfg, "catalog", "")
        try:
            cfg.catalog = cat
            yield
        finally:
            cfg.catalog = previous

    # Names that are system / built-in catalogs across the catalog-aware
    # backends AMX supports. Filtered out before the LLM sees the catalog
    # list so a Databricks workspace with one user catalog and three
    # system catalogs auto-routes instead of asking the user to pick.
    # Lower-cased — we compare case-insensitively.
    _SYSTEM_CATALOG_NAMES: frozenset[str] = frozenset(
        {
            # Databricks Unity Catalog system catalogs.
            "system",
            "samples",
            "workspace",
            "hive_metastore",
            "spark_catalog",
            "__databricks_internal",
        }
    )

    @classmethod
    def _user_catalogs(cls, catalogs: list[str]) -> list[str]:
        """Drop well-known system / built-in catalogs from a candidate list.

        Used to disambiguate the no-catalog-pinned auto-pick path: if
        the only non-system catalog is a user catalog, we can route
        listings to it without asking the LLM to pick.
        """
        return [c for c in catalogs if c and c.lower() not in cls._SYSTEM_CATALOG_NAMES]

    def _resolve_catalog_or_autopick(
        self,
        db: DatabaseConnector,
        explicit: str,
    ) -> tuple[str, list[str], list[str]]:
        """Pick a catalog for catalog-scoped operations.

        Returns ``(catalog, user_catalogs, all_catalogs)``:
        - ``catalog`` — what the caller should scope to, or ``""``.
        - ``user_catalogs`` — the system-filtered catalog list.
        - ``all_catalogs`` — the full live list (for surfacing).

        Logic:
        1. ``explicit`` (the LLM-passed argument) wins.
        2. The active profile's pinned catalog wins next.
        3. Otherwise, when the backend supports catalogs and exactly one
           user catalog is visible, auto-pick it. The user-reported case
           "/ask which tables do we have" on a profile saved without a
           catalog used to fail at ``list_tables_in_schema`` because that
           tool didn't run the auto-pick — the SQL emitted ``None.<schema>``
           and the warehouse rejected it. Centralising the logic here
           means every catalog-scoped tool gets the same behaviour.
        4. Otherwise return ``""`` and let the caller surface the list.

        ``explicit`` and the pinned value are returned verbatim — no
        membership check against the live catalog list, so "type a
        custom catalog" still works on a slow / permission-restricted
        workspace.
        """
        explicit = (explicit or "").strip()
        if explicit:
            return explicit, [], []
        pinned = str(getattr(self.cfg.db, "catalog", "") or "").strip()
        if pinned:
            return pinned, [], []
        try:
            supports = bool(db.supports_catalogs())
        except Exception:
            supports = False
        if not supports:
            return "", [], []
        try:
            all_catalogs = [str(c) for c in db.list_catalogs()]
        except Exception:
            return "", [], []
        user_catalogs = self._user_catalogs(all_catalogs)
        if len(user_catalogs) == 1:
            return user_catalogs[0], user_catalogs, all_catalogs
        return "", user_catalogs, all_catalogs

    # 3-level backends use a (catalog, schema, table) namespace; everything
    # else is 2-level (database, schema, table). The /ask tool uses this to
    # decide whether an unpinned profile is operating in "browse the server"
    # mode (2-level, common) vs "auto-pick the only user catalog" (3-level).
    _THREE_LEVEL_BACKENDS: frozenset[str] = frozenset({"databricks", "bigquery"})

    def _profile_unpinned_two_level(self, profile: str) -> bool:
        """``True`` when *profile* is a 2-level backend with no database pinned.

        That state is fully supported (Studio's browse sidebar handles it
        via per-database lazy loading), but the live-DB tools must NOT
        silently fall through to the maintenance database — they would
        list ``public`` from ``postgres`` (the bootstrap DB), which is
        almost never what the user wants. Detected at metadata level so
        we can refuse-with-hint before opening a connection on the wrong
        database.
        """
        base = self.cfg.db_profiles.get(profile)
        if base is None:
            return False
        backend = (str(getattr(base, "backend", "") or "")).lower()
        if backend in self._THREE_LEVEL_BACKENDS:
            return False
        pinned_db = str(getattr(base, "database", "") or "").strip()
        return not pinned_db

    def _list_schemas_on_profile(
        self,
        profile: str,
        *,
        catalog: str = "",
    ) -> dict[str, Any]:
        """Run ``list_schemas()`` against a specific named profile.

        Used by the targeted single-profile dispatch path (LLM passed
        ``db_profile=X``) and as the per-profile worker function for
        the multi-profile fan-out. Returns the same payload shape as
        the top-level tool with ``db_profile`` added.

        Every payload carries ``pinned_database`` + ``pinned_catalog`` so
        the LLM can see EACH profile's pinned scope independently — that
        defends against cross-profile name bleed (one profile's pinned
        catalog being applied to another profile's row).
        """
        base = self.cfg.db_profiles.get(profile)
        pinned_database = str(getattr(base, "database", "") or "").strip() if base else ""
        pinned_catalog = str(getattr(base, "catalog", "") or "").strip() if base else ""
        # 2-level profile with no DB pinned: refuse with a hint instead of
        # falling through to the bootstrap DB. The LLM should call
        # list_databases(with_counts=true) for the cross-DB rollup, or
        # narrow with database= per call.
        if self._profile_unpinned_two_level(profile):
            return {
                "db_profile": profile,
                "pinned_database": pinned_database or None,
                "pinned_catalog": pinned_catalog or None,
                "unpinned": True,
                "error": (
                    "This 2-level profile has no database pinned. Call "
                    "list_databases(with_counts=true) to enumerate every reachable "
                    "database with schema/table rollups, or pass `database=` to scope "
                    "this listing to one database."
                ),
                "schemas": [],
                "count": 0,
            }
        try:
            db = self._connector_for_profile(profile)
        except _ToolError as exc:
            return {
                "db_profile": profile,
                "pinned_database": pinned_database or None,
                "pinned_catalog": pinned_catalog or None,
                "error": str(exc),
                "schemas": [],
                "count": 0,
            }
        explicit = (catalog or "").strip()
        cat_arg = explicit or pinned_catalog
        try:
            with self._scoped_catalog(db, cat_arg):
                schemas = [str(s) for s in db.list_schemas()]
        except Exception as exc:
            return {
                "db_profile": profile,
                "pinned_database": pinned_database or None,
                "pinned_catalog": pinned_catalog or None,
                "error": f"{exc.__class__.__name__}: {exc}",
                "schemas": [],
                "count": 0,
            }
        database = (
            cat_arg
            or pinned_database
            or pinned_catalog
            or (getattr(base, "project", "") or "")
            or "(no database pinned)"
        )
        payload: dict[str, Any] = {
            "db_profile": profile,
            "pinned_database": pinned_database or None,
            "pinned_catalog": pinned_catalog or None,
            "database": database,
            "schemas": schemas,
            "count": len(schemas),
        }
        if cat_arg:
            payload["catalog"] = cat_arg
        return payload

    def _list_tables_on_profile(
        self,
        profile: str,
        schema: str,
        *,
        catalog: str = "",
    ) -> dict[str, Any]:
        """Run ``list_tables(schema)`` (or ``list_assets``) against a
        named profile. Same payload shape as the top-level tool with
        ``db_profile`` added for cross-profile rendering.

        Every payload carries ``pinned_database`` + ``pinned_catalog`` so
        the LLM can see EACH profile's pinned scope independently and
        won't bleed one profile's catalog onto another's row.
        """
        base = self.cfg.db_profiles.get(profile)
        pinned_database = str(getattr(base, "database", "") or "").strip() if base else ""
        pinned_catalog = str(getattr(base, "catalog", "") or "").strip() if base else ""
        # 2-level + unpinned: refuse-with-hint rather than silently
        # listing the bootstrap DB's schemas.
        if self._profile_unpinned_two_level(profile):
            return {
                "db_profile": profile,
                "pinned_database": pinned_database or None,
                "pinned_catalog": pinned_catalog or None,
                "schema": schema,
                "found": False,
                "unpinned": True,
                "error": (
                    "This 2-level profile has no database pinned. Call "
                    "list_databases(with_counts=true) to see what's reachable, "
                    "or pass `database=` to scope this listing to one database."
                ),
                "tables": [],
                "count": 0,
            }
        try:
            db = self._connector_for_profile(profile)
        except _ToolError as exc:
            return {
                "db_profile": profile,
                "pinned_database": pinned_database or None,
                "pinned_catalog": pinned_catalog or None,
                "schema": schema,
                "found": False,
                "error": str(exc),
                "tables": [],
                "count": 0,
            }
        cat_arg = (catalog or "").strip() or pinned_catalog
        try:
            with self._scoped_catalog(db, cat_arg):
                available = list(db.list_schemas())
                match = next((s for s in available if str(s).lower() == schema.lower()), None)
                if match is None:
                    return {
                        "db_profile": profile,
                        "pinned_database": pinned_database or None,
                        "pinned_catalog": pinned_catalog or None,
                        "schema": schema,
                        "catalog": cat_arg or None,
                        "found": False,
                        "available_schemas": [str(s) for s in available],
                    }
                items: list[dict[str, str]] = []
                if hasattr(db, "list_assets"):
                    for name, kind in db.list_assets(match):
                        items.append({"name": str(name), "kind": str(kind)})
                else:
                    for name in db.list_tables(match):
                        items.append({"name": str(name), "kind": "table"})
        except Exception as exc:
            return {
                "db_profile": profile,
                "pinned_database": pinned_database or None,
                "pinned_catalog": pinned_catalog or None,
                "schema": schema,
                "found": False,
                "error": f"{exc.__class__.__name__}: {exc}",
                "tables": [],
                "count": 0,
            }
        return {
            "db_profile": profile,
            "pinned_database": pinned_database or None,
            "pinned_catalog": pinned_catalog or None,
            "schema": match,
            "catalog": cat_arg or None,
            "found": True,
            "tables": items,
            "count": len(items),
        }

    def _fanout_list_tables_in_schema(self, schema: str, *, catalog: str = "") -> dict[str, Any]:
        """Parallel ``list_tables_in_schema`` across every profile in
        scope. Profiles where the schema doesn't exist surface as
        ``found: False`` with their visible schemas — the LLM can
        then suggest the closest match per profile.
        """
        from concurrent.futures import ThreadPoolExecutor, wait

        results: dict[str, dict[str, Any]] = {}
        max_workers = min(self._LIVE_FANOUT_MAX_WORKERS, max(1, len(self.db_profiles)))
        with ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="amx-toolbox-fanout-tables",
        ) as pool:
            future_map = {
                pool.submit(self._list_tables_on_profile, name, schema, catalog=catalog): name
                for name in self.db_profiles
            }
            done, not_done = wait(future_map, timeout=self._LIVE_FANOUT_TIMEOUT_SEC)
            for future in done:
                name = future_map[future]
                try:
                    results[name] = future.result(timeout=0)
                except Exception as exc:
                    results[name] = {
                        "db_profile": name,
                        "schema": schema,
                        "found": False,
                        "error": f"{exc.__class__.__name__}: {exc}",
                        "tables": [],
                        "count": 0,
                    }
            for future in not_done:
                name = future_map[future]
                future.cancel()
                results[name] = {
                    "db_profile": name,
                    "schema": schema,
                    "found": False,
                    "timeout": True,
                    "error": (f"timed out after {self._LIVE_FANOUT_TIMEOUT_SEC:.0f}s"),
                    "tables": [],
                    "count": 0,
                }
        total = sum(int(p.get("count") or 0) for p in results.values())
        found_in = [name for name, payload in results.items() if payload.get("found")]
        return {
            "multi_profile": True,
            "schema": schema,
            "scope": list(self.db_profiles),
            "found_in": found_in,
            "profiles": results,
            "total_tables": total,
        }

    def _fanout_list_schemas(self, *, catalog: str = "") -> dict[str, Any]:
        """Parallel ``list_schemas`` across every profile in scope.

        Returns ``{"profiles": {...per-profile payload...},
        "total_schemas", "profiles_with_errors"}``. Profiles that time
        out / error are surfaced explicitly so the LLM can mention
        which profiles answered and which didn't.
        """

        def _op(_conn: DatabaseConnector) -> dict[str, Any]:
            # Hand-off: the actual per-profile work needs the profile
            # NAME for catalog resolution, not just the connector. We
            # bind it via a wrapper below since fan-out passes the
            # connector positionally.
            return {}  # pragma: no cover — replaced by closure below

        results: dict[str, dict[str, Any]] = {}
        from concurrent.futures import ThreadPoolExecutor, wait

        max_workers = min(self._LIVE_FANOUT_MAX_WORKERS, max(1, len(self.db_profiles)))
        with ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="amx-toolbox-fanout-schemas",
        ) as pool:
            future_map = {
                pool.submit(self._list_schemas_on_profile, name, catalog=catalog): name
                for name in self.db_profiles
            }
            done, not_done = wait(future_map, timeout=self._LIVE_FANOUT_TIMEOUT_SEC)
            for future in done:
                name = future_map[future]
                try:
                    results[name] = future.result(timeout=0)
                except Exception as exc:
                    results[name] = {
                        "db_profile": name,
                        "error": f"{exc.__class__.__name__}: {exc}",
                        "schemas": [],
                        "count": 0,
                    }
            for future in not_done:
                name = future_map[future]
                future.cancel()
                results[name] = {
                    "db_profile": name,
                    "error": (
                        f"timed out after {self._LIVE_FANOUT_TIMEOUT_SEC:.0f}s — "
                        "this profile didn't respond. Other profiles below."
                    ),
                    "schemas": [],
                    "count": 0,
                    "timeout": True,
                }
        total = sum(int(p.get("count") or 0) for p in results.values())
        with_errors = [
            name
            for name, payload in results.items()
            if "error" in payload or payload.get("timeout")
        ]
        return {
            "multi_profile": True,
            "scope": list(self.db_profiles),
            "profiles": results,
            "total_schemas": total,
            "profiles_with_errors": with_errors,
        }

    def _tool_describe_column(
        self,
        schema: str,
        table: str,
        column: str,
        db_profile: str = "",
    ) -> dict[str, Any]:
        """Cache-served column description / dtype / flags lookup.

        Single ``catalog_entities`` row + description join — the agent
        used to chain ``describe_table`` and parse its column list to
        answer "is column X nullable?" / "what type is column Y?". This
        tool returns the same data in one SQLite read.
        """
        sch = (schema or "").strip()
        tbl = (table or "").strip()
        col = (column or "").strip()
        if not sch or not tbl or not col:
            raise _ToolError("'schema', 'table', and 'column' are all required.")
        target = (db_profile or "").strip()
        if target and target not in self.db_profiles:
            return {
                "schema": sch,
                "table": tbl,
                "column": col,
                "matches": [],
                "source": "catalog",
                "error": f"Profile {target!r} is not in this Ask's scope.",
            }
        scope: str | list[str] = target if target else self.db_profile_filter
        try:
            rows = self.catalog.fetch_column_detail(
                scope,
                schema_name=sch,
                table_name=tbl,
                column_name=col,
            )
        except Exception as exc:  # noqa: BLE001
            return {
                "schema": sch,
                "table": tbl,
                "column": col,
                "matches": [],
                "source": "catalog",
                "error": f"Catalog column lookup failed: {exc}",
            }
        if not rows:
            return {
                "schema": sch,
                "table": tbl,
                "column": col,
                "found": False,
                "matches": [],
                "source": "catalog",
                "cache_only": True,
                "hint": (
                    f"No column '{sch}.{tbl}.{col}' in the catalog for "
                    f"this scope. Enable Live refresh and ask again, or "
                    f"run /search sync."
                ),
            }
        return {
            "schema": sch,
            "table": tbl,
            "column": col,
            "found": True,
            "matches": rows,
            "count": len(rows),
            "source": "catalog",
        }

    def _tool_catalog_inventory(
        self,
        scope: str = "schemas",
        db_profile: str = "",
    ) -> dict[str, Any]:
        """Distinct databases / schemas from the catalog cache.

        Cache-served replacement for the live ``list_databases`` /
        ``list_server_databases`` paths when the user just wants the
        inventory, not a live re-enumeration. Counts come from
        ``catalog_entities`` — sub-50 ms regardless of profile
        size.
        """
        normalised = (scope or "schemas").strip().lower()
        if normalised not in {"databases", "schemas"}:
            normalised = "schemas"
        target = (db_profile or "").strip()
        if target and target not in self.db_profiles:
            return {
                "scope_arg": normalised,
                "rows": [],
                "profiles": list(self.db_profiles),
                "source": "catalog",
                "error": f"Profile {target!r} is not in this Ask's scope.",
            }
        clause: str | list[str] = target if target else self.db_profile_filter
        try:
            rows = self.catalog.fetch_inventory(clause, scope=normalised)
        except Exception as exc:  # noqa: BLE001
            return {
                "scope_arg": normalised,
                "rows": [],
                "profiles": list(self.db_profiles),
                "source": "catalog",
                "error": f"Catalog inventory query failed: {exc}",
            }
        return {
            "scope_arg": normalised,
            "rows": rows,
            "profiles": list(self.db_profiles),
            "count": len(rows),
            "source": "catalog",
        }

    def _tool_catalog_coverage_summary(
        self,
        db_profile: str = "",
        schema: str = "",
    ) -> dict[str, Any]:
        """Per-profile + per-schema description coverage from the catalog.

        Single GROUP BY against ``catalog_entities`` — sub-50 ms — so the
        Ask agent answers "how many tables don't have comments" in one
        round-trip instead of chaining describe_table per asset (the
        82-second hallucination loop the user hit).
        """
        target = (db_profile or "").strip()
        if target and target not in self.db_profiles:
            return {
                "profiles": [],
                "scope": list(self.db_profiles),
                "source": "catalog",
                "error": f"Profile {target!r} is not in this Ask's scope.",
            }
        scope: str | list[str] = target if target else self.db_profile_filter
        sch = (schema or "").strip() or None
        try:
            rows = self.catalog.fetch_coverage_summary(scope, schema_name=sch)
        except Exception as exc:  # noqa: BLE001
            return {
                "profiles": [],
                "scope": list(self.db_profiles),
                "source": "catalog",
                "error": f"Catalog coverage query failed: {exc}",
            }
        total_tables = sum(row["total_tables"] for row in rows)
        undocumented_tables = sum(row["undocumented_tables"] for row in rows)
        total_columns = sum(row["total_columns"] for row in rows)
        undocumented_columns = sum(row["undocumented_columns"] for row in rows)
        documented_tables = max(0, total_tables - undocumented_tables)
        documented_columns = max(0, total_columns - undocumented_columns)
        totals = {
            "total_tables": total_tables,
            "undocumented_tables": undocumented_tables,
            "documented_tables": documented_tables,
            "total_columns": total_columns,
            "undocumented_columns": undocumented_columns,
            "documented_columns": documented_columns,
            "table_coverage_pct": (
                round(100.0 * documented_tables / total_tables, 1)
                if total_tables > 0
                else None
            ),
            "column_coverage_pct": (
                round(100.0 * documented_columns / total_columns, 1)
                if total_columns > 0
                else None
            ),
        }
        last_synced_at = max(
            (row["last_synced_at"] or 0.0 for row in rows), default=0.0
        )
        return {
            "profiles": rows,
            "totals": totals,
            "scope": list(self.db_profiles),
            "source": "catalog",
            "last_synced_at": last_synced_at or None,
        }

    def _tool_catalog_sync_status(self, db_profile: str = "") -> dict[str, Any]:
        """One-shot freshness report for every DB profile in scope.

        Reads ``catalog_profile_state`` via the SyncMixin getter, with
        zero live-DB calls. The canonical answer to "are my tables
        synced / up to date / fresh / stale" — single LLM round-trip,
        no hypothesis loop.
        """
        target = (db_profile or "").strip()
        if target:
            profiles = [target] if target in self.db_profiles else []
        else:
            profiles = list(self.db_profiles)

        now = self._now()
        out: list[dict[str, Any]] = []
        for name in profiles:
            try:
                state = self.catalog.get_profile_state(name)
            except Exception as exc:  # noqa: BLE001
                out.append(
                    {
                        "db_profile": name,
                        "state": "unknown",
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )
                continue
            last_full = state.get("last_full_sync_at")
            age = max(0.0, now - float(last_full)) if last_full else None
            out.append(
                {
                    "db_profile": name,
                    "state": state.get("state") or "none",
                    "last_synced_at": float(last_full) if last_full else None,
                    "age_seconds": age,
                    # 24h and 7d windows match the staleness signals the
                    # rest of the catalog uses (``is_profile_fully_synced``
                    # rejects anything older than 7 days, the Studio
                    # freshness pill flags >24h).
                    "is_fresh_24h": bool(age is not None and age < 86_400),
                    "is_fresh_7d": bool(age is not None and age < 7 * 86_400),
                    "processed_tables": int(state.get("processed_tables") or 0),
                    "total_tables": int(state.get("total_tables") or 0),
                    "last_error": state.get("last_error") or "",
                }
            )
        return {"profiles": out, "scope": list(self.db_profiles)}

    def _tool_list_schemas(
        self,
        catalog: str = "",
        db_profile: str = "",
        force_fresh: bool = False,
    ) -> dict[str, Any]:
        force_fresh = self._gate_force_fresh(force_fresh)
        # Multi-profile fan-out path: when the scope spans 2+ profiles
        # AND the LLM didn't target a specific one via ``db_profile``,
        # parallel-list schemas across every profile in scope. Each
        # profile gets its own connector + per-call timeout so a slow
        # backend never blocks the question.
        targeted = (db_profile or "").strip()
        if self.is_multi_profile and not targeted:
            return self._fanout_list_schemas(catalog=catalog)
        if targeted and targeted != self.db_profile:
            # Single-target dispatch: use the named profile's connector
            # + its DBConfig (which knows its own pinned catalog).
            return self._list_schemas_on_profile(targeted, catalog=catalog)
        # ── Cache-first: catalog_entities ──
        # When /search sync has covered this profile the catalog already
        # knows every schema; the LLM doesn't need to pay for a live
        # ``list_schemas`` round-trip. ``force_fresh`` bypasses this and
        # goes straight to live.
        if not force_fresh and not (catalog or "").strip():
            # Completeness gate: only trust the catalog when a
            # skeleton sync has confirmed every schema + table is
            # represented. A partial catalog would lie to the LLM
            # ("9 tables total" when there are 10 000) — fall through
            # to the live DB and tag the result so the agent's
            # post-tool wrapper can warn the user.
            fully_synced = False
            try:
                fully_synced = bool(self.catalog.is_profile_fully_synced(self.db_profile))
            except Exception:
                fully_synced = False
            # ``fully_synced`` controls *trusting* the cache as
            # exhaustive. Even when False we may still have a partial
            # catalog of schemas; in cache-only mode we serve THAT and
            # flag ``possibly_partial`` instead of going live.
            try:
                db_scope = str(
                    getattr(self.cfg.db, "database", "")
                    or getattr(self.cfg.db, "catalog", "")
                    or getattr(self.cfg.db, "project", "")
                    or ""
                )
                catalog_schemas = self.catalog.fetch_distinct_schemas(
                    self.db_profile,
                    database_name=db_scope or None,
                )
            except Exception:
                catalog_schemas = []
            # Strict list check — tests stub ``self.catalog`` with a
            # bare MagicMock, which makes the call return a truthy
            # MagicMock that isn't iterable as expected.
            have_rows = isinstance(catalog_schemas, list) and bool(catalog_schemas)
            if have_rows and (fully_synced or not self._allow_live_refresh):
                fresh_ts = max((s.get("last_synced_at") or 0.0) for s in catalog_schemas)
                age = max(0.0, self._now() - fresh_ts) if fresh_ts > 0 else 0.0
                payload = {
                    "database": self.cfg.db.database
                    or self.cfg.db.catalog
                    or self.cfg.db.project
                    or "(active database)",
                    "schemas": [s["name"] for s in catalog_schemas],
                    "count": len(catalog_schemas),
                    "source": "catalog",
                    "age_seconds": age,
                }
                if not fully_synced:
                    # Cache-only mode is the reason we did not go live.
                    # Tell the LLM honestly so it can surface the caveat.
                    payload["possibly_partial"] = True
                    payload["cache_only"] = True
                    payload["hint"] = (
                        "Catalog sync is not marked complete; this list may "
                        "be partial. Enable the 'Live refresh' toggle in "
                        "the Ask composer and ask again to confirm against "
                        "the live database, OR run /search sync first."
                    )
                return payload
            if not self._allow_live_refresh:
                # No rows AND cache-only — refuse instead of falling to
                # live DB. Better to tell the user "nothing in cache"
                # than to silently bypass their toggle.
                return {
                    "database": self.cfg.db.database
                    or self.cfg.db.catalog
                    or self.cfg.db.project
                    or "(active database)",
                    "schemas": [],
                    "count": 0,
                    "source": "catalog",
                    "cache_only": True,
                    "possibly_partial": True,
                    "hint": (
                        "Catalog has no schemas recorded for this profile. "
                        "Enable 'Live refresh' in the composer and ask "
                        "again to query the database, or run /search sync."
                    ),
                }
        db = self._live_db()
        explicit = (catalog or "").strip()
        pinned_catalog = str(getattr(self.cfg.db, "catalog", "") or "").strip()
        cat_arg, user_catalogs, all_catalogs = self._resolve_catalog_or_autopick(db, explicit)
        # 3-level backend (Databricks UC, BigQuery): when neither the
        # active profile nor the LLM has named a catalog and no single
        # user catalog could be auto-picked, surface the candidate list
        # so the LLM can recurse instead of failing with the warehouse
        # error "Catalog 'none' was not found".
        if not cat_arg:
            try:
                supports = bool(db.supports_catalogs())
            except Exception:
                supports = False
            if supports:
                surface = user_catalogs or all_catalogs
                return {
                    "database": "(no catalog pinned)",
                    "schemas": [],
                    "count": 0,
                    "catalogs": surface,
                    "all_catalogs": all_catalogs,
                    "needs_catalog": True,
                    "message": (
                        "The active DB profile has no catalog pinned and the workspace "
                        "has multiple user catalogs. Pick the most likely one from the "
                        "`catalogs` list and IMMEDIATELY call this tool again with the "
                        "`catalog` argument set — do NOT just narrate the choice to the "
                        "user. If you genuinely cannot tell which catalog the user means, "
                        "answer in one short sentence asking them to pick."
                    ),
                }

        try:
            with self._scoped_catalog(db, cat_arg):
                schemas = [str(s) for s in db.list_schemas()]
        except Exception as exc:
            raise _ToolError(f"Could not list schemas live: {exc}") from exc
        database = (
            cat_arg
            or self.cfg.db.database
            or self.cfg.db.catalog
            or self.cfg.db.project
            or "(active database)"
        )
        payload: dict[str, Any] = {
            "database": database,
            "schemas": schemas,
            "count": len(schemas),
            "source": "live",
            "age_seconds": 0.0,
        }
        # Flag a partial-catalog state so the agent's post-tool wrapper
        # warns the LLM not to claim this is the complete picture as
        # being authoritatively cached. ``partial`` only fires when
        # we'd have served the catalog if it were complete — i.e. the
        # branch above bailed because ``is_profile_fully_synced`` was
        # False. ``force_fresh`` and explicit catalog args bypass the
        # gate entirely so they leave ``partial`` off.
        if not force_fresh and not (catalog or "").strip():
            try:
                fully_synced = bool(self.catalog.is_profile_fully_synced(self.db_profile))
            except Exception:
                fully_synced = True  # uncertain → silent
            if not fully_synced:
                payload["partial"] = True
                payload["partial_reason"] = (
                    f"catalog for profile {self.db_profile} is not fully synced yet"
                )
        if cat_arg:
            payload["catalog"] = cat_arg
            if not pinned_catalog and not explicit:
                # We auto-resolved the catalog (single user catalog
                # heuristic). Surface it so the LLM mentions which
                # catalog the schemas live in instead of pretending
                # the profile already had one pinned.
                payload["auto_picked_catalog"] = cat_arg
        return payload

    def _tool_list_tables_in_schema(
        self,
        schema: str,
        catalog: str = "",
        db_profile: str = "",
        force_fresh: bool = False,
    ) -> dict[str, Any]:
        force_fresh = self._gate_force_fresh(force_fresh)
        target = (schema or "").strip()
        if not target:
            raise _ToolError("Argument 'schema' is required.")
        targeted = (db_profile or "").strip()
        if self.is_multi_profile and not targeted:
            return self._fanout_list_tables_in_schema(target, catalog=catalog)
        if targeted and targeted != self.db_profile:
            return self._list_tables_on_profile(targeted, target, catalog=catalog)
        # ── Cache-first ── gated on full skeleton sync; partial
        # catalog falls through to live DB and the live payload is
        # tagged ``partial: true`` so the agent warns the user.
        partial_fallback = False
        if not force_fresh and not (catalog or "").strip():
            fully_synced = False
            try:
                fully_synced = bool(self.catalog.is_profile_fully_synced(self.db_profile))
            except Exception:
                fully_synced = False
            try:
                db_scope = str(
                    getattr(self.cfg.db, "database", "")
                    or getattr(self.cfg.db, "catalog", "")
                    or getattr(self.cfg.db, "project", "")
                    or ""
                )
                catalog_tables = self.catalog.fetch_distinct_tables_in_schema(
                    self.db_profile,
                    target,
                    database_name=db_scope or None,
                )
            except Exception:
                catalog_tables = []
            have_rows = isinstance(catalog_tables, list) and bool(catalog_tables)
            # Serve cached rows when EITHER the catalog is fully
            # synced (trust the snapshot) OR the user is in cache-only
            # mode (Live refresh OFF means do not go live regardless
            # of completeness).
            if have_rows and (fully_synced or not self._allow_live_refresh):
                fresh_ts = max((t.get("last_synced_at") or 0.0) for t in catalog_tables)
                age = max(0.0, self._now() - fresh_ts) if fresh_ts > 0 else 0.0
                payload = {
                    "schema": target,
                    "catalog": None,
                    "found": True,
                    "tables": [{"name": t["name"], "kind": "table"} for t in catalog_tables],
                    "count": len(catalog_tables),
                    "source": "catalog",
                    "age_seconds": age,
                }
                if not fully_synced:
                    payload["possibly_partial"] = True
                    payload["cache_only"] = True
                    payload["hint"] = (
                        "Catalog sync for this profile is not marked "
                        "complete; the table list may be partial. Enable "
                        "the 'Live refresh' toggle to confirm against the "
                        "live database, or run /search sync."
                    )
                return payload
            if not fully_synced:
                partial_fallback = True
            if not self._allow_live_refresh:
                # Cache-only AND nothing in the catalog for this schema.
                # Refuse to go live; tell the user we don't have it
                # cached so they can opt in via the toggle.
                return {
                    "schema": target,
                    "catalog": None,
                    "found": False,
                    "tables": [],
                    "count": 0,
                    "source": "catalog",
                    "cache_only": True,
                    "possibly_partial": True,
                    "hint": (
                        f"Catalog has no tables recorded for schema "
                        f"'{target}' on profile {self.db_profile}. "
                        "Enable 'Live refresh' to query the database, or "
                        "run /search sync to refresh the catalog first."
                    ),
                }
        db = self._live_db()
        # Resolve the catalog: explicit > pinned > single-user-catalog
        # auto-pick. Without this, a Databricks UC backend without a
        # pinned catalog would issue ``SHOW TABLES FROM None.<schema>``
        # and fail with NO_SUCH_CATALOG_EXCEPTION (the user-reported
        # third loop on /ask).
        explicit = (catalog or "").strip()
        cat_arg, user_catalogs, all_catalogs = self._resolve_catalog_or_autopick(db, explicit)
        if not cat_arg:
            try:
                supports = bool(db.supports_catalogs())
            except Exception:
                supports = False
            if supports:
                # No catalog could be resolved AND we're on a 3-level
                # backend — surface what we know so the LLM can pick
                # instead of failing with the cryptic warehouse error.
                return {
                    "schema": target,
                    "catalog": None,
                    "found": False,
                    "needs_catalog": True,
                    "catalogs": user_catalogs or all_catalogs,
                    "all_catalogs": all_catalogs,
                    "message": (
                        "Multiple user catalogs are visible and the active profile "
                        "didn't pin one. Call this tool again with `catalog` set to "
                        "the most likely entry from `catalogs`. Do NOT enumerate the "
                        "list back to the user — pick and recurse."
                    ),
                }

        with self._scoped_catalog(db, cat_arg):
            try:
                available = list(db.list_schemas())
            except Exception as exc:
                raise _ToolError(f"Could not list schemas: {exc}") from exc
            match = next((s for s in available if str(s).lower() == target.lower()), None)
            if match is None:
                return {
                    "schema": target,
                    "catalog": cat_arg or None,
                    "found": False,
                    "available_schemas": [str(s) for s in available],
                    "message": (
                        f"No schema named '{target}' in catalog '{cat_arg}'. "
                        "Available schemas: " + ", ".join(str(s) for s in available)
                    ),
                }
            items: list[dict[str, str]] = []
            try:
                if hasattr(db, "list_assets"):
                    for name, kind in db.list_assets(match):
                        items.append({"name": str(name), "kind": str(kind)})
                else:
                    for name in db.list_tables(match):
                        items.append({"name": str(name), "kind": "table"})
            except Exception as exc:
                raise _ToolError(f"Could not list tables in {match}: {exc}") from exc
        payload: dict[str, Any] = {
            "schema": match,
            "catalog": cat_arg,
            "found": True,
            "tables": items,
            "count": len(items),
            "source": "live",
            "age_seconds": 0.0,
        }
        if partial_fallback:
            payload["partial"] = True
            payload["partial_reason"] = (
                f"catalog for profile {self.db_profile} is not fully synced yet"
            )
        if not explicit and not str(getattr(self.cfg.db, "catalog", "") or "").strip():
            # We auto-picked the catalog. Tell the LLM so it can mention
            # which catalog the tables live in instead of pretending the
            # profile already had one pinned.
            payload["auto_picked_catalog"] = cat_arg
        return payload

    def _tool_list_catalogs(self) -> dict[str, Any]:
        db = self._live_db()
        try:
            supports = bool(db.supports_catalogs())
        except Exception:
            supports = False
        if not supports:
            return {
                "supports_catalogs": False,
                "catalogs": [],
                "count": 0,
                "message": (
                    "The active backend does not expose multiple catalogs. Use "
                    "`list_server_databases` for 2-level backends (PostgreSQL, "
                    "Snowflake, MySQL, MSSQL, Redshift, ClickHouse)."
                ),
            }
        try:
            catalogs = [str(c) for c in db.list_catalogs()]
        except Exception as exc:
            raise _ToolError(f"SHOW CATALOGS failed: {exc}") from exc
        pinned = str(getattr(self.cfg.db, "catalog", "") or "").strip()
        user_catalogs = self._user_catalogs(catalogs)
        payload: dict[str, Any] = {
            "supports_catalogs": True,
            "catalogs": catalogs,
            "user_catalogs": user_catalogs,
            "count": len(catalogs),
            "active_catalog": pinned or None,
        }
        # When no catalog is pinned and the workspace exposes exactly
        # one user catalog, eagerly attach its schema list. This breaks
        # the kimi-thinking loop where the model would call
        # list_catalogs, see the 4-catalog Databricks listing, and
        # narrate the obvious choice instead of calling list_schemas.
        # Embedding the answer in the same response means the next
        # iteration of the agent loop already has the schemas to work
        # with — there's nothing left to "decide".
        if not pinned and len(user_catalogs) == 1:
            try:
                with self._scoped_catalog(db, user_catalogs[0]):
                    schemas = [str(s) for s in db.list_schemas()]
                payload["auto_picked_catalog"] = user_catalogs[0]
                payload["schemas_in_auto_picked_catalog"] = schemas
                payload["instruction"] = (
                    f"Only one user catalog (`{user_catalogs[0]}`) is visible. "
                    "Treat it as the active catalog for this turn — the schemas "
                    "are already in `schemas_in_auto_picked_catalog`. Answer the "
                    "user's question directly using these schemas; do NOT enumerate "
                    "the catalog list back to the user."
                )
            except Exception as exc:
                # Don't block the catalog list on a follow-up failure;
                # the LLM can still pick manually from the list above.
                payload["auto_pick_failed"] = str(exc)
        return payload

    def _tool_list_server_databases(self) -> dict[str, Any]:
        db = self._live_db()
        try:
            databases = [str(d) for d in db.list_databases()]
        except Exception as exc:
            raise _ToolError(f"Listing databases failed: {exc}") from exc
        pinned = str(getattr(self.cfg.db, "database", "") or "").strip()
        if not databases:
            return {
                "databases": [],
                "count": 0,
                "active_database": pinned or None,
                "message": (
                    "The active backend does not expose multiple databases on this server, "
                    "or the role has no privilege to list them. For 3-level backends "
                    "(Databricks Unity Catalog, BigQuery) use `list_catalogs`."
                ),
            }
        return {
            "databases": databases,
            "count": len(databases),
            "active_database": pinned or None,
        }

    def _tool_list_volumes(self, schema: str = "", catalog: str = "") -> dict[str, Any]:
        db = self._live_db()
        if not getattr(db.capabilities, "volumes", False):
            return {
                "supported": False,
                "volumes": [],
                "count": 0,
                "message": (
                    "The active backend does not expose Volumes — they're a "
                    "Databricks-distinctive object type. Reply that volumes don't "
                    "apply to this backend instead of inventing a query."
                ),
            }

        cat_arg, _user_catalogs, _all_catalogs = self._resolve_catalog_or_autopick(
            db, (catalog or "").strip()
        )
        if not cat_arg:
            return {
                "supported": True,
                "volumes": [],
                "count": 0,
                "needs_catalog": True,
                "message": (
                    "Multiple user catalogs are visible and the active profile didn't "
                    "pin one. Call this tool again with `catalog` set to the most "
                    "likely entry."
                ),
            }

        sch_arg = (schema or "").strip()
        with self._scoped_catalog(db, cat_arg):
            # Phase 4 fast path: bulk INFORMATION_SCHEMA query for every
            # volume in the catalog. Skips when ``schema`` is set
            # (per-schema query is already cheap) or when the adapter
            # didn't implement the bulk variant.
            rows: list[dict[str, Any]] = []
            warnings: list[str] = []
            target_schemas: list[str] | str
            list_volumes_bulk = getattr(db, "list_volumes_bulk", None)
            bulk = list_volumes_bulk(cat_arg) if list_volumes_bulk and not sch_arg else None
            if bulk is not None:
                rows = [
                    {
                        "schema": v["schema"],
                        "name": v["name"],
                        "kind": v.get("type") or "volume",
                        "comment": v.get("comment") or "",
                    }
                    for v in bulk
                ]
                target_schemas = "(bulk via information_schema)"
            else:
                try:
                    target_schemas = [sch_arg] if sch_arg else [str(s) for s in db.list_schemas()]
                except Exception as exc:
                    raise _ToolError(f"Could not list schemas in {cat_arg!r}: {exc}") from exc

                for sch in target_schemas:
                    try:
                        for vol in db.list_volumes(sch, cat_arg):
                            rows.append(
                                {
                                    "schema": sch,
                                    "name": str(vol.get("name") or ""),
                                    "kind": str(vol.get("type") or "volume"),
                                    "comment": str(vol.get("comment") or ""),
                                }
                            )
                    except Exception as exc:
                        warnings.append(f"{sch}: {exc.__class__.__name__}: {exc}")

        payload: dict[str, Any] = {
            "supported": True,
            "catalog": cat_arg,
            "schemas_scanned": target_schemas,
            "volumes": rows,
            "count": len(rows),
        }
        if not sch_arg and not str(getattr(self.cfg.db, "catalog", "") or "").strip():
            payload["auto_picked_catalog"] = cat_arg
        if warnings:
            payload["warnings"] = warnings
        return payload

    def _tool_find_table_by_name(self, name: str, force_fresh: bool = False) -> dict[str, Any]:
        force_fresh = self._gate_force_fresh(force_fresh)
        target = (name or "").strip()
        if not target:
            raise _ToolError("Argument 'name' is required.")
        # ── Stage 1 — exact match in both catalog + live DB ──
        # Multi-profile scope: ``find_tables_by_exact_name`` accepts a
        # DBProfileFilter, so a single SQL pass covers every profile.
        # Result rows carry their own ``db_profile`` field; we tag each
        # match path with it so the LLM can disambiguate cross-profile.
        catalog_rows = self.catalog.find_tables_by_exact_name(
            self.db_profile_filter, target, limit=20
        )
        catalog_paths: list[str] = []
        for row in catalog_rows:
            schema_name = str(row.get("schema_name") or "")
            table_name = str(row.get("table_name") or "")
            row_profile = str(row.get("db_profile") or "")
            if schema_name and table_name:
                # In multi-profile mode prefix the path with profile so
                # downstream dedupe doesn't collapse same-named tables
                # from different profiles into one.
                if self.is_multi_profile and row_profile:
                    catalog_paths.append(f"{row_profile}::{schema_name}.{table_name}")
                else:
                    catalog_paths.append(f"{schema_name}.{table_name}")
        live_paths: list[str] = []
        # Walk live DB once and remember every table name we see; the
        # exact-match check happens here, fuzzy fallback (Stage 2)
        # reuses the same list so we don't pay for two passes.
        all_live_tables: list[str] = []
        # For each ``(database, path)`` we record where the table lives
        # so callers can route ``describe_table`` straight at the right
        # database without re-running the cross-DB sweep. Empty string
        # means "active connection's default database" (legacy behaviour).
        path_to_database: dict[str, str] = {}
        try:
            anchor = self._live_db()
            # Phase 4 fast path: a single ``information_schema.tables``
            # query covers every schema in the catalog. The original
            # per-schema loop walks 100 schemas × 1 ``SHOW TABLES`` each =
            # 100 round-trips on Databricks; the bulk path collapses
            # that to 1 query. Falls through to the loop when the
            # adapter doesn't implement the bulk variant or returns None.
            catalog_pin = str(getattr(self.cfg.db, "catalog", "") or "").strip()
            list_assets_bulk = getattr(anchor, "list_assets_bulk", None)
            bulk_assets = (
                list_assets_bulk(catalog_pin) if list_assets_bulk and catalog_pin else None
            )
            if bulk_assets is not None:
                for sch_name, asset_name, _kind in bulk_assets:
                    full_path = f"{sch_name}.{asset_name}"
                    all_live_tables.append(full_path)
                    if asset_name.lower() == target.lower():
                        live_paths.append(full_path)
            else:
                # Cross-database sweep: for an unpinned 2-level backend
                # (PostgreSQL / MySQL / SQL Server with no
                # ``cfg.db.database``) ``self._databases_to_sweep`` returns
                # every database the server exposes. Pinned profiles and
                # catalog-style (3-level) backends short-circuit to a
                # single ``[None]`` entry so they keep the legacy
                # single-database walk. This is the fix for the
                # user-reported case where ``find_table_by_name("vbrk")``
                # missed a table in the ``SAP`` database because the
                # PostgreSQL connection happened to default to ``public``
                # in another database.
                for db_name in self._databases_to_sweep():
                    db_conn = self._connector_for_database(db_name) if db_name else anchor
                    try:
                        schemas = db_conn.list_schemas()
                    except Exception:
                        continue
                    for schema in schemas:
                        try:
                            if hasattr(db_conn, "list_assets"):
                                asset_iter = (
                                    (str(n), str(k)) for n, k in db_conn.list_assets(schema)
                                )
                            else:
                                asset_iter = (
                                    (str(n), "table") for n in db_conn.list_tables(schema)
                                )
                            for asset_name, _kind in asset_iter:
                                full_path = f"{schema}.{asset_name}"
                                all_live_tables.append(full_path)
                                path_to_database[full_path] = db_name or ""
                                if asset_name.lower() == target.lower():
                                    live_paths.append(full_path)
                        except Exception:
                            continue
        except Exception:
            # Live discovery is best-effort. Fall back to whatever the catalog had.
            pass
        merged = list(dict.fromkeys(catalog_paths + live_paths))

        # ── Stage 2 — substring + fuzzy fallback ──
        # When the user only remembers PART of the table name ("trog"
        # for "trogr_v"), exact match returns nothing and the LLM
        # honestly says "no such table". Give it a wider net: any
        # table where the target is a substring, prefix, suffix, OR
        # within edit distance ≤ 2. The LLM gets each match tagged
        # with ``match_kind`` so it can rank / present them
        # transparently. Same design fix as v0.9.10's columns_by_dtype:
        # complete coverage, no whack-a-mole per question phrasing.
        target_lower = target.lower()
        fuzzy_matches: list[dict[str, str]] = []
        seen = {p.lower() for p in merged}
        for path in all_live_tables:
            if path.lower() in seen:
                continue
            asset_name = path.split(".", 1)[1] if "." in path else path
            asset_lower = asset_name.lower()
            kind: str | None = None
            if target_lower == asset_lower:
                continue  # already in merged via Stage 1
            if target_lower in asset_lower:
                kind = "contains"
            elif asset_lower.startswith(target_lower):
                kind = "prefix"
            elif asset_lower.endswith(target_lower):
                kind = "suffix"
            else:
                # Edit-distance fallback. Use SequenceMatcher's ratio
                # as a cheap proxy: 0.7+ ≈ 1-2 edits on short SAP-style
                # names (4-8 chars).
                ratio = SequenceMatcher(
                    None,
                    target_lower,
                    asset_lower,
                ).ratio()
                if ratio >= 0.7 and abs(len(target_lower) - len(asset_lower)) <= 3:
                    kind = "fuzzy"
            if kind is not None:
                fuzzy_matches.append({"path": path, "match_kind": kind})
                seen.add(path.lower())

        # Catalog-side fuzzy: also scan catalog entities so we catch
        # tables that exist in the catalog but aren't in the live DB
        # listing yet (or live discovery failed). Multi-profile scope
        # expands ``WHERE db_profile = ?`` to ``IN (?, ?, …)``.
        from amx.search._catalog._db_profile_clause import build_db_profile_clause

        try:
            clause, binds = build_db_profile_clause(self.db_profile_filter)
            with self.catalog._connect() as conn:  # noqa: SLF001
                catalog_all = conn.execute(
                    f"SELECT db_profile, schema_name, table_name FROM catalog_entities "
                    f"WHERE {clause} AND entity_kind = 'table'",
                    tuple(binds),
                ).fetchall()
            for r in catalog_all:
                schema_name = str(r["schema_name"] or "")
                table_name = str(r["table_name"] or "")
                if not schema_name or not table_name:
                    continue
                path = f"{schema_name}.{table_name}"
                if path.lower() in seen:
                    continue
                asset_lower = table_name.lower()
                if target_lower == asset_lower:
                    continue
                kind: str | None = None
                if target_lower in asset_lower:
                    kind = "contains"
                elif asset_lower.startswith(target_lower):
                    kind = "prefix"
                elif asset_lower.endswith(target_lower):
                    kind = "suffix"
                else:
                    ratio = SequenceMatcher(
                        None,
                        target_lower,
                        asset_lower,
                    ).ratio()
                    if ratio >= 0.7 and abs(len(target_lower) - len(asset_lower)) <= 3:
                        kind = "fuzzy"
                if kind is not None:
                    fuzzy_matches.append({"path": path, "match_kind": kind})
                    seen.add(path.lower())
        except Exception:
            pass

        # Rank fuzzy matches: prefix/suffix > contains > fuzzy. Within
        # each tier, shorter table names rank first (assumption: a
        # 5-char table name containing "trog" is a closer hit than a
        # 30-char one).
        order = {"prefix": 0, "suffix": 1, "contains": 2, "fuzzy": 3}
        fuzzy_matches.sort(
            key=lambda r: (
                order.get(r["match_kind"], 99),
                len(r["path"].split(".", 1)[1] if "." in r["path"] else r["path"]),
                r["path"].lower(),
            )
        )
        # Cap so the prompt stays tight on huge schemas.
        fuzzy_matches = fuzzy_matches[:25]

        # Surface the resolved database per live-DB match so the LLM
        # can route ``describe_table`` straight at the right database
        # on follow-up calls (unpinned 2-level backends only — pinned
        # / 3-level profiles populate an empty map).
        match_databases: dict[str, str] = {}
        for path in live_paths:
            db_name = path_to_database.get(path, "")
            if db_name:
                match_databases[path] = db_name
        # ── Write-back: discovery rows ──
        # Each live-DB match writes a lightweight ``discovery`` row
        # into run_context_cache so a sibling ``describe_table`` (or a
        # later /ask question within 24h) can resolve the database
        # without paying for the cross-DB sweep again. We do NOT store
        # column lists here — describe_table will fill those in when
        # the LLM follows up. The empty ``columns`` field also lets
        # ``_resolve_table_metadata`` distinguish a discovery row from
        # a real table profile (it requires ``columns`` to be a list
        # with entries before serving from the live cache).
        for path, db_name in match_databases.items():
            schema_part, _, table_part = path.partition(".")
            if not schema_part or not table_part:
                continue
            self._writeback_table_metadata(
                db_profile=self.db_profile,
                database=db_name,
                schema=schema_part,
                table=table_part,
                payload={"kind": "discovery", "columns": []},
            )
        # Provenance for the overall response: ``catalog`` when every
        # match came from the catalog (no live sweep needed), ``live``
        # when the sweep ran. Per-match source is implicit:
        # ``from_catalog`` vs ``from_live_db`` already split the list.
        overall_source = (
            "catalog" if catalog_paths and not live_paths else "live" if live_paths else "catalog"
        )
        return {
            "name": target,
            "matches": merged,
            "match_count": len(merged),
            "from_catalog": catalog_paths,
            "from_live_db": live_paths,
            # ``{path: database}`` for unpinned 2-level backends so the
            # LLM passes ``database=…`` to follow-up describe_table /
            # sample_column_values calls without paying for another
            # cross-DB sweep. Empty for pinned profiles and 3-level
            # backends where the catalog/database layer is fixed.
            "resolved_databases": match_databases,
            # Substring + fuzzy fallback. ALWAYS populated (empty list
            # when nothing matches) so the LLM has one shape to reason
            # over. Each entry is ``{path, match_kind}`` where
            # match_kind is one of: prefix / suffix / contains / fuzzy.
            "fuzzy_matches": fuzzy_matches,
            "source": overall_source,
        }

    def _tool_describe_table(
        self,
        schema: str,
        table: str,
        catalog: str = "",
        db_profile: str = "",
        database: str = "",
        force_fresh: bool = False,
    ) -> dict[str, Any]:
        force_fresh = self._gate_force_fresh(force_fresh)
        schema_name = (schema or "").strip()
        table_name = (table or "").strip()
        if not schema_name or not table_name:
            raise _ToolError("Both 'schema' and 'table' are required.")
        # Profile-targeted bookkeeping. The actual connector is opened
        # LAZILY below — only when we need to pay for a live profile_table
        # round-trip. Without this lazy-open the cache-only mode would
        # still spin up a SQLAlchemy engine on every describe_table call
        # even though the cache resolver would have returned the answer.
        targeted = (db_profile or "").strip()
        target_profile = targeted or self.db_profile

        explicit = (catalog or "").strip()
        explicit_db = (database or "").strip()

        # ── Cache-first lookup ──
        # Try the catalog (filled by /search sync) and the 24h live
        # cache (filled by /run-apply or a prior /ask write-back) before
        # paying for a live ``profile_table`` round-trip. The resolver
        # returns (None, "live", 0.0) on miss; we then sweep databases
        # and write the live result back so the NEXT call is free.
        cache_payload, source, age_seconds = self._resolve_table_metadata(
            db_profile=target_profile,
            database=explicit_db,
            schema=schema_name,
            table=table_name,
            force_fresh=force_fresh,
        )

        # Cache miss + cache-only mode: do not spin up a live connector.
        # Return a structured "not in cache" payload that the LLM can
        # surface back to the user; the assistant should then suggest
        # enabling Live refresh + re-asking (or running /search sync).
        if (
            cache_payload is None
            and not self._allow_live_refresh
            and not force_fresh
        ):
            return {
                "schema": schema_name,
                "table": table_name,
                "found": False,
                "source": "catalog",
                "cache_only": True,
                "possibly_partial": True,
                "hint": (
                    f"Table '{schema_name}.{table_name}' is not in the "
                    f"catalog for profile {target_profile}. Enable "
                    f"'Live refresh' in the composer and ask again "
                    f"to fetch it live, or run /search sync first."
                ),
            }

        # Defer the live-connector + cat_arg resolution to the live
        # branch below — cache-hit responses never need them.
        db = None  # opened lazily in the else-branch
        cat_arg: str = ""

        if cache_payload is not None and source != "live":
            # Reconstruct the wire shape from the cache payload. The
            # ``analytics`` block is best-effort — the catalog doesn't
            # carry partition / index / governance metadata.
            cached_cols = list(cache_payload.get("columns") or [])
            all_cols = [
                {
                    "name": c.get("name", ""),
                    "type": c.get("dtype") or c.get("type") or "",
                    "nullable": bool(c.get("nullable", True)),
                    "comment": str(c.get("comment") or ""),
                }
                for c in cached_cols
                if c.get("name")
            ]
            row_count = int(cache_payload.get("row_count") or 0)
            table_comment = str(cache_payload.get("table_comment") or "")
            resolved_database = explicit_db or None
            last_synced_at = cache_payload.get("last_synced_at")
            # Skip live ``profile_table``; jump to response assembly
            # below using the synthesised inputs.
            profile = _CacheBackedTableProfile(
                columns=all_cols,
                row_count=row_count,
                table_comment=table_comment,
            )
        else:
            # Lazy live-connector open: only reached when the cache
            # missed AND the user (or force_fresh) allowed the trip.
            if targeted and targeted != self.db_profile:
                db = self._connector_for_profile(targeted)
            else:
                db = self._live_db()
            # Resolve the catalog for 3-level backends so describe_table
            # doesn't end up issuing ``DESCRIBE None.<schema>.<table>``
            # when the active profile didn't pin a catalog.
            cat_arg, _user_catalogs, _all_catalogs = self._resolve_catalog_or_autopick(
                db, explicit
            )
            # Cross-database resolution for unpinned 2-level backends.
            # When the user (or the LLM) passes ``database=…`` explicitly,
            # honour it — otherwise fall through to the connection-time
            # default. If that fails AND the profile is unpinned, sweep
            # every database the server exposes and try each.
            candidate_dbs: list[str | None] = (
                [explicit_db] if explicit_db else self._databases_to_sweep()
            )

            last_error: str | None = None
            resolved_database = None
            profile = None
            for db_name in candidate_dbs:
                scoped = self._connector_for_database(db_name) if db_name else db
                try:
                    with self._scoped_catalog(scoped, cat_arg):
                        profile = scoped.profile_table(schema_name, table_name, sample_size=0)
                    resolved_database = (
                        db_name or str(getattr(scoped.cfg, "database", "") or "") or None
                    )
                    db = scoped
                    break
                except (ProfilingError, Exception) as exc:  # noqa: BLE001
                    last_error = str(exc)
                    continue
            if profile is None:
                return {
                    "schema": schema_name,
                    "table": table_name,
                    "catalog": cat_arg or None,
                    "found": False,
                    "error": last_error
                    or f"Could not resolve {schema_name}.{table_name} in any visible database.",
                    "source": "live",
                    "age_seconds": 0.0,
                }
            all_cols = [
                {
                    "name": c.name,
                    "type": c.dtype,
                    "nullable": bool(c.nullable),
                    "comment": str(c.existing_comment or ""),
                }
                for c in profile.columns
            ]
            # Write-back so the next call hits the 24h cache instead of
            # paying the cross-DB sweep again. /run-apply reads the same
            # key, so this also primes a future re-run.
            self._writeback_table_metadata(
                db_profile=target_profile,
                database=resolved_database or "",
                schema=schema_name,
                table=table_name,
                payload={
                    "columns": all_cols,
                    "row_count": int(profile.row_count or 0),
                    "table_comment": str(profile.existing_comment or ""),
                },
            )
            last_synced_at = None

        # ── Per-dtype family summary + complete coverage map ──
        # The summary gives the LLM the complete dtype picture of the
        # table even when the columns list below is truncated.
        # ``columns_by_dtype`` carries the actual column NAMES grouped
        # by family (NOT truncated, regardless of total table width)
        # so the LLM can answer "which columns are int / double / bool /
        # string / date / … in TABLE" by reading one map instead of
        # asking AMX for one tool-call per dtype. This is the design
        # fix for the false-negative loop ("we can't enumerate every
        # dtype question one by one"): give the LLM the complete
        # picture and trust it to reason.
        dtype_summary: dict[str, int] = {}
        columns_by_dtype: dict[str, list[str]] = {}
        for c in all_cols:
            family = self._dtype_family_label(c["type"])
            dtype_summary[family] = dtype_summary.get(family, 0) + 1
            columns_by_dtype.setdefault(family, []).append(c["name"])

        # ── Smart truncation order ──
        # When wide tables get capped, the truncation should leave the
        # MOST INTERESTING columns visible: rare dtypes (bool / date /
        # uuid / json — usually one or two per table) and columns that
        # already have a comment (someone curated them, so they're
        # worth seeing). Numeric / varchar columns without comments
        # cluster at the bottom because there are many of them and
        # they're typically interchangeable.
        rarity = dict(dtype_summary.items())

        def _sort_key(col: dict[str, Any]) -> tuple[int, int, str]:
            family = self._dtype_family_label(col["type"])
            commented = 1 if col.get("comment") else 0
            # rarity rank — fewer columns of this dtype family => earlier
            return (
                -commented,  # comments first
                rarity.get(family, 999),  # rare dtypes next (lower count first)
                col["name"],  # alphabetical tiebreak
            )

        sorted_cols = sorted(all_cols, key=_sort_key)

        # ── Analytics metadata ──
        # v0.10.0 introduced AnalyticsMetadata on TableProfile; pull
        # the non-empty fields here so the LLM can answer
        # performance-optimization / freshness / governance questions
        # without an extra tool round-trip. Empty fields are dropped to
        # keep the prompt tight.
        analytics_payload: dict[str, Any] = {}
        am = getattr(profile, "analytics", None)
        if am is not None:
            for attr in (
                "partition_keys",
                "partition_strategy",
                "clustering_keys",
                "storage_format",
                "storage_bytes",
                "storage_files_count",
                "last_modified",
                "table_type",
                "tags",
                "pii_columns",
                "indexes",
                "warnings",
            ):
                value = getattr(am, attr, None)
                if value:  # drop empty list / "" / 0 / {}
                    analytics_payload[attr] = value

        # Aggregated cache-side stats so the LLM can answer "how many
        # columns are nullable / undocumented / primary-key" without a
        # second tool call. Pulled directly from the column list we
        # already assembled — zero extra cost.
        nullable_count = sum(1 for c in all_cols if c.get("nullable"))
        documented_count = sum(1 for c in all_cols if str(c.get("comment") or "").strip())
        stats = {
            "row_count": int(profile.row_count or 0),
            "column_count": len(all_cols),
            "nullable_columns": nullable_count,
            "non_nullable_columns": max(0, len(all_cols) - nullable_count),
            "documented_columns": documented_count,
            "undocumented_columns": max(0, len(all_cols) - documented_count),
            "column_coverage_pct": (
                round(100.0 * documented_count / len(all_cols), 1)
                if all_cols
                else None
            ),
        }
        result = {
            "schema": schema_name,
            "table": table_name,
            "catalog": cat_arg or None,
            "found": True,
            "table_comment": str(profile.existing_comment or ""),
            "row_count": int(profile.row_count or 0),
            "column_count": len(all_cols),
            "stats": stats,
            "dtype_summary": dtype_summary,
            # Complete coverage — no truncation. Authoritative source
            # for "which columns of dtype X exist on this table".
            "columns_by_dtype": columns_by_dtype,
            "columns_truncated": len(all_cols) > 60,
            "columns": sorted_cols[:60],
            # Analytics-aware metadata — partition / cluster / size /
            # format / freshness / tags. Per-backend coverage varies;
            # only non-empty fields are included.
            "analytics": analytics_payload,
        }
        if cat_arg and not explicit and not str(getattr(self.cfg.db, "catalog", "") or "").strip():
            result["auto_picked_catalog"] = cat_arg
        # Tag the answer with the resolved profile so multi-profile
        # callers can render "this table on profile X has …" without
        # the LLM having to track which dispatch went to which profile.
        result["db_profile"] = targeted or self.db_profile
        # Surface the database we actually resolved against so the LLM
        # can pass it back on follow-up calls (sample_column_values,
        # rerun describe_table, etc.) without re-running the cross-DB
        # fanout. For pinned profiles this is just the pinned name; for
        # unpinned PostgreSQL it's the database where the table was
        # found during the sweep.
        try:
            result["resolved_database"] = str(getattr(db.cfg, "database", "") or "") or None
        except Exception:
            result["resolved_database"] = None
        # If the cache branch ran, ``resolved_database`` was set from
        # the explicit kwarg; keep that value rather than overwriting
        # with the connector's database. Catalog hits typically have
        # no explicit database (the catalog row is keyed only on
        # profile + schema + table) so leaving it as None is correct.
        if source != "live" and not result.get("resolved_database"):
            result["resolved_database"] = resolved_database
        # Cache-first provenance: every read tool surfaces source +
        # age so the LLM can hedge ("data is 12 days old, may be
        # stale — re-sync recommended") AND so cache/live behaviour
        # is testable end-to-end.
        result["source"] = source
        result["age_seconds"] = float(age_seconds)
        if last_synced_at:
            from datetime import datetime as _dt
            from datetime import timezone as _tz

            try:
                result["last_synced_at"] = (
                    _dt.fromtimestamp(float(last_synced_at), tz=_tz.utc)
                    .isoformat()
                    .replace("+00:00", "Z")
                )
            except Exception:
                pass
        return result

    @staticmethod
    def _dtype_family_label(dtype: str) -> str:
        """Coarse dtype family label used in ``dtype_summary``.

        Mirrors the agent_tools dtype-family map but compresses to one
        label per column (``bool`` / ``int`` / ``float`` / ``string`` /
        ``date`` / ``timestamp`` / ``json`` / ``uuid`` / etc.). Returns
        the lowered raw dtype when no family matches so exotic types
        still appear in the summary instead of silently merging into
        a generic bucket.
        """
        raw = (dtype or "").strip().lower()
        if not raw:
            return "unknown"
        # Strip array suffix and length/precision parens.
        base = raw.rstrip("[]")
        base = base.split("(", 1)[0].strip()
        head = base.split()[0] if base else raw
        if head in {"bool", "boolean"}:
            return "bool"
        if head in {
            "int",
            "integer",
            "int4",
            "int8",
            "int2",
            "bigint",
            "smallint",
            "serial",
            "bigserial",
        }:
            return "int"
        if head in {"float", "float4", "float8", "double", "real", "numeric", "decimal", "money"}:
            return "float"
        if head in {
            "char",
            "varchar",
            "text",
            "string",
            "nchar",
            "nvarchar",
            "character",
            "bpchar",
        }:
            return "string"
        if head in {"date"}:
            return "date"
        if head in {"timestamp", "timestamptz", "datetime", "datetime2", "smalldatetime"}:
            return "timestamp"
        if head in {"time", "timetz"}:
            return "time"
        if head in {"json", "jsonb"}:
            return "json"
        if head in {"uuid"}:
            return "uuid"
        if head in {"bytea", "blob", "binary", "varbinary"}:
            return "binary"
        return head

    def _tool_search_tables_by_concept(self, concept: str, limit: int = 10) -> dict[str, Any]:
        # ``db_profile_filter`` collapses to a scalar in single-profile
        # scope and a list in multi-profile scope — search_tables expands
        # the WHERE clause via build_db_profile_clause either way, so the
        # SQL stays one query regardless of how many profiles are in scope.
        rows = self.catalog.search_tables(self.db_profile_filter, concept or "", limit=int(limit))
        return {
            "concept": concept,
            "count": len(rows),
            "matches": [
                {
                    "db_profile": str(r.get("db_profile") or ""),
                    "schema": str(r.get("schema_name") or ""),
                    "table": str(r.get("table_name") or ""),
                    "score": float(r.get("rank_score") or r.get("score") or 0.0),
                    "description": str(r.get("effective_description") or ""),
                }
                for r in rows
            ],
        }

    def _tool_search_columns_by_concept(self, concept: str, limit: int = 10) -> dict[str, Any]:
        rows = self.catalog.search_columns(self.db_profile_filter, concept or "", limit=int(limit))
        return {
            "concept": concept,
            "count": len(rows),
            "matches": [
                {
                    "db_profile": str(r.get("db_profile") or ""),
                    "schema": str(r.get("schema_name") or ""),
                    "table": str(r.get("table_name") or ""),
                    "column": str(r.get("column_name") or ""),
                    "score": float(r.get("rank_score") or r.get("score") or 0.0),
                    "description": str(r.get("effective_description") or ""),
                }
                for r in rows
            ],
        }

    def _tool_find_assets_missing_comment(
        self,
        schema: str = "",
        scope: str = "both",
        limit: int = 50,
        include_system: bool = False,
    ) -> dict[str, Any]:
        """Return tables/columns with no comment, queried from the LIVE DB.

        The catalog can lag behind the live DB right after a ``/run-apply``,
        so coverage-type questions ("which tables are missing a comment?")
        must NOT come from ``catalog_entities`` rows — they must come from
        the source of truth. This tool calls ``get_table_comment`` /
        ``get_column_comments`` per asset and reports anything blank.

        System / telemetry assets (PostgreSQL extension views like
        ``pg_stat_statements``) are filtered out by default — the same
        filter the ``/run`` flow uses — because they aren't user data and
        AMX never describes them. Set ``include_system=True`` only when the
        user explicitly asks about system tables.
        """
        scope = (scope or "both").strip().lower()
        if scope not in {"tables", "columns", "both"}:
            scope = "both"
        limit = max(1, int(limit or 50))
        db = self._live_db()
        # Resolve schema list (case-insensitive when the user passed one).
        try:
            available = [str(s) for s in db.list_schemas()]
        except Exception as exc:
            raise _ToolError(f"Could not list schemas: {exc}") from exc
        target_schemas: list[str]
        target = (schema or "").strip()
        if target:
            match = next((s for s in available if s.lower() == target.lower()), None)
            if match is None:
                return {
                    "schema": target,
                    "found": False,
                    "available_schemas": available,
                    "message": (
                        f"No schema named '{target}'. Available schemas: " + ", ".join(available)
                    ),
                    "tables_missing_comment": [],
                    "columns_missing_comment": [],
                }
            target_schemas = [match]
        else:
            target_schemas = available

        # Reuse the same system-asset filter the /run flow uses so /ask
        # doesn't surface PostgreSQL extension views (pg_stat_statements,
        # pg_statio_*, etc.) as gaps. Users can ask about system tables
        # explicitly via include_system=True if needed.
        try:
            from amx.services.analyze_scope import is_non_business_asset
        except Exception:

            def is_non_business_asset(_name: str) -> bool:  # type: ignore[misc]
                return False

        tables_missing: list[dict[str, str]] = []
        columns_missing: list[dict[str, str]] = []
        skipped_system: list[str] = []
        for sch in target_schemas:
            try:
                if hasattr(db, "list_assets"):
                    asset_iter = [(str(n), str(k)) for n, k in db.list_assets(sch)]
                else:
                    asset_iter = [(str(n), "table") for n in db.list_tables(sch)]
            except Exception:
                continue
            for asset_name, asset_kind in asset_iter:
                if not include_system and is_non_business_asset(asset_name):
                    skipped_system.append(f"{sch}.{asset_name}")
                    continue
                if scope in {"tables", "both"} and len(tables_missing) < limit:
                    try:
                        tcom = db.get_table_comment(sch, asset_name)
                    except Exception:
                        tcom = None
                    if not (tcom or "").strip():
                        tables_missing.append(
                            {"schema": sch, "table": asset_name, "kind": asset_kind}
                        )
                if scope in {"columns", "both"} and len(columns_missing) < limit:
                    try:
                        col_comments = db.get_column_comments(sch, asset_name)
                    except Exception:
                        col_comments = {}
                    for col_name, col_comment in col_comments.items():
                        if not (col_comment or "").strip():
                            columns_missing.append(
                                {
                                    "schema": sch,
                                    "table": asset_name,
                                    "column": col_name,
                                }
                            )
                            if len(columns_missing) >= limit:
                                break
                if len(tables_missing) >= limit and len(columns_missing) >= limit:
                    break
            if len(tables_missing) >= limit and len(columns_missing) >= limit:
                break

        return {
            "scope": scope,
            "schemas_scanned": target_schemas,
            "tables_missing_comment": tables_missing if scope != "columns" else [],
            "tables_missing_count": len(tables_missing) if scope != "columns" else 0,
            "columns_missing_comment": columns_missing if scope != "tables" else [],
            "columns_missing_count": len(columns_missing) if scope != "tables" else 0,
            # Surfaced so the LLM knows we filtered system objects and can
            # mention it in the answer ("4 system views were excluded;
            # they aren't user data and AMX doesn't describe them").
            "system_assets_skipped": skipped_system,
            "system_assets_skipped_count": len(skipped_system),
            "include_system": bool(include_system),
        }

    def _count_database_assets(
        self,
        profile_name: str,
        target: str,
        supports_catalogs: bool,
    ) -> dict[str, Any]:
        """Return ``{"schema_count": N, "table_count": M}`` for *target*.

        For 3-level backends (Databricks, BigQuery), the per-profile
        connector is reused with ``_scoped_catalog`` to pin the catalog
        — opening a fresh ``DatabaseConnector`` per catalog would
        retrigger the SQL-warehouse cold-start (~2s per catalog on
        Databricks) and easily blow the 8s per-profile fan-out budget.

        For 2-level backends, the database lives in the connection
        string, so a fresh connector is unavoidable; that's cheap on
        PostgreSQL / Snowflake / MySQL.

        ``list_assets_bulk`` is used when the adapter supports it
        (single round trip); otherwise we sum ``list_tables(schema)``
        per schema. Failures degrade gracefully: keys are omitted from
        the returned dict when the count couldn't be computed, so the
        caller can still surface the database name without numbers.
        """
        base = self.cfg.db_profiles.get(profile_name)
        if base is None:
            return {}

        # 3-level: reuse the existing per-profile connector via
        # _scoped_catalog. The connector's engine is already warm.
        if supports_catalogs:
            try:
                conn = self._connector_for_profile(profile_name)
            except _ToolError:
                return {}
            try:
                with self._scoped_catalog(conn, target):
                    try:
                        schemas = [str(s) for s in conn.list_schemas()]
                    except Exception:
                        return {}
                    out: dict[str, Any] = {"schema_count": len(schemas)}
                    table_count: int | None = None
                    try:
                        bulk = conn.list_assets_bulk(target)
                    except Exception:
                        bulk = None
                    if bulk is not None:
                        table_count = len(bulk)
                    if table_count is None:
                        table_count = 0
                        for s in schemas:
                            try:
                                table_count += len(conn.list_tables(s))
                            except Exception:
                                continue
                    out["table_count"] = table_count
                    return out
            except Exception:
                return {}

        # 2-level: database lives in the connection string; open a fresh
        # connector scoped to the target database.
        try:
            scoped_cfg = _dc_replace(base, database=target)
        except TypeError:
            return {}
        scoped_conn: DatabaseConnector | None = None
        try:
            scoped_conn = DatabaseConnector(scoped_cfg)
            try:
                schemas = [str(s) for s in scoped_conn.list_schemas()]
            except Exception:
                return {}
            out = {"schema_count": len(schemas)}
            table_count = 0
            for s in schemas:
                try:
                    table_count += len(scoped_conn.list_tables(s))
                except Exception:
                    continue
            out["table_count"] = table_count
            return out
        except Exception:
            return {}
        finally:
            if scoped_conn is not None:
                with contextlib.suppress(Exception):
                    scoped_conn.close()

    def _tool_list_databases(self, *, with_counts: bool = False) -> dict[str, Any]:
        """List EVERY database (or catalog) reachable across the
        configured DB profiles — not just the one each profile has
        pinned.

        For each profile in scope:
        - 3-level backend (Databricks UC, BigQuery) → ``list_catalogs``
          on that connector returns every catalog the role can see.
        - 2-level backend (PostgreSQL, Snowflake, MySQL, …) →
          ``list_databases`` returns every server-side database.

        The legacy single-pinned-db output ("dbr → amx_test") was
        misleading — when the user asks "which databases do I have"
        they expect to see the full reach of each connection, not the
        currently-pinned default. The tool now answers that literally.

        Per-profile fan-out runs through the same ThreadPoolExecutor
        the live-DB tools use (cap 8 workers, 8s per-profile timeout)
        so a slow / unreachable profile doesn't block the others.

        ``with_counts``: when True, each database/catalog entry becomes
        ``{name, schema_count, table_count}`` so the LLM can answer
        "which tables can we reach" with the STATS-EXAMPLE-DRILL pattern
        in one turn (no follow-up tool call to get totals). Bulk-listing
        is preferred when the backend supports it; otherwise we fan
        out ``list_tables`` per schema. The 8s per-profile timeout
        still applies, so very large workspaces may surface partial
        counts.
        """
        from concurrent.futures import ThreadPoolExecutor, wait

        targets = list(self.db_profiles)
        if not targets:
            return {"profiles": {}, "count": 0, "scope": [], "with_counts": with_counts}

        def _per_profile(profile_name: str) -> dict[str, Any]:
            base = self.cfg.db_profiles.get(profile_name)
            backend = (str(getattr(base, "backend", "") or "") if base else "").lower()
            try:
                conn = self._connector_for_profile(profile_name)
            except _ToolError as exc:
                return {
                    "db_profile": profile_name,
                    "backend": backend,
                    "error": str(exc),
                    "databases": [],
                    "catalogs": [],
                }
            try:
                supports_catalogs = bool(conn.supports_catalogs())
            except Exception:
                supports_catalogs = False
            payload: dict[str, Any] = {
                "db_profile": profile_name,
                "backend": backend,
                "supports_catalogs": supports_catalogs,
                "pinned_database": (str(getattr(base, "database", "") or "") if base else "")
                or None,
                "pinned_catalog": (str(getattr(base, "catalog", "") or "") if base else "") or None,
                "databases": [],
                "catalogs": [],
            }
            try:
                if supports_catalogs:
                    catalogs = [str(c) for c in conn.list_catalogs()]
                    if with_counts:
                        # System catalogs (system, samples, workspace,
                        # hive_metastore, …) carry the workspace's own
                        # tooling, NOT user data — skip count-enrichment
                        # for them. Without this filter a Databricks
                        # workspace with 4+ system catalogs blew through
                        # the 8s per-profile timeout (each scoped
                        # list_assets_bulk is ~1-2s on Databricks).
                        # System catalogs are still surfaced (with no
                        # counts) so the LLM knows the full reach.
                        user_cats = set(self._user_catalogs(catalogs))
                        enriched: list[dict[str, Any]] = []
                        for c in catalogs:
                            entry: dict[str, Any] = {"name": c}
                            if c in user_cats:
                                entry.update(self._count_database_assets(profile_name, c, True))
                            else:
                                entry["system_catalog"] = True
                            enriched.append(entry)
                        payload["catalogs"] = enriched
                    else:
                        payload["catalogs"] = catalogs
                else:
                    databases = [str(d) for d in conn.list_databases()]
                    if with_counts:
                        payload["databases"] = [
                            {
                                "name": d,
                                **self._count_database_assets(profile_name, d, False),
                            }
                            for d in databases
                        ]
                    else:
                        payload["databases"] = databases
            except Exception as exc:
                payload["error"] = f"{exc.__class__.__name__}: {exc}"
            return payload

        max_workers = min(self._LIVE_FANOUT_MAX_WORKERS, max(1, len(targets)))
        per_profile: dict[str, dict[str, Any]] = {}
        with ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="amx-toolbox-fanout-databases",
        ) as pool:
            future_map = {pool.submit(_per_profile, name): name for name in targets}
            done, not_done = wait(future_map, timeout=self._LIVE_FANOUT_TIMEOUT_SEC)
            for future in done:
                name = future_map[future]
                try:
                    per_profile[name] = future.result(timeout=0)
                except Exception as exc:
                    per_profile[name] = {
                        "db_profile": name,
                        "error": f"{exc.__class__.__name__}: {exc}",
                        "databases": [],
                        "catalogs": [],
                    }
            for future in not_done:
                name = future_map[future]
                future.cancel()
                per_profile[name] = {
                    "db_profile": name,
                    "timeout": True,
                    "error": (f"timed out after {self._LIVE_FANOUT_TIMEOUT_SEC:.0f}s"),
                    "databases": [],
                    "catalogs": [],
                }

        total_dbs = sum(
            len(p.get("databases") or []) + len(p.get("catalogs") or [])
            for p in per_profile.values()
        )
        with_errors = [
            name
            for name, payload in per_profile.items()
            if payload.get("error") or payload.get("timeout")
        ]
        result: dict[str, Any] = {
            "scope": targets,
            "profiles": per_profile,
            "total_reachable": total_dbs,
            "profiles_with_errors": with_errors,
            "count": len(per_profile),
            "with_counts": with_counts,
        }
        if with_counts:
            # Per-profile and grand totals so the LLM can compose the
            # STATS-EXAMPLE-DRILL line without re-summing dicts itself.
            grand_schemas = 0
            grand_tables = 0
            for payload in per_profile.values():
                p_schemas = 0
                p_tables = 0
                for entry in (payload.get("databases") or []) + (payload.get("catalogs") or []):
                    if isinstance(entry, dict):
                        p_schemas += int(entry.get("schema_count") or 0)
                        p_tables += int(entry.get("table_count") or 0)
                payload["total_schemas"] = p_schemas
                payload["total_tables"] = p_tables
                grand_schemas += p_schemas
                grand_tables += p_tables
            result["grand_total_schemas"] = grand_schemas
            result["grand_total_tables"] = grand_tables
        return result

    # ------------------------------------------------------------ dtype family
    # Map a user-supplied dtype token to a concrete SQL-LIKE pattern set so
    # 'boolean' covers BOOL/BOOLEAN, 'int' covers BIGINT/INTEGER/SMALLINT,
    # 'date' covers DATE/TIMESTAMP/TIMESTAMPTZ, etc. Any unknown token is
    # passed through verbatim and matched as a substring against the column's
    # dtype field.
    _DTYPE_FAMILIES: dict[str, list[str]] = {
        # ``boolean`` matches the literal PG ``bool``/``boolean`` types
        # AND single-character fixed-width strings (``char(1)`` /
        # ``varchar(1)`` / ``character(1)``) which SAP and many legacy
        # schemas use as boolean flags ("X" / "" or "Y" / "N"). Without
        # the char(1) family, /ask "are there any boolean columns in
        # vbak?" would say "no" with confidence even though SAP vbak
        # has dozens of single-char flags (autlf, faksk, lifsk, ...).
        "boolean": [
            "bool",
            "boolean",
            "char(1)",
            "varchar(1)",
            "character(1)",
            "character varying(1)",
        ],
        "bool": [
            "bool",
            "boolean",
            "char(1)",
            "varchar(1)",
            "character(1)",
            "character varying(1)",
        ],
        "int": ["int", "integer", "bigint", "smallint", "tinyint", "mediumint"],
        # ``date`` is a SEMANTIC bucket — it covers every temporal
        # native type (``date``, ``timestamp``, ``timestamptz``,
        # ``datetime``, ``time``) so /ask "which tables have date
        # related columns" returns one set instead of forcing the LLM
        # to call once per dtype. Name-inferred date matches
        # (varchar columns whose NAME suggests date semantics —
        # ``erdat``, ``audat``, ``*_date``, ``created_at``, etc.) are
        # added in ``_tool_find_columns_by_dtype`` via a separate
        # name-pattern query, NOT as additional dtype tokens here.
        "date": [
            "date",
            "timestamp",
            "timestamptz",
            "datetime",
            "datetime2",
            "smalldatetime",
            "time",
            "timetz",
            "timestamp_ntz",
            "timestamp_ltz",
        ],
        "timestamp": [
            "timestamp",
            "timestamptz",
            "datetime",
            "datetime2",
            "smalldatetime",
            "timestamp_ntz",
            "timestamp_ltz",
        ],
        "time": ["time", "timetz"],
        "temporal": [
            "date",
            "timestamp",
            "timestamptz",
            "datetime",
            "datetime2",
            "smalldatetime",
            "time",
            "timetz",
            "timestamp_ntz",
            "timestamp_ltz",
        ],
        "integer": ["int", "integer", "bigint", "smallint", "tinyint", "mediumint"],
        "bigint": ["bigint"],
        "smallint": ["smallint", "int2"],
        "float": ["float", "double", "real", "numeric", "decimal"],
        "double": ["double", "float8"],
        "numeric": ["numeric", "decimal"],
        "decimal": ["numeric", "decimal"],
        "text": ["text", "varchar", "char", "string"],
        "varchar": ["varchar", "text", "char"],
        "string": ["text", "varchar", "char", "string"],
        "char": ["char", "varchar"],
        "datetime": ["timestamp", "timestamptz", "datetime"],
        "json": ["json", "jsonb"],
        "jsonb": ["jsonb"],
        "uuid": ["uuid"],
        "bytea": ["bytea", "blob", "binary"],
    }

    def _tool_find_columns_by_dtype(self, dtype: str, limit: int = 30) -> dict[str, Any]:
        token = (dtype or "").strip().lower()
        if not token:
            raise _ToolError("Argument 'dtype' is required.")
        family = self._DTYPE_FAMILIES.get(token, [token])
        from amx.search._catalog._db_profile_clause import build_db_profile_clause as _bdp

        profile_clause, profile_binds = _bdp(self.db_profile_filter, column="ce.db_profile")
        # Build a single SQL OR-set so we run one query.
        with self.catalog._connect() as conn:  # noqa: SLF001 — internal helper
            placeholders = ", ".join(["?"] * len(family))
            like_clause = " OR ".join(["LOWER(dtype) LIKE ?"] * len(family))
            query = f"""
                SELECT db_profile, schema_name, table_name, column_name, dtype,
                       effective_description
                FROM (
                    SELECT
                        ce.db_profile,
                        ce.schema_name,
                        ce.table_name,
                        ce.column_name,
                        ce.dtype,
                        cd.description_text AS effective_description
                    FROM catalog_entities ce
                    LEFT JOIN catalog_descriptions cd ON cd.id = ce.effective_description_id
                    WHERE {profile_clause}
                      AND ce.entity_kind = 'column'
                      AND ce.dtype IS NOT NULL
                ) WHERE LOWER(dtype) IN ({placeholders})
                   OR {like_clause}
                ORDER BY db_profile, schema_name, table_name, column_name
                LIMIT ?
            """
            params: list[Any] = list(profile_binds)
            params.extend(family)
            params.extend([f"%{f}%" for f in family])
            params.append(int(limit))
            rows = conn.execute(query, tuple(params)).fetchall()
        # Classify each match so the LLM can be honest in its
        # answer: native_boolean vs flag_candidate (single-char
        # fixed-width fields used as boolean flags by SAP / legacy
        # schemas). For non-boolean queries this is always
        # ``exact_dtype_match``.
        is_boolean_query = token in {"bool", "boolean"}
        is_temporal_query = token in {"date", "timestamp", "time", "temporal"}
        results: list[dict[str, Any]] = []
        for r in rows:
            dtype_raw = str(r["dtype"] or "")
            dtype_lower = dtype_raw.lower()
            if is_boolean_query:
                if dtype_lower in {"bool", "boolean"}:
                    kind = "native_boolean"
                elif "(1)" in dtype_lower and any(
                    base in dtype_lower for base in ("char", "varchar", "character")
                ):
                    kind = "flag_candidate"
                else:
                    kind = "exact_dtype_match"
            elif is_temporal_query:
                # Native temporal dtype hits.
                kind = "native_temporal"
            else:
                kind = "exact_dtype_match"
            results.append(
                {
                    "db_profile": str(r["db_profile"] or ""),
                    "schema": str(r["schema_name"] or ""),
                    "table": str(r["table_name"] or ""),
                    "column": str(r["column_name"] or ""),
                    "dtype": dtype_raw,
                    "description": str(r["effective_description"] or ""),
                    "kind": kind,
                }
            )

        # ── Name-pattern inference for semantic buckets ──
        # When the user asks about "date" (semantic) and the catalog
        # has SAP-style dates stored as varchar(8) / text, the
        # native-dtype query above misses them. Run a second query
        # against the same catalog that matches column names against
        # well-known temporal naming conventions, restricted to
        # string-family dtypes so we don't tag a numeric column as
        # date just because its name happens to contain "date".
        if is_temporal_query:
            seen_keys = {(r["schema"], r["table"], r["column"]) for r in results}
            name_patterns = [
                "%_date",
                "%_dt",
                "%_at",
                "%_time",
                "%_ts",
                "dat_%",
                "date_%",
                "time_%",
                "erdat",
                "audat",
                "ernam_dat",
                "letzd",
                "valid_from",
                "valid_to",
                "created%",
                "updated%",
                "modified%",
                "deleted%",
                "begda",
                "endda",
                "rldat",
                "psotg",
                "tzonso",
            ]
            string_dtypes_like = ["%char%", "%text%", "%string%", "%varchar%"]
            with self.catalog._connect() as conn:  # noqa: SLF001
                # OR-of name LIKE patterns AND OR-of string dtype LIKE patterns
                name_like_clause = " OR ".join("LOWER(column_name) LIKE ?" for _ in name_patterns)
                dtype_like_clause = " OR ".join("LOWER(dtype) LIKE ?" for _ in string_dtypes_like)
                profile_clause2, profile_binds2 = _bdp(
                    self.db_profile_filter, column="ce.db_profile"
                )
                q = f"""
                    SELECT ce.db_profile, ce.schema_name, ce.table_name, ce.column_name,
                           ce.dtype,
                           cd.description_text AS effective_description
                    FROM catalog_entities ce
                    LEFT JOIN catalog_descriptions cd ON cd.id = ce.effective_description_id
                    WHERE {profile_clause2}
                      AND ce.entity_kind = 'column'
                      AND ce.dtype IS NOT NULL
                      AND ({name_like_clause})
                      AND ({dtype_like_clause})
                    ORDER BY ce.db_profile, ce.schema_name, ce.table_name, ce.column_name
                    LIMIT ?
                """
                params2: list[Any] = list(profile_binds2)
                params2.extend(name_patterns)
                params2.extend(string_dtypes_like)
                params2.append(int(limit))
                try:
                    name_rows = conn.execute(q, tuple(params2)).fetchall()
                except Exception:
                    name_rows = []
            for r in name_rows:
                schema_n = str(r["schema_name"] or "")
                table_n = str(r["table_name"] or "")
                column_n = str(r["column_name"] or "")
                if (schema_n, table_n, column_n) in seen_keys:
                    continue
                results.append(
                    {
                        "db_profile": str(r["db_profile"] or ""),
                        "schema": schema_n,
                        "table": table_n,
                        "column": column_n,
                        "dtype": str(r["dtype"] or ""),
                        "description": str(r["effective_description"] or ""),
                        "kind": "name_inferred_temporal",
                    }
                )
        # Roll up to (schema, table) so the LLM gets a clean per-table view.
        by_table: dict[tuple[str, str], list[dict[str, str]]] = {}
        for entry in results:
            key = (entry["schema"], entry["table"])
            by_table.setdefault(key, []).append(
                {
                    "column": entry["column"],
                    "dtype": entry["dtype"],
                    "description": entry["description"],
                    "kind": entry["kind"],
                }
            )
        tables = [
            {
                "schema": schema,
                "table": table,
                "matching_columns": cols,
                "match_count": len(cols),
            }
            for (schema, table), cols in by_table.items()
        ]
        return {
            "dtype": token,
            "matched_family": family,
            "table_count": len(tables),
            "column_count": len(results),
            "tables": tables,
        }

    # ── Data-quality / uniqueness probes (v0.10.2) ─────────────────────────

    _DATE_FORMAT_PATTERNS: list[tuple[str, str]] = [
        # (regex, label) — matched against samples; first match wins.
        (r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", "ISO 8601 timestamp"),
        (r"^\d{4}-\d{2}-\d{2}$", "YYYY-MM-DD"),
        (r"^\d{4}/\d{2}/\d{2}$", "YYYY/MM/DD"),
        (r"^\d{8}$", "YYYYMMDD"),
        (r"^\d{2}-\d{2}-\d{4}$", "DD-MM-YYYY"),
        (r"^\d{2}/\d{2}/\d{4}$", "DD/MM/YYYY"),
        (r"^\d{2}\.\d{2}\.\d{4}$", "DD.MM.YYYY"),
        (r"^\d{6}$", "YYMMDD or YYYYMM"),
        (r"^\d{2}/\d{2}/\d{2}$", "DD/MM/YY or MM/DD/YY"),
        (r"^\d{4}-\d{2}$", "YYYY-MM"),
    ]

    @staticmethod
    def _detect_date_format(samples: list[Any]) -> str:
        """Return the dominant date-format label across non-null samples.

        Returns the empty string when no pattern matches a majority of
        the samples. Used by ``inspect_data_quality`` for varchar/text
        columns whose stored type doesn't reveal the temporal format.
        """
        import re as _re

        clean = [str(s).strip() for s in samples if s is not None and str(s).strip()]
        if not clean:
            return ""
        counts: dict[str, int] = {}
        for value in clean:
            for pattern, label in ToolBox._DATE_FORMAT_PATTERNS:
                if _re.match(pattern, value):
                    counts[label] = counts.get(label, 0) + 1
                    break
        if not counts:
            return ""
        # Pick the most common; require at least 60% confidence so we
        # don't slap a date label on a column that just happens to
        # have a few date-shaped values.
        best_label, best_count = max(counts.items(), key=lambda kv: kv[1])
        if best_count / len(clean) >= 0.6:
            return best_label
        return ""

    def _tool_check_uniqueness(
        self,
        schema: str,
        table: str,
        columns: list[str] | None = None,
    ) -> dict[str, Any]:
        """Verify whether (col1, col2, ...) is unique across the table.

        Runs ``SELECT COUNT(*), COUNT(DISTINCT (cols))`` against the
        live DB. When ``columns`` is omitted, falls back to the
        table's declared primary key.
        """
        from sqlalchemy import text as _text

        schema_name = (schema or "").strip()
        table_name = (table or "").strip()
        if not schema_name or not table_name:
            raise _ToolError("Both 'schema' and 'table' are required.")

        # Resolve columns: explicit > primary key.
        target_cols = list(columns or [])
        if not target_cols:
            try:
                profile = self._live_db().profile_table(
                    schema_name,
                    table_name,
                    sample_size=0,
                )
                target_cols = list(profile.primary_key or [])
            except Exception as exc:
                return {
                    "schema": schema_name,
                    "table": table_name,
                    "found": False,
                    "error": f"Could not load table profile: {exc}",
                }
        if not target_cols:
            # No PK declared and the caller didn't pass columns. Don't
            # bounce back with "give me columns" — that's the literal
            # answer the user is trying to escape. Instead, run
            # inspect_data_quality so the LLM sees per-column distinct
            # ratios + can name the most likely candidate keys
            # (columns where distinct_ratio ≈ 1.0). The LLM can then
            # follow up with a targeted check_uniqueness call once it
            # has a hypothesis.
            try:
                quality = self._tool_inspect_data_quality(
                    schema_name,
                    table_name,
                    columns=None,
                )
            except Exception as exc:
                quality = {"error": str(exc)}
            candidate_cols = []
            if isinstance(quality, dict) and quality.get("found"):
                # Likely-unique columns first (distinct_ratio close to 1.0).
                for entry in quality.get("columns", []):
                    if entry.get("distinct_ratio", 0) >= 0.99:
                        candidate_cols.append(entry["column"])
            return {
                "schema": schema_name,
                "table": table_name,
                "columns": [],
                "found": False,
                "no_primary_key": True,
                "duplicate_summary": quality,
                "likely_unique_columns": candidate_cols,
                "hint": (
                    "No primary key is declared on this table. The "
                    "duplicate_summary above carries per-column distinct "
                    "ratios; columns with ratio ≈ 1.0 are likely unique. "
                    "Pick a candidate composite key and call "
                    "check_uniqueness again with explicit ``columns``, or "
                    "ask the user which key they care about."
                ),
            }

        db = self._live_db()
        adapter = db._adapter  # noqa: SLF001
        fqn = adapter.fully_qualified_name(schema_name, table_name)
        quoted_cols = [adapter.quote_identifier(c) for c in target_cols]
        col_tuple = ", ".join(quoted_cols)
        try:
            with db.engine.connect() as conn:
                # COUNT(DISTINCT (a, b, c)) is supported by all 4 backends
                # we target; the parens make it a row-tuple comparison.
                row = conn.execute(
                    _text(
                        f"SELECT COUNT(*) AS total, "
                        f"COUNT(DISTINCT ({col_tuple})) AS distinct_count "
                        f"FROM {fqn}"
                    ),
                ).fetchone()
        except Exception as exc:
            return {
                "schema": schema_name,
                "table": table_name,
                "columns": target_cols,
                "found": False,
                "error": f"Uniqueness probe failed: {exc}",
            }
        total = int(row[0] or 0) if row else 0
        distinct = int(row[1] or 0) if row else 0
        duplicate_rows = max(0, total - distinct)
        ratio = (distinct / total) if total else 0.0
        return {
            "schema": schema_name,
            "table": table_name,
            "columns": target_cols,
            "total_rows": total,
            "distinct_rows": distinct,
            "duplicate_rows": duplicate_rows,
            "uniqueness_ratio": round(ratio, 6),
            "is_unique": (total > 0 and total == distinct),
        }

    def _tool_inspect_data_quality(
        self,
        schema: str,
        table: str,
        columns: list[str] | None = None,
    ) -> dict[str, Any]:
        """Per-column live-DB stats: nulls, distincts, min/max, date format.

        Loads a single TableProfile (sampled) and returns a per-column
        dict so the LLM has one map for "how nullable is X", "what's
        the min/max of created_at", "is this a date column stored as
        varchar?".
        """
        schema_name = (schema or "").strip()
        table_name = (table or "").strip()
        if not schema_name or not table_name:
            raise _ToolError("Both 'schema' and 'table' are required.")
        try:
            # sample_size>0 enables the column stats collection (null
            # count, distinct count, min/max, samples) the existing
            # profiler already does. Use a small but informative sample
            # so this stays fast even on huge tables.
            profile = self._live_db().profile_table(
                schema_name,
                table_name,
                sample_size=50,
            )
        except Exception as exc:
            return {
                "schema": schema_name,
                "table": table_name,
                "found": False,
                "error": str(exc),
            }

        wanted = {c.lower() for c in (columns or [])}
        per_col: list[dict[str, Any]] = []
        total_rows = int(profile.row_count or 0)
        for cp in profile.columns:
            if wanted and cp.name.lower() not in wanted:
                continue
            non_null = max(0, total_rows - int(cp.null_count or 0))
            null_ratio = (int(cp.null_count or 0) / total_rows) if total_rows else 0.0
            distinct_ratio = (int(cp.distinct_count or 0) / total_rows) if total_rows else 0.0
            entry: dict[str, Any] = {
                "column": cp.name,
                "dtype": str(cp.dtype),
                "nullable": bool(cp.nullable),
                "row_count": total_rows,
                "null_count": int(cp.null_count or 0),
                "non_null_count": non_null,
                "null_ratio": round(null_ratio, 6),
                "distinct_count": int(cp.distinct_count or 0),
                "distinct_ratio": round(distinct_ratio, 6),
                "min_value": (str(cp.min_val) if cp.min_val is not None else ""),
                "max_value": (str(cp.max_val) if cp.max_val is not None else ""),
            }
            # Detected date format — only meaningful for string-family
            # dtypes (varchar / text / char). Native date / timestamp
            # columns advertise their format via dtype itself.
            dtype_low = str(cp.dtype).lower()
            if any(token in dtype_low for token in ("char", "text", "string", "varchar")):
                fmt = self._detect_date_format(cp.samples or [])
                if fmt:
                    entry["detected_format"] = fmt
                    entry["likely_kind"] = "date_or_timestamp_in_string"
            per_col.append(entry)

        return {
            "schema": schema_name,
            "table": table_name,
            "found": True,
            "row_count": total_rows,
            "column_count": len(profile.columns),
            "columns": per_col,
        }

    def _tool_sample_column_values(
        self,
        schema: str,
        table: str,
        column: str,
        limit: int = 5,
    ) -> dict[str, Any]:
        """Pull a few distinct non-null example values from a single column.

        Direct ``SELECT DISTINCT col FROM schema.table WHERE col IS NOT
        NULL LIMIT N`` against the live DB — bypasses ``profile_table``
        (which scans every column + foreign keys + stats) so a "give
        me an example" question doesn't pay for a full table profile.
        """
        schema_name = (schema or "").strip()
        table_name = (table or "").strip()
        column_name = (column or "").strip()
        if not schema_name or not table_name or not column_name:
            raise _ToolError(
                "All of 'schema', 'table', 'column' are required.",
            )
        n = max(1, min(int(limit or 5), 50))

        try:
            samples, distinct_count = _sample_distinct_values(
                self._live_db(), schema_name, table_name, column_name, n
            )
        except Exception as exc:
            return {
                "schema": schema_name,
                "table": table_name,
                "column": column_name,
                "found": False,
                "error": str(exc),
                "hint": (
                    "If the schema/table didn't resolve, call "
                    "find_table_by_name first — the user may have "
                    "given a bare table name and the agent picked the "
                    "wrong schema."
                ),
            }

        return {
            "schema": schema_name,
            "table": table_name,
            "column": column_name,
            "found": True,
            "samples": samples,
            "sample_count": len(samples),
            "distinct_count": distinct_count,
        }
