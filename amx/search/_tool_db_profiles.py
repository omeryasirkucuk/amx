"""``list_db_profiles`` tool for :class:`ToolBox`.

Surfaces the configured database profiles with their engine ``backend``
(``databricks`` / ``postgresql`` / ``duckdb`` / …) and non-secret
connection coordinates, plus a best-effort summary of what catalog data
is already available per profile (synced tables, docs/code presence,
ingested assets, lineage graphs, past runs).

This is the entry-point tool an MCP / IDE agent should call first: it
answers "which profile is Databricks vs Postgres?" and "what can I query
here?" without touching the live database — everything comes from config
and the cached history store.

Secrets are never emitted. The connection block is built by
*whitelisting* known non-secret coordinate fields (``_SAFE_COORD_FIELDS``),
so a credential field added to :class:`amx.config.DBConfig` later cannot
leak by default — it simply won't be on the allowlist.

The mixin is compose-only — it never overrides ``ToolBox.__init__``. It
relies on the host ``ToolBox`` providing ``self.cfg``, ``self.catalog``,
and ``self.db_profiles``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from amx.config import AMXConfig


class _DbProfilesToolMixin:
    """``list_db_profiles`` implementation."""

    # Provided by the host ``ToolBox`` instance.
    cfg: AMXConfig
    db_profiles: list[str]
    catalog: Any

    #: Non-secret connection coordinates safe to expose to an external
    #: IDE / LLM. Built as an allowlist (never a denylist) so any new
    #: credential field on ``DBConfig`` stays hidden until explicitly
    #: added here. Secrets (``password``, ``access_token``, ``jwt_token``,
    #: ``workspace_token``, ``motherduck_token``, ``credentials_path``, …)
    #: are deliberately absent.
    _SAFE_COORD_FIELDS: tuple[str, ...] = (
        "host",
        "port",
        "user",
        "database",
        "catalog",
        "account",
        "warehouse",
        "role",
        "http_path",
        "http_scheme",
        "project",
        "dataset",
        "location",
        "service_name",
        "cluster_identifier",
        "driver",
        "auth_mode",
    )

    #: Remote-asset tables keyed by ``profile_name``; counted best-effort
    #: and summed into a single ``assets`` figure.
    _ASSET_TABLES: tuple[str, ...] = (
        "remote_notebooks",
        "remote_jobs",
        "remote_pipelines",
        "remote_streamlit_apps",
        "remote_streams",
        "remote_queries",
    )

    def _tool_list_db_profiles(self) -> dict[str, Any]:
        """List configured DB profiles with engine type + data availability.

        Reads only config and the cached history store — never the live
        database. Every per-profile data count is best-effort: a failure
        yields ``null`` for that field rather than failing the whole tool.
        """
        profiles_cfg = getattr(self.cfg, "db_profiles", None) or {}
        if not profiles_cfg:
            return {
                "profiles": [],
                "count": 0,
                "active_scope": list(self.db_profiles),
                "note": "No database profiles are configured in AMX.",
            }

        # One default instance to distinguish "set by the user" from the
        # dataclass default, so we don't echo every profile's localhost:5432.
        default_db = self._default_dbconfig()
        active = set(self.db_profiles)

        # Open a single history-store connection for the whole call so we
        # don't reconnect per (profile, count). ``None`` when the store
        # isn't initialised (e.g. a stripped-down embedding context); the
        # data summary then degrades to ``null`` counts.
        conn = self._history_conn()
        try:
            out: list[dict[str, Any]] = []
            for name in profiles_cfg:
                profile = profiles_cfg[name]
                out.append(
                    {
                        "name": name,
                        "backend": str(getattr(profile, "backend", "") or ""),
                        "active": name in active,
                        "connection": self._safe_coords(profile, default_db),
                        "available_data": self._availability(name, conn),
                    }
                )
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:  # noqa: BLE001 - best-effort cleanup
                    pass

        return {
            "profiles": out,
            "count": len(out),
            "active_scope": list(self.db_profiles),
        }

    # ── helpers ────────────────────────────────────────────────────────

    @staticmethod
    def _default_dbconfig() -> Any:
        """A fresh ``DBConfig`` used as the "unset" baseline for coords."""
        from amx.config import DBConfig

        return DBConfig()

    def _safe_coords(self, profile: Any, default_db: Any) -> dict[str, Any]:
        """Non-secret connection coordinates the user actually set.

        A field is emitted only when it is truthy *and* differs from the
        ``DBConfig`` default, so unconfigured ``localhost`` / ``5432`` /
        ``https`` defaults don't add noise. Secrets are structurally
        excluded by the allowlist.
        """
        coords: dict[str, Any] = {}
        for field in self._SAFE_COORD_FIELDS:
            value = getattr(profile, field, None)
            if not value:
                continue
            if value == getattr(default_db, field, None):
                continue
            coords[field] = value
        return coords

    def _history_conn(self) -> Any | None:
        """A history-store SQLite connection, or ``None`` if unavailable."""
        try:
            from amx.storage.sqlite_store import history_store

            store = history_store()
            if store is None:
                return None
            return store._connect()  # noqa: SLF001 - documented store access
        except Exception:  # noqa: BLE001 - summary is best-effort
            return None

    def _availability(self, name: str, conn: Any | None) -> dict[str, Any]:
        """Best-effort per-profile data summary for the MCP/LLM reader."""
        summary: dict[str, Any] = {
            "synced_tables": None,
            "sync_state": None,
            "docs": None,
            "code": None,
            "assets": None,
            "lineage_graphs": None,
            "past_runs": None,
        }

        # Synced-table count + state come from the cached catalog state,
        # the same source ``catalog_sync_status`` reads (zero live calls).
        try:
            state = self.catalog.get_profile_state(name)
            summary["synced_tables"] = int(state.get("processed_tables") or 0)
            summary["sync_state"] = state.get("state") or "none"
        except Exception:  # noqa: BLE001
            pass

        # Docs / code: presence (not a full RAG scan) for this db scope —
        # tells the agent whether ``search_docs`` / ``search_code`` will
        # find anything, cheaply.
        try:
            from amx.search._agent.scope import (
                resolve_code_profiles_for_scope,
                resolve_doc_profiles_for_scope,
            )

            summary["docs"] = bool(resolve_doc_profiles_for_scope(self.cfg, [name]))
            summary["code"] = bool(resolve_code_profiles_for_scope(self.cfg, [name]))
        except Exception:  # noqa: BLE001
            pass

        if conn is not None:
            summary["assets"] = self._count_assets(conn, name)
            summary["lineage_graphs"] = self._count_where(
                conn, "lineage_artifacts", "db_profile", name
            )
            summary["past_runs"] = self._count_where(conn, "analysis_runs", "db_profile", name)

        return summary

    def _count_assets(self, conn: Any, profile: str) -> int | None:
        """Sum ingested remote assets for *profile* across kind tables.

        Each table is counted independently and guarded, so a schema that
        predates one of the tables contributes 0 instead of failing the
        whole figure. Returns ``None`` only if every table errors.
        """
        total = 0
        any_ok = False
        for table in self._ASSET_TABLES:
            count = self._count_where(conn, table, "profile_name", profile)
            if count is not None:
                any_ok = True
                total += count
        return total if any_ok else None

    @staticmethod
    def _count_where(conn: Any, table: str, column: str, value: str) -> int | None:
        """``SELECT COUNT(*) FROM <table> WHERE <column> = ?`` — guarded.

        ``table`` / ``column`` are module-internal constants (never user
        input), so the identifier interpolation carries no injection risk;
        ``value`` is always bound as a parameter.
        """
        try:
            cur = conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE {column} = ?",  # noqa: S608
                (value,),
            )
            row = cur.fetchone()
            return int(row[0]) if row else 0
        except Exception:  # noqa: BLE001 - missing table/column -> unknown
            return None
