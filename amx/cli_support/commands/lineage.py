"""``/lineage`` namespace commands.

Cache-first by construction: no command opens a live DB connection
without the user explicitly answering ``y`` to the cost-confirm prompt
(or passing ``--prefetch`` / ``--no-cache``).

Wizard-first by construction: every subcommand, when invoked bare,
walks through ``ask_choice`` / ``ask`` pickers for profile → database
→ schema → table → column → format → cache strategy. Flags stay
available for power users but are never required.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import click

from amx.config import AMXConfig, DBConfig
from amx.lineage import service
from amx.lineage import store as lineage_store
from amx.lineage.extractors.view_ddl import ConnectorHandle
from amx.lineage.native import (
    LineageFetchService,
    NativeLineageError,
    supported_backends,
)
from amx.lineage.render import (
    SUPPORTED_FORMATS,
    DotBinaryNotFound,
    open_artifact,
)
from amx.lineage.service import CacheMissReport, FillDecision, LineageRunResult, ScaleVerdict
from amx.lineage.types import ColumnRef, Scope
from amx.storage.sqlite_store import history_store
from amx.utils.console import (
    ask,
    ask_choice,
    confirm,
    error,
    heading,
    info,
    render_table,
    success,
    warn,
)

LogEvent = Callable[..., None]

_DEFAULT_FORMAT = "svg"
_DEFAULT_DEPTH = 1

_CACHE_STRATEGY_LABELS = {
    "cache-only": "use only cached data, never call the DB (safest, fastest)",
    "ask if needed": "if a cache miss occurs, prompt for permission before any DB call",
    "fetch from DB": "fill cache misses immediately (auto-confirm; pulls fresh DDL)",
    "force fresh (no cache)": "invalidate cached view DDL and re-pull from DB",
}


def register_lineage_commands(
    main: click.Group,
    *,
    pass_config: Callable[[Callable[..., Any]], Callable[..., Any]],
    log_event: LogEvent,
) -> click.Group:
    """Attach ``/lineage`` namespace commands to the main Click group."""

    @main.group()
    def lineage() -> None:
        """Render and manage column-level lineage diagrams (cache-first)."""

    @lineage.command("create")
    @click.argument("anchor", required=False)
    @click.option("--column", "column", default=None, help="Restrict anchor to a specific column.")
    @click.option(
        "--out", "out", default=None, help="Output image path (defaults under ~/.amx/lineage)."
    )
    @click.option(
        "--format",
        "fmt",
        type=click.Choice(SUPPORTED_FORMATS, case_sensitive=False),
        default=None,
        help="Image format. Default: svg.",
    )
    @click.option("--depth-up", "depth_up", type=int, default=None, help="Upstream hop limit.")
    @click.option(
        "--depth-down", "depth_down", type=int, default=None, help="Downstream hop limit."
    )
    @click.option(
        "--name", "name", default=None, help="Artifact slug (default: derived from anchor)."
    )
    @click.option("--profile", "profile_flag", default=None, help="Override active DB profile.")
    @click.option(
        "--no-cache", is_flag=True, help="Force fresh DB fetch (refills cache afterwards)."
    )
    @click.option("--cache-only", is_flag=True, help="Refuse any DB hit. For scripted/CI use.")
    @click.option("--prefetch", is_flag=True, help="Auto-confirm the cache-fill prompt.")
    @click.option("--force", is_flag=True, help="Override scale guardrails.")
    @pass_config
    def lineage_create(
        cfg: AMXConfig,
        anchor: str | None,
        column: str | None,
        out: str | None,
        fmt: str | None,
        depth_up: int | None,
        depth_down: int | None,
        name: str | None,
        profile_flag: str | None,
        no_cache: bool,
        cache_only: bool,
        prefetch: bool,
        force: bool,
    ) -> None:
        hs = history_store()
        if hs is None:
            error("History store not initialised — run /db first.")
            return
        if cache_only and (prefetch or no_cache):
            error("--cache-only is mutually exclusive with --prefetch / --no-cache.")
            return

        heading("Lineage · create")
        profile_name, profile_cfg = _pick_profile(cfg, profile_flag)
        if profile_name is None:
            return

        database, schema, table, column = _pick_anchor_location(
            hs,
            profile=profile_name,
            profile_default_db=_default_database(profile_cfg),
            raw_anchor=anchor,
            preset_column=column,
            require_column=False,
        )
        if schema is None or table is None:
            return

        fmt = _pick_format() if fmt is None else fmt.lower()
        if fmt not in SUPPORTED_FORMATS:
            error(f"Unsupported format {fmt!r}. Choose one of {SUPPORTED_FORMATS}.")
            return

        if depth_up is None:
            depth_up = _pick_depth("Upstream hop limit", default=_DEFAULT_DEPTH)
        if depth_down is None:
            depth_down = _pick_depth("Downstream hop limit", default=_DEFAULT_DEPTH)

        anchor_ref = ColumnRef(database=database, schema=schema, table=table, column=column or "")
        slug = name or _ask_slug(anchor_ref)
        output_path = _ask_output_path(out, slug, fmt)
        if output_path is None:
            return

        strategy = _pick_cache_strategy(cache_only, no_cache, prefetch)
        fill_decision: FillDecision | None
        if strategy == "cache-only":
            fill_decision = "skip"
            no_cache = False
        elif strategy == "fetch from DB":
            fill_decision = "fill"
            no_cache = False
        elif strategy == "force fresh (no cache)":
            fill_decision = "fill"
            no_cache = True
        else:
            fill_decision = None  # 'ask if needed' → interactive prompt mid-run

        scope = Scope(
            profile=profile_name,
            anchor=anchor_ref,
            depth_up=depth_up,
            depth_down=depth_down,
            database=database,
            schema=schema,
        )

        if no_cache:
            # User chose force-fresh from the wizard; invalidate this scope.
            lineage_store.invalidate_view_definitions(
                hs, db_profile=profile_name, database=database, schema=schema
            )

        connector_factory = _build_connector_factory(cfg)

        try:
            result = service.create_lineage(
                hs=hs,
                scope=scope,
                name=slug,
                output_path=output_path,
                fmt=fmt,
                fill_prompt=_make_fill_prompt(),
                fill_decision=fill_decision,
                force_scale=force,
                soft_confirm=_make_soft_confirm(),
                connector_factory=connector_factory,
            )
        except LookupError as exc:
            error(str(exc))
            return
        except DotBinaryNotFound as exc:
            error(str(exc))
            return
        except Exception as exc:
            error(f"Lineage render failed: {exc}")
            return

        _print_run_result(result)
        log_event(
            event_type="lineage.create",
            status="aborted" if result.aborted else "ok",
            command="/lineage create",
            details={
                "profile": profile_name,
                "anchor": _ref_to_str(anchor_ref),
                "format": fmt,
                "node_count": result.node_count,
                "edge_count": result.edge_count,
                "extractors": result.extractors_used,
                "partial": result.extractors_partial,
            },
        )

    @lineage.command("fetch")
    @click.argument("anchor", required=False)
    @click.option("--profile", "profile_flag", default=None, help="Override active DB profile.")
    @click.option(
        "--with-columns",
        is_flag=True,
        help="Also fetch column-level lineage (one API call per anchor column).",
    )
    @pass_config
    def lineage_fetch(
        cfg: AMXConfig,
        anchor: str | None,
        profile_flag: str | None,
        with_columns: bool,
    ) -> None:
        """Fetch lineage for a table from the database's own lineage system.

        Reads the platform-native lineage (Unity Catalog for Databricks)
        for a user-picked table and records the upstream / downstream
        tables plus the producer / consumer assets it touches. Entities
        the active token cannot read are kept as name-only nodes so the
        relationship still shows; entities AMX already holds are linked
        in full.
        """
        hs = history_store()
        if hs is None:
            error("History store not initialised — run /db first.")
            return

        heading("Lineage · fetch")
        profile_name, profile_cfg = _pick_profile(cfg, profile_flag)
        if profile_name is None:
            return

        backend = (getattr(profile_cfg, "backend", "") or "").lower()
        if backend not in supported_backends():
            supported = ", ".join(sorted(supported_backends())) or "none"
            error(
                f"Native lineage fetch is not available for backend "
                f"{backend or '(unknown)'!r}. Supported backends: {supported}."
            )
            return

        database, schema, table, _column = _pick_anchor_location(
            hs,
            profile=profile_name,
            profile_default_db=_default_database(profile_cfg),
            raw_anchor=anchor,
            preset_column=None,
            require_column=False,
        )
        if schema is None or table is None:
            return

        fqn = ".".join(p for p in (database, schema, table) if p)

        from amx.search.catalog import SearchCatalog

        catalog = SearchCatalog(hs.db_path)
        svc = LineageFetchService(catalog)
        try:
            counts = svc.fetch(
                profile_name=profile_name,
                backend=backend,
                fqn=fqn,
                with_columns=with_columns,
            )
        except NativeLineageError as exc:
            error(str(exc))
            log_event(
                event_type="lineage.fetch",
                status="error",
                command="/lineage fetch",
                details={"profile": profile_name, "anchor": fqn, "error": str(exc)},
            )
            return
        except Exception as exc:  # noqa: BLE001
            error(f"Native lineage fetch failed: {exc}")
            return

        success(f"Fetched native lineage for {fqn}.")
        render_table(
            "Fetched",
            ["metric", "count"],
            [
                ["tables", str(counts.tables)],
                ["assets", str(counts.assets)],
                ["columns", str(counts.columns)],
                ["edges", str(counts.edges)],
                ["name-only nodes", str(counts.name_only)],
            ],
        )
        if counts.name_only:
            info(
                f"{counts.name_only} node(s) recorded by name only — you can see "
                "the relationship but not their contents (no read access)."
            )
        info("View the graph with /lineage show or open it in Studio.")
        log_event(
            event_type="lineage.fetch",
            status="ok",
            command="/lineage fetch",
            details={"profile": profile_name, "anchor": fqn, **counts.as_dict()},
        )

    @lineage.command("list")
    @click.option("--profile", "profile_flag", default=None, help="Filter by DB profile.")
    @pass_config
    def lineage_list(cfg: AMXConfig, profile_flag: str | None) -> None:
        hs = history_store()
        if hs is None:
            error("History store not initialised — run /db first.")
            return
        rows = lineage_store.list_lineage_artifacts(hs, db_profile=profile_flag or "")
        if not rows:
            info("No lineage artifacts yet. Try /lineage create.")
            return
        render_table(
            "Lineage artifacts",
            [
                "id",
                "name",
                "profile",
                "anchor",
                "fmt",
                "nodes",
                "edges",
                "extractors",
                "partial",
                "generated_at",
                "path",
            ],
            [
                [
                    str(r["id"]),
                    r["name"],
                    r["db_profile"],
                    _anchor_display(hs, r["anchor_entity_id"]),
                    r["format"],
                    str(r["node_count"]),
                    str(r["edge_count"]),
                    ",".join(r["extractors_used"]) or "-",
                    "yes" if r["extractors_partial"] else "no",
                    _fmt_time(r["generated_at"]),
                    r["output_path"],
                ]
                for r in rows
            ],
        )

    @lineage.command("open")
    @click.argument("name_or_id", required=False)
    @pass_config
    def lineage_open(cfg: AMXConfig, name_or_id: str | None) -> None:
        hs = history_store()
        if hs is None:
            error("History store not initialised — run /db first.")
            return
        artifact = _pick_artifact(hs, name_or_id, verb="open")
        if artifact is None:
            return
        path = Path(artifact["output_path"])
        if not path.exists():
            warn(f"File missing at {path} — run /lineage refresh {artifact['name']}.")
            return
        _warn_if_stale(hs, artifact)
        info(f"Opening {path}")
        if not open_artifact(path):
            info(f"Path: {path}")

    @lineage.command("refresh")
    @click.argument("name_or_id", required=False)
    @click.option(
        "--no-cache", is_flag=True, help="Invalidate view-cache and force fresh DB fetch."
    )
    @click.option("--cache-only", is_flag=True, help="Refuse any DB hit.")
    @click.option("--prefetch", is_flag=True, help="Auto-confirm the cache-fill prompt.")
    @click.option("--force", is_flag=True, help="Override scale guardrails.")
    @click.option("--yes", is_flag=True, help="Skip the overwrite confirmation.")
    @pass_config
    def lineage_refresh(
        cfg: AMXConfig,
        name_or_id: str | None,
        no_cache: bool,
        cache_only: bool,
        prefetch: bool,
        force: bool,
        yes: bool,
    ) -> None:
        hs = history_store()
        if hs is None:
            error("History store not initialised — run /db first.")
            return
        if cache_only and (prefetch or no_cache):
            error("--cache-only is mutually exclusive with --prefetch / --no-cache.")
            return
        heading("Lineage · refresh")
        artifact = _pick_artifact(hs, name_or_id, verb="refresh")
        if artifact is None:
            return
        if not yes and not confirm(
            f"This will overwrite {artifact['output_path']}. Continue?",
            default=True,
        ):
            warn("Cancelled.")
            return

        strategy = _pick_cache_strategy(cache_only, no_cache, prefetch)
        fill_decision: FillDecision | None
        if strategy == "cache-only":
            fill_decision = "skip"
            no_cache_effective = False
        elif strategy == "fetch from DB":
            fill_decision = "fill"
            no_cache_effective = False
        elif strategy == "force fresh (no cache)":
            fill_decision = "fill"
            no_cache_effective = True
        else:
            fill_decision = None
            no_cache_effective = False

        connector_factory = _build_connector_factory(cfg)
        try:
            result = service.refresh_lineage(
                hs=hs,
                artifact=artifact,
                fill_prompt=_make_fill_prompt(),
                fill_decision=fill_decision,
                no_cache=no_cache_effective,
                force_scale=force,
                soft_confirm=_make_soft_confirm(),
                connector_factory=connector_factory,
            )
        except DotBinaryNotFound as exc:
            error(str(exc))
            return
        except Exception as exc:
            error(f"Refresh failed: {exc}")
            return
        _print_run_result(result)
        log_event(
            event_type="lineage.refresh",
            status="aborted" if result.aborted else "ok",
            command="/lineage refresh",
            details={
                "id": artifact["id"],
                "node_count": result.node_count,
                "edge_count": result.edge_count,
                "partial": result.extractors_partial,
            },
        )

    @lineage.command("delete")
    @click.argument("name_or_id", required=False)
    @click.option("--yes", is_flag=True, help="Skip confirmation.")
    @pass_config
    def lineage_delete(cfg: AMXConfig, name_or_id: str | None, yes: bool) -> None:
        hs = history_store()
        if hs is None:
            error("History store not initialised — run /db first.")
            return
        heading("Lineage · delete")
        artifact = _pick_artifact(hs, name_or_id, verb="delete")
        if artifact is None:
            return
        path = Path(artifact["output_path"])
        if not yes and not confirm(
            f"Delete artifact {artifact['name']!r} and file {path}?",
            default=False,
        ):
            warn("Cancelled.")
            return
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass
        lineage_store.delete_lineage_artifact(hs, artifact_id=int(artifact["id"]))
        success(f"Deleted artifact {artifact['name']!r}.")
        log_event(
            event_type="lineage.delete",
            status="ok",
            command="/lineage delete",
            details={"id": artifact["id"]},
        )

    @lineage.command("show")
    @click.argument("anchor", required=False)
    @click.option("--column", "column", default=None, help="Restrict anchor to a specific column.")
    @click.option("--depth-up", "depth_up", type=int, default=None, help="Upstream hop limit.")
    @click.option(
        "--depth-down", "depth_down", type=int, default=None, help="Downstream hop limit."
    )
    @click.option("--profile", "profile_flag", default=None, help="Override active DB profile.")
    @pass_config
    def lineage_show(
        cfg: AMXConfig,
        anchor: str | None,
        column: str | None,
        depth_up: int | None,
        depth_down: int | None,
        profile_flag: str | None,
    ) -> None:
        hs = history_store()
        if hs is None:
            error("History store not initialised — run /db first.")
            return
        heading("Lineage · show")
        profile_name, profile_cfg = _pick_profile(cfg, profile_flag)
        if profile_name is None:
            return
        database, schema, table, column = _pick_anchor_location(
            hs,
            profile=profile_name,
            profile_default_db=_default_database(profile_cfg),
            raw_anchor=anchor,
            preset_column=column,
            require_column=False,
        )
        if schema is None or table is None:
            return

        if depth_up is None:
            depth_up = _pick_depth("Upstream hop limit", default=_DEFAULT_DEPTH)
        if depth_down is None:
            depth_down = _pick_depth("Downstream hop limit", default=_DEFAULT_DEPTH)

        anchor_ref = ColumnRef(database=database, schema=schema, table=table, column=column or "")
        scope = Scope(
            profile=profile_name,
            anchor=anchor_ref,
            depth_up=depth_up,
            depth_down=depth_down,
            database=database,
            schema=schema,
        )
        for line in service.text_tree(hs=hs, scope=scope):
            click.echo(line)

    @lineage.command("suggest")
    @click.argument("anchor", required=False)
    @click.option("--profile", "profile_flag", default=None, help="Override active DB profile.")
    @click.option(
        "--schema",
        "schema_flag",
        default=None,
        help="Bulk mode: AI-suggest every table in this schema (budget-gated).",
    )
    @click.option(
        "--budget-tokens",
        "budget_tokens",
        type=int,
        default=50_000,
        help="Bulk mode: hard token spend cap (default 50000).",
    )
    @click.option(
        "--budget-tables",
        "budget_tables",
        type=int,
        default=25,
        help="Bulk mode: hard table-count cap (default 25).",
    )
    @click.option("--yes", "skip_confirm", is_flag=True, help="Skip confirm prompts.")
    @pass_config
    def lineage_suggest(
        cfg: AMXConfig,
        anchor: str | None,
        profile_flag: str | None,
        schema_flag: str | None,
        budget_tokens: int,
        budget_tables: int,
        skip_confirm: bool,
    ) -> None:
        """Ask the active LLM to propose lineage edges for an anchor or whole schema.

        Strictly opt-in: this is the only command in the namespace that
        spends LLM tokens. Single-anchor mode (default) calls the LLM
        once for the picked table. Bulk mode (``--schema X``) iterates
        every catalogued table in the schema, hard-stops on the first
        of ``--budget-tokens`` / ``--budget-tables`` exceeded.
        """
        hs = history_store()
        if hs is None:
            error("History store not initialised — run /db first.")
            return

        # Bulk mode — schema-wide AI suggest.
        if schema_flag:
            heading("Lineage · AI suggest (bulk)")
            profile_name, profile_cfg = _pick_profile(cfg, profile_flag)
            if profile_name is None:
                return
            database = _default_database(profile_cfg)
            if not skip_confirm and not confirm(
                f"Bulk AI-suggest every table in {schema_flag!r} via "
                f"{getattr(cfg.llm, 'provider', '?')}/{getattr(cfg.llm, 'model', '?')} "
                f"(budget: {budget_tokens} tokens / {budget_tables} tables). Continue?",
                default=False,
            ):
                warn("Cancelled.")
                return
            try:
                bulk = service.suggest_lineage_llm_bulk(
                    hs=hs,
                    profile=profile_name,
                    schema=schema_flag,
                    database=database,
                    cfg=cfg,
                    budget_tokens=budget_tokens,
                    budget_tables=budget_tables,
                )
            except Exception as exc:
                error(f"Bulk suggestion failed: {exc}")
                return
            if bulk.aborted:
                warn(f"Aborted: {bulk.abort_reason}")
                return
            success(
                f"Examined {bulk.tables_examined} table(s); "
                f"{bulk.tables_with_edges} carried edges; "
                f"{bulk.total_edges_persisted} edges persisted; "
                f"{bulk.total_tokens_used} tokens spent. "
                f"Halted by: {bulk.halted_by or 'completed'}."
            )
            log_event(
                event_type="lineage.suggest_bulk",
                status="ok",
                command="/lineage suggest --schema",
                details={
                    "profile": profile_name,
                    "schema": schema_flag,
                    "model": bulk.model,
                    "tables_examined": bulk.tables_examined,
                    "tables_with_edges": bulk.tables_with_edges,
                    "edges": bulk.total_edges_persisted,
                    "tokens": bulk.total_tokens_used,
                    "halted_by": bulk.halted_by,
                },
            )
            return

        heading("Lineage · AI suggest")
        profile_name, profile_cfg = _pick_profile(cfg, profile_flag)
        if profile_name is None:
            return
        database, schema, table, _ = _pick_anchor_location(
            hs,
            profile=profile_name,
            profile_default_db=_default_database(profile_cfg),
            raw_anchor=anchor,
            preset_column=None,
            require_column=False,
        )
        if schema is None or table is None:
            return
        anchor_ref = ColumnRef(database=database, schema=schema, table=table, column="")
        scope = Scope(
            profile=profile_name,
            anchor=anchor_ref,
            depth_up=1,
            depth_down=1,
            database=database,
            schema=schema,
        )
        if not confirm(
            f"This will call the active LLM ({getattr(cfg.llm, 'provider', '?')}/"
            f"{getattr(cfg.llm, 'model', '?')}) once for {_ref_to_str(anchor_ref)!r}. Continue?",
            default=True,
        ):
            warn("Cancelled.")
            return
        try:
            result = service.suggest_lineage_llm(hs=hs, scope=scope, cfg=cfg)
        except Exception as exc:
            error(f"LLM suggestion failed: {exc}")
            return
        if result.aborted:
            warn(f"Aborted: {result.abort_reason}")
            return
        if not result.edges:
            info("LLM returned no edges meeting the confidence threshold.")
            log_event(
                event_type="lineage.suggest",
                status="ok",
                command="/lineage suggest",
                details={"anchor": _ref_to_str(anchor_ref), "persisted": 0},
            )
            return
        success(
            f"Persisted {result.persisted_count} LLM-suggested edge(s) for "
            f"{_ref_to_str(anchor_ref)!r} using {result.model}."
        )
        for edge in result.edges:
            info(
                f"  {edge['from']} → {edge['to']}   "
                f"conf={edge['confidence']:.2f}   {edge['evidence']}"
            )
        log_event(
            event_type="lineage.suggest",
            status="ok",
            command="/lineage suggest",
            details={
                "anchor": _ref_to_str(anchor_ref),
                "persisted": result.persisted_count,
                "model": result.model,
            },
        )

    return lineage


# ── wizard pickers ───────────────────────────────────────────────────────


def _pick_profile(cfg: AMXConfig, flag: str | None) -> tuple[str | None, DBConfig | None]:
    """Resolve a profile via flag, sole-profile fallback, active profile, or picker."""
    profiles = getattr(cfg, "db_profiles", {}) or {}
    if not profiles:
        error("No DB profile available. Run /db add-profile first.")
        return None, None
    if flag:
        if flag in profiles:
            return flag, profiles[flag]
        error(f"Unknown profile {flag!r}. Available: {', '.join(sorted(profiles))}.")
        return None, None
    if len(profiles) == 1:
        name, cfg_obj = next(iter(profiles.items()))
        info(f"Using DB profile [bold]{name}[/bold] (only profile configured).")
        return name, cfg_obj
    names = sorted(profiles)
    active = (getattr(cfg, "active_db_profile", "") or "").strip()
    default = active if active in names else names[0]
    picked = ask_choice("Pick a DB profile", names, default=default)
    if not picked:
        error("No profile picked.")
        return None, None
    return picked, profiles[picked]


def _pick_anchor_location(
    hs: Any,
    *,
    profile: str,
    profile_default_db: str,
    raw_anchor: str | None,
    preset_column: str | None,
    require_column: bool,
) -> tuple[str | None, str | None, str | None, str | None]:
    """Pick (database, schema, table, column). ``raw_anchor`` short-circuits
    the wizard when it's a fully-qualified path.

    Path parsing convention:
      * ``table``                          → table
      * ``schema.table``                   → schema + table
      * ``schema.table.column``            → schema + table + column
      * ``database.schema.table.column``   → fully qualified
    """
    database = ""
    schema = ""
    table = ""
    column = preset_column

    if raw_anchor:
        parts = [p for p in re.split(r"[./]", raw_anchor) if p]
        if len(parts) == 1:
            table = parts[0]
        elif len(parts) == 2:
            schema, table = parts
        elif len(parts) == 3:
            schema, table = parts[0], parts[1]
            if not column:
                column = parts[2]
        elif len(parts) >= 4:
            database = parts[-4]
            schema = parts[-3]
            table = parts[-2]
            if not column:
                column = parts[-1]

    if not database:
        database = _pick_database(hs, profile, profile_default_db)
        if database is None:
            return None, None, None, None

    if not schema:
        schemas = _list_cached_schemas(hs, profile, database)
        if not schemas:
            error(
                f"No cached schemas under database {database!r}. "
                "Run /db inspect first to populate the catalog."
            )
            return None, None, None, None
        schema = ask_choice("Pick a schema", schemas, default=schemas[0])
        if not schema:
            error("No schema picked.")
            return None, None, None, None

    if not table:
        tables = _list_cached_tables(hs, profile, database, schema)
        if not tables:
            error(
                f"No cached tables under schema {schema!r}. Run /db inspect on this schema first."
            )
            return None, None, None, None
        table = ask_choice("Pick a table", tables, default=tables[0])
        if not table:
            error("No table picked.")
            return None, None, None, None

    if not column:
        # Offer column-level anchor as an optional refinement.
        columns = _list_cached_columns(hs, profile, database, schema, table)
        if columns:
            options = ["(whole table)", *columns]
            picked = ask_choice("Anchor on a column? (optional)", options, default="(whole table)")
            if picked and picked != "(whole table)":
                column = picked
        elif require_column:
            error(f"No columns cached for {schema}.{table}.")
            return None, None, None, None

    return database, schema, table, column


def _pick_database(hs: Any, profile: str, profile_default: str) -> str | None:
    """Resolve a database via the profile pin, sole-cached fallback, or picker.

    Returns ``None`` when the user explicitly aborts the picker. An empty
    string return means "no database scope" — flat backends (SQLite,
    DuckDB single-file) carry no database hierarchy, so the downstream
    catalog query is keyed on ``database_name = ''`` and still works.
    """
    if profile_default:
        return profile_default
    cached = _list_cached_databases(hs, profile)
    if not cached:
        # Nothing in the catalog — fall back to the empty-database scope
        # so flat profiles keep working. The schema picker below will
        # complain if nothing is cached there either.
        return ""
    if len(cached) == 1:
        only = cached[0]
        label = only or "(unspecified)"
        info(f"Using database [bold]{label}[/bold] (only cached database for this profile).")
        return only
    picked = ask_choice("Pick a database", cached, default=cached[0])
    if not picked:
        error("No database picked.")
        return None
    return picked


def _pick_format() -> str:
    return ask_choice(
        "Output format",
        list(SUPPORTED_FORMATS),
        default=_DEFAULT_FORMAT,
        descriptions={
            "svg": "scalable vector — best for browsers + design tools (default)",
            "png": "raster bitmap — best for slide decks",
            "jpg": "compressed raster — smaller file, lossy",
        },
    )


def _pick_depth(label: str, *, default: int) -> int:
    raw = ask(f"{label} (Enter for {default})", default=str(default)).strip()
    if not raw:
        return default
    try:
        value = int(raw)
        return max(1, min(value, 5))
    except ValueError:
        warn(f"Could not parse {raw!r} as int — using default {default}.")
        return default


def _pick_cache_strategy(cache_only: bool, no_cache: bool, prefetch: bool) -> str:
    """Pick a cache strategy. Flags short-circuit the wizard."""
    if cache_only:
        return "cache-only"
    if no_cache:
        return "force fresh (no cache)"
    if prefetch:
        return "fetch from DB"
    options = list(_CACHE_STRATEGY_LABELS.keys())
    return ask_choice(
        "Cache strategy",
        options,
        default="cache-only",
        descriptions=_CACHE_STRATEGY_LABELS,
    )


def _pick_artifact(hs: Any, name_or_id: str | None, *, verb: str) -> dict[str, Any] | None:
    """Resolve an existing artifact via id/slug or interactive picker."""
    if name_or_id:
        artifact = lineage_store.lookup_lineage_artifact(hs, name_or_id=name_or_id)
        if artifact is None:
            error(f"No artifact named {name_or_id!r}.")
        return artifact
    rows = lineage_store.list_lineage_artifacts(hs)
    if not rows:
        info("No lineage artifacts to pick from. Run /lineage create first.")
        return None
    labels = [
        f"{r['id']}: {r['name']}  ({_anchor_display(hs, r['anchor_entity_id'])}, "
        f"{r['format']}, {_fmt_time(r['generated_at'])})"
        for r in rows
    ]
    picked_label = ask_choice(f"Pick artifact to {verb}", labels, default=labels[0])
    if not picked_label:
        error("No artifact picked.")
        return None
    return rows[labels.index(picked_label)]


def _ask_slug(ref: ColumnRef) -> str:
    default = _default_slug(ref)
    raw = ask(f"Artifact slug (Enter for {default!r})", default=default).strip()
    return raw or default


def _ask_output_path(out: str | None, slug: str, fmt: str) -> Path | None:
    if out is None:
        from amx.config import _resolve_config_dir as resolve_config_dir

        base = Path(resolve_config_dir()) / "lineage"
        suggested = base / f"{slug}.{fmt}"
        raw = ask(
            f"Output path (Enter for {suggested})",
            default=str(suggested),
        ).strip()
        out = raw or str(suggested)
    return _resolve_output_path(out, slug, fmt)


# ── catalog readers (cache-only) ─────────────────────────────────────────


def _list_cached_databases(hs: Any, profile: str) -> list[str]:
    """Return distinct, non-empty cached databases for the profile.

    Empty-string ``database_name`` rows are filtered out — they get
    written by upstream callers that don't carry a database scope
    (connection probes, schema-level entities). Surfacing them would
    show up as a confusing blank picker entry. When *every* cached
    row is empty (flat backends like SQLite) the caller falls back to
    the empty scope on its own.
    """
    with hs._connect() as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT database_name
            FROM catalog_entities
            WHERE db_profile = ? AND database_name IS NOT NULL AND database_name <> ''
            ORDER BY database_name
            """,
            (profile,),
        ).fetchall()
    return [str(r[0]) for r in rows]


def _list_cached_schemas(hs: Any, profile: str, database: str) -> list[str]:
    with hs._connect() as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT schema_name
            FROM catalog_entities
            WHERE db_profile = ? AND database_name = ?
              AND schema_name <> ''
            ORDER BY schema_name
            """,
            (profile, database),
        ).fetchall()
    return [str(r[0]) for r in rows]


def _list_cached_tables(hs: Any, profile: str, database: str, schema: str) -> list[str]:
    with hs._connect() as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT table_name
            FROM catalog_entities
            WHERE db_profile = ? AND database_name = ? AND schema_name = ?
              AND table_name <> '' AND entity_kind = 'table'
            ORDER BY table_name
            """,
            (profile, database, schema),
        ).fetchall()
    return [str(r[0]) for r in rows]


def _list_cached_columns(
    hs: Any, profile: str, database: str, schema: str, table: str
) -> list[str]:
    with hs._connect() as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT column_name
            FROM catalog_entities
            WHERE db_profile = ? AND database_name = ? AND schema_name = ?
              AND table_name = ?
              AND column_name IS NOT NULL AND column_name <> ''
              AND entity_kind = 'column'
            ORDER BY column_name
            """,
            (profile, database, schema, table),
        ).fetchall()
    return [str(r[0]) for r in rows]


# ── shared helpers ───────────────────────────────────────────────────────


def _default_database(profile_cfg: DBConfig | None) -> str:
    if profile_cfg is None:
        return ""
    return getattr(profile_cfg, "database", "") or ""


def _build_connector_factory(cfg: AMXConfig):
    """Return a ConnectorFactory closed over the live config.

    The factory imports DatabaseConnector lazily so adapters that need
    optional driver wheels are only pulled when the user actually opts
    into a db_fill round-trip.
    """

    def factory(profile_name: str) -> ConnectorHandle | None:
        profiles = getattr(cfg, "db_profiles", {}) or {}
        profile_cfg = profiles.get(profile_name)
        if profile_cfg is None:
            return None
        try:
            from amx.db.connector import DatabaseConnector

            connector = DatabaseConnector(profile_cfg, profile_name=profile_name)
            return ConnectorHandle(
                engine=connector.engine,
                adapter=connector._adapter,
                backend=connector.backend,
            )
        except Exception as exc:  # pragma: no cover - defensive
            warn(f"Could not open connector for profile {profile_name!r}: {exc}")
            return None

    return factory


def _make_fill_prompt() -> Callable[[CacheMissReport], FillDecision]:
    def prompter(report: CacheMissReport) -> FillDecision:
        scopes = (
            ", ".join(
                f"{s.schema}" if not s.database else f"{s.database}.{s.schema}"
                for s in report.missing_scopes
            )
            or "(unknown)"
        )
        heading("Lineage cache miss")
        info(f"  Extractors needing fresh data: {', '.join(report.extractors_with_misses) or '-'}")
        info(f"  Schemas without cached view DDL: {scopes}")
        if report.estimated_views:
            info(
                f"  Estimated cost: ~{report.estimated_views} views, "
                f"~{report.estimated_seconds:.1f}s DB time."
            )
        choice = ask_choice(
            "Fetch from DB now?",
            ["cache-only (skip DB)", "fetch from DB", "abort"],
            default="cache-only (skip DB)",
        )
        if choice.startswith("fetch"):
            return "fill"
        if choice == "abort":
            return "abort"
        return "skip"

    return prompter


def _make_soft_confirm() -> Callable[[ScaleVerdict], bool]:
    def confirmer(verdict: ScaleVerdict) -> bool:
        return confirm(
            f"Graph has {verdict.node_count} nodes and {verdict.edge_count} edges. "
            "Rendering may be slow. Continue?",
            default=False,
        )

    return confirmer


def _resolve_output_path(out: str | None, slug: str, fmt: str) -> Path | None:
    if out:
        path = Path(out).expanduser()
    else:
        from amx.config import _resolve_config_dir as resolve_config_dir

        base = Path(resolve_config_dir()) / "lineage"
        path = base / f"{slug}.{fmt}"
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except OSError:
        error(f"Cannot create parent directory for {path}.")
        return None
    return path.resolve()


def _default_slug(ref: ColumnRef) -> str:
    parts = [p for p in (ref.schema, ref.table, ref.column) if p]
    base = "-".join(parts) or "lineage"
    return re.sub(r"[^A-Za-z0-9_-]+", "_", base)


def _anchor_display(hs: Any, anchor_entity_id: int) -> str:
    with hs._connect() as conn:
        row = conn.execute(
            "SELECT database_name, schema_name, table_name, column_name "
            "FROM catalog_entities WHERE id = ?",
            (int(anchor_entity_id),),
        ).fetchone()
    if not row:
        return f"#{anchor_entity_id} (missing)"
    parts = [str(p) for p in row if p]
    return ".".join(parts) if parts else f"#{anchor_entity_id}"


def _fmt_time(ts: float) -> str:
    try:
        return datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return "-"


def _print_run_result(result: LineageRunResult) -> None:
    if result.aborted:
        warn(f"Aborted: {result.abort_reason}")
        return
    success(f"Lineage written to {result.output_path}")
    info(
        f"  Nodes: {result.node_count}  Edges: {result.edge_count}  "
        f"Extractors: {', '.join(result.extractors_used) or '-'}"
        + (" (partial)" if result.extractors_partial else "")
    )


def _warn_if_stale(hs: Any, artifact: dict[str, Any]) -> None:
    """Best-effort hash comparison. A miss is just a warning, not a block."""
    scope_anchor = _anchor_from_db(hs, int(artifact["anchor_entity_id"]))
    if scope_anchor is None:
        return
    scope = Scope(
        profile=str(artifact["db_profile"]),
        anchor=scope_anchor,
        depth_up=int(artifact["depth_up"]),
        depth_down=int(artifact["depth_down"]),
        database=scope_anchor.database,
        schema=scope_anchor.schema,
    )
    extractors = service.build_default_extractors(connector_factory=None)
    try:
        edges, _, _ = service.gather_edges(hs, scope, extractors)
    except Exception:
        return
    current_hash = lineage_store.compute_edge_set_hash(
        service._edges_for_hash(hs, scope, edges)  # type: ignore[attr-defined]
    )
    if current_hash != artifact["edge_set_hash"]:
        warn(
            "The cached image may be stale — current edges differ from when it was rendered. "
            f"Run /lineage refresh {artifact['name']} to update."
        )


def _anchor_from_db(hs: Any, anchor_entity_id: int) -> ColumnRef | None:
    with hs._connect() as conn:
        row = conn.execute(
            "SELECT database_name, schema_name, table_name, column_name "
            "FROM catalog_entities WHERE id = ?",
            (int(anchor_entity_id),),
        ).fetchone()
    if not row:
        return None
    return ColumnRef(
        database=str(row[0] or ""),
        schema=str(row[1] or ""),
        table=str(row[2] or ""),
        column=str(row[3] or ""),
    )


def _ref_to_str(ref: ColumnRef) -> str:
    return ".".join(p for p in (ref.database, ref.schema, ref.table, ref.column) if p)
