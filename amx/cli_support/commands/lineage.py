"""``/lineage`` namespace commands.

Cache-first by construction: no command opens a live DB connection
without the user explicitly answering ``y`` to the cost-confirm prompt
(or passing ``--prefetch`` / ``--no-cache``).
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

        profile_name, profile_cfg = _resolve_profile(cfg, profile_flag)
        if profile_name is None:
            error("No DB profile available. Run /db add-profile first.")
            return

        if not anchor:
            anchor = ask("Anchor table (schema.table or schema.table.column)").strip()
            if not anchor:
                error("Anchor is required.")
                return

        parts = [p for p in re.split(r"[./]", anchor) if p]
        if len(parts) == 1:
            schema = ""
            table = parts[0]
        elif len(parts) == 2:
            schema, table = parts
        elif len(parts) >= 3:
            schema = parts[-3]
            table = parts[-2]
            if not column:
                column = parts[-1]
        else:
            error(f"Could not parse anchor {anchor!r}.")
            return

        database = _default_database(profile_cfg)
        anchor_ref = ColumnRef(database=database, schema=schema, table=table, column=column or "")

        fmt = (fmt or _DEFAULT_FORMAT).lower()
        if fmt not in SUPPORTED_FORMATS:
            error(f"Unsupported format {fmt!r}. Choose one of {SUPPORTED_FORMATS}.")
            return

        depth_up = depth_up if depth_up is not None else _DEFAULT_DEPTH
        depth_down = depth_down if depth_down is not None else _DEFAULT_DEPTH
        slug = name or _default_slug(anchor_ref)

        output_path = _resolve_output_path(out, slug, fmt)
        if output_path is None:
            error("Output path is invalid or not writable.")
            return

        scope = Scope(
            profile=profile_name,
            anchor=anchor_ref,
            depth_up=depth_up,
            depth_down=depth_down,
            database=database,
            schema=schema,
        )

        fill_decision: FillDecision | None
        if cache_only:
            fill_decision = "skip"
        elif prefetch or no_cache:
            fill_decision = "fill"
        else:
            fill_decision = None  # interactive prompt below

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
            info("No lineage artifacts yet. Try /lineage create <table>.")
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
    @click.argument("name_or_id", required=True)
    @pass_config
    def lineage_open(cfg: AMXConfig, name_or_id: str) -> None:
        hs = history_store()
        if hs is None:
            error("History store not initialised — run /db first.")
            return
        artifact = lineage_store.lookup_lineage_artifact(hs, name_or_id=name_or_id)
        if artifact is None:
            error(f"No artifact named {name_or_id!r}.")
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
    @click.argument("name_or_id", required=True)
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
        name_or_id: str,
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
        artifact = lineage_store.lookup_lineage_artifact(hs, name_or_id=name_or_id)
        if artifact is None:
            error(f"No artifact named {name_or_id!r}.")
            return
        if not yes and not confirm(
            f"This will overwrite {artifact['output_path']}. Continue?",
            default=True,
        ):
            warn("Cancelled.")
            return

        fill_decision: FillDecision | None
        if cache_only:
            fill_decision = "skip"
        elif prefetch:
            fill_decision = "fill"
        else:
            fill_decision = None

        connector_factory = _build_connector_factory(cfg)
        try:
            result = service.refresh_lineage(
                hs=hs,
                artifact=artifact,
                fill_prompt=_make_fill_prompt(),
                fill_decision=fill_decision,
                no_cache=no_cache,
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
    @click.argument("name_or_id", required=True)
    @click.option("--yes", is_flag=True, help="Skip confirmation.")
    @pass_config
    def lineage_delete(cfg: AMXConfig, name_or_id: str, yes: bool) -> None:
        hs = history_store()
        if hs is None:
            error("History store not initialised — run /db first.")
            return
        artifact = lineage_store.lookup_lineage_artifact(hs, name_or_id=name_or_id)
        if artifact is None:
            error(f"No artifact named {name_or_id!r}.")
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
    @click.argument("anchor", required=True)
    @click.option("--column", "column", default=None, help="Restrict anchor to a specific column.")
    @click.option(
        "--depth-up", "depth_up", type=int, default=_DEFAULT_DEPTH, help="Upstream hop limit."
    )
    @click.option(
        "--depth-down", "depth_down", type=int, default=_DEFAULT_DEPTH, help="Downstream hop limit."
    )
    @click.option("--profile", "profile_flag", default=None, help="Override active DB profile.")
    @pass_config
    def lineage_show(
        cfg: AMXConfig,
        anchor: str,
        column: str | None,
        depth_up: int,
        depth_down: int,
        profile_flag: str | None,
    ) -> None:
        hs = history_store()
        if hs is None:
            error("History store not initialised — run /db first.")
            return
        profile_name, profile_cfg = _resolve_profile(cfg, profile_flag)
        if profile_name is None:
            error("No DB profile available.")
            return
        parts = [p for p in re.split(r"[./]", anchor) if p]
        if len(parts) == 1:
            schema, table = "", parts[0]
        elif len(parts) == 2:
            schema, table = parts
        else:
            schema, table = parts[-3], parts[-2]
            column = column or parts[-1]
        anchor_ref = ColumnRef(
            database=_default_database(profile_cfg),
            schema=schema,
            table=table,
            column=column or "",
        )
        scope = Scope(
            profile=profile_name,
            anchor=anchor_ref,
            depth_up=depth_up,
            depth_down=depth_down,
            database=anchor_ref.database,
            schema=schema,
        )
        for line in service.text_tree(hs=hs, scope=scope):
            click.echo(line)

    return lineage


# ── helpers ─────────────────────────────────────────────────────────────


def _resolve_profile(cfg: AMXConfig, flag: str | None) -> tuple[str | None, DBConfig | None]:
    """Return (profile_name, DBConfig) or (None, None) when no profile resolves."""
    profiles = getattr(cfg, "db_profiles", {}) or {}
    if flag:
        if flag in profiles:
            return flag, profiles[flag]
        return None, None
    active = getattr(cfg, "active_db_profile", "") or ""
    if active and active in profiles:
        return active, profiles[active]
    # Fall back to the only-profile case (common on fresh installs).
    if len(profiles) == 1:
        only = next(iter(profiles.items()))
        return only[0], only[1]
    return None, None


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
        # Default under the AMX config dir so the file survives /clear.
        from amx.config import resolve_config_dir

        base = Path(resolve_config_dir()) / "lineage"
        path = base / f"{slug}.{fmt}"
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except OSError:
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
