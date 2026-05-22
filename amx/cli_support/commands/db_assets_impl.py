"""Implementation helpers behind /db ingest-assets and /db assets ... commands.

Keeps the click.command decorators in db_assets.py thin and the business
logic here testable in isolation.
"""

from __future__ import annotations

import re
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import click

from amx.config import AMXConfig
from amx.services.ingest_assets import (
    IngestAssetsService,
    IngestProgressEvent,
    IngestRequest,
)

ASSET_TYPES = [
    "notebooks",
    "jobs",
    "pipelines",
    "streamlit_apps",
    "streams",
    "task_dependencies",
    "queries",
]

_WINDOW_RX = re.compile(r"^(\d+)([dhm])$")


# PR-A: kinds the user can cherry-pick. Queries + task_dependencies
# are time-windowed aggregates filtered by ``history_days`` /
# ``query_history_limit`` — they're not per-asset rows.
_PICKABLE_KINDS = {"notebooks", "jobs", "pipelines", "streamlit_apps", "streams"}


def run_ingest_wizard(
    cfg: AMXConfig,
    *,
    profile: str | None,
    types_csv: str | None,
    history_days: int,
    runs_per_job: int,
    query_history_limit: int,
    include_ids: tuple[str, ...] = (),
) -> None:
    """Wizard-first ingestion: prompt for missing inputs, then run."""
    if types_csv:
        requested = [t.strip() for t in types_csv.split(",") if t.strip()]
        unknown = [t for t in requested if t not in ASSET_TYPES]
        if unknown:
            raise click.ClickException(
                f"Unknown asset type(s): {', '.join(unknown)}. Valid: {', '.join(ASSET_TYPES)}."
            )
        types = requested
    else:
        types = _prompt_types(ASSET_TYPES)
    if not types:
        click.echo("Nothing to do (no asset types selected).")
        return

    # PR-A: validate --include-id flags up front. Doing this before
    # opening the connector keeps the error surface tight — a typo
    # in the flag shouldn't depend on whether the DB profile happens
    # to be reachable. Wizard prompts are deferred to after the
    # connector is open, since they need the live adapter to browse.
    selection: dict[str, list[str]] | None = _parse_include_ids(include_ids, types)

    profile_name = _resolve_profile(cfg, profile)
    connector = _open_connector(cfg, profile_name)
    catalog = _open_catalog(cfg)
    svc = IngestAssetsService(connector=connector, catalog=catalog)

    if selection is None and types_csv is None:
        # Wizard mode: only prompt when stdin is interactive AND the
        # user picked at least one pickable kind. Non-interactive
        # (piped) sessions skip the prompt and fall back to "ingest
        # all", same as `--types ...` on the command line.
        pickable = [t for t in types if t in _PICKABLE_KINDS]
        if pickable and sys.stdin.isatty():
            if click.confirm(
                "Browse and pick specific assets instead of ingesting all?",
                default=False,
            ):
                selection = _browse_and_pick(connector, pickable)
                if selection is not None and not any(selection.values()):
                    click.echo("Nothing to do (no assets picked).")
                    return

    def on_progress(evt: IngestProgressEvent) -> None:
        if evt.state == "started":
            click.echo(f"  · {evt.asset_type}: starting...")
        elif evt.state == "completed":
            tail = "" if evt.count is None else f" ({evt.count})"
            click.echo(f"  ✓ {evt.asset_type}: done{tail}")
        elif evt.state in ("failed", "error"):
            click.echo(f"  ✗ {evt.asset_type}: failed — {evt.message}")
        elif evt.state == "skipped":
            click.echo(f"  · {evt.asset_type}: skipped — {evt.message}")

    click.echo(
        f"Ingesting assets for profile '{profile_name}' "
        f"({', '.join(types)}, history_days={history_days}, "
        f"runs_per_job={runs_per_job}):"
    )
    req = IngestRequest(
        profile_name=profile_name,
        types=types,
        history_days=history_days,
        runs_per_job=runs_per_job,
        query_history_limit=query_history_limit,
        selection=selection,
    )
    result = svc.run(req, progress=on_progress)
    summary = ", ".join(f"{k}={v}" for k, v in result.counts.items())
    click.echo(f"Done. Counts: {summary}")
    if result.failures:
        click.echo("Failures:")
        for k, v in result.failures.items():
            click.echo(f"  - {k}: {v}")


def _parse_include_ids(
    raw: tuple[str, ...], scoped_types: list[str]
) -> dict[str, list[str]] | None:
    """Parse repeated ``--include-id KIND:EXTERNAL_ID`` flags.

    Returns ``None`` when the user supplied no flags (so the caller
    keeps the pre-PR-A "ingest all" path); otherwise a dict
    ``{kind: [ids]}`` ready to plug into ``IngestRequest.selection``.

    Validates that every KIND is one of the pickable kinds **and**
    is in the user's selected ``types`` — passing
    ``--include-id queries:42`` would be silently ignored otherwise
    since the service can't filter on a time-windowed kind.
    """
    if not raw:
        return None
    out: dict[str, list[str]] = {}
    scoped = set(scoped_types)
    for token in raw:
        if ":" not in token:
            raise click.ClickException(f"--include-id expects KIND:EXTERNAL_ID, got {token!r}.")
        kind, ext_id = token.split(":", 1)
        kind = kind.strip()
        ext_id = ext_id.strip()
        if kind not in _PICKABLE_KINDS:
            raise click.ClickException(
                f"--include-id kind {kind!r} is not pickable. "
                f"Valid: {', '.join(sorted(_PICKABLE_KINDS))}."
            )
        if kind not in scoped:
            raise click.ClickException(
                f"--include-id refers to {kind!r} but --types does not include it. "
                "Add the kind to --types or drop the flag."
            )
        if not ext_id:
            raise click.ClickException(
                f"--include-id {token!r} is missing an EXTERNAL_ID after the colon."
            )
        out.setdefault(kind, []).append(ext_id)
    return out


def _browse_and_pick(connector, pickable: list[str]) -> dict[str, list[str]] | None:
    """Interactive "browse, then pick" CLI flow.

    Mirrors the Studio IngestDialog browse step: for each pickable
    kind the user has in scope, list its assets via the connector's
    cheap metadata methods and prompt for a comma-separated index
    selection. Returns ``None`` (use defaults) if the user bails
    out mid-flow; otherwise ``{kind: [ids]}`` (empty list for kinds
    the user skipped — those still get filtered to nothing).
    """
    selection: dict[str, list[str]] = {kind: [] for kind in pickable}
    method_map = {
        "notebooks": "list_remote_notebooks_metadata",
        "jobs": "list_remote_jobs_metadata",
        "pipelines": "list_remote_pipelines_metadata",
        "streamlit_apps": "list_remote_streamlit_apps_metadata",
        "streams": "list_remote_streams_metadata",
    }
    for kind in pickable:
        method = getattr(connector, method_map[kind], None)
        if method is None:
            click.echo(f"  · {kind}: profile adapter has no metadata listing — skipped.")
            continue
        try:
            items = list(method())
        except Exception as exc:  # noqa: BLE001
            click.echo(f"  ✗ {kind}: failed to browse — {exc}")
            continue
        if not items:
            click.echo(f"  · {kind}: none available — skipped.")
            continue
        click.echo(f"\nAvailable {kind} ({len(items)}):")
        for i, meta in enumerate(items, 1):
            tail = f"  ({meta.path})" if meta.path else ""
            click.echo(f"  [{i}] {meta.name}{tail}")
        raw = click.prompt(
            f"Pick {kind} (comma-separated indices, blank to skip kind)",
            default="",
            show_default=False,
        )
        if not raw.strip():
            continue
        picks: list[str] = []
        for tok in raw.split(","):
            tok = tok.strip()
            if not tok:
                continue
            try:
                idx = int(tok)
            except ValueError:
                click.echo(f"  · ignoring non-integer pick {tok!r}.")
                continue
            if 1 <= idx <= len(items):
                picks.append(items[idx - 1].external_id)
            else:
                click.echo(f"  · ignoring out-of-range pick {idx}.")
        selection[kind] = picks
    return selection


def _prompt_types(available: list[str]) -> list[str]:
    """Prompt for a comma-separated selection of asset types or 'all'."""
    click.echo("Select asset types to ingest (comma-separated indices, or 'all'):")
    for i, t in enumerate(available, 1):
        click.echo(f"  [{i}] {t}")
    raw = click.prompt("Choice", default="all")
    if raw.strip().lower() == "all":
        return list(available)
    picks: list[str] = []
    for tok in raw.split(","):
        tok = tok.strip()
        if not tok:
            continue
        try:
            idx = int(tok)
            if 1 <= idx <= len(available):
                picks.append(available[idx - 1])
        except ValueError:
            if tok in available:
                picks.append(tok)
    return picks


def _resolve_profile(cfg: AMXConfig, name: str | None) -> str:
    if name:
        return name
    active = cfg.active_db_profile
    if not active:
        raise click.ClickException(
            "No active DB profile. Run /use-db <profile> first or pass --profile."
        )
    return active


def _open_connector(cfg: AMXConfig, profile_name: str):
    """Open a DatabaseConnector for the named profile.

    Uses ``cfg.db_profiles[profile_name]`` as the DB config snapshot, since
    the active profile may differ from the one we're ingesting against.
    """
    from amx.db.connector import DatabaseConnector

    if profile_name not in cfg.db_profiles:
        raise click.ClickException(
            f"DB profile '{profile_name}' not found. Run /db-profiles to list."
        )
    db_cfg = cfg.db_profiles[profile_name]
    return DatabaseConnector(db_cfg, profile_name=profile_name)


def _open_catalog(cfg: AMXConfig):
    """Open a SearchCatalog rooted at the local SQLite history store."""
    from amx.search.catalog import SearchCatalog

    catalog = SearchCatalog.from_history_store()
    if catalog is None:
        raise click.ClickException(
            "No local history store found. Run /run at least once to initialise the store."
        )
    return catalog


# ── Helpers ────────────────────────────────────────────────────────────────


def _history_db_path(cfg: AMXConfig) -> Path:
    """Resolve the local history DB path from an AMXConfig."""
    config_dir = getattr(cfg, "CONFIG_DIR", None) or str(Path.home() / ".amx")
    return Path(config_dir) / "history.db"


def _ask_choice(label: str, options: list[str]) -> str:
    click.echo(f"{label}:")
    for i, opt in enumerate(options, 1):
        click.echo(f"  [{i}] {opt}")
    idx = click.prompt("Choice", type=int, default=1)
    if not 1 <= idx <= len(options):
        raise click.ClickException("Choice out of range")
    return options[idx - 1]


def _singular(asset_type: str) -> str:
    return {
        "notebooks": "notebook",
        "jobs": "job",
        "pipelines": "pipeline",
        "streamlit_apps": "streamlit",
        "streams": "stream",
        "queries": "query",
        "task_dependencies": "task_dependency",
    }.get(asset_type, asset_type)


# ── run_list ────────────────────────────────────────────────────────────────

# PR-C (scale): per-kind config consumed by ``run_list`` to apply
# search + pagination uniformly. ``name_col`` is the column used for
# the substring match + ORDER BY; ``path_expr`` is the second axis
# for search (kinds without a natural path use ``None``).
_LIST_SEARCH_CONFIG = {
    "notebooks": ("remote_notebooks", "name", "COALESCE(workspace_path, qualified_name, '')"),
    "jobs": ("remote_jobs", "name", None),
    "pipelines": ("remote_pipelines", "name", "COALESCE(target_schema, '')"),
    "streamlit_apps": ("remote_streamlit_apps", "qualified_name", None),
    "streams": ("remote_streams", "qualified_name", None),
    "task_dependencies": ("remote_task_dependencies", "parent_task_fqn", "child_task_fqn"),
    "queries": ("remote_queries", "name", None),
}


def _build_list_filter(asset_type: str, profile_name: str, search: str) -> tuple[str, list]:
    """Build the WHERE clause + params for ``run_list`` pagination."""
    spec = _LIST_SEARCH_CONFIG.get(asset_type)
    if spec is None:
        return "profile_name = ?", [profile_name]
    _table, name_col, path_expr = spec
    params: list = [profile_name]
    where = "profile_name = ?"
    needle = (search or "").strip().lower()
    if needle:
        like = f"%{needle}%"
        if path_expr:
            where += f" AND (LOWER({name_col}) LIKE ? OR LOWER({path_expr}) LIKE ?)"
            params.extend([like, like])
        else:
            where += f" AND LOWER({name_col}) LIKE ?"
            params.append(like)
    return where, params


def run_list(
    cfg,
    *,
    profile,
    asset_type,
    limit: int = 50,
    offset: int = 0,
    search: str = "",
):
    """List remote-ingested assets in a paginated tabular view.

    PR-C (scale): pagination + substring search keep the CLI usable
    past a few hundred assets. ``--limit`` caps the page size,
    ``--offset`` steps through pages, ``--search`` filters on name +
    path. When the result set is truncated, the footer surfaces the
    exact rerun command.
    """
    from rich.console import Console
    from rich.table import Table as RichTable

    profile_name = _resolve_profile(cfg, profile)
    if not asset_type:
        asset_type = _ask_choice("Asset type", ASSET_TYPES)
    if asset_type not in _LIST_SEARCH_CONFIG:
        raise click.ClickException(f"Unknown asset type: {asset_type}")
    where_sql, base_params = _build_list_filter(asset_type, profile_name, search)
    page_params = [*base_params, int(limit), int(offset)]
    db_path = _history_db_path(cfg)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        total = conn.execute(
            f"SELECT COUNT(*) FROM {_LIST_SEARCH_CONFIG[asset_type][0]} WHERE {where_sql}",  # noqa: S608 — identifiers controlled above
            base_params,
        ).fetchone()[0]
        if asset_type == "notebooks":
            rows = conn.execute(
                "SELECT id, name, "
                "COALESCE(workspace_path, qualified_name, '') AS path, "
                "platform, language, cell_count, last_modified_at, owner "
                f"FROM remote_notebooks WHERE {where_sql} "  # noqa: S608 — controlled above
                "ORDER BY name, path LIMIT ? OFFSET ?",
                page_params,
            ).fetchall()
            table = RichTable(title=f"Remote Notebooks ({profile_name})")
            table.add_column("ID")
            table.add_column("Name")
            # PR-B: Path disambiguates two notebooks with the same name in
            # different folders / schemas. Empty for legacy rows ingested
            # before the bridge captured the path.
            table.add_column("Path", overflow="fold")
            table.add_column("Platform")
            table.add_column("Lang")
            table.add_column("Cells")
            table.add_column("Last modified")
            table.add_column("Owner")
            for r in rows:
                table.add_row(
                    str(r["id"]),
                    r["name"] or "-",
                    r["path"] or "-",
                    r["platform"] or "-",
                    r["language"] or "-",
                    str(r["cell_count"] or "-"),
                    str(r["last_modified_at"] or "-"),
                    r["owner"] or "-",
                )
        elif asset_type == "jobs":
            rows = conn.execute(
                "SELECT id, job_id, name, schedule_cron, schedule_pause_status, "
                "last_run_status, success_rate_30d "
                f"FROM remote_jobs WHERE {where_sql} "  # noqa: S608 — controlled above
                "ORDER BY name, id LIMIT ? OFFSET ?",
                page_params,
            ).fetchall()
            table = RichTable(title=f"Remote Jobs ({profile_name})")
            for col in ("ID", "Job ID", "Name", "Schedule", "Pause", "Last run", "Success 30d"):
                table.add_column(col)
            for r in rows:
                rate = r["success_rate_30d"]
                rate_str = f"{rate:.0%}" if rate is not None else "-"
                table.add_row(
                    str(r["id"]),
                    str(r["job_id"]),
                    r["name"] or "-",
                    r["schedule_cron"] or "-",
                    r["schedule_pause_status"] or "-",
                    r["last_run_status"] or "-",
                    rate_str,
                )
        elif asset_type == "pipelines":
            rows = conn.execute(
                "SELECT id, pipeline_id, name, target_schema, edition, continuous, "
                "photon, latest_update_state "
                f"FROM remote_pipelines WHERE {where_sql} "  # noqa: S608 — controlled above
                "ORDER BY name, id LIMIT ? OFFSET ?",
                page_params,
            ).fetchall()
            table = RichTable(title=f"Remote Pipelines ({profile_name})")
            for col in ("ID", "Pipeline", "Name", "Target", "Edition", "Cont.", "Photon", "Latest"):
                table.add_column(col)
            for r in rows:
                table.add_row(
                    str(r["id"]),
                    r["pipeline_id"] or "-",
                    r["name"] or "-",
                    r["target_schema"] or "-",
                    r["edition"] or "-",
                    "yes" if r["continuous"] else "no",
                    "yes" if r["photon"] else "no",
                    r["latest_update_state"] or "-",
                )
        elif asset_type == "streamlit_apps":
            rows = conn.execute(
                "SELECT id, qualified_name, main_file, query_warehouse, owner, "
                "last_altered_at FROM remote_streamlit_apps "
                f"WHERE {where_sql} "  # noqa: S608 — controlled above
                "ORDER BY qualified_name, id LIMIT ? OFFSET ?",
                page_params,
            ).fetchall()
            table = RichTable(title=f"Streamlit Apps ({profile_name})")
            for col in ("ID", "Qualified name", "Main file", "Warehouse", "Owner", "Last altered"):
                table.add_column(col)
            for r in rows:
                table.add_row(
                    str(r["id"]),
                    r["qualified_name"] or "-",
                    r["main_file"] or "-",
                    r["query_warehouse"] or "-",
                    r["owner"] or "-",
                    str(r["last_altered_at"] or "-"),
                )
        elif asset_type == "streams":
            rows = conn.execute(
                "SELECT id, qualified_name, source_table_fqn, mode, stale_after, owner "
                f"FROM remote_streams WHERE {where_sql} "  # noqa: S608 — controlled above
                "ORDER BY qualified_name, id LIMIT ? OFFSET ?",
                page_params,
            ).fetchall()
            table = RichTable(title=f"Streams ({profile_name})")
            for col in ("ID", "Stream", "Source table", "Mode", "Stale after", "Owner"):
                table.add_column(col)
            for r in rows:
                table.add_row(
                    str(r["id"]),
                    r["qualified_name"] or "-",
                    r["source_table_fqn"] or "-",
                    r["mode"] or "-",
                    str(r["stale_after"] or "-"),
                    r["owner"] or "-",
                )
        elif asset_type == "task_dependencies":
            rows = conn.execute(
                "SELECT parent_task_fqn, child_task_fqn "
                f"FROM remote_task_dependencies WHERE {where_sql} "  # noqa: S608 — controlled above
                "ORDER BY parent_task_fqn, child_task_fqn "
                "LIMIT ? OFFSET ?",
                page_params,
            ).fetchall()
            table = RichTable(title=f"Task dependencies ({profile_name})")
            table.add_column("Parent")
            table.add_column("Child")
            for r in rows:
                table.add_row(r["parent_task_fqn"], r["child_task_fqn"])
        elif asset_type == "queries":
            rows = conn.execute(
                "SELECT id, platform, kind, name, warehouse, user_name, "
                "executed_at, duration_ms "
                f"FROM remote_queries WHERE {where_sql} "  # noqa: S608 — controlled above
                "ORDER BY COALESCE(executed_at, '0000') DESC, id LIMIT ? OFFSET ?",
                page_params,
            ).fetchall()
            table = RichTable(title=f"Queries ({profile_name})")
            for col in (
                "ID",
                "Platform",
                "Kind",
                "Name/Id",
                "Warehouse",
                "User",
                "Executed",
                "Dur ms",
            ):
                table.add_column(col)
            for r in rows:
                table.add_row(
                    str(r["id"]),
                    r["platform"] or "-",
                    r["kind"] or "-",
                    r["name"] or "-",
                    r["warehouse"] or "-",
                    r["user_name"] or "-",
                    str(r["executed_at"] or "-"),
                    str(r["duration_ms"] or "-"),
                )
        else:
            raise click.ClickException(f"Unknown asset type: {asset_type}")
    console = Console()
    console.print(table)
    # PR-C: pagination footer — surface "Showing X of Y" plus an
    # explicit rerun command so the user knows how to step forward
    # without guessing the flag name.
    shown_end = int(offset) + len(rows)
    footer = f"Showing {int(offset) + 1}–{shown_end} of {total} {asset_type}"
    if shown_end < total:
        next_offset = int(offset) + int(limit)
        more = (
            f" · next page: /db assets list --type {asset_type} "
            f"--limit {int(limit)} --offset {next_offset}"
        )
        if search:
            more += f' --search "{search}"'
        footer += more
    console.print(footer, style="dim")


# ── run_show ─────────────────────────────────────────────────────────────────


def _fetch_asset_by_identifier(conn, asset_type, profile_name, identifier):
    """Look up a single asset row by id, name, or ``name@path-prefix``.

    PR-B (path-as-identity) shapes the identifier into one of three
    resolution paths:

    * pure-digit string → match by ``id`` (legacy behaviour).
    * ``name@prefix`` form → match by name AND ``LIKE prefix%`` on the
      kind's path column. Lets the user disambiguate two same-name
      notebooks with ``etl@/Workspace/team-a``.
    * bare ``name`` (no ``@``) → match by name; an ambiguous result
      raises an ``_AmbiguousAsset`` error carrying every candidate
      so the caller can render a disambiguation list.
    """
    by_id = identifier.isdigit()
    table_map = {
        "notebooks": "remote_notebooks",
        "jobs": "remote_jobs",
        "pipelines": "remote_pipelines",
        "streamlit_apps": "remote_streamlit_apps",
        "streams": "remote_streams",
        "queries": "remote_queries",
    }
    if asset_type not in table_map:
        return None, []
    tbl = table_map[asset_type]
    if by_id:
        cur = conn.execute(
            f"SELECT * FROM {tbl} WHERE profile_name = ? AND id = ?",
            (profile_name, int(identifier)),
        )
        row = cur.fetchone()
        cols = [d[0] for d in cur.description] if cur.description else []
        return (dict(row) if row else None), cols

    name_col = "qualified_name" if asset_type in {"streamlit_apps", "streams"} else "name"
    path_expr = _path_expr_for(asset_type)
    name_part, _, path_prefix = identifier.partition("@")

    if path_prefix and path_expr:
        cur = conn.execute(
            f"SELECT * FROM {tbl} WHERE profile_name = ? "  # noqa: S608 — literals are controlled above
            f"AND {name_col} = ? AND {path_expr} LIKE ?",
            (profile_name, name_part, f"{path_prefix}%"),
        )
        candidates = cur.fetchall()
    else:
        cur = conn.execute(
            f"SELECT * FROM {tbl} WHERE profile_name = ? AND {name_col} = ?",
            (profile_name, name_part),
        )
        candidates = cur.fetchall()

    cols = [d[0] for d in cur.description] if cur.description else []
    if len(candidates) > 1:
        raise _AmbiguousAsset(
            asset_type=asset_type,
            identifier=identifier,
            candidates=[dict(c) for c in candidates],
            path_expr_label=path_expr,
        )
    return (dict(candidates[0]) if candidates else None), cols


def _path_expr_for(asset_type: str) -> str:
    """Return the path-column SQL expression for a kind, or ``''``.

    Notebooks store the path in ``workspace_path`` (Databricks) OR
    ``qualified_name`` (Snowflake); pipelines use ``target_schema``;
    everything else has no natural path disambiguator.
    """
    if asset_type == "notebooks":
        return "COALESCE(workspace_path, qualified_name, '')"
    if asset_type == "pipelines":
        return "COALESCE(target_schema, '')"
    return ""


class _AmbiguousAsset(click.ClickException):
    """Raised when a bare name maps to more than one asset row."""

    def __init__(
        self,
        *,
        asset_type: str,
        identifier: str,
        candidates: list[dict],
        path_expr_label: str,
    ) -> None:
        lines = [
            f"{len(candidates)} {asset_type} match '{identifier}'. "
            "Disambiguate with id, or with name@path-prefix:",
        ]
        for c in candidates:
            path = (
                c.get("workspace_path") or c.get("qualified_name") or c.get("target_schema") or ""
            )
            lines.append(
                f"  [{c.get('id')}] {c.get('name', '?')}{' (' + path + ')' if path else ''}"
            )
        super().__init__("\n".join(lines))
        self.asset_type = asset_type
        self.candidates = candidates
        self._path_expr_label = path_expr_label


def _list_downstream_tables(conn, *, asset_kind, asset_id):
    """Resolve catalog_entities rows that this asset references."""
    rows = conn.execute(
        """
        SELECT ce.database_name, ce.schema_name, ce.table_name
        FROM catalog_relationships cr
        JOIN catalog_entities ce ON ce.id = cr.to_entity_id
        WHERE cr.relationship_type = 'asset_references_table'
          AND cr.from_entity_kind = ?
          AND cr.from_entity_id = ?
        ORDER BY ce.database_name, ce.schema_name, ce.table_name
        """,
        (asset_kind, asset_id),
    ).fetchall()
    return [
        ".".join(filter(None, (r["database_name"], r["schema_name"], r["table_name"])))
        for r in rows
    ]


def run_show(cfg, *, identifier, profile, asset_type):
    """Show full detail (source, lineage, owner, ...) for one asset."""
    profile_name = _resolve_profile(cfg, profile)
    if not asset_type:
        asset_type = _ask_choice("Asset type", ASSET_TYPES)
    db_path = _history_db_path(cfg)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row, _columns = _fetch_asset_by_identifier(conn, asset_type, profile_name, identifier)
        if row is None:
            raise click.ClickException(
                f"No {asset_type} asset matches '{identifier}' in profile '{profile_name}'."
            )
        click.echo("=" * 70)
        click.echo(
            f"{asset_type.upper()} — {row.get('name') or row.get('qualified_name') or identifier}"
        )
        click.echo("=" * 70)
        for k, v in row.items():
            if k in {"source_text", "sql_text", "raw_definition_json"}:
                continue
            click.echo(f"  {k}: {v}")
        # Source / SQL body where applicable.
        if asset_type == "notebooks":
            click.echo("\n--- Source (normalized .ipynb) ---")
            try:
                import json as _json

                cells = _json.loads(row["source_text"]).get("cells", [])
                for i, cell in enumerate(cells, 1):
                    lang = cell.get("metadata", {}).get("language") or cell.get("cell_type", "")
                    body = "".join(cell.get("source", []))
                    click.echo(f"\n[cell {i} · {cell.get('cell_type', '?')} · {lang}]")
                    click.echo(body)
            except (_json.JSONDecodeError, AttributeError, TypeError):
                click.echo(row["source_text"])
        elif asset_type == "queries":
            click.echo("\n--- SQL ---")
            click.echo(row["sql_text"])
        downstream = _list_downstream_tables(
            conn, asset_kind=_singular(asset_type), asset_id=row["id"]
        )
        click.echo("\n--- Downstream tables ---")
        if not downstream:
            click.echo("  (none resolved)")
        else:
            for fqn in downstream:
                click.echo(f"  · {fqn}")


# ── run_search ───────────────────────────────────────────────────────────────


def run_search(cfg, *, query, profile, limit):
    """Semantic search across ingested remote assets.

    Tries the chunked + embedded :class:`AssetRAGStore` first
    (notebooks, queries, pipelines, streams, streamlit apps, jobs
    indexed at ingest time). Falls back to the legacy ``LIKE``
    substring scan over notebook source + saved-query SQL when the
    store is unavailable (Chroma not installed yet, no ingest run,
    or a one-time CollectionIdentityMismatch).
    """
    profile_name = _resolve_profile(cfg, profile)
    hits = _semantic_asset_search(cfg, query=query, profile=profile_name, limit=limit)
    if hits is None:
        hits = _like_asset_search(cfg, query=query, profile=profile_name, limit=limit)
    if not hits:
        click.echo(f"No remote assets matched '{query}' in profile '{profile_name}'.")
        return
    for h in hits[:limit]:
        score_str = f" score={h['score']:.2f}" if h.get("score") is not None else ""
        click.echo(f"  {h['tag']} {h['kind']} #{h['id']}: {h['name']} ({h['context']}){score_str}")


def _semantic_asset_search(cfg, *, query, profile, limit):
    try:
        from amx.assets.rag import AssetRAGStore
    except Exception:  # noqa: BLE001
        return None
    try:
        store = AssetRAGStore(cfg=cfg)
    except Exception:  # noqa: BLE001
        return None
    # Empty collection → tell the caller to fall back to LIKE. This
    # keeps the CLI useful for users who haven't run ``/db assets
    # reindex`` yet (e.g. legacy ingests from before the RAG store
    # landed); the LIKE channel still surfaces matches over the raw
    # source_text. Once the collection has at least one chunk the
    # store is the canonical channel — an empty *result set* (no
    # semantic match) is genuine and we don't fall back.
    if store.count() == 0:
        return None
    try:
        results = store.query(query, top_k=limit, profile=profile)
    except Exception:  # noqa: BLE001
        return None
    if not results:
        return []
    out = []
    for hit in results:
        out.append(
            {
                "kind": hit.kind,
                "id": hit.remote_id,
                "name": hit.name or hit.chunk_id,
                "tag": f"[rag:{hit.kind}]",
                "context": hit.metadata.get("workspace_path")
                or hit.metadata.get("warehouse")
                or hit.metadata.get("query_kind")
                or "",
                "score": hit.score,
            }
        )
    return out


def _like_asset_search(cfg, *, query, profile, limit):
    db_path = _history_db_path(cfg)
    pattern = f"%{query}%"
    hits = []
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        for row in conn.execute(
            "SELECT id, name, platform, language FROM remote_notebooks "
            "WHERE profile_name = ? AND source_text LIKE ? LIMIT ?",
            (profile, pattern, limit),
        ).fetchall():
            hits.append(
                {
                    "kind": "notebook",
                    "id": row["id"],
                    "name": row["name"],
                    "tag": f"[remote:{row['platform']}]",
                    "context": row["language"],
                    "score": None,
                }
            )
        for row in conn.execute(
            "SELECT id, platform, kind, name, external_id FROM remote_queries "
            "WHERE profile_name = ? AND sql_text LIKE ? LIMIT ?",
            (profile, pattern, limit),
        ).fetchall():
            display_name = row["name"] or row["external_id"]
            hits.append(
                {
                    "kind": "query",
                    "id": row["id"],
                    "name": display_name,
                    "tag": f"[remote:{row['platform']}]",
                    "context": row["kind"],
                    "score": None,
                }
            )
    return hits


# ── run_refresh ──────────────────────────────────────────────────────────────


_NOTEBOOK_STRATEGIES = ("whole", "cell", "char_window")
_QUERY_STRATEGIES = ("whole", "statement", "char_window")
_PIPELINE_STRATEGIES = ("metadata", "whole")


def run_chunking(cfg, *, show_only=False):
    """Interactive editor / printer for ``cfg.assets_chunking``."""
    from amx.assets.chunking_config import (
        NotebookChunkingConfig,
        PipelineChunkingConfig,
        QueryChunkingConfig,
    )

    ac = cfg.assets_chunking
    if show_only:
        _print_chunking(ac)
        return

    _print_chunking(ac)
    click.echo("\nUpdate per-kind settings (press Enter to keep the current value):\n")

    new_nb_strategy = _prompt_choice(
        "Notebook strategy", ac.notebook.strategy, _NOTEBOOK_STRATEGIES
    )
    new_nb_chunk = _prompt_int("Notebook chunk_chars", ac.notebook.chunk_chars, minimum=200)
    new_nb_overlap = _prompt_int("Notebook chunk_overlap", ac.notebook.chunk_overlap, minimum=0)

    new_q_strategy = _prompt_choice("Query strategy", ac.query.strategy, _QUERY_STRATEGIES)
    new_q_chunk = _prompt_int("Query chunk_chars", ac.query.chunk_chars, minimum=200)
    new_q_overlap = _prompt_int("Query chunk_overlap", ac.query.chunk_overlap, minimum=0)

    new_p_strategy = _prompt_choice("Pipeline strategy", ac.pipeline.strategy, _PIPELINE_STRATEGIES)

    cfg.assets_chunking = type(ac)(
        notebook=NotebookChunkingConfig(
            strategy=new_nb_strategy,
            chunk_chars=new_nb_chunk,
            chunk_overlap=new_nb_overlap,
        ),
        query=QueryChunkingConfig(
            strategy=new_q_strategy,
            chunk_chars=new_q_chunk,
            chunk_overlap=new_q_overlap,
        ),
        pipeline=PipelineChunkingConfig(strategy=new_p_strategy),
    )
    cfg.save()
    click.echo("\nSaved. Run /db assets reindex to re-embed under the new chunking.")
    _print_chunking(cfg.assets_chunking)


def _print_chunking(ac):
    click.echo("Asset chunking config:")
    click.echo(
        f"  notebook  strategy={ac.notebook.strategy:<12} "
        f"chunk_chars={ac.notebook.chunk_chars}  chunk_overlap={ac.notebook.chunk_overlap}"
    )
    click.echo(
        f"  query     strategy={ac.query.strategy:<12} "
        f"chunk_chars={ac.query.chunk_chars}  chunk_overlap={ac.query.chunk_overlap}"
    )
    click.echo(f"  pipeline  strategy={ac.pipeline.strategy}")
    click.echo("  stream / streamlit_app / job: metadata-only (one chunk per asset)")


def _prompt_choice(label, current, choices):
    raw = click.prompt(f"{label} {choices}", default=current, show_default=True).strip()
    if raw not in choices:
        click.echo(f"  '{raw}' not in {choices} — keeping '{current}'.", err=True)
        return current
    return raw


def _prompt_int(label, current, *, minimum):
    raw = click.prompt(label, default=str(current), show_default=True).strip()
    try:
        value = int(raw)
    except ValueError:
        click.echo(f"  '{raw}' not an int — keeping {current}.", err=True)
        return current
    if value < minimum:
        click.echo(f"  {value} < {minimum} — keeping {current}.", err=True)
        return current
    return value


def run_reindex(cfg, *, profile, skip_confirm, force: bool = False):
    """Drop the asset RAG collection and re-embed under the active model.

    Used after the user switches embedding providers (the
    ``EmbeddingProviderMismatch`` recovery path) or to repair a
    corrupted Chroma collection. Wraps
    :meth:`AssetRAGStore.reset_collection` + a fresh
    :meth:`ingest_profile` so the user sees one progress message
    rather than two separate calls.

    PR-D: ``force=True`` re-embeds every asset regardless of
    ``last_embedded_hash`` (the typical reason to run /reindex);
    ``force=False`` calls the incremental path so re-running the
    command after a partial failure only re-embeds what's still
    stale.
    """
    from amx.assets.rag import AssetRAGStore
    from amx.storage.sqlite_store import history_store

    profile_name = _resolve_profile(cfg, profile)
    if not skip_confirm and not click.confirm(
        f"Re-embed every ingested asset for profile '{profile_name}' "
        f"under the active embedding model? This drops the existing "
        f"Chroma collection.",
        default=False,
    ):
        click.echo("Cancelled.")
        return
    hs = history_store()
    if hs is None:
        click.echo("History store unavailable — run /db ingest-assets first.")
        return
    try:
        store = AssetRAGStore(cfg=cfg)
    except Exception as exc:  # noqa: BLE001
        click.echo(f"Could not open asset RAG store: {exc}")
        return
    store.reset_collection()
    with hs._connect() as conn:
        indexed = store.reindex_profile(conn=conn, profile_name=profile_name, force=force)
    click.echo(f"Re-indexed {indexed} chunks for profile '{profile_name}'.")


def run_refresh(cfg, *, profile, skip_confirm):
    """Drop and re-ingest all assets for a profile."""
    if not skip_confirm:
        if not click.confirm(
            "Drop all remote assets for this profile and re-ingest? "
            "(Re-ingest consumes tokens on the active LLM for some warehouses.)"
        ):
            click.echo("Cancelled.")
            return
    profile_name = _resolve_profile(cfg, profile)
    db_path = _history_db_path(cfg)
    with sqlite3.connect(db_path) as conn:
        for tbl in (
            "remote_notebooks",
            "remote_jobs",
            "remote_pipelines",
            "remote_streamlit_apps",
            "remote_streams",
            "remote_queries",
            "remote_task_dependencies",
        ):
            conn.execute(f"DELETE FROM {tbl} WHERE profile_name = ?", (profile_name,))
        conn.commit()
    click.echo(f"Cleared assets for profile '{profile_name}'. Re-ingesting all types...")
    run_ingest_wizard(
        cfg,
        profile=profile_name,
        types_csv=",".join(ASSET_TYPES),
        history_days=7,
        runs_per_job=20,
        query_history_limit=1000,
    )


# ── run_prune ────────────────────────────────────────────────────────────────


def run_prune(cfg, *, older_than, profile, skip_confirm):
    """Drop assets that haven't been re-ingested within the given time window."""
    m = _WINDOW_RX.match(older_than.strip())
    if not m:
        raise click.ClickException("--older-than must look like '30d', '7d', or '12h'.")
    n, unit = int(m.group(1)), m.group(2)
    seconds = {"d": 86400, "h": 3600, "m": 60}[unit] * n
    cutoff = (datetime.now(timezone.utc) - timedelta(seconds=seconds)).isoformat()

    profile_name = _resolve_profile(cfg, profile)
    if not skip_confirm:
        if not click.confirm(
            f"Drop remote assets last ingested before {cutoff} for profile '{profile_name}'?"
        ):
            click.echo("Cancelled.")
            return

    db_path = _history_db_path(cfg)
    dropped = {}
    with sqlite3.connect(db_path) as conn:
        for tbl in (
            "remote_notebooks",
            "remote_jobs",
            "remote_pipelines",
            "remote_streamlit_apps",
            "remote_streams",
            "remote_queries",
        ):
            cur = conn.execute(
                f"DELETE FROM {tbl} WHERE profile_name = ? AND ingested_at < ?",
                (profile_name, cutoff),
            )
            dropped[tbl] = cur.rowcount
        conn.commit()
    total = sum(dropped.values())
    summary = ", ".join(f"{k}={v}" for k, v in dropped.items() if v)
    click.echo(f"Dropped {total} rows: {summary or '(nothing matched)'}")


_KIND_TO_PRIMARY_TABLE = {
    "notebooks": ("remote_notebooks", "notebook"),
    "jobs": ("remote_jobs", "job"),
    "pipelines": ("remote_pipelines", "pipeline"),
    "streamlit_apps": ("remote_streamlit_apps", "streamlit"),
    "streams": ("remote_streams", "stream"),
    "queries": ("remote_queries", "query"),
}


def run_delete(cfg, *, identifier, asset_type, profile, skip_confirm):
    """Delete a single remote asset by id or by name within an asset type."""
    profile_name = _resolve_profile(cfg, profile)
    db_path = _history_db_path(cfg)

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row, kind_table, kind_singular = _resolve_asset_for_delete(
            conn, identifier=identifier, asset_type=asset_type, profile=profile_name
        )
        asset_id = row["id"]
        row_keys = row.keys()
        if "name" in row_keys and row["name"]:
            display = row["name"]
        elif "qualified_name" in row_keys:
            display = row["qualified_name"]
        else:
            display = f"#{asset_id}"

        if not skip_confirm:
            if not click.confirm(
                f"Delete {kind_singular} '{display}' (id={asset_id}) from profile '{profile_name}'?"
            ):
                click.echo("Cancelled.")
                return

        children = 0
        if kind_singular == "job":
            cur = conn.execute("DELETE FROM remote_job_tasks WHERE job_id_fk = ?", (asset_id,))
            children += cur.rowcount or 0
            cur = conn.execute("DELETE FROM remote_job_runs WHERE job_id_fk = ?", (asset_id,))
            children += cur.rowcount or 0

        edges = (
            conn.execute(
                "DELETE FROM catalog_relationships "
                "WHERE relationship_type = 'asset_references_table' "
                "AND from_entity_kind = ? AND from_entity_id = ?",
                (kind_singular, asset_id),
            ).rowcount
            or 0
        )

        primary = conn.execute(f"DELETE FROM {kind_table} WHERE id = ?", (asset_id,)).rowcount or 0
        conn.commit()

    click.echo(
        f"Deleted {kind_singular} #{asset_id} '{display}': "
        f"primary={primary}, children={children}, lineage_edges={edges}"
    )


def _resolve_asset_for_delete(conn, *, identifier, asset_type, profile):
    """Resolve identifier+asset_type to (row, table_name, kind_singular).

    Supports numeric ids (without --type by searching every kind) OR
    name lookups (require --type to disambiguate).
    """
    by_id = identifier.isdigit()
    if by_id:
        target_id = int(identifier)
        if asset_type:
            table, singular = _KIND_TO_PRIMARY_TABLE[asset_type]
            row = conn.execute(
                f"SELECT * FROM {table} WHERE id = ? AND profile_name = ?",
                (target_id, profile),
            ).fetchone()
            if row is None:
                raise click.ClickException(
                    f"No {asset_type} asset with id={target_id} in profile '{profile}'."
                )
            return row, table, singular
        # No --type: search every kind for the id.
        hits: list[tuple[sqlite3.Row, str, str]] = []
        for _kind, (table, singular) in _KIND_TO_PRIMARY_TABLE.items():
            row = conn.execute(
                f"SELECT * FROM {table} WHERE id = ? AND profile_name = ?",
                (target_id, profile),
            ).fetchone()
            if row is not None:
                hits.append((row, table, singular))
        if not hits:
            raise click.ClickException(
                f"No asset with id={target_id} in profile '{profile}'. "
                f"(Numeric ids are per-table; pass --type to scope.)"
            )
        if len(hits) > 1:
            kinds = ", ".join(h[2] for h in hits)
            raise click.ClickException(
                f"id={target_id} matches multiple asset kinds: {kinds}. "
                f"Pass --type to disambiguate."
            )
        return hits[0]
    # Identifier is a name: --type is required.
    if not asset_type:
        raise click.ClickException(
            "Identifier is a name; pass --type so AMX knows which table to scan."
        )
    table, singular = _KIND_TO_PRIMARY_TABLE[asset_type]
    name_col = "qualified_name" if asset_type in {"streamlit_apps", "streams"} else "name"
    row = conn.execute(
        f"SELECT * FROM {table} WHERE {name_col} = ? AND profile_name = ?",
        (identifier, profile),
    ).fetchone()
    if row is None:
        raise click.ClickException(
            f"No {asset_type} asset named {identifier!r} in profile '{profile}'."
        )
    return row, table, singular
