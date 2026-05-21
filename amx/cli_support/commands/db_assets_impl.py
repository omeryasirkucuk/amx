"""Implementation helpers behind /db ingest-assets and /db assets ... commands.

Keeps the click.command decorators in db_assets.py thin and the business
logic here testable in isolation.
"""

from __future__ import annotations

import re
import sqlite3
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


def run_ingest_wizard(
    cfg: AMXConfig,
    *,
    profile: str | None,
    types_csv: str | None,
    history_days: int,
    runs_per_job: int,
    query_history_limit: int,
) -> None:
    """Wizard-first ingestion: prompt for missing inputs, then run."""
    profile_name = _resolve_profile(cfg, profile)
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

    connector = _open_connector(cfg, profile_name)
    catalog = _open_catalog(cfg)
    svc = IngestAssetsService(connector=connector, catalog=catalog)

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
    )
    result = svc.run(req, progress=on_progress)
    summary = ", ".join(f"{k}={v}" for k, v in result.counts.items())
    click.echo(f"Done. Counts: {summary}")
    if result.failures:
        click.echo("Failures:")
        for k, v in result.failures.items():
            click.echo(f"  - {k}: {v}")


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


def run_list(cfg, *, profile, asset_type):
    """List remote-ingested assets in a tabular view."""
    from rich.console import Console
    from rich.table import Table as RichTable

    profile_name = _resolve_profile(cfg, profile)
    if not asset_type:
        asset_type = _ask_choice("Asset type", ASSET_TYPES)
    db_path = _history_db_path(cfg)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        if asset_type == "notebooks":
            rows = conn.execute(
                "SELECT id, name, platform, language, cell_count, "
                "last_modified_at, owner "
                "FROM remote_notebooks WHERE profile_name = ? "
                "ORDER BY name",
                (profile_name,),
            ).fetchall()
            table = RichTable(title=f"Remote Notebooks ({profile_name})")
            table.add_column("ID")
            table.add_column("Name")
            table.add_column("Platform")
            table.add_column("Lang")
            table.add_column("Cells")
            table.add_column("Last modified")
            table.add_column("Owner")
            for r in rows:
                table.add_row(
                    str(r["id"]),
                    r["name"] or "-",
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
                "FROM remote_jobs WHERE profile_name = ? ORDER BY name",
                (profile_name,),
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
                "FROM remote_pipelines WHERE profile_name = ? ORDER BY name",
                (profile_name,),
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
                "WHERE profile_name = ? ORDER BY qualified_name",
                (profile_name,),
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
                "FROM remote_streams WHERE profile_name = ? "
                "ORDER BY qualified_name",
                (profile_name,),
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
                "FROM remote_task_dependencies WHERE profile_name = ? "
                "ORDER BY parent_task_fqn, child_task_fqn",
                (profile_name,),
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
                "FROM remote_queries WHERE profile_name = ? "
                "ORDER BY COALESCE(executed_at, '0000') DESC",
                (profile_name,),
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


# ── run_show ─────────────────────────────────────────────────────────────────


def _fetch_asset_by_identifier(conn, asset_type, profile_name, identifier):
    """Look up a single asset row by id (numeric) or name/qualified_name."""
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
    else:
        name_col = "qualified_name" if asset_type in {"streamlit_apps", "streams"} else "name"
        cur = conn.execute(
            f"SELECT * FROM {tbl} WHERE profile_name = ? AND {name_col} = ?",
            (profile_name, identifier),
        )
    row = cur.fetchone()
    cols = [d[0] for d in cur.description] if cur.description else []
    return (dict(row) if row else None), cols


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
    """Substring search across remote-ingested notebook source and saved/history SQL.

    Note: ships with naive LIKE-based matching. A future PR will add
    semantic / embedding-based search via the shared amx_code collection
    (the same one driving /code search).
    """
    profile_name = _resolve_profile(cfg, profile)
    db_path = _history_db_path(cfg)
    pattern = f"%{query}%"
    hits = []
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        for row in conn.execute(
            "SELECT id, name, platform, language FROM remote_notebooks "
            "WHERE profile_name = ? AND source_text LIKE ? LIMIT ?",
            (profile_name, pattern, limit),
        ).fetchall():
            hits.append(
                {
                    "kind": "notebook",
                    "id": row["id"],
                    "name": row["name"],
                    "tag": f"[remote:{row['platform']}]",
                    "context": row["language"],
                }
            )
        for row in conn.execute(
            "SELECT id, platform, kind, name, external_id FROM remote_queries "
            "WHERE profile_name = ? AND sql_text LIKE ? LIMIT ?",
            (profile_name, pattern, limit),
        ).fetchall():
            display_name = row["name"] or row["external_id"]
            hits.append(
                {
                    "kind": "query",
                    "id": row["id"],
                    "name": display_name,
                    "tag": f"[remote:{row['platform']}]",
                    "context": row["kind"],
                }
            )
    if not hits:
        click.echo(f"No remote assets matched '{query}' in profile '{profile_name}'.")
        return
    for h in hits[:limit]:
        click.echo(f"  {h['tag']} {h['kind']} #{h['id']}: {h['name']} ({h['context']})")


# ── run_refresh ──────────────────────────────────────────────────────────────


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
