"""Implementation helpers behind /db ingest-assets and /db assets ... commands.

Keeps the click.command decorators in db_assets.py thin and the business
logic here testable in isolation.
"""

from __future__ import annotations

import click

from amx.config import AMXConfig
from amx.services.ingest_assets import (
    IngestAssetsService,
    IngestProgressEvent,
    IngestRequest,
)

ASSET_TYPES = [
    "notebooks", "jobs", "pipelines", "streamlit_apps",
    "streams", "task_dependencies", "queries",
]


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
                f"Unknown asset type(s): {', '.join(unknown)}. "
                f"Valid: {', '.join(ASSET_TYPES)}."
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


# Stub functions for /db assets list/show/search/refresh/prune — filled by Tasks 33-36.
def run_list(cfg, *, profile, asset_type):
    raise click.ClickException("/db assets list is not implemented yet (Phase E Task 33).")


def run_show(cfg, *, identifier, profile, asset_type):
    raise click.ClickException("/db assets show is not implemented yet (Phase E Task 34).")


def run_search(cfg, *, query, profile, limit):
    raise click.ClickException("/db assets search is not implemented yet (Phase E Task 35).")


def run_refresh(cfg, *, profile, skip_confirm):
    raise click.ClickException("/db assets refresh is not implemented yet (Phase E Task 36).")


def run_prune(cfg, *, older_than, profile, skip_confirm):
    raise click.ClickException("/db assets prune is not implemented yet (Phase E Task 36).")
