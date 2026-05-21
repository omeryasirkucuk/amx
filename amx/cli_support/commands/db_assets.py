"""Click subcommands for ``/db ingest-assets`` and ``/db assets ...``.

Wizard-first: bare invocations prompt for profile/type/details. Flags are
optional power-user shortcuts. All commands attach to the existing
``/db`` namespace — no new top-level Click groups (house rule on AMX CLI
invocation style).
"""

from __future__ import annotations

import click

from amx.config import AMXConfig

ASSET_TYPES = [
    "notebooks", "jobs", "pipelines", "streamlit_apps",
    "streams", "task_dependencies", "queries",
]


def register_db_assets_commands(db_group: click.Group, *, pass_config) -> None:
    """Attach the ``/db ingest-assets`` and ``/db assets ...`` commands.

    ``db_group`` is the existing ``/db`` Click group (cached on
    ``register_root_commands._db_group``). ``pass_config`` is the
    ``click.make_pass_decorator(AMXConfig)`` used elsewhere.
    """

    @db_group.command("ingest-assets")
    @click.option(
        "--profile",
        default=None,
        help="DB profile name (default: active DB profile).",
    )
    @click.option(
        "--types",
        default=None,
        help=(
            "Comma-separated asset types. One or more of: "
            + ", ".join(ASSET_TYPES)
            + ". Omit to be prompted."
        ),
    )
    @click.option(
        "--history-days",
        default=7,
        show_default=True,
        type=int,
        help="How many days of query history to pull (queries only).",
    )
    @click.option(
        "--runs-per-job",
        default=20,
        show_default=True,
        type=int,
        help="How many recent runs to keep per Databricks job.",
    )
    @click.option(
        "--query-history-limit",
        default=1000,
        show_default=True,
        type=int,
        help="Cap on query-history rows fetched (Snowflake + Databricks).",
    )
    @pass_config
    def ingest_assets_cmd(
        cfg: AMXConfig,
        profile: str | None,
        types: str | None,
        history_days: int,
        runs_per_job: int,
        query_history_limit: int,
    ) -> None:
        """Ingest remote executable assets (notebooks, jobs, pipelines, ...) for a profile."""
        from amx.cli_support.commands.db_assets_impl import run_ingest_wizard
        run_ingest_wizard(
            cfg,
            profile=profile,
            types_csv=types,
            history_days=history_days,
            runs_per_job=runs_per_job,
            query_history_limit=query_history_limit,
        )

    @db_group.group("assets")
    def assets() -> None:
        """Browse and search remote-ingested assets (notebooks, jobs, queries, ...)."""

    @assets.command("list")
    @click.option("--profile", default=None, help="DB profile (default: active).")
    @click.option(
        "--type", "asset_type",
        default=None,
        type=click.Choice(ASSET_TYPES),
        help="Asset type to list. Omit to be prompted.",
    )
    @pass_config
    def assets_list_cmd(cfg: AMXConfig, profile: str | None, asset_type: str | None) -> None:
        """List remote-ingested assets in a tabular view."""
        from amx.cli_support.commands.db_assets_impl import run_list
        run_list(cfg, profile=profile, asset_type=asset_type)

    @assets.command("show")
    @click.argument("identifier")
    @click.option("--profile", default=None)
    @click.option(
        "--type", "asset_type",
        default=None, type=click.Choice(ASSET_TYPES),
    )
    @pass_config
    def assets_show_cmd(
        cfg: AMXConfig, identifier: str,
        profile: str | None, asset_type: str | None,
    ) -> None:
        """Show full detail (source, lineage, owner, ...) for one asset."""
        from amx.cli_support.commands.db_assets_impl import run_show
        run_show(cfg, identifier=identifier, profile=profile, asset_type=asset_type)

    @assets.command("search")
    @click.argument("query")
    @click.option("--profile", default=None)
    @click.option("--limit", default=10, show_default=True, type=int)
    @pass_config
    def assets_search_cmd(
        cfg: AMXConfig, query: str,
        profile: str | None, limit: int,
    ) -> None:
        """Embedding search across remote-ingested asset sources."""
        from amx.cli_support.commands.db_assets_impl import run_search
        run_search(cfg, query=query, profile=profile, limit=limit)

    @assets.command("refresh")
    @click.option("--profile", default=None)
    @click.option("-y", "--yes", is_flag=True, help="Skip the confirmation prompt.")
    @pass_config
    def assets_refresh_cmd(cfg: AMXConfig, profile: str | None, yes: bool) -> None:
        """Drop and re-ingest all assets for a profile."""
        from amx.cli_support.commands.db_assets_impl import run_refresh
        run_refresh(cfg, profile=profile, skip_confirm=yes)

    @assets.command("prune")
    @click.option(
        "--older-than",
        default="30d",
        show_default=True,
        help="Drop assets last ingested before this window (e.g. 30d, 7d, 12h).",
    )
    @click.option("--profile", default=None)
    @click.option("-y", "--yes", is_flag=True)
    @pass_config
    def assets_prune_cmd(
        cfg: AMXConfig, older_than: str, profile: str | None, yes: bool,
    ) -> None:
        """Drop assets that haven't been re-ingested in N days."""
        from amx.cli_support.commands.db_assets_impl import run_prune
        run_prune(cfg, older_than=older_than, profile=profile, skip_confirm=yes)
