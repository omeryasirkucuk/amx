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
    "notebooks",
    "jobs",
    "pipelines",
    "streamlit_apps",
    "streams",
    "task_dependencies",
    "queries",
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
    @click.option(
        "--include-id",
        "include_ids",
        multiple=True,
        metavar="KIND:EXTERNAL_ID",
        help=(
            "Cherry-pick individual platform assets, repeatable. "
            "KIND is one of notebooks/jobs/pipelines/streamlit_apps/streams. "
            "Example: --include-id notebooks:abc123 --include-id jobs:42. "
            "When set, only the listed ids ingest for each named kind; other "
            "kinds keep the default 'ingest all' behaviour."
        ),
    )
    @pass_config
    def ingest_assets_cmd(
        cfg: AMXConfig,
        profile: str | None,
        types: str | None,
        history_days: int,
        runs_per_job: int,
        query_history_limit: int,
        include_ids: tuple[str, ...],
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
            include_ids=include_ids,
        )

    @db_group.group("assets")
    def assets() -> None:
        """Browse and search remote-ingested assets (notebooks, jobs, queries, ...)."""

    @assets.command("list")
    @click.option("--profile", default=None, help="DB profile (default: active).")
    @click.option(
        "--type",
        "asset_type",
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
        "--type",
        "asset_type",
        default=None,
        type=click.Choice(ASSET_TYPES),
    )
    @pass_config
    def assets_show_cmd(
        cfg: AMXConfig,
        identifier: str,
        profile: str | None,
        asset_type: str | None,
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
        cfg: AMXConfig,
        query: str,
        profile: str | None,
        limit: int,
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

    @assets.command("chunking")
    @click.option("--show", is_flag=True, help="Print the active chunking config and exit.")
    @pass_config
    def assets_chunking_cmd(cfg: AMXConfig, show: bool) -> None:
        """View or edit the per-kind chunking strategy for asset RAG ingestion.

        Defaults are ``whole`` (one chunk per notebook / query / pipeline)
        — coarse but predictable. Pick ``cell`` / ``statement`` to slice
        a notebook by cell or a query by ``;`` boundary, or
        ``char_window`` for pure character windows that ignore semantic
        boundaries. Stream / streamlit / job assets always emit a
        single metadata chunk; they are not configurable.

        The wizard writes back to ``cfg.embedding_assets`` /
        ``cfg.assets_chunking`` so the next ``/db ingest-assets`` picks
        up the new strategy. Run ``/db assets reindex`` afterwards to
        re-embed already-ingested assets under the new chunking.
        """
        from amx.cli_support.commands.db_assets_impl import run_chunking

        run_chunking(cfg, show_only=show)

    @assets.command("reindex")
    @click.option(
        "--profile",
        default=None,
        help="Re-chunk + re-embed all ingested assets for this DB profile.",
    )
    @click.option("-y", "--yes", is_flag=True, help="Skip the confirmation prompt.")
    @pass_config
    def assets_reindex_cmd(cfg: AMXConfig, profile: str | None, yes: bool) -> None:
        """Re-embed every ingested asset under the active embedding model.

        Use after switching ``cfg.embedding_assets`` (e.g. MiniLM ->
        OpenAI ada) — the existing Chroma vectors are in a different
        space and AssetRAGStore would otherwise raise
        ``EmbeddingProviderMismatch`` on next open. Recovery is
        ``reset_collection`` + a fresh ingest under the active triple.
        """
        from amx.cli_support.commands.db_assets_impl import run_reindex

        run_reindex(cfg, profile=profile, skip_confirm=yes)

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
        cfg: AMXConfig,
        older_than: str,
        profile: str | None,
        yes: bool,
    ) -> None:
        """Drop assets that haven't been re-ingested in N days."""
        from amx.cli_support.commands.db_assets_impl import run_prune

        run_prune(cfg, older_than=older_than, profile=profile, skip_confirm=yes)

    @assets.command("delete")
    @click.argument("identifier")
    @click.option(
        "--type",
        "asset_type",
        default=None,
        type=click.Choice(ASSET_TYPES),
        help="Asset type. Required when the identifier is a name (not a numeric id).",
    )
    @click.option("--profile", default=None)
    @click.option("-y", "--yes", is_flag=True, help="Skip the confirmation prompt.")
    @pass_config
    def assets_delete_cmd(
        cfg: AMXConfig,
        identifier: str,
        asset_type: str | None,
        profile: str | None,
        yes: bool,
    ) -> None:
        """Delete a single remote asset (cascade tasks/runs + lineage edges)."""
        from amx.cli_support.commands.db_assets_impl import run_delete

        run_delete(
            cfg,
            identifier=identifier,
            asset_type=asset_type,
            profile=profile,
            skip_confirm=yes,
        )
