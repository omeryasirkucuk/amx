"""``/db inspect`` + ``/db profiling`` CLI handlers.

Extracted from :mod:`amx.cli_support.commands.db`. The two commands
read live-DB metadata via :class:`DatabaseConnector` and render Rich
tables for the user; ``_format_age`` is the cache-age formatter both
rely on.

``db.py`` re-exports each name so session.py's ``from
amx.cli_support.commands.db import cmd_inspect, cmd_profiling`` keeps
working unchanged.
"""

from __future__ import annotations

from amx.config import PROFILING_MODES, AMXConfig
from amx.utils.console import (
    error,
    heading,
    info,
    info_styled,
    render_table,
    success,
    warn,
)


def _format_age(ts: float | None) -> str:
    if ts is None or ts <= 0:
        return "—"
    import time as _t

    delta = max(0.0, _t.time() - float(ts))
    if delta < 60:
        return "just now"
    if delta < 3600:
        return f"{int(delta // 60)}m ago"
    if delta < 86400:
        return f"{int(delta // 3600)}h ago"
    return f"{int(delta // 86400)}d ago"


def cmd_inspect(cfg: AMXConfig, rest: list[str]) -> None:
    """Diagnose the active (or named) DB profile.

    Usage::

        /inspect              # diagnose the active profile
        /inspect <profile>    # diagnose a specific profile

    Shows: backend, connection summary, capability flags, connection
    test result, visible schemas, and table counts per schema. The
    intent is to give users a self-service way to answer "why is my
    connection / metadata flaky?" without filing an issue. Secrets
    (password, access_token, api_key) are never printed.
    """
    profile_name = rest[0] if rest else cfg.active_db_profile
    if not profile_name:
        error("No active DB profile. Run /add-db-profile first.")
        return
    if profile_name not in cfg.db_profiles:
        error(
            f"Profile {profile_name!r} not found. "
            f"Available: {', '.join(sorted(cfg.db_profiles)) or '(none)'}."
        )
        return

    profile = cfg.db_profiles[profile_name]
    heading(f"DB profile: {profile_name}")
    info_styled("  Backend", profile.backend, value_style="info")
    info(f"  Connection: {profile.display_summary}")

    # Print non-secret connection fields per backend so users can sanity-
    # check that what they think is configured is what AMX is using.
    if profile.backend == "postgresql":
        info(f"  Host: {profile.host}:{profile.port}, user: {profile.user}, db: {profile.database}")
    elif profile.backend == "snowflake":
        info(
            f"  Account: {profile.account}, user: {profile.user}, db: {profile.database}, "
            f"warehouse: {profile.warehouse or '(default)'}, role: {profile.role or '(default)'}"
        )
    elif profile.backend == "databricks":
        info(
            f"  Host: {profile.host}, http_path: {profile.http_path or '(missing)'}, "
            f"catalog: {profile.catalog or '(default)'}, "
            f"tls: {'no-verify' if profile.tls_no_verify else 'verify'}"
        )
        if profile.tls_trusted_ca_file:
            info(f"  Trusted CA bundle: {profile.tls_trusted_ca_file}")
    elif profile.backend == "bigquery":
        info(
            f"  Project: {profile.project}, dataset: {profile.dataset or '(any)'}, "
            f"credentials: {profile.credentials_path or '(ADC)'}"
        )
    elif profile.backend == "mysql":
        info(
            f"  Host: {profile.host}:{profile.port}, user: {profile.user}, "
            f"db: {profile.database or '(unpinned)'}"
        )
    elif profile.backend == "oracle":
        target = (
            f"service_name={profile.service_name}"
            if profile.service_name
            else f"SID={profile.database or '(unpinned)'}"
        )
        info(f"  Host: {profile.host}:{profile.port}, user: {profile.user}, {target}")
    elif profile.backend == "mssql":
        info(
            f"  Host: {profile.host}:{profile.port}, user: {profile.user}, "
            f"db: {profile.database or '(unpinned)'}, "
            f"driver: {profile.driver or '(default ODBC 18)'}, "
            f"encrypt: {profile.encrypt}, trust_server_cert: {profile.trust_server_certificate}"
        )
    elif profile.backend == "redshift":
        info(
            f"  Host: {profile.host}:{profile.port}, user: {profile.user}, "
            f"db: {profile.database or '(unpinned)'}"
            + (f", cluster_id: {profile.cluster_identifier}" if profile.cluster_identifier else "")
        )
    elif profile.backend == "clickhouse":
        scheme = "https" if profile.secure else "http"
        info(
            f"  Host: {profile.host}:{profile.port} ({scheme}), "
            f"user: {profile.user or 'default'}, "
            f"db: {profile.database or '(unpinned)'}"
        )
    elif profile.backend == "duckdb":
        info(f"  File: {profile.database or ':memory:'}")

    # Connection test using the existing typed result so categorised
    # error hints from amx.core.errors flow through unchanged.
    info("\nTesting connection…")
    try:
        from amx.db.connector import DatabaseConnector

        connector = DatabaseConnector(profile)
        result = connector.test_connection_result()
    except Exception as exc:  # noqa: BLE001 — surface as a themed error
        error(f"Could not even build the connector: {exc}")
        return

    if not result.ok:
        error(f"Connection failed: {result.message}")
        info("Fix the profile via /add-db-profile and re-run /inspect.")
        return
    success("Connection OK")

    info(
        "\nCapabilities: "
        f"comments={connector.capabilities.column_comments}, "
        f"relationships={connector.capabilities.relationships}, "
        f"row_count_stats={connector.capabilities.row_count_stats}, "
        f"materialized_views={connector.capabilities.materialized_views}"
    )

    # Schema discovery — can the user actually see anything?
    info("\nDiscovering visible schemas…")
    try:
        schemas = connector.list_schemas()
    except Exception as exc:  # noqa: BLE001 — show, don't crash /inspect
        error(f"Schema introspection failed: {exc}")
        return

    if not schemas:
        warn(
            "No user-visible schemas. Either the role lacks USAGE / SELECT, "
            "or this database is genuinely empty."
        )
        return

    info(f"  {len(schemas)} schemas visible.")
    rows: list[list[object]] = []
    show_limit = 15
    skipped_count = 0
    for schema_name in schemas[:show_limit]:
        try:
            tables = connector.list_tables(schema_name)
            rows.append([schema_name, str(len(tables))])
        except Exception as exc:  # noqa: BLE001 — partial inventory
            rows.append([schema_name, f"failed — {exc.__class__.__name__}"])
            skipped_count += 1
    if len(schemas) > show_limit:
        rows.append([f"… ({len(schemas) - show_limit} more)", ""])
    render_table("Schemas", ["Schema", "Tables"], rows)
    if skipped_count:
        warn(
            f"{skipped_count} schemas could not be enumerated — see ~/.amx/logs/amx.log "
            "for the underlying error."
        )


def cmd_profiling(cfg: AMXConfig, rest: list[str]) -> None:
    """Show or update active DB profiling guardrails."""
    if not rest:
        max_rows = int(getattr(cfg.db, "profiling_max_rows", 1_000_000) or 0)
        max_label = "off" if max_rows <= 0 else f"{max_rows:,}"
        approx = bool(getattr(cfg.db, "profiling_approximate", False))
        info(
            "Current profiling guardrails: "
            f"mode=[info]{cfg.db.profiling_mode}[/info], "
            f"max_full_scan_rows=[info]{max_label}[/info], "
            f"sample_size=[info]{cfg.db.profiling_sample_size}[/info], "
            f"approximate=[info]{approx}[/info]. "
            "Use /profiling <full|sampled|metadata> [max_rows|off] "
            "[sample_size] [approximate]."
        )
        return

    mode = rest[0].lower().strip()
    if mode not in PROFILING_MODES:
        error(f"Unknown profiling mode {rest[0]!r}. Use: {', '.join(PROFILING_MODES)}")
        return

    max_rows = int(getattr(cfg.db, "profiling_max_rows", 1_000_000) or 0)
    if len(rest) >= 2:
        raw_max = rest[1].lower().strip()
        if raw_max in {"off", "none", "0"}:
            max_rows = 0
        else:
            try:
                max_rows = int(raw_max.replace("_", ""))
            except ValueError:
                error(f"Expected max rows as integer or off, got: {rest[1]!r}")
                return
            if max_rows < 0:
                error("Max rows must be >= 0, or use off.")
                return

    sample_size = int(getattr(cfg.db, "profiling_sample_size", 5) or 0)
    if len(rest) >= 3:
        try:
            sample_size = int(rest[2])
        except ValueError:
            error(f"Expected sample size as integer, got: {rest[2]!r}")
            return
        if sample_size < 0:
            error("Sample size must be >= 0.")
            return

    approximate = bool(getattr(cfg.db, "profiling_approximate", False))
    if len(rest) >= 4:
        raw = rest[3].lower().strip()
        truthy = {"on", "true", "yes", "y", "1", "approx", "approximate"}
        falsy = {"off", "false", "no", "n", "0", "exact"}
        if raw in truthy:
            approximate = True
        elif raw in falsy:
            approximate = False
        else:
            error(f"Expected approximate flag as on/off, got: {rest[3]!r}.")
            return

    cfg.db.profiling_mode = mode
    cfg.db.profiling_max_rows = max_rows
    cfg.db.profiling_sample_size = sample_size
    cfg.db.profiling_approximate = approximate
    if cfg.active_db_profile and cfg.active_db_profile in cfg.db_profiles:
        cfg.db_profiles[cfg.active_db_profile].profiling_mode = mode
        cfg.db_profiles[cfg.active_db_profile].profiling_max_rows = max_rows
        cfg.db_profiles[cfg.active_db_profile].profiling_sample_size = sample_size
        cfg.db_profiles[cfg.active_db_profile].profiling_approximate = approximate
    cfg.save()

    max_label = "off" if max_rows <= 0 else f"{max_rows:,}"
    success(
        f"Profiling guardrails saved: mode={mode}, "
        f"max_full_scan_rows={max_label}, sample_size={sample_size}, "
        f"approximate={approximate}."
    )
