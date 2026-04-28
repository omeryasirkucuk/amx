"""Database namespace helpers for the AMX interactive CLI."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace

from amx.config import AMXConfig, DBConfig, PROFILING_MODES, SUPPORTED_BACKENDS
from amx.utils.console import (
    ask,
    ask_choice,
    ask_password,
    confirm,
    error,
    info,
    render_table,
    success,
    warn,
)

LogEvent = Callable[..., None]


def print_db_namespace_hint() -> None:
    """Shown when the user enters `/db` - how to pick engine and switch profiles."""
    backends = ", ".join(SUPPORTED_BACKENDS)
    info(
        f"Database engines: {backends}. "
        "Use /db-profiles to list saved profiles (each row shows its backend). "
        "/use-db switches the active profile - you will see backend + connection summary. "
        "/add-db-profile first asks which engine (PostgreSQL, Snowflake, Databricks, BigQuery), then connection details."
    )


def cmd_profiles(cfg: AMXConfig) -> None:
    rows = []
    for name, db in sorted(cfg.db_profiles.items(), key=lambda x: x[0]):
        mark = "*" if name == cfg.active_db_profile else " "
        rows.append([f"{mark} {name}", db.backend, db.display_summary])
    render_table(
        "DB profiles (* = active)",
        ["Profile", "Backend", "Connection"],
        rows,
    )


def cmd_use(
    cfg: AMXConfig,
    rest: list[str],
    *,
    log_event: LogEvent | None = None,
) -> None:
    if len(rest) >= 1:
        name = rest[0]
    else:
        names = sorted(cfg.db_profiles.keys())
        if not names:
            error("No profiles configured. Use /add-db-profile to create one (pick PostgreSQL, Snowflake, Databricks, or BigQuery).")
            return
        descriptions = {
            n: f"[{p.backend}] {p.display_summary}"
            for n, p in cfg.db_profiles.items()
        }
        name = ask_choice(
            "Select DB profile (by name or number)",
            names,
            default=cfg.active_db_profile or names[0],
            descriptions=descriptions,
        )
        if not name:
            error("No profile selected.")
            return
    try:
        cfg.set_active_db_profile(name)
        cfg.save()
        p = cfg.db
        success(f"Switched active DB profile to: {name} [{p.backend}] - {p.display_summary}")
        if log_event is not None:
            log_event(
                event_type="db_profile_switch",
                status="success",
                command="use-db",
                details={"profile": name, "backend": p.backend},
            )
    except Exception as exc:
        if log_event is not None:
            log_event(
                event_type="db_profile_switch",
                status="failed",
                command="use-db",
                details={"profile": name, "error": str(exc)},
            )
        error(str(exc))


def interactive_db_block(defaults: DBConfig | None = None) -> DBConfig:
    """Interactive prompts to build a DBConfig for any supported backend."""
    if defaults is None:
        defaults = DBConfig()
    backend = ask_choice(
        "Select database backend (engine)",
        list(SUPPORTED_BACKENDS),
        default=defaults.backend or "postgresql",
        descriptions={
            "postgresql": "Host/port user/password - COMMENT ON metadata",
            "snowflake": "Account, warehouse, role - Snowflake COMMENT",
            "databricks": "SQL warehouse HTTP path + token - Unity Catalog",
            "bigquery": "GCP project + dataset - table/column descriptions via OPTIONS",
        },
    )

    if backend == "postgresql":
        host = ask("Database host", defaults.host or "localhost")
        port_raw = ask("Port", str(defaults.port or 5432))
        while not port_raw.isdigit():
            warn("Port must be a number.")
            port_raw = ask("Port", str(defaults.port or 5432))
        user = ask("Username", defaults.user or "amx")
        password = ask_password("Password") or defaults.password or ""
        database = ask("Database name", defaults.database or "postgres")
        return replace(
            defaults,
            backend="postgresql",
            host=host,
            port=int(port_raw),
            user=user,
            password=password,
            database=database,
        )

    if backend == "snowflake":
        account = ask("Snowflake account identifier (e.g. xy12345.us-east-1)", defaults.account)
        user = ask("Username", defaults.user)
        password = ask_password("Password") or defaults.password or ""
        database = ask("Database name", defaults.database)
        warehouse = ask("Warehouse (optional)", defaults.warehouse or "")
        role = ask("Role (optional)", defaults.role or "")
        return replace(
            defaults,
            backend="snowflake",
            account=account,
            user=user,
            password=password,
            database=database,
            warehouse=warehouse,
            role=role,
        )

    if backend == "databricks":
        host = ask("Databricks host (e.g. adb-xxx.azuredatabricks.net)", defaults.host)
        http_path = ask("SQL warehouse HTTP path", defaults.http_path)
        access_token = ask_password("Access token") or defaults.access_token or ""
        catalog = ask("Unity Catalog name (optional)", defaults.catalog or "")
        database = ask("Schema / database (optional)", defaults.database or "")
        tls_trusted_ca_file = ask(
            "Trusted CA bundle path (optional, for corporate/self-signed TLS)",
            defaults.tls_trusted_ca_file or "",
        )
        tls_no_verify = confirm(
            "Disable TLS certificate verification? (insecure; use only if a trusted CA bundle is not available)",
            default=bool(defaults.tls_no_verify),
        )
        return replace(
            defaults,
            backend="databricks",
            host=host,
            http_path=http_path,
            access_token=access_token,
            catalog=catalog,
            database=database,
            tls_trusted_ca_file=tls_trusted_ca_file,
            tls_no_verify=tls_no_verify,
        )

    if backend == "bigquery":
        project = ask("GCP project ID", defaults.project)
        dataset = ask("Default dataset (optional)", defaults.dataset or "")
        creds = ask("Service account JSON path (optional, uses ADC if empty)", defaults.credentials_path or "")
        return replace(
            defaults,
            backend="bigquery",
            project=project,
            dataset=dataset,
            credentials_path=creds,
        )

    return defaults


def cmd_add_profile(
    cfg: AMXConfig,
    rest: list[str],
    *,
    log_event: LogEvent | None = None,
) -> None:
    if len(rest) >= 1:
        name = rest[0]
    else:
        name = ask("Profile name", default="local")
    info(f"Creating/updating profile: {name}")
    existing = cfg.db_profiles.get(name)
    db = interactive_db_block(existing or cfg.db)
    cfg.upsert_db_profile(name, db)
    cfg.set_active_db_profile(name)
    cfg.save()
    success(f"Profile saved and activated: {name} [{db.backend}]")
    if log_event is not None:
        log_event(
            event_type="db_profile_upsert",
            status="success",
            command="add-db-profile",
            details={"profile": name, "backend": db.backend},
        )


def cmd_remove_profile(cfg: AMXConfig, rest: list[str]) -> None:
    if len(rest) < 1:
        error("Usage: /remove-db-profile <name>")
        return
    name = rest[0]
    try:
        cfg.remove_db_profile(name)
        cfg.save()
        success(f"Removed profile: {name} (active: {cfg.active_db_profile})")
    except Exception as exc:
        error(str(exc))


def cmd_profiling(cfg: AMXConfig, rest: list[str]) -> None:
    """Show or update active DB profiling guardrails."""
    if not rest:
        max_rows = int(getattr(cfg.db, "profiling_max_rows", 1_000_000) or 0)
        max_label = "off" if max_rows <= 0 else f"{max_rows:,}"
        info(
            "Current profiling guardrails: "
            f"mode=[cyan]{cfg.db.profiling_mode}[/cyan], "
            f"max_full_scan_rows=[cyan]{max_label}[/cyan], "
            f"sample_size=[cyan]{cfg.db.profiling_sample_size}[/cyan]. "
            "Use /profiling <full|sampled|metadata> [max_rows|off] [sample_size]."
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

    cfg.db.profiling_mode = mode
    cfg.db.profiling_max_rows = max_rows
    cfg.db.profiling_sample_size = sample_size
    if cfg.active_db_profile and cfg.active_db_profile in cfg.db_profiles:
        cfg.db_profiles[cfg.active_db_profile].profiling_mode = mode
        cfg.db_profiles[cfg.active_db_profile].profiling_max_rows = max_rows
        cfg.db_profiles[cfg.active_db_profile].profiling_sample_size = sample_size
    cfg.save()

    max_label = "off" if max_rows <= 0 else f"{max_rows:,}"
    success(
        f"Profiling guardrails saved: mode={mode}, "
        f"max_full_scan_rows={max_label}, sample_size={sample_size}."
    )
