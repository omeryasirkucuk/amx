"""Database namespace helpers for the AMX interactive CLI."""

from __future__ import annotations

import os
from dataclasses import dataclass, replace
from collections.abc import Callable
from pathlib import Path

from amx.config import AMXConfig, DBConfig, PROFILING_MODES, SUPPORTED_BACKENDS
from amx.utils.console import (
    ask,
    ask_choice,
    ask_password,
    error,
    info,
    render_table,
    success,
    warn,
)

LogEvent = Callable[..., None]


@dataclass
class DBConnectAttempt:
    label: str
    ok: bool
    detail: str = ""


def _ask_update_text(
    label: str,
    current: str = "",
    *,
    required: bool = False,
    allow_clear: bool = True,
) -> str:
    current_clean = str(current or "")
    if current_clean:
        hint = "Enter keeps current"
        if allow_clear:
            hint += ", '-' clears"
        prompt = f"{label} [{hint}]"
    else:
        prompt = label

    while True:
        value = ask(prompt, "")
        if not value:
            if current_clean:
                return current_clean
            if required:
                warn(f"{label} is required.")
                continue
            return ""
        if allow_clear and value.strip() == "-":
            if required:
                warn(f"{label} cannot be cleared.")
                continue
            return ""
        return value.strip()


def _ask_update_secret(
    label: str,
    current: str = "",
    *,
    required: bool = False,
) -> str:
    has_current = bool(str(current or ""))
    prompt = f"{label} [Enter keeps current, '-' clears]" if has_current else label

    while True:
        value = ask_password(prompt)
        if not value:
            if has_current:
                return str(current or "")
            if required:
                warn(f"{label} is required.")
                continue
            return ""
        if value.strip() == "-":
            if required:
                warn(f"{label} cannot be cleared.")
                continue
            return ""
        return value.strip()


def _ask_update_bool(label: str, current: bool = False) -> bool:
    picked = ask_choice(
        label,
        ["yes", "no"],
        default="yes" if current else "no",
        descriptions={
            "yes": "Enable this setting",
            "no": "Disable this setting",
        },
    )
    return picked == "yes"


def _save_active_db_profile(cfg: AMXConfig, db: DBConfig) -> None:
    cfg.db = db
    if cfg.active_db_profile and cfg.active_db_profile in cfg.db_profiles:
        cfg.db_profiles[cfg.active_db_profile] = db
    cfg.save()


def _is_databricks_tls_failure(message: str) -> bool:
    msg = (message or "").lower()
    return any(
        token in msg
        for token in (
            "tls",
            "certificate",
            "ssl",
            "trusted ca bundle",
            "self-signed",
        )
    )


def _env_trusted_ca_candidate() -> tuple[str, str] | None:
    for env_name in (
        "AMX_DATABRICKS_TRUSTED_CA_FILE",
        "DATABRICKS_TRUSTED_CA_FILE",
        "REQUESTS_CA_BUNDLE",
        "SSL_CERT_FILE",
    ):
        raw = os.environ.get(env_name, "").strip()
        if not raw:
            continue
        resolved = Path(os.path.expandvars(os.path.expanduser(raw)))
        if resolved.is_file():
            return env_name, str(resolved)
    return None


def databricks_connect_with_recovery(
    cfg: AMXConfig,
    connect_fn: Callable[[DBConfig], tuple[bool, str]],
) -> tuple[bool, list[DBConnectAttempt]]:
    attempts: list[DBConnectAttempt] = []
    current = cfg.db

    ok, detail = connect_fn(current)
    attempts.append(DBConnectAttempt("saved profile", ok, detail))
    if ok or not _is_databricks_tls_failure(detail):
        return ok, attempts

    candidate = _env_trusted_ca_candidate()
    if candidate and not str(current.tls_trusted_ca_file or "").strip():
        env_name, ca_path = candidate
        updated = replace(current, tls_trusted_ca_file=ca_path, tls_no_verify=False)
        ok, detail = connect_fn(updated)
        attempts.append(DBConnectAttempt(f"env CA bundle ({env_name})", ok, detail))
        if ok:
            _save_active_db_profile(cfg, updated)
            return True, attempts

    if not bool(current.tls_no_verify):
        updated = replace(current, tls_no_verify=True)
        ok, detail = connect_fn(updated)
        attempts.append(DBConnectAttempt("TLS no-verify fallback", ok, detail))
        if ok:
            _save_active_db_profile(cfg, updated)
            return True, attempts

    return False, attempts


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
        host = _ask_update_text("Database host", defaults.host or "localhost", required=True, allow_clear=False)
        port_raw = _ask_update_text("Port", str(defaults.port or 5432), required=True, allow_clear=False)
        while not port_raw.isdigit():
            warn("Port must be a number.")
            port_raw = _ask_update_text("Port", str(defaults.port or 5432), required=True, allow_clear=False)
        user = _ask_update_text("Username", defaults.user or "amx", required=True, allow_clear=False)
        password = _ask_update_secret("Password", defaults.password or "", required=True)
        database = _ask_update_text("Database name", defaults.database or "postgres", required=True, allow_clear=False)
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
        account = _ask_update_text("Snowflake account identifier (e.g. xy12345.us-east-1)", defaults.account, required=True, allow_clear=False)
        user = _ask_update_text("Username", defaults.user, required=True, allow_clear=False)
        password = _ask_update_secret("Password", defaults.password or "", required=True)
        database = _ask_update_text("Database name", defaults.database, required=True, allow_clear=False)
        warehouse = _ask_update_text("Warehouse (optional)", defaults.warehouse or "")
        role = _ask_update_text("Role (optional)", defaults.role or "")
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
        host = _ask_update_text(
            "Databricks host (e.g. adb-xxx.azuredatabricks.net)",
            defaults.host,
            required=True,
            allow_clear=False,
        )
        http_path = _ask_update_text(
            "SQL warehouse HTTP path",
            defaults.http_path,
            required=True,
            allow_clear=False,
        )
        access_token = _ask_update_secret("Access token", defaults.access_token or "", required=True)
        catalog = _ask_update_text("Unity Catalog name (optional)", defaults.catalog or "")
        database = _ask_update_text("Schema / database (optional)", defaults.database or "")
        tls_trusted_ca_file = _ask_update_text(
            "Trusted CA bundle path (optional, for corporate/self-signed TLS)",
            defaults.tls_trusted_ca_file or "",
        )
        tls_no_verify = _ask_update_bool(
            "Disable TLS certificate verification? (insecure; use only if a trusted CA bundle is not available)",
            bool(defaults.tls_no_verify),
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
        project = _ask_update_text("GCP project ID", defaults.project, required=True, allow_clear=False)
        dataset = _ask_update_text("Default dataset (optional)", defaults.dataset or "")
        creds = _ask_update_text("Service account JSON path (optional, uses ADC if empty)", defaults.credentials_path or "")
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
    cfg.db_profiles[name] = db
    cfg.active_db_profile = name
    cfg.db = db
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


def cmd_tls(cfg: AMXConfig, rest: list[str]) -> None:
    """Show or update active Databricks TLS settings."""
    if cfg.db.backend != "databricks":
        error("The active DB profile is not Databricks. Switch to a Databricks profile first.")
        return

    if not rest:
        ca_path = str(getattr(cfg.db, "tls_trusted_ca_file", "") or "").strip() or "(none)"
        info(
            "Current Databricks TLS settings: "
            f"tls_no_verify=[cyan]{bool(getattr(cfg.db, 'tls_no_verify', False))}[/cyan], "
            f"trusted_ca=[cyan]{ca_path}[/cyan]. "
            "Use /tls <on|off> [ca_path|clear]."
        )
        return

    raw = rest[0].lower().strip()
    truthy = {"on", "true", "yes", "y", "1"}
    falsy = {"off", "false", "no", "n", "0"}
    if raw in truthy:
        no_verify = True
    elif raw in falsy:
        no_verify = False
    else:
        error(f"Unknown TLS mode {rest[0]!r}. Use on/off.")
        return

    ca_path = str(getattr(cfg.db, "tls_trusted_ca_file", "") or "").strip()
    if len(rest) >= 2:
        ca_arg = rest[1].strip()
        if ca_arg.lower() in {"clear", "none", "off", "-"}:
            ca_path = ""
        else:
            ca_path = ca_arg

    cfg.db.tls_no_verify = no_verify
    cfg.db.tls_trusted_ca_file = ca_path
    if cfg.active_db_profile and cfg.active_db_profile in cfg.db_profiles:
        cfg.db_profiles[cfg.active_db_profile].tls_no_verify = no_verify
        cfg.db_profiles[cfg.active_db_profile].tls_trusted_ca_file = ca_path
    cfg.save()

    shown_path = ca_path or "(none)"
    success(
        f"Databricks TLS settings saved: tls_no_verify={no_verify}, trusted_ca={shown_path}."
    )
