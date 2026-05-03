"""Database namespace helpers for the AMX interactive CLI."""

from __future__ import annotations

import contextlib
import os
from collections.abc import Callable
from dataclasses import dataclass, replace
from pathlib import Path

from amx.config import PROFILING_MODES, SUPPORTED_BACKENDS, AMXConfig, DBConfig
from amx.utils.console import (
    ask,
    ask_choice,
    ask_multi_choice,
    ask_password,
    confirm,
    error,
    heading,
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
    from amx.config import has_legacy_database_default

    rows = []
    legacy_profiles: list[str] = []
    for name, db in sorted(cfg.db_profiles.items(), key=lambda x: x[0]):
        mark = "*" if name == cfg.active_db_profile else " "
        # 0.11.0: surface unpinned-database state (database is now optional).
        # The display_summary already includes ``(no DB pinned)`` so we just
        # add a small ``?`` next to the backend so the table glance is
        # still clean.
        backend_label = db.backend if db.is_database_pinned() else f"{db.backend} ?"
        rows.append([f"{mark} {name}", backend_label, db.display_summary])
        if has_legacy_database_default(db):
            legacy_profiles.append(name)
    render_table(
        "DB profiles (* = active, ? = no DB pinned)",
        ["Profile", "Backend", "Connection"],
        rows,
    )
    # Suggest-don't-mutate: the historical demo default ``database='SAP'``
    # leaks into UIs as a phantom localhost connection. We never edit the
    # user's YAML — just hint once per ``/db-profiles`` view.
    if legacy_profiles:
        warn(
            "Profile(s) "
            + ", ".join(sorted(legacy_profiles))
            + " still carry the legacy demo default database='SAP'. "
            "Run `/edit` (and clear the database field) if this isn't your real DB."
        )


def cmd_use(
    cfg: AMXConfig,
    rest: list[str],
    *,
    log_event: LogEvent | None = None,
) -> None:
    """Switch the active DB scope.

    0.11.0 multi-pick:
        /use-db prod_pg                 → single-profile (legacy behaviour)
        /use-db prod_pg analytics_bq    → persisted multi-profile scope used
                                          by /ask, /run, /sync.

    Interactive form (no args): prompts whether the user wants single
    or multi-pick, then runs the appropriate selector.
    """
    available = sorted(cfg.db_profiles.keys())
    if not available:
        error(
            "No profiles configured. Use /add-db-profile to create one (pick PostgreSQL, Snowflake, Databricks, or BigQuery)."
        )
        return

    # Inline-arg form: /use-db NAME [NAME ...]
    if rest:
        chosen: list[str] = []
        unknown: list[str] = []
        for raw in rest:
            n = (raw or "").strip()
            if not n:
                continue
            if n in cfg.db_profiles and n not in chosen:
                chosen.append(n)
            else:
                unknown.append(n)
        if unknown:
            error(
                f"Unknown profile(s): {', '.join(unknown)}. "
                f"Available: {', '.join(available) or '(none)'}."
            )
            return
        if not chosen:
            error("No profile selected.")
            return
    else:
        # Interactive: ask single vs multi, then route to the right picker.
        descriptions = {n: f"[{p.backend}] {p.display_summary}" for n, p in cfg.db_profiles.items()}
        if len(available) >= 2 and confirm(
            "Pick multiple profiles for the active scope (used by /ask /run /sync)?",
            default=False,
        ):
            display = [
                f"{n}  -  [{cfg.db_profiles[n].backend}] {cfg.db_profiles[n].display_summary}"
                for n in available
            ]
            picked = ask_multi_choice("Select DB profiles (comma-separated)", display)
            chosen = [s.split("  -  ", 1)[0].strip() for s in picked]
            chosen = [n for n in chosen if n in cfg.db_profiles]
            if not chosen:
                error("No profile selected.")
                return
        else:
            single = ask_choice(
                "Select DB profile (by name or number)",
                available,
                default=cfg.active_db_profile or available[0],
                descriptions=descriptions,
            )
            if not single:
                error("No profile selected.")
                return
            chosen = [single]

    try:
        if len(chosen) == 1:
            cfg.set_active_db_profile(chosen[0])
        else:
            cfg.set_active_db_profiles(chosen)
        cfg.save()
        p = cfg.db
        if len(chosen) == 1:
            success(
                f"Switched active DB profile to: {chosen[0]} [{p.backend}] - {p.display_summary}"
            )
        else:
            success(f"Active DB scope: {', '.join(chosen)} (default = {chosen[0]} [{p.backend}])")
        if log_event is not None:
            log_event(
                event_type="db_profile_switch",
                status="success",
                command="use-db",
                details={
                    "profile": chosen[0],
                    "profiles": chosen,
                    "backend": p.backend,
                    "multi": len(chosen) > 1,
                },
            )
    except Exception as exc:
        if log_event is not None:
            log_event(
                event_type="db_profile_switch",
                status="failed",
                command="use-db",
                details={"profiles": chosen, "error": str(exc)},
            )
        error(str(exc))


def interactive_db_block(defaults: DBConfig | None = None) -> DBConfig:
    """Interactive prompts to build a DBConfig for any supported backend.

    When ``defaults`` is ``None``, every prompt starts blank — used for
    new profiles so we never leak fields from an already-configured
    active profile.

    When ``defaults`` is supplied (editing an existing profile) the
    user can press Enter to keep each current value. If they switch
    the backend mid-flow, we reset to a fresh ``DBConfig`` so values
    from the previous backend (e.g. a Databricks workspace URL) do
    NOT silently fill into a freshly chosen PostgreSQL profile.
    """
    new_profile = defaults is None
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
            "mysql": "Host/port user/password - MySQL/MariaDB; ALTER TABLE COMMENT",
            "oracle": "Host/port user/password + service_name - Oracle DB; ALL_* catalogs",
            "mssql": "Host/port user/password + ODBC driver - SQL Server / Azure SQL",
            "redshift": "Host/port user/password - Amazon Redshift; PG-compatible + Spectrum",
            "clickhouse": "Host/port user/password - ClickHouse; system.* catalogs, MergeTree engines",
            "duckdb": "Single-file or in-memory; no host/auth (analytical, embedded)",
        },
    )

    # Truly-blank defaults for a brand-new profile or after a cross-
    # backend reset. ``DBConfig()`` carries the dataclass-level
    # placeholders (``host="localhost"``, ``user="amx"``, etc.), which
    # would still appear as Enter-to-keep defaults and feel like the
    # active profile is leaking — so we wipe every connection field.
    def _blank(active_backend: str) -> DBConfig:
        return DBConfig(
            backend=active_backend,
            host="",
            port=0,
            user="",
            password="",
            database="",
            account="",
            warehouse="",
            role="",
            http_path="",
            access_token="",
            catalog="",
            tls_no_verify=False,
            tls_trusted_ca_file="",
            project="",
            dataset="",
            credentials_path="",
            service_name="",
            driver="",
            encrypt=True,
            trust_server_certificate=False,
            cluster_identifier="",
            secure=False,
        )

    # If the user picked a backend different from what ``defaults``
    # was built for, drop those defaults — they belong to a different
    # backend and would otherwise leak into the wrong fields.
    if defaults.backend and backend != defaults.backend:
        defaults = _blank(backend)

    if new_profile:
        defaults = _blank(backend)

    if backend == "postgresql":
        # New-profile prompts start fully empty so users never silently
        # inherit a value from a different profile by pressing Enter.
        # Existing-profile edits keep their current value as the default.
        host = _ask_update_text(
            "Database host (e.g. db.example.com)",
            defaults.host or "",
            required=True,
            allow_clear=False,
        )
        port_raw = _ask_update_text(
            "Port (e.g. 5432)",
            str(defaults.port) if defaults.port else "",
            required=True,
            allow_clear=False,
        )
        while not port_raw.isdigit():
            warn("Port must be a number.")
            port_raw = _ask_update_text(
                "Port (e.g. 5432)",
                str(defaults.port) if defaults.port else "",
                required=True,
                allow_clear=False,
            )
        user = _ask_update_text(
            "Username (e.g. amx)",
            defaults.user or "",
            required=True,
            allow_clear=False,
        )
        password = _ask_update_secret("Password", defaults.password or "", required=True)
        # Pinning a default database is now OPTIONAL (0.11.0). When the
        # user leaves it blank we connect to the server and they can pick
        # the database at /run / /sync / /ask time. Encourage filling it
        # in for single-DB workflows by keeping the prompt example and
        # using ``allow_clear=True`` so an explicit blank is accepted.
        database = _ask_update_text(
            "Database name (optional, e.g. postgres — leave blank to pick at command time)",
            defaults.database or "",
        )
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
        account = _ask_update_text(
            "Snowflake account identifier (e.g. xy12345.us-east-1)",
            defaults.account,
            required=True,
            allow_clear=False,
        )
        user = _ask_update_text(
            "Username (e.g. ANALYST)", defaults.user, required=True, allow_clear=False
        )
        password = _ask_update_secret("Password", defaults.password or "", required=True)
        # Optional in 0.11.0 — see note on PostgreSQL above.
        database = _ask_update_text(
            "Database name (optional, e.g. ANALYTICS — leave blank to pick at command time)",
            defaults.database,
        )
        warehouse = _ask_update_text(
            "Warehouse (optional, e.g. COMPUTE_WH)", defaults.warehouse or ""
        )
        role = _ask_update_text("Role (optional, e.g. ANALYST)", defaults.role or "")
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
            "Databricks host (e.g. adb-xxxxxxxxxxxxxxxx.0.azuredatabricks.net)",
            defaults.host,
            required=True,
            allow_clear=False,
        )
        http_path = _ask_update_text(
            "SQL warehouse HTTP path (e.g. /sql/1.0/warehouses/abc1234567890)",
            defaults.http_path,
            required=True,
            allow_clear=False,
        )
        access_token = _ask_update_secret(
            "Access token", defaults.access_token or "", required=True
        )
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
        project = _ask_update_text(
            "GCP project ID (e.g. my-company-prod)",
            defaults.project,
            required=True,
            allow_clear=False,
        )
        dataset = _ask_update_text(
            "Default dataset (optional, e.g. analytics)",
            defaults.dataset or "",
        )
        creds = _ask_update_text(
            "Service account JSON path (optional, e.g. /etc/gcp/sa.json — uses ADC if empty)",
            defaults.credentials_path or "",
        )
        return replace(
            defaults,
            backend="bigquery",
            project=project,
            dataset=dataset,
            credentials_path=creds,
        )

    if backend == "mysql":
        host = _ask_update_text(
            "Database host (e.g. db.example.com)",
            defaults.host or "",
            required=True,
            allow_clear=False,
        )
        port_raw = _ask_update_text(
            "Port (e.g. 3306)",
            str(defaults.port) if defaults.port and defaults.port != 5432 else "3306",
            required=True,
            allow_clear=False,
        )
        while not port_raw.isdigit():
            warn("Port must be a number.")
            port_raw = _ask_update_text(
                "Port (e.g. 3306)", "3306", required=True, allow_clear=False
            )
        user = _ask_update_text(
            "Username (e.g. analyst)", defaults.user or "", required=True, allow_clear=False
        )
        password = _ask_update_secret("Password", defaults.password or "", required=True)
        database = _ask_update_text(
            "Database name (optional — leave blank to pick at command time)",
            defaults.database or "",
        )
        return replace(
            defaults,
            backend="mysql",
            host=host,
            port=int(port_raw),
            user=user,
            password=password,
            database=database,
        )

    if backend == "oracle":
        host = _ask_update_text(
            "Database host (e.g. ora.example.com)",
            defaults.host or "",
            required=True,
            allow_clear=False,
        )
        port_raw = _ask_update_text(
            "Port (e.g. 1521)",
            str(defaults.port) if defaults.port and defaults.port != 5432 else "1521",
            required=True,
            allow_clear=False,
        )
        while not port_raw.isdigit():
            warn("Port must be a number.")
            port_raw = _ask_update_text(
                "Port (e.g. 1521)", "1521", required=True, allow_clear=False
            )
        user = _ask_update_text(
            "Username (e.g. APP_USER)", defaults.user or "", required=True, allow_clear=False
        )
        password = _ask_update_secret("Password", defaults.password or "", required=True)
        service_name = _ask_update_text(
            "Service name (preferred for Oracle Cloud / RAC, e.g. XEPDB1) — "
            "leave blank to use SID instead",
            defaults.service_name or "",
        )
        database = ""
        if not service_name:
            database = _ask_update_text(
                "SID (used when service_name is blank, e.g. XE)",
                defaults.database or "",
                required=True,
                allow_clear=False,
            )
        return replace(
            defaults,
            backend="oracle",
            host=host,
            port=int(port_raw),
            user=user,
            password=password,
            service_name=service_name,
            database=database,
        )

    if backend == "mssql":
        host = _ask_update_text(
            "Database host (e.g. mssql.example.com)",
            defaults.host or "",
            required=True,
            allow_clear=False,
        )
        port_raw = _ask_update_text(
            "Port (e.g. 1433)",
            str(defaults.port) if defaults.port and defaults.port != 5432 else "1433",
            required=True,
            allow_clear=False,
        )
        while not port_raw.isdigit():
            warn("Port must be a number.")
            port_raw = _ask_update_text(
                "Port (e.g. 1433)", "1433", required=True, allow_clear=False
            )
        user = _ask_update_text(
            "Username (e.g. sa)", defaults.user or "", required=True, allow_clear=False
        )
        password = _ask_update_secret("Password", defaults.password or "", required=True)
        database = _ask_update_text(
            "Database name (optional — leave blank to pick at command time)",
            defaults.database or "",
        )
        driver = _ask_update_text(
            "ODBC driver name (default: 'ODBC Driver 18 for SQL Server')",
            defaults.driver or "",
        )
        encrypt = _ask_update_bool(
            "Encrypt the connection? (set False only for legacy on-prem servers without TLS)",
            bool(defaults.encrypt),
        )
        trust = _ask_update_bool(
            "Trust the server certificate? (use True only with self-signed certs)",
            bool(defaults.trust_server_certificate),
        )
        return replace(
            defaults,
            backend="mssql",
            host=host,
            port=int(port_raw),
            user=user,
            password=password,
            database=database,
            driver=driver,
            encrypt=encrypt,
            trust_server_certificate=trust,
        )

    if backend == "redshift":
        host = _ask_update_text(
            "Cluster endpoint (e.g. my-cluster.xxx.eu-west-1.redshift.amazonaws.com)",
            defaults.host or "",
            required=True,
            allow_clear=False,
        )
        port_raw = _ask_update_text(
            "Port (e.g. 5439)",
            str(defaults.port) if defaults.port and defaults.port != 5432 else "5439",
            required=True,
            allow_clear=False,
        )
        while not port_raw.isdigit():
            warn("Port must be a number.")
            port_raw = _ask_update_text(
                "Port (e.g. 5439)", "5439", required=True, allow_clear=False
            )
        user = _ask_update_text(
            "Username (e.g. admin)", defaults.user or "", required=True, allow_clear=False
        )
        password = _ask_update_secret("Password", defaults.password or "", required=True)
        database = _ask_update_text(
            "Database name (e.g. dev — leave blank to pick at command time)",
            defaults.database or "",
        )
        cluster = _ask_update_text(
            "Cluster identifier (optional, only needed for IAM auth)",
            defaults.cluster_identifier or "",
        )
        return replace(
            defaults,
            backend="redshift",
            host=host,
            port=int(port_raw),
            user=user,
            password=password,
            database=database,
            cluster_identifier=cluster,
        )

    if backend == "clickhouse":
        host = _ask_update_text(
            "ClickHouse host (e.g. ch.example.com)",
            defaults.host or "",
            required=True,
            allow_clear=False,
        )
        secure = _ask_update_bool(
            "Use HTTPS? (8443 / cloud) — answer No for plain HTTP (8123 / on-prem)",
            bool(defaults.secure),
        )
        default_port = "8443" if secure else "8123"
        port_raw = _ask_update_text(
            f"Port (default {default_port})",
            str(defaults.port) if defaults.port and defaults.port != 5432 else default_port,
            required=True,
            allow_clear=False,
        )
        while not port_raw.isdigit():
            warn("Port must be a number.")
            port_raw = _ask_update_text(
                f"Port (default {default_port})", default_port, required=True, allow_clear=False
            )
        user = _ask_update_text("Username (default: 'default')", defaults.user or "default")
        password = _ask_update_secret(
            "Password (blank for the default 'no password' user)",
            defaults.password or "",
            required=False,
        )
        database = _ask_update_text(
            "Database (e.g. analytics — leave blank to pick at command time)",
            defaults.database or "",
        )
        return replace(
            defaults,
            backend="clickhouse",
            host=host,
            port=int(port_raw),
            user=user,
            password=password,
            database=database,
            secure=secure,
        )

    if backend == "duckdb":
        database = _ask_update_text(
            "Path to .duckdb file (or ':memory:' for an ephemeral database)",
            defaults.database or ":memory:",
            required=True,
            allow_clear=False,
        )
        return replace(
            defaults,
            backend="duckdb",
            database=database,
        )

    return defaults


def cmd_add_profile(
    cfg: AMXConfig,
    rest: list[str],
    *,
    log_event: LogEvent | None = None,
) -> None:
    name = rest[0] if len(rest) >= 1 else ask("Profile name", default="local")
    existing = cfg.db_profiles.get(name)
    if existing is not None:
        info(f"Editing profile: {name}")
        # Editing an existing profile — keep its current values as
        # defaults so the user can press Enter to skip unchanged fields.
        db = interactive_db_block(existing)
    else:
        info(f"Creating new profile: {name}")
        # New profile — every prompt starts blank. We deliberately do
        # NOT use ``cfg.db`` (the active profile) as defaults here, or a
        # /add-db-profile would silently pre-fill with the active
        # profile's host / token / etc.
        db = interactive_db_block(None)
    # Use the safe upsert + transactional set_active path. The previous inline
    # ``cfg.active_db_profile = name; cfg.db = db`` ordering tripped the
    # autosave hook between the two assignments — the intermediate save() ran
    # ``db_profiles[name] = self.db`` while ``self.db`` was still the OLD active
    # profile's data. The final ``cfg.db = db`` corrected the dict, but the
    # symmetric LLM path (cmd_add_llm_profile + set_active_llm_profile) had no
    # such corrective second write and persisted the stale-mirror data —
    # surfacing as the user-reported "newly created profile is empty after
    # restart". upsert_db_profile + set_active_db_profile inside transaction()
    # collapse the writes so save runs once with consistent state.
    with cfg.transaction():
        cfg.upsert_db_profile(name, db)
        cfg.set_active_db_profile(name)
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
    # Guard: removing the profile that hosts the shared run-history
    # schema would orphan the connection on the next AMX startup
    # (factory falls back to local-only with a warning, but the user
    # would not necessarily notice). Make the consequence explicit
    # before dropping the profile.
    if (
        getattr(cfg, "history_store_enabled", False)
        and getattr(cfg, "history_store_profile", "") == name
    ):
        schema = cfg.history_store_schema or "AMX"
        warn(
            f"Profile {name!r} is the host for shared run-history "
            f"(schema {schema!r}). Removing it will:\n"
            "  • Disable shared mode on this machine.\n"
            "  • Leave the shared schema on the remote untouched (your\n"
            "    teammates' rows are safe).\n"
            "  • Strand any pending shared writes still in the local outbox."
        )
        if not confirm(
            f"Remove profile {name!r} AND disable shared run-history?",
            default=False,
        ):
            info("Profile removal cancelled.")
            return
        # Auto-disable shared mode so the next session does not try
        # to bootstrap against the now-deleted profile and surface a
        # confusing "profile not found" warning at startup.
        with cfg.transaction():
            cfg.history_store_enabled = False
            cfg.history_store_profile = ""
    try:
        cfg.remove_db_profile(name)
        cfg.save()
        success(f"Removed profile: {name} (active: {cfg.active_db_profile})")
    except Exception as exc:
        error(str(exc))


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
    info(f"  Backend:    [cyan]{profile.backend}[/cyan]")
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
        ca_path = "" if ca_arg.lower() in {"clear", "none", "off", "-"} else ca_arg

    cfg.db.tls_no_verify = no_verify
    cfg.db.tls_trusted_ca_file = ca_path
    if cfg.active_db_profile and cfg.active_db_profile in cfg.db_profiles:
        cfg.db_profiles[cfg.active_db_profile].tls_no_verify = no_verify
        cfg.db_profiles[cfg.active_db_profile].tls_trusted_ca_file = ca_path
    cfg.save()

    shown_path = ca_path or "(none)"
    success(f"Databricks TLS settings saved: tls_no_verify={no_verify}, trusted_ca={shown_path}.")


def cmd_cleanup_placeholders(cfg: AMXConfig, rest: list[str]) -> None:
    """Remove auto-inference fallback placeholder strings from the live DB.

    Pre-v0.6.3 ``/run-apply`` could write the placeholder text
    ``"... Auto-inference missed a reliable description; please review
    manually."`` directly to ``COMMENT ON TABLE/COLUMN`` when the LLM
    failed to produce a real description. v0.6.3 stops the write at the
    source, but legacy DBs already polluted with the placeholder need
    a one-shot cleanup. This command scans every table/column comment
    in the active DB profile, NULLs out any matching placeholder, and
    reports counts. Optional ``[schema]`` arg limits the scope.
    """
    from amx.agents.orchestrator import is_placeholder_description
    from amx.db.connector import AssetKind, DatabaseConnector

    target_schema = rest[0].strip() if rest else ""
    db = DatabaseConnector(cfg.db)
    try:
        try:
            available = [str(s) for s in db.list_schemas()]
        except Exception as exc:
            error(f"Could not list schemas: {exc}")
            return

        target_schemas: list[str]
        if target_schema:
            match = next((s for s in available if s.lower() == target_schema.lower()), None)
            if match is None:
                error(f"No schema named '{target_schema}'. Available: {', '.join(available)}")
                return
            target_schemas = [match]
        else:
            target_schemas = available

        heading(f"Cleanup: scanning {len(target_schemas)} schema(s) for fallback placeholders")
        cleaned_table = 0
        cleaned_column = 0
        for sch in target_schemas:
            try:
                if hasattr(db, "list_assets"):
                    assets = list(db.list_assets(sch))
                else:
                    assets = [(name, AssetKind.TABLE) for name in db.list_tables(sch)]
            except Exception as exc:
                warn(f"Could not list assets in {sch}: {exc}")
                continue
            for asset_name, asset_kind in assets:
                kind = asset_kind if isinstance(asset_kind, AssetKind) else AssetKind.TABLE
                # Table-level comment
                try:
                    tcom = db.get_table_comment(sch, asset_name)
                except Exception:
                    tcom = None
                if is_placeholder_description(tcom):
                    try:
                        db.apply_comment(
                            schema=sch,
                            table=asset_name,
                            column=None,
                            comment="",
                            asset_kind=kind,
                        )
                        cleaned_table += 1
                        info(f"Cleared placeholder on {sch}.{asset_name} (table comment)")
                    except Exception as exc:
                        warn(f"Could not clear {sch}.{asset_name}: {exc}")
                # Column-level comments
                try:
                    col_comments = db.get_column_comments(sch, asset_name)
                except Exception:
                    col_comments = {}
                for col_name, col_comment in col_comments.items():
                    if is_placeholder_description(col_comment):
                        try:
                            db.apply_comment(
                                schema=sch,
                                table=asset_name,
                                column=col_name,
                                comment="",
                                asset_kind=kind,
                            )
                            cleaned_column += 1
                        except Exception as exc:
                            warn(f"Could not clear {sch}.{asset_name}.{col_name}: {exc}")
                if cleaned_column and cleaned_column % 25 == 0:
                    info(f"  cleared {cleaned_column} column placeholders so far …")
        success(
            f"Cleanup done. Cleared {cleaned_table} table comment(s) and "
            f"{cleaned_column} column comment(s). Re-run /run-apply with "
            f"missing-only to fill them with real descriptions."
        )
    finally:
        with contextlib.suppress(Exception):
            db.close()
