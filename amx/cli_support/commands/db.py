"""Database namespace helpers for the AMX interactive CLI."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import Any

from amx.cli_support._db_diagnostics import (  # noqa: PLC0414
    _BACKEND_DRIVER_PROBES as _BACKEND_DRIVER_PROBES,
)
from amx.cli_support._db_diagnostics import (
    _env_trusted_ca_candidate as _env_trusted_ca_candidate,
)
from amx.cli_support._db_diagnostics import (
    _is_databricks_tls_failure as _is_databricks_tls_failure,
)
from amx.cli_support._db_diagnostics import (
    _offer_to_install_backend_driver as _offer_to_install_backend_driver,
)
from amx.cli_support._db_diagnostics import (
    _print_system_prereq_hint as _print_system_prereq_hint,
)
from amx.cli_support._db_profiling import (  # noqa: PLC0414
    _format_age as _format_age,
)
from amx.cli_support._db_profiling import (
    cmd_inspect as cmd_inspect,
)
from amx.cli_support._db_profiling import (
    cmd_profiling as cmd_profiling,
)
from amx.config import (
    SUPPORTED_BACKENDS,
    AMXConfig,
    DBConfig,
    _normalize_db_host,
)
from amx.utils.console import (
    ask,
    ask_choice,
    ask_multi_choice,
    ask_password,
    confirm,
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
        "/use-db sets the default-fallback profile -- the one CLI commands "
        "use when no --profile is passed. AMX Studio picks per-action; the "
        "default only matters for the CLI. "
        "/add-db-profile first asks which engine (PostgreSQL, Snowflake, Databricks, BigQuery), then connection details."
    )


def cmd_profiles(cfg: AMXConfig) -> None:
    from amx.config import has_legacy_database_default

    rows = []
    legacy_profiles: list[str] = []
    # The asterisk now flags the *default-fallback* profile -- the one
    # CLI commands use when ``--profile`` is omitted. Studio no longer
    # has an "active" notion (every defined profile is selectable
    # per-action), so the marker only matters here in the CLI listing.
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
        "DB profiles (* = CLI default-fallback, ? = no DB pinned)",
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
    """Set the CLI's default-fallback DB scope.

    AMX Studio (0.13+) picks a DB profile per-action -- the user names
    the target on Run, Ask, and Browse, so there is no Studio-level
    "active" pill. ``/use-db`` survives because the CLI still needs a
    default for ``amx run`` / ``/sync`` etc. invoked without
    ``--profile``: pick the profile (or profiles) that the next
    such call should fall back to.

    Single-arg form pins one profile as the default-fallback;
    multi-arg form pins a multi-profile scope (used by /ask, /run,
    /sync to fan out automatically).

        /use-db prod_pg                 -> single profile
        /use-db prod_pg analytics_bq    -> multi-profile scope

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
                f"Default-fallback DB profile set to: {chosen[0]} [{p.backend}] - {p.display_summary}"
            )
        else:
            success(
                f"Default-fallback DB scope: {', '.join(chosen)} (primary = {chosen[0]} [{p.backend}])"
            )
        # Reassure the user that switching the default-fallback scope
        # does not detach the shared run-history store: that one stays
        # pinned to whichever profile was passed to
        # `/history-store enable` until the user explicitly disables
        # it or removes the host profile.
        host = (getattr(cfg, "history_store_profile", "") or "").strip()
        if getattr(cfg, "history_store_enabled", False) and host and host != chosen[0]:
            info(
                f"Shared run-history is still hosted on profile {host!r} "
                "(this switch does not detach it). "
                "Run `/history-store status` to inspect."
            )
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


def _ask_catalog_or_database_with_picker(
    *,
    label: str,
    current_value: str,
    optional: bool,
    probe_cfg: DBConfig,
    listing_kind: str,
) -> str:
    """Offer a picker over the catalogs/databases the live backend reports.

    Replaces the historical free-form prompt at this stage of the
    wizard. The user typed a non-existent catalog (``sap``) on a
    Databricks workspace where it didn't exist; the profile saved
    silently, and every subsequent ``amx`` startup raised
    SCHEMA_NOT_FOUND when the lazy bootstrap reached for the missing
    schema. With a picker, that class of footgun cannot happen unless
    the user explicitly bypasses it via "type custom value" + a
    "save anyway?" confirm.

    Falls back to the free-form prompt when listing fails (no driver,
    permission denied, network down). The fallback path emits one
    ``warn()`` so the user knows validation is degraded, but the wizard
    still progresses — you can configure a profile while offline.
    """
    from amx.db.connector import DatabaseConnector

    info(f"Probing the {probe_cfg.backend} backend for available {listing_kind}…")
    candidates: list[str] = []
    listing_error: BaseException | None = None
    try:
        connector = DatabaseConnector(probe_cfg)
        if listing_kind == "catalogs":
            candidates = list(connector.list_catalogs() or [])
        else:
            candidates = list(connector.list_databases() or [])
    except BaseException as exc:  # noqa: BLE001 — we genuinely want every failure mode
        listing_error = exc

    if not candidates:
        if listing_error is not None:
            warn(
                f"Could not probe {listing_kind} ({listing_error.__class__.__name__}). "
                "Falling back to free-form input — double-check spelling."
            )
        else:
            warn(
                f"Backend returned no {listing_kind} (role lacks visibility, or "
                "discovery is unsupported). Falling back to free-form input."
            )
        return _ask_update_text(
            f"{label.capitalize()} (optional)" if optional else label.capitalize(),
            current_value,
            required=not optional,
            allow_clear=optional,
        )

    keep_label = f"(keep current: {current_value})" if current_value else ""
    custom_label = "(type custom value)"
    none_label = "(none)" if optional else ""

    choices: list[str] = []
    descriptions: dict[str, str] = {}
    if keep_label:
        choices.append(keep_label)
        descriptions[keep_label] = (
            f"Keep the existing value {current_value!r}."
            if current_value in candidates
            else f"Keep {current_value!r} (NOT visible in current listing)."
        )
    if none_label:
        choices.append(none_label)
        descriptions[none_label] = f"Leave {label} unset — pick at command time."
    for value in candidates:
        if value == current_value:
            continue  # already covered by the keep-current entry
        choices.append(value)
    choices.append(custom_label)
    descriptions[custom_label] = (
        "Type a name not in the listing (only when discovery is permission-blocked)."
    )

    default = keep_label or none_label or candidates[0]
    picked = ask_choice(
        f"Select {label}",
        choices,
        default=default,
        descriptions=descriptions,
    )

    if not picked or picked == keep_label:
        return current_value
    if picked == none_label:
        return ""
    if picked != custom_label:
        return picked

    # Free-form escape hatch — keep the listing visible so the user can
    # tell whether they're typing a real name or going off-piste.
    info(f"Available {listing_kind}: {', '.join(candidates) or '(none)'}")
    typed = _ask_update_text(
        f"{label.capitalize()} (custom)",
        current_value,
        required=not optional,
        allow_clear=optional,
    )
    if typed and typed not in candidates:
        warn(
            f"{typed!r} is NOT in the listing returned by the backend. "
            f"This is the same class of bug that produced the SCHEMA_NOT_FOUND "
            f"warning — only override if you know discovery is permission-blocked."
        )
        if not confirm("Save anyway?", default=False):
            warn("Re-opening the picker so you can pick a real value.")
            return _ask_catalog_or_database_with_picker(
                label=label,
                current_value=current_value,
                optional=optional,
                probe_cfg=probe_cfg,
                listing_kind=listing_kind,
            )
    return typed


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
            "trino": "Host/port user/password (or JWT) + catalog - Trino / Presto; COMMENT ON DDL",
            "hive": "Host/port user/password - HiveServer2; partial comment write-back (no columns)",
        },
    )
    # Probe the chosen backend's optional driver up front and offer
    # to ``pip install`` it inline. The 0.12.2 version of this hook
    # only printed a hint and continued — but the hint itself was
    # eaten by Rich markup, so the user typed the wrong command (or
    # gave up). 0.12.3: actually run the install for them when they
    # say Y.
    _offer_to_install_backend_driver(backend)

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
            jwt_token="",
            http_scheme="https",
            auth_mode="",
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
        # the database at /run / /sync / /ask time. The picker below
        # confirms the typed name actually exists on the server before
        # saving — same gate the Databricks catalog prompt added.
        probe_cfg_for_db = replace(
            defaults,
            backend="postgresql",
            host=host,
            port=int(port_raw),
            user=user,
            password=password,
            database="",
        )
        database = _ask_catalog_or_database_with_picker(
            label="database",
            current_value=defaults.database or "",
            optional=True,
            probe_cfg=probe_cfg_for_db,
            listing_kind="databases",
        )
        # Advanced TLS — gated so the basic flow stays terse for the
        # 90% case (libpq's default ``prefer`` is fine on local Postgres).
        # Corporate / managed PG (RDS, CloudSQL, Azure Database for PG)
        # increasingly requires ``verify-full`` against a private CA;
        # surface those knobs here so users don't fall back to manual
        # YAML editing the way the Databricks-TLS gap forced them to.
        sslmode = defaults.sslmode or ""
        sslrootcert = defaults.sslrootcert or ""
        if _ask_update_bool(
            "Configure TLS / SSL (sslmode, sslrootcert)?",
            current=bool(defaults.sslmode or defaults.sslrootcert),
        ):
            sslmode = _ask_update_text(
                "sslmode (disable / allow / prefer / require / verify-ca / verify-full)",
                defaults.sslmode or "",
            )
            sslrootcert = _ask_update_text(
                "Path to SSL root cert (required for verify-ca / verify-full)",
                defaults.sslrootcert or "",
            )
        return replace(
            defaults,
            backend="postgresql",
            host=host,
            port=int(port_raw),
            user=user,
            password=password,
            database=database,
            sslmode=sslmode,
            sslrootcert=sslrootcert,
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
        # Optional in 0.11.0 — same picker pattern as PostgreSQL.
        probe_cfg_for_db = replace(
            defaults,
            backend="snowflake",
            account=account,
            user=user,
            password=password,
            database="",
        )
        database = _ask_catalog_or_database_with_picker(
            label="database",
            current_value=defaults.database or "",
            optional=True,
            probe_cfg=probe_cfg_for_db,
            listing_kind="databases",
        )
        warehouse = _ask_update_text(
            "Warehouse (optional, e.g. COMPUTE_WH)", defaults.warehouse or ""
        )
        role = _ask_update_text("Role (optional, e.g. ANALYST)", defaults.role or "")
        # Advanced TLS / OCSP — most users don't need it, but corporate
        # proxies that block the OCSP responder hang the connect
        # handshake until ``ocsp_fail_open=True`` is set.
        insecure_mode = bool(defaults.insecure_mode)
        ocsp_fail_open = bool(defaults.ocsp_fail_open)
        if _ask_update_bool(
            "Configure TLS / OCSP escape hatches (corporate proxies)?",
            current=insecure_mode or ocsp_fail_open,
        ):
            insecure_mode = _ask_update_bool(
                "Disable Snowflake TLS validation (insecure_mode)?",
                current=insecure_mode,
            )
            ocsp_fail_open = _ask_update_bool(
                "Allow connect when OCSP responder is blocked (ocsp_fail_open)?",
                current=ocsp_fail_open,
            )
        return replace(
            defaults,
            backend="snowflake",
            account=account,
            user=user,
            password=password,
            database=database,
            warehouse=warehouse,
            role=role,
            insecure_mode=insecure_mode,
            ocsp_fail_open=ocsp_fail_open,
        )

    if backend == "databricks":
        host = _ask_update_text(
            "Databricks host (e.g. adb-xxxxxxxxxxxxxxxx.0.azuredatabricks.net)",
            defaults.host,
            required=True,
            allow_clear=False,
        )
        # Accept the full workspace URL too — strip scheme + trailing
        # slash so the SQLAlchemy URL builder doesn't choke on
        # ``host/:443`` later.
        host = _normalize_db_host(host)
        http_path = _ask_update_text(
            "SQL warehouse HTTP path (e.g. /sql/1.0/warehouses/abc1234567890)",
            defaults.http_path,
            required=True,
            allow_clear=False,
        )
        access_token = _ask_update_secret(
            "Access token", defaults.access_token or "", required=True
        )
        # Ask TLS settings BEFORE probing the workspace. Corporate
        # Databricks setups frequently sit behind a self-signed proxy,
        # and the previous wizard order (probe-then-TLS) blew up with
        # SSLCertVerificationError on the very first ``SHOW CATALOGS``
        # call — the fallback masked the cert problem and pushed the
        # user back to free-form input. Asking TLS first means the
        # picker probe respects the user's trust settings.
        tls_trusted_ca_file = _ask_update_text(
            "Trusted CA bundle path (optional, for corporate/self-signed TLS)",
            defaults.tls_trusted_ca_file or "",
        )
        tls_no_verify = _ask_update_bool(
            "Disable TLS certificate verification? "
            "(on enterprise networks: set the Trusted CA bundle path above, "
            "or enable this — pick whichever your IT policy allows)",
            bool(defaults.tls_no_verify),
        )
        # Gate the catalog probe behind an explicit yes/no. Probing
        # always involves a live SHOW CATALOGS round-trip; on flaky or
        # restricted networks that's a 30-second wait followed by an
        # unhelpful warning. When the user already knows their catalog
        # name (typical second-time-through case) they should just type
        # it instead of paying the round-trip cost.
        probe_catalogs = _ask_update_bool(
            "List the available Unity Catalog catalogs from the workspace? "
            "(uses the credentials you just entered to run SHOW CATALOGS)",
            current=False,
        )
        if probe_catalogs:
            probe_cfg_for_catalog = replace(
                defaults,
                backend="databricks",
                host=host,
                http_path=http_path,
                access_token=access_token,
                catalog="",
                database="",
                tls_trusted_ca_file=tls_trusted_ca_file,
                tls_no_verify=tls_no_verify,
            )
            # Catalog is required because the adapter's catalog-less
            # ``SHOW SCHEMAS`` path falls back to SQLAlchemy's inspector,
            # which on Unity Catalog returns ambiguous (or empty) results
            # — the bug surfaced as "fresh profile, listing returns
            # nothing" on every UC workspace. Legacy hive_metastore-only
            # workspaces should type ``hive_metastore`` here explicitly.
            catalog = _ask_catalog_or_database_with_picker(
                label="Unity Catalog",
                current_value=defaults.catalog or "",
                optional=False,
                probe_cfg=probe_cfg_for_catalog,
                listing_kind="catalogs",
            )
        else:
            catalog = _ask_update_text(
                "Unity Catalog (required; type 'hive_metastore' for legacy workspaces)",
                defaults.catalog or "",
                required=True,
                allow_clear=False,
            )
        database = _ask_update_text("Schema / database (optional)", defaults.database or "")
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
        location = _ask_update_text(
            "Query location (optional, e.g. EU / US / europe-west3 — empty uses project default)",
            defaults.location or "",
        )
        impersonate = _ask_update_text(
            "Impersonate service-account email (optional, for workload-identity flows)",
            defaults.impersonate_service_account or "",
        )
        return replace(
            defaults,
            backend="bigquery",
            project=project,
            dataset=dataset,
            credentials_path=creds,
            location=location,
            impersonate_service_account=impersonate,
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
        probe_cfg_for_db = replace(
            defaults,
            backend="mysql",
            host=host,
            port=int(port_raw),
            user=user,
            password=password,
            database="",
        )
        database = _ask_catalog_or_database_with_picker(
            label="database",
            current_value=defaults.database or "",
            optional=True,
            probe_cfg=probe_cfg_for_db,
            listing_kind="databases",
        )
        # MySQL TLS — same pattern as PG. Default behaviour (the driver
        # negotiates TLS opportunistically) needs no input; the gate is
        # for users who must point at a private CA bundle or who must
        # explicitly opt out for legacy intra-DC links.
        ssl_disabled = bool(defaults.ssl_disabled)
        ssl_ca = defaults.ssl_ca or ""
        if _ask_update_bool(
            "Configure TLS (ssl_disabled / ssl_ca)?",
            current=ssl_disabled or bool(ssl_ca),
        ):
            ssl_disabled = _ask_update_bool(
                "Disable TLS entirely (legacy intra-DC only)?",
                current=ssl_disabled,
            )
            if not ssl_disabled:
                ssl_ca = _ask_update_text(
                    "Path to SSL CA bundle (optional, for private CA)",
                    ssl_ca,
                )
        return replace(
            defaults,
            backend="mysql",
            host=host,
            port=int(port_raw),
            user=user,
            password=password,
            database=database,
            ssl_disabled=ssl_disabled,
            ssl_ca=ssl_ca,
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
        probe_cfg_for_db = replace(
            defaults,
            backend="mssql",
            host=host,
            port=int(port_raw),
            user=user,
            password=password,
            database="",
        )
        database = _ask_catalog_or_database_with_picker(
            label="database",
            current_value=defaults.database or "",
            optional=True,
            probe_cfg=probe_cfg_for_db,
            listing_kind="databases",
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
        probe_cfg_for_db = replace(
            defaults,
            backend="redshift",
            host=host,
            port=int(port_raw),
            user=user,
            password=password,
            database="",
        )
        database = _ask_catalog_or_database_with_picker(
            label="database",
            current_value=defaults.database or "",
            optional=True,
            probe_cfg=probe_cfg_for_db,
            listing_kind="databases",
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
        probe_cfg_for_db = replace(
            defaults,
            backend="clickhouse",
            host=host,
            port=int(port_raw),
            user=user,
            password=password,
            database="",
            secure=secure,
        )
        database = _ask_catalog_or_database_with_picker(
            label="database",
            current_value=defaults.database or "",
            optional=True,
            probe_cfg=probe_cfg_for_db,
            listing_kind="databases",
        )
        # ClickHouse TLS — only meaningful on HTTPS. ``ca_cert`` lets
        # users point at a private CA bundle; ``verify=False`` is the
        # last-resort drop for TLS-inspecting proxies that present a
        # non-distributable root.
        ca_cert = defaults.ca_cert or ""
        verify = bool(defaults.verify) if defaults.verify is not None else True
        if secure and _ask_update_bool(
            "Configure TLS verification (ca_cert / verify)?",
            current=bool(ca_cert) or not verify,
        ):
            ca_cert = _ask_update_text(
                "Path to CA bundle (optional, for private root)",
                ca_cert,
            )
            verify = _ask_update_bool(
                "Verify TLS certificate (off only for inspecting proxies)?",
                current=verify,
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
            ca_cert=ca_cert,
            verify=verify,
        )

    if backend == "trino":
        host = _ask_update_text(
            "Trino coordinator host (e.g. trino.example.com or localhost)",
            defaults.host or "",
            required=True,
            allow_clear=False,
        )
        scheme_default = (defaults.http_scheme or "https").lower()
        scheme = ask_choice(
            "HTTP scheme (https for production, http for local Docker)",
            ["https", "http"],
            default=scheme_default,
        )
        default_port = 443 if scheme == "https" else 8080
        port_raw = _ask_update_text(
            f"Port (default {default_port} for {scheme})",
            str(defaults.port) if defaults.port else str(default_port),
            required=True,
            allow_clear=False,
        )
        while not port_raw.isdigit():
            warn("Port must be a number.")
            port_raw = _ask_update_text(
                f"Port (default {default_port} for {scheme})",
                str(defaults.port) if defaults.port else str(default_port),
                required=True,
                allow_clear=False,
            )
        user = _ask_update_text(
            "Username (Trino user — used for query auditing even with JWT)",
            defaults.user or "",
            required=True,
            allow_clear=False,
        )
        # Auth picker — Basic (default) covers most deployments; JWT is
        # common on Starburst Galaxy and behind OAuth-issued tokens.
        # OAuth2 / Kerberos are documented as future wizard surface but
        # not collected here — power users can hand-edit config.yml.
        auth_choice = ask_choice(
            "Authentication mode",
            ["basic", "jwt"],
            default="jwt" if defaults.jwt_token else "basic",
            descriptions={
                "basic": "Username + password (HTTP Basic against the coordinator)",
                "jwt": "Username + JWT bearer token (Starburst Galaxy, OAuth-minted tokens)",
            },
        )
        password = ""
        jwt_token = ""
        if auth_choice == "basic":
            password = _ask_update_secret(
                "Password (leave blank for anonymous on a dev cluster)",
                defaults.password or "",
                required=False,
            )
        else:
            jwt_token = _ask_update_secret(
                "JWT bearer token",
                defaults.jwt_token or "",
                required=True,
            )
        # Catalog — Trino's 3-level hierarchy. Optional at profile
        # time: when blank, the user picks at command time.
        catalog = _ask_update_text(
            "Default catalog (leave blank to pick at command time)",
            defaults.catalog or "",
            required=False,
            allow_clear=True,
        )
        # Schema is optional too — kept under ``database`` per AMX
        # convention so the rest of the codebase reuses one field.
        database = _ask_update_text(
            "Default schema (leave blank to pick at command time)",
            defaults.database or "",
            required=False,
            allow_clear=True,
        )
        # TLS verification — only meaningful on https. When the user
        # picked http for a local Docker test, skip the prompt entirely.
        verify = True
        tls_ca = ""
        if scheme == "https":
            verify = _ask_update_bool(
                "Verify TLS certificate? (Y for production, N only behind a TLS-inspecting proxy)",
                current=bool(defaults.verify),
            )
            tls_ca = _ask_update_text(
                "Path to private CA bundle (leave blank for system trust store)",
                defaults.tls_trusted_ca_file or "",
                required=False,
                allow_clear=True,
            )
        return replace(
            defaults,
            backend="trino",
            host=host,
            port=int(port_raw),
            user=user,
            password=password,
            jwt_token=jwt_token,
            catalog=catalog,
            database=database,
            http_scheme=scheme,
            verify=verify,
            tls_trusted_ca_file=tls_ca,
        )

    if backend == "hive":
        host = _ask_update_text(
            "HiveServer2 host (e.g. hive.example.com or localhost)",
            defaults.host or "",
            required=True,
            allow_clear=False,
        )
        port_raw = _ask_update_text(
            "Port (default 10000 for HiveServer2)",
            str(defaults.port) if defaults.port else "10000",
            required=True,
            allow_clear=False,
        )
        while not port_raw.isdigit():
            warn("Port must be a number.")
            port_raw = _ask_update_text(
                "Port (default 10000 for HiveServer2)",
                str(defaults.port) if defaults.port else "10000",
                required=True,
                allow_clear=False,
            )
        # Auth picker — NOSASL is dev/local only; PLAIN is the workhorse
        # against a SASL/PLAIN HiveServer2 (often LDAP-backed); LDAP is
        # the explicit "auth against an LDAP directory" path. Kerberos
        # is documented but not in the picker — see CLAUDE.md for the
        # hand-edit path.
        auth_default = (defaults.auth_mode or "PLAIN").upper()
        auth_mode = ask_choice(
            "Authentication mode (NOSASL is for local dev clusters only)",
            ["PLAIN", "LDAP", "NOSASL"],
            default=auth_default if auth_default in {"PLAIN", "LDAP", "NOSASL"} else "PLAIN",
            descriptions={
                "PLAIN": "SASL PLAIN — username + password (most production clusters)",
                "LDAP": "SASL PLAIN against an LDAP directory (Cloudera / EMR pattern)",
                "NOSASL": "No authentication — dev clusters only",
            },
        )
        user = _ask_update_text(
            "Username (Hive user — also recorded in query history)",
            defaults.user or "",
            required=True,
            allow_clear=False,
        )
        password = ""
        if auth_mode in {"PLAIN", "LDAP"}:
            password = _ask_update_secret(
                "Password",
                defaults.password or "",
                required=True,
            )
        database = _ask_update_text(
            "Default database / schema (leave blank to pick at command time)",
            defaults.database or "",
            required=False,
            allow_clear=True,
        )
        return replace(
            defaults,
            backend="hive",
            host=host,
            port=int(port_raw),
            user=user,
            password=password,
            database=database,
            auth_mode=auth_mode,
        )

    if backend == "duckdb":
        database = _ask_update_text(
            "Path to .duckdb file (or ':memory:' / 'md:<db>' for MotherDuck)",
            defaults.database or ":memory:",
            required=True,
            allow_clear=False,
        )
        read_only = bool(defaults.read_only)
        motherduck_token = defaults.motherduck_token or ""
        is_motherduck = database.startswith("md:") or database == "md"
        if is_motherduck:
            # MotherDuck always needs a token. ``read_only`` is moot for
            # the cloud-attach flow, so we skip the prompt.
            motherduck_token = _ask_update_secret(
                "MotherDuck token (PAT from your MotherDuck account)",
                motherduck_token,
                required=True,
            )
        elif database != ":memory:":
            # Local file — offer the read-only toggle so multiple AMX
            # processes can share the file without fighting for the
            # exclusive lock. ``:memory:`` doesn't need it.
            read_only = _ask_update_bool(
                "Open read-only? (lets a second AMX process attach the same file)",
                current=read_only,
            )
        return replace(
            defaults,
            backend="duckdb",
            database=database,
            read_only=read_only,
            motherduck_token=motherduck_token,
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
        # Add and edit are now two clean, non-overlapping channels —
        # silently switching to edit mode here used to surprise users
        # who typed an existing name by accident. Surface the collision
        # and point them at /edit-db-profile.
        error(
            f"Profile {name!r} already exists. Run `/edit-db-profile {name}` to change its values."
        )
        return
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


def cmd_edit_profile(
    cfg: AMXConfig,
    rest: list[str],
    *,
    log_event: LogEvent | None = None,
) -> None:
    """Edit an existing DB profile — pick by name, walk the wizard
    with current values prefilled.

    Replaces the silent edit-on-collision behaviour that ``/add-db-profile``
    used to have. The wizard helpers (``_ask_update_text`` /
    ``_ask_update_secret`` / ``_ask_update_bool``) already render
    ``[Enter keeps current]`` hints, so the user can step through and
    only change what they need. Saving never auto-switches the active
    profile unless the edited profile WAS already active — touching one
    profile must not silently move the user's working scope.
    """
    available = sorted(cfg.db_profiles.keys())
    if not available:
        error("No profiles configured. Use `/add-db-profile` to create one.")
        return

    if len(rest) >= 1:
        name = rest[0]
        if name not in cfg.db_profiles:
            error(f"Unknown profile: {name!r}. Available: {', '.join(available)}.")
            return
    else:
        descriptions = {n: f"[{p.backend}] {p.display_summary}" for n, p in cfg.db_profiles.items()}
        picked = ask_choice(
            "Select profile to edit",
            available,
            default=cfg.active_db_profile or available[0],
            descriptions=descriptions,
        )
        if not picked:
            error("No profile selected.")
            return
        name = picked

    info(f"Editing profile: {name}")
    existing = cfg.db_profiles[name]
    db = interactive_db_block(existing)

    was_active = cfg.active_db_profile == name
    with cfg.transaction():
        cfg.upsert_db_profile(name, db)
        if was_active:
            # Refresh cfg.db / mirrors so the rest of the session sees
            # the edits without forcing the user to re-run /use-db.
            cfg.set_active_db_profile(name)
    if was_active:
        success(f"Profile saved (still active): {name} [{db.backend}]")
    else:
        success(f"Profile saved: {name} [{db.backend}] (active stays {cfg.active_db_profile!r})")
    if log_event is not None:
        log_event(
            event_type="db_profile_edit",
            status="success",
            command="edit-db-profile",
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
    # would not necessarily notice). Lead with the headline, reassure
    # the user that local rows survive, and offer to drain a non-empty
    # outbox to the remote backend before the profile disappears.
    if (
        getattr(cfg, "history_store_enabled", False)
        and getattr(cfg, "history_store_profile", "") == name
    ):
        schema = cfg.history_store_schema or "AMX"
        warn(
            "Heads up — shared run-history is enabled on this profile.\n"
            f"  Profile {name!r} hosts the AMX schema {schema!r}."
        )
        info(
            "Your local run history (~/.amx/history.db) stays intact. "
            "Past runs and events created on this machine remain "
            "queryable via `/history-runs` after deletion."
        )
        info(
            "The shared schema on the remote backend is also untouched — "
            "your teammates' rows are safe."
        )
        # Lazy import to avoid a circular dependency between the db and
        # history_store command modules.
        from amx.cli_support.commands.history_store import (
            _action_flush,
            _resolve_history_dual_store,
        )

        store = _resolve_history_dual_store()
        depth = 0
        if store is not None:
            try:
                depth = store.pending_count()
            except Exception:
                depth = 0
        if depth > 0:
            warn(
                f"Outbox has {depth} pending shared write(s) that have NOT "
                "reached the team backend yet."
            )
            if confirm(
                "Flush them to the remote backend before deleting? (Recommended)",
                default=True,
            ):
                _action_flush(cfg)
                try:
                    depth = store.pending_count() if store is not None else 0
                except Exception:
                    depth = 0
            if depth > 0:
                warn(
                    f"{depth} write(s) still queued — they will not be retried "
                    "after the profile is removed."
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


def cmd_tls(cfg: AMXConfig, rest: list[str]) -> None:
    """Show or update active Databricks TLS settings."""
    if cfg.db.backend != "databricks":
        error("The active DB profile is not Databricks. Switch to a Databricks profile first.")
        return

    if not rest:
        ca_path = str(getattr(cfg.db, "tls_trusted_ca_file", "") or "").strip() or "(none)"
        info(
            "Current Databricks TLS settings: "
            f"tls_no_verify=[info]{bool(getattr(cfg.db, 'tls_no_verify', False))}[/info], "
            f"trusted_ca=[info]{ca_path}[/info]. "
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


def _parse_cache_args(rest: list[str]) -> dict[str, Any]:
    """Parse ``--profile=X --database=Y --type=schemas --force`` style
    flags from ``/db cache-*`` rest tokens. Unknown flags raise so the
    handler can surface a precise error instead of silently dropping
    user intent.
    """
    out: dict[str, Any] = {
        "profile": None,
        "database": None,
        "types": None,
        "force": False,
    }
    for token in rest:
        if not token:
            continue
        if token == "--force":
            out["force"] = True
            continue
        if "=" not in token:
            raise ValueError(f"Unknown argument {token!r}; expected --flag=value")
        key, _, value = token.partition("=")
        key = key.lstrip("-").strip()
        value = value.strip()
        if key == "profile":
            out["profile"] = value
        elif key == "database":
            out["database"] = value
        elif key == "type":
            out["types"] = [t.strip() for t in value.split(",") if t.strip()]
        else:
            raise ValueError(
                f"Unknown flag --{key}; expected --profile / --database / --type / --force"
            )
    return out


def cmd_cache_show(cfg: AMXConfig, rest: list[str]) -> None:
    """/db cache-show — render per-(profile, database) cache row counts."""
    try:
        args = _parse_cache_args(rest)
    except ValueError as exc:
        error(str(exc))
        return
    from amx.storage.cache_ops import cache_inventory

    rows = cache_inventory(profile=args["profile"], database=args["database"])
    if not rows:
        info("No cached rows for the requested scope.")
        return
    table_rows = [
        [
            r.profile,
            r.database or "—",
            str(r.schemas_rows),
            str(r.columns_rows),
            str(r.catalog_rows),
            _format_age(r.last_fetch),
        ]
        for r in rows
    ]
    render_table(
        "DB cache inventory",
        ["Profile", "Database", "Schemas", "Columns", "Catalog", "Last fetch"],
        table_rows,
    )


def cmd_cache_stats(cfg: AMXConfig, rest: list[str]) -> None:
    """/db cache-stats — aggregate metrics per cache table."""
    if rest:
        warn("/db cache-stats takes no arguments; ignoring extras.")
    from amx.storage.cache_ops import cache_stats

    stats = cache_stats()
    if not stats:
        info("Cache store unavailable — has /history-store been initialised?")
        return
    for key in ("schemas", "columns", "catalog"):
        stat = stats.get(key)
        if stat is None:
            continue
        rows: list[list[str]] = [
            ["Table", stat.table],
            ["Total rows", str(stat.total_rows)],
            ["Distinct profiles", str(stat.distinct_profiles)],
            ["Distinct databases", str(stat.distinct_databases)],
            ["Oldest fetch", _format_age(stat.oldest_fetch)],
            ["Newest fetch", _format_age(stat.newest_fetch)],
        ]
        if stat.ttl_aware:
            rows.append(["Expired rows", str(stat.expired_rows)])
        else:
            rows.append(["TTL", "none — rewritten by /sync"])
        render_table(f"Cache: {key}", ["Metric", "Value"], rows)


def cmd_cache_clear(cfg: AMXConfig, rest: list[str]) -> None:
    """/db cache-clear — DELETE rows from the requested cache tables.

    Defaults to clearing all three tables for the scope. Without explicit
    --profile/--database AND without --force we double-confirm: a
    global flush is recoverable (next live read repopulates) but it
    can spike latency for the rest of the active session.
    """
    try:
        args = _parse_cache_args(rest)
    except ValueError as exc:
        error(str(exc))
        return
    from amx.storage.cache_ops import cache_clear

    profile = args["profile"]
    database = args["database"]
    types = args["types"]
    force = bool(args["force"])

    scope_desc = []
    if profile:
        scope_desc.append(f"profile={profile}")
    if database is not None:
        scope_desc.append(f"database={database or '(empty)'}")
    if types:
        scope_desc.append(f"type={','.join(types)}")
    scope_str = ", ".join(scope_desc) if scope_desc else "EVERY profile, EVERY database"

    if not force:
        if not profile and database is None:
            # Global flush — make the user type yes twice.
            if not confirm(
                f"Clear DB caches for {scope_str}? This deletes every cached schema, column comment, and catalog row.",
                default=False,
            ):
                info("Cache clear cancelled.")
                return
            if not confirm(
                "Really? Every profile, every database — confirm one more time.",
                default=False,
            ):
                info("Cache clear cancelled.")
                return
        else:
            if not confirm(f"Clear DB caches for {scope_str}?", default=False):
                info("Cache clear cancelled.")
                return

    try:
        report = cache_clear(profile=profile, database=database, types=types)
    except ValueError as exc:
        error(str(exc))
        return

    summary_rows = [[k, str(v)] for k, v in report.deleted.items()]
    summary_rows.append(["Total", str(report.total)])
    render_table(
        f"Cache clear ({scope_str})",
        ["Cache", "Rows deleted"],
        summary_rows,
    )
    success(f"Cleared {report.total} row(s) across {len(report.deleted)} cache table(s).")
