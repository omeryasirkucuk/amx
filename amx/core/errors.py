"""Central actionable error mapping for AMX."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ActionableError:
    title: str
    detail: str
    instruction: str

    def render(self) -> str:
        return f"{self.title}: {self.detail} Action: {self.instruction}"


_AUTH_PATTERNS: tuple[str, ...] = (
    "password authentication failed",
    "authentication failed",
    "incorrect username or password",
    "no password supplied",
    "role does not exist",
    "user does not exist",
    "invalid_grant",
    "invalid grant",
    "401 client error",
    "401 unauthorized",
    "unauthorized",
    "invalid token",
    "invalid access token",
    "invalid api key",
    "ldap authentication failed",
    "kerberos error",
)

_NETWORK_PATTERNS: tuple[str, ...] = (
    "could not connect",
    "connection refused",
    "connection reset",
    "name or service not known",
    "name resolution",
    "could not translate host name",
    "could not resolve host",
    "host not found",
    "no route to host",
    "network is unreachable",
    "connection aborted",
    "broken pipe",
    "temporary failure in name resolution",
    "getaddrinfo",
)

_SSL_PATTERNS: tuple[str, ...] = (
    "certificate_verify_failed",
    "certificate verify failed",
    "self-signed certificate",
    "self signed certificate",
    "ssl handshake",
    "sslv3 handshake",
    "tls handshake",
    "ssl: ",
    "ssl error",
    "certificate has expired",
    "hostname mismatch",
    "ca_md_too_weak",
)


def _matches_any(haystack: str, needles: tuple[str, ...]) -> bool:
    return any(needle in haystack for needle in needles)


def _looks_like_missing_database(lower: str) -> bool:
    """Return True for messages like 'database "orders" does not exist' or
    'unknown database'. We deliberately exclude messages that mention
    schema/table/column, which are handled by the generic "object not found"
    branch and usually point at the wrong schema rather than the wrong DB."""
    if "unknown database" in lower or "no such database" in lower:
        return True
    if not ("does not exist" in lower or "not found" in lower):
        return False
    if "database" not in lower and "catalog" not in lower:
        return False
    # Skip when the message is clearly about an object inside the database.
    return not any(
        token in lower for token in ("schema", "table", "column", "view", "function", "type")
    )


class ErrorMapper:
    """Map low-level driver errors to operator instructions."""

    @staticmethod
    def map(exc: Exception, *, backend: str = "") -> ActionableError | None:
        msg = str(exc)
        lower = msg.lower()
        backend_l = backend.lower()
        backend_label = backend.capitalize() if backend else "Database"

        if "pg_stat_statements" in lower:
            return ActionableError(
                "PostgreSQL extension missing",
                "AMX tried to read pg_stat_statements telemetry, but the extension is not loaded.",
                "Run `CREATE EXTENSION IF NOT EXISTS pg_stat_statements;`, add it to `shared_preload_libraries`, restart PostgreSQL, or switch profiling detail to avoid usage stats.",
            )
        if backend_l == "postgresql" and (
            "permission denied" in lower or "insufficient privilege" in lower
        ):
            return ActionableError(
                "PostgreSQL permission denied",
                "The active role cannot read one or more selected objects.",
                "Grant `SELECT` on the target schema/tables or use a profile with sufficient read privileges.",
            )
        if backend_l == "bigquery" and (
            "access denied" in lower or "forbidden" in lower or "permission" in lower
        ):
            return ActionableError(
                "BigQuery permission denied",
                "The configured principal cannot read metadata or sample table data.",
                "Grant BigQuery Metadata Viewer and Data Viewer on the project/dataset, then retry.",
            )
        if backend_l == "databricks" and (
            "certificate_verify_failed" in lower or "self-signed certificate" in lower
        ):
            return ActionableError(
                "Databricks TLS verification failed",
                "The SQL warehouse certificate could not be verified by the local trust store.",
                "Configure a trusted CA bundle in the DB profile, or only as a last resort enable TLS no-verify for that profile.",
            )
        # ODBC driver-not-found is an install issue, not a config issue —
        # flag it before the connector even fires actionable_profile_error.
        if "im002" in lower or (
            "data source name not found" in lower and "no default driver specified" in lower
        ):
            return ActionableError(
                f"{backend_label} ODBC driver missing",
                "pyodbc could not locate the ODBC driver named in the profile.",
                "Install Microsoft's ODBC Driver 18 for SQL Server (`brew install msodbcsql18` on macOS, see Microsoft docs on Linux), or set the `driver` field on the profile to a driver you do have.",
            )
        # Oracle ORA-NNNNN / ClickHouse Code: NNN — give the user the
        # error code to grep for if no specific handler matched.
        if backend_l == "oracle" and "ora-" in lower:
            return ActionableError(
                "Oracle returned an error code",
                msg.split(".")[0],
                "Look up the ORA- code in the Oracle Error Messages reference for the exact remedy.",
            )
        if backend_l == "clickhouse" and "code:" in lower:
            return ActionableError(
                "ClickHouse returned an error code",
                msg.split("(")[0],
                "Look up the Code in ClickHouse's docs/en/sql-reference/error-codes for the exact remedy.",
            )

        # Authentication — wrong credentials, expired token, bad role
        if _matches_any(lower, _AUTH_PATTERNS):
            return ActionableError(
                f"{backend_label} authentication failed",
                "The driver rejected the credentials or token before opening a session.",
                "Verify the username, password / API key / access token in the active profile via /add-db-profile or /llm. For Databricks, regenerate the personal access token if it is older than the warehouse rotation policy.",
            )

        # SSL / TLS — handle before generic "connection failed" so we can hint
        # at certificate trust rather than firewall.
        if _matches_any(lower, _SSL_PATTERNS):
            return ActionableError(
                f"{backend_label} TLS / SSL error",
                "AMX could not establish a trusted TLS session with the database.",
                "If your organisation uses an internal CA, set the trusted CA bundle in the DB profile (Databricks: tls_trusted_ca_file, others: REQUESTS_CA_BUNDLE / SSL_CERT_FILE). Avoid disabling verification in production.",
            )

        # Network — host unreachable / connection refused
        if _matches_any(lower, _NETWORK_PATTERNS):
            return ActionableError(
                f"{backend_label} network unreachable",
                "AMX could not reach the database host from this machine.",
                "Check the host / port in the DB profile, your VPN or office network, and any firewall / IP allow-list on the database side, then retry.",
            )

        # Database / catalog missing — distinct from "object not found"
        if _looks_like_missing_database(lower):
            return ActionableError(
                f"{backend_label} database not found",
                "The driver connected to the host but cannot find the configured database / catalog.",
                "Check the database name in the active profile (PostgreSQL/MySQL/SQL Server/Redshift: database, Snowflake: database, Databricks: catalog, BigQuery: project, Oracle: service_name or SID, ClickHouse: database, DuckDB: file path) and confirm the user has access to it.",
            )

        if (
            "not found" in lower
            or "does not exist" in lower
            or "not exist or not authorized" in lower
        ):
            return ActionableError(
                "Object not found or not visible",
                "The requested database object is missing or hidden by permissions.",
                "Check the active database/profile, schema, table name, and grants before retrying.",
            )
        if "timeout" in lower or "timed out" in lower:
            return ActionableError(
                "Connection timed out",
                "The database did not respond before the configured timeout.",
                "Verify network access, warehouse/server status, credentials, and retry with a reachable endpoint.",
            )
        return None


def actionable_error_message(exc: Exception, *, backend: str = "") -> str:
    mapped = ErrorMapper.map(exc, backend=backend)
    return mapped.render() if mapped else str(exc)


# ── Write-path classifier ────────────────────────────────────────────────
#
# ``ErrorMapper.map`` above covers read-path connection / auth errors
# (and a smattering of write-path cases). When a COMMENT ON / ALTER
# TABLE inside :func:`amx.agents._orchestrator.writeback.apply_review_results_to_db`
# fails, we want a sharper classification: which row failed, why, and
# what the user can do about it without reading the raw driver stack
# trace. The Studio aggregate banner reads ``WriteErrorClass.kind`` to
# decide which icon / hint to render alongside the failed row's
# qualified name.


@dataclass(frozen=True)
class WriteErrorClass:
    """Structured classification of a single live-DB write failure.

    ``kind`` is a stable slug (``alter_privilege_denied``,
    ``table_not_found``, ``comment_unsupported``,
    ``savepoint_unsupported``, ``connection_lost``, ``syntax_error``,
    ``unknown``) the SPA pivots on. ``title`` / ``body`` /
    ``suggested_action`` are the human-readable surfaces — the SPA
    banner renders title + body inline and the "Copy DBA-ready hint"
    button copies the full triple.
    """

    kind: str
    title: str
    body: str
    suggested_action: str


_PRIVILEGE_PATTERNS: tuple[str, ...] = (
    "permission denied",
    "insufficient privilege",
    "insufficient_permissions",
    "access denied",
    "accessdenied",
    "lacks permission",
    "not authorized",
    "no permission",
    "denied for user",
    # MSSQL "ALTER permission was denied on the object"
    "permission was denied",
)

_NOT_FOUND_PATTERNS: tuple[str, ...] = (
    "does not exist",
    "no such table",
    "no such column",
    "table or view does not exist",
    "object not found",
    "not found or not authorized",
    "not exist or not authorized",
    "invalid identifier",
)

_COMMENT_UNSUPPORTED_PATTERNS: tuple[str, ...] = (
    "unsupporteddatabaseoperation",
    "comment is not supported",
    "comments not supported",
    "syntax error at or near \"comment\"",
)

_CONNECTION_LOST_PATTERNS: tuple[str, ...] = (
    "server closed the connection",
    "connection lost",
    "connection closed",
    "connection reset by peer",
    "broken pipe",
)


def _qualified_label(schema: str, table: str, column: str | None) -> str:
    parts = [p for p in (schema or "", table or "", column or "") if p]
    return ".".join(parts) if parts else "(unknown asset)"


def _privilege_hint(backend: str) -> str:
    b = backend.lower()
    if b == "databricks":
        return (
            "Ask your Databricks workspace admin to grant ALTER on the "
            "table (or the parent schema for inherited ALTER)."
        )
    if b == "bigquery":
        return (
            "Grant the BigQuery Data Editor role (or the narrower "
            "bigquery.tables.update permission) on the dataset and retry."
        )
    if b == "snowflake":
        return (
            "Grant MODIFY (table-level) or USAGE + MODIFY on the schema "
            "to the active role and retry."
        )
    if b in ("postgresql", "redshift"):
        return (
            "Run `GRANT ALTER ON TABLE <table> TO <role>;` as the owner "
            "or a superuser, then retry."
        )
    if b == "mysql":
        return "Grant ALTER on the table or schema to the active user and retry."
    if b == "mssql":
        return (
            "Grant ALTER permission on the object (or its schema) to the "
            "active login and retry."
        )
    return (
        "Grant the equivalent of ALTER / MODIFY on the target object to "
        "the active connection's role, then retry."
    )


def classify_write_error(
    exc: Exception,
    *,
    backend: str = "",
    schema: str = "",
    table: str = "",
    column: str | None = None,
) -> WriteErrorClass:
    """Map a single live-DB write exception to a :class:`WriteErrorClass`.

    Always returns a value — never ``None`` — so the writeback path
    can stamp every ``RowApplyOutcome`` with a ``kind`` slug even on
    drivers we have not yet special-cased. The fallback ``kind`` is
    ``unknown`` and the body carries the raw driver text truncated to
    keep the SPA payload small.

    The classifier reads the exception text (case-insensitive) plus
    the backend label; ``schema`` / ``table`` / ``column`` are
    pre-rendered into the title so the SPA banner can be specific
    ("missing ALTER privilege on samples.nyctaxi.trips") without the
    caller building strings.
    """
    msg = str(exc)
    lower = msg.lower()
    asset = _qualified_label(schema, table, column)

    # 1) SAVEPOINT explicitly rejected (Databricks et al. — defence in
    #    depth; the writeback path already gates on capability so this
    #    should not appear in practice, but we want the classifier to
    #    say so loudly if it ever does).
    if "savepoint" in lower and (
        "syntax error" in lower
        or "not supported" in lower
        or "unsupported" in lower
    ):
        return WriteErrorClass(
            kind="savepoint_unsupported",
            title=f"{backend.capitalize() or 'Backend'} rejected SAVEPOINT",
            body=(
                f"AMX tried to wrap the write to {asset} in a SAVEPOINT, "
                "but the server has no SAVEPOINT primitive. This is a "
                "bug in the writeback path — the backend should be "
                "flagged supports_savepoints=False."
            ),
            suggested_action=(
                "Report this so the adapter's BackendCapabilities can "
                "set supports_savepoints=False; in the meantime, retry "
                "after the next AMX deploy."
            ),
        )

    # 2) Privilege denied — the screenshot-evidence case.
    if _matches_any(lower, _PRIVILEGE_PATTERNS):
        return WriteErrorClass(
            kind="alter_privilege_denied",
            title=f"Missing ALTER privilege on {asset}",
            body=(
                f"The active connection's role cannot ALTER / COMMENT "
                f"on {asset}. The write was rolled back; the description "
                "stays in the pending queue."
            ),
            suggested_action=_privilege_hint(backend),
        )

    # 3) Object not found — likely race condition (table dropped or
    #    renamed between review and apply).
    if _matches_any(lower, _NOT_FOUND_PATTERNS):
        return WriteErrorClass(
            kind="table_not_found",
            title=f"{asset} no longer exists",
            body=(
                "The driver could not find the target object. Either the "
                "table / column was dropped or renamed after the review, "
                "or the active database / catalog scope no longer matches."
            ),
            suggested_action=(
                "Re-sync the catalog (/sync) and re-review the affected "
                "row. If the target was renamed, edit the suggestion in "
                "place before re-applying."
            ),
        )

    # 4) COMMENT unsupported on this backend / capability missing.
    if _matches_any(lower, _COMMENT_UNSUPPORTED_PATTERNS):
        return WriteErrorClass(
            kind="comment_unsupported",
            title=f"{backend.capitalize() or 'Backend'} cannot persist this comment",
            body=(
                f"{backend.capitalize() or 'The backend'} either has no "
                f"COMMENT ON syntax for the target object kind, or the "
                "adapter's capability flag is off. Nothing was written."
            ),
            suggested_action=(
                "Skip this row, or store the description in AMX's "
                "internal description store (catalog_descriptions) "
                "instead of pushing it back to the live DB."
            ),
        )

    # 5) Connection lost mid-write — partial commit on some backends.
    if _matches_any(lower, _CONNECTION_LOST_PATTERNS):
        return WriteErrorClass(
            kind="connection_lost",
            title=f"Connection to {backend.capitalize() or 'backend'} dropped during write",
            body=(
                "The driver lost its session before the write committed. "
                "The row stays queued and will retry cleanly once the "
                "connection is healthy."
            ),
            suggested_action=(
                "Check warehouse / database status, network / VPN, and "
                "retry Apply pending queue. Recurring drops usually mean "
                "an idle-timeout shorter than the apply duration."
            ),
        )

    # 6) Generic SQL syntax error — usually identifier quoting.
    if "syntax error" in lower:
        return WriteErrorClass(
            kind="syntax_error",
            title=f"{backend.capitalize() or 'Backend'} rejected the write SQL",
            body=(
                f"The COMMENT / ALTER statement for {asset} failed to "
                "parse. This is almost always an identifier quoting issue "
                "in the adapter's COMMENT template."
            ),
            suggested_action=(
                "Report the failing object name (special characters, "
                "reserved words) so the adapter quoting can be fixed."
            ),
        )

    # 7) Unknown / fallback — keep the raw text, truncated.
    return WriteErrorClass(
        kind="unknown",
        title=f"Write to {asset} failed",
        body=msg[:500],
        suggested_action=(
            "Inspect the driver message above and the run's activity "
            "log; re-apply once the underlying issue is resolved."
        ),
    )
