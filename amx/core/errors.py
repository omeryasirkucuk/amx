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
