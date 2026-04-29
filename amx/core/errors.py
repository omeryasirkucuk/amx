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


class ErrorMapper:
    """Map low-level driver errors to operator instructions."""

    @staticmethod
    def map(exc: Exception, *, backend: str = "") -> ActionableError | None:
        msg = str(exc)
        lower = msg.lower()
        backend_l = backend.lower()

        if "pg_stat_statements" in lower:
            return ActionableError(
                "PostgreSQL extension missing",
                "AMX tried to read pg_stat_statements telemetry, but the extension is not loaded.",
                "Run `CREATE EXTENSION IF NOT EXISTS pg_stat_statements;`, add it to `shared_preload_libraries`, restart PostgreSQL, or switch profiling detail to avoid usage stats.",
            )
        if backend_l == "postgresql" and ("permission denied" in lower or "insufficient privilege" in lower):
            return ActionableError(
                "PostgreSQL permission denied",
                "The active role cannot read one or more selected objects.",
                "Grant `SELECT` on the target schema/tables or use a profile with sufficient read privileges.",
            )
        if backend_l == "bigquery" and ("access denied" in lower or "forbidden" in lower or "permission" in lower):
            return ActionableError(
                "BigQuery permission denied",
                "The configured principal cannot read metadata or sample table data.",
                "Grant BigQuery Metadata Viewer and Data Viewer on the project/dataset, then retry.",
            )
        if backend_l == "databricks" and ("certificate_verify_failed" in lower or "self-signed certificate" in lower):
            return ActionableError(
                "Databricks TLS verification failed",
                "The SQL warehouse certificate could not be verified by the local trust store.",
                "Configure a trusted CA bundle in the DB profile, or only as a last resort enable TLS no-verify for that profile.",
            )
        if "not found" in lower or "does not exist" in lower or "not exist or not authorized" in lower:
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
