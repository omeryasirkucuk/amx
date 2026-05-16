"""DB error summarization for ``amx metadata`` CLI commands.

Extracted from :mod:`amx.cli_support.commands.manual`. The two
helpers convert raw DB driver exceptions into a short user-friendly
sentence plus an actionable error line — the manual edit wizards
catch every DB write failure and pipe them through here so the user
sees ``Permission denied (HTTP 403)`` instead of an SQL stack trace.

``manual.py`` re-exports both names so any caller importing the
underscore form keeps working unchanged.
"""

from __future__ import annotations

from amx.utils.console import error, warn


def _summarize_db_exception(exc: Exception) -> str:
    detail = str(exc).strip()
    lower = detail.lower()
    if "connection refused" in lower:
        return "Database connection refused."
    if "timeout" in lower or "timed out" in lower:
        return "Database connection timed out."
    if "authentication" in lower or "password" in lower or "permission denied" in lower:
        return "Database authentication failed."
    first_line = next((line.strip() for line in detail.splitlines() if line.strip()), "")
    return first_line[:220]


def _report_manual_db_error(action: str, exc: Exception) -> None:
    error(f"Could not {action} because AMX cannot reach the active database.")
    warn("Check the active DB profile and run /db then /connect.")
    summary = _summarize_db_exception(exc)
    if summary:
        warn(f"Cause: {summary}")
