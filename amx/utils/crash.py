"""Crash report generation with secret redaction.

When AMX hits an unhandled exception, the top-level handler in
``amx.cli.run_cli`` writes a sanitized crash report to
``~/.amx/logs/crashes/<timestamp>-<request_id>.txt`` and prints the
path so the user can attach it to a GitHub issue without leaking
their DB password or API key.

The redactor is best-effort: it catches the well-known shapes
(provider-prefixed API keys, Databricks PATs, Bearer tokens, common
key=value patterns) but cannot guarantee zero leakage of arbitrary
custom secrets. Users should still skim the report before sharing.
"""

from __future__ import annotations

import os
import re
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path

from amx.utils.logging import LOG_DIR

CRASH_DIR = LOG_DIR / "crashes"
_REDACTED_PLACEHOLDER = "<redacted>"


# ── Redaction patterns ────────────────────────────────────────────────
#
# Order matters: more specific provider patterns run first so we keep
# enough of the prefix in the placeholder for the user to recognise
# which provider's key was leaked. Generic fall-throughs come last.

_PROVIDER_KEY_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    # Order matters: most specific patterns first so the labelled
    # placeholder reflects the actual provider when keys share a
    # prefix (sk-ant- and sk-or- both start with sk-).
    (re.compile(r"\bsk-or-[A-Za-z0-9_\-]{20,}\b"), "sk-or-<redacted>"),
    (re.compile(r"\bsk-ant-[A-Za-z0-9_\-]{20,}\b"), "sk-ant-<redacted>"),
    # Generic OpenAI (sk-…) catches everything else with that prefix.
    (re.compile(r"\bsk-[A-Za-z0-9_\-]{20,}\b"), "sk-<redacted>"),
    # Databricks personal access tokens.
    (re.compile(r"\bdapi[a-z0-9_\-]{16,}\b"), "dapi<redacted>"),
    # AWS access keys (used by Bedrock customers).
    (re.compile(r"\bAKIA[0-9A-Z]{16,}\b"), "AKIA<redacted>"),
    # GitHub fine-grained / classic.
    (re.compile(r"\bghp_[A-Za-z0-9]{30,}\b"), "ghp_<redacted>"),
    (re.compile(r"\bgithub_pat_[A-Za-z0-9_]{30,}\b"), "github_pat_<redacted>"),
)


_KV_PATTERNS: tuple[re.Pattern[str], ...] = (
    # password=..., password: ..., password "..."
    re.compile(r"(?i)(password\s*[:=]\s*)['\"]?([^'\"\s,;}]+)['\"]?"),
    # api_key=..., api-key=..., apiKey=...
    re.compile(r"(?i)(api[_-]?key\s*[:=]\s*)['\"]?([^'\"\s,;}]+)['\"]?"),
    # access_token=..., token=...
    re.compile(r"(?i)(access[_-]?token\s*[:=]\s*)['\"]?([^'\"\s,;}]+)['\"]?"),
    re.compile(r"(?i)(\btoken\s*[:=]\s*)['\"]?([A-Za-z0-9_\-]{16,})['\"]?"),
    # secret=...
    re.compile(r"(?i)(\bsecret\s*[:=]\s*)['\"]?([^'\"\s,;}]+)['\"]?"),
)

# Bearer / Basic auth tokens in HTTP headers.
_BEARER_PATTERN = re.compile(r"(?i)\b(Bearer|Basic)\s+([A-Za-z0-9_\-./=+]{8,})")


def redact_secrets(text: str) -> str:
    """Remove the common secret-shaped substrings from *text*.

    Returns a copy with each match replaced by a labelled placeholder.
    Best-effort: regex coverage is broad but not exhaustive; users
    should still read the redacted text before sharing.
    """
    if not text:
        return text

    redacted = text
    for pattern, placeholder in _PROVIDER_KEY_PATTERNS:
        redacted = pattern.sub(placeholder, redacted)

    for pattern in _KV_PATTERNS:
        redacted = pattern.sub(
            lambda match: f"{match.group(1)}{_REDACTED_PLACEHOLDER}",
            redacted,
        )

    redacted = _BEARER_PATTERN.sub(
        lambda match: f"{match.group(1)} {_REDACTED_PLACEHOLDER}",
        redacted,
    )

    return redacted


def _format_environment() -> str:
    """Capture the most useful environment context for triage without
    leaking the full ``os.environ``. AMX-prefixed env vars are often
    relevant; the rest could carry tokens (CI providers, IDE settings)
    that are not worth the leak risk."""
    interesting: dict[str, str] = {}
    for key in sorted(os.environ):
        if key.startswith("AMX_") or key in {"PYTHONPATH", "PATH"}:
            interesting[key] = os.environ[key]
    if not interesting:
        return "(no AMX_* env vars set)"
    return "\n".join(f"  {k}={v!r}" for k, v in interesting.items())


def write_crash_report(
    exc: BaseException,
    *,
    request_id: str | None = None,
    extra_context: str | None = None,
) -> Path:
    """Write a sanitized crash report and return its path.

    The report contains: timestamp, request id (if set), exception
    class + message, full traceback, AMX-prefixed env vars, and any
    caller-supplied extra context. Every component runs through
    :func:`redact_secrets` before being written to disk.
    """
    CRASH_DIR.mkdir(parents=True, exist_ok=True)

    rid = request_id or uuid.uuid4().hex[:12]
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = CRASH_DIR / f"{stamp}-{rid}.txt"

    tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    parts: list[str] = [
        "AMX crash report",
        f"timestamp: {stamp}",
        f"request_id: {rid}",
        f"exception: {exc.__class__.__name__}: {exc}",
        "",
        "Traceback:",
        tb.rstrip(),
        "",
        "Environment (AMX_* vars only):",
        _format_environment(),
    ]
    if extra_context:
        parts.extend(["", "Extra context:", extra_context])

    payload = "\n".join(parts) + "\n"
    redacted = redact_secrets(payload)
    path.write_text(redacted, encoding="utf-8")
    try:
        os.chmod(path, 0o600)
    except OSError:
        # Best-effort on Windows / non-POSIX filesystems.
        pass
    return path
