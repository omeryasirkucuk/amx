"""Structured logging for AMX.

The file handler at ``~/.amx/logs/amx.log`` now writes one JSON object
per line so log shippers and ``jq`` workflows do not need an AMX-
specific parser. The stderr handler keeps the historical human-readable
format so users see the same warnings and errors they always have.

A :class:`contextvars.ContextVar` carries a per-request id through the
call stack — set it once at the entry point of a CLI command (e.g.
``/analyze run``) and every log line emitted between then and the
matching :func:`clear_request_id` call will carry the same id under the
``request_id`` JSON field, making it trivial to filter the log to one
session/run/question.
"""

from __future__ import annotations

import json
import logging
import sys
import uuid
from contextvars import ContextVar
from pathlib import Path
from typing import Any

LOG_DIR = Path.home() / ".amx" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Written when the profile agent cannot parse an LLM reply (debugging).
LAST_PROFILE_RESPONSE_FILE = LOG_DIR / "last_profile_agent_response.txt"

# ── Request-id context propagation ────────────────────────────────────

_request_id_var: ContextVar[str | None] = ContextVar(
    "amx_request_id", default=None
)


def set_request_id(request_id: str | None = None) -> str:
    """Set the current request id and return it.

    Pass an explicit id to thread through child processes / async tasks,
    or let the helper generate a 12-char random hex when called with no
    argument.
    """
    rid = request_id or uuid.uuid4().hex[:12]
    _request_id_var.set(rid)
    return rid


def clear_request_id() -> None:
    """Drop the current request id (call at the end of a CLI command)."""
    _request_id_var.set(None)


def get_request_id() -> str | None:
    return _request_id_var.get()


class _RequestIdFilter(logging.Filter):
    """Inject the current request id into every log record under the
    ``request_id`` attribute so both the JSON formatter (file) and the
    human formatter (stderr) can include it without coupling to the
    context variable directly.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = _request_id_var.get() or "-"
        return True


# ── Formatters ───────────────────────────────────────────────────────


class JsonFormatter(logging.Formatter):
    """JSON-Lines formatter for the on-disk log.

    One log record → one JSON object on its own line. Includes the
    timestamp (ISO 8601 with offset), level, logger name, request id,
    and the rendered message. Stack traces from ``log.exception`` land
    in an ``exc_info`` field as a single string so the line stays one
    JSON object.
    """

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "request_id": getattr(record, "request_id", "-"),
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


_human_fmt = logging.Formatter(
    "%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(f"amx.{name}")
    if not logger.handlers:
        logger.setLevel(logging.DEBUG)
        logger.addFilter(_RequestIdFilter())
        fh = logging.FileHandler(LOG_DIR / "amx.log")
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(JsonFormatter())
        logger.addHandler(fh)
        sh = logging.StreamHandler(sys.stderr)
        sh.setLevel(logging.WARNING)
        sh.setFormatter(_human_fmt)
        logger.addHandler(sh)
    return logger
