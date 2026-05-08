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
import logging.handlers
import os
import sys
import uuid
from contextvars import ContextVar
from pathlib import Path
from typing import Any

LOG_DIR = Path.home() / ".amx" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# ── Log rotation ─────────────────────────────────────────────────────
#
# Long-running Studio sessions used to grow ``~/.amx/logs/amx.log``
# without bound. The rotating handler caps the active file at
# ``LOG_MAX_BYTES`` and keeps ``LOG_BACKUP_COUNT`` archives alongside
# it, so the on-disk footprint is bounded at roughly
# ``LOG_MAX_BYTES * (1 + LOG_BACKUP_COUNT)``.
#
# Defaults are conservative — 10 MB × 5 archives ≈ 60 MB ceiling — and
# overridable per-environment via ``AMX_LOG_MAX_BYTES`` and
# ``AMX_LOG_BACKUP_COUNT`` for users who want more retained history or
# tighter caps. Rotation is on-write so the active file is always
# ``amx.log``; archives become ``amx.log.1`` … ``amx.log.N``.

_DEFAULT_LOG_MAX_BYTES = 10 * 1024 * 1024  # 10 MB
_DEFAULT_LOG_BACKUP_COUNT = 5


def _int_env(name: str, default: int, *, minimum: int = 0) -> int:
    """Read a non-negative int from the environment, fall back on parse errors."""
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    return max(minimum, value)


LOG_MAX_BYTES = _int_env("AMX_LOG_MAX_BYTES", _DEFAULT_LOG_MAX_BYTES, minimum=1024)
LOG_BACKUP_COUNT = _int_env("AMX_LOG_BACKUP_COUNT", _DEFAULT_LOG_BACKUP_COUNT, minimum=0)

# Written when the profile agent cannot parse an LLM reply (debugging).
LAST_PROFILE_RESPONSE_FILE = LOG_DIR / "last_profile_agent_response.txt"

# ── Request-id context propagation ────────────────────────────────────

_request_id_var: ContextVar[str | None] = ContextVar("amx_request_id", default=None)


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


class _EncodingSafeStreamHandler(logging.StreamHandler):
    """StreamHandler that never crashes on a non-encodable glyph.

    Windows consoles default to cp1252, which can't encode common log
    characters: the human formatter contains an em-dash (—), and call-
    sites use arrows (→) and ellipses (…) liberally. The default
    ``StreamHandler.emit`` re-raises ``UnicodeEncodeError`` straight up
    the call stack, which on Windows surfaces as a "Logging error"
    block plus a multi-frame traceback printed in the middle of the
    interactive REPL — once per affected log call.

    Catch the encode error, re-render the message with the stream's
    own codec under ``errors='replace'``, and write that. The user
    sees a ``?`` instead of ``→`` on cp1252; on every other codec the
    fast path runs and nothing changes.
    """

    def emit(self, record: logging.LogRecord) -> None:
        try:
            super().emit(record)
        except UnicodeEncodeError:
            try:
                msg = self.format(record)
                stream = self.stream
                enc = getattr(stream, "encoding", None) or "ascii"
                stream.write(msg.encode(enc, errors="replace").decode(enc) + self.terminator)
                self.flush()
            except Exception:
                self.handleError(record)


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(f"amx.{name}")
    if not logger.handlers:
        logger.setLevel(logging.DEBUG)
        logger.addFilter(_RequestIdFilter())
        # Don't propagate to the root logger. Several third-party
        # imports (``transformers``, ``litellm``, ``bert_score``,
        # ``uvicorn[standard]``) call ``logging.basicConfig`` at
        # import time, which attaches a default stream handler to
        # root at ``INFO`` (sometimes ``DEBUG``). Without
        # ``propagate=False`` every amx.* INFO / DEBUG record fans
        # out to that root handler too — the user's REPL terminal
        # gets flooded with ``INFO:amx.db.connector:Connected via
        # postgresql ...`` and ``DEBUG:amx.llm.provider:LLM call
        # ...`` lines while AMX Studio is up. Those records still
        # land on disk via the file handler below; the on-screen
        # leak is what we're closing here.
        logger.propagate = False
        # Pin the on-disk log to UTF-8 explicitly. Without this, Python
        # on Windows opens the file with the platform default codec
        # (cp1252), and any log message containing →, —, … — including
        # ones produced by the human formatter itself — raises
        # UnicodeEncodeError on emit.
        #
        # ``RotatingFileHandler`` caps the active log at
        # ``LOG_MAX_BYTES`` and keeps ``LOG_BACKUP_COUNT`` archives.
        # Set ``LOG_BACKUP_COUNT=0`` to keep one bounded file with no
        # rollovers (matches the legacy single-file behaviour while
        # still capping disk usage).
        fh = logging.handlers.RotatingFileHandler(
            LOG_DIR / "amx.log",
            maxBytes=LOG_MAX_BYTES,
            backupCount=LOG_BACKUP_COUNT,
            encoding="utf-8",
        )
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(JsonFormatter())
        logger.addHandler(fh)
        sh = _EncodingSafeStreamHandler(sys.stderr)
        sh.setLevel(logging.WARNING)
        sh.setFormatter(_human_fmt)
        logger.addHandler(sh)
    return logger


# ── Structured events ────────────────────────────────────────────────


def log_event(event_type: str, /, **fields: Any) -> None:
    """Write a structured event line to the rotating JSON log.

    Each call emits one ``INFO``-level record on the ``amx.events``
    logger. ``JsonFormatter`` already serialises ``ts``, ``level``,
    ``logger``, ``request_id``, and the rendered message; the message
    itself is the JSON-encoded ``{event, ...fields}`` payload so log
    shippers can pivot on the event type without re-parsing the text.

    This helper is intentionally thin and free of subscribers — it only
    writes to the rotating file. Real-time fan-out for AMX Studio
    continues to flow through :mod:`amx.web.progress_bus` (SSE).

    Examples
    --------
    >>> log_event("run.start", profile="prod_pg", scope="public.transactions",
    ...           backend="postgresql", model="claude-opus-4-7")
    >>> log_event("run.cancellation.requested", run_id="abcd1234")
    >>> log_event("run.agent.failed", run_id="abcd1234", agent="rag",
    ...           reason="timeout")

    Parameters
    ----------
    event_type:
        Dot-separated event name (e.g. ``run.start``,
        ``run.agent.failed``, ``run.cancellation.completed``).
    fields:
        JSON-serialisable key/value pairs describing the event.
    """
    logger = get_logger("events")
    payload: dict[str, Any] = {"event": event_type}
    if fields:
        payload.update(fields)
    logger.info(json.dumps(payload, ensure_ascii=False, default=str))
