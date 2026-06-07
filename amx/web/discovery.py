"""Discovery file for a running AMX Studio server.

Whoever starts a Studio server — the REPL's ``/studio`` launcher or
an external host such as an IDE integration — records the connection
details in ``<config-dir>/studio.json`` so other local AMX tooling
can find and reuse the running instance instead of spawning a second
one. The file is removed when the server shuts down; a stale file
(crash, SIGKILL) is harmless because consumers must health-check the
recorded endpoint before trusting it.

The file contains the bearer token, so it is written with owner-only
permissions (best effort on platforms where POSIX modes don't apply)
and must never be served, logged, or committed anywhere.
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from amx import __version__ as AMX_VERSION
from amx.config import _resolve_config_dir

_DISCOVERY_FILENAME = "studio.json"


@dataclass(frozen=True)
class StudioDiscovery:
    """Connection details for a running Studio server."""

    port: int
    token: str
    pid: int
    started_at: str
    owner: str
    version: str


def discovery_path() -> Path:
    """Return the discovery file path inside the active config dir."""
    return Path(_resolve_config_dir()) / _DISCOVERY_FILENAME


def write_discovery(port: int, token: str, *, owner: str = "cli") -> StudioDiscovery:
    """Atomically record a running server in the discovery file.

    The write goes through a temp file + ``os.replace`` so a reader
    never observes a partially written JSON document. Failures are
    swallowed by callers — discovery is an optimisation, never a
    launch blocker.
    """
    record = StudioDiscovery(
        port=port,
        token=token,
        pid=os.getpid(),
        started_at=datetime.now(timezone.utc).isoformat(),
        owner=owner,
        version=AMX_VERSION,
    )
    path = discovery_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{_DISCOVERY_FILENAME}.", dir=str(path.parent)
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(asdict(record), handle, indent=2)
        try:
            os.chmod(tmp_name, 0o600)
        except OSError:  # pragma: no cover - non-POSIX permission models
            pass
        os.replace(tmp_name, path)
    except OSError:
        # Clean the temp file up on any failure so the config dir
        # doesn't accumulate orphans; re-raise for the caller's
        # best-effort guard to log/ignore.
        Path(tmp_name).unlink(missing_ok=True)
        raise
    return record


def read_discovery() -> StudioDiscovery | None:
    """Return the recorded server details, or ``None``.

    ``None`` covers every non-usable state — missing file, unreadable
    file, malformed JSON, or a document missing required fields. The
    caller must still health-check the endpoint; a well-formed record
    can describe a server that has since died.
    """
    path = discovery_path()
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(raw, dict):
        return None
    try:
        return StudioDiscovery(
            port=int(raw["port"]),
            token=str(raw["token"]),
            pid=int(raw["pid"]),
            started_at=str(raw.get("started_at", "")),
            owner=str(raw.get("owner", "")),
            version=str(raw.get("version", "")),
        )
    except (KeyError, TypeError, ValueError):
        return None


def clear_discovery(*, pid: int | None = None) -> None:
    """Remove the discovery file.

    When ``pid`` is given, the file is only removed if it still
    belongs to that process — this keeps a dying server from deleting
    the record of a *newer* server that already replaced the file.
    """
    path = discovery_path()
    if pid is not None:
        current = read_discovery()
        if current is not None and current.pid != pid:
            return
    try:
        path.unlink(missing_ok=True)
    except OSError:  # pragma: no cover - delete is best-effort
        pass
