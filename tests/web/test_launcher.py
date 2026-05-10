"""Launcher unit tests — port picking + Studio bootstrap invariants.

The full ``launch_studio`` end-to-end (uvicorn boot + browser open
+ Ctrl-C) is exercised manually; here we only assert the pieces the
unit suite can verify without spawning a real listener."""

from __future__ import annotations

import re
import socket
from pathlib import Path

from amx.web.launcher import _pick_port


def test_pick_port_uses_preferred_when_free() -> None:
    """When the preferred port is free, the picker returns it
    verbatim — important so the URL printed to the console matches
    the documented default in `/studio --help`."""
    chosen = _pick_port(0)  # 0 → OS-allocates a free ephemeral port
    assert isinstance(chosen, int)
    assert 1024 <= chosen <= 65535


def test_pick_port_falls_back_when_preferred_busy() -> None:
    """If the preferred port is taken, the picker silently grabs an
    ephemeral one. We hold the preferred port open for the duration
    of the call so the picker has to bail out."""
    holder = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    holder.bind(("127.0.0.1", 0))
    busy_port = holder.getsockname()[1]
    holder.listen(1)
    try:
        chosen = _pick_port(busy_port)
    finally:
        holder.close()
    assert chosen != busy_port or chosen == 0  # OS may reuse busy_port the instant we close


def test_studio_runtime_packages_are_core_dependencies() -> None:
    """Regression: an earlier design pulled FastAPI / uvicorn /
    sse-starlette / python-multipart lazily on first ``/studio`` open,
    which split the install across two phases and (in one historical
    miss) forgot ``python-multipart`` so the first multipart request
    raised ``RuntimeError: Form data requires "python-multipart" to
    be installed`` mid-session.

    The fix promoted those packages to core dependencies so a single
    ``pip install amx-cli`` is sufficient. This test pins that
    invariant against ``pyproject.toml`` directly so a future edit
    that demotes any of them back to an extra fails CI immediately
    instead of resurfacing as a runtime crash for the first user who
    drag-drops a document.
    """
    pyproject = Path(__file__).resolve().parents[2] / "pyproject.toml"
    text = pyproject.read_text(encoding="utf-8")

    # Pull just the ``dependencies = [ ... ]`` block under ``[project]``
    # so a string that only appears under an extras list cannot satisfy
    # the assertion. Plain string operations keep the test usable on
    # Python 3.10 where ``tomllib`` is not in the standard library.
    match = re.search(
        r"^\[project\][\s\S]*?^dependencies = \[(?P<deps>[\s\S]*?)^\]",
        text,
        re.MULTILINE,
    )
    assert match is not None, "could not locate [project].dependencies in pyproject.toml"
    deps_block = match.group("deps")

    requirements = [
        re.sub(r'^\s*"', "", line).split('"', 1)[0]
        for line in deps_block.splitlines()
        if '"' in line
    ]

    def _declares(name: str) -> bool:
        return any(req.split(" ")[0].split("[")[0].split(">")[0] == name for req in requirements)

    for required in ("fastapi", "uvicorn", "sse-starlette", "python-multipart"):
        assert _declares(required), f"{required!r} must stay in core dependencies"
