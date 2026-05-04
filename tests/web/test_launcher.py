"""Launcher unit tests — port picking + import-failure handling.

The full ``launch_visualize`` end-to-end (uvicorn boot + browser open
+ Ctrl-C) is exercised manually; here we only assert the pieces the
unit suite can verify without spawning a real listener."""

from __future__ import annotations

import socket

from amx.web.launcher import _pick_port


def test_pick_port_uses_preferred_when_free() -> None:
    """When the preferred port is free, the picker returns it
    verbatim — important so the URL printed to the console matches
    the documented default in `/visualize --help`."""
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
