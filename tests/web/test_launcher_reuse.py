"""Discovery-file reuse in ``launch_studio`` (single shared Studio).

A second ``/studio`` — from another REPL or alongside an IDE-owned
server — must reuse the already-running instance instead of spawning
a duplicate: probe the discovery record, health-check it with its
bearer token, and on success just print/open the recorded URL. Every
non-usable record state (missing, malformed, dead server, wrong
token semantics) must fall through to a normal spawn, and an
explicit ``--port`` override must bypass the reuse probe entirely.
"""

from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from unittest.mock import MagicMock, patch

import pytest

from amx.config import AMXConfig
from amx.web import discovery, launcher


@pytest.fixture(autouse=True)
def isolated_config_dir(tmp_path, monkeypatch):
    """Point AMX_CONFIG_DIR at a temp dir so tests never touch ~/.amx."""
    monkeypatch.setenv("AMX_CONFIG_DIR", str(tmp_path))
    return tmp_path


@pytest.fixture
def cfg(tmp_path):
    config = AMXConfig()
    object.__setattr__(config, "_config_path", str(tmp_path / "config.yml"))
    object.__setattr__(config, "CONFIG_DIR", str(tmp_path))
    return config


class _HealthHandler(BaseHTTPRequestHandler):
    """Minimal stand-in for a live Studio: 200 on a token-authed
    ``/api/health``, 401 otherwise — mirroring TokenAuthMiddleware."""

    expected_token = ""

    def do_GET(self) -> None:  # noqa: N802 - http.server API
        authed = self.headers.get("Authorization") == f"Bearer {self.expected_token}"
        if self.path == "/api/health" and authed:
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status": "ok"}')
        else:
            self.send_response(401)
            self.end_headers()

    def log_message(self, *args) -> None:  # silence test output
        del args


@pytest.fixture
def fake_studio():
    """A live loopback HTTP server registered in the discovery file."""
    server = HTTPServer(("127.0.0.1", 0), _HealthHandler)
    port = server.server_address[1]
    _HealthHandler.expected_token = "tok-live"
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    discovery.write_discovery(port, "tok-live", owner="cli")
    try:
        yield port
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_reuses_healthy_running_studio_without_spawning(cfg, fake_studio) -> None:
    with patch.object(launcher.subprocess, "Popen") as fake_popen:
        with patch.object(launcher, "webbrowser") as fake_browser:
            ok = launcher.launch_studio(cfg, open_browser=True)
    assert ok is True
    fake_popen.assert_not_called()
    opened_url = fake_browser.open_new_tab.call_args.args[0]
    assert opened_url == f"http://127.0.0.1:{fake_studio}/?t=tok-live"


def test_reuse_respects_open_browser_false(cfg, fake_studio) -> None:
    with patch.object(launcher.subprocess, "Popen") as fake_popen:
        with patch.object(launcher, "webbrowser") as fake_browser:
            ok = launcher.launch_studio(cfg, open_browser=False)
    assert ok is True
    fake_popen.assert_not_called()
    fake_browser.open_new_tab.assert_not_called()


def test_dead_record_falls_through_to_spawn(cfg) -> None:
    """A record pointing at a closed port must not block the launch."""
    closed_port = launcher._pick_port(0)  # allocated then released → closed
    discovery.write_discovery(closed_port, "tok-dead", owner="cli")

    fake_proc = MagicMock()
    fake_proc.wait.return_value = 0
    with patch.object(launcher.subprocess, "Popen", return_value=fake_proc) as fake_popen:
        with (
            patch.object(launcher, "_wait_for_http", return_value=True),
            patch.object(launcher, "webbrowser"),
        ):
            ok = launcher.launch_studio(cfg, open_browser=False)
    assert ok is True
    fake_popen.assert_called_once()


def test_missing_record_spawns_normally(cfg) -> None:
    fake_proc = MagicMock()
    fake_proc.wait.return_value = 0
    with patch.object(launcher.subprocess, "Popen", return_value=fake_proc) as fake_popen:
        with (
            patch.object(launcher, "_wait_for_http", return_value=True),
            patch.object(launcher, "webbrowser"),
        ):
            ok = launcher.launch_studio(cfg, open_browser=False)
    assert ok is True
    fake_popen.assert_called_once()


def test_malformed_record_spawns_normally(cfg) -> None:
    discovery.discovery_path().parent.mkdir(parents=True, exist_ok=True)
    discovery.discovery_path().write_text("{not json", encoding="utf-8")

    fake_proc = MagicMock()
    fake_proc.wait.return_value = 0
    with patch.object(launcher.subprocess, "Popen", return_value=fake_proc) as fake_popen:
        with (
            patch.object(launcher, "_wait_for_http", return_value=True),
            patch.object(launcher, "webbrowser"),
        ):
            ok = launcher.launch_studio(cfg, open_browser=False)
    assert ok is True
    fake_popen.assert_called_once()


def test_explicit_port_bypasses_reuse_probe(cfg, fake_studio) -> None:
    """Tests and power users pin a port deliberately — a running
    server elsewhere must not hijack that request."""
    fake_proc = MagicMock()
    fake_proc.wait.return_value = 0
    with patch.object(launcher.subprocess, "Popen", return_value=fake_proc) as fake_popen:
        with (
            patch.object(launcher, "_wait_for_http", return_value=True),
            patch.object(launcher, "webbrowser"),
        ):
            ok = launcher.launch_studio(cfg, port=fake_studio + 1, open_browser=False)
    assert ok is True
    fake_popen.assert_called_once()


def test_probe_rejects_record_with_stale_token(cfg, fake_studio) -> None:
    """A record whose token the server no longer accepts (401) is not
    reusable — fall through to spawn."""
    discovery.write_discovery(fake_studio, "tok-stale", owner="cli")

    fake_proc = MagicMock()
    fake_proc.wait.return_value = 0
    with patch.object(launcher.subprocess, "Popen", return_value=fake_proc) as fake_popen:
        with (
            patch.object(launcher, "_wait_for_http", return_value=True),
            patch.object(launcher, "webbrowser"),
        ):
            ok = launcher.launch_studio(cfg, open_browser=False)
    assert ok is True
    fake_popen.assert_called_once()
