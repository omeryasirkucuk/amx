"""Regression: ``/studio`` must run uvicorn in a child PROCESS that
is fully isolated from the parent CLI's terminal state.

Three layers verified here (one each per defect that earlier PRs
exposed and failed to fully fix):

1. ``start_new_session=True`` so the child has its own session and
   process group, and the parent's foreground Ctrl-C does not reach
   the child implicitly — we forward explicitly via ``os.killpg``.
2. ``stdin=DEVNULL`` + ``stdout=<log_fd>`` + ``stderr=STDOUT`` so the
   child cannot write to the parent's tty regardless of what
   uvicorn or anyio raises on shutdown.
3. The Ctrl-C path forwards ``SIGINT`` via ``os.killpg`` (covers any
   uvicorn workers that may exist in the future) and escalates to
   ``SIGTERM`` then ``SIGKILL`` on timeout.

Without these, the user-visible symptom is arrow keys echoing
literal ``^[[C`` after ``/studio`` + Ctrl-C until the CLI restarts.
"""

from __future__ import annotations

import signal
import subprocess
import sys
from unittest.mock import MagicMock, patch


def test_launch_studio_spawns_subprocess_with_python_module(tmp_path, monkeypatch):
    from amx.config import AMXConfig
    from amx.web import launcher

    cfg = AMXConfig()
    object.__setattr__(cfg, "_config_path", str(tmp_path / "config.yml"))
    object.__setattr__(cfg, "CONFIG_DIR", str(tmp_path))

    fake_proc = MagicMock()
    fake_proc.wait.return_value = 0
    with patch.object(launcher.subprocess, "Popen", return_value=fake_proc) as fake_popen:
        with (
            patch.object(launcher, "_wait_for_http", return_value=True),
            patch.object(launcher, "webbrowser"),
        ):
            ok = launcher.launch_studio(cfg, port=47821, open_browser=False)
    assert ok is True
    cmd = fake_popen.call_args.args[0]
    assert cmd[0] == sys.executable
    assert cmd[1:4] == ["-m", "amx.web._studio_subprocess", "--port"]
    assert "47821" in cmd
    assert "--token" in cmd
    assert "--config-path" in cmd


def test_popen_kwargs_isolate_child_from_parent_terminal(tmp_path):
    """Triple-layer isolation: separate session, DEVNULL stdin, log
    file stdout, stderr merged to stdout. Without this combination the
    child's tracebacks corrupt the parent's prompt_toolkit rendering."""
    from amx.config import AMXConfig
    from amx.web import launcher

    cfg = AMXConfig()
    object.__setattr__(cfg, "_config_path", str(tmp_path / "config.yml"))
    object.__setattr__(cfg, "CONFIG_DIR", str(tmp_path))

    fake_proc = MagicMock()
    fake_proc.wait.return_value = 0
    with patch.object(launcher.subprocess, "Popen", return_value=fake_proc) as fake_popen:
        with (
            patch.object(launcher, "_wait_for_http", return_value=True),
            patch.object(launcher, "webbrowser"),
        ):
            launcher.launch_studio(cfg, port=47823, open_browser=False)
    kwargs = fake_popen.call_args.kwargs
    assert kwargs["start_new_session"] is True
    assert kwargs["stdin"] is subprocess.DEVNULL
    assert kwargs["stderr"] is subprocess.STDOUT
    assert kwargs["close_fds"] is True
    # stdout must be a writable file handle, not the parent's stdout.
    out = kwargs["stdout"]
    assert hasattr(out, "write")
    assert out is not sys.stdout


def test_log_file_is_created_under_config_dir(tmp_path):
    from amx.config import AMXConfig
    from amx.web import launcher

    cfg = AMXConfig()
    object.__setattr__(cfg, "_config_path", str(tmp_path / "config.yml"))
    object.__setattr__(cfg, "CONFIG_DIR", str(tmp_path))

    fake_proc = MagicMock()
    fake_proc.wait.return_value = 0
    with patch.object(launcher.subprocess, "Popen", return_value=fake_proc):
        with (
            patch.object(launcher, "_wait_for_http", return_value=True),
            patch.object(launcher, "webbrowser"),
        ):
            launcher.launch_studio(cfg, port=47824, open_browser=False)
    expected = tmp_path / "logs" / "studio-47824.log"
    assert expected.exists()
    contents = expected.read_text()
    assert "studio launch" in contents


def test_sigint_uses_killpg_not_proc_send_signal(tmp_path):
    """Because the child is a session leader (``start_new_session=True``),
    ``proc.pid`` is also the pgid. We use ``os.killpg`` so any worker
    processes the child may spawn (uvicorn ``--reload`` etc.) are
    signalled too — and so the parent doesn't accidentally rely on
    the now-broken shared-pgrp implicit Ctrl-C delivery."""
    from amx.config import AMXConfig
    from amx.web import launcher

    cfg = AMXConfig()
    object.__setattr__(cfg, "_config_path", str(tmp_path / "config.yml"))
    object.__setattr__(cfg, "CONFIG_DIR", str(tmp_path))

    fake_proc = MagicMock()
    fake_proc.pid = 12345
    fake_proc.wait.side_effect = [KeyboardInterrupt, 0]

    with patch.object(launcher.subprocess, "Popen", return_value=fake_proc):
        with (
            patch.object(launcher, "_wait_for_http", return_value=True),
            patch.object(launcher, "webbrowser"),
            patch.object(launcher.os, "killpg") as fake_killpg,
        ):
            launcher.launch_studio(cfg, port=47825, open_browser=False)
    assert fake_killpg.called
    pid, sig = fake_killpg.call_args.args
    assert pid == 12345
    assert sig == signal.SIGINT


def test_sigterm_escalation_on_shutdown_timeout(tmp_path):
    """When the child ignores SIGINT we escalate to SIGTERM, then
    SIGKILL — each within its grace period — so a hung uvicorn never
    leaves the parent blocked indefinitely after Ctrl-C."""
    from amx.config import AMXConfig
    from amx.web import launcher

    cfg = AMXConfig()
    object.__setattr__(cfg, "_config_path", str(tmp_path / "config.yml"))
    object.__setattr__(cfg, "CONFIG_DIR", str(tmp_path))

    fake_proc = MagicMock()
    fake_proc.pid = 9999
    # Initial wait → KI; post-SIGINT wait → TimeoutExpired (we then
    # send SIGTERM); post-SIGTERM wait → succeeds.
    fake_proc.wait.side_effect = [
        KeyboardInterrupt,
        subprocess.TimeoutExpired(cmd="x", timeout=3.0),
        0,
    ]

    with patch.object(launcher.subprocess, "Popen", return_value=fake_proc):
        with (
            patch.object(launcher, "_wait_for_http", return_value=True),
            patch.object(launcher, "webbrowser"),
            patch.object(launcher.os, "killpg") as fake_killpg,
        ):
            launcher.launch_studio(cfg, port=47826, open_browser=False)
    sent = [call.args[1] for call in fake_killpg.call_args_list]
    assert signal.SIGINT in sent
    assert signal.SIGTERM in sent


def test_subprocess_entry_module_is_importable():
    """Smoke check the child entry module imports clean; ``-m`` must
    succeed in production."""
    import importlib

    mod = importlib.import_module("amx.web._studio_subprocess")
    assert hasattr(mod, "main")
    assert callable(mod.main)
