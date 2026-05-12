"""Regression: ``/studio`` must run uvicorn in a child PROCESS, not
a daemon thread. The thread-based implementation left stdin in a
flushed-but-non-canonical state after Ctrl-C on macOS — arrow keys
echoed as literal ``^[[C`` and the user had to restart the CLI.
Process isolation fixes the symptom categorically because uvicorn's
asyncio loop, signal handlers, and stdin file-descriptor edits live
in a separate Python interpreter.
"""

from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch


def test_launch_studio_spawns_subprocess_with_python_module(tmp_path, monkeypatch):
    from amx.config import AMXConfig
    from amx.web import launcher

    cfg = AMXConfig()
    object.__setattr__(cfg, "_config_path", str(tmp_path / "config.yml"))

    fake_proc = MagicMock()
    fake_proc.wait.return_value = 0
    with patch.object(launcher, "subprocess") as fake_sub:
        fake_sub.Popen.return_value = fake_proc
        with (
            patch.object(launcher, "_wait_for_http", return_value=True),
            patch.object(launcher, "webbrowser"),
        ):
            ok = launcher.launch_studio(cfg, port=47821, open_browser=False)
    assert ok is True
    args, _ = fake_sub.Popen.call_args
    cmd = args[0]
    assert cmd[0] == sys.executable
    assert cmd[1:4] == ["-m", "amx.web._studio_subprocess", "--port"]
    assert "47821" in cmd
    assert "--token" in cmd
    assert "--config-path" in cmd


def test_launch_studio_sigint_triggers_child_shutdown(tmp_path, monkeypatch):
    """Ctrl-C in the parent must send SIGINT to the child and wait
    for it to exit. Without this, the parent CLI's prompt loop would
    block forever on proc.wait() when the user wants out of /studio."""
    from amx.config import AMXConfig
    from amx.web import launcher

    cfg = AMXConfig()
    object.__setattr__(cfg, "_config_path", str(tmp_path / "config.yml"))

    fake_proc = MagicMock()
    fake_proc.wait.side_effect = [KeyboardInterrupt, 0]
    with patch.object(launcher, "subprocess") as fake_sub:
        fake_sub.Popen.return_value = fake_proc
        fake_sub.TimeoutExpired = TimeoutError  # so .wait(timeout=...) compares fine
        with (
            patch.object(launcher, "_wait_for_http", return_value=True),
            patch.object(launcher, "webbrowser"),
        ):
            launcher.launch_studio(cfg, port=47822, open_browser=False)
    # First wait() raised KI, then we send SIGINT and wait again.
    assert fake_proc.send_signal.called
    sent_sig = fake_proc.send_signal.call_args[0][0]
    import signal as _signal

    assert sent_sig == _signal.SIGINT
    # wait() called at least twice (initial + post-SIGINT)
    assert fake_proc.wait.call_count >= 2


def test_subprocess_entry_module_is_importable():
    """Smoke check the child entry module imports clean; ``-m`` must
    succeed in production."""
    import importlib

    mod = importlib.import_module("amx.web._studio_subprocess")
    assert hasattr(mod, "main")
    assert callable(mod.main)
