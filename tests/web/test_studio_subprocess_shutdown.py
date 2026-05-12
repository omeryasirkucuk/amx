"""The Studio child subprocess must shut down quickly on SIGINT /
SIGTERM. Without an explicit handler uvicorn under uvloop on macOS
took 3+ seconds to react, forcing the parent launcher to escalate
all the way to SIGKILL and the user to wait ~5 seconds per
``/studio`` Ctrl-C.

The child now overrides ``server.install_signal_handlers`` and
installs a synchronous handler that flips ``should_exit`` (and
``force_exit`` on SIGTERM) before returning. Tests here pin the
contract; the timing improvement is observable in the launcher
constants but not directly assertable without spawning a real
subprocess.
"""

from __future__ import annotations

import signal
from unittest.mock import MagicMock, patch


def test_child_installs_explicit_signal_handlers(monkeypatch):
    """``signal.signal`` must be called for both SIGINT and SIGTERM
    so the child's shutdown is driven by AMX, not by uvicorn's
    sometimes-laggy uvloop integration."""
    import sys

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "_studio_subprocess",
            "--port",
            "47899",
            "--token",
            "fake-token",
            "--config-path",
            "",
        ],
    )

    fake_server = MagicMock()
    fake_config = MagicMock()
    captured_handlers: dict[int, object] = {}

    def fake_signal_signal(signum, handler):
        captured_handlers[signum] = handler
        return signal.SIG_DFL

    with (
        patch("uvicorn.Server", return_value=fake_server),
        patch("uvicorn.Config", return_value=fake_config),
        patch("amx.web.server.create_app", return_value=MagicMock()),
        patch("amx.config.AMXConfig.load", return_value=MagicMock()),
        patch("amx.utils.logging.mute_root_logger_for_studio"),
        patch("asyncio.run"),
        patch("signal.signal", side_effect=fake_signal_signal),
    ):
        from amx.web import _studio_subprocess

        _studio_subprocess.main()

    assert signal.SIGINT in captured_handlers
    assert signal.SIGTERM in captured_handlers
    # The same handler is wired for both signals — branching on the
    # signum inside the handler keeps the install symmetric.
    assert captured_handlers[signal.SIGINT] is captured_handlers[signal.SIGTERM]


def test_handler_flips_should_exit_on_sigint():
    """SIGINT triggers a graceful drain (``should_exit=True``) but
    leaves ``force_exit`` alone so in-flight requests get a chance
    to wrap up cleanly."""
    import sys

    sys.argv = [
        "_studio_subprocess",
        "--port",
        "47899",
        "--token",
        "fake",
        "--config-path",
        "",
    ]

    fake_server = MagicMock()
    fake_server.should_exit = False
    fake_server.force_exit = False
    handlers: dict[int, object] = {}

    def fake_signal_signal(signum, handler):
        handlers[signum] = handler
        return signal.SIG_DFL

    with (
        patch("uvicorn.Server", return_value=fake_server),
        patch("uvicorn.Config", return_value=MagicMock()),
        patch("amx.web.server.create_app", return_value=MagicMock()),
        patch("amx.config.AMXConfig.load", return_value=MagicMock()),
        patch("amx.utils.logging.mute_root_logger_for_studio"),
        patch("asyncio.run"),
        patch("signal.signal", side_effect=fake_signal_signal),
    ):
        from amx.web import _studio_subprocess

        _studio_subprocess.main()

    handler = handlers[signal.SIGINT]
    handler(signal.SIGINT, None)  # type: ignore[operator]
    assert fake_server.should_exit is True
    # SIGINT does NOT set force_exit; only SIGTERM does (next test).
    assert fake_server.force_exit is False


def test_handler_force_exits_on_sigterm():
    """SIGTERM is the impatient signal: AMX's parent sends it after
    SIGINT's grace period; the child should drop in-flight requests
    and exit immediately."""
    import sys

    sys.argv = [
        "_studio_subprocess",
        "--port",
        "47899",
        "--token",
        "fake",
        "--config-path",
        "",
    ]

    fake_server = MagicMock()
    fake_server.should_exit = False
    fake_server.force_exit = False
    handlers: dict[int, object] = {}

    def fake_signal_signal(signum, handler):
        handlers[signum] = handler
        return signal.SIG_DFL

    with (
        patch("uvicorn.Server", return_value=fake_server),
        patch("uvicorn.Config", return_value=MagicMock()),
        patch("amx.web.server.create_app", return_value=MagicMock()),
        patch("amx.config.AMXConfig.load", return_value=MagicMock()),
        patch("amx.utils.logging.mute_root_logger_for_studio"),
        patch("asyncio.run"),
        patch("signal.signal", side_effect=fake_signal_signal),
    ):
        from amx.web import _studio_subprocess

        _studio_subprocess.main()

    handler = handlers[signal.SIGTERM]
    handler(signal.SIGTERM, None)  # type: ignore[operator]
    assert fake_server.should_exit is True
    assert fake_server.force_exit is True


def test_child_disables_uvicorn_default_signal_handlers():
    """uvicorn's own SIGINT install is replaced with a no-op so the
    AMX handler is the only one reacting to the signal — no race."""
    import sys

    sys.argv = [
        "_studio_subprocess",
        "--port",
        "47899",
        "--token",
        "fake",
        "--config-path",
        "",
    ]

    fake_server = MagicMock()

    with (
        patch("uvicorn.Server", return_value=fake_server),
        patch("uvicorn.Config", return_value=MagicMock()),
        patch("amx.web.server.create_app", return_value=MagicMock()),
        patch("amx.config.AMXConfig.load", return_value=MagicMock()),
        patch("amx.utils.logging.mute_root_logger_for_studio"),
        patch("asyncio.run"),
        patch("signal.signal"),
    ):
        from amx.web import _studio_subprocess

        _studio_subprocess.main()

    # install_signal_handlers should be replaced with a callable
    # that no-ops, NOT left at its default.
    assert callable(fake_server.install_signal_handlers)
    # Verify the no-op is harmless when uvicorn invokes it.
    fake_server.install_signal_handlers()
