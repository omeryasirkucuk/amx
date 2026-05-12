"""The Studio child subprocess must shut down quickly on SIGINT /
SIGTERM. Without an explicit handler uvicorn under uvloop on macOS
took 3+ seconds to react, forcing the parent launcher to escalate
all the way to SIGKILL and the user to wait ~5 seconds per
``/studio`` Ctrl-C.

The child overrides ``server.install_signal_handlers`` and uses
``loop.add_signal_handler`` (the asyncio-aware variant) so the
flag flips inside the running loop on the next tick — Python's
synchronous :func:`signal.signal` handler races with uvloop and is
why the earlier fast-shutdown attempt didn't actually feel fast.
"""

from __future__ import annotations

import asyncio
import signal
from unittest.mock import AsyncMock, MagicMock, patch


def _studio_argv() -> list[str]:
    return [
        "_studio_subprocess",
        "--port",
        "47899",
        "--token",
        "fake-token",
        "--config-path",
        "",
    ]


def _run(coro):
    """Tiny wrapper so the tests don't need pytest-asyncio."""
    return asyncio.run(coro)


def test_child_bootstraps_history_store_singleton(monkeypatch):
    """Regression: the child subprocess must call ``init_history_store``
    on its own ``AMXConfig`` before handing the app to uvicorn. Earlier
    versions relied on the parent CLI's init carrying over, but Studio
    spawns the subprocess via ``_studio_subprocess.main`` in a fresh
    Python process — so ``history_store()`` returned ``None`` for every
    request and ``/api/ask`` rendered the spurious "Search catalog
    isn't initialised yet — run /search sync first." error even when
    the SQLite file was fully populated."""
    import sys

    monkeypatch.setattr(sys, "argv", _studio_argv())

    init_calls: list[object] = []

    def fake_init(cfg):
        init_calls.append(cfg)
        return MagicMock()

    fake_cfg = MagicMock(name="cfg")

    with (
        patch("uvicorn.Server", return_value=MagicMock()),
        patch("uvicorn.Config", return_value=MagicMock()),
        patch("amx.web.server.create_app", return_value=MagicMock()),
        patch("amx.config.AMXConfig.load", return_value=fake_cfg),
        patch("amx.utils.logging.mute_root_logger_for_studio"),
        patch("amx.storage.factory.init_history_store", side_effect=fake_init),
        patch("asyncio.run"),
    ):
        from amx.web import _studio_subprocess

        _studio_subprocess.main()

    assert init_calls == [fake_cfg]


def test_child_disables_uvicorn_default_signal_handlers(monkeypatch):
    """uvicorn's own SIGINT install is replaced with a no-op so the
    AMX handler is the only one reacting to the signal — no race."""
    import sys

    monkeypatch.setattr(sys, "argv", _studio_argv())

    fake_server = MagicMock()

    with (
        patch("uvicorn.Server", return_value=fake_server),
        patch("uvicorn.Config", return_value=MagicMock()),
        patch("amx.web.server.create_app", return_value=MagicMock()),
        patch("amx.config.AMXConfig.load", return_value=MagicMock()),
        patch("amx.utils.logging.mute_root_logger_for_studio"),
        patch("asyncio.run"),
    ):
        from amx.web import _studio_subprocess

        _studio_subprocess.main()

    # install_signal_handlers replaced with a callable that no-ops.
    assert callable(fake_server.install_signal_handlers)
    fake_server.install_signal_handlers()  # must not raise


def test_loop_signal_handlers_registered_for_sigint_and_sigterm():
    """Both signals must be wired to the loop, not to Python's
    synchronous handler — the loop variant is what makes the
    shutdown fast under uvloop."""
    from amx.web._studio_subprocess import _serve_with_fast_shutdown

    fake_server = MagicMock()
    fake_server.serve = AsyncMock(return_value=None)
    fake_loop = MagicMock()
    captured: dict[int, object] = {}

    def fake_add_signal_handler(sig, callback, *args):
        captured[sig] = (callback, args)

    fake_loop.add_signal_handler = fake_add_signal_handler

    with patch("asyncio.get_running_loop", return_value=fake_loop):
        _run(_serve_with_fast_shutdown(fake_server))

    assert signal.SIGINT in captured
    assert signal.SIGTERM in captured


def test_sigint_callback_flips_should_exit_only():
    """SIGINT triggers a graceful drain; force_exit stays False so
    in-flight requests get a chance to finish."""
    from amx.web._studio_subprocess import _serve_with_fast_shutdown

    fake_server = MagicMock()
    fake_server.serve = AsyncMock(return_value=None)
    fake_server.should_exit = False
    fake_server.force_exit = False
    captured: dict[int, object] = {}

    def fake_add(sig, cb, *args):
        captured[sig] = (cb, args)

    fake_loop = MagicMock()
    fake_loop.add_signal_handler = fake_add
    with patch("asyncio.get_running_loop", return_value=fake_loop):
        _run(_serve_with_fast_shutdown(fake_server))

    cb, args = captured[signal.SIGINT]
    cb(*args)
    assert fake_server.should_exit is True
    assert fake_server.force_exit is False


def test_sigterm_callback_also_sets_force_exit():
    """SIGTERM is the impatient signal: parent sends it after
    SIGINT's grace period; the child drops in-flight work."""
    from amx.web._studio_subprocess import _serve_with_fast_shutdown

    fake_server = MagicMock()
    fake_server.serve = AsyncMock(return_value=None)
    fake_server.should_exit = False
    fake_server.force_exit = False
    captured: dict[int, object] = {}

    def fake_add(sig, cb, *args):
        captured[sig] = (cb, args)

    fake_loop = MagicMock()
    fake_loop.add_signal_handler = fake_add
    with patch("asyncio.get_running_loop", return_value=fake_loop):
        _run(_serve_with_fast_shutdown(fake_server))

    cb, args = captured[signal.SIGTERM]
    cb(*args)
    assert fake_server.should_exit is True
    assert fake_server.force_exit is True


def test_windows_falls_back_to_synchronous_signal():
    """asyncio on Windows raises NotImplementedError from
    add_signal_handler. We must fall back to signal.signal so the
    child still has a working shutdown path."""
    from amx.web._studio_subprocess import _serve_with_fast_shutdown

    fake_server = MagicMock()
    fake_server.serve = AsyncMock(return_value=None)
    fake_loop = MagicMock()
    fake_loop.add_signal_handler.side_effect = NotImplementedError

    captured: dict[int, object] = {}

    def fake_signal(sig, handler):
        captured[sig] = handler
        return signal.SIG_DFL

    with (
        patch("asyncio.get_running_loop", return_value=fake_loop),
        patch("signal.signal", side_effect=fake_signal),
    ):
        _run(_serve_with_fast_shutdown(fake_server))

    assert signal.SIGINT in captured
    assert signal.SIGTERM in captured
