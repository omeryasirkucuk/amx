"""prompt_toolkit caches a single ``Vt100Input`` per AppSession; when
``/studio`` (or any other subprocess) leaves stdin in a state that
makes ``tcgetattr`` fail, ``raw_mode.__enter__`` silently no-ops and
the terminal stays cooked — arrow keys then echo as literal
``^[[C`` until the CLI restarts.

The fix is :func:`amx.cli_support.session._rebuild_prompt_input`
called once per prompt iteration after dispatch. It drops the cached
``_input`` so the next ``prompt()`` rebuilds via ``create_input()``,
and defensively ``tcflush``-es stale bytes from the kernel input
queue. These tests pin both behaviours.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch


def test_rebuild_clears_app_session_cached_input():
    from amx.cli_support.session import _rebuild_prompt_input

    fake_session = MagicMock()
    fake_session._input = object()  # pretend a Vt100Input is cached

    with patch(
        "prompt_toolkit.application.current.get_app_session",
        return_value=fake_session,
    ):
        _rebuild_prompt_input()

    assert fake_session._input is None


def test_rebuild_calls_tcflush_when_stdin_is_a_tty():
    """Drain any stale bytes the subprocess may have left buffered
    by the kernel so the next prompt doesn't replay them as input."""
    from amx.cli_support import session as session_mod

    fake_session = MagicMock()
    fake_session._input = object()

    with patch.object(session_mod, "_rebuild_prompt_input.__wrapped__", create=True):
        # the actual helper is module-level — patch its termios call
        pass

    with (
        patch("sys.stdin") as fake_stdin,
        patch("os.isatty", return_value=True),
        patch("termios.tcflush") as fake_tcflush,
        patch(
            "prompt_toolkit.application.current.get_app_session",
            return_value=fake_session,
        ),
    ):
        fake_stdin.fileno.return_value = 0
        session_mod._rebuild_prompt_input()

    import termios

    fake_tcflush.assert_called_once_with(0, termios.TCIFLUSH)


def test_rebuild_no_ops_when_stdin_not_a_tty():
    """In tests / piped input / detached tty the tcflush call must
    silently no-op — no exception bubbles up."""
    from amx.cli_support import session as session_mod

    fake_session = MagicMock()
    fake_session._input = object()

    with (
        patch("sys.stdin") as fake_stdin,
        patch("os.isatty", return_value=False),
        patch("termios.tcflush") as fake_tcflush,
        patch(
            "prompt_toolkit.application.current.get_app_session",
            return_value=fake_session,
        ),
    ):
        fake_stdin.fileno.return_value = 7
        session_mod._rebuild_prompt_input()

    fake_tcflush.assert_not_called()
    # Still invalidates the cached input even when the tty branch is
    # skipped — the input layer needs rebuilding regardless of the
    # kernel buffer drain.
    assert fake_session._input is None


def test_rebuild_swallows_termios_error():
    """A failed tcflush (e.g. transient OSError) must NOT break the
    prompt loop. Worst case we lose one chance to recover this
    iteration; the next prompt round will try again."""
    from amx.cli_support import session as session_mod

    fake_session = MagicMock()
    fake_session._input = object()

    with (
        patch("sys.stdin") as fake_stdin,
        patch("os.isatty", return_value=True),
        patch("termios.tcflush", side_effect=OSError("transient")),
        patch(
            "prompt_toolkit.application.current.get_app_session",
            return_value=fake_session,
        ),
    ):
        fake_stdin.fileno.return_value = 0
        # Must not raise
        session_mod._rebuild_prompt_input()


def test_rebuild_swallows_app_session_unavailable():
    """If prompt_toolkit internals shift and ``get_app_session`` is
    gone or raises, we still don't take down the prompt loop."""
    from amx.cli_support import session as session_mod

    with (
        patch(
            "prompt_toolkit.application.current.get_app_session",
            side_effect=RuntimeError("no current session"),
        ),
        patch("sys.stdin") as fake_stdin,
        patch("os.isatty", return_value=False),
    ):
        fake_stdin.fileno.return_value = 0
        # Must not raise
        session_mod._rebuild_prompt_input()
