"""Real commands are both reachable from the REPL and discoverable.

`/rerun` and `/variations` are real top-level Click commands but were
missing from `session._CROSS_NAMESPACE_HEADS`, so typing them at the
REPL root returned None → "Unknown command" (unreachable, not just
invisible). `/usage` works via a builtin handler but was absent from the
slash-command registry, so it never appeared in /help or autocomplete.
"""

from __future__ import annotations

from amx.cli_support.session import session_to_click_args
from amx.cli_support.slash_commands import find_command


def test_rerun_dispatches_from_root() -> None:
    assert session_to_click_args("", ["rerun"]) == ["rerun"]
    # with an argument (a result id)
    assert session_to_click_args("", ["rerun", "42"]) == ["rerun", "42"]


def test_variations_dispatches_from_root() -> None:
    assert session_to_click_args("", ["variations"]) == ["variations"]
    assert session_to_click_args("", ["variations", "42"]) == ["variations", "42"]


def test_existing_root_command_still_dispatches() -> None:
    # regression guard for the cross-namespace set
    assert session_to_click_args("", ["setup"]) == ["setup"]


def test_invisible_commands_now_in_registry() -> None:
    for cmd in ("rerun", "variations", "usage"):
        assert find_command(cmd) is not None, f"/{cmd} missing from the registry"
        assert find_command(f"/{cmd}") is not None
