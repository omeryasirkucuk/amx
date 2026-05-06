"""``/session`` works from any namespace + lives under /search.

Reported: typing ``/session list`` while inside ``/search`` returned
"Unknown command". The dispatcher's search-namespace branch had a
narrow allowlist of cross-namespace heads that didn't include
``"session"`` — every other tab fell through to the broader allow
set further down, so the bug was specific to /search.

Plus a presentation fix: the slash registry listed ``/session``
under "root" so it didn't appear in /search's command help —
inconsistent with where users naturally look (next to /ask).
"""

from __future__ import annotations

from amx.cli_support.session import session_to_click_args
from amx.cli_support.slash_commands import _ROOT_BUILTINS, _SEARCH_COMMANDS


def test_session_dispatches_from_search_namespace() -> None:
    """The bug-report path: ``/session list`` typed inside /search
    must produce the Click args ``["session", "list"]`` so the
    chat_session command group runs."""
    assert session_to_click_args("search", ["session", "list"]) == ["session", "list"]
    assert session_to_click_args("search", ["session", "resume", "42"]) == [
        "session",
        "resume",
        "42",
    ]
    assert session_to_click_args("search", ["session", "new"]) == ["session", "new"]
    assert session_to_click_args("search", ["session", "end"]) == ["session", "end"]
    assert session_to_click_args("search", ["session", "scope", "a", "b"]) == [
        "session",
        "scope",
        "a",
        "b",
    ]


def test_session_still_works_from_other_tabs() -> None:
    """Cross-namespace dispatch from every other tab — same as
    /doctor, /compare, /history-store. Pin the contract so a future
    refactor doesn't quietly break it."""
    for namespace in ("", "db", "metadata", "docs", "llm", "code", "analyze", "history"):
        assert session_to_click_args(namespace, ["session", "list"]) == [
            "session",
            "list",
        ], f"failed from namespace={namespace!r}"


def test_session_listed_under_search_group() -> None:
    """The slash registry now lists /session under the search tab
    so its help line appears next to /ask. Pin the move so it
    doesn't drift back to root."""
    search_names = {sc.command for sc in _SEARCH_COMMANDS}
    root_names = {sc.command for sc in _ROOT_BUILTINS}
    assert "/session" in search_names
    assert "/session" not in root_names


def test_studio_dispatches_from_every_tab() -> None:
    """``/studio`` (open AMX Studio in browser) used to fail with
    "Unknown command" inside /search because the search-namespace
    dispatcher's allowed set didn't list it. Same fix as /session:
    one cross-namespace head set, used by every dispatcher branch."""
    for namespace in ("", "db", "metadata", "docs", "llm", "code", "analyze", "search", "history"):
        out = session_to_click_args(namespace, ["studio"])
        assert out == ["studio"], f"failed from namespace={namespace!r}: {out!r}"


def test_config_dispatches_globally_except_inside_search() -> None:
    """``/config`` is global — dispatches to the top-level config
    Click subcommand from every tab — except /search, which has its
    own search-config subcommand. Inside /search the head resolves
    to ``["search", "config", ...]`` (search-config); everywhere else
    it resolves to ``["config", ...]`` (root-config). Pin both so a
    future allowed-set drift doesn't accidentally hide one of them."""
    for namespace in ("", "db", "metadata", "docs", "llm", "code", "analyze", "history"):
        assert session_to_click_args(namespace, ["config", "show"]) == [
            "config",
            "show",
        ], f"failed from namespace={namespace!r}"
    # /search has its own /config — keeps the existing precedence.
    assert session_to_click_args("search", ["config", "show"]) == [
        "search",
        "config",
        "show",
    ]


def test_setup_dispatches_from_every_tab() -> None:
    """``/setup`` (interactive first-run wizard) reachable from every
    tab including /search."""
    for namespace in ("", "db", "metadata", "docs", "llm", "code", "analyze", "search", "history"):
        out = session_to_click_args(namespace, ["setup"])
        assert out == ["setup"], f"failed from namespace={namespace!r}: {out!r}"
