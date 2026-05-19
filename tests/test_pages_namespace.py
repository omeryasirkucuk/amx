"""``/pages`` is a first-class REPL namespace.

Pins:
* ``/pages`` shows up as a root entry-point (so typing it at root
  switches to the Pages tab instead of "Unknown command").
* The Pages namespace owns its own subcommands and the dispatch /
  autocomplete derivations agree on the set.
* The arrow-nav tab order in the keybindings module + the tab-bar
  order in the UI module + ``all_namespaces()`` agree — a missing
  entry in any of those caused the historical bug where a freshly
  added tab couldn't be reached by Left/Right.
* ``session_to_click_args`` routes subcommands from any tab.
"""

from __future__ import annotations

from amx.cli_support._session_keybindings import _kb_escape_namespace
from amx.cli_support._session_ui import _TAB_ORDER
from amx.cli_support.session import session_to_click_args
from amx.cli_support.slash_commands import (
    _PAGES_COMMANDS,
    _ROOT_ENTRYPOINTS,
    all_namespaces,
    cmd_heads_for_namespace,
    commands_for_namespace,
    find_command,
)


def test_pages_is_registered_root_entrypoint() -> None:
    """Typing ``/pages`` at root must resolve to a known root command."""
    names = {sc.command for sc in _ROOT_ENTRYPOINTS}
    assert "/pages" in names
    sc = find_command("/pages")
    assert sc is not None
    assert sc.namespace == "root"


def test_pages_namespace_listed_in_all_namespaces() -> None:
    assert "pages" in all_namespaces()


def test_pages_namespace_owns_its_subcommands() -> None:
    heads = cmd_heads_for_namespace("pages")
    for expected in ("new", "list", "show", "edit", "export", "delete"):
        assert expected in heads, f"missing /pages {expected} in dispatch heads"


def test_pages_namespace_autocomplete_only_lists_pages_commands() -> None:
    """Pressing / in the Pages tab shows builtins + the six pages
    subcommands — NOT history/lineage's identically-named commands."""
    cmds = commands_for_namespace("pages")
    cmd_names = {c.command for c in cmds}
    expected = {"/new", "/list", "/show", "/edit", "/export", "/delete"}
    assert expected.issubset(cmd_names)
    # And nothing from sibling tabs leaks in.
    assert "/profile" not in cmd_names  # /db
    assert "/run" not in cmd_names  # /analyze
    assert "/results" not in cmd_names  # /history
    assert "/create" not in cmd_names  # /lineage


def test_pages_subcommand_routes_from_every_namespace() -> None:
    """``/pages list`` etc. must reach Click as ``pages list`` from
    any tab — that's the contract behind 'every command reachable
    from every page'."""
    for namespace in (
        "",
        "db",
        "metadata",
        "docs",
        "llm",
        "code",
        "analyze",
        "search",
        "history",
        "lineage",
        "pages",
    ):
        assert session_to_click_args(namespace, ["pages", "list"]) == ["pages", "list"], (
            f"failed from namespace={namespace!r}"
        )
        assert session_to_click_args(namespace, ["pages", "new"]) == ["pages", "new"], (
            f"failed from namespace={namespace!r}"
        )


def test_pages_subcommand_routes_when_already_in_pages_tab() -> None:
    """Inside the /pages tab the user types the bare ``/list`` form;
    dispatch must rewrite it to ``pages list``."""
    assert session_to_click_args("pages", ["list"]) == ["pages", "list"]
    assert session_to_click_args("pages", ["new"]) == ["pages", "new"]
    assert session_to_click_args("pages", ["show", "abc"]) == ["pages", "show", "abc"]
    assert session_to_click_args("pages", ["export", "abc", "--format", "md"]) == [
        "pages",
        "export",
        "abc",
        "--format",
        "md",
    ]


def test_pages_subcommand_definitions_carry_pages_namespace() -> None:
    """Each entry in the pages registry tuple lives under the
    ``pages`` namespace so ``commands_for_namespace`` and
    ``cmd_heads_for_namespace`` reflect it."""
    for sc in _PAGES_COMMANDS:
        assert sc.namespace == "pages", (
            f"{sc.command} declared namespace={sc.namespace!r}, expected 'pages'"
        )


def test_tab_strip_includes_pages_after_lineage() -> None:
    """Tab bar order — pages renders to the right of lineage."""
    assert "pages" in _TAB_ORDER
    assert _TAB_ORDER.index("pages") > _TAB_ORDER.index("lineage")


def test_arrow_navigation_reaches_pages_tab() -> None:
    """Arrow-key Left/Right tab cycling must include the new tab.

    Past regression: a tab added to the UI tab strip + the slash
    registry was unreachable by arrow keys because the keybinding
    module kept its own hand-maintained copy of the tab list. The
    list is now derived from ``all_namespaces()`` — this test pins
    that derivation.
    """
    # The keybinding closure builds its internal tabs list when
    # invoked; we can't read it directly, so we assert the upstream
    # source it derives from.
    _kb_escape_namespace()  # smoke-test the closure builds
    assert "pages" in all_namespaces()


def test_tab_order_agrees_with_registry() -> None:
    """``_TAB_ORDER`` is just ``("root",) + all_namespaces()`` — any
    drift here means a tab is visible in the strip but unreachable
    via the registry-driven dispatch (or vice versa)."""
    assert tuple(_TAB_ORDER) == ("root", *all_namespaces())
