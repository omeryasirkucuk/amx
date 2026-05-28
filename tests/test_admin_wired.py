"""The /admin namespace is wired into the CLI.

`register_admin_commands` is implemented + tested but was never called,
and session dispatch omitted "admin", so the ADMIN tab showed everywhere
yet `/admin` returned "Unknown command" and its subcommands raised
"No such command" — a 100% dead tab. Guard the wiring so it can't regress.
"""

from __future__ import annotations

from amx.cli_support.session import session_to_click_args


def test_admin_group_registered_on_main() -> None:
    import amx.cli as cli

    assert "admin" in cli.main.commands, "/admin group not registered on the CLI"
    admin = cli.main.commands["admin"]
    assert "members" in admin.commands  # type: ignore[attr-defined]


def test_admin_enters_namespace_and_subcommands_route() -> None:
    assert session_to_click_args("", ["admin"]) == ["admin"]
    assert session_to_click_args("admin", ["members"]) == ["admin", "members"]
