"""Tests for the ``--columns`` flag added to ``/analyze run``.

The flag is a non-interactive equivalent of the existing Column scope
picker. These tests cover the parsing helper that turns the
``schema.table.column[,…]`` strings into the
``{(schema, table): {col1, col2, ...}}`` map ``ScopeResult.column_overrides``
expects. The downstream Orchestrator integration is exercised by the
existing column-scope tests.
"""

from __future__ import annotations

import click
import pytest

from amx.cli_support.commands.analyze_flow import _parse_columns_opt


def test_empty_input_returns_empty_map() -> None:
    assert _parse_columns_opt(None) == {}
    assert _parse_columns_opt(()) == {}
    assert _parse_columns_opt(("",)) == {}


def test_single_triple() -> None:
    out = _parse_columns_opt(("public.users.email",))
    assert out == {("public", "users"): {"email"}}


def test_repeated_flag_accumulates() -> None:
    out = _parse_columns_opt(
        ("public.users.email", "public.users.id", "staging.events.ts"),
    )
    assert out[("public", "users")] == {"email", "id"}
    assert out[("staging", "events")] == {"ts"}


def test_comma_batched_single_flag() -> None:
    out = _parse_columns_opt(("public.users.id,public.users.email",))
    assert out == {("public", "users"): {"id", "email"}}


def test_mixed_repeat_and_batch() -> None:
    out = _parse_columns_opt(
        ("public.users.id,public.users.email", "staging.events.ts"),
    )
    assert out[("public", "users")] == {"id", "email"}
    assert out[("staging", "events")] == {"ts"}


def test_malformed_triple_raises() -> None:
    with pytest.raises(click.BadParameter, match="schema.table.column"):
        _parse_columns_opt(("public.users",))


def test_empty_part_raises() -> None:
    with pytest.raises(click.BadParameter):
        _parse_columns_opt(("public..email",))


def test_whitespace_only_entries_skipped() -> None:
    out = _parse_columns_opt((" , , public.users.email , ",))
    assert out == {("public", "users"): {"email"}}
