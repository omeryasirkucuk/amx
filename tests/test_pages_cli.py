"""End-to-end CLI tests for the ``/pages`` namespace subcommands."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import click
import pytest
from click.testing import CliRunner

from amx.cli_support.commands.pages import register_pages_commands
from amx.config import AMXConfig
from amx.pages import factory as pages_factory
from amx.pages.service import PagesService
from amx.pages.store import PageStore
from amx.pages.types import AssetRef
from amx.storage import sqlite_store as _store_module
from amx.storage.sqlite_store import SQLiteHistoryStore


class _StubLLM:
    def chat(self, messages: list[dict[str, str]], **kw: Any) -> object:
        class R:
            content = "# Overview\n\nTest body line 1\nTest body line 2"

        return R()


class _StubResolver:
    def resolve_db_asset(self, ref: str) -> str:
        return f"db {ref}"

    def resolve_doc_profile(self, ref: str, intent: str, k: int = 5) -> list[str]:
        return []

    def resolve_lineage(self, ref: str) -> str:
        return f"lineage {ref}"

    def resolve_source(self, src: Any) -> str:
        return f"src {src.original_name}"


@pytest.fixture
def cli(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Root Click group with the /pages namespace wired to a tmp history.db."""
    hs = SQLiteHistoryStore(tmp_path / "history.db")
    hs.init()
    _store_module._store = hs

    def _stub_build(cfg: AMXConfig) -> PagesService:
        store = PageStore(history=hs)
        store.init_schema()
        return PagesService(
            store=store, llm=_StubLLM(), resolver=_StubResolver(), model_name="test"
        )

    monkeypatch.setattr(pages_factory, "build_pages_service", _stub_build)
    # Re-import target inside the commands module too.
    from amx.cli_support.commands import pages as pages_cmd

    monkeypatch.setattr(pages_cmd, "build_pages_service", _stub_build)

    cfg = AMXConfig()

    @click.group()
    @click.pass_context
    def root(ctx: click.Context) -> None:
        ctx.obj = cfg

    register_pages_commands(root)
    return root, hs, cfg


@pytest.fixture(autouse=True)
def _reset_singleton():
    yield
    _store_module._store = None


def _make_page(cfg: AMXConfig) -> str:
    svc = pages_factory.build_pages_service(cfg)
    pid = svc.create_draft(
        title="Test Page",
        intent="explain",
        assets=[AssetRef("db_table", "p/d/s/t")],
        sources=[],
        created_by="omer",
        now=datetime.now(timezone.utc),
    )
    svc.generate(pid, now=datetime.now(timezone.utc))
    return pid


def test_pages_list_empty(cli) -> None:
    root, _hs, _cfg = cli
    runner = CliRunner()
    result = runner.invoke(root, ["pages", "list"])
    assert result.exit_code == 0, result.output
    assert "No pages yet" in result.output


def test_pages_list_shows_created_page(cli) -> None:
    root, _hs, cfg = cli
    pid = _make_page(cfg)
    runner = CliRunner()
    result = runner.invoke(root, ["pages", "list"])
    assert result.exit_code == 0, result.output
    assert "Test Page" in result.output
    assert pid[:8] in result.output


def test_pages_show_prints_body(cli) -> None:
    root, _hs, cfg = cli
    pid = _make_page(cfg)
    runner = CliRunner()
    result = runner.invoke(root, ["pages", "show", pid])
    assert result.exit_code == 0, result.output
    assert "Overview" in result.output
    assert "Test body line 1" in result.output


def test_pages_show_unknown_id(cli) -> None:
    root, _hs, _cfg = cli
    runner = CliRunner()
    result = runner.invoke(root, ["pages", "show", "no-such-id"])
    assert result.exit_code == 0
    assert "not found" in result.output


def test_pages_export_md_writes_file(cli, tmp_path: Path) -> None:
    root, _hs, cfg = cli
    pid = _make_page(cfg)
    out = tmp_path / "out.md"
    runner = CliRunner()
    result = runner.invoke(root, ["pages", "export", pid, "--format", "md", "--out", str(out)])
    assert result.exit_code == 0, result.output
    assert out.exists()
    body = out.read_text(encoding="utf-8")
    assert "Overview" in body


def test_pages_delete_soft(cli) -> None:
    root, _hs, cfg = cli
    pid = _make_page(cfg)
    runner = CliRunner()
    result = runner.invoke(root, ["pages", "delete", pid])
    assert result.exit_code == 0, result.output

    # Soft-deleted pages should not appear in /pages list.
    list_result = runner.invoke(root, ["pages", "list"])
    assert pid not in list_result.output


def test_pages_delete_purge_removes_row(cli) -> None:
    root, hs, cfg = cli
    pid = _make_page(cfg)
    runner = CliRunner()
    result = runner.invoke(root, ["pages", "delete", pid, "--purge"])
    assert result.exit_code == 0, result.output
    assert hs.get_documentation_page(pid) is None


def test_pages_new_with_intent_template_flag(cli) -> None:
    """Power-user mode: --intent-template + --asset + --no-generate skips
    the wizard entirely and stores a template-rendered intent string on
    the page draft."""
    root, _hs, cfg = cli
    runner = CliRunner()
    # ``--source`` is absent so the sources wizard prompt fires; feed it
    # an empty stdin so click takes the prompt default ("" = skip) on
    # every Click version. Without ``input``, older Click releases abort
    # on EOFError when the test runner has no TTY (which is exactly the
    # CI matrix shape for py3.10-py3.13).
    result = runner.invoke(
        root,
        [
            "pages",
            "new",
            "--title",
            "Orders Table Doc",
            "--intent-template",
            "single-table",
            "--asset",
            "db_table:prod/main/sales/orders",
            "--no-generate",
        ],
        input="\n",
    )
    assert result.exit_code == 0, result.output
    assert "Created page" in result.output

    svc = pages_factory.build_pages_service(cfg)
    rows = svc.store.list_active()
    assert len(rows) == 1
    page = svc.store.get(rows[0]["id"])
    assert page is not None
    intent = page.get("generation_prompt", "")
    # The template's prompt skeleton must have rendered.
    assert "documentation page" in intent
    # No placeholder values were supplied via flags, so the template
    # was rendered with no params — but the prompt skeleton itself
    # must still be present (not a free-text empty string).
    assert "{table}" in intent or "table" in intent


def test_pages_new_rejects_unknown_template(cli) -> None:
    root, _hs, _cfg = cli
    runner = CliRunner()
    result = runner.invoke(
        root,
        [
            "pages",
            "new",
            "--title",
            "x",
            "--intent-template",
            "made-up-slug",
            "--asset",
            "db_profile:p",
            "--no-generate",
        ],
    )
    # Error path returns cleanly (no exception) but logs the error.
    assert result.exit_code == 0
    assert "Unknown intent template" in result.output
