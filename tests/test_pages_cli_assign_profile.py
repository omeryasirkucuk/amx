"""Tests for the /pages assign-profile CLI command.

Invokes the command programmatically via Click's test runner and
asserts that the underlying store is updated correctly.

``build_pages_service`` (imported inside ``_svc``) is patched at the
factory module level so no real AMXConfig / LLM provider is needed.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import patch

import click
import pytest
from click.testing import CliRunner

from amx.pages.service import PagesService
from amx.pages.store import PageStore
from amx.storage.sqlite_store import SQLiteHistoryStore


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


# ── Minimal stub collaborators ────────────────────────────────────────────────


class _StubLLMClient:
    @property
    def model_name(self) -> str:
        return "stub"

    @property
    def cfg(self) -> Any:
        return None

    def chat(self, messages: list[dict[str, str]], **kw: Any) -> Any:  # pragma: no cover
        raise NotImplementedError


class _StubResolver:
    def resolve_db_asset(self, ref: str) -> str:  # pragma: no cover
        return ""

    def resolve_doc_profile(self, ref: str, intent: str, k: int = 5) -> list[str]:  # pragma: no cover
        return []

    def resolve_lineage(self, ref: str) -> str:  # pragma: no cover
        return ""

    def resolve_source(self, src: Any) -> str:  # pragma: no cover
        return ""


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture
def tmp_store(tmp_path: Path) -> SQLiteHistoryStore:
    db = SQLiteHistoryStore(tmp_path / "history.db")
    db.init()
    return db


def _make_service(store: SQLiteHistoryStore) -> PagesService:
    return PagesService(
        store=PageStore(history=store),
        llm=_StubLLMClient(),
        resolver=_StubResolver(),
        model_name="stub",
    )


def _seed_page(store: SQLiteHistoryStore, *, slug: str) -> str:
    page_id = f"bbbbbbbb-0000-0000-0000-{slug[:12].ljust(12, '0')}"
    now = _utcnow()
    store.create_documentation_page(
        page_id=page_id,
        title="My Page",
        slug=slug,
        markdown_body="",
        rendered_html=None,
        status="draft",
        created_at=now,
        updated_at=now,
        created_by=None,
        generation_prompt=None,
        model_used=None,
        db_profile=None,
    )
    return page_id


def _invoke(
    store: SQLiteHistoryStore,
    args: list[str],
) -> CliRunner:
    """Register the pages commands with a patched factory and invoke *args*."""
    svc = _make_service(store)

    @click.group()
    @click.pass_context
    def main(ctx: click.Context) -> None:
        pass

    from amx.cli_support.commands.pages import register_pages_commands

    # ``_svc`` calls the locally-imported name ``build_pages_service``
    # (imported at the top of pages.py), so the patch must target the name
    # in the commands module's namespace.
    target = "amx.cli_support.commands.pages.build_pages_service"

    with patch(target, return_value=svc):
        register_pages_commands(main)
        runner = CliRunner()
        result = runner.invoke(
            main,
            args,
            obj=object(),  # cfg stub — not used by assign-profile
            catch_exceptions=False,
        )

    return result


# ── Tests ─────────────────────────────────────────────────────────────────────


def test_assign_profile_flag_form_updates_row(tmp_store: SQLiteHistoryStore) -> None:
    """Power-user flag form updates db_profile without interactive prompts."""
    page_id = _seed_page(tmp_store, slug="flag-page")

    result = _invoke(tmp_store, ["pages", "assign-profile", "flag-page", "--profile", "prod"])
    assert result.exit_code == 0, f"Command failed:\n{result.output}"

    row = tmp_store.get_documentation_page(page_id)
    assert row is not None
    assert row["db_profile"] == "prod", (
        f"Expected db_profile='prod', got {row['db_profile']!r}\nOutput: {result.output}"
    )


def test_assign_profile_unknown_slug_reports_error(tmp_store: SQLiteHistoryStore) -> None:
    """A non-existent slug causes an error message and exits cleanly."""
    result = _invoke(
        tmp_store, ["pages", "assign-profile", "ghost-page", "--profile", "prod"]
    )
    assert result.exit_code == 0, f"Unexpected exception:\n{result.output}"
    assert "ghost-page" in result.output


def test_assign_profile_clear_with_empty_profile(tmp_store: SQLiteHistoryStore) -> None:
    """Passing an empty --profile string clears db_profile to NULL."""
    page_id = _seed_page(tmp_store, slug="clear-page")
    tmp_store.update_documentation_page_db_profile(
        slug="clear-page", db_profile="staging", updated_at=_utcnow()
    )

    result = _invoke(tmp_store, ["pages", "assign-profile", "clear-page", "--profile", ""])
    assert result.exit_code == 0, f"Command failed:\n{result.output}"

    row = tmp_store.get_documentation_page(page_id)
    assert row is not None
    assert row["db_profile"] is None, (
        f"Expected db_profile=None after clearing, got {row['db_profile']!r}"
    )
