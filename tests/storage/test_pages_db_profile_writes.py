"""Tests for db_profile propagation through the pages CRUD layer.

Verifies that create/update operations correctly store and preserve the
db_profile field and that the update_documentation_page_db_profile helper
works as expected.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from amx.storage.sqlite_store import SQLiteHistoryStore


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@pytest.fixture
def store(tmp_path: Path) -> SQLiteHistoryStore:
    db = SQLiteHistoryStore(tmp_path / "history.db")
    db.init()
    return db


def _create_page(
    store: SQLiteHistoryStore,
    *,
    slug: str = "test-page",
    db_profile: str | None = None,
) -> str:
    page_id = "aaaaaaaa-0000-0000-0000-000000000001"
    now = _utcnow()
    store.create_documentation_page(
        page_id=page_id,
        title="Test Page",
        slug=slug,
        markdown_body="",
        rendered_html=None,
        status="draft",
        created_at=now,
        updated_at=now,
        created_by="test_user",
        generation_prompt="describe the db",
        model_used=None,
        db_profile=db_profile,
    )
    return page_id


def test_create_page_with_db_profile_round_trip(store: SQLiteHistoryStore) -> None:
    """A page created with db_profile='prod' must return that value on read."""
    page_id = _create_page(store, db_profile="prod")
    row = store.get_documentation_page(page_id)
    assert row is not None, "Page must exist after creation"
    assert row["db_profile"] == "prod", (
        f"Expected db_profile='prod', got {row['db_profile']!r}"
    )


def test_create_page_without_db_profile_remains_null(store: SQLiteHistoryStore) -> None:
    """A page created without db_profile must have db_profile=NULL."""
    page_id = _create_page(store, db_profile=None)
    row = store.get_documentation_page(page_id)
    assert row is not None, "Page must exist after creation"
    assert row["db_profile"] is None, (
        f"Expected db_profile=None (NULL), got {row['db_profile']!r}"
    )


def test_update_page_preserves_db_profile(store: SQLiteHistoryStore) -> None:
    """Updating the page body must not clobber the db_profile field."""
    page_id = _create_page(store, slug="preserve-test", db_profile="staging")
    now = _utcnow()
    store.update_documentation_page_body(
        page_id,
        markdown_body="updated content",
        rendered_html=None,
        updated_at=now,
    )
    row = store.get_documentation_page(page_id)
    assert row is not None
    assert row["db_profile"] == "staging", (
        f"db_profile must survive a body update, got {row['db_profile']!r}"
    )


def test_update_db_profile_by_slug_returns_true_on_match(store: SQLiteHistoryStore) -> None:
    """update_documentation_page_db_profile returns True when slug matches."""
    _create_page(store, slug="slug-match")
    result = store.update_documentation_page_db_profile(
        slug="slug-match",
        db_profile="analytics",
        updated_at=_utcnow(),
    )
    assert result is True, "Expected True when a row was updated"


def test_update_db_profile_by_slug_returns_false_on_miss(store: SQLiteHistoryStore) -> None:
    """update_documentation_page_db_profile returns False for unknown slug."""
    result = store.update_documentation_page_db_profile(
        slug="does-not-exist",
        db_profile="analytics",
        updated_at=_utcnow(),
    )
    assert result is False, "Expected False when no row matched the slug"


def test_update_db_profile_clear_to_none(store: SQLiteHistoryStore) -> None:
    """Passing db_profile=None clears the field (marks page as unscoped)."""
    page_id = _create_page(store, slug="clear-test", db_profile="old_profile")
    store.update_documentation_page_db_profile(
        slug="clear-test",
        db_profile=None,
        updated_at=_utcnow(),
    )
    row = store.get_documentation_page(page_id)
    assert row is not None
    assert row["db_profile"] is None, (
        f"db_profile should be cleared to NULL, got {row['db_profile']!r}"
    )
