"""Documentation-pages CRUD methods extracted from sqlite_store.

Wraps single SQL statements for every mutation/read used by the
:mod:`amx.pages.store` facade. Functions take the
:class:`SQLiteHistoryStore` as ``hs`` and use its ``_connect()`` for
locking and PRAGMA setup; DDL stays inside ``SQLiteHistoryStore.init()``.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    from amx.storage.sqlite_store import SQLiteHistoryStore


def _row_to_dict(row: Any) -> dict[str, Any]:
    return dict(row)


def create_documentation_page(
    hs: SQLiteHistoryStore,
    *,
    page_id: str,
    title: str,
    slug: str,
    markdown_body: str,
    rendered_html: str | None,
    status: str,
    created_at: datetime,
    updated_at: datetime,
    created_by: str | None,
    generation_prompt: str | None,
    model_used: str | None,
) -> None:
    with hs._connect() as conn:
        conn.execute(
            """
            INSERT INTO documentation_pages (
                id, title, slug, markdown_body, rendered_html, status,
                created_at, updated_at, created_by, generation_prompt, model_used
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                page_id,
                title,
                slug,
                markdown_body,
                rendered_html,
                status,
                created_at.isoformat(),
                updated_at.isoformat(),
                created_by,
                generation_prompt,
                model_used,
            ),
        )


def get_documentation_page(hs: SQLiteHistoryStore, page_id: str) -> dict[str, Any] | None:
    with hs._connect() as conn:
        row = conn.execute("SELECT * FROM documentation_pages WHERE id = ?", (page_id,)).fetchone()
    return _row_to_dict(row) if row is not None else None


def list_documentation_pages(
    hs: SQLiteHistoryStore, *, status: str | None = None
) -> list[dict[str, Any]]:
    with hs._connect() as conn:
        if status is None:
            rows = conn.execute(
                "SELECT * FROM documentation_pages ORDER BY updated_at DESC"
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM documentation_pages WHERE status = ? ORDER BY updated_at DESC",
                (status,),
            ).fetchall()
    return [_row_to_dict(r) for r in rows]


def update_documentation_page_body(
    hs: SQLiteHistoryStore,
    page_id: str,
    *,
    markdown_body: str,
    rendered_html: str | None,
    updated_at: datetime,
) -> None:
    with hs._connect() as conn:
        conn.execute(
            """
            UPDATE documentation_pages
               SET markdown_body = ?, rendered_html = ?, updated_at = ?
             WHERE id = ?
            """,
            (markdown_body, rendered_html, updated_at.isoformat(), page_id),
        )


def soft_delete_documentation_page(
    hs: SQLiteHistoryStore, page_id: str, *, updated_at: datetime
) -> None:
    with hs._connect() as conn:
        conn.execute(
            "UPDATE documentation_pages SET status = 'deleted', updated_at = ? WHERE id = ?",
            (updated_at.isoformat(), page_id),
        )


def append_documentation_page_version(
    hs: SQLiteHistoryStore,
    page_id: str,
    *,
    markdown_body: str,
    saved_at: datetime,
    saved_by: str | None,
    note: str | None,
) -> int:
    with hs._connect() as conn:
        next_no = int(
            conn.execute(
                "SELECT COALESCE(MAX(version_no), 0) + 1 "
                "FROM documentation_page_versions WHERE page_id = ?",
                (page_id,),
            ).fetchone()[0]
        )
        conn.execute(
            """
            INSERT INTO documentation_page_versions (
                page_id, version_no, markdown_body, saved_at, saved_by, note
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (page_id, next_no, markdown_body, saved_at.isoformat(), saved_by, note),
        )
    return next_no


def attach_documentation_page_asset(
    hs: SQLiteHistoryStore,
    page_id: str,
    *,
    asset_kind: str,
    asset_ref: str,
    included: bool = True,
) -> None:
    with hs._connect() as conn:
        conn.execute(
            """
            INSERT INTO documentation_page_assets (page_id, asset_kind, asset_ref, included)
            VALUES (?, ?, ?, ?)
            """,
            (page_id, asset_kind, asset_ref, 1 if included else 0),
        )


def attach_documentation_page_source(
    hs: SQLiteHistoryStore,
    page_id: str,
    *,
    source_kind: str,
    source_path: str,
    original_name: str,
    created_at: datetime,
) -> None:
    with hs._connect() as conn:
        conn.execute(
            """
            INSERT INTO documentation_page_sources (
                page_id, source_kind, source_path, original_name, created_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (page_id, source_kind, source_path, original_name, created_at.isoformat()),
        )


def list_documentation_page_assets(hs: SQLiteHistoryStore, page_id: str) -> list[dict[str, Any]]:
    with hs._connect() as conn:
        rows = conn.execute(
            "SELECT * FROM documentation_page_assets WHERE page_id = ? ORDER BY id",
            (page_id,),
        ).fetchall()
    return [_row_to_dict(r) for r in rows]


def list_documentation_page_sources(hs: SQLiteHistoryStore, page_id: str) -> list[dict[str, Any]]:
    with hs._connect() as conn:
        rows = conn.execute(
            "SELECT * FROM documentation_page_sources WHERE page_id = ? ORDER BY id",
            (page_id,),
        ).fetchall()
    return [_row_to_dict(r) for r in rows]


def list_documentation_page_versions(hs: SQLiteHistoryStore, page_id: str) -> list[dict[str, Any]]:
    with hs._connect() as conn:
        rows = conn.execute(
            "SELECT * FROM documentation_page_versions WHERE page_id = ? ORDER BY version_no DESC",
            (page_id,),
        ).fetchall()
    return [_row_to_dict(r) for r in rows]
