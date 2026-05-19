"""Persistence facade for documentation pages.

Wraps :class:`amx.storage.sqlite_store.SQLiteHistoryStore` behind a
small class the service layer uses; keeps SQLite concerns out of
``service.py`` and out of the FastAPI router.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterable
from datetime import datetime
from pathlib import Path
from typing import Any

from amx.pages.types import AssetRef, SourceRef
from amx.storage.sqlite_store import SQLiteHistoryStore


class PageStore:
    """Pythonic CRUD over the documentation_pages* tables."""

    def __init__(
        self,
        *,
        history: SQLiteHistoryStore | None = None,
        db_path: str | Path | None = None,
    ) -> None:
        if history is None:
            if db_path is None:
                raise ValueError("PageStore needs either a history store or a db_path")
            history = SQLiteHistoryStore(Path(db_path))
        self._history = history

    @property
    def history(self) -> SQLiteHistoryStore:
        return self._history

    def init_schema(self) -> None:
        self._history.init()

    def create(
        self,
        *,
        title: str,
        slug: str,
        intent: str,
        assets: Iterable[AssetRef],
        sources: Iterable[SourceRef],
        created_by: str | None,
        now: datetime,
    ) -> str:
        pid = str(uuid.uuid4())
        self._history.create_documentation_page(
            page_id=pid,
            title=title,
            slug=slug,
            markdown_body="",
            rendered_html=None,
            status="draft",
            created_at=now,
            updated_at=now,
            created_by=created_by,
            generation_prompt=intent,
            model_used=None,
        )
        for a in assets:
            self._history.attach_documentation_page_asset(pid, asset_kind=a.kind, asset_ref=a.ref)
        for s in sources:
            self._history.attach_documentation_page_source(
                pid,
                source_kind=s.kind,
                source_path=s.path,
                original_name=s.original_name,
                created_at=now,
            )
        return pid

    def get(self, page_id: str) -> dict[str, Any] | None:
        row = self._history.get_documentation_page(page_id)
        if row is None:
            return None
        row["assets"] = self._history.list_documentation_page_assets(page_id)
        row["sources"] = self._history.list_documentation_page_sources(page_id)
        row["versions"] = self._history.list_documentation_page_versions(page_id)
        return row

    def list_active(self) -> list[dict[str, Any]]:
        return [r for r in self._history.list_documentation_pages() if r["status"] != "deleted"]

    def update_body(
        self,
        page_id: str,
        *,
        markdown_body: str,
        rendered_html: str | None,
        now: datetime,
        saved_by: str | None,
        note: str | None,
    ) -> int:
        self._history.update_documentation_page_body(
            page_id,
            markdown_body=markdown_body,
            rendered_html=rendered_html,
            updated_at=now,
        )
        return self._history.append_documentation_page_version(
            page_id,
            markdown_body=markdown_body,
            saved_at=now,
            saved_by=saved_by,
            note=note,
        )

    def soft_delete(self, page_id: str, *, now: datetime) -> None:
        self._history.soft_delete_documentation_page(page_id, updated_at=now)
