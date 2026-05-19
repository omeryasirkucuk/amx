"""Public orchestration API for the pages feature.

Both FastAPI routes and CLI commands call methods here; everything
below is infrastructure (``store``, ``context``, ``composer``,
``exporters``) and everything above is transport (FastAPI + Click).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from amx.pages import composer, exporters
from amx.pages import context as ctx_mod
from amx.pages.store import PageStore
from amx.pages.types import AssetRef, SourceRef

ExportFmt = Literal["md", "pdf"]

_SLUG_RE = re.compile(r"[^a-z0-9]+")


def _slugify(title: str) -> str:
    return _SLUG_RE.sub("-", title.lower()).strip("-") or "page"


@dataclass
class PagesService:
    store: PageStore
    llm: composer.LLMClient
    resolver: ctx_mod.Resolver
    model_name: str

    def create_draft(
        self,
        *,
        title: str,
        intent: str,
        assets: list[AssetRef],
        sources: list[SourceRef],
        created_by: str | None,
        now: datetime,
    ) -> str:
        return self.store.create(
            title=title,
            slug=_slugify(title),
            intent=intent,
            assets=assets,
            sources=sources,
            created_by=created_by,
            now=now,
        )

    def generate(self, page_id: str, *, now: datetime) -> None:
        page = self.store.get(page_id)
        if page is None:
            raise KeyError(page_id)
        assets = [AssetRef(a["kind"], a["ref"]) for a in page["assets"] if a.get("included", 1)]
        sources = [SourceRef(s["kind"], s["path"], s["original_name"]) for s in page["sources"]]
        ctx = ctx_mod.gather(
            intent=page["generation_prompt"] or "",
            assets=assets,
            sources=sources,
            resolver=self.resolver,
        )
        body, _model = composer.compose(ctx, llm=self.llm, model_name=self.model_name)
        self.store.update_body(
            page_id,
            markdown_body=body,
            rendered_html=None,
            now=now,
            saved_by=page.get("created_by"),
            note="generated",
        )

    def save_revision(
        self,
        page_id: str,
        *,
        markdown_body: str,
        now: datetime,
        saved_by: str | None,
        note: str | None,
    ) -> None:
        self.store.update_body(
            page_id,
            markdown_body=markdown_body,
            rendered_html=None,
            now=now,
            saved_by=saved_by,
            note=note,
        )

    def export(self, page_id: str, fmt: ExportFmt) -> bytes | str:
        page = self.store.get(page_id)
        if page is None:
            raise KeyError(page_id)
        body = page["markdown_body"]
        if fmt == "md":
            return exporters.to_markdown(body)
        return exporters.to_pdf(body)

    def soft_delete(self, page_id: str, *, now: datetime) -> None:
        self.store.soft_delete(page_id, now=now)
