"""Anchor-based pages retrieval for the ASK pipeline.

Returns published documentation pages anchored to one of the supplied
asset refs (table-/column-level), with a short keyword-relevant excerpt
of the markdown body. No semantic index — a lightweight BM25-lite score
selects the best paragraph for each page.
"""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass, field
from typing import Iterable

from amx.storage.sqlite_store import SQLiteHistoryStore


_ANCHORED_KINDS = ("db_table", "db_column")
_WORD_RE = re.compile(r"[A-Za-z0-9_]+")


@dataclass(slots=True)
class PageItem:
    page_id: str
    slug: str
    title: str
    excerpt: str


@dataclass(slots=True)
class PagesEvidence:
    items: list[PageItem] = field(default_factory=list)

    @property
    def is_empty(self) -> bool:
        return not self.items


def build_pages_evidence(
    *,
    store: SQLiteHistoryStore,
    asset_refs: Iterable[str],
    question_terms: Iterable[str],
    max_pages: int = 3,
    max_excerpt_chars: int = 400,
    enabled: bool = True,
) -> PagesEvidence:
    """Find published pages anchored to ``asset_refs``, scored by keyword
    overlap with ``question_terms``. Falls back to update-time ordering
    when there are no usable question terms."""
    if not enabled:
        return PagesEvidence()
    refs = [r for r in asset_refs if r]
    if not refs:
        return PagesEvidence()
    terms = [t.lower() for t in question_terms if t and len(t) > 2]
    placeholders = ",".join("?" for _ in refs)
    kind_placeholders = ",".join("?" for _ in _ANCHORED_KINDS)
    with store._connect() as conn:  # noqa: SLF001
        rows = conn.execute(
            f"""
            SELECT DISTINCT p.id, p.slug, p.title, p.markdown_body
            FROM documentation_pages p
            JOIN documentation_page_assets a ON a.page_id = p.id
            WHERE p.status = 'published'
              AND a.asset_kind IN ({kind_placeholders})
              AND a.asset_ref IN ({placeholders})
            ORDER BY p.updated_at DESC
            """,
            (*_ANCHORED_KINDS, *refs),
        ).fetchall()
    scored: list[tuple[float, PageItem]] = []
    for pid, slug, title, body in rows:
        excerpt = _best_excerpt(str(body or ""), terms, max_excerpt_chars)
        score = _bm25_lite_score(str(body or ""), terms)
        scored.append(
            (
                score,
                PageItem(
                    page_id=str(pid),
                    slug=str(slug),
                    title=str(title),
                    excerpt=excerpt,
                ),
            )
        )
    scored.sort(key=lambda pair: pair[0], reverse=True)
    return PagesEvidence(items=[item for _, item in scored[:max_pages]])


def _bm25_lite_score(body: str, terms: list[str]) -> float:
    if not terms:
        return 0.0
    tokens = [m.group(0).lower() for m in _WORD_RE.finditer(body)]
    if not tokens:
        return 0.0
    counts = Counter(tokens)
    total = len(tokens)
    score = 0.0
    for term in terms:
        tf = counts.get(term, 0)
        if tf:
            score += tf / (tf + 1.5 * (total / 500))
    return score


def _best_excerpt(body: str, terms: list[str], cap: int) -> str:
    if not body:
        return ""
    body = body.strip()
    if len(body) <= cap:
        return body
    if not terms:
        return body[:cap].rstrip() + "…"
    paragraphs = [p.strip() for p in re.split(r"\n\s*\n", body) if p.strip()]
    if not paragraphs:
        return body[:cap].rstrip() + "…"
    best = max(paragraphs, key=lambda p: sum(p.lower().count(t) for t in terms))
    if len(best) <= cap:
        return best
    return best[:cap].rstrip() + "…"
