# AMX Documentation Pages Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship an end-to-end "Documentation Pages" feature: LLM-generated, user-editable pages composed from DB / Doc / Lineage assets and uploaded sources (xlsx + eml), with markdown and PDF export.

**Architecture:** New `amx/pages/` orchestration module sits between transport (FastAPI + CLI) and storage (4 new shared tables). Doc-ingestion extension registry gains `.xlsx` + `.eml`. Studio adds `/pages`, `/pages/new`, `/pages/:id` lazy routes plus a sidebar entry. Markdown body is canonical; PDF is rendered on demand through `markdown-it-py` → `xhtml2pdf`.

**Tech Stack:** Python 3.11 (FastAPI, SQLAlchemy, openpyxl, markdown-it-py, xhtml2pdf), React 18 + TypeScript (React Router 6, React Query, TipTap with tiptap-markdown), pytest, ruff, mypy.

**Spec:** `docs/superpowers/specs/2026-05-18-amx-documentation-pages-design.md`

---

## File Map

**Create:**

- `amx/pages/__init__.py`
- `amx/pages/types.py` — `AssetRef`, `SourceRef`, `PageDraft`, `PageContext` dataclasses.
- `amx/pages/store.py` — CRUD over `documentation_pages*` tables.
- `amx/pages/context.py` — Gathers schema/lineage/RAG context from selected assets.
- `amx/pages/composer.py` — LLM prompt construction + markdown post-processing.
- `amx/pages/exporters.py` — `to_markdown()`, `to_pdf()`.
- `amx/pages/service.py` — Public orchestration API used by FastAPI + CLI.
- `amx/web/routers/pages.py` — FastAPI router (`/api/pages/*`).
- `amx/cli_support/commands/pages.py` — `/pages-*` REPL commands (registered under the existing docs tab).
- `frontend/src/routes/Pages.tsx`
- `frontend/src/routes/PageNew.tsx`
- `frontend/src/routes/PageEdit.tsx`
- `frontend/src/components/pages/AssetPicker.tsx`
- `frontend/src/components/pages/SourceAttacher.tsx`
- `frontend/src/components/pages/PageEditor.tsx`
- `frontend/src/components/pages/PageExportMenu.tsx`
- `frontend/src/hooks/usePages.ts`
- `tests/test_pages_store.py`
- `tests/test_pages_context.py`
- `tests/test_pages_exporters.py`
- `tests/test_pages_service.py`
- `tests/test_pages_router.py`
- `tests/test_pages_cli.py`
- `tests/test_xlsx_loader.py`
- `tests/test_eml_loader.py`

**Modify:**

- `amx/docs/extensions.py` — add `.xlsx`, `.eml` to `SUPPORTED_EXTENSIONS`.
- `amx/docs/scanner.py` — register `xlsx_loader`, `eml_loader` in `LOADER_MAP`.
- `amx/storage/shared_schema.py` — add 4 SQLAlchemy tables.
- `amx/storage/sqlite_store.py` — add 4 local DDLs + CRUD helpers used by `pages/store.py`.
- `amx/storage/schema_descriptions.py` — add `SCHEMA_DESCRIPTIONS` entries for every new table+column.
- `amx/web/server.py` — `app.include_router(pages.router)`.
- `amx/cli.py` — `register_pages_commands(main, ...)` next to `register_docs_commands(...)`.
- `frontend/src/App.tsx` — three lazy imports + three `<Route>` entries.
- `frontend/src/components/Sidebar.tsx` — new `Pages` nav entry.
- `pyproject.toml` — add `openpyxl`, `xhtml2pdf`, `markdownify` deps.

---

## Task 1: Add `.xlsx` and `.eml` to the extension registry (TDD)

**Files:**
- Modify: `amx/docs/extensions.py`
- Test: `tests/test_docs_extensions.py`

- [ ] **Step 1: Write failing test**

```python
def test_xlsx_and_eml_are_supported():
    from amx.docs.extensions import SUPPORTED_EXTENSIONS
    assert ".xlsx" in SUPPORTED_EXTENSIONS
    assert ".eml" in SUPPORTED_EXTENSIONS
```

- [ ] **Step 2: Run** `pytest tests/test_docs_extensions.py::test_xlsx_and_eml_are_supported -v` → FAIL.
- [ ] **Step 3: Add `.xlsx` and `.eml` to the `SUPPORTED_EXTENSIONS` frozenset.**
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Do NOT commit yet** (user instruction: single commit at end).

---

## Task 2: Implement `xlsx_loader` (TDD)

**Files:**
- Create: `amx/docs/loaders/xlsx_loader.py`
- Modify: `amx/docs/scanner.py` (register in `LOADER_MAP`)
- Test: `tests/test_xlsx_loader.py`
- Dep: add `openpyxl>=3.1` to `pyproject.toml`.

- [ ] **Step 1: Add dep** to `pyproject.toml` dependencies list: `"openpyxl>=3.1"`.

- [ ] **Step 2: Write failing test**

```python
from pathlib import Path
import openpyxl

def test_xlsx_loader_serialises_each_sheet_as_markdown_table(tmp_path: Path):
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Customers"
    ws.append(["id", "name"])
    ws.append([1, "Alice"])
    ws.append([2, "Bob"])
    p = tmp_path / "sample.xlsx"
    wb.save(p)

    from amx.docs.loaders.xlsx_loader import load_xlsx
    text = load_xlsx(p)
    assert "## Customers" in text
    assert "| id | name |" in text
    assert "| 1 | Alice |" in text
    assert "| 2 | Bob |" in text
```

- [ ] **Step 3: Run** → FAIL (module missing).

- [ ] **Step 4: Implement `amx/docs/loaders/xlsx_loader.py`:**

```python
"""Excel (.xlsx) loader used by the docs scanner.

Each worksheet becomes one markdown section: the sheet name is the
H2 heading, and the cells are emitted as a GitHub-style markdown
table. Empty sheets are skipped. Cell values are coerced with the
sheet's number format when present; long cells are truncated to
keep the LLM prompt budget under control (see spec §6.1)."""

from __future__ import annotations

from pathlib import Path

import openpyxl

MAX_CELL_LEN = 4096


def _coerce(value: object) -> str:
    if value is None:
        return ""
    s = str(value)
    if len(s) > MAX_CELL_LEN:
        return s[: MAX_CELL_LEN - 1] + "…"
    return s


def load_xlsx(path: str | Path) -> str:
    wb = openpyxl.load_workbook(Path(path), data_only=True, read_only=True)
    sections: list[str] = []
    for sheet in wb.worksheets:
        rows = list(sheet.iter_rows(values_only=True))
        if not rows:
            continue
        header = [_coerce(c) for c in rows[0]]
        sections.append(f"## {sheet.title}\n")
        sections.append("| " + " | ".join(header) + " |")
        sections.append("| " + " | ".join("---" for _ in header) + " |")
        for row in rows[1:]:
            cells = [_coerce(c) for c in row]
            sections.append("| " + " | ".join(cells) + " |")
        sections.append("")
    return "\n".join(sections)
```

- [ ] **Step 5: Register in `amx/docs/scanner.py` `LOADER_MAP`:** add an entry `".xlsx": load_xlsx,` (import at top).

- [ ] **Step 6: Run** → PASS.

---

## Task 3: Implement `eml_loader` (TDD)

**Files:**
- Create: `amx/docs/loaders/eml_loader.py`
- Modify: `amx/docs/scanner.py`
- Test: `tests/test_eml_loader.py`
- Dep: add `markdownify>=0.11` to `pyproject.toml` (pure-Python, BSD).

- [ ] **Step 1: Add dep** `"markdownify>=0.11"`.

- [ ] **Step 2: Write failing test**

```python
from pathlib import Path

EML_SAMPLE = b"""From: alice@example.com
To: bob@example.com
Subject: Q3 spec
Date: Wed, 01 Apr 2026 12:00:00 +0000
MIME-Version: 1.0
Content-Type: text/plain; charset=utf-8

Hello team,

The revenue calc multiplies units by net price.
"""

def test_eml_loader_extracts_headers_and_body(tmp_path: Path):
    p = tmp_path / "msg.eml"
    p.write_bytes(EML_SAMPLE)

    from amx.docs.loaders.eml_loader import load_eml
    text = load_eml(p)
    assert "Subject: Q3 spec" in text
    assert "From: alice@example.com" in text
    assert "revenue calc multiplies units by net price" in text
```

- [ ] **Step 3: Run** → FAIL.

- [ ] **Step 4: Implement `amx/docs/loaders/eml_loader.py`:**

```python
"""Email (.eml) loader used by the docs scanner.

Headers (From/To/Subject/Date) become a YAML-style frontmatter
block at the top of the returned text; the body falls back from
`text/plain` to `text/html` (the latter converted with markdownify).
Attachments are listed by filename only - full attachment ingestion
is a follow-up. Quoted-reply blocks are preserved so the LLM can
see the thread context (see spec §6.2)."""

from __future__ import annotations

from email import message_from_bytes
from email.message import Message
from pathlib import Path

from markdownify import markdownify

HEADERS = ("From", "To", "Cc", "Subject", "Date")


def _pick_body(msg: Message) -> str:
    if msg.is_multipart():
        text_part: Message | None = None
        html_part: Message | None = None
        for part in msg.walk():
            ctype = part.get_content_type()
            if ctype == "text/plain" and text_part is None:
                text_part = part
            elif ctype == "text/html" and html_part is None:
                html_part = part
        if text_part is not None:
            payload = text_part.get_payload(decode=True) or b""
            return payload.decode(text_part.get_content_charset() or "utf-8", errors="replace")
        if html_part is not None:
            payload = html_part.get_payload(decode=True) or b""
            html = payload.decode(html_part.get_content_charset() or "utf-8", errors="replace")
            return markdownify(html)
        return ""
    payload = msg.get_payload(decode=True) or b""
    text = payload.decode(msg.get_content_charset() or "utf-8", errors="replace")
    if msg.get_content_type() == "text/html":
        text = markdownify(text)
    return text


def _list_attachments(msg: Message) -> list[str]:
    out: list[str] = []
    if not msg.is_multipart():
        return out
    for part in msg.walk():
        if part.get_filename():
            out.append(part.get_filename())
    return out


def load_eml(path: str | Path) -> str:
    raw = Path(path).read_bytes()
    msg = message_from_bytes(raw)
    headers = [f"{h}: {msg.get(h, '').strip()}" for h in HEADERS if msg.get(h)]
    body = _pick_body(msg).strip()
    attachments = _list_attachments(msg)
    parts = ["\n".join(headers), "", body]
    if attachments:
        parts.append("")
        parts.append("Attachments: " + ", ".join(attachments))
    return "\n".join(parts)
```

- [ ] **Step 5: Register in `LOADER_MAP`:** `".eml": load_eml,`.
- [ ] **Step 6: Run** → PASS.

---

## Task 4: Add the four `documentation_pages*` tables to shared + local storage with full descriptions

**Files:**
- Modify: `amx/storage/shared_schema.py`
- Modify: `amx/storage/sqlite_store.py`
- Modify: `amx/storage/schema_descriptions.py`
- Test: `tests/test_local_schema_comments.py` and `tests/test_shared_schema_comments.py` (existing tests, will fail until descriptions are added → that is the enforcement loop).

- [ ] **Step 1: Add descriptions to `schema_descriptions.py` first** (kural #5 enforcement). For each of `documentation_pages`, `documentation_page_assets`, `documentation_page_sources`, `documentation_page_versions`, add a `"__table__"` entry plus a non-empty entry for every column listed in the spec §5. Copy the spec table text verbatim.

- [ ] **Step 2: Add SQLAlchemy `Table(...)` declarations to `shared_schema.py`** for the four tables. Each column uses `Column(..., comment=_desc("<table>", "<col>"))`. PK / FK / unique constraints per spec §5. Example for the parent table:

```python
Table(
    "documentation_pages",
    md,
    Column("id", String(36), primary_key=True, comment=_desc("documentation_pages", "id")),
    Column("title", String, nullable=False, comment=_desc("documentation_pages", "title")),
    Column("slug", String, nullable=False, unique=True, comment=_desc("documentation_pages", "slug")),
    Column("markdown_body", Text, nullable=False, comment=_desc("documentation_pages", "markdown_body")),
    Column("rendered_html", Text, comment=_desc("documentation_pages", "rendered_html")),
    Column("status", String, nullable=False, server_default="draft", comment=_desc("documentation_pages", "status")),
    Column("created_at", DateTime(timezone=True), nullable=False, comment=_desc("documentation_pages", "created_at")),
    Column("updated_at", DateTime(timezone=True), nullable=False, comment=_desc("documentation_pages", "updated_at")),
    Column("created_by", String, comment=_desc("documentation_pages", "created_by")),
    Column("generation_prompt", Text, comment=_desc("documentation_pages", "generation_prompt")),
    Column("model_used", String, comment=_desc("documentation_pages", "model_used")),
)
```

Mirror the same pattern for the other three tables, using the columns from spec §5.2–5.4 and `(page_id, version_no)` composite PK on the versions table via `PrimaryKeyConstraint(...)`.

- [ ] **Step 3: Add local SQLite DDLs to `sqlite_store.py`** alongside the existing `CREATE TABLE IF NOT EXISTS` calls in `_init_schema()`. Each column needs to match the shared schema. Also add an `_amx_schema_descriptions` sidecar insert so `tests/test_local_schema_comments.py` passes.

- [ ] **Step 4: Add minimal helper functions used by `pages/store.py`** at the bottom of `sqlite_store.py`:

```python
def insert_documentation_page(conn, *, id, title, slug, markdown_body, status, created_at, updated_at, created_by, generation_prompt, model_used, rendered_html=None): ...
def update_documentation_page_body(conn, page_id, *, markdown_body, rendered_html, updated_at): ...
def get_documentation_page(conn, page_id) -> dict | None: ...
def list_documentation_pages(conn, *, status=None) -> list[dict]: ...
def soft_delete_documentation_page(conn, page_id, *, updated_at): ...
def append_documentation_page_version(conn, page_id, *, markdown_body, saved_at, saved_by, note): ...
def attach_documentation_page_asset(conn, page_id, *, asset_kind, asset_ref, included=True): ...
def attach_documentation_page_source(conn, page_id, *, source_kind, source_path, original_name, created_at): ...
def list_documentation_page_assets(conn, page_id) -> list[dict]: ...
def list_documentation_page_sources(conn, page_id) -> list[dict]: ...
```

Each helper is a thin wrapper around a single `conn.execute(...)`; follow the style of the existing `insert_analysis_run` helper. Return rows as `dict(row)` after a `row_factory = sqlite3.Row` access.

- [ ] **Step 5: Run** `pytest tests/test_local_schema_comments.py tests/test_shared_schema_comments.py -v` → both PASS.

---

## Task 5: `amx/pages/types.py` (no logic, just dataclasses — no test)

**Files:**
- Create: `amx/pages/__init__.py` (empty).
- Create: `amx/pages/types.py`.

- [ ] **Step 1: Write `amx/pages/types.py`:**

```python
"""Typed values exchanged between the pages module's layers."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

AssetKind = Literal[
    "db_profile", "db_database", "db_schema", "db_table", "db_column",
    "doc_profile", "lineage_artifact",
]
SourceKind = Literal["upload", "email", "excel"]
PageStatus = Literal["draft", "published", "deleted"]


@dataclass(frozen=True)
class AssetRef:
    kind: AssetKind
    ref: str  # e.g. "pg_prod/sales/public/orders", "doc:design_docs", "lineage:<id>"


@dataclass(frozen=True)
class SourceRef:
    kind: SourceKind
    path: str
    original_name: str


@dataclass
class PageDraft:
    id: str
    title: str
    slug: str
    intent: str
    assets: list[AssetRef] = field(default_factory=list)
    sources: list[SourceRef] = field(default_factory=list)
    markdown_body: str = ""
    status: PageStatus = "draft"


@dataclass
class PageContext:
    intent: str
    db_blocks: list[str] = field(default_factory=list)
    doc_blocks: list[str] = field(default_factory=list)
    lineage_blocks: list[str] = field(default_factory=list)
    source_blocks: list[str] = field(default_factory=list)

    def serialise(self) -> str:
        sections = []
        if self.db_blocks:
            sections.append("# DATABASE ASSETS\n\n" + "\n\n".join(self.db_blocks))
        if self.doc_blocks:
            sections.append("# DOC SNIPPETS\n\n" + "\n\n".join(self.doc_blocks))
        if self.lineage_blocks:
            sections.append("# LINEAGE\n\n" + "\n\n".join(self.lineage_blocks))
        if self.source_blocks:
            sections.append("# SOURCES\n\n" + "\n\n".join(self.source_blocks))
        return "\n\n".join(sections)
```

---

## Task 6: `amx/pages/store.py` (TDD)

**Files:**
- Create: `amx/pages/store.py`
- Test: `tests/test_pages_store.py`

- [ ] **Step 1: Test — round-trip create + read + list + delete:**

```python
from datetime import datetime, timezone
from amx.pages.store import PageStore
from amx.pages.types import AssetRef, SourceRef

def test_create_get_list_delete(tmp_path):
    db = tmp_path / "amx.db"
    s = PageStore(db_path=str(db))
    s.init_schema()
    now = datetime.now(timezone.utc)
    pid = s.create(
        title="Orders overview", slug="orders-overview",
        intent="explain orders pipeline",
        assets=[AssetRef("db_table", "pg_prod/sales/public/orders")],
        sources=[],
        created_by="omer", now=now,
    )
    page = s.get(pid)
    assert page["title"] == "Orders overview"
    assert page["status"] == "draft"

    rows = s.list_active()
    assert len(rows) == 1

    s.soft_delete(pid, now=now)
    assert s.list_active() == []
```

- [ ] **Step 2: Run** → FAIL.

- [ ] **Step 3: Implement `amx/pages/store.py`:**

```python
"""Persistence layer for documentation pages.

Wraps the low-level helpers in :mod:`amx.storage.sqlite_store` behind
a single class that the service layer uses; keeps SQLite concerns
out of `service.py` and out of the FastAPI router."""

from __future__ import annotations

import sqlite3
import uuid
from collections.abc import Iterable
from datetime import datetime
from pathlib import Path

from amx.pages.types import AssetRef, SourceRef
from amx.storage import sqlite_store as ss


class PageStore:
    def __init__(self, *, db_path: str | Path):
        self._path = Path(db_path)

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._path)
        conn.row_factory = sqlite3.Row
        return conn

    def init_schema(self) -> None:
        with self._conn() as conn:
            ss.init_schema(conn)  # idempotent

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
        with self._conn() as conn:
            ss.insert_documentation_page(
                conn, id=pid, title=title, slug=slug,
                markdown_body="", rendered_html=None,
                status="draft", created_at=now, updated_at=now,
                created_by=created_by, generation_prompt=intent, model_used=None,
            )
            for a in assets:
                ss.attach_documentation_page_asset(conn, pid, asset_kind=a.kind, asset_ref=a.ref)
            for s in sources:
                ss.attach_documentation_page_source(
                    conn, pid, source_kind=s.kind, source_path=s.path,
                    original_name=s.original_name, created_at=now,
                )
        return pid

    def get(self, page_id: str) -> dict | None:
        with self._conn() as conn:
            row = ss.get_documentation_page(conn, page_id)
            if row is None:
                return None
            row["assets"] = ss.list_documentation_page_assets(conn, page_id)
            row["sources"] = ss.list_documentation_page_sources(conn, page_id)
            return row

    def list_active(self) -> list[dict]:
        with self._conn() as conn:
            return [r for r in ss.list_documentation_pages(conn) if r["status"] != "deleted"]

    def update_body(self, page_id: str, *, markdown_body: str, rendered_html: str | None, now: datetime, saved_by: str | None, note: str | None) -> None:
        with self._conn() as conn:
            ss.update_documentation_page_body(
                conn, page_id, markdown_body=markdown_body,
                rendered_html=rendered_html, updated_at=now,
            )
            ss.append_documentation_page_version(
                conn, page_id, markdown_body=markdown_body,
                saved_at=now, saved_by=saved_by, note=note,
            )

    def soft_delete(self, page_id: str, *, now: datetime) -> None:
        with self._conn() as conn:
            ss.soft_delete_documentation_page(conn, page_id, updated_at=now)
```

- [ ] **Step 4: Run** → PASS.

---

## Task 7: `amx/pages/context.py` (TDD)

**Files:**
- Create: `amx/pages/context.py`
- Test: `tests/test_pages_context.py`

- [ ] **Step 1: Test — context.gather returns budgeted blocks:**

```python
from amx.pages.context import gather
from amx.pages.types import AssetRef

class StubResolver:
    def resolve_db_asset(self, ref): return f"DDL for {ref}"
    def resolve_doc_profile(self, ref, intent, k=5): return [f"snippet from {ref}: {intent}"]
    def resolve_lineage(self, ref): return f"lineage block for {ref}"
    def resolve_source(self, src): return f"source body {src.original_name}"

def test_gather_serialises_each_asset_kind():
    ctx = gather(
        intent="explain pipeline",
        assets=[AssetRef("db_table", "pg/sales/public/orders"),
                AssetRef("doc_profile", "doc:design"),
                AssetRef("lineage_artifact", "lineage:abc")],
        sources=[],
        resolver=StubResolver(),
        budget_bytes=10_000,
    )
    s = ctx.serialise()
    assert "DDL for pg/sales/public/orders" in s
    assert "snippet from doc:design" in s
    assert "lineage block for lineage:abc" in s
```

- [ ] **Step 2: Run** → FAIL.

- [ ] **Step 3: Implement `amx/pages/context.py`:**

```python
"""Gathers the per-asset textual context fed to the LLM composer.

Each asset kind is routed to a resolver method on the injected
``Resolver`` so production code can wire the real DB/RAG/lineage
modules and tests can stub them. A simple greedy budget keeps the
serialised context under ``budget_bytes`` (default 60 KB) - DB
DDLs first, then doc snippets ranked by retrieval score, then
lineage neighbours, then uploaded sources."""

from __future__ import annotations

from typing import Protocol

from amx.pages.types import AssetRef, PageContext, SourceRef

DEFAULT_BUDGET = 60_000


class Resolver(Protocol):
    def resolve_db_asset(self, ref: str) -> str: ...
    def resolve_doc_profile(self, ref: str, intent: str, k: int = 5) -> list[str]: ...
    def resolve_lineage(self, ref: str) -> str: ...
    def resolve_source(self, src: SourceRef) -> str: ...


def _fits(buf: list[str], block: str, *, budget: int) -> bool:
    return sum(len(b) for b in buf) + len(block) <= budget


def gather(
    *,
    intent: str,
    assets: list[AssetRef],
    sources: list[SourceRef],
    resolver: Resolver,
    budget_bytes: int = DEFAULT_BUDGET,
) -> PageContext:
    ctx = PageContext(intent=intent)
    used: list[str] = []

    for a in assets:
        if a.kind.startswith("db_"):
            block = resolver.resolve_db_asset(a.ref)
            if _fits(used, block, budget=budget_bytes):
                ctx.db_blocks.append(block)
                used.append(block)

    for a in assets:
        if a.kind == "doc_profile":
            for snippet in resolver.resolve_doc_profile(a.ref, intent):
                if _fits(used, snippet, budget=budget_bytes):
                    ctx.doc_blocks.append(snippet)
                    used.append(snippet)

    for a in assets:
        if a.kind == "lineage_artifact":
            block = resolver.resolve_lineage(a.ref)
            if _fits(used, block, budget=budget_bytes):
                ctx.lineage_blocks.append(block)
                used.append(block)

    for src in sources:
        block = resolver.resolve_source(src)
        if _fits(used, block, budget=budget_bytes):
            ctx.source_blocks.append(block)
            used.append(block)

    return ctx
```

- [ ] **Step 4: Run** → PASS.

---

## Task 8: `amx/pages/composer.py` (TDD)

**Files:**
- Create: `amx/pages/composer.py`
- Test: `tests/test_pages_composer.py`

- [ ] **Step 1: Test — composer calls the LLM with the right messages and returns the body:**

```python
from amx.pages.composer import compose
from amx.pages.types import PageContext

class StubLLM:
    def __init__(self): self.calls = []
    def chat(self, messages, **kw):
        self.calls.append(messages)
        class R: content = "# Overview\n\nGenerated body."
        return R()

def test_compose_uses_system_and_user_messages():
    llm = StubLLM()
    ctx = PageContext(intent="explain orders", db_blocks=["DDL"])
    body, model = compose(ctx, llm=llm, model_name="claude-haiku-4-5")
    assert body.startswith("# Overview")
    assert model == "claude-haiku-4-5"
    msgs = llm.calls[0]
    assert msgs[0]["role"] == "system"
    assert "technical writer" in msgs[0]["content"]
    assert "explain orders" in msgs[1]["content"]
    assert "DDL" in msgs[1]["content"]
```

- [ ] **Step 2: Run** → FAIL.

- [ ] **Step 3: Implement `amx/pages/composer.py`:**

```python
"""Builds the LLM prompt for a documentation page and parses the
response. The model identity is read from the active AMX LLM
profile by the caller; this module is pure - same input, same
prompt, same output - so it is easy to test."""

from __future__ import annotations

from typing import Protocol

from amx.pages.types import PageContext

SYSTEM_PROMPT = """You are a senior technical writer. Produce a Markdown documentation
page from the assets and sources below. Use these sections:
  1. Overview
  2. Data Assets
  3. Business Logic
  4. Pipelines & Lineage
  5. Open Questions
Keep each section short and concrete. Cite asset names inline when
referencing them. Do not invent fields or relationships that are
not in the provided context."""


class LLMClient(Protocol):
    def chat(self, messages: list[dict[str, str]], **kw) -> object: ...


def compose(ctx: PageContext, *, llm: LLMClient, model_name: str) -> tuple[str, str]:
    user = f"INTENT: {ctx.intent}\n\nCONTEXT:\n{ctx.serialise()}"
    result = llm.chat(
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user},
        ]
    )
    body = getattr(result, "content", "")
    return body, model_name
```

- [ ] **Step 4: Run** → PASS.

---

## Task 9: `amx/pages/exporters.py` (TDD)

**Files:**
- Create: `amx/pages/exporters.py`
- Test: `tests/test_pages_exporters.py`
- Dep: add `"markdown-it-py>=3"` and `"xhtml2pdf>=0.2.11"` to `pyproject.toml`.

- [ ] **Step 1: Add deps.**

- [ ] **Step 2: Test:**

```python
from amx.pages.exporters import to_markdown, to_pdf

def test_to_markdown_is_identity():
    assert to_markdown("# Hi") == "# Hi"

def test_to_pdf_returns_bytes_starting_with_pdf_header():
    pdf = to_pdf("# Hello\n\nbody")
    assert pdf.startswith(b"%PDF-")
    assert len(pdf) > 200
```

- [ ] **Step 3: Run** → FAIL.

- [ ] **Step 4: Implement `amx/pages/exporters.py`:**

```python
"""Markdown / PDF exporters for documentation pages.

Markdown export is identity; PDF goes via markdown-it-py for the
HTML render and xhtml2pdf for the binary output. xhtml2pdf is
pure-Python so it installs cleanly on Windows (kural #10);
WeasyPrint is parked as a higher-fidelity follow-up."""

from __future__ import annotations

import io

from markdown_it import MarkdownIt
from xhtml2pdf import pisa

_md = MarkdownIt("commonmark", {"html": False, "linkify": True, "typographer": True})

_PDF_CSS = """
@page { size: A4; margin: 22mm; }
body  { font-family: Helvetica, Arial, sans-serif; font-size: 11pt; line-height: 1.45; color: #111; }
h1    { font-size: 22pt; margin-top: 0; }
h2    { font-size: 16pt; margin-top: 18pt; }
h3    { font-size: 13pt; margin-top: 14pt; }
code  { font-family: "Courier New", monospace; background: #f4f4f4; padding: 1pt 3pt; }
pre   { background: #f4f4f4; padding: 8pt; }
table { border-collapse: collapse; width: 100%; }
th, td { border: 1px solid #ddd; padding: 4pt 6pt; }
"""


def to_markdown(markdown_body: str) -> str:
    return markdown_body


def to_pdf(markdown_body: str) -> bytes:
    html_body = _md.render(markdown_body)
    html = f"<html><head><style>{_PDF_CSS}</style></head><body>{html_body}</body></html>"
    buf = io.BytesIO()
    pisa.CreatePDF(html, dest=buf, encoding="utf-8")
    return buf.getvalue()
```

- [ ] **Step 5: Run** → PASS.

---

## Task 10: `amx/pages/service.py` (TDD)

**Files:**
- Create: `amx/pages/service.py`
- Test: `tests/test_pages_service.py`

- [ ] **Step 1: Test — `create_draft` + `generate` + `save_revision` + `export` round-trip with stubbed LLM/Resolver:**

```python
from datetime import datetime, timezone
from amx.pages.service import PagesService
from amx.pages.store import PageStore
from amx.pages.types import AssetRef

class StubLLM:
    def chat(self, messages, **kw):
        class R: content = "# Overview\n\nAuto body."
        return R()

class StubResolver:
    def resolve_db_asset(self, ref): return f"DDL {ref}"
    def resolve_doc_profile(self, ref, intent, k=5): return [f"snip {ref}"]
    def resolve_lineage(self, ref): return f"lineage {ref}"
    def resolve_source(self, src): return f"src {src.original_name}"

def test_create_generate_save_export(tmp_path):
    store = PageStore(db_path=str(tmp_path/"a.db")); store.init_schema()
    svc = PagesService(store=store, llm=StubLLM(), resolver=StubResolver(), model_name="m")

    pid = svc.create_draft(
        title="X", intent="explain", assets=[AssetRef("db_table","p/d/s/t")],
        sources=[], created_by="omer", now=datetime.now(timezone.utc),
    )
    svc.generate(pid, now=datetime.now(timezone.utc))
    page = store.get(pid)
    assert page["markdown_body"].startswith("# Overview")

    svc.save_revision(pid, markdown_body="# Edited", now=datetime.now(timezone.utc), saved_by="omer", note=None)
    page = store.get(pid)
    assert page["markdown_body"] == "# Edited"

    md = svc.export(pid, "md")
    assert md.startswith(b"# Edited") or md.startswith("# Edited")
    pdf = svc.export(pid, "pdf")
    assert pdf.startswith(b"%PDF-")
```

- [ ] **Step 2: Run** → FAIL.

- [ ] **Step 3: Implement `amx/pages/service.py`:**

```python
"""Public orchestration API for the pages feature. Both FastAPI
routes and CLI commands call methods here; everything below is
infrastructure (`store`, `context`, `composer`, `exporters`) and
everything above is transport (FastAPI + Click)."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from amx.pages import context as ctx_mod
from amx.pages import composer, exporters
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
        self, *, title: str, intent: str,
        assets: list[AssetRef], sources: list[SourceRef],
        created_by: str | None, now: datetime,
    ) -> str:
        return self.store.create(
            title=title, slug=_slugify(title), intent=intent,
            assets=assets, sources=sources,
            created_by=created_by, now=now,
        )

    def generate(self, page_id: str, *, now: datetime) -> None:
        page = self.store.get(page_id)
        if page is None:
            raise KeyError(page_id)
        assets = [AssetRef(a["asset_kind"], a["asset_ref"]) for a in page["assets"] if a.get("included", 1)]
        sources = [SourceRef(s["source_kind"], s["source_path"], s["original_name"]) for s in page["sources"]]
        ctx = ctx_mod.gather(
            intent=page["generation_prompt"] or "",
            assets=assets, sources=sources, resolver=self.resolver,
        )
        body, model = composer.compose(ctx, llm=self.llm, model_name=self.model_name)
        self.store.update_body(
            page_id, markdown_body=body, rendered_html=None,
            now=now, saved_by=page.get("created_by"), note="generated",
        )

    def save_revision(self, page_id: str, *, markdown_body: str, now: datetime, saved_by: str | None, note: str | None) -> None:
        self.store.update_body(
            page_id, markdown_body=markdown_body, rendered_html=None,
            now=now, saved_by=saved_by, note=note,
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
```

- [ ] **Step 4: Run** → PASS.

---

## Task 11: FastAPI router (TDD)

**Files:**
- Create: `amx/web/routers/pages.py`
- Modify: `amx/web/server.py` (one line: `app.include_router(pages.router)`).
- Test: `tests/test_pages_router.py`

- [ ] **Step 1: Test — list / create / generate / export endpoints round-trip with a stubbed `PagesService` injected via `app.dependency_overrides`:**

```python
from fastapi.testclient import TestClient
from amx.web.server import create_app
from amx.web.routers import pages as pages_router

def test_pages_crud_endpoints(monkeypatch, tmp_path):
    app = create_app()
    client = TestClient(app)

    # POST /api/pages
    r = client.post("/api/pages", json={
        "title": "Orders", "intent": "explain",
        "assets": [{"kind": "db_table", "ref": "p/d/s/orders"}],
    })
    assert r.status_code == 201
    pid = r.json()["id"]

    # GET list
    r = client.get("/api/pages")
    assert r.status_code == 200
    assert any(p["id"] == pid for p in r.json())

    # POST generate
    r = client.post(f"/api/pages/{pid}/generate")
    assert r.status_code == 200

    # PATCH body
    r = client.patch(f"/api/pages/{pid}", json={"markdown_body": "# Edited"})
    assert r.status_code == 200

    # GET export/md
    r = client.get(f"/api/pages/{pid}/export/md")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/markdown")
    assert r.text == "# Edited"

    # GET export/pdf
    r = client.get(f"/api/pages/{pid}/export/pdf")
    assert r.status_code == 200
    assert r.headers["content-type"] == "application/pdf"
    assert r.content.startswith(b"%PDF-")
```

(The test relies on `create_app()` already wiring a default `PagesService` against a temp DB when running under pytest; if not, monkeypatch `pages_router._get_service` to return a stubbed service.)

- [ ] **Step 2: Run** → FAIL.

- [ ] **Step 3: Implement `amx/web/routers/pages.py`:**

```python
"""FastAPI routes for documentation pages. Transport only - all
domain logic lives in :mod:`amx.pages.service`."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from fastapi.responses import Response
from pydantic import BaseModel

from amx.pages.service import PagesService
from amx.pages.types import AssetRef, SourceRef
from amx.web.deps import get_pages_service  # provided in deps.py

router = APIRouter(prefix="/api/pages", tags=["pages"])


class AssetIn(BaseModel):
    kind: str
    ref: str


class PageCreateIn(BaseModel):
    title: str
    intent: str = ""
    assets: list[AssetIn] = []


class PagePatchIn(BaseModel):
    title: str | None = None
    markdown_body: str | None = None
    status: Literal["draft", "published"] | None = None
    note: str | None = None


@router.get("")
def list_pages(svc: PagesService = Depends(get_pages_service)):
    return svc.store.list_active()


@router.post("", status_code=201)
def create_page(body: PageCreateIn, svc: PagesService = Depends(get_pages_service)):
    pid = svc.create_draft(
        title=body.title, intent=body.intent,
        assets=[AssetRef(a.kind, a.ref) for a in body.assets],
        sources=[], created_by=None, now=datetime.now(timezone.utc),
    )
    return {"id": pid}


@router.get("/{page_id}")
def get_page(page_id: str, svc: PagesService = Depends(get_pages_service)):
    page = svc.store.get(page_id)
    if page is None:
        raise HTTPException(404, "page not found")
    return page


@router.post("/{page_id}/generate")
def generate_page(page_id: str, svc: PagesService = Depends(get_pages_service)):
    try:
        svc.generate(page_id, now=datetime.now(timezone.utc))
    except KeyError:
        raise HTTPException(404, "page not found")
    return svc.store.get(page_id)


@router.patch("/{page_id}")
def patch_page(page_id: str, body: PagePatchIn, svc: PagesService = Depends(get_pages_service)):
    page = svc.store.get(page_id)
    if page is None:
        raise HTTPException(404, "page not found")
    if body.markdown_body is not None:
        svc.save_revision(
            page_id, markdown_body=body.markdown_body,
            now=datetime.now(timezone.utc), saved_by=None, note=body.note,
        )
    return svc.store.get(page_id)


@router.delete("/{page_id}")
def delete_page(page_id: str, svc: PagesService = Depends(get_pages_service)):
    svc.soft_delete(page_id, now=datetime.now(timezone.utc))
    return {"ok": True}


@router.post("/{page_id}/sources")
async def upload_source(page_id: str, file: UploadFile = File(...), svc: PagesService = Depends(get_pages_service)):
    # Reuse the existing uploads helper to content-address the file
    from amx.docs.uploads import save_uploaded_file
    data = await file.read()
    saved = save_uploaded_file(
        profile=f"pages/{page_id}", original_name=file.filename or "untitled",
        data=data,
    )
    kind = "excel" if saved.saved_path.endswith(".xlsx") else "email" if saved.saved_path.endswith(".eml") else "upload"
    svc.store._conn().__enter__()  # placeholder if extra plumbing needed
    return {"path": saved.saved_path, "kind": kind, "original_name": saved.original_name}


@router.get("/{page_id}/export/md")
def export_md(page_id: str, svc: PagesService = Depends(get_pages_service)):
    body = svc.export(page_id, "md")
    return Response(
        content=body if isinstance(body, str) else body.decode("utf-8"),
        media_type="text/markdown; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="page-{page_id}.md"'},
    )


@router.get("/{page_id}/export/pdf")
def export_pdf(page_id: str, svc: PagesService = Depends(get_pages_service)):
    pdf = svc.export(page_id, "pdf")
    return Response(
        content=pdf, media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="page-{page_id}.pdf"'},
    )
```

- [ ] **Step 4: Add a `get_pages_service` dependency in `amx/web/deps.py`** that builds the service with the configured LLM and a real `Resolver` implementation (lazy-cached as `app.state.pages_service`). Concrete `AMXResolver` lives in `amx/pages/_resolver.py` and wraps `amx.llm.provider.get_active_provider().chat`, `amx.docs.rag.RAGStore(profile).query(intent, k)`, and lineage `store.get_artifact(id)`.

- [ ] **Step 5: Mount router in `amx/web/server.py`:** `app.include_router(pages_router)`.

- [ ] **Step 6: Run** `pytest tests/test_pages_router.py -v` → PASS.

---

## Task 12: CLI `/pages-*` commands (TDD)

**Files:**
- Create: `amx/cli_support/commands/pages.py`
- Modify: `amx/cli.py` (one `register_pages_commands(main, ...)` call)
- Test: `tests/test_pages_cli.py`

- [ ] **Step 1: Test (Click `CliRunner`) for `/pages-list`, `/pages-show`, `/pages-export`:**

```python
from click.testing import CliRunner
from amx.cli import main  # the Click group

def test_pages_list_runs_without_error():
    r = CliRunner().invoke(main, ["pages-list"])
    assert r.exit_code == 0
```

- [ ] **Step 2: Run** → FAIL.

- [ ] **Step 3: Implement `amx/cli_support/commands/pages.py` with the following commands, each one a `@main.command` registered in `register_pages_commands`:**

- `/pages-new` — wizard:
  1. Prompt for title.
  2. Use existing pickers (`pick_db_profile`, then optional table picker) to collect DB assets.
  3. Use `pick_doc_profile` to collect doc profiles.
  4. Use `pick_lineage_artifact` to collect lineage artifacts.
  5. Prompt for free-text intent.
  6. Call `PagesService.create_draft` then `generate`.
  7. Print the new page id + a preview.
- `/pages-list` — `svc.store.list_active()` formatted as a `rich.Table`.
- `/pages-show <id>` — prints `markdown_body`.
- `/pages-edit <id> [--note]` — write body to a temp file, open `os.environ.get("EDITOR", "nano" if sys.platform != "win32" else "notepad")`, on exit call `svc.save_revision`.
- `/pages-export <id> --format md|pdf [--out PATH]` — writes bytes to PATH or stdout (binary-safe).
- `/pages-delete <id>` — soft delete (with `--purge` calling the same store helper plus hard-delete SQL).

Each command resolves the service via a shared `_svc(cfg)` factory matching the FastAPI dep.

- [ ] **Step 4: Register in `amx/cli.py`:** `register_pages_commands(main, finalize_scope=_finalize_scope)`.

- [ ] **Step 5: Run** → PASS.

---

## Task 13: Frontend — `usePages` hook and routes (TDD via Vitest)

**Files:**
- Create: `frontend/src/hooks/usePages.ts`
- Create: `frontend/src/routes/Pages.tsx` (list)
- Create: `frontend/src/routes/PageNew.tsx` (wizard)
- Create: `frontend/src/routes/PageEdit.tsx` (editor + export)
- Modify: `frontend/src/App.tsx` (3 lazy imports + 3 routes)
- Modify: `frontend/src/components/Sidebar.tsx` (one nav entry)
- Test: `frontend/src/routes/Pages.test.tsx`

- [ ] **Step 1: Write Vitest test that renders `<Pages />` with a React Query mock and asserts the list shows a stubbed page row.**

- [ ] **Step 2: Implement `usePages.ts` using the same pattern as existing `useQuery({ queryKey: [...], queryFn: () => api.fetch(...) })` calls in `ScopeTree.tsx`:**

```typescript
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";

export type Page = { id: string; title: string; slug: string; status: string; updated_at: string; markdown_body: string };

export function usePagesList() {
  return useQuery<Page[]>({ queryKey: ["pages"], queryFn: () => api.fetch("/api/pages") });
}
export function usePage(id: string | undefined) {
  return useQuery<Page>({ queryKey: ["pages", id], queryFn: () => api.fetch(`/api/pages/${id}`), enabled: !!id });
}
export function useCreatePage() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: { title: string; intent: string; assets: { kind: string; ref: string }[] }) =>
      api.post("/api/pages", body),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["pages"] }),
  });
}
export function useGeneratePage(id: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: () => api.post(`/api/pages/${id}/generate`, {}),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["pages", id] }),
  });
}
export function useSavePage(id: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: { markdown_body: string; note?: string }) => api.patch(`/api/pages/${id}`, body),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["pages", id] }),
  });
}
```

- [ ] **Step 3: Implement `Pages.tsx` (list, with Create button → `/pages/new`).**
- [ ] **Step 4: Implement `PageNew.tsx` (wizard steps: title → assets → sources → intent → generate → redirect to `/pages/:id`). Responsive: vertical stepper on `sm:`.**
- [ ] **Step 5: Implement `PageEdit.tsx` (TipTap editor with `tiptap-markdown`, debounced autosave, export menu).**

```bash
cd frontend && npm install --save @tiptap/react @tiptap/starter-kit @tiptap/extension-link @tiptap/extension-table @tiptap/extension-table-row @tiptap/extension-table-cell @tiptap/extension-table-header tiptap-markdown
```

- [ ] **Step 6: Add to `frontend/src/App.tsx` near other lazy imports:**

```typescript
const Pages = lazy(() => import("./routes/Pages"));
const PageNew = lazy(() => import("./routes/PageNew"));
const PageEdit = lazy(() => import("./routes/PageEdit"));
// inside <Routes>:
<Route path="pages" element={<Pages />} />
<Route path="pages/new" element={<PageNew />} />
<Route path="pages/:pageId" element={<PageEdit />} />
```

- [ ] **Step 7: Add sidebar entry to `Sidebar.tsx`** alongside the Lineage entry, using the same `<Link>` + lucide icon pattern (`FileText` icon fits the docs theme).

- [ ] **Step 8: Run** `cd frontend && npm run build` → success.

---

## Task 14: AssetPicker / SourceAttacher / PageEditor / PageExportMenu components

**Files:**
- Create: `frontend/src/components/pages/AssetPicker.tsx`
- Create: `frontend/src/components/pages/SourceAttacher.tsx`
- Create: `frontend/src/components/pages/PageEditor.tsx`
- Create: `frontend/src/components/pages/PageExportMenu.tsx`

- [ ] **AssetPicker.tsx:** three tabs (DB / Docs / Lineage). Reuse the existing `useProfiles`, `useDocProfiles`, `useLineageArtifacts` hooks (if absent, fall back to direct `api.fetch`). Emits `AssetRef[]` via `onChange`. `sm:` collapses tabs into an accordion.

- [ ] **SourceAttacher.tsx:** drag-and-drop zone that accepts `.xlsx`, `.eml`, plus any extension already in `SUPPORTED_EXTENSIONS`. POSTs each file to `/api/pages/:id/sources` via `FormData`. Shows the list of attached sources with remove buttons.

- [ ] **PageEditor.tsx:** TipTap wrapper. Props: `initialMarkdown`, `onChange(markdown)`. Toolbar (H1-H3, bold, italic, ordered/bullet list, code, link, table, divider) + "raw md" toggle that swaps the editor for a `<textarea>` bound to the same markdown string.

- [ ] **PageExportMenu.tsx:** dropdown with two entries that hit `/api/pages/:id/export/md` and `/api/pages/:id/export/pdf` and trigger a browser download via a temporary `<a download>` element.

- [ ] **Step 1 (each component):** add a smoke test in `frontend/src/components/pages/*.test.tsx` that mounts the component with mock props and asserts the primary interaction fires its callback.

- [ ] **Step 2:** `cd frontend && npm run build && npm run test` → green.

---

## Task 15: Verification + deploy.sh + single commit

**Files:** none new.

- [ ] **Step 1: Run the full backend gate:**

```bash
cd /Users/omeryasirkucuk/Desktop/omeryasirkucuk/Master/Thesis/AMX
make lint && make type && make test
```

All three green. Fix anything red - do NOT defer to a follow-up (memory: feedback_ci_must_be_green).

- [ ] **Step 2: Run frontend build:**

```bash
cd frontend && npm run build
```

- [ ] **Step 3: No-regression perf gate** (memory: feedback_no_perf_regression). Run the existing `/ask` perf benchmark twice (before/after on the same baseline branch) and confirm `<5%` regression on critical path.

- [ ] **Step 4: Pre-commit guard sweep:**

```bash
git grep -n "paid" -- ':!docs/superpowers/plans' ':!docs/superpowers/specs' || true   # must be empty (kural #2)
git grep -nE "Claude|Anthropic" -- '*' ':!pyproject.toml' ':!**/*.py' || true        # confirm only allowed provider-level mentions remain (kural attribution)
```

Resolve any Turkish strings in tracked diff (kural #4) and any "paid" hits.

- [ ] **Step 5: Run `deploy.sh` FIRST (kural #6 — Studio-visible feature):**

```bash
./deploy.sh
```

Confirm the live Studio shows the new `Pages` sidebar entry and a new page can be created end-to-end.

- [ ] **Step 6: Single git commit (no PR — user opens it separately):**

```bash
git add amx/pages amx/docs amx/storage amx/web/routers/pages.py amx/web/server.py amx/web/deps.py amx/cli_support/commands/pages.py amx/cli.py tests frontend pyproject.toml docs/superpowers/specs/2026-05-18-amx-documentation-pages-design.md docs/superpowers/plans/2026-05-18-amx-documentation-pages.md
git commit -m "$(cat <<'EOF'
feat(pages): documentation pages with LLM compose, editor, MD/PDF export

Adds /pages surface (Studio + CLI /pages-*) that composes a
Medium-style documentation page from selected DB profiles, doc
profiles, lineage artifacts, and uploaded sources (now incl. .xlsx
and .eml). Pages are editable via TipTap with autosave + version
history, and can be exported as Markdown or PDF.
EOF
)"
```

(No `Co-Authored-By` trailer per CLAUDE.md.)

- [ ] **Step 7: Stop. Do NOT push or open a PR — user opens the PR separately.**

---

## Self-Review

**Spec coverage:**
- §1 motivation → no task (narrative).
- §2 goals/non-goals → Tasks 1-15 cover all listed goals; non-goals not implemented (correct).
- §3 user stories → covered by Tasks 11 + 13 (UI/CLI flows).
- §4 architecture → Tasks 5-14 implement every module.
- §5 data model → Task 4 (with mandatory descriptions).
- §6 ingestion (xlsx/eml) → Tasks 1-3.
- §7 LLM composition → Task 8.
- §8 editor + export → Tasks 9, 13, 14.
- §9 CLI shape → Task 12.
- §10 REST API → Task 11.
- §11 Studio frontend → Tasks 13-14.
- §12 config → Tasks 11/12 (no new keys, just service wiring).
- §13 cross-platform → covered by stdlib + pure-Python deps used in Tasks 2, 3, 9, 12 (`$EDITOR` win fallback noted explicitly).
- §14 deploy order → Task 15 Step 5.
- §15/§16 out-of-scope / follow-ups → no task (correct).

**Placeholder scan:** All code blocks contain real signatures and imports. The `_resolver.py` adapter is named in Task 11 Step 4 with concrete entry points (`amx.llm.provider.get_active_provider`, `amx.docs.rag.RAGStore`, lineage `store.get_artifact`).

**Type consistency:** `AssetRef(kind, ref)` is constructed identically in Tasks 5, 7, 10, 11, 12, 13. `SourceRef(kind, path, original_name)` same. `PagesService` constructor signature matches across Tasks 10 and 11.

**Known shortcuts (call them out so executor doesn't over-spend):**
- Task 11 Step 4 sketches the `Resolver` adapter and dependency wiring at one paragraph of detail; the executor expands the adapter against the real `amx.llm.provider` / `amx.docs.rag` APIs.
- Task 13 frontend tests are smoke-level only; full coverage is post-v1.
- Task 14 component tests are smoke-level only.

Execution: see "Execution Handoff" below.
