# AMX Documentation Pages — Design

**Date:** 2026-05-18
**Status:** Approved (verbal); ready for implementation plan
**Scope:** v1 — single-user, in-app generation + editing + export

## 1. Problem and motivation

AMX already collects database metadata (schemas, columns, lineage)
and ingests user-supplied documents (PDF/MD/DOCX/CSV/…). What it
does not produce today is a **user-facing narrative artifact** —
"what is this project, what are its calculations, how does the
pipeline work" — written in plain prose against the union of those
assets.

The Documentation Pages feature closes that gap. A user selects
assets (DB profiles, doc profiles, lineage artifacts), optionally
attaches new sources (e-mail `.eml`, Excel `.xlsx`), provides a free
text intent, and AMX composes a Medium-style page that the user can
then **edit in-place**, **save**, and **export as Markdown or PDF**.

## 2. Goals and non-goals

### Goals (v1)

- New top-level surface in Studio (`/pages`) and CLI (`/pages-*`).
- Multi-asset selection: DB profiles down to column, doc profiles,
  lineage artifacts.
- Two new source loaders: `.xlsx` and `.eml`.
- LLM-generated initial draft using existing RAG + lineage context.
- WYSIWYG / markdown-backed editor with autosave and version history.
- Exports: Markdown (`.md`) and PDF (`.pdf`).
- Cross-platform (macOS / Windows / Linux) — pure-Python deps only.

### Non-goals (v1)

- Multi-user collaboration / real-time co-editing.
- Comments, annotations, inline review threads.
- Public sharing URLs (read-only links). Internal-only.
- Embedded interactive lineage canvas inside a page (only static
  reference + link).
- `.msg` Outlook ingestion (follow-up — needs `extract-msg`).
- WeasyPrint high-fidelity PDF (follow-up — Cairo/Pango install
  pain on Windows).

## 3. User stories

1. As an analyst, I select my `pg_prod` profile and three lineage
   artifacts, type "explain the orders aggregation pipeline", and
   get a draft page covering the source tables, joins, and target
   marts.
2. As an engineer, I attach last quarter's design `.docx` plus the
   PM's spec `.eml` plus the SLO `.xlsx` to a page about the
   billing service, then hand-edit the generated draft and export
   it as PDF for the architecture review.
3. As a team lead, I keep a "platform overview" page that I
   re-generate after each schema change and download the markdown
   to commit to the team's internal repo.

## 4. Architecture

### 4.1 Module layout

```
amx/
  pages/                 ← new module
    __init__.py
    service.py           orchestrates: assets → context → LLM → save
    context.py           gathers schema / lineage / RAG snippets
    composer.py          LLM prompt construction and parsing
    exporters.py         to_markdown(), to_pdf()
    store.py             CRUD over storage.sqlite_store
    types.py             dataclasses: PageDraft, PageContext, AssetRef
  cli_support/commands/
    docs.py              ← extended with /pages-* commands
  web/
    pages_routes.py      ← new FastAPI router
  docs/
    extensions.py        ← + ".xlsx", ".eml"
    scanner.py           ← + xlsx_loader, eml_loader
    uploads.py           ← no change (extensions registry is single source of truth)
  storage/
    shared_schema.py     ← + 4 tables
    sqlite_store.py      ← + 4 tables (local mirror)
    schema_descriptions.py ← + descriptions for every new column

frontend/src/
  routes/
    Pages.tsx            list view
    PageNew.tsx          wizard
    PageEdit.tsx         editor + export
  components/pages/
    AssetPicker.tsx
    SourceAttacher.tsx
    PageEditor.tsx       (TipTap, markdown-backed)
    PageExportMenu.tsx
```

Each file owns one responsibility (kural #8). `service.py` is the
seam between transport (FastAPI / CLI) and the underlying modules;
neither transport layer reaches past it.

### 4.2 Data flow — generation

```
Studio / CLI
   │  POST /api/pages          { title, intent, assets, sources }
   ▼
service.create_draft()
   │
   ├─► store.insert_page()           # row in documentation_pages (status=draft)
   ├─► store.attach_assets()         # rows in _assets
   ├─► store.attach_sources()        # rows in _sources
   │
   ▼
service.generate(page_id)
   │
   ├─► context.gather(assets, sources, intent)
   │     ├─ DB assets → DDL + column descriptions + sample counts
   │     ├─ Doc profiles → RAG top-k snippets retrieved with intent
   │     ├─ Lineage artifacts → anchor + neighbors + render summary
   │     └─ Sources → ingested text from xlsx/eml loaders
   │
   ├─► composer.compose(context, intent)
   │     └─ LLM (active provider) → Markdown body
   │
   └─► store.update_body(page_id, markdown_body)
         └─ creates documentation_page_versions row #1
```

### 4.3 Data flow — edit and export

```
Studio editor (TipTap)
   │  PATCH /api/pages/:id     { markdown_body }   (debounced 5s)
   ▼
service.save_revision()
   │
   ├─► store.update_body()             # documentation_pages.markdown_body
   └─► store.append_version()          # documentation_page_versions row

Studio export menu
   │  GET /api/pages/:id/export/md   →  text/markdown attachment
   │  GET /api/pages/:id/export/pdf  →  application/pdf attachment
   ▼
service.export(page_id, fmt)
   │
   ├─ md  → store.get_body() → response
   └─ pdf → exporters.to_pdf(markdown_body)
              → markdown_it render → HTML
              → xhtml2pdf → PDF bytes
              → response
```

## 5. Data model

All new tables ship with descriptions in
`schema_descriptions.py` in the **same commit** (kural #5). The CI
tests `tests/test_local_schema_comments.py` and
`tests/test_shared_schema_comments.py` enforce this.

### 5.1 `documentation_pages`

| Column | Type | Description |
|---|---|---|
| `id` | `TEXT PK` | Stable UUID for the page across versions and exports. |
| `title` | `TEXT NOT NULL` | Human-readable page title; rendered as the H1 of the document and shown in list views. |
| `slug` | `TEXT UNIQUE NOT NULL` | URL-safe identifier used by `/pages/:slug` in Studio routing. |
| `markdown_body` | `TEXT NOT NULL` | Current markdown source; canonical content backing both the editor and exports. |
| `rendered_html` | `TEXT` | Cached HTML render of `markdown_body`; used by PDF export and read-only views to skip re-rendering. |
| `status` | `TEXT NOT NULL DEFAULT 'draft'` | Lifecycle flag — `draft`, `published`, or `deleted`; controls visibility in listings and supports soft-delete via `/pages-delete`. |
| `created_at` | `TIMESTAMP NOT NULL` | Creation timestamp in UTC. |
| `updated_at` | `TIMESTAMP NOT NULL` | Timestamp of the last edit; updated on every body change. |
| `created_by` | `TEXT` | Author identity; populated from the active AMX session/user record when present. |
| `generation_prompt` | `TEXT` | User's free-text intent passed to the LLM on the most recent generate call; persisted so users can re-generate with the same prompt. |
| `model_used` | `TEXT` | LLM model id that produced the last generated draft; supports reproducibility and attribution. |

### 5.2 `documentation_page_assets`

| Column | Type | Description |
|---|---|---|
| `id` | `INTEGER PK` | Surrogate key for asset row identity. |
| `page_id` | `TEXT FK` | Parent page reference. |
| `asset_kind` | `TEXT NOT NULL` | Discriminator — one of `db_profile`, `db_database`, `db_schema`, `db_table`, `db_column`, `doc_profile`, `lineage_artifact`. |
| `asset_ref` | `TEXT NOT NULL` | Fully-qualified reference (e.g. `pg_prod/sales/public/orders`, `doc:design_docs`, `lineage:<artifact_id>`); resolves to the live object at generation time. |
| `included` | `INTEGER NOT NULL DEFAULT 1` | 1/0 flag — whether the asset is active for re-generation; lets users keep history while excluding from future runs. |

### 5.3 `documentation_page_sources`

| Column | Type | Description |
|---|---|---|
| `id` | `INTEGER PK` | Surrogate key for source row identity. |
| `page_id` | `TEXT FK` | Parent page reference. |
| `source_kind` | `TEXT NOT NULL` | Discriminator — `upload`, `email`, or `excel`; controls which loader handles the file. |
| `source_path` | `TEXT NOT NULL` | Content-addressed path under `~/.amx/uploads/pages/<page_id>/<sha>.<ext>`. |
| `original_name` | `TEXT NOT NULL` | User-facing filename to display in the source list. |
| `created_at` | `TIMESTAMP NOT NULL` | Upload timestamp in UTC. |

### 5.4 `documentation_page_versions`

| Column | Type | Description |
|---|---|---|
| `page_id` | `TEXT FK` | Parent page reference. |
| `version_no` | `INTEGER NOT NULL` | Monotonically-increasing version index for the page; together with `page_id` uniquely identifies a snapshot. |
| `markdown_body` | `TEXT NOT NULL` | Frozen markdown content at the moment of save; supports diff/restore workflows. |
| `saved_at` | `TIMESTAMP NOT NULL` | Timestamp the version was captured. |
| `saved_by` | `TEXT` | Author identity at save time. |
| `note` | `TEXT` | Optional short change note supplied by the user (e.g. "regenerated after schema change"). |

Composite PK on `(page_id, version_no)`.

## 6. Document ingestion extensions

### 6.1 `.xlsx`

- Library: `openpyxl` (pure-Python, MIT, ships wheels for every OS).
- Loader: each sheet → serialise to a markdown table with the sheet
  name as `## <sheet>` heading; empty sheets are skipped; merged
  cells are flattened by repeating the anchor value.
- Cell value coercion: strings as-is; numerics formatted with the
  cell's number format if present; dates ISO-8601.
- Per-cell hard limit 4 KB to keep one runaway cell from blowing up
  the prompt; truncated cells get a trailing `…`.

### 6.2 `.eml`

- Library: stdlib `email` (no new dep).
- Loader: parse subject/from/to/date headers into a YAML frontmatter
  block, then prefer `text/plain` body; fall back to `text/html`
  passed through `markdownify` (already a transitive dep). Inline
  attachments are listed by name only — full attachment ingestion
  is a follow-up.
- Quoted-reply history (`>` blocks) is kept so the LLM can see the
  thread context.

### 6.3 Registry update

`amx/docs/extensions.py` is the single source of truth.
Adding `.xlsx` and `.eml` to `SUPPORTED_EXTENSIONS` automatically
exposes them to:

- `amx/docs/uploads.py::ACCEPTED_EXTENSIONS`
- `amx/docs/scanner.py` whitelist
- `amx/rag_core` `LOADER_MAP` (loader entries added in the same change)

## 7. LLM composition

### 7.1 Prompt structure

```
[system]
You are a senior technical writer. Produce a Markdown documentation
page from the assets and sources below. Use these sections:
  1. Overview
  2. Data Assets
  3. Business Logic
  4. Pipelines & Lineage
  5. Open Questions
Keep each section short and concrete. Cite asset names inline when
referencing them. Do not invent fields or relationships that are
not in the provided context.

[user]
INTENT: {{ user free-text intent }}
CONTEXT:
{{ serialised PageContext: assets block, sources block, lineage block }}
```

### 7.2 Context budget

- Hard ceiling: 60 KB of serialised context (≈ 15k tokens) to leave
  room for output. `context.gather()` truncates per-asset with
  deterministic priority: explicit user-selected columns → table
  DDLs → doc snippets ranked by retrieval score → lineage
  neighbours.

### 7.3 Performance budget (kural: no perf regression)

- `service.generate()` latency budget: `≤ 1.5×` the median `/ask`
  baseline at the same model. Benchmark added to the existing
  `tests/perf/` harness; PR is blocked on >5% regression of `/ask`.

## 8. Editor and export

### 8.1 Editor

- TipTap (React) with `@tiptap/extension-*` standard set +
  `tiptap-markdown` for round-trip markdown.
- "View raw markdown" toggle for power users.
- Autosave: 5-second debounce → PATCH `/api/pages/:id`. Each save
  appends a `documentation_page_versions` row.
- Version drawer: list of versions with `saved_at` / `note`; one
  click restores into a new version (no destructive overwrite).

### 8.2 Markdown export

- Endpoint returns `markdown_body` verbatim with
  `Content-Type: text/markdown; charset=utf-8` and
  `Content-Disposition: attachment; filename="<slug>.md"`.

### 8.3 PDF export

- Pipeline: `markdown_body` → `markdown-it-py` HTML render →
  `xhtml2pdf` → PDF bytes.
- `xhtml2pdf` chosen over WeasyPrint because it is pure Python and
  installs cleanly on Windows (kural #10). WeasyPrint is parked as
  a follow-up for users who need higher fidelity.
- Stylesheet: shared `pages/pdf.css` matching the on-screen editor's
  read view so on-screen and PDF stay visually close.

## 9. CLI shape (kural #9 — tab-based)

All commands live under the existing `docs` tab. There is **no**
top-level `amx pages` Click group.

| Command | Behaviour |
|---|---|
| `/pages-new` | Wizard (kural: wizard-first): pick page title → pick db assets → pick doc assets → pick lineage artifacts → attach sources → enter intent → preview → save. Flags optional. |
| `/pages-list` | Table of saved pages (id, title, status, updated_at). |
| `/pages-show <id>` | Prints markdown body to stdout. |
| `/pages-edit <id>` | Opens body in `$EDITOR`; on exit, saves as a new version with optional `--note`. |
| `/pages-export <id> --format md\|pdf [--out PATH]` | Writes to PATH or stdout. |
| `/pages-delete <id>` | Soft-delete (status=`deleted`); hard-delete behind `--purge`. |

## 10. REST API

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/api/pages` | List pages (filter by status, search by title). |
| `POST` | `/api/pages` | Create draft (title, intent, asset refs). |
| `GET` | `/api/pages/{id}` | Read page + versions + asset/source lists. |
| `PATCH` | `/api/pages/{id}` | Update title/body/status (creates new version on body change). |
| `DELETE` | `/api/pages/{id}` | Soft-delete; `?purge=1` for hard delete. |
| `POST` | `/api/pages/{id}/generate` | Run LLM composition; replaces body, creates version. |
| `POST` | `/api/pages/{id}/sources` | Upload one or more source files (multipart). |
| `DELETE` | `/api/pages/{id}/sources/{src_id}` | Remove a source. |
| `GET` | `/api/pages/{id}/export/md` | Markdown attachment. |
| `GET` | `/api/pages/{id}/export/pdf` | PDF attachment. |

All endpoints reuse the existing AMX auth middleware and the same
JSON error envelope used by `/api/runs`.

## 11. Frontend — Studio

### 11.1 Routes

```
/pages          → list
/pages/new      → wizard
/pages/:id      → editor + export menu
```

Lazy-loaded chunk (matches the pattern in `frontend/src/App.tsx`).
AppShell sidebar gains a `Pages` entry next to `Lineage`.

### 11.2 Components

- `AssetPicker.tsx` — three tabs (DB / Docs / Lineage), each with
  the existing pickers reused (no duplicate code; pull shared
  helpers from `components/profiles/`).
- `SourceAttacher.tsx` — drag-and-drop for `.xlsx` / `.eml` /
  existing supported extensions; shows uploaded source list with
  remove buttons.
- `PageEditor.tsx` — TipTap container with toolbar (H1-H3, bold,
  italic, lists, code, link, table, image, divider) + "raw md"
  toggle.
- `PageExportMenu.tsx` — dropdown with Markdown / PDF entries.

### 11.3 Responsive (memory: feedback_studio_responsive_required)

- All layouts use `sm:` / `md:` / `lg:` prefixes; the wizard
  collapses to a vertical stepper on narrow viewports.
- Editor toolbar hides secondary buttons behind a "More" menu on
  `sm:`.
- Asset picker tables apply `hideOnMobile` to low-priority columns.

## 12. Configuration

No new top-level config keys. Per-page settings live in the page
rows themselves. The pages feature respects:

- The active LLM profile (existing `cfg.active_llm_profile`).
- The active doc profiles selected on the page (rather than the
  global active set).
- `AMX_UPLOAD_DIR` env var (already honoured by uploads.py) for the
  source storage root.

## 13. Cross-platform notes (kural #10)

- All file paths via `pathlib.Path`.
- Uploads root via `Path.home() / ".amx" / "uploads" / "pages" / <page_id>`.
- No shell-out: `openpyxl` and stdlib `email` are pure Python; PDF
  pipeline is pure Python (`markdown-it-py` + `xhtml2pdf`).
- `$EDITOR` resolution uses `os.environ.get("EDITOR")` with a
  Windows-aware default (`notepad.exe`) when unset.
- File encoding is always explicit `utf-8`.

## 14. Studio deploy order (kural #6)

This feature is Studio-visible. Integration order is:

1. `deploy.sh` first — live Studio reflects the new routes.
2. PR opens against the deployed state.
3. Merge after review.

This spec is committed before deploy because the design document
itself is not Studio-visible.

## 15. Out of scope (recap)

- Multi-user / real-time collaboration.
- Public read-only share URLs.
- Inline comments / annotations.
- `.msg` Outlook ingestion (follow-up).
- WeasyPrint PDF (follow-up).
- Embedded interactive lineage canvas (only static reference).

## 16. Open follow-ups (post-v1)

- `.msg` ingest via `extract-msg`.
- WeasyPrint-based "high fidelity PDF" toggle.
- Page templates (e.g. "Table profile", "Pipeline runbook").
- Scheduled re-generation via the existing `/schedule` system.
- Public read-only share links (with the standard AMX auth gate).
