# Code RAG Hardening — Design Spec

**Status:** Approved (audit-driven)
**Date:** 2026-05-12
**Scope:** AMX CLI + AMX Studio code-RAG path, end-to-end

## Motivation

A read-only end-to-end audit of the code RAG pipeline (ingestion +
retrieval, CLI + Studio) revealed that the code path repeats most of
the docs-RAG defects that PRs #335-339 fixed, plus adds new
code-specific issues.

Headline user-visible symptoms:

- **Studio "Scan" is non-functional end-to-end.** `analyze_codebase`
  is called without `index_semantic=True`, so the Chroma collection
  stays empty. `save_cached_report` is also skipped, so the analyze
  endpoint dead-ends with "Run /code-scan first." A Studio user has
  to drop into the CLI to make anything work.
- **No `.gitignore` / `node_modules` / `.git` filter.** Scans walk the
  entire tree, flooding the code index with third-party JS, vendored
  artifacts, and packed git refs. Embedding cost and retrieval
  relevance both suffer.
- **Silent MiniLM override.** `cfg.embedding` is ignored — the docs
  pre-PR-A bug, unfixed for code. The collection records nothing
  about which embedding model wrote it.
- **Orphan chunks on file shrink/delete.** Pure `upsert` with no
  per-file pre-delete leaves stale chunks for renamed functions,
  deleted files, and moved classes.
- **No citations on code-derived suggestions.** `CodeAgent` never
  populates `MetadataSuggestion.citations`. Users cannot trace a
  suggestion back to `src/foo.py:120`.
- **No re-Run snapshot for code hits.** `rerun_context.code_context`
  is a hard-coded `[]` placeholder. Re-runs re-query live and can
  produce different evidence than the original run.
- **Most PR D capabilities missing for code**: no query timeout, no
  doctor check, no health endpoint, no multi-profile `/run`, no
  cancellation polling.

## Approach

Four sequenced PRs, each green-on-CI before the next starts. Same
pattern as the docs-RAG hardening (PRs #335-339). Each PR is
independently reviewable + revertable.

### PR α — Studio code surface fix

Bring Studio to the same functional level as the CLI. Highest
user-visible impact; least invasive on the indexer.

- `web/routers/code_ops.py` `_scan_worker_body` calls
  `analyze_codebase(..., index_semantic=True)` and threads a progress
  callback through the existing SSE infrastructure (matches PR A's
  per-file events for docs).
- `_scan_worker_body` calls `save_cached_report(...)` on success and
  `sync_code_report(...)` to the search catalog — mirroring the CLI
  exactly (`commands/code.py:272-279`).
- `POST /api/code/jobs/{job_id}/cancel` endpoint sets `job.cancel`;
  the worker polls between files.
- `/api/code/search` accepts a list of profile names (or derives the
  union from `cfg.code_profile_linked_dbs`) and passes
  `source_filters=` to `query_code_snippets` so search respects
  active scope.

### PR β — Storage correctness

Same template as docs-RAG PR B. Plumb the configured embedding
provider into the code Chroma collection, record provenance, fix
idempotency, and start ignoring directories nobody wants in the
index.

- `codebase/code_rag.py` `index_codebase_tree` accepts a `cfg`
  argument and calls `make_embedding_function(cfg.embedding.kind,
  model=..., api_key=..., base_url=...)`; passes the result as
  `embedding_function=` to `get_or_create_collection`.
- Collection metadata records
  `{embedding_model, embedding_provider, hnsw:space}` on first
  create. On reopen, mismatched provider/model raises
  `CodeEmbeddingMismatch` (new exception) which propagates to the
  run record's `code_unavailable_reason` and the CLI/Studio surface.
- Grandfather: pre-PR-β collections with no recorded provider
  inherit current config and write metadata silently on next access.
- Per-file pre-delete: before `coll.upsert` for a file, `coll.get`
  IDs scoped by `rel_path` and `coll.delete` any IDs that aren't in
  the new chunk set. Closes the orphan-chunks bug for shrink, file
  deletion, and function rename.
- `.gitignore` parsing (via `pathspec`, already in
  `amx/docs/scanner.py`), plus a hard-coded denylist for
  `{node_modules, .git, .venv, venv, dist, build, target, vendor,
  .next, .cache}`. The scanner walks the tree once and emits a
  filtered file list; both `analyze_codebase` and
  `index_codebase_tree` consume it instead of re-rglobbing.
- `.ipynb` cell-aware: parse JSON, extract `cell.source` and
  `cell.cell_type` (`code` / `markdown`) into per-cell chunks
  metadata`{kind: "ipynb_code"|"ipynb_md", cell_idx: N}`.

### PR γ — Citation chain

Same template as docs-RAG PR C. Make every code-derived suggestion
trace back to a file + line range.

- Chunk metadata gains `start_line` / `end_line` (where available
  from AST; fallback splitter sets them from token offsets).
- `CodeAgent._parse_response` no longer drops the hits — the prompt
  hits are remembered (`last_prompt_hits` mirroring `RAGAgent`) and
  each emitted `MetadataSuggestion` carries citations populated
  from retrieval metadata, NOT from LLM output.
- Citation dataclass already exists (`Citation` from PR C). Code
  citations reuse it; `source` is repo-relative `rel_path`,
  `chunk_idx` is the `cid` numeric (0-based across the file's
  chunks), `score` is rerank score, `snippet` is the first 200
  chars of chunk text, plus a new optional `line_range:
  tuple[int, int] | None` field (extends the dataclass; backwards
  compatible because the field is optional with a `None` default).
- `tool_agent._summarise_tool_call` extends the citation extraction
  to `tool_call.name == "search_code"` (currently only
  `search_docs`).
- `AskChat.tsx` `CitationsList` renders code citations identical to
  doc citations except the `line_range` appended after the path:
  `src/foo.py:120-145 · score 0.84`.
- CLI `_format_sources_cell` already supports the format generically
  (`path:chunk_idx`); extends to `path:line_range` when present.
- Drop the hardcoded `"SQL Spark dataframe usage"` semantic-query
  bias (`code_agent.py:174`). Use a neutral
  `f"{ctx.schema} {ctx.table}"` query string with optional per-column
  expansion via a new `pd.code_col_hits` config knob (defaults to 0,
  preserving today's behaviour).

### PR δ — Stability + perf + UI

Same template as docs-RAG PRs D + E combined. Closes the remaining
audit items.

- Query timeout: `query_code_snippets(... timeout=cfg.llm.rag_query_timeout_sec)`
  reusing the same field PR D added (one timeout value covers both
  doc and code paths — both run through Chroma).
- Doctor `_check_code_rag`: opens the `amx_code` collection,
  verifies provider metadata matches active config, sample query,
  emits warn for empty collection, fail on exception.
- `GET /api/profiles/code/{name}/health` returns
  `{chunk_count, last_indexed_at, last_error, embedding_model,
  embedding_provider, paths}`.
- `AMXConfig.run_code_profiles: list[str]` (parallel to
  `run_doc_profiles`). CLI `/run … --code <name>` repeatable; Studio
  Run dialog gains a code-profile multi-select chip row mirroring
  the doc one already shipped in PR E.
- Re-Run RAG-style snapshot for code: `rerun_context.code_context`
  is populated by `_cache_code_hits_for_rerun` after the agent
  fan-out. On re-run, CodeAgent reads `ctx.code_hits` and skips the
  live Chroma query when present (falls back when empty).
- `CodeAgent._record_diagnostic` mirroring `RAGAgent`; orchestrator's
  `table_processor` drains it so "no code context for X.Y" reaches
  the user.
- Batch Chroma upsert: `coll.upsert(ids=..., documents=..., metadatas=...)`
  receives batched lists per file (and per source root) instead of
  one chunk at a time.
- `code_collection_count` server-side filter via Chroma `where={}`
  instead of full-table Python filter.
- Settings → Code tab in Studio gains a per-profile health line
  identical to the docs version (chunks count, last indexed,
  embedding model, optional last-error chip).
- AskChat `ToolCallList` for `search_code` renders the same compact
  hit table as docs (file:line, score).

## Out of scope (deferred)

- Tree-sitter / language-server based chunking — current AST-only
  for Python is acceptable; broader language coverage is a
  multi-PR effort better handled when retrieval quality data drives
  the priority list.
- Incremental git scans (`git diff --name-only HEAD`-driven
  re-index) — separate perf PR. Today re-indexing a 200-file repo
  re-embeds everything; with batched embeddings (PR δ) this is
  cheaper but not free.
- Cell-output suppression / executable-output normalization in
  `.ipynb` — for now treat code cells as ordinary chunks and
  markdown cells as documentation chunks.
- Multi-language symbol extraction (Go, TypeScript, Java) — current
  regex-based approach in `analyzer.py` is fine for evidence-finding
  but not perfect for semantic search; tracker.

## Per-PR risk

- α: low-medium. Pure plumbing on Studio worker + endpoint, but the
  scan worker hasn't been touched in a while; verify the cancel
  polling doesn't strand orphan files.
- β: medium-high. Touches the chunker, the collection metadata, and
  the per-file pre-delete logic. Grandfather rule for existing
  collections is critical; users with populated indexes must not
  see them wiped on upgrade.
- γ: medium. Adds a field to `Citation` (optional, backwards-compat),
  but the chunk metadata format change in β must land first.
- δ: medium. New thread executor for queries; multi-profile changes
  the contract of code resolution; Studio surface gains a new card.

## Test strategy

Each PR ships with its own test file under `tests/` (flat layout
matching docs-RAG convention):

- α: `tests/web/test_code_scan_indexes.py`,
  `tests/web/test_code_cancellation.py`,
  `tests/web/test_code_search_respects_filter.py`
- β: `tests/test_code_storage_correctness.py`,
  `tests/test_code_gitignore_filter.py`,
  `tests/test_code_ipynb_cells.py`
- γ: `tests/test_code_citation_chain.py`,
  `tests/test_search_code_citations.py`
- δ: `tests/test_code_query_timeout.py`,
  `tests/test_doctor_code_check.py`,
  `tests/test_code_multi_profile.py`,
  `tests/test_rerun_code_snapshot.py`,
  `tests/web/test_code_profile_health.py`

## Migration

Pre-existing users have a populated `~/.amx/chroma_db/amx_code`
collection. The grandfather rule in PR β handles this: absence of
provider metadata → write current values, do NOT reindex. No data
loss on upgrade.

`run_code_profiles` defaults to `[]` (empty list) so the
single-profile path is preserved until the user opts in.
