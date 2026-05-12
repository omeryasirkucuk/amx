# RAG Hardening — Design Spec

**Status:** Approved (audit-driven)
**Date:** 2026-05-12
**Scope:** AMX CLI + AMX Studio document RAG, end-to-end

## Motivation

A read-only end-to-end audit of the document RAG pipeline (ingestion +
retrieval, CLI + Studio) surfaced silent failure modes, opacity, and
correctness gaps. Concrete user-visible symptoms:

- The `cfg.embedding` provider chosen in `/llm` settings is **ignored**
  for document RAG — Chroma's bundled MiniLM is always used.
- Ingestion of a folder of 50 PDFs where 49 fail reports "Ingested 12
  chunks" with no per-file outcome.
- `RAGStore` failing to initialise during `/run` is caught by
  `except: pass`; the run silently proceeds with no doc context and
  the user is never told.
- The user can never trace a suggestion back to the document that
  informed it — `MetadataSuggestion.source` is just the string
  `"rag"`; the rich `{source, chunk_idx}` metadata from Chroma is
  dropped at the parser boundary.
- Editing a 10-chunk file down to 8 chunks leaves chunks 8 and 9 as
  orphans in the collection forever (idempotency bug).
- `.markdown` and `.tsv` files are accepted by the upload UI but
  silently dropped at scan time (whitelist mismatch).
- `/run` cannot pull from multiple doc profiles in one execution
  (`/ask` can).
- `/rerun` re-queries the live RAG store instead of snapshotting hits
  at the original run, so re-runs after a re-ingest behave
  differently with no audit trail.

## Approach

Four sequenced PRs, each landing independently with full CI green and
no behaviour regression on the existing test suite:

### PR A — Observability + error surfacing

Make every silent failure visible. No behavioural changes; just
plumb signal to the user.

- Ingest summary (CLI + Studio progress event) reports
  `{succeeded, failed, failed_files}` with a per-file outcome list.
- `analyze_flow:1059-1060` and `inference.py:83-84` replace
  `except: pass` with a single-line `error(...)` /
  `_record_diagnostic(...)` plus a structured "no-rag" reason
  pushed into the run record so post-run summaries can read it.
- Empty-context skip in `RAGAgent` emits an informational marker
  the orchestrator can render as "no relevant documents found".
- Extension whitelist alignment: pick a single source of truth.
  `ACCEPTED_EXTENSIONS` and `SUPPORTED_EXTENSIONS` are reconciled;
  one canonical `frozenset` lives in `amx/docs/extensions.py`, both
  callers import it. `.rtf` either gets a `LoaderMap` entry or is
  removed from the whitelist (decision: remove — `langchain`
  doesn't have a deterministic RTF loader without optional
  dependencies).
- Scanner per-source failures surfaced in the
  `scan_all_sources` return value (already returns docs; add a
  parallel `failures: list[ScanFailure]`) and propagated to both
  the CLI scan summary and the Studio `scan.summary` SSE event.

### PR B — Storage correctness

Stop silently overriding the user's embedding provider; make the
ingest idempotent on file edits.

- `RAGStore.__init__` accepts an `embedding_function` argument and
  passes it through to `get_or_create_collection`. The collection
  metadata records the resolved embedding model name +
  dimensionality (`{embedding_model: "...", embedding_dim: N,
  embedding_provider: "..."}`) at first create.
- On reopen, if the active provider's model name differs from the
  collection's recorded model, `RAGStore` raises a
  `EmbeddingProviderMismatch` exception with a clear message:
  "Collection was indexed with X; current config says Y. Run
  `/docs reindex` to rebuild or `/docs revert-embedding` to switch
  back."
- Caller path (`analyze_flow`, `ask`, CLI commands) catches the
  mismatch and surfaces it as a user-facing error instead of a
  silent run.
- Idempotency: `RAGStore.ingest` queries the collection for all
  existing IDs that start with `f"{doc.path}::"`, computes
  `expected_ids = {f"{doc.path}::{i}" for i in range(len(chunks))}`,
  and `collection.delete(ids=list(existing - expected))` before the
  `upsert`. Net effect: shrinking a file leaves the collection in
  the right state without `--refresh`.

### PR C — Citation chain

End the opacity. Every RAG-derived suggestion carries machine-
readable citations all the way to the UI.

- New `Citation` dataclass in `amx/agents/base.py`:
  ```python
  @dataclass(frozen=True)
  class Citation:
      source: str        # repo-relative path or URL
      chunk_idx: int
      score: float       # post-rerank score
      snippet: str       # first 200 chars of the chunk
  ```
- `MetadataSuggestion.citations: list[Citation] = []`.
- `RAGAgent.run` populates citations from the retrieval result
  (NOT from the LLM's `reasoning` text — that's unreliable). Each
  suggestion carries the citations of the chunks fed into its
  prompt.
- CLI `run_summary` adds a compact "Sources" column when any
  suggestion in the row has citations (rendered as
  `pdf.pdf:5, README.md:2`).
- Studio `RunDetail.tsx` adds a citations list under each
  suggestion's `ReasoningDisclosure`.
- Both surfaces use a deterministic format so the user can scan
  hundreds of suggestions and immediately know which docs
  influenced which columns.

### PR D — Stability + perf

Close the remaining important findings.

- **Query timeout**: `RAGStore.query` accepts `timeout: float | None`
  (default `cfg.rag_query_timeout_sec`, plumbed from a new
  `LLMConfig.rag_query_timeout_sec: float = 5.0`). Wrap the Chroma
  call in `concurrent.futures.ThreadPoolExecutor.submit(...).result(timeout=...)`
  and on timeout return `[]` + structured warning ("RAG retrieval
  exceeded Ns timeout, proceeding without context").
- **Cancellation**: Studio `docs_ops` workers poll `job.cancelled`
  between documents during ingest. New
  `POST /api/docs/jobs/{id}/cancel` endpoint exposes it. CLI
  receives the same flag via the existing `RunCancelled` mechanism.
- **Doctor RAG check**: `/doctor` adds a `RAG store` line. Checks:
  collection opens, embedding model matches config, query returns
  at least one chunk on a sentinel query, persist dir is
  writable.
- **Doc profile health endpoint**: `GET /api/profiles/docs/{name}/health`
  returns `{chunk_count, last_ingested_at, last_error,
  embedding_model}`. Studio Settings card renders this inline.
- **Multi-profile for `/run`**: `cfg.effective_doc_paths()` already
  takes a profile name; the orchestrator looks at
  `cfg.run_doc_profiles` (new field, list of names, defaults to
  `[active_doc_profile]`) and passes the union to
  `RAGStore(source_filters=...)`. CLI: `/run … --doc <name>` (repeatable);
  Studio: multi-select in the Run dialog.
- **Re-Run RAG snapshot**: `rerun_context.py` captures the actual
  chunks (text + metadata) returned by `RAGStore.query` during the
  original run. `rerun.py` reuses the snapshot instead of
  re-querying. The snapshot lives in
  `rerun_context_snapshots.snapshot_json` (existing column, new
  `rag_hits` key).
- **`/api/docs/search` source filter**: pass `source_filters` from
  the active profile.

## Out of scope (deferred)

- Parallel ingestion (concurrency knob) — separate hardening PR.
- Configurable chunk size / overlap — separate ergonomics PR.
- Doc-source-aware similarity threshold — needs corpus-specific
  tuning; ship without first, revisit when usage data shows the
  noise problem.
- Drag-drop folder upload on Studio — UX work, separate PR.
- Resumable cancellable downloads for very large S3/Drive objects —
  separate networking PR.

## Test strategy per PR

- **A**: end-to-end test that runs `RAGStore.ingest` on a fixture
  folder containing a corrupted PDF + 2 valid files and asserts the
  returned summary contains the failed file name. Test that
  `RAGStore` init failure is surfaced via the orchestrator's
  diagnostic channel.
- **B**: integration test that ingests with provider `openai`,
  reopens with `cfg.embedding.provider = "bge"`, expects
  `EmbeddingProviderMismatch`. Idempotency test: ingest a file
  with 10 chunks, edit to 8, re-ingest, assert collection has
  exactly 8 IDs for that path.
- **C**: parser test asserts `citations` populated from the
  retrieval mock (LLM output unchanged). Snapshot test of CLI
  summary including the "Sources" column.
- **D**: timeout test (Chroma stubbed with `time.sleep(2)`,
  timeout=0.5, assert empty + warning). Cancellation test
  (POST cancel mid-loop, assert worker exits without indexing
  remaining files). Re-Run test (snapshot present → no live
  RAGStore call).

## Per-PR risk

- A: low. Pure plumbing.
- B: medium. Touches Chroma init; existing collections need a
  one-shot migration. Mitigation: collection metadata is missing on
  pre-existing collections → treat absence as "trust current
  config, write metadata now". No reindex forced on upgrade.
- C: low-medium. Adds a non-nullable list (defaults to empty);
  serialization layer needs to know about the new field.
- D: medium. New thread pool for query timeout (small extra
  resource cost); cancellation discipline needs to be tested with
  partial ingests. Multi-profile changes the contract of
  `effective_doc_paths()`.

Each PR ships independently; if a later PR causes regressions the
prior PRs can stand alone.
