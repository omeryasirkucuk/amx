# Docs & Code settings: collapse actions to "Index" + "Active"

## Context

The Docs and Code profile rows in Studio Settings (and their CLI
counterparts) expose too many overlapping verbs, and users can't tell
which to use:

* **Docs** row: `Scan` (preview), `Ingest` (add), `Reindex` (drop +
  rebuild), plus `Active`. Three of these are all "index the docs"; the
  difference (incremental vs rebuild vs preview, and which one recovers
  from an embedding-model change) is invisible.
* **Code** row: `Scan`, `+Cols` (scan incl. column references),
  `Analyze`, plus `Active`. `Scan`/`+Cols` are indexing; `Analyze` is
  something else entirely — it runs the LLM Code Agent to *generate*
  descriptions (consumes tokens).

The ask: collapse each side to exactly **`Index`** + **`Active`** (plus
Edit/Delete), in both Studio and the CLI, so the surface is obvious —
one button to make a profile's content usable by the agents, one to mark
it active.

Key distinction that shapes the design: **indexing ≠ generation.**
`Analyze` is generation and does not belong under an "Index" button.

## Decisions (confirmed)

1. **Code `Analyze` is removed from Settings.** Code-grounded generation
   already happens automatically in the main Runs / analyze flow: when an
   active code profile is indexed, the Code Agent participates. No
   separate "analyze" button is needed in Settings.
2. **`Index` is smart**, not a dumb full rebuild: incremental when the
   collection's embedding identity matches the active model; full rebuild
   only when the model changed (identity mismatch) or the collection is
   empty. No wasted re-embedding on every click.
3. **CLI: only the index-variants merge.** `/docs scan|ingest|reindex` →
   `/docs index`; `/code scan|+cols|refresh` → `/code index`. Read-only
   power-user commands stay: `/docs search-docs`, `/code search`.
4. **Code `Index` always includes column references** (folds in `+Cols`)
   plus the semantic code index — one richer, predictable operation
   instead of a confusing fast/slow choice.

## Design

### Studio — profile rows

* **Docs** (`Settings → Docs`): `Active/Activate` · **`Index`** · Edit ·
  Delete. Remove the `Scan`, `Ingest`, `Reindex` buttons.
* **Code** (`Settings → Code`): `Active/Activate` · **`Index`** · Edit ·
  Delete. Remove `Scan`, `+Cols`, `Analyze` (and the analyze dialog wired
  to this tab).

`Active` is unchanged on both — it marks the default profile the agents
use.

### `Index` behaviour (smart, both sides)

**Docs** (`POST /api/docs/index`, worker):
1. Open `RAGStore`. On `EmbeddingProviderMismatch`, force-drop `amx_docs`
   and reopen (re-stamps the active provider/model).
2. If a mismatch was recovered (or the collection is empty) →
   `reset_collection()` then full ingest — a clean rebuild under the
   active embedding.
3. Otherwise → `ingest(refresh=False)` — incremental add of new/changed
   files, no re-embedding of unchanged chunks.

**Code** (`POST /api/code/index`, worker):
1. Collect the active DB profile's table **and column** names (so source
   references resolve), then `analyze_codebase(..., column_names=...,
   index_semantic=True)` — builds the static reference report AND upserts
   code chunks into `amx_code` (incremental upsert).
2. On `CodeEmbeddingMismatch`, drop `amx_code` and re-index under the
   active code-embedding model.

Both operations are idempotent and safe to click repeatedly: steady
state is cheap (incremental), a model change triggers exactly one
rebuild.

### Generation (the removed Code `Analyze`)

No replacement button. Code-grounded descriptions are produced by the
normal analyze run (Studio **Runs**, CLI `/run`) whenever an active code
profile has been indexed — the orchestrator's Code Agent already consumes
the report + `amx_code` retrieval. Removing the Settings button removes a
duplicate generation entry point, not a capability.

### CLI

* **Merge into one verb each:**
  * `/docs index` replaces `/docs scan`, `/docs ingest`, `/docs reindex`
    (smart: incremental, rebuild on embedding mismatch).
  * `/code index` replaces `/code scan` (+ the column flag) and
    `/code refresh` (smart: incremental upsert, rebuild on code-embedding
    mismatch; always includes columns).
* **Keep (read-only power-user):** `/docs search-docs`, `/code search`,
  and `/docs export-report` (confirmed: stays).
* **Remove:** `/code analyze` — generation belongs to the main
  `/run`/analyze flow (consistent with dropping the Studio button).

### Backend

* New `POST /api/docs/index` and `POST /api/code/index` jobs implementing
  the smart logic above. Reuse the existing docs ingest worker
  (`_ingest_worker_body`, which already grew a reindex/mismatch path) and
  the code scan worker (`_scan_worker_body` + `analyze_codebase`); add a
  "smart index" mode that resets only on mismatch.
* The drag-drop `/api/docs/upload` post-save step calls the same smart
  docs-index op instead of a bare ingest.
* The old `/api/docs/{scan,ingest,reindex}` and
  `/api/code/{scan,analyze}` endpoints stop being UI-facing. They are
  folded into `index` (CLI shares the same op). The standalone analyze
  endpoint is retained only if the main analyze flow still calls it
  internally — to be confirmed during implementation; otherwise removed.

### Cross-cutting

* CLI invocation stays inside the existing namespace tabs (`/docs …`,
  `/code …`); no new top-level groups (per AMX CLI rule).
* Studio-visible → ship order is `deploy.sh → PR → merge`.

## Verification

1. **Docs index — steady state:** index twice with no file changes; the
   second run is incremental (no full re-embed), chunk count stable.
2. **Docs index — model change:** switch the docs embedding, click
   `Index`; collection is dropped and rebuilt, re-stamped with the active
   model, `/ask` docs work (no mismatch error).
3. **Code index:** `Index` produces table + column references and a
   populated `amx_code`; a code-embedding change triggers a rebuild.
4. **Generation still code-grounded:** a normal analyze run with an
   active, indexed code profile still uses code evidence (no regression
   from removing the Analyze button).
5. **CLI parity:** `/docs index` and `/code index` behave like their
   Studio buttons; `/docs search-docs` and `/code search` still work;
   `/code analyze` and the merged index-variants are gone.
6. **No dead UI:** removed buttons/dialogs leave no orphaned state or
   handlers; `grep` shows no references to the dropped endpoints from the
   SPA.

## Sequencing (branch interaction)

The pending `feat-docs-reindex-ui` branch carries three commits not yet on
`main`: the critical chromadb `name()` fix, the doc-status-line fix, and
the Reindex button + reindex backend. The reindex backend is exactly the
"rebuild" path smart `Index` reuses.

Decision: **merge `feat-docs-reindex-ui` first** (lands the critical fixes
and the reindex backend), then implement Index+Active on top in a second
PR that removes the now-redundant Reindex/Ingest/Scan/+Cols/Analyze
buttons and CLI subcommands and folds their backend into smart `index`.
The Reindex button existing for one PR cycle is acceptable; delaying the
already-deployed critical `name()` fix from landing on `main` is not.

## Out of scope

* Changing what indexing/generation actually *does* internally (chunking,
  embedding model selection, agent prompts) — only the action surface and
  the smart wrapper change.
* The embedding-model management surface (`Settings → Embeddings`) — it
  stays as-is; `Index` consumes the active model it configures.
