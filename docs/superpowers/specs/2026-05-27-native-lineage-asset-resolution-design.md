# Native Lineage: Real Asset Names, Click-to-Open Assets, and Source Deep-Links

**Date:** 2026-05-27
**Status:** Approved (design)
**Follows:** `2026-05-26-databricks-native-lineage-fetch-design.md`

## Background

The native (database-side) lineage fetch pulls a table's upstream/downstream
graph straight from Databricks Unity Catalog and renders it on the Studio
lineage canvas. The first release works, but three problems block real use:

1. **Notebook names never resolve.** The Unity Catalog lineage response
   identifies a notebook only by a numeric `object_id`. To turn that into a
   name, the current code recursively scans the *entire* workspace folder tree
   (`/api/2.0/workspace/list`) to build an `object_id → name` map. That scan is
   capped at a 40-second wall-clock budget so it can never hang a fetch — and
   on a real workspace it never finishes, so most notebooks keep a bare
   `notebook <id>` label. The log line
   `workspace index: hit 40s budget, partial map (44 notebooks)` is the symptom.

2. **Notebooks and jobs stay `name_only` and are not drillable.** Even when a
   name resolves (jobs already resolve via `/api/2.2/jobs/get`), the canvas node
   is a greyed-out `name_only` ghost: there is no backing `remote_*` row, so the
   "open in Assets" affordance never appears. The user must copy the id and
   search for it by hand in Databricks.

3. **The Databricks logo badge is decorative.** A backend logo is auto-bound to
   the table-node header (`Canvas.tsx` via `logoKeyForBackend`), but clicking it
   only opens the logo picker. In the Databricks UI the equivalent affordance is
   a link that opens the asset in the workspace.

## Goals

- Notebook and job nodes show their **real names** on the canvas.
- Asset nodes are **clickable** and open inside AMX's Assets view; the per-asset
  content is fetched **lazily on click**, never during the lineage fetch.
- A clicked asset is **cached** (the existing Assets `remote_*` store), so
  reopening it consumes no further tokens on the active LLM and triggers no
  re-fetch.
- The header logo becomes a functional **"open in Databricks"** deep-link.
- The lineage fetch gets **faster** (no full-workspace scan, no eager content
  ingest); no performance regression on any path.

## Non-Goals

- Column-level lineage (already intentionally disabled upstream).
- Backends other than Databricks (the seams stay backend-agnostic, but only
  the Databricks provider is implemented here).
- Bulk pre-ingest of every discovered asset.

## Architecture Decision (chosen direction)

**Names are resolved eagerly and cheaply; content is fetched lazily on click and
cached.** The lineage fetch resolves display names only and records each asset
as a clickable, not-yet-ingested node. Content ingest happens the moment the
user opens the node, reusing the existing Assets ingest machinery, and the
result persists in the Assets store.

## Component Design

### A. Name resolution (fixes problems 1 and 2a)

- **Remove the full-workspace scan.** `amx/lineage/native/workspace_index.py`'s
  recursive `list_workspace_objects` scan and its 40-second budget are deleted.
- **Resolve only the notebooks present in the fetched result.** A lineage graph
  for one table references a handful of notebooks, not the whole workspace. For
  each notebook node carrying an `object_id`, resolve its name via
  `get-status?object_id=<id>` → `path` → basename. Jobs already resolve via
  `/api/2.2/jobs/get`; pipelines/queries already resolve in the provider.
- **Outcome:** real names on the canvas, the `hit 40s budget` log line is gone,
  and the fetch no longer blocks on a workspace scan.

**Open verification (first implementation step):** confirm that
`/api/2.0/workspace/get-status` accepts an `object_id` query parameter against a
live workspace. The codebase already assumes this in
`DatabricksWorkspaceClient.path_for_object_id` (used at
`databricks.py:1159`), but that branch may be untested.
- If supported → per-node resolution as above; no scan anywhere.
- If not supported → fall back to a **persisted** `object_id → path` index
  (SQLite-backed, built/refreshed in the background, off the fetch path). The
  fetch still never blocks; a cold index simply leaves a placeholder until the
  background build catches up. The 40-second in-process budget is not
  reintroduced.

### B. Click-to-open assets with lazy ingest (fixes problem 2b)

- **Drop eager ingest from the fetch path.** Remove the `ingester` wiring in
  `service.py` and the `self.ingester(...)` call in `materializer.py`. Asset
  nodes are materialized as `name_only` rows that carry `kind` + `external_id`
  and an "ingestable" flag — enough for the frontend to request ingest later.
- **New endpoint:** `POST /api/lineage/asset/ingest` with body
  `{ profile, kind, external_id }`. It invokes the existing
  `IngestAssetsService` with a single-asset `selection`, creating the matching
  `remote_<kind>s` row, then returns the `remote_id` (and the Assets-page
  `asset_id`) so the caller can open and drill into it. Lives in its own module
  (e.g. `amx/lineage/native/lazy_ingest.py`), not bolted onto the materializer.
- **Frontend `AssetNode`:** when a node is `name_only` and ingestable, the
  primary click (or an explicit action button) calls the endpoint, shows a
  spinner, and on success opens the existing
  `/assets?kind=<kind>&id=<id>` drawer; the node upgrades to `full` and becomes
  drillable.
- **Cache:** the created `remote_*` row is the Assets cache. Reopening the asset
  reads from it — no re-fetch, no additional token-consuming work.

### C. Databricks deep-link logo (fixes problem 3)

- Make the header logo badge a link that opens the asset in the Databricks
  workspace in a new tab. URL is built per node kind from the profile `host`:
  - table → `/explore/data/<catalog>/<schema>/<table>`
  - notebook → `/editor/notebooks/<object_id>`
  - job → `/jobs/<job_id>`
  - pipeline → `/pipelines/<pipeline_id>`
  - query → `/sql/editor/<query_id>`
- URL construction lives in one small helper (`databricksDeepLink(kind, host,
  …)`). The lineage graph API surfaces `host` and `externalId` per node so the
  frontend can build the link. Exact path formats are confirmed against the live
  workspace during implementation; an unknown kind falls back to no link rather
  than a broken one.

## Data Flow

```
fetch (per table)
  └─ provider.fetch_table_lineage  → resolve names (cheap REST, per node)
  └─ materializer                  → record asset nodes as name_only +
                                     external_id + kind + ingestable; NO content
canvas render
  └─ lineage graph API             → node data carries kind, externalId, host
  └─ AssetNode                     → name_only+ingestable ⇒ clickable
                                   → logo badge ⇒ Databricks deep-link
click asset
  └─ POST /api/lineage/asset/ingest → IngestAssetsService(selection=one)
                                    → remote_<kind>s row (cached)
                                    → open /assets?kind&id ; node ⇒ full
```

## Modularity and Constraints

- Lazy-ingest logic in its own module; `materializer.py` is not enlarged with a
  second responsibility.
- The deep-link helper is a single small frontend utility.
- English-only across all tracked content; cross-platform (no POSIX-only paths
  or shells); no misleading feature-gating wording.
- CI is intentionally skipped for this branch per the current working
  agreement, but the files this change touches are left green — no test is
  weakened or skipped to pass.

## Verification and Risks

- **`get-status?object_id` support** is the one external unknown; it is verified
  first, with the persisted-index fallback specified above.
- **Performance:** the fetch path *loses* work (no scan, no eager ingest), so it
  gets faster; lazy ingest moves cost to an explicit user action. No critical
  path slows down.
- **Idempotency:** anchor-scoped re-fetch behavior in the materializer is
  preserved; a previously lazy-ingested asset stays `full` across re-fetches.

## Deployment

This change is Studio-visible (lineage canvas). For this iteration the user has
explicitly chosen the order **PR + merge → `deploy.sh`** (testing happens on a
different machine this time), an intentional exception to the usual
deploy-first order. Confirm before executing the deployment step.
