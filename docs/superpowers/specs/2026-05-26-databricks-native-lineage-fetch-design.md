# Native database lineage fetch — design

**Date:** 2026-05-26
**Status:** Draft for review
**Scope:** Make the AMX Lineage feature able to fetch lineage directly from a
database's own lineage system (Databricks Unity Catalog first), route every
discovered entity to its correct home in AMX, and feed the result into the
existing lineage graph, ASK, Pages, and Run.

---

## 1. Problem & motivation

Databricks Unity Catalog renders a per-table lineage graph: a central table,
its upstream **producers** and downstream **consumers** (notebooks, jobs,
pipelines, dashboards, queries), and related non-table assets such as a vector
search index. AMX should be able to read that same lineage and surface it.

Today AMX already reads *some* Unity Catalog lineage, but with three gaps:

1. **Source requires elevated privileges.** The current extractor
   (`amx/lineage/extractors/system_tables/databricks.py`) reads
   `system.access.table_lineage` / `column_lineage`. In most organizations the
   `system.access.*` schemas are **not** granted to an individual developer —
   they require metastore-admin level access. AMX is an app each individual
   user runs with their own token, so it cannot depend on this source.
2. **Only table↔table and column↔column edges are captured.** The producer /
   consumer **non-table** endpoints (notebook, job, pipeline, dashboard) shown
   in the UC graph are never read — the extractor pulls only
   `source_table_full_name` / `target_table_full_name` and silently drops
   anything that is not a 3-part `catalog.schema.table` FQN
   (`databricks.py:131-139,166,238`). Vector search indexes, ML models, and
   dashboards are not modeled at all.
3. **Run does not consume lineage.** ASK and Pages already read lineage
   (`amx/search/_agent/retrieval.py:172`, `amx/pages/context.py:32`), but the
   Run / analyze flow only resolves `asset_references_table` to map an attached
   asset to its tables (`amx/analyze/asset_context.py:201-214`) — it has no
   upstream/downstream or producer/consumer awareness.

The product goal stated by the user: a user enters the Lineage feature, runs a
**"fetch lineage"** command, picks a table with a wizard picker (exactly like
`runs`), and the system fetches the lineage behind that table. **Whatever the
lineage contains is taken and routed to its correct home** — a notebook goes
under Assets, a table under the database — but everything is collected together
under Lineage. Crucially, this must work **even when the user can only see the
name** of a related table / view / notebook and cannot inspect its contents;
when privileges do exist, AMX ingests everything (caches the table, files the
notebook under Assets, and so on).

---

## 2. Goals & non-goals

### Goals
- Fetch lineage for a user-picked table from the database's native lineage
  system, owned by the existing Lineage feature.
- Work for an **individual user with ordinary table visibility** — no
  dependency on `system.access.*` grants.
- Capture the full UC graph shape: upstream/downstream tables **and** the
  producer/consumer non-table assets, plus the vector search index from the
  reference screenshot.
- **Privilege-tiered materialization**: full ingest where access exists;
  name-only "ghost" nodes where it does not. Never drop a discovered entity.
- Route each entity to its correct AMX home (table → catalog, notebook/job/etc.
  → Assets) while linking everything in the lineage graph.
- Surface the richer graph in the Studio canvas + `/lineage` CLI.
- Feed the result into ASK, Pages, and **Run** (new for Run).

### Non-goals
- Building Snowflake / BigQuery providers now (the provider seam is designed
  for them, but only Databricks is implemented).
- Replacing the existing heuristic / FK / view-DDL / LLM extractors — native
  lineage is an **additional** signal, not a replacement.
- Removing the `system.access.*` extractor — it stays as an optional bulk
  accelerator for profiles that happen to have access.
- A full BI-dashboard or ML-model registry integration beyond representing them
  as lineage nodes.

---

## 3. Architecture overview

```
/lineage fetch (CLI wizard)  ─┐
Studio "Fetch lineage" button ─┴──► LineageFetchService (amx/lineage/native/service.py)
                                          │
                                          │ picks provider by profile backend
                                          ▼
                              NativeLineageProvider (protocol)
                                          │
                              DatabricksLineageProvider
                                  (REST: /api/2.0/lineage-tracking/*)
                                          │
                                  raw upstream/downstream + entity lineage
                                          ▼
                              LineageMaterializer
                              ├─ table  → catalog entity (cache if accessible,
                              │           else name-only ghost)
                              ├─ notebook/job/pipeline/query/dashboard
                              │         → Assets bridge row (full ingest if
                              │           accessible, else name-only)
                              ├─ vector_search_index → new entity_kind
                              └─ unknown UC type → generic `external` node
                                          ▼
                              catalog_entities + catalog_relationships
                                          ▼
              ┌───────────────┬───────────────┬──────────────┐
              ▼               ▼               ▼              ▼
        Studio canvas    /lineage CLI       ASK            Pages / Run
```

### Why REST, not system tables
The **Databricks Lineage Tracking REST API** is the primary source:
- `GET /api/2.0/lineage-tracking/table-lineage` — body/params
  `{ table_name, include_entity_lineage: true }`. Returns `upstreams` and
  `downstreams`; each item carries a `tableInfo` and, when
  `include_entity_lineage` is set, the entity producers/consumers
  (`notebookInfos`, `jobInfos`, `pipelineInfos`, `dashboardInfos`, query infos).
- `GET /api/2.0/lineage-tracking/column-lineage` — body/params
  `{ table_name, column_name }`. Returns `upstream_cols` / `downstream_cols`.

This API authorizes against **table visibility** (USE CATALOG / USE SCHEMA +
SELECT or ownership) rather than metastore-admin, so it works for the
individual-user model. It returns related entity **names** even when the caller
cannot open them — satisfying the name-only requirement directly.

> **Verification item (must confirm against live API before/while implementing):**
> exact JSON field names (`upstreams[].tableInfo.name`, `notebookInfos[].name`,
> HTTP verb GET vs POST, and whether the endpoint is paginated). Whether a
> vector search index appears in lineage-tracking responses at all, or whether
> it must be discovered via the separate Vector Search API
> (`/api/2.0/vector-search/indexes`) and linked by its backing Delta table.
> The design below is written so the field mapping lives in one adapter method;
> confirming the shape changes only that method, not the architecture.

---

## 4. Phase 1 — Fetch & taxonomy (keystone)

### 4.1 New module: `amx/lineage/native/`
Kept separate from the existing `extractors/system_tables/` package so the
two sources stay decoupled (house rule #8 — one responsibility per module).

```
amx/lineage/native/
  __init__.py
  provider.py        # NativeLineageProvider protocol + value types
  databricks.py      # DatabricksLineageProvider (REST)
  service.py         # LineageFetchService: orchestrates fetch + materialize
  materializer.py    # LineageMaterializer: routes entities to homes
```

#### `provider.py` — the cross-backend seam
```python
class NativeLineageNode:        # value object
    kind: str                   # 'table' | 'notebook' | 'job' | 'pipeline'
                                # | 'dashboard' | 'query' | 'vector_search_index'
                                # | 'external'
    fqn: str | None             # 3-part FQN for tables; None for assets
    name: str                   # always present (the name-only guarantee)
    external_id: str | None     # workspace object id / job id / index name
    columns: list[str] | None   # populated only when metadata is accessible

class NativeLineageEdge:
    source: NativeLineageNode
    target: NativeLineageNode
    direction: str              # 'upstream' | 'downstream'
    from_column: str | None
    to_column: str | None

class NativeLineageResult:
    anchor: NativeLineageNode
    edges: list[NativeLineageEdge]

class NativeLineageProvider(Protocol):
    backend: str                # 'databricks'
    def fetch_table_lineage(self, fqn: str, *, with_columns: bool) -> NativeLineageResult: ...
```

A registry maps `profile.backend -> provider factory`. Only `databricks` is
registered now; Snowflake (`ACCOUNT_USAGE.OBJECT_DEPENDENCIES`) and BigQuery
(`INFORMATION_SCHEMA.JOBS_BY_PROJECT` / Data Catalog) slot in later behind the
same protocol.

#### `databricks.py` — REST provider
- Extends the existing `DatabricksWorkspaceClient`
  (`amx/db/adapters/_databricks_workspace.py`) with `table_lineage()` and
  `column_lineage()` methods (same `_get` / `_raise_if_error` plumbing, reusing
  `DatabricksAuthError` for 403 handling).
- Maps the REST response to `NativeLineageResult`. All field-name coupling lives
  here (see verification item).
- Column lineage is fetched lazily: only for the anchor's columns, and only when
  `with_columns=True`, to bound request count.

### 4.2 `LineageFetchService`
Given `(profile_name, fqn, with_columns)`:
1. Resolve provider by backend; if none, return a clear "native lineage not
   supported for this backend yet" result.
2. Call `provider.fetch_table_lineage(...)`.
3. Hand the `NativeLineageResult` to `LineageMaterializer`.
4. Return counts `{tables, assets, columns, name_only, edges}` for CLI/Studio
   progress display.

Wrapped so a provider-level failure (network, auth on the anchor itself) is
reported to the user rather than crashing — but a 403 on an *individual related
entity* degrades only that node (see materializer).

### 4.3 `LineageMaterializer` — privilege-tiered routing
For each node in the result:

| Node kind | Tier: access available | Tier: name-only (403 / not visible) |
|-----------|------------------------|--------------------------------------|
| table | Ensure catalog entity exists; trigger metadata cache via existing ingest path | Insert `entity_kind='table'` ghost row with `metadata_state='name_only'` |
| notebook / job / pipeline / query | Create/ensure Assets bridge row (existing `source_remote_id` pattern); optionally deep-ingest | Bridge row with `metadata_state='name_only'` |
| dashboard | New `entity_kind='dashboard'` bridge row | Same, name-only |
| vector_search_index | New `entity_kind='vector_search_index'` row, linked to backing table | Same, name-only |
| unknown UC type | Generic `entity_kind='external'` node | Same, name-only |

Then upsert edges into `catalog_relationships`:
- table↔table: `relationship_type='lineage_native_table'`
- column↔column: `relationship_type='lineage_native_column'`
  (`from_column`/`to_column`)
- asset→table producer/consumer: `relationship_type='lineage_native_asset'`
  with `from_entity_kind`/`to_entity_kind` (v6 columns already exist) and a
  read/write `direction` recorded in `details_json`.
- `source='databricks_native_lineage'` for provenance; idempotent
  delete-and-reinsert per `(relationship_type, source, profile)` slice, mirroring
  the existing extractor (`databricks.py:184-207`).

**Never drop:** any node that fails to map to a known kind becomes an `external`
node by name. This is the explicit contrast with today's silent-drop behavior.

### 4.4 Taxonomy & storage changes
- Add to `catalog_entities.entity_kind` the values `vector_search_index`,
  `dashboard`, `external`. (`notebook|job|pipeline|query|stream|streamlit_app`
  already exist.)
- Add a `metadata_state` column to `catalog_entities`:
  `'full' | 'name_only'` (default `'full'`), marking lineage-discovered ghosts.
- **House rule #5:** every new column/table gets a non-empty entry in
  `amx/storage/schema_descriptions.py` in the same commit; both
  `tests/test_local_schema_comments.py` and
  `tests/test_shared_schema_comments.py` must stay green. New `entity_kind`
  values and the `metadata_state` column ship with descriptions.
- Shared schema (`amx/storage/shared_schema.py`) mirrors the column with a
  non-empty `comment=` and DDL `COMMENT ON`.

### 4.5 CLI: `/lineage fetch`
Registered under the existing `/lineage` Click group
(`amx/cli_support/commands/lineage.py`). Wizard-first (house rule, the
wizard-first preference): bare invocation walks
profile → database → schema → table picker (reusing the same picker
infrastructure as `runs`). Flags (`--profile`, `--table`, `--with-columns`) are
optional power-user shortcuts. Output: a summary table of what was fetched and
how many nodes are full vs name-only, then a pointer to view the graph
(`/lineage show` or Studio).

### 4.6 Studio: "Fetch lineage" button
A button on the table page / lineage canvas that calls a new
`POST /api/lineage/fetch` endpoint `{profile, fqn, with_columns}` →
`LineageFetchService`. Returns the fetch summary; the canvas then refreshes via
the existing `lineage_for_studio` read path.

---

## 5. Phase 2 — Frontend

- Extend the frontend `NodeKind` / `AssetNodeKind` unions
  (`frontend/src/lineage-canvas/types.ts:12,104-110`) with
  `vector_search_index`, `dashboard`, `external`.
- Add node renderers + entries in `frontend/src/lineage-canvas/nodes/registry.ts`
  — `vector_search_index` and `dashboard` can reuse the `AssetNode` component
  with new icon/accent; `external` gets a muted generic node.
- **Name-only rendering:** ghost nodes (`metadata_state='name_only'`) render
  greyed with a "name only" badge and no expandable column rail / drill-in,
  so the relationship is visible even without privileges. The Studio payload
  (`lineage_for_studio`, `amx/lineage/service.py:606-725`) gains a
  `metadataState` field per node.
- **Producer / consumer grouping:** optional affordance grouping upstream
  asset producers ("writes") and downstream consumers ("reads") to echo the UC
  "Assets that write / read data" buckets, reusing the existing read/write
  `direction` already stored on edges.
- Translate the Turkish comment at
  `frontend/src/lineage-canvas/Canvas.tsx:707` to English while that file is
  touched (English-only tracked-content rule).

---

## 6. Phase 3 — Downstream consumption

### Run (new)
- Add a `lineage_context` field to `AgentContext` (`amx/agents/base.py:222`).
- Populate it per-table in the orchestrator
  (`amx/agents/orchestrator.py:202,682-683`, keyed by
  `(schema.lower(), table.lower())`) via `build_lineage_evidence`
  (`amx/lineage/evidence.py:49`).
- Render an upstream/downstream + producer/consumer block in the ProfileAgent
  prompt (`amx/agents/profile_agent.py:784,886`). The table being described
  gains awareness of its parents, downstream views, and the assets that touch it.

### ASK (already wired — extend)
- The auto-injected evidence
  (`amx/search/_agent/retrieval.py:172` → `build_lineage_evidence`) and the
  LLM-callable `lineage_for_table` / `lineage_for_column` tools
  (`amx/search/_tool_lineage.py`) pick up the new `lineage_native_*`
  relationship types and new entity kinds automatically once the edges exist.
  Verify the tool's relationship-type filter includes the new types and that
  asset producers/consumers surface in `asset_edges`.

### Pages (already wired — extend)
- `_resolve_lineage` (`amx/pages/_resolver.py:189`) renders the new node kinds
  in its edge sections; confirm `list_artifact_edges`
  (`amx/lineage/store.py:364-522`) surfaces the new `from_kind`/`to_kind`
  values (it already returns kinds generically).

---

## 7. Permissions & graceful degradation

- **Primary path never needs `system.access.*`.** REST lineage-tracking
  authorizes on table visibility.
- A `403 / DatabricksAuthError` on the **anchor** table's lineage call is
  reported to the user as "you don't have lineage access to this table".
- A `403` on an **individual related entity's** metadata fetch degrades that one
  node to `name_only` and continues — it never fails the whole fetch (mirrors
  the existing `_run` swallow at `databricks.py:363-371`).
- The `system.access.*` extractor remains available as an optional bulk
  accelerator for profiles that do have metastore access; it is no longer the
  only or primary source.

---

## 8. Cross-cutting constraints

- **Cross-platform (rule #10):** REST via `requests`; no POSIX-only paths,
  shells, or signals introduced.
- **Modularity (rule #8):** new `amx/lineage/native/` package, one
  responsibility per file; the REST field mapping is isolated in
  `databricks.py`.
- **Schema descriptions (rule #5):** mandatory entries for the new column and
  reflected in both schema-comment tests.
- **Studio deploy order (rule #6):** this change is Studio-visible (new node
  kinds, fetch button), so integration order is `deploy.sh` → PR → merge.
- **English-only / OSS-neutral wording / no agent attribution:** apply to all tracked
  output, including the `Canvas.tsx` comment translation.
- **No performance regression:** the new fetch is user-initiated and per-table,
  off the hot path; the existing refresh/render paths are unchanged.

---

## 9. Testing strategy

- **Provider unit tests** with a mocked HTTP client: REST response →
  `NativeLineageResult` mapping, including entity lineage and missing-field
  resilience.
- **Materializer unit tests**: full-access vs name-only routing per kind;
  unknown type → `external`; idempotent re-fetch (no duplicate edges).
- **Auth degradation test**: 403 on a related entity yields a name-only node and
  a successful overall fetch; 403 on the anchor surfaces a clear error.
- **Taxonomy tests**: `test_local_schema_comments.py` /
  `test_shared_schema_comments.py` stay green with the new column + kinds.
- **CLI test**: `/lineage fetch` wizard picker flow (extends
  `tests/cli/test_db_assets_commands.py` patterns).
- **Web test**: `POST /api/lineage/fetch` happy path + the new `metadataState`
  field in `lineage_for_studio` output.
- **Run consumption test**: `lineage_context` populated and rendered in the
  ProfileAgent prompt.
- **CI green (Lint + Format + Type + Tests) on Ubuntu + Windows** before merge.

---

## 10. Open verification items (resolve during implementation)

1. Exact Databricks lineage-tracking REST field names, HTTP verb, and pagination.
2. Whether vector search indexes appear in lineage-tracking responses or require
   the separate Vector Search API, linked by backing Delta table.
3. Whether dashboards arrive as a distinct entity type or under a generic entity
   bucket in the REST response.
4. Minimum token scopes the REST endpoints require, to document for users.

---

## 11. Implementation order

1. **Phase 1a** — taxonomy + storage (`entity_kind` values, `metadata_state`
   column, schema descriptions, tests green).
2. **Phase 1b** — `amx/lineage/native/` provider + materializer + service, unit
   tested with mocks.
3. **Phase 1c** — `/lineage fetch` CLI + `POST /api/lineage/fetch`.
4. **Phase 2** — frontend node kinds, name-only rendering, producer/consumer
   grouping, comment translation.
5. **Phase 3** — Run `lineage_context`; verify ASK / Pages pick up new types.

Each phase leaves CI green and the codebase at least as modular as it found it.
