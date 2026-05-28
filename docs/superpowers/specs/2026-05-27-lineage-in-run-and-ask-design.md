# Deepening Native Lineage Use in RUN and ASK

**Date:** 2026-05-27
**Status:** Approved (design)
**Follows:** `2026-05-26-databricks-native-lineage-fetch-design.md`,
`2026-05-27-native-lineage-asset-resolution-design.md`

## Background

`/lineage fetch` now pulls a table's upstream/downstream graph straight from
Databricks Unity Catalog — table↔table edges **plus** the producer/consumer
assets (notebooks, jobs, pipelines, dashboards, queries) with real resolved
names. The materializer writes these into `catalog_relationships` as
`lineage_native_table` / `lineage_native_column` / `lineage_native_asset` edges,
each carrying `details_json.direction`, `from/to_entity_kind`, and a
`metadata_state` of `full` or `name_only`.

Both RUN (`/analyze run` description generation) and ASK (natural-language
catalog Q&A) already read these edges — but shallowly. This design deepens that
use on both surfaces under a hard performance constraint: **accuracy and latency
both matter**; ASK must not get slower or consume materially more tokens.

### Verified current state

| | RUN | ASK |
|---|---|---|
| Wired at all? | Yes, but **web-only** — CLI `/analyze run` gets no lineage (`resolve_lineage_context_for_run` is invoked only at `web/routers/runs.py:1095`) | Yes, two paths |
| Always-on path | Name-only neighbour blocks, **1-hop** (`amx/analyze/lineage_context.py`), rendered as bare lines in the ProfileAgent prompt (`amx/agents/profile_agent.py:905-918`) | `build_lineage_evidence` (`amx/lineage/evidence.py`) is **canvas-gated** (requires a saved `lineage_artifacts` row) and returns raw **entity IDs**, printed literally in the prompt appendix |
| Deep path | Asset **content** exists but in a **separate** `asset_context_by_table` block (web-only, `web/routers/runs.py:1065-1080`), not joined to the lineage edges | Tool path `_tool_lineage_for_table/column` (`amx/search/_tool_lineage.py`) — named edges + `asset_edges` with `remote_id` for a `describe_asset` chain — but only fires **if the LLM chooses to call it** |
| Multi-hop | Unused | Unused (a recursive CTE exists in `amx/lineage/store.py:list_artifact_edges` but neither RUN nor ASK uses it) |

**Bottom line:** AMX fetches a rich Databricks asset graph and then flattens it
to names (RUN) or numeric IDs (ASK) on the always-on path. The good paths are
web-only (RUN) or opportunistic (ASK).

## Goals

- RUN descriptions are lineage-aware on **both CLI and Studio** (close the
  web-only parity gap).
- RUN can describe *how* a table is produced/consumed — neighbour table
  descriptions and short producer-asset excerpts reach the ProfileAgent prompt,
  not just neighbour names.
- ASK's **always-on** enrichment surfaces real neighbour **names** (not entity
  IDs) and works **without a saved canvas**, so freshly fetched native lineage
  informs answers immediately.
- ASK can answer "where does this data come from / what consumes it" from named
  native assets **without a tool round-trip**.
- A single shared neighbour-query core serves both surfaces, so the graph walk
  lives in exactly one place.
- No new LLM round-trips on either hot path; no >5% latency or token regression.

## Non-Goals

- Re-enabling column-level lineage (intentionally disabled upstream; its REST
  shape is unverified). Out of scope here.
- Any **live** Databricks fetch inside the RUN/ASK hot path. Both surfaces read
  only already-materialized local data; live fetch stays behind explicit
  `/lineage fetch`.
- Lineage-aware RAG (precomputing per-table lineage summaries into the vector
  index). Noted as a possible future layer, not built here.
- Backends other than Databricks. The shared core is backend-agnostic, but only
  the native edges already materialized are consumed.

## Architecture Decision (chosen direction)

**Approach A — shared bounded core + deterministic enrichment.** A single
helper produces a name-resolved, bounded, optionally shallow-multi-hop neighbour
set from `catalog_relationships`. RUN and ASK consume it through thin adapters.
All enrichment is assembled **before** the LLM call, so neither surface gains an
extra round-trip. The existing ASK tool path stays as the deep, agentic
escalation for explicit lineage questions.

Two alternatives were considered and rejected as the default:

- **Agentic-first (B):** add multi-hop lineage tools and let the LLM drive every
  lineage question. Most flexible, but each question pays extra LLM round-trips —
  directly against the latency/token constraint.
- **Lineage-aware RAG (C):** embed per-table lineage summaries into the vector
  index at ingest time. Deepest retrieval, but heavy (index schema changes,
  recompute cost, storage) and higher risk. A possible future layer on top of A.

## Component Design

### A. Shared core — `lineage_neighbors()`

A new helper (its own module under `amx/lineage/`, e.g.
`amx/lineage/neighbors.py`) is the single place the native-lineage graph walk
lives.

- **Signature (shape):**
  `lineage_neighbors(store, *, profile, anchor_entity_ids, depth=1, fanout, rel_types)`
  returns, per anchor, a bounded list of neighbours:
  `{direction, kind, name, relationship, remote_id?, metadata_state}`.
- **Name-resolved:** joins `catalog_relationships` to `catalog_entities` on both
  sides so every neighbour carries a human name (table FQN or asset display
  name), never a raw entity ID.
- **Profile-scoped, canvas-free:** reads `catalog_relationships` directly,
  filtered by `db_profile`. No dependency on saved `lineage_artifacts`.
- **Bounded:** `depth` defaults to **1**, capped at **2**; per-direction
  `fanout` cap; SQL `LIMIT`. Multi-hop reuses the recursive-CTE pattern already
  proven in `store.list_artifact_edges`, anchored on catalog entity IDs rather
  than a saved artifact.
- **Relationship types:** native (`lineage_native_table/column/asset`) plus FK /
  `view_depends_on` / `asset_references_table`, matching today's RUN set;
  `join_inference` excluded by default.

RUN and ASK each wrap this with a thin adapter that shapes the result into the
form their prompt expects. The existing `resolve_lineage_context_for_run`
(`amx/analyze/lineage_context.py`) and the always-on branch of
`build_lineage_evidence` (`amx/lineage/evidence.py`) are refactored to call the
shared core instead of carrying their own one-hop queries.

### B. RUN deepening

1. **CLI parity.** Wire lineage context (and the ingested-asset context block)
   into the CLI run path the same way the web worker does, so `/analyze run`
   from the REPL produces lineage-aware descriptions. Today only
   `web/routers/runs.py` populates `orchestrator.lineage_context_by_table`; the
   CLI orchestration in `amx/cli_support/commands/_analyze/run_loop.py` must do
   the same via the shared resolver.
2. **Enriched neighbour blocks.** Each neighbour block may carry a **truncated**
   description, read from the local catalog via
   `catalog_entities.effective_description_id → catalog_descriptions.description_text`.
   This works for a neighbour table *and* for any asset that already has a
   generated description, so the ProfileAgent can write "feeds
   `orders_summary` (described as 'daily revenue rollup')" rather than naming the
   neighbour only. The description is truncated to a fixed character cap and the
   existing `_MAX_BLOCKS_PER_TABLE = 12` still bounds block count. A neighbour
   with no description contributes its name only — never a live fetch. (Full
   notebook/query *body* excerpts remain the existing user-attached
   `asset_context` path; auto-linking lineage-discovered assets to that path is a
   separate follow-up, out of scope here.)
3. **Multi-hop deferred behind a measured toggle.** RUN ships at **1-hop by
   default**. The shared core supports `depth=2` (upstream, capped fanout), but
   it stays off until the benchmark gate shows the extra hop earns its latency
   and token cost. This keeps the first release strictly performance-safe.
4. All of the above is assembled pre-LLM — no extra round-trip; cost is a few
   bounded SQLite reads plus bounded prompt text.

### C. ASK deepening

1. **Fix the always-on enrichment.** Replace the entity-ID output of
   `build_lineage_evidence` with **named** neighbours from the shared core, and
   **drop the canvas gate** for native edges — query `catalog_relationships`
   directly. Saved-canvas comments and logo keys remain as an *additive* signal
   when a canvas exists, layered on top of the always-available native edges.
2. **Named asset sources in the deterministic appendix.** The lineage appendix
   rendered into the system prompt (`amx/search/tool_agent.py`
   `_format_lineage_pages_appendix`) shows named upstream producers and
   downstream consumers, including native assets, so "where does this data come
   from?" is answerable without the LLM calling a tool.
3. **Tool path unchanged.** `_tool_lineage_for_table/column` stays the deep
   escalation (multi-hop, `asset_edges` → `describe_asset` body reads) for
   explicit, exploratory lineage questions. Because the always-on path now
   carries named lineage, the LLM should need the tool *less* often — a likely
   net token reduction.

## Data Flow

```
shared core
  └─ lineage_neighbors(store, profile, anchor_ids, depth, fanout)
       → reads catalog_relationships (native + FK + view + asset edges)
       → joins catalog_entities both sides → names + kind + remote_id
       → bounded (depth ≤ 2, fanout cap, LIMIT)

RUN (/analyze run, CLI and web)
  └─ run_loop / web worker → resolve_lineage_context_for_run (shared core)
                           → enrich blocks with neighbour descriptions +
                             ingested-asset excerpts (truncated, name_only ⇒ name)
  └─ Orchestrator → AgentContext.lineage_context
  └─ ProfileAgent prompt → "Lineage context" section (pre-LLM, one call)

ASK (/ask)
  always-on:
    └─ retrieval → build_lineage_evidence (shared core) → NAMED neighbours,
                   no canvas required (+ canvas comments/logos if present)
    └─ tool_agent appendix → named producers/consumers in system prompt
  deep escalation (LLM-driven, unchanged):
    └─ _tool_lineage_for_* → multi-hop edges + asset_edges → describe_asset
```

## Modularity and Constraints

- The graph walk lives in one new module (`amx/lineage/neighbors.py`); RUN's
  `lineage_context.py` and ASK's `evidence.py` become thin adapters over it
  rather than each owning a near-duplicate query. No file grows a second
  responsibility.
- English-only across all tracked content; cross-platform (no POSIX-only paths,
  shells, or signals); no misleading feature-gating wording.
- The files this change touches are left green — no test is weakened or skipped
  to pass. (CI on `main` is intentionally red on preexisting items; this branch
  does not chase that, only keeps its own surface clean.)

## Performance Plan (the gate)

- **No new LLM round-trips** on either hot path. RUN already assembles context
  before its single ProfileAgent call; ASK's always-on enrichment stays
  deterministic.
- **All reads local SQLite, `cache_ok`,** bounded by `depth ≤ 2` + per-direction
  fanout caps + `LIMIT`.
- **Token caps:** top-N neighbours per direction and an M-character excerpt
  truncation, both fixed constants.
- **Benchmark gate before merge:**
  - ASK p50 latency and total token count on a representative **non-lineage**
    question must not regress >5%.
  - The lineage-question path must add **zero** LLM round-trips versus today
    (target: fewer, since named lineage is now in-prompt).
  - RUN wall-clock per table must not regress >5%.
- **Kill switch:** the environment variable `AMX_LINEAGE_CONTEXT_DISABLED=1`
  short-circuits the shared core to an empty result, reverting both surfaces to
  current behaviour if a regression is observed in the field. An env var avoids a
  config-schema change and is cross-platform.

## Verification and Risks

- **Excerpt sourcing:** producer-asset excerpts come only from already-ingested
  `remote_*` rows; confirm the join from a native `lineage_native_asset` edge's
  `remote_id` to the asset body, and that a `name_only` asset cleanly degrades to
  name-only with no fetch.
- **Multi-hop cost:** the recursive CTE must stay depth- and fanout-bounded;
  verify on a wide table (many producers/consumers) that the LIMIT and caps hold
  result size within the token budget.
- **ASK regression:** the always-on enrichment change is the main latency risk;
  the benchmark gate above is mandatory before merge.
- **Idempotency / scoping:** lineage stays profile-scoped; cross-profile edges
  are never introduced.

## Deployment

This change is Studio-visible (ASK answers and `/analyze run` are surfaced in
Studio). Per the standing deploy order for Studio-visible work, run
`deploy.sh` first, then open the PR, then merge — confirm the order with the
user before executing the deployment step.
