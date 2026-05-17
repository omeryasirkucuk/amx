# AMX `/lineage` v4 — Column-Level Rewrite

## Context

V3 (PRs #503, #505, #506, #507) shipped a table-relationship canvas
with manual edge authoring, verdict-driven LLM feedback, audit
trail, share links, and a welcome hub. **It is the wrong product.**
The user's reference throughout has been
[PaveLuchkov/datapav](https://github.com/PaveLuchkov/datapav), which
is a **column-level data-flow editor with first-class
transformation-operator nodes** — not the table relationship canvas
we built.

Concrete gaps surfaced from a side-by-side read of datapav's README
screenshot vs the current AMX lineage canvas:

1. AMX renders one node per table, one edge per table-pair.
   Datapav renders one node per table **with per-column rows**
   carrying individual connector ports, and edges go column→column.
2. AMX has no transformation-operator nodes. Datapav has
   `completed_only` (filter), `compute_ltv` (function with
   `INPUTS → OUTPUTS`), color-coded by operator kind, with the
   operator's logic embedded and editable.
3. AMX's chain-highlight is table-level. Datapav's
   "Tracing column X" panel walks the upstream **column-by-column**
   path and renders a numbered ORIGIN → HERE list.
4. AMX has no in-place edit. Adding a table requires a modal;
   connecting columns is impossible. Datapav lets you type a column
   name into a row, change a filter predicate, add an output port
   without leaving the canvas.
5. AMX's drag-to-connect is node-level (whole-table to whole-table).
   Datapav's affordance is a `+ & link` button on every column row;
   drag is column-port to column-port.

On top of the conceptual gap, two execution failures:

6. The current canvas at `/lineage/:profile/:anchor` renders empty
   or tiny in real use. Auto-fit is broken at high zoom levels.
7. The default extractor set produces noisy results. **`name_match`
   is the primary offender** — it joins any two columns sharing a
   name across the catalog, regardless of semantic relation.

This spec rewrites lineage as a column-level system that matches the
datapav model while staying within AMX's "documentary, no execution
engine" constraint.

## Existing surfaces touched

- `amx/lineage/types.py` — `Edge`, `ColumnRef`, `Scope`, `ExtractMode`
- `amx/lineage/service.py` — `lineage_for_studio`, `suggest_lineage_llm*`
- `amx/lineage/store.py` — read/write to `catalog_relationships`
- `amx/lineage/extractors/{fk,view_ddl,query_log,name_match,llm,manual,codebase_scan}.py`
- `amx/lineage/llm_prompt.py` — feedback-loop prompt builder
- `amx/web/routers/lineage.py` — all `/api/lineage/*` endpoints
- `amx/cli_support/commands/lineage.py` — `/lineage` REPL slash command
- `amx/storage/sqlite_store.py` — DDL for `catalog_relationships`
- `amx/storage/schema_descriptions.py` — column descriptions (CI-enforced)
- `frontend/src/components/LineageCanvas.tsx` — React Flow canvas
- `frontend/src/routes/LineageDetail.tsx` — artifact viewer
- `frontend/src/routes/LineageNew.tsx` — blank-canvas authoring
- `frontend/src/lib/api.ts` — typed lineage client

## Roadmap

| Slice | Theme | Studio-visible? | Ship verdict |
|---|---|---|---|
| **S1** | Storage + `Edge.column` round-trip | No | Build now |
| **S2** | `view_ddl` → `sqlglot.lineage` with operator nodes | No | Build next |
| **S3** | `TableNode` per-column ports + `OperatorNode` render + drag-from-port | Yes | After S2 |
| **S4** | Trace panel (click column → ORIGIN→HERE path) | Yes | After S3 |
| **S5** | Operator expression editor + `query_log` / `codebase_scan` / `llm` rewrite | Yes | After S4 |
| **S6** | Polish: LOD, layout, `name_match` flag, v1→v2 migration banner, perf | Yes | After S5 |

Each slice = `deploy.sh` → PR → merge (house rule #6) for the
Studio-visible ones. S1+S2 are backend-only; the standard `git
commit → PR → merge` order applies.

---

## Slice S1 — Storage + `Edge.column` round-trip

### Storage

Additive ALTER on `catalog_relationships` (mirrors the v3-S4
`verdict` column pattern):

```python
for _ddl in (
    "ALTER TABLE catalog_relationships ADD COLUMN from_column TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE catalog_relationships ADD COLUMN to_column TEXT NOT NULL DEFAULT ''",
):
    with contextlib.suppress(sqlite3.OperationalError):
        conn.execute(_ddl)
```

Empty string = table-level edge (legacy). Non-empty = column-level
edge. **`schema_descriptions.py` entries are mandatory in the same
commit** (house rule #5).

Index on `(db_profile, from_entity_id, from_column, to_entity_id,
to_column)` to keep trace-panel BFS fast at 10k+ column edges.

### Operator nodes

Reuse `catalog_entities` with a new `entity_kind='operator'`,
`asset_kind='operator'`. Path slug:
`op:<schema>.<table>:<op_kind>:<short_hash>` so synthetic and real
entities never collide. `details_json` carries:

```json
{
    "op_kind": "filter|join|function|aggregate|projection",
    "expression": "status == 'completed'",
    "input_columns": [{"fqn": "public.orders_raw.status", "alias": "status"}],
    "output_columns": [{"fqn": "public.completed_only.<row>", "alias": "<row>"}]
}
```

No new table — `entity_kind` is already free-text per the
descriptions in `schema_descriptions.py`.

### `Edge` model

`Edge` stays as-is (`ColumnRef` already has `.column`). The change
is in the persistence layer + extractor output convention: empty
`.column` is the legacy table-level signal.

### `lineage_artifacts.payload_json`

Bump `schema_version` 1 → 2. v2 payload carries:

```json
{
    "schema_version": 2,
    "nodes": [
        {"kind": "table", "fqn": "public.orders", "columns": [...]},
        {"kind": "operator", "id": "op:...", "op_kind": "filter", "expression": "..."}
    ],
    "edges": [{"from": "public.orders.id", "to": "op:...", "operator_id": null}]
}
```

v1 readers (the existing canvas) keep working — server emits v1
shape when the artifact has `schema_version=1`, v2 shape otherwise.

### Tests (S1)

- `tests/lineage/test_column_edge_persistence.py` — round-trip
  `from_column`/`to_column` through `catalog_relationships`.
- `tests/lineage/test_operator_entity.py` — insert an
  `entity_kind='operator'` row, read back, assert `details_json`
  parses.
- `tests/lineage/test_v2_artifact_payload.py` — serialize +
  deserialize a v2 artifact; v1 artifact still loads through v2
  reader.
- `tests/test_local_schema_comments.py` — already enforces; new
  columns must have non-empty descriptions in
  `schema_descriptions.py`.

### Out of scope for S1

No extractor changes yet. No UI changes. No new endpoints. S1 is
the foundation that lets S2+ start emitting column edges without
schema churn.

---

## Slice S2 — `view_ddl` extractor rewrite

### Goal

Replace the current `sqlglot.optimizer.qualify` + manual column
walk in `view_ddl.py` with `sqlglot.lineage.lineage(column, sql)`.
For each column in the view's SELECT, sqlglot returns a tree whose
nodes carry the upstream column and the SQL fragment that
transformed it. Walk that tree:

- Leaf node = source column → emit `Edge(source_col → view.col)`
  with `relationship_type='lineage_view_ddl'`.
- Intermediate node with a non-trivial transform (filter predicate,
  aggregate function, scalar function call, join condition) →
  create an operator entity, emit two edges:
  `source_col → operator` and `operator → view.col`.
- Intermediate node with a pure pass-through (alias rename, no
  expression change) → no operator; collapse into a single
  `source_col → view.col` edge.

### Operator-kind classification

Map sqlglot expression types to `op_kind`:

| sqlglot expression | op_kind |
|---|---|
| `Where` | `filter` |
| `Group` / agg functions | `aggregate` |
| `Join` | `join` |
| Scalar function call | `function` |
| `Cast`, `Alias` only | (no operator — pass-through) |

`expression` field = the SQL fragment for that node, normalized
through `sqlglot.transpile`.

### Confidence

`view_ddl` edges keep `confidence=1.0` — view DDL is authoritative.
Operator nodes carry the same confidence as their producing edge.

### Tests (S2)

- `tests/lineage/test_view_ddl_column_lineage.py` — table of
  view DDL → expected edges + operators.
  - Simple `SELECT a, b FROM t` → 2 pass-through edges, no operators.
  - `SELECT a FROM t WHERE status='ok'` → filter operator + edge chain.
  - `SELECT sum(amount) FROM t GROUP BY country` → aggregate operator
    with two output columns.
  - `SELECT a.x FROM a JOIN b ON a.id=b.id` → join operator.
  - `SELECT lower(name) FROM t` → function operator with `expression`
    field set to `LOWER(name)`.

### Out of scope for S2

`query_log`, `codebase_scan`, `llm` extractor rewrites land in S5.
Canvas changes land in S3.

---

## Slice S3 — `TableNode` + `OperatorNode` + drag-from-port

### `TableNode` (new React Flow custom node)

Structure (top-down):

1. Header row: table fqn + asset-kind badge + collapse caret.
2. One row per column. Row layout:
   - Left port (`Handle id={column} type="target"`) flush to row edge.
   - Type badge (`int`, `str`, `flt`, `dat`).
   - Column name.
   - Right port (`Handle id={column} type="source"`) flush to row edge.
   - Hover state: green `+` button overlays the right port (datapav
     affordance) — clicking it starts a connection drag.

Tailwind tokens that already exist on the canvas:
`bg-slate-900/80`, `border-slate-700`, hover state via `group/row`.

### `OperatorNode` (new React Flow custom node)

Structure:

1. Header: op_kind icon (Filter / Join / Function / Aggregate) +
   op_kind label.
2. Body: inline `<code>` rendering of `expression`. Read-only in
   S3 (editor lands in S5).
3. Left side: one port per input column.
4. Right side: one port per output column.

Color tokens by op_kind:
- `filter` — orange `border-orange-500/60`
- `join` — purple `border-purple-500/60`
- `aggregate` — cyan `border-cyan-500/60`
- `function` — green `border-green-500/60`
- `projection` — slate `border-slate-500/60`

### React Flow wiring

`nodesConnectable={true}` (already on from V3 S4) and
`isValidConnection` rejects:
- self-connections on the same column,
- connections that would create a cycle within an artifact.

`onConnect` payload now carries `source` (column), `sourceHandle`
(column name), `target` (column), `targetHandle` — POSTs
`/api/lineage/edges` with all four.

### Layout

Two-pass:
1. dagre on `{TableNode, OperatorNode}` → produces x/y for each
   parent node.
2. Per-column row positions are determined by the React Flow node
   layout itself (rows stack inside the node DOM). React Flow's
   `Handle` positions are relative to the node, so edges connect
   correctly without manual port coordinate math.

### Endpoint changes

`GET /api/lineage/{anchor}` returns v2 payload (see S1 schema)
when the artifact is v2. `POST /api/lineage/edges` extended:

```json
{
    "from_fqn": "public.orders.id",
    "to_fqn": "public.customers.order_id",
    "relationship_type": "lineage_manual"
}
```

Server parses the fqn into `(schema, table, column)`, resolves
entity ids, writes the row.

### Tests (S3)

- `tests/web/test_lineage_router_column_edges.py` — POST with column
  fqns, assert `catalog_relationships` row has
  `from_column`/`to_column` populated.
- Frontend Vitest: `LineageCanvas.test.tsx` — render a v2 payload
  with 2 tables + 1 operator, assert `Handle` count matches column
  count, drag synthetic event emits the correct POST payload.

### Out of scope for S3

Trace panel (S4). Operator editor (S5). LOD (S6).

---

## Slice S4 — Trace panel

### Goal

Datapav's "Tracing column X" panel: click any column row, the
right rail shows the numbered ORIGIN → HERE upstream path. Click a
step in the list to fitView on that node + highlight the column.

### Backend

`GET /api/lineage/trace/{profile}?from_table=&from_column=` —
server-side BFS over `catalog_relationships` where
`from_column`/`to_column` are non-empty, returns ordered list of
`{step_no, table, column, edge_kind, operator?}`. Cap at depth 50;
beyond that the panel shows "Path truncated, deeper edges may
exist."

### Frontend

`LineageTracePanel.tsx` — right rail component:

- Header: `Tracing column <fqn>`, close `×` button.
- `ORIGIN` section: list of source-leaf rows.
- Trail: numbered list of intermediate steps; operator steps
  render the op_kind icon.
- `HERE` section: the clicked column highlighted.
- Click any step → `useReactFlow().fitView({nodes: [step.id]})`
  and apply transient highlight class for 1.5s.

Click handler on `TableNode` column rows fires panel open. URL
state via search param `?trace=public.customers.ltv_score` so the
panel survives page reload.

### Tests (S4)

- `tests/web/test_lineage_trace_endpoint.py` — seed a 3-hop column
  chain, assert returned steps in order, assert depth cap honored.
- Frontend Vitest: `LineageTracePanel.test.tsx` — given trace
  payload, assert correct rendering + click handler fires `fitView`.

### Out of scope for S4

Downstream tracing (HERE → consumers). Defer to S6 polish.

---

## Slice S5 — Operator editor + extractor rewrites

### Operator expression editor

`OperatorNode` body becomes an inline `<textarea>` in edit mode.
Toggle via double-click on the operator. Save = blur or Enter (with
shift+Enter for newline). PATCH
`/api/lineage/operators/{id}` writes back to
`catalog_entities.details_json.expression`.

Expression text is treated as opaque SQL — AMX does not validate or
execute it. Optional `sqlglot.parse(expression)` check surfaces a
warning toast if unparseable; the save still goes through (user
might be drafting).

### `query_log` rewrite

Currently emits table co-occurrence from `analysis_runs.scope_json`
and `chat_turns.tables_json`. Replace with: for each SQL captured
in those tables, run the same `sqlglot.lineage.lineage()` walk
used in S2, emit column edges + operators with
`relationship_type='lineage_query_log'`. Unparseable SQL is
skipped silently. **Confidence 0.7** (lower than view_ddl because
query log may include exploratory or ad-hoc SQL).

### `codebase_scan` rewrite

Same sqlglot path. For each indexed chunk from
`amx.codebase.code_rag`, parse, emit column edges with
`relationship_type='lineage_codebase'`, evidence `path:line`.
Confidence 0.6.

### `llm` rewrite

`amx/lineage/llm_prompt.py` builds messages asking the LLM to
return:

```json
{
    "edges": [
        {
            "from_fqn": "public.orders.customer_id",
            "to_fqn": "public.customers.id",
            "operator_kind": null,
            "expression": null,
            "confidence": 0.75,
            "reasoning": "naming convention + FK pattern"
        }
    ]
}
```

Feedback loop (verdict-aware few-shot from V3 S5) keeps working;
positive/negative examples now reference column pairs instead of
table pairs.

### `name_match` move

`name_match` extractor stays in the codebase but is **removed from
`build_default_extractors()`**. Add `cfg.lineage.include_heuristics:
bool = False`. CLI flag `/lineage refresh --include-heuristics`
opt-in. Studio: checkbox in the AI Suggest modal labeled "Include
heuristic name-match (high false-positive rate)".

### `manual` extension

`ManualEdgeExtractor.extract` now reads column fields. Manual
operator creation supported: a `POST /api/lineage/operators`
endpoint creates an operator entity + two chained edges in one
transaction.

### Tests (S5)

- `tests/lineage/test_query_log_column_extraction.py`
- `tests/lineage/test_codebase_scan_column_extraction.py`
- `tests/lineage/test_llm_column_prompt.py` — prompt builder
  snapshot.
- `tests/web/test_lineage_operator_patch.py` — operator
  expression update round-trip.

---

## Slice S6 — Polish

### Level-of-detail rendering

When `useReactFlow().getZoom() < 0.5`, collapse every `TableNode`
into header + `N cols` badge. When ≥0.5, restore per-column rows.
Pure CSS toggle keyed off a class on the canvas root; no React
re-render churn.

### Empty-canvas fix

Auto-fit on mount: `useEffect(() => fitView({padding: 0.2}), [])`
after nodes are laid out. Empty results render a centered
"No lineage detected yet — try AI suggest or draw manually" card
instead of a tiny graph in the bottom-left.

### v1 → v2 migration banner

`/lineage/saved` shows an "Upgrade to column-level view" button on
v1 artifacts. Click → `POST /api/lineage/{anchor}/refresh?mode=v2`
re-runs the (now column-level) extractors and persists a v2
artifact. The v1 artifact is kept; v2 supersedes for canvas
rendering.

### Layout pass

Dagre `rankdir: 'LR'` (already) + per-rank `nodesep` tuned for
column-heavy nodes. Operator nodes drawn between their producer
and consumer ranks rather than as siblings.

### Performance

- `onlyRenderVisibleElements` threshold dropped from 200 → 100
  since column ports inflate React Flow's internal node count.
- `LineageTracePanel` BFS memoized on the edge set.
- Edge style switch (`type`/`color`) moved to a stable lookup
  table at module scope (small win, avoids per-render allocations).

### Tests (S6)

- `tests/lineage/test_v1_to_v2_migration.py` — `/refresh?mode=v2`
  on a v1 artifact produces a v2 payload.
- `tests/lineage/test_perf_budget.py` — extend with a column-level
  budget: 200 tables × 8 cols stays under 1.5 s warm cache.

---

## Cross-cutting concerns

- **English-only** on every new tracked file (comments, prompts,
  fixtures, tests, UI strings). House rule #4.
- **Cross-platform paths.** All file evidence uses POSIX-style
  relative paths via `Path(...).as_posix()`. House rule #10.
- **Cache-first.** Every extractor stays on `mode='cache_only'` by
  default. LLM is opt-in. No silent wire calls.
- **Modular files.** `LineageCanvas.tsx` is already 426 lines; do
  not let it grow past ~600. Extract `TableNode.tsx`,
  `OperatorNode.tsx`, `LineageTracePanel.tsx` as separate files
  under `frontend/src/components/lineage/`. House rule #8.
- **Schema descriptions mandatory.** New columns
  (`from_column`, `to_column`) ship with descriptions in
  `schema_descriptions.py` in the same commit. House rule #5.
- **Deploy order.** S3+ slices use `deploy.sh` → PR → merge per
  house rule #6.

## Out of scope (explicitly deferred)

- Execution engine. Operators are documentary, populated
  automatically from sqlglot. The user can manually create
  operators in the canvas; AMX never runs them.
- Cross-profile / cross-database column lineage. dbt manifest
  ingestion stays out (was originally in V3 S5 sketch). One
  profile per canvas.
- Real-time collaborative editing. Single-user assumption holds.
- Downstream tracing UI (HERE → consumers panel). S6 candidate if
  it doesn't bloat polish.
- Lineage diff across two timestamps.
- Playwright visual regression. Defer to a separate hardening pass.

## Verification per slice

1. `pytest tests/lineage -q` green.
2. `pytest tests/test_local_schema_comments.py tests/test_shared_schema_comments.py -q` green.
3. `npx tsc --noEmit && npm run build` (frontend) green for S3+.
4. **Manual smoke** (the "eline sağlık" sequence, V4 version):
   - Open `/lineage/:profile/:anchor` for a view with a known
     non-trivial DDL → see column rows on every table, operator
     nodes between them with the correct expressions.
   - Click any column row → trace panel shows ORIGIN → HERE list.
   - Click a step → canvas zooms to that node, column highlights.
   - Drag a column's `+` port to another table's column → edge
     persists, reappears on reload.
   - Double-click an operator → expression editor opens, edit,
     save → details_json updates.
   - Schema page "AI suggest" → returns column-level edges, not
     table pairs.
   - `name_match` is off by default; opt-in via checkbox produces
     visibly more edges (and visibly more noise).
5. CI green on every PR.

## Migration story for existing users

- Existing v1 artifacts continue to render as table-level on the
  current canvas component (the v2 reader downgrades gracefully
  when the artifact's `schema_version=1`).
- The `/lineage/saved` page surfaces an "Upgrade to column-level"
  button per artifact (S6).
- v2 is the default for any new artifact created after S1 ships.
- Drop of `name_match` from defaults will reduce edge count on
  subsequent `refresh` calls — that is the intended behavior and
  not a regression.

## Critical files (creation order)

- `amx/storage/sqlite_store.py` — S1 ALTER
- `amx/storage/schema_descriptions.py` — S1 descriptions
- `amx/lineage/types.py` — S1 (no model change; documentation)
- `amx/lineage/store.py` — S1 column-field persistence
- `amx/lineage/service.py` — S1 v2 payload assembly + S2 operator
  integration
- `amx/lineage/extractors/view_ddl.py` — S2 sqlglot.lineage rewrite
- `amx/lineage/operator_ops.py` (new) — S1 helper for creating
  operator entities + chained edges atomically
- `frontend/src/components/lineage/TableNode.tsx` (new) — S3
- `frontend/src/components/lineage/OperatorNode.tsx` (new) — S3
- `frontend/src/components/lineage/LineageTracePanel.tsx` (new) — S4
- `frontend/src/components/LineageCanvas.tsx` — S3 wire new nodes,
  S6 LOD class toggle
- `amx/web/routers/lineage.py` — S3 column-aware POST, S4 trace
  endpoint, S5 operator PATCH
- `amx/lineage/extractors/query_log.py` — S5 sqlglot rewrite
- `amx/lineage/extractors/codebase_scan.py` — S5 sqlglot rewrite
- `amx/lineage/extractors/llm.py` — S5 column-pair prompt
- `amx/lineage/llm_prompt.py` — S5 column-pair message shape
- `amx/cli_support/commands/lineage.py` — S5
  `--include-heuristics` flag
