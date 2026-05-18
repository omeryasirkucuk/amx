# Pages — Per-Asset DB Picker

**Date:** 2026-05-18
**Status:** Approved (verbal)
**Parent feature:** [Documentation Pages](./2026-05-18-amx-documentation-pages-design.md)

## 1. Problem

The Pages wizard's DB tab lets the user pick a whole DB profile as a
context asset. The backend already supports finer-grained kinds
(`db_database`, `db_schema`, `db_table`, `db_column`) and the
resolver knows how to fetch DDL for each, but the UI exposes none of
that — a 5-table schema and a 5,000-table warehouse end up identical
in the LLM prompt.

Users need to attach exactly the assets they care about: a single
column, one table, a schema, or a whole database, and mix granularity
freely within and across profiles.

## 2. Goal

Replace the profile-card list in `DbProfileTab` with a per-profile
collapsible tree that drills `profile → database → schema → table →
column`, with an independent checkbox at every level. Selection
emits the same `PageAssetRef[]` shape the rest of the wizard already
consumes — no backend or storage change.

## 3. Non-goals (v1)

- Granular file selection inside a doc profile.
- Granular node selection inside a lineage artifact.
- Parent-select-implies-children "select all under" helpers.
- Saved selection presets.
- Free-text asset reference input.

## 4. Architecture

```
components/pages/
  AssetPicker.tsx          (existing — DbProfileTab body replaced)
  DbAssetTree.tsx          (new — per-profile collapsible cards)
  AssetTreeNode.tsx        (new — recursive node: db / schema / table / column)
```

`AssetPicker` keeps its public surface (`value: PageAssetRef[]`,
`onChange`). `DbAssetTree` reads the DB profiles list (same
`/api/profiles/db` call used today), then renders one
`<ProfileCard>` per profile. Each card lazily fetches its children
through the existing `api.liveDatabases / liveSchemas / liveAssets /
liveColumns` helpers in `lib/api.ts`. No new backend endpoint.

`AssetTreeNode` is a single recursive component parametrised by
`level` and `path`; it owns the expand state, the children fetch,
and the selection toggle. Keeping recursion in one file (kural #8)
caps the tree code at one module under ~300 lines.

## 5. Asset ref formats

`AMXResolver.resolve_db_asset` already parses every shape below
(see `amx/pages/_resolver.py`):

| Kind | `ref` format | Example |
|---|---|---|
| `db_database` | `<profile>/<database>` | `pg_prod/orders_db` |
| `db_schema` | `<profile>/<database>/<schema>` | `pg_prod/orders_db/public` |
| `db_table` | `<profile>/<database>/<schema>/<table>` | `pg_prod/orders_db/public/orders` |
| `db_column` | `<profile>/<database>/<schema>/<table>.<column>` | `pg_prod/orders_db/public/orders.id` |

(Databricks/BigQuery use catalog instead of database — the same path
shape works because the existing `liveDatabases` endpoint returns
catalogs there too.)

## 6. UX

### 6.1 Selection model

Every checkbox is independent. Selecting a schema does not implicitly
select its tables. The user picks exactly which entities go into the
LLM context. This is the simpler and more explicit mental model;
"select all under" can ship later as a power-user shortcut.

### 6.2 Profile cards

Profile rows have **no top-level checkbox** — picking "the whole
profile" was the v1 behaviour we are explicitly removing. The
profile row is a clickable header that expands the tree. The
profile name + backend stays on the left; the right side shows a
"N selected" badge counting any of this profile's descendants that
appear in the current `value`.

### 6.3 Lazy loading

- Profile card collapsed by default. First expand → `liveDatabases`.
- Database node expand → `liveSchemas`.
- Schema node expand → `liveAssets` (returns tables + views).
- Table node expand → `liveColumns`.

Each fetch is a separate React Query key so cache hits skip the
round-trip on re-expand. Failures render inline under the parent
node and do not break the rest of the tree.

### 6.4 Visual shape

```
┌─ pg_prod (postgresql)                           [▼] 2 ┐
│  [ ] orders_db                                        │
│  [▼] sales_db                                         │
│     [ ] public                                        │
│     [▼] analytics                                     │
│        [x] daily_orders                       (table) │
│        [ ] customer_summary                           │
│        [▼] revenue_facts                              │
│           [x] revenue_facts.amount_usd       (column) │
│           [ ] revenue_facts.txn_date                  │
└───────────────────────────────────────────────────────┘
```

### 6.5 Responsive (memory: feedback_studio_responsive_required)

- `md:+` — profile cards in a 2-column grid; tree text 13px.
- `sm:` — single column; tree indent 8px; "N selected" badge mini.
- Selection chip strip above the tree summarises the current pick
  and lets the user remove individual entries; overflows scroll.

## 7. Edge cases

- DB profile list empty → existing "No DB profiles configured"
  message (no tree rendered).
- Child fetch fails → inline error on the parent node, sibling
  branches keep working.
- Column fetch is gated behind a table expand to avoid up-front load.
- Selection state lives in the wizard's existing
  `useState<PageAssetRef[]>`; wizard step changes preserve it.
- The same asset added twice (e.g. a column and its parent table) is
  passed through to the backend as-is. The resolver handles each
  ref independently; duplication is on the user.

## 8. Tests

- `DbAssetTree.test.tsx` — renders profile list, expands a profile
  (mocked `liveDatabases`), expands a database (mocked
  `liveSchemas`), toggles a schema checkbox → `onChange` called
  with one `db_schema` ref of the right path.
- `AssetTreeNode.test.tsx` — checkbox toggle emits the right ref;
  expand triggers exactly one fetch; second expand reuses cache.

## 9. Out of scope (recap, follow-ups)

- Doc profile drill-down (file-level selection).
- Lineage artifact drill-down (node-level selection).
- "Select all under" parent shortcut.
- Schema-wide column glob (e.g. "all columns matching `_id`").
- Multi-profile bulk import (paste a list of FQNs).
