# Bulk-Run Review UX — Design Spec

**Status:** Approved
**Date:** 2026-05-12
**Scope:** CLI + Studio, post-run review surfaces (`/run` results,
`/inspect`, Run detail page, Pending page, Compare)

## Motivation

Bulk runs (a database, a wide schema, or 20+ tables in one shot)
produce hundreds of suggestions. Today reviewing them is painful:

- **Studio `RunDetail.tsx`** (2747 lines, single monolithic file)
  renders every suggestion as a flat row with **no search, no filter,
  no sort, no grouping, no pagination, no status indicator** at the
  row level, and no keyboard navigation. A 200-suggestion run is one
  enormous scroll.
- **CLI individual review** walks suggestions in fixed order with no
  filter, no jump-to, no sort, no pick-by-pattern.
- **`/compare`** only operates at run-id granularity. There is no way
  to compare the **same column's description across multiple runs**.
- **URL state is non-existent** in Studio — every reload resets
  filters, position, and selection.
- **CLI ↔ Studio parity** is uneven: Pending tab has text search;
  Run detail does not.

The fix needs to land on both surfaces with feature parity. Studio
gains rich interactive controls; CLI gains structured flags and an
interactive picker that mirrors the same mental model.

## Approach

Three sequenced PRs. Each PR ships both Studio and CLI changes.

### PR A — Filter / search / sort / group + status chips

Foundation work. Make 200 rows browsable without scrolling. Both
surfaces gain the same vocabulary of filters / sorts / groupings.

**Studio:**
- New `<FilterBar>` component above the suggestions table:
  - Free-text search input (matches against schema, table, column,
    description). Debounced 150ms.
  - Sort dropdown: confidence asc/desc, logprob asc/desc, table
    name asc, status (unreviewed → accepted → skipped).
  - Group-by toggle: none / schema / table.
  - Status chip filter row: `[All] [Unreviewed] [Accepted] [Skipped]`.
  - Three quick presets: `[Low confidence (<0.7)] [Has citations]
    [Table-level only]`.
- Per-row status badge (small pill on the left): unreviewed (gray) /
  accepted (green) / skipped (amber). Today the underlying state is
  tracked server-side; we just need to surface it visually.
- `RunDetail.tsx` is split into focused sub-components:
  - `RunDetailHeader.tsx` (badges, metrics, scope summary)
  - `ResultsFilterBar.tsx` (the new filter UI)
  - `ResultsRowList.tsx` (the table loop + grouping logic)
  - `ResultsRow.tsx` (a single row)
  - `RunDetail.tsx` becomes the orchestrator (<800 lines target)
- Group-by-table renders a collapsible header per group with a
  count chip; clicking collapses/expands the rows.

**CLI:**
- New flags on `/review` (and on `amx run --filter --sort` when the
  user enters review after a non-interactive run):
  - `--filter <regex>` — keep only rows matching the regex against
    "schema.table.column" path
  - `--sort <key>` — `conf-asc`, `conf-desc`, `logprob-asc`,
    `logprob-desc`, `name-asc`, `status`
  - `--only-unreviewed` / `--only-low-conf` shortcuts
  - `--group-by <schema|table|none>` for the summary table only
- During `individual` review, the rendered "Next suggestion" prompt
  shows a status indicator: `[3/47 · unreviewed · conf 0.82] sales.orders.customer_id`.
- `/inspect` summary table gains the same `--sort` + `--filter`
  flags and a `STATUS` column (`✓ accepted / ✗ skipped / · pending`).

### PR B — Selection + bulk actions + pagination + URL state + keyboard nav

Build on PR A's filter infrastructure to enable scale.

**Studio:**
- Multi-select **review mode** (separate from PR A's existing bulk
  re-run multi-select):
  - Toggle button switches a global "Review selected" mode
  - Each row's checkbox now also participates in review actions
  - Bulk actions toolbar: `[Accept selected (N)] [Skip selected (N)]
    [Apply selected (N)]`
  - When a filter is active AND the user clicks `[Accept all
    visible]` / `[Skip all visible]`, the filtered set is the target
- Pagination:
  - 50 rows per page by default
  - `[< Prev] [1] [2] [3] [Next >]` controls under the table
  - **Alternatives selection state preserved across page navigation**
    — the source of truth is the server (`PATCH /api/runs/{id}/results/{idx}`),
    so the page navigation is a pure render concern. Tests pin this.
- URL state:
  - `?q=...&sort=conf-desc&group=table&page=2&status=unreviewed`
  - Filter changes update history.replaceState (not pushState — avoid
    polluting back button history)
  - Reload restores the full view
- Keyboard navigation (when the page is focused, no input is active):
  - `j` / `k` move row focus down / up
  - `Enter` accepts the focused row
  - `x` skips the focused row
  - `/` focuses the search input
  - `g g` jumps to first row, `G` to last
  - `?` shows a keyboard cheatsheet modal

**CLI:**
- New `/review --pick` mode launches an `fzf`-style interactive
  multi-select over the matching rows (filter + sort apply first).
  User toggles selections with TAB, confirms with ENTER, then enters
  individual review on only the selected set.
  - If `fzf` is not installed, fall back to a numbered Python prompt
    ("Pick rows: 1,3,5-8 or `all`")
- During individual review, new keystrokes:
  - `j` / `k` / `g` / `G` navigation (within the in-process review
    loop)
  - `n` next, `p` previous (today only forward)
  - `/` opens a sub-prompt to filter the remaining queue
- `/review --paginate 20` controls the page size for the summary
  table render (today is "all-at-once").
- CLI summary table (Rich-rendered) gets a footer line:
  `Page 1/4 · showing rows 1-50 of 187 · filter: sales.* · sort: conf-asc`.

### PR C — Column-level compare

Reframe `/compare` to support cell granularity.

**Backend:**
- New endpoint:
  `GET /api/history/compare?cell=db.schema.table.column&runs=10,12,15`
- Returns: for each run id, the row matching that cell (or null), with
  description, confidence, logprob_score, citations, source agent.
- Tolerates absent rows (the cell didn't appear in some runs).

**Studio:**
- Per-row "Pin to comparison" affordance (small pin icon) in
  `ResultsRow`. Clicking it adds the cell to a sidebar drawer.
- Drawer accumulates cells across multiple Run detail visits (state
  in localStorage, scoped by `db_profile`).
- "Compare pinned cells" button in the drawer opens
  `RunsCompare` in a new "cell mode" tab that:
  - Shows one cell per section
  - Inside each section, runs are columns (one per pinned run id) —
    descriptions sit side by side
  - Best-pick markers from `compare.py:_highlight_best` apply
    per-cell, not per-run
- The existing run-id-level compare is unchanged; cell-mode is a
  parallel tab.

**CLI:**
- `/compare --cell <db.schema.table.column> --runs 10,12,15` —
  renders a Rich table per cell, columns = runs.
- `/compare --cell <pattern>` with a glob (`sales.orders.*`) renders
  N stacked tables, one per matching cell.
- Existing `/compare <run_ids>` behaviour unchanged.

## Out of scope (deferred)

- Server-side filter/sort/pagination — v1 keeps client-side
  filtering. Telemetry from v1 will tell us whether 2000+ row runs
  need server-paginated endpoints.
- Saved filters across sessions — quick presets are hard-coded in
  v1; user-defined saved filters can land in v2.
- Diff-style comparison highlighting (word-level diff between two
  cell descriptions) — `/compare` today does best-pick highlighting;
  full diff is a separate UX exercise.
- Drag-and-drop reordering of pinned cells — for v1 the drawer is
  insertion-order.

## CLI parity discipline

Every PR's commit message + PR body must explicitly call out the
CLI counterpart of each Studio change. The user has confirmed CLI
is a first-class surface — implementer must not skip the CLI side
because "Studio is enough."

## Test strategy

Each PR ships its own test file under `tests/` (flat layout) plus
`tests/web/` for the FastAPI / SSE bits.

- **A**: `tests/test_review_filter_sort_group.py` (CLI flag
  parsing, sort comparators, group rendering) plus
  `tests/web/test_run_detail_filter_api.py` if any server-side
  shape change is needed. Frontend smoke via `cd frontend && npm
  run build` (TS strict).
- **B**: `tests/test_review_pick.py` (CLI `--pick` flow with mocked
  fzf), `tests/test_review_keynav.py` (Python REPL keystroke
  handling), `tests/web/test_run_detail_pagination.py` if any new
  endpoint is added. **Alternatives-state-survives-pagination test
  is mandatory** — covers PR B's most fragile contract.
- **C**: `tests/test_compare_cell.py` (CLI `/compare --cell`),
  `tests/web/test_history_compare_cell.py` (the new endpoint),
  plus a snapshot test of the cell-mode RunsCompare component.

## Per-PR risk

- A: low-medium. New components, no contract changes. The 2747-line
  RunDetail.tsx split is the main risk — break it cautiously.
- B: medium. Keyboard nav can collide with existing input handlers;
  test with focus management. Alternatives state retention is the
  one place this PR can introduce real bugs.
- C: medium. New compare mode is a parallel surface — won't disturb
  existing run-id compare. Backend endpoint is small.

## Migration

No data migration. URL state is additive (old URLs without query
params still work). Multi-select review mode opts in via a toggle;
default off preserves today's UX.
