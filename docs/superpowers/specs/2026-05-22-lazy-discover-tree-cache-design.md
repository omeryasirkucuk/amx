# Lazy discover tree cache + per-folder refresh + smart search

**Date:** 2026-05-22
**Status:** Approved (pending implementation)
**Surface:** Studio IngestDialog "Browse and pick" step (`AssetBrowsePicker`),
`/api/assets/discover`, Databricks + Snowflake adapters,
`amx/storage/sqlite_store.py`.

## Problem

The current `/api/assets/discover?profile=&kind=notebooks` endpoint
calls `DatabricksAdapter.list_remote_notebooks_metadata`, which in
turn recursively walks the full workspace tree via
`_workspace_client.list_workspace_objects(path="/")`. Every dialog
open re-runs the full sequential paginated walk before the picker
shows a single row. A workspace with a few hundred directories
takes 10–30 seconds; the user sees nothing but a "Loading
notebooks…" spinner.

Two follow-on issues compound the wait:

* No persistence — the result is thrown away on dialog close, so
  re-opening pays the same cost.
* No way to refresh a specific folder — the user either lives with
  stale data or pays the full walk again.

## Goals

* Sub-second perceived load on first open after the cache is
  warm.
* User can refresh a specific folder (or all of root) on demand.
* Search works whether the cache is partial or empty.
* Disk-persisted cache (history.db SQLite) so the warm state
  survives Studio restarts.
* Snowflake stays functional (it has no real folder hierarchy
  for notebooks — flat list is fine).

## Non-goals (v1)

* Folder-level checkbox ("select all in this folder"). Lazy load
  makes the semantics ambiguous; deferred.
* Per-asset ACL filtering — the user already has whatever the
  Databricks token permits.
* Snowflake "expandable DB.SCHEMA" tree — flat list for v1.
* TTL / background expiration. Manual refresh only, per user.

## Design

### Storage

New SQLite table `remote_workspace_tree`:

```sql
CREATE TABLE remote_workspace_tree (
    profile_name TEXT NOT NULL,
    kind TEXT NOT NULL,              -- 'notebook' | future kinds
    path TEXT NOT NULL,              -- full path (workspace path or qualified_name)
    parent_path TEXT NOT NULL DEFAULT '',  -- '' = root level
    name TEXT NOT NULL,
    is_directory INTEGER NOT NULL DEFAULT 0,
    external_id TEXT,                -- NULL for directories
    owner TEXT,
    last_modified TIMESTAMP,
    children_fetched_at REAL,        -- NULL = children never fetched
    fetched_at REAL NOT NULL,        -- this row's own write timestamp
    PRIMARY KEY (profile_name, kind, path)
);

CREATE INDEX idx_remote_workspace_tree_parent
    ON remote_workspace_tree(profile_name, kind, parent_path);
```

Schema descriptions for every column land in
`amx/storage/schema_descriptions.py` (house rule §5). The CI gate
`tests/test_local_schema_comments.py` catches missing entries.

Per house rule §10, the migration runs through
`SQLiteHistoryStore._ensure_*_columns`-style idempotent helper so
existing history.db files pick up the new table on next `init()`.

### Adapter API

New method on `DatabaseAdapter`:

```python
def list_workspace_children(
    self, engine, *, parent_path: str, kind: str
) -> Iterable[WorkspaceEntry]:
    """Yield immediate children of parent_path. Single, non-recursive API call."""
```

`WorkspaceEntry` is a frozen dataclass in `remote_asset_types.py`:

```python
@dataclass(frozen=True)
class WorkspaceEntry:
    kind: str                  # 'notebook' | future
    path: str
    name: str
    is_directory: bool
    external_id: str | None    # None for dirs
    owner: str | None
    last_modified: datetime | None
```

**Databricks implementation.** One call to
`/api/2.0/workspace/list?path=<parent_path>` (defaulting to `/`).
Parse:
* `object_type=DIRECTORY` → `WorkspaceEntry(is_directory=True, external_id=None)`
* `object_type=NOTEBOOK` → `WorkspaceEntry(is_directory=False, external_id=str(object_id))`
* `object_type=FILE` / `REPO` → skipped (not ingestable as notebooks)

**Snowflake implementation.** Native Snowflake notebooks don't have
a hierarchy; the existing `SHOW NOTEBOOKS IN ACCOUNT` walk is the
only path. For v1:
* `parent_path = ""` → return everything from `list_remote_notebooks_metadata`
  as a flat list of `is_directory=False` leaves.
* `parent_path = <anything>` → empty.

`list_remote_notebooks_metadata` stays unchanged — it backs the
"search walk" fallback.

### REST endpoints

Three new endpoints on `amx/web/routers/assets.py`:

#### `GET /api/assets/discover/tree?profile=&kind=&parent=`

Cache-first. Returns the immediate children of `parent`:

```json
{
  "items": [
    {
      "path": "/Users/x@y.com",
      "name": "x@y.com",
      "is_directory": true,
      "external_id": null,
      "owner": null,
      "last_modified": null,
      "child_count": null
    }
  ],
  "parent_path": "",
  "parent_fetched_at": 1716333600.0,
  "cache_empty": false
}
```

Flow:

1. `SELECT * FROM remote_workspace_tree WHERE profile = ? AND kind = ? AND parent_path = ?` → if rows exist AND the parent's `children_fetched_at` is not NULL → return them.
2. Else: call adapter `list_workspace_children(parent_path=parent)`,
   `INSERT OR REPLACE` each row, stamp the parent row's
   `children_fetched_at = now()` (or insert a synthetic root parent
   row when `parent=""`), return the rows.

#### `POST /api/assets/discover/tree/refresh?profile=&kind=&parent=`

Atomic refresh of one folder's immediate children:

1. `BEGIN`
2. `DELETE FROM remote_workspace_tree WHERE profile = ? AND kind = ? AND parent_path = ?`
3. Call adapter `list_workspace_children(parent_path=parent)`
4. `INSERT OR REPLACE` new rows
5. `UPDATE remote_workspace_tree SET children_fetched_at = ?, fetched_at = ? WHERE path = ?` (the parent row itself)
6. `COMMIT`

Grandchildren of `parent` are NOT touched. If a previously-cached
directory child still exists in the new listing, its own children
remain valid (the directory's `children_fetched_at` is untouched).
A directory that disappears from the listing has its descendants
orphaned in the table — periodic cleanup is out of scope for v1
(orphans only hurt disk size, not correctness — the parent_path
join hides them).

#### `POST /api/assets/discover/tree/walk?profile=&kind=`

Full recursive walk. Used by:
* Search-when-cache-empty path.
* User clicking "Walk entire workspace" (rendered as a fallback
  link inside the empty-search state).

Implementation:

1. `DELETE FROM remote_workspace_tree WHERE profile = ? AND kind = ?`
2. Call the existing `list_remote_notebooks_metadata` (Databricks)
   or `list_remote_notebooks_metadata` Snowflake variant.
3. For every yielded leaf, also synthesize directory ancestors:
   split the path on `/` and create an `is_directory=True` row
   for every prefix, with `children_fetched_at = now()`.
4. Atomic transaction.

Returns:
```json
{ "rows_written": 1234, "directories": 56, "leaves": 1178 }
```

### Studio UI (`AssetBrowsePicker` rewrite)

The current file groups loaded rows client-side. The new
implementation switches to a true lazy tree:

* On mount: GET `/api/assets/discover/tree?parent=` → render root
  level (folders + any root-level leaves).
* Each tree node row:
  * Folder: `<chevron> <name> <child-count> <refresh icon>`
  * Leaf:   `<spacer> <checkbox> <name> <owner>`
* Click chevron on collapsed folder: if children present in local
  state, expand instantly; else fire GET with `parent=<this>`,
  show inline spinner on the chevron icon, expand on response.
* Per-folder refresh icon: POST refresh for that folder, spinner
  on the icon during the round-trip, on success re-fetch GET.
* Header refresh icon (above the search input): refreshes only
  the immediate children of `/` (does not cascade).
* Search input behavior:
  * `cache_empty=false` (have at least one row): client-side
    filter against all loaded rows. Matches name + path + owner.
    "Search scope: loaded folders" hint appears under the input.
  * `cache_empty=true`: typing triggers POST walk. While walking,
    show a wide spinner panel with "Walking workspace — first
    search will populate the cache". On success, render flat
    filtered results.
* Selection: leaf-only (`external_id`). The parent IngestDialog
  consumes the same `selection: dict[kind, list[id]]` shape as
  PR-A.

Mock for the empty-state-then-walking path:
```
[Search input] [icon: walk-and-search]
                  ↓ user types "etl"
Walking workspace to populate the cache (first time only)…
```

After walk completes the table renders as a flat list (no folders
in search mode). Clearing the search returns to tree mode.

### Snowflake UI specialization

Because Snowflake's `list_workspace_children` returns flat leaves
at `parent=""`, the tree degenerates to a one-level list (no
chevrons). The same component handles it transparently — when
every child has `is_directory=false`, no chevrons render.

### Error handling

* GET tree adapter call fails → router returns 502 with a clean
  message; UI shows a per-folder error chip with retry button.
* POST refresh fails mid-transaction → SQL rollback, cache stays
  at the previous state, UI surfaces error toast.
* Cache row points at a directory that has since disappeared on
  the platform → next refresh of its parent removes it
  automatically. Until then, expanding the stale directory
  returns an empty body + a 200 (not an error).

### Tests

* `tests/storage/test_workspace_tree_schema.py` — table create,
  PK + index, schema descriptions present.
* `tests/db/test_adapter_workspace_children.py` — Databricks
  workspace client fake yields the right `WorkspaceEntry` shape
  for DIRECTORY + NOTEBOOK + skip-FILE. Snowflake returns flat
  list on `parent=""` and empty on `parent="x"`.
* `tests/web/test_assets_discover_tree.py` — GET cache hit, GET
  cache miss → adapter fetch + write, POST refresh atomicity
  (sibling folders unchanged), POST walk seeds the full cache,
  empty-cache GET triggers a walk transparently.
* `tests/web/test_assets_discover_tree_router.py` — router
  parameter validation, 502 on adapter exception.
* Frontend (manual sanity check on remote Studio): tree expand,
  per-folder refresh, search-while-cache-empty, search-while-
  cache-populated.

### Migration / rollout

* Migration is additive (new table); existing rows untouched.
* The new endpoints live alongside the old
  `/api/assets/discover`. The old endpoint stays in place as a
  fallback for the search-walk path and any non-Studio consumer.
* Studio AssetBrowsePicker switches to the new endpoints
  exclusively.

## Out of scope (separate brainstorm if needed)

* Folder-level "select all under this folder" semantics
  (requires resolving "what if children not yet loaded?").
* Snowflake DB.SCHEMA tree mode.
* TTL-driven background refresh.
* "Show only recently-touched" filter — orthogonal.
