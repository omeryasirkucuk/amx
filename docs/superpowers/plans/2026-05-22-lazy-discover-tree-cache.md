# Lazy Discover Tree Cache Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the synchronous recursive workspace walk behind `/api/assets/discover` with a per-folder lazy fetch backed by a persistent SQLite cache (`remote_workspace_tree`), plus a Studio tree UI with per-folder refresh and a search-when-empty walk fallback.

**Architecture:** Cache table holds one row per tree node (folder or leaf). REST has three endpoints: GET tree (cache-first immediate children), POST refresh (atomic per-folder replace), POST walk (full cache seed). Studio `AssetBrowsePicker` rewritten as a true lazy tree with chevron + per-folder refresh icons. Snowflake degrades to a flat list (no native hierarchy).

**Tech Stack:** Python 3.12 / SQLite / FastAPI / SQLAlchemy / React 18 / TypeScript / TanStack Query.

---

## File Structure

**Backend (Python):**
- Create: `tests/storage/test_workspace_tree_schema.py` — schema + migration test
- Create: `tests/db/test_adapter_workspace_children.py` — adapter unit tests
- Create: `tests/web/test_assets_discover_tree.py` — router integration tests
- Modify: `amx/storage/sqlite_store.py` — add CREATE TABLE + `_ensure_workspace_tree_columns` migration
- Modify: `amx/storage/schema_descriptions.py` — schema descriptions for the new table
- Modify: `amx/db/adapters/remote_asset_types.py` — `WorkspaceEntry` dataclass
- Modify: `amx/db/adapters/databricks.py` — `list_workspace_children` method
- Modify: `amx/db/adapters/_databricks_workspace.py` — `list_workspace_objects_immediate`
- Modify: `amx/db/adapters/snowflake.py` — `list_workspace_children` (flat-on-root)
- Modify: `amx/db/connector.py` — passthrough wrapper
- Create: `amx/assets/discover_cache.py` — cache read / refresh / walk helpers (keeps router thin)
- Modify: `amx/web/routers/assets.py` — three new endpoints

**Frontend (TypeScript):**
- Modify: `frontend/src/lib/api.ts` — `discoverTree` / `refreshDiscoverTree` / `walkDiscover` helpers + types
- Rewrite: `frontend/src/components/assets/AssetBrowsePicker.tsx` — lazy tree with per-folder refresh

---

## Task 1: Storage table + migration + schema descriptions

**Files:**
- Modify: `amx/storage/sqlite_store.py` (add CREATE TABLE block alongside existing `remote_*`, add `_ensure_workspace_tree_columns`, call it from `init()`)
- Modify: `amx/storage/schema_descriptions.py` (add entry to `SCHEMA_DESCRIPTIONS`)
- Test: `tests/storage/test_workspace_tree_schema.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/storage/test_workspace_tree_schema.py
"""PR-E: ``remote_workspace_tree`` cache table + schema descriptions."""

from __future__ import annotations

import sqlite3

from amx.storage.schema_descriptions import SCHEMA_DESCRIPTIONS
from amx.storage.sqlite_store import SQLiteHistoryStore


def test_init_creates_workspace_tree_table(tmp_path):
    db_path = tmp_path / "history.db"
    SQLiteHistoryStore(db_path).init()
    with sqlite3.connect(db_path) as conn:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(remote_workspace_tree)").fetchall()}
    expected = {
        "profile_name",
        "kind",
        "path",
        "parent_path",
        "name",
        "is_directory",
        "external_id",
        "owner",
        "last_modified",
        "children_fetched_at",
        "fetched_at",
    }
    assert expected.issubset(cols)


def test_init_creates_workspace_tree_parent_index(tmp_path):
    db_path = tmp_path / "history.db"
    SQLiteHistoryStore(db_path).init()
    with sqlite3.connect(db_path) as conn:
        idx = {r[1] for r in conn.execute("PRAGMA index_list(remote_workspace_tree)").fetchall()}
    assert any("parent" in i for i in idx)


def test_schema_descriptions_cover_workspace_tree():
    desc = SCHEMA_DESCRIPTIONS.get("remote_workspace_tree", {})
    for col in (
        "__table__",
        "profile_name",
        "kind",
        "path",
        "parent_path",
        "name",
        "is_directory",
        "external_id",
        "owner",
        "last_modified",
        "children_fetched_at",
        "fetched_at",
    ):
        assert desc.get(col), f"remote_workspace_tree.{col} missing description"


def test_migration_adds_table_to_legacy_db(tmp_path):
    db_path = tmp_path / "history.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE legacy_marker (x INTEGER)")
        conn.commit()
    SQLiteHistoryStore(db_path).init()
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='remote_workspace_tree'"
        ).fetchall()
    assert rows
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/storage/test_workspace_tree_schema.py -q`
Expected: FAIL (table doesn't exist yet)

- [ ] **Step 3: Add CREATE TABLE block**

In `amx/storage/sqlite_store.py`, immediately after the `asset_chunking_overrides` block, add:

```python
            # ── remote_workspace_tree: lazy-fetch cache for the
            # IngestDialog browse step (PR-E). One row per workspace
            # node (folder or leaf); ``parent_path = ''`` is the root
            # level. ``is_directory=1`` rows describe a folder whose
            # immediate children may be lazily fetched on demand;
            # ``children_fetched_at`` stamps when those children
            # were last refreshed (NULL = never).
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS remote_workspace_tree (
                    profile_name TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    path TEXT NOT NULL,
                    parent_path TEXT NOT NULL DEFAULT '',
                    name TEXT NOT NULL,
                    is_directory INTEGER NOT NULL DEFAULT 0,
                    external_id TEXT,
                    owner TEXT,
                    last_modified TIMESTAMP,
                    children_fetched_at REAL,
                    fetched_at REAL NOT NULL,
                    PRIMARY KEY (profile_name, kind, path)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_remote_workspace_tree_parent "
                "ON remote_workspace_tree(profile_name, kind, parent_path)"
            )
```

- [ ] **Step 4: Add migration helper**

After `_ensure_remote_embed_columns`, add:

```python
    def _ensure_workspace_tree_table(self, conn) -> None:
        """Idempotently create the PR-E discover cache table.

        Legacy history.db files don't carry ``remote_workspace_tree``;
        the CREATE TABLE in init() handles fresh DBs, but mirror it
        here so a freshly-opened legacy DB also picks up the table on
        the next ``init()`` boot.
        """
        try:
            row = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name='remote_workspace_tree'"
            ).fetchone()
        except Exception as exc:
            log.warning("Could not check for remote_workspace_tree: %s", exc)
            return
        if row:
            return
        try:
            conn.execute(
                """
                CREATE TABLE remote_workspace_tree (
                    profile_name TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    path TEXT NOT NULL,
                    parent_path TEXT NOT NULL DEFAULT '',
                    name TEXT NOT NULL,
                    is_directory INTEGER NOT NULL DEFAULT 0,
                    external_id TEXT,
                    owner TEXT,
                    last_modified TIMESTAMP,
                    children_fetched_at REAL,
                    fetched_at REAL NOT NULL,
                    PRIMARY KEY (profile_name, kind, path)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_remote_workspace_tree_parent "
                "ON remote_workspace_tree(profile_name, kind, parent_path)"
            )
            log.info("Migrated history.db: created remote_workspace_tree")
        except Exception as exc:
            log.warning("Could not create remote_workspace_tree: %s", exc)
```

Call it from `init()` next to the other `_ensure_*` calls, right after `_ensure_remote_embed_columns(conn)`:

```python
            self._ensure_remote_embed_columns(conn)
            self._ensure_workspace_tree_table(conn)
```

- [ ] **Step 5: Add schema descriptions**

In `amx/storage/schema_descriptions.py`, add a new top-level entry inside `SCHEMA_DESCRIPTIONS`:

```python
    "remote_workspace_tree": {
        "__table__": (
            "PR-E discover cache. Lazy-fetched workspace tree backing the Studio "
            "IngestDialog 'Browse and pick' step — one row per node (folder or "
            "leaf). The picker reads immediate children of a parent on demand "
            "instead of paying the full recursive workspace walk on every dialog "
            "open."
        ),
        "profile_name": "Owning AMX DB profile (matches db_profiles.name).",
        "kind": "Asset kind this node belongs to ('notebook'; reserved for future kinds).",
        "path": "Full platform-native path of the node (Databricks workspace path or Snowflake qualified_name).",
        "parent_path": "Path of the parent folder. '' (empty string) means root level.",
        "name": "Display name (the leaf segment of path).",
        "is_directory": "1 when this row is a folder whose children can be lazily fetched, 0 when this row is an ingestable leaf asset.",
        "external_id": "Platform-native external_id for leaves (e.g. Databricks object_id); NULL for directories.",
        "owner": "Owner per platform conventions (NULL if API doesn't expose).",
        "last_modified": "Last modification TIMESTAMP per the platform (NULL when unavailable).",
        "children_fetched_at": "When this folder's immediate children were last fetched into the cache. NULL means children have never been listed (the chevron expand still triggers a fetch).",
        "fetched_at": "When this row itself was written. Used to age rows for /db assets prune-style cleanup in future passes.",
    },
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `python -m pytest tests/storage/test_workspace_tree_schema.py tests/test_local_schema_comments.py -q`
Expected: PASS (4 + existing schema-comment gate tests)

- [ ] **Step 7: Commit**

```bash
git add amx/storage/sqlite_store.py amx/storage/schema_descriptions.py tests/storage/test_workspace_tree_schema.py
git commit -m "feat(storage): remote_workspace_tree cache table for PR-E lazy discover"
```

---

## Task 2: `WorkspaceEntry` dataclass

**Files:**
- Modify: `amx/db/adapters/remote_asset_types.py`

- [ ] **Step 1: Add the dataclass + export**

At the end of `amx/db/adapters/remote_asset_types.py`, before the existing `RemoteQuery`-style entries finish, add:

```python


@dataclass(frozen=True)
class WorkspaceEntry:
    """One node in a lazily-fetched workspace tree (PR-E).

    The discover cache stores these per parent folder; the Studio
    tree picker reads them via ``GET /api/assets/discover/tree``.
    ``is_directory=True`` rows have ``external_id=None`` — only
    leaves are ingestable. ``last_modified`` is best-effort and may
    be ``None`` on platforms that don't expose it cheaply.
    """

    kind: str  # 'notebook' (future: 'job' | 'pipeline' | ...)
    path: str
    name: str
    is_directory: bool
    external_id: str | None
    owner: str | None
    last_modified: datetime | None
```

(The `datetime` import is already present at the top of the file.)

- [ ] **Step 2: Run import smoke check**

Run: `python -c "from amx.db.adapters.remote_asset_types import WorkspaceEntry; print(WorkspaceEntry.__dataclass_fields__.keys())"`
Expected: dict_keys with the seven fields

- [ ] **Step 3: Commit**

```bash
git add amx/db/adapters/remote_asset_types.py
git commit -m "feat(adapters): WorkspaceEntry dataclass for PR-E lazy discover"
```

---

## Task 3: Databricks workspace client — `list_workspace_objects_immediate`

**Files:**
- Modify: `amx/db/adapters/_databricks_workspace.py`
- Test: `tests/db/test_adapter_workspace_children.py` (test added in Task 5, after the adapter method too)

- [ ] **Step 1: Add immediate-children fetcher**

After the existing `list_workspace_objects` method in `amx/db/adapters/_databricks_workspace.py`, add:

```python
    def list_workspace_objects_immediate(self, *, path: str) -> Iterator[dict[str, Any]]:
        """Yield NOTEBOOK / FILE / DIRECTORY entries immediately under ``path``.

        PR-E: this is the cheap, non-recursive counterpart of
        ``list_workspace_objects``. The Studio tree picker expands
        one folder at a time, so we make a single
        ``/api/2.0/workspace/list`` call per expand instead of
        walking the whole tree on dialog open.
        """
        for page in self._paginated_get(
            "/api/2.0/workspace/list",
            params={"path": path or "/"},
            page_token_field="next_page_token",
            items_field="objects",
        ):
            yield from page
```

- [ ] **Step 2: Lint + format**

Run: `ruff check amx/db/adapters/_databricks_workspace.py && ruff format --check amx/db/adapters/_databricks_workspace.py`
Expected: All checks passed!

- [ ] **Step 3: Commit**

```bash
git add amx/db/adapters/_databricks_workspace.py
git commit -m "feat(adapters/databricks): list_workspace_objects_immediate for PR-E"
```

---

## Task 4: Databricks adapter — `list_workspace_children`

**Files:**
- Modify: `amx/db/adapters/databricks.py`

- [ ] **Step 1: Add adapter method**

In `amx/db/adapters/databricks.py`, near the other `list_remote_*_metadata` methods (right after `list_remote_notebooks_metadata`), add:

```python
    def list_workspace_children(self, engine=None, *, parent_path: str, kind: str):
        """Yield :class:`WorkspaceEntry` rows immediately under ``parent_path``.

        PR-E lazy discover: one ``/api/2.0/workspace/list`` call per
        expand. Files and Git repos are skipped — only NOTEBOOK
        leaves are ingestable. The Studio tree shows folders + leaves
        interleaved at each level.
        """
        from amx.db.adapters.remote_asset_types import WorkspaceEntry

        del engine
        if kind != "notebook":
            return
        for obj in self._workspace_client.list_workspace_objects_immediate(
            path=parent_path or "/"
        ):
            object_type = obj.get("object_type")
            full_path = obj.get("path") or ""
            name = full_path.rsplit("/", 1)[-1] or full_path
            modified_ms = obj.get("modified_at")
            last_modified = (
                datetime.fromtimestamp(modified_ms / 1000, tz=timezone.utc)
                if modified_ms
                else None
            )
            if object_type == "DIRECTORY":
                yield WorkspaceEntry(
                    kind="notebook",
                    path=full_path,
                    name=name,
                    is_directory=True,
                    external_id=None,
                    owner=obj.get("creator_user_name"),
                    last_modified=last_modified,
                )
            elif object_type == "NOTEBOOK":
                yield WorkspaceEntry(
                    kind="notebook",
                    path=full_path,
                    name=name,
                    is_directory=False,
                    external_id=str(obj.get("object_id") or ""),
                    owner=obj.get("creator_user_name"),
                    last_modified=last_modified,
                )
            # FILE / REPO / other types: skipped silently
```

- [ ] **Step 2: Lint + format**

Run: `ruff check amx/db/adapters/databricks.py && ruff format --check amx/db/adapters/databricks.py`
Expected: All checks passed!

- [ ] **Step 3: Commit**

```bash
git add amx/db/adapters/databricks.py
git commit -m "feat(adapters/databricks): list_workspace_children yields immediate tree level"
```

---

## Task 5: Adapter tests — Databricks + Snowflake `list_workspace_children`

**Files:**
- Test: `tests/db/test_adapter_workspace_children.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/db/test_adapter_workspace_children.py
"""PR-E: per-adapter ``list_workspace_children`` returns immediate tree level."""

from __future__ import annotations

from unittest.mock import MagicMock


def _databricks_adapter():
    from types import SimpleNamespace

    from amx.db.adapters.databricks import DatabricksAdapter

    a = DatabricksAdapter.__new__(DatabricksAdapter)
    a.cfg = SimpleNamespace(  # type: ignore[attr-defined]
        host="https://example", access_token="t", workspace_token=None
    )
    a._workspace_client_override = MagicMock()  # type: ignore[attr-defined]
    return a


def test_databricks_yields_dir_and_notebook_skips_file():
    a = _databricks_adapter()
    a._workspace_client_override.list_workspace_objects_immediate.return_value = iter(
        [
            {
                "object_id": 1,
                "object_type": "DIRECTORY",
                "path": "/Users/alice/folder",
            },
            {
                "object_id": 2,
                "object_type": "NOTEBOOK",
                "path": "/Users/alice/nb1",
                "modified_at": 1700000000000,
                "creator_user_name": "alice@x.com",
            },
            {
                "object_id": 3,
                "object_type": "FILE",
                "path": "/Users/alice/readme.txt",
            },
            {
                "object_id": 4,
                "object_type": "REPO",
                "path": "/Repos/alice",
            },
        ]
    )
    rows = list(a.list_workspace_children(parent_path="/Users/alice", kind="notebook"))
    assert len(rows) == 2
    folder, notebook = rows
    assert folder.is_directory and folder.external_id is None
    assert folder.path == "/Users/alice/folder"
    assert folder.name == "folder"
    assert not notebook.is_directory
    assert notebook.external_id == "2"
    assert notebook.owner == "alice@x.com"


def test_databricks_unknown_kind_yields_nothing():
    a = _databricks_adapter()
    a._workspace_client_override.list_workspace_objects_immediate.return_value = iter(
        [{"object_id": 1, "object_type": "NOTEBOOK", "path": "/x"}]
    )
    assert list(a.list_workspace_children(parent_path="/", kind="job")) == []


def test_snowflake_root_returns_flat_leaves():
    """Snowflake has no notebook folder hierarchy — root yields leaves."""
    from types import SimpleNamespace

    from amx.db.adapters.snowflake import SnowflakeAdapter

    a = SnowflakeAdapter.__new__(SnowflakeAdapter)

    class _Meta:
        def __init__(self):
            self.kind = "notebook"
            self.path = "DB.SCHEMA.NB1"
            self.name = "NB1"
            self.external_id = "DB.SCHEMA.NB1"
            self.owner = "OWNER"
            self.last_modified = None

    def fake_metadata(engine):
        del engine
        yield _Meta()

    a.list_remote_notebooks_metadata = fake_metadata  # type: ignore[method-assign]
    rows = list(a.list_workspace_children(engine=None, parent_path="", kind="notebook"))
    assert len(rows) == 1
    assert rows[0].path == "DB.SCHEMA.NB1"
    assert rows[0].is_directory is False


def test_snowflake_subfolder_returns_empty():
    from amx.db.adapters.snowflake import SnowflakeAdapter

    a = SnowflakeAdapter.__new__(SnowflakeAdapter)
    rows = list(a.list_workspace_children(engine=None, parent_path="DB.SCHEMA", kind="notebook"))
    assert rows == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/db/test_adapter_workspace_children.py -q`
Expected: FAIL on the Snowflake tests (method doesn't exist yet on Snowflake adapter); Databricks tests should now PASS thanks to Task 4.

- [ ] **Step 3: Add Snowflake adapter method**

In `amx/db/adapters/snowflake.py`, near `list_remote_notebooks_metadata`, add:

```python
    def list_workspace_children(self, engine=None, *, parent_path: str, kind: str):
        """PR-E lazy discover for Snowflake.

        Snowflake notebooks have no folder hierarchy: every notebook
        sits directly under ``DB.SCHEMA.NAME``. For tree-mode parity
        with Databricks we flatten:

        * ``parent_path = ""`` → yield every notebook as a leaf via
          ``list_remote_notebooks_metadata``.
        * Any non-empty ``parent_path`` → yield nothing. (A future
          pass can introduce DB.SCHEMA pseudo-folders.)
        """
        from amx.db.adapters.remote_asset_types import WorkspaceEntry

        if kind != "notebook" or parent_path:
            return
        for meta in self.list_remote_notebooks_metadata(engine):
            yield WorkspaceEntry(
                kind="notebook",
                path=meta.path or meta.external_id,
                name=meta.name,
                is_directory=False,
                external_id=meta.external_id,
                owner=meta.owner,
                last_modified=meta.last_modified,
            )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/db/test_adapter_workspace_children.py -q`
Expected: PASS (4 tests)

- [ ] **Step 5: Lint + format**

Run: `ruff check amx/db/adapters/snowflake.py && ruff format --check amx/db/adapters/snowflake.py`
Expected: All checks passed!

- [ ] **Step 6: Commit**

```bash
git add amx/db/adapters/snowflake.py tests/db/test_adapter_workspace_children.py
git commit -m "feat(adapters/snowflake): list_workspace_children flat-on-root for PR-E"
```

---

## Task 6: Connector passthrough wrapper

**Files:**
- Modify: `amx/db/connector.py`

- [ ] **Step 1: Add the wrapper next to the existing `list_remote_*` passthroughs**

In `amx/db/connector.py`, alongside the `list_remote_*` methods near the bottom of the `DatabaseConnector` class, add:

```python
    def list_workspace_children(self, *, parent_path: str, kind: str):
        """PR-E lazy discover — yield immediate children of ``parent_path``.

        Forwards to the adapter's ``list_workspace_children`` with
        the connector's live engine so the call shape matches the
        rest of the ``list_remote_*`` passthroughs.
        """
        return self._adapter.list_workspace_children(
            self.engine, parent_path=parent_path, kind=kind
        )
```

- [ ] **Step 2: Smoke check imports**

Run: `python -c "from amx.db.connector import DatabaseConnector; import inspect; print(inspect.signature(DatabaseConnector.list_workspace_children))"`
Expected: prints `(self, *, parent_path: 'str', kind: 'str')`

- [ ] **Step 3: Commit**

```bash
git add amx/db/connector.py
git commit -m "feat(db/connector): list_workspace_children passthrough"
```

---

## Task 7: Cache helpers module (`discover_cache.py`)

**Files:**
- Create: `amx/assets/discover_cache.py`
- Test: tests come in Task 8 (router integration covers helpers).

- [ ] **Step 1: Create the cache helpers module**

```python
# amx/assets/discover_cache.py
"""Lazy discover-tree cache reads + writes (PR-E).

Keeps the router thin. Three operations:

* :func:`read_children` — fetch immediate children for a parent
  from cache. Returns ``None`` when the parent's
  ``children_fetched_at`` is still NULL (caller should fall back
  to a fetch).
* :func:`refresh_parent` — atomic per-parent replace: drop old
  children + insert new ones + stamp the parent row's
  ``children_fetched_at``.
* :func:`walk_full` — drop all rows for (profile, kind), then
  insert every leaf yielded by a recursive walker plus the
  synthetic directory ancestors. Stamps ``children_fetched_at``
  on every materialised directory so the tree picker doesn't
  refetch them when the user expands.
"""

from __future__ import annotations

import time
from typing import Any, Iterable

from amx.db.adapters.remote_asset_types import WorkspaceEntry


def _row_dict(row: Any) -> dict[str, Any]:
    """Normalise a sqlite3.Row (or tuple) into a plain dict."""
    if hasattr(row, "keys"):
        return {k: row[k] for k in row.keys()}
    return dict(row)


def read_children(
    conn: Any, *, profile: str, kind: str, parent_path: str
) -> tuple[list[dict[str, Any]], float | None]:
    """Return (rows, parent_fetched_at) when children are cached.

    ``parent_fetched_at`` is ``None`` when the parent row exists
    but its children have not been listed yet (treated as a cache
    miss by the router). When the parent row itself is missing
    (e.g. root before any seed), the function returns
    ``([], None)``.
    """
    parent_row = conn.execute(
        "SELECT children_fetched_at FROM remote_workspace_tree "
        "WHERE profile_name = ? AND kind = ? AND path = ?",
        (profile, kind, parent_path),
    ).fetchone()
    # Root parent ('') is implicit — we use the all-children row
    # marker stored separately below. When parent_path is '' and
    # there's no marker row, the cache is cold.
    if parent_row is None and parent_path != "":
        return [], None
    parent_fetched_at = None
    if parent_row is not None:
        parent_fetched_at = parent_row["children_fetched_at"]
        if parent_fetched_at is None:
            return [], None
    else:
        # parent_path == "" path: look up the synthetic root marker
        root_marker = conn.execute(
            "SELECT children_fetched_at FROM remote_workspace_tree "
            "WHERE profile_name = ? AND kind = ? AND path = '' AND is_directory = 1",
            (profile, kind),
        ).fetchone()
        if root_marker is None:
            return [], None
        parent_fetched_at = root_marker["children_fetched_at"]
        if parent_fetched_at is None:
            return [], None
    rows = conn.execute(
        "SELECT path, parent_path, name, is_directory, external_id, "
        "owner, last_modified, children_fetched_at, fetched_at "
        "FROM remote_workspace_tree "
        "WHERE profile_name = ? AND kind = ? AND parent_path = ?",
        (profile, kind, parent_path),
    ).fetchall()
    return [_row_dict(r) for r in rows], float(parent_fetched_at)


def refresh_parent(
    conn: Any,
    *,
    profile: str,
    kind: str,
    parent_path: str,
    entries: Iterable[WorkspaceEntry],
) -> int:
    """Atomically replace ``parent_path``'s immediate children.

    Returns the number of children written. Grandchildren of the
    parent are left untouched — their own ``children_fetched_at``
    survives so subsequent expands stay cache-hits.
    """
    now = time.time()
    materialised = list(entries)
    conn.execute(
        "DELETE FROM remote_workspace_tree "
        "WHERE profile_name = ? AND kind = ? AND parent_path = ?",
        (profile, kind, parent_path),
    )
    for entry in materialised:
        conn.execute(
            "INSERT OR REPLACE INTO remote_workspace_tree "
            "(profile_name, kind, path, parent_path, name, is_directory, "
            "external_id, owner, last_modified, children_fetched_at, fetched_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?)",
            (
                profile,
                kind,
                entry.path,
                parent_path,
                entry.name,
                1 if entry.is_directory else 0,
                entry.external_id,
                entry.owner,
                entry.last_modified.isoformat() if entry.last_modified else None,
                now,
            ),
        )
    # Stamp the parent (or the synthetic root marker for parent_path='').
    if parent_path == "":
        conn.execute(
            "INSERT OR REPLACE INTO remote_workspace_tree "
            "(profile_name, kind, path, parent_path, name, is_directory, "
            "external_id, owner, last_modified, children_fetched_at, fetched_at) "
            "VALUES (?, ?, '', '', '', 1, NULL, NULL, NULL, ?, ?)",
            (profile, kind, now, now),
        )
    else:
        conn.execute(
            "UPDATE remote_workspace_tree "
            "SET children_fetched_at = ?, fetched_at = ? "
            "WHERE profile_name = ? AND kind = ? AND path = ?",
            (now, now, profile, kind, parent_path),
        )
    conn.commit()
    return len(materialised)


def walk_full(
    conn: Any,
    *,
    profile: str,
    kind: str,
    leaves: Iterable[Any],
) -> dict[str, int]:
    """Replace every row for (profile, kind) with the walk result.

    ``leaves`` yields ``AssetMetadata``-shaped objects with ``path``,
    ``name``, ``external_id``, ``owner``, ``last_modified``. Folder
    ancestors are synthesised from each leaf's path so the tree
    renders correctly afterwards.
    """
    now = time.time()
    conn.execute(
        "DELETE FROM remote_workspace_tree WHERE profile_name = ? AND kind = ?",
        (profile, kind),
    )
    leaves_written = 0
    directories_written = 0
    dirs_seen: set[str] = {""}  # root marker handled below
    for leaf in leaves:
        leaf_path = leaf.path or leaf.external_id
        if not leaf_path:
            continue
        # Synthesize ancestor directories.
        if "/" in leaf_path:
            parts = leaf_path.split("/")
            for i in range(1, len(parts)):
                ancestor = "/".join(parts[:i]) or "/"
                if ancestor in dirs_seen:
                    continue
                dirs_seen.add(ancestor)
                ancestor_parent = "/".join(parts[: i - 1]) or ""
                if ancestor == "/":
                    ancestor_parent = ""
                ancestor_name = parts[i - 1] if i > 0 else "/"
                conn.execute(
                    "INSERT OR REPLACE INTO remote_workspace_tree "
                    "(profile_name, kind, path, parent_path, name, is_directory, "
                    "external_id, owner, last_modified, children_fetched_at, fetched_at) "
                    "VALUES (?, ?, ?, ?, ?, 1, NULL, NULL, NULL, ?, ?)",
                    (profile, kind, ancestor, ancestor_parent, ancestor_name, now, now),
                )
                directories_written += 1
        leaf_parent = leaf_path.rsplit("/", 1)[0] if "/" in leaf_path else ""
        conn.execute(
            "INSERT OR REPLACE INTO remote_workspace_tree "
            "(profile_name, kind, path, parent_path, name, is_directory, "
            "external_id, owner, last_modified, children_fetched_at, fetched_at) "
            "VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?, NULL, ?)",
            (
                profile,
                kind,
                leaf_path,
                leaf_parent,
                leaf.name,
                leaf.external_id,
                leaf.owner,
                leaf.last_modified.isoformat() if leaf.last_modified else None,
                now,
            ),
        )
        leaves_written += 1
    # Synthetic root marker.
    conn.execute(
        "INSERT OR REPLACE INTO remote_workspace_tree "
        "(profile_name, kind, path, parent_path, name, is_directory, "
        "external_id, owner, last_modified, children_fetched_at, fetched_at) "
        "VALUES (?, ?, '', '', '', 1, NULL, NULL, NULL, ?, ?)",
        (profile, kind, now, now),
    )
    conn.commit()
    return {
        "rows_written": leaves_written + directories_written,
        "directories": directories_written,
        "leaves": leaves_written,
    }


__all__ = ["read_children", "refresh_parent", "walk_full"]
```

- [ ] **Step 2: Lint + format**

Run: `ruff check amx/assets/discover_cache.py && ruff format amx/assets/discover_cache.py`
Expected: All checks passed; possibly reformatted.

- [ ] **Step 3: Smoke import**

Run: `python -c "from amx.assets.discover_cache import read_children, refresh_parent, walk_full; print('ok')"`
Expected: `ok`

- [ ] **Step 4: Commit**

```bash
git add amx/assets/discover_cache.py
git commit -m "feat(assets): discover_cache helpers — read/refresh/walk"
```

---

## Task 8: Router — GET /api/assets/discover/tree

**Files:**
- Modify: `amx/web/routers/assets.py`
- Test: `tests/web/test_assets_discover_tree.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/web/test_assets_discover_tree.py
"""PR-E: GET /api/assets/discover/tree cache-first read."""

from __future__ import annotations

import sqlite3

from fastapi.testclient import TestClient

from amx.config import AMXConfig
from amx.db.adapters.remote_asset_types import WorkspaceEntry
from amx.storage.sqlite_store import SQLiteHistoryStore
from amx.web.server import create_app

_TEST_TOKEN = "test-discover-tree-token"
_AUTH = {"Authorization": f"Bearer {_TEST_TOKEN}"}


def _make_client(tmp_path):
    cfg = AMXConfig()
    cfg.CONFIG_DIR = str(tmp_path)
    db_path = tmp_path / "history.db"
    SQLiteHistoryStore(db_path).init()
    app = create_app(cfg, token=_TEST_TOKEN)
    return TestClient(app), db_path


class _StubConnector:
    def __init__(self, entries):
        self._entries = entries

    def list_workspace_children(self, *, parent_path, kind):
        return iter(self._entries)


def test_tree_get_cache_miss_triggers_adapter_fetch(monkeypatch, tmp_path):
    """First call with empty cache should fetch + write + return rows."""
    from amx.cli_support.commands import db_assets_impl as impl_mod
    from datetime import datetime, timezone

    entries = [
        WorkspaceEntry(
            kind="notebook",
            path="/Users",
            name="Users",
            is_directory=True,
            external_id=None,
            owner=None,
            last_modified=None,
        ),
        WorkspaceEntry(
            kind="notebook",
            path="/Sample.py",
            name="Sample.py",
            is_directory=False,
            external_id="42",
            owner="alice",
            last_modified=datetime(2026, 5, 1, tzinfo=timezone.utc),
        ),
    ]
    monkeypatch.setattr(
        impl_mod, "_open_connector", lambda cfg, profile: _StubConnector(entries)
    )

    client, _db = _make_client(tmp_path)
    resp = client.get(
        "/api/assets/discover/tree?profile=prod&kind=notebook&parent=", headers=_AUTH
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["cache_empty"] is False
    assert body["parent_path"] == ""
    paths = sorted([r["path"] for r in body["items"]])
    assert paths == ["/Sample.py", "/Users"]


def test_tree_get_cache_hit_does_not_call_adapter(monkeypatch, tmp_path):
    """A populated cache must short-circuit the adapter call."""
    from amx.cli_support.commands import db_assets_impl as impl_mod

    client, db_path = _make_client(tmp_path)
    # Seed the cache directly.
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO remote_workspace_tree "
            "(profile_name, kind, path, parent_path, name, is_directory, "
            "external_id, owner, last_modified, children_fetched_at, fetched_at) "
            "VALUES ('prod', 'notebook', '', '', '', 1, NULL, NULL, NULL, ?, ?)",
            (1716333600.0, 1716333600.0),
        )
        conn.execute(
            "INSERT INTO remote_workspace_tree "
            "(profile_name, kind, path, parent_path, name, is_directory, "
            "external_id, owner, last_modified, children_fetched_at, fetched_at) "
            "VALUES ('prod', 'notebook', '/Cached', '', 'Cached', 1, NULL, "
            "NULL, NULL, NULL, ?)",
            (1716333600.0,),
        )
        conn.commit()

    def _boom(cfg, profile):
        raise AssertionError("adapter must not be opened on cache hit")

    monkeypatch.setattr(impl_mod, "_open_connector", _boom)
    resp = client.get(
        "/api/assets/discover/tree?profile=prod&kind=notebook&parent=", headers=_AUTH
    )
    assert resp.status_code == 200
    body = resp.json()
    assert any(r["path"] == "/Cached" for r in body["items"])
    assert body["cache_empty"] is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/web/test_assets_discover_tree.py -q`
Expected: FAIL (route doesn't exist)

- [ ] **Step 3: Add the route**

In `amx/web/routers/assets.py`, after the existing `discover_assets` route, add:

```python
@router.get("/discover/tree")
def discover_tree(
    profile: str = Query(...),
    kind: str = Query(default="notebook"),
    parent: str = Query(default=""),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Return immediate children of ``parent`` for the Studio tree picker.

    PR-E: cache-first. When the cache lacks a fresh entry for the
    parent, fetch the immediate children from the connected adapter
    once and stamp the cache.
    """
    from amx.assets.discover_cache import read_children, refresh_parent

    db_path = _history_db_path(cfg)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cached, parent_fetched_at = read_children(
            conn, profile=profile, kind=kind, parent_path=parent
        )
        if parent_fetched_at is not None:
            return {
                "items": cached,
                "parent_path": parent,
                "parent_fetched_at": parent_fetched_at,
                "cache_empty": False,
            }
        # Cache miss: ask the adapter for this folder's immediate
        # children. ``_open_connector`` lives in the CLI impl module
        # alongside its profile-resolution logic; reuse it so the
        # router doesn't grow a second connector factory.
        from amx.cli_support.commands.db_assets_impl import _open_connector

        try:
            connector = _open_connector(cfg, profile)
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(502, f"Could not open connector: {exc}") from exc
        try:
            entries = list(
                connector.list_workspace_children(parent_path=parent, kind=kind)
            )
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(
                502, f"Adapter listing failed for parent={parent!r}: {exc}"
            ) from exc
        refresh_parent(
            conn, profile=profile, kind=kind, parent_path=parent, entries=entries
        )
        rows, parent_fetched_at = read_children(
            conn, profile=profile, kind=kind, parent_path=parent
        )
    return {
        "items": rows,
        "parent_path": parent,
        "parent_fetched_at": parent_fetched_at,
        "cache_empty": False,
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/web/test_assets_discover_tree.py -q`
Expected: PASS (2 tests)

- [ ] **Step 5: Lint + format**

Run: `ruff check amx/web/routers/assets.py && ruff format --check amx/web/routers/assets.py`
Expected: All checks passed!

- [ ] **Step 6: Commit**

```bash
git add amx/web/routers/assets.py tests/web/test_assets_discover_tree.py
git commit -m "feat(assets/web): GET /api/assets/discover/tree cache-first"
```

---

## Task 9: Router — POST /api/assets/discover/tree/refresh

**Files:**
- Modify: `amx/web/routers/assets.py`
- Test: `tests/web/test_assets_discover_tree.py` (extend)

- [ ] **Step 1: Write the failing test**

Append to `tests/web/test_assets_discover_tree.py`:

```python
def test_tree_refresh_replaces_only_target_parent(monkeypatch, tmp_path):
    """Refreshing /Users must drop its old children but leave /Other intact."""
    from amx.cli_support.commands import db_assets_impl as impl_mod

    client, db_path = _make_client(tmp_path)

    with sqlite3.connect(db_path) as conn:
        # Two sibling parents each carrying one child + the root marker.
        conn.executescript(
            """
            INSERT INTO remote_workspace_tree
                (profile_name, kind, path, parent_path, name, is_directory,
                 external_id, owner, last_modified, children_fetched_at, fetched_at)
            VALUES
                ('prod', 'notebook', '', '', '', 1, NULL, NULL, NULL, 1.0, 1.0),
                ('prod', 'notebook', '/Users', '', 'Users', 1, NULL, NULL, NULL, 1.0, 1.0),
                ('prod', 'notebook', '/Other', '', 'Other', 1, NULL, NULL, NULL, 1.0, 1.0),
                ('prod', 'notebook', '/Users/stale.py', '/Users', 'stale.py',
                 0, '99', NULL, NULL, NULL, 1.0),
                ('prod', 'notebook', '/Other/keep.py', '/Other', 'keep.py',
                 0, '77', NULL, NULL, NULL, 1.0);
            """
        )
        conn.commit()

    fresh = [
        WorkspaceEntry(
            kind="notebook",
            path="/Users/fresh.py",
            name="fresh.py",
            is_directory=False,
            external_id="100",
            owner="alice",
            last_modified=None,
        )
    ]
    monkeypatch.setattr(
        impl_mod, "_open_connector", lambda cfg, profile: _StubConnector(fresh)
    )
    resp = client.post(
        "/api/assets/discover/tree/refresh?profile=prod&kind=notebook&parent=/Users",
        headers=_AUTH,
    )
    assert resp.status_code == 200, resp.text

    with sqlite3.connect(db_path) as conn:
        children_users = sorted(
            r[0]
            for r in conn.execute(
                "SELECT path FROM remote_workspace_tree "
                "WHERE profile_name='prod' AND parent_path='/Users'"
            ).fetchall()
        )
        children_other = sorted(
            r[0]
            for r in conn.execute(
                "SELECT path FROM remote_workspace_tree "
                "WHERE profile_name='prod' AND parent_path='/Other'"
            ).fetchall()
        )
    assert children_users == ["/Users/fresh.py"]  # stale.py replaced
    assert children_other == ["/Other/keep.py"]  # sibling untouched
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/web/test_assets_discover_tree.py::test_tree_refresh_replaces_only_target_parent -q`
Expected: FAIL (route doesn't exist)

- [ ] **Step 3: Add the refresh route**

After `discover_tree` in `amx/web/routers/assets.py`, add:

```python
@router.post("/discover/tree/refresh")
def refresh_tree(
    profile: str = Query(...),
    kind: str = Query(default="notebook"),
    parent: str = Query(default=""),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Force-refresh ``parent``'s immediate children (atomic replace).

    PR-E: descendants of grandchildren are NOT touched — their own
    ``children_fetched_at`` survives so the picker keeps deeper
    cache hits valid.
    """
    from amx.assets.discover_cache import refresh_parent, read_children
    from amx.cli_support.commands.db_assets_impl import _open_connector

    db_path = _history_db_path(cfg)
    try:
        connector = _open_connector(cfg, profile)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(502, f"Could not open connector: {exc}") from exc
    try:
        entries = list(
            connector.list_workspace_children(parent_path=parent, kind=kind)
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            502, f"Adapter listing failed for parent={parent!r}: {exc}"
        ) from exc
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        written = refresh_parent(
            conn, profile=profile, kind=kind, parent_path=parent, entries=entries
        )
        rows, parent_fetched_at = read_children(
            conn, profile=profile, kind=kind, parent_path=parent
        )
    return {
        "items": rows,
        "parent_path": parent,
        "parent_fetched_at": parent_fetched_at,
        "cache_empty": False,
        "written": written,
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/web/test_assets_discover_tree.py -q`
Expected: PASS (3 tests now)

- [ ] **Step 5: Lint + format**

Run: `ruff check amx/web/routers/assets.py && ruff format --check amx/web/routers/assets.py`
Expected: All checks passed!

- [ ] **Step 6: Commit**

```bash
git add amx/web/routers/assets.py tests/web/test_assets_discover_tree.py
git commit -m "feat(assets/web): POST /discover/tree/refresh atomic per-folder replace"
```

---

## Task 10: Router — POST /api/assets/discover/tree/walk

**Files:**
- Modify: `amx/web/routers/assets.py`
- Test: `tests/web/test_assets_discover_tree.py` (extend)

- [ ] **Step 1: Write the failing test**

Append to `tests/web/test_assets_discover_tree.py`:

```python
def test_tree_walk_seeds_full_cache(monkeypatch, tmp_path):
    """POST /walk replaces every row + synthesises ancestor dirs."""
    from amx.cli_support.commands import db_assets_impl as impl_mod
    from amx.db.adapters.remote_asset_types import AssetMetadata

    client, db_path = _make_client(tmp_path)

    leaves = [
        AssetMetadata(
            kind="notebook",
            external_id="1",
            name="nb_a.py",
            path="/Users/alice/nb_a.py",
            owner="alice",
            last_modified=None,
        ),
        AssetMetadata(
            kind="notebook",
            external_id="2",
            name="nb_b.py",
            path="/Users/alice/nb_b.py",
            owner="alice",
            last_modified=None,
        ),
    ]

    class _WalkConnector:
        def list_remote_notebooks_metadata(self):
            return iter(leaves)

    monkeypatch.setattr(
        impl_mod, "_open_connector", lambda cfg, profile: _WalkConnector()
    )

    resp = client.post(
        "/api/assets/discover/tree/walk?profile=prod&kind=notebook", headers=_AUTH
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["leaves"] == 2
    assert body["directories"] >= 2  # /Users + /Users/alice synthesised

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT path, is_directory FROM remote_workspace_tree "
            "WHERE profile_name='prod' AND kind='notebook' ORDER BY path"
        ).fetchall()
    paths = {p for p, _d in rows}
    assert "/Users" in paths
    assert "/Users/alice" in paths
    assert "/Users/alice/nb_a.py" in paths
    assert "/Users/alice/nb_b.py" in paths
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/web/test_assets_discover_tree.py::test_tree_walk_seeds_full_cache -q`
Expected: FAIL

- [ ] **Step 3: Add the walk route**

After `refresh_tree`, add:

```python
@router.post("/discover/tree/walk")
def walk_tree(
    profile: str = Query(...),
    kind: str = Query(default="notebook"),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Full recursive walk that seeds the entire cache (PR-E).

    Used by the Studio search input when the cache is completely
    empty — paying the slow walk once unlocks instant subsequent
    searches AND tree expansions.
    """
    from amx.assets.discover_cache import walk_full
    from amx.cli_support.commands.db_assets_impl import _open_connector

    db_path = _history_db_path(cfg)
    try:
        connector = _open_connector(cfg, profile)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(502, f"Could not open connector: {exc}") from exc
    fetch_method_name = {
        "notebook": "list_remote_notebooks_metadata",
    }.get(kind)
    if fetch_method_name is None:
        raise HTTPException(400, f"Walk not supported for kind={kind!r}.")
    fetcher = getattr(connector, fetch_method_name, None)
    if fetcher is None:
        raise HTTPException(
            501, f"Adapter has no {fetch_method_name} for profile={profile!r}."
        )
    try:
        leaves = list(fetcher())
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(502, f"Walk failed: {exc}") from exc
    with sqlite3.connect(db_path) as conn:
        counts = walk_full(conn, profile=profile, kind=kind, leaves=leaves)
    return counts
```

- [ ] **Step 4: Run tests to verify all pass**

Run: `python -m pytest tests/web/test_assets_discover_tree.py -q`
Expected: PASS (4 tests now)

- [ ] **Step 5: Lint + format**

Run: `ruff check amx/web/routers/assets.py && ruff format --check amx/web/routers/assets.py`
Expected: All checks passed!

- [ ] **Step 6: Commit**

```bash
git add amx/web/routers/assets.py tests/web/test_assets_discover_tree.py
git commit -m "feat(assets/web): POST /discover/tree/walk seeds full cache"
```

---

## Task 11: Frontend API helpers + types

**Files:**
- Modify: `frontend/src/lib/api.ts`

- [ ] **Step 1: Add types alongside the existing `RemoteAssetMetadata`**

Find the `RemoteAssetMetadata` interface block in `frontend/src/lib/api.ts`. Immediately after it, add:

```ts
/** One row from GET /api/assets/discover/tree (PR-E lazy tree). */
export interface DiscoverTreeNode {
  path: string;
  parent_path: string;
  name: string;
  is_directory: boolean | number; // SQLite returns 0/1
  external_id: string | null;
  owner: string | null;
  last_modified: string | null;
  children_fetched_at: number | null;
  fetched_at: number;
}

/** Response shape for GET /api/assets/discover/tree. */
export interface DiscoverTreeResponse {
  items: DiscoverTreeNode[];
  parent_path: string;
  parent_fetched_at: number | null;
  cache_empty: boolean;
}

/** Response shape for POST /api/assets/discover/tree/walk. */
export interface DiscoverTreeWalkResult {
  rows_written: number;
  directories: number;
  leaves: number;
}
```

- [ ] **Step 2: Add the three API helpers**

Find the `discoverAssets` helper in the `api` object. Immediately after it, add:

```ts
  /**
   * PR-E: fetch immediate children of ``parent`` from the lazy
   * discover cache. Backend transparently fetches + writes the
   * cache on a miss.
   */
  discoverTree: (params: {
    profile: string;
    kind?: string;
    parent?: string;
  }) => {
    const qs = new URLSearchParams({
      profile: params.profile,
      kind: params.kind ?? "notebook",
      parent: params.parent ?? "",
    });
    return apiFetch<DiscoverTreeResponse>(
      `/api/assets/discover/tree?${qs.toString()}`,
    );
  },

  /** PR-E: refresh one folder's immediate children. Atomic replace. */
  refreshDiscoverTree: (params: {
    profile: string;
    kind?: string;
    parent?: string;
  }) => {
    const qs = new URLSearchParams({
      profile: params.profile,
      kind: params.kind ?? "notebook",
      parent: params.parent ?? "",
    });
    return apiFetch<DiscoverTreeResponse>(
      `/api/assets/discover/tree/refresh?${qs.toString()}`,
      { method: "POST" },
    );
  },

  /**
   * PR-E: full recursive walk — seeds the entire cache. Slow on
   * first call, instant on subsequent reads. Used by the search
   * input when the cache is empty.
   */
  walkDiscoverTree: (params: { profile: string; kind?: string }) => {
    const qs = new URLSearchParams({
      profile: params.profile,
      kind: params.kind ?? "notebook",
    });
    return apiFetch<DiscoverTreeWalkResult>(
      `/api/assets/discover/tree/walk?${qs.toString()}`,
      { method: "POST" },
    );
  },
```

- [ ] **Step 3: Type-check**

Run: `cd frontend && npx tsc --noEmit`
Expected: no output (clean)

- [ ] **Step 4: Commit**

```bash
git add frontend/src/lib/api.ts
git commit -m "feat(assets/api): discoverTree + refreshDiscoverTree + walkDiscoverTree"
```

---

## Task 12: Frontend `AssetBrowsePicker` rewrite — lazy tree

**Files:**
- Rewrite: `frontend/src/components/assets/AssetBrowsePicker.tsx`

- [ ] **Step 1: Replace AssetBrowsePicker with the lazy-tree implementation**

Overwrite the file completely with this content (the previous folder-grouping client-side approach is superseded by the server-driven tree):

```tsx
/**
 * AssetBrowsePicker — PR-E lazy tree.
 *
 * Talks to the new ``/api/assets/discover/tree`` endpoints:
 * root level loads instantly from the SQLite cache, each folder
 * expand fires one /tree call for its immediate children, and a
 * per-folder refresh icon re-fetches just that level. Search
 * filters across loaded rows when the cache has any data; an
 * empty cache triggers a full walk on first search.
 */

import {
  ChevronDown,
  ChevronRight,
  Loader2,
  RefreshCw,
  Search,
} from "lucide-react";
import { useCallback, useEffect, useRef, useState } from "react";

import {
  api,
  type DiscoverTreeNode,
} from "../../lib/api";

const PICKABLE_KINDS: Array<{ id: string; label: string; kindParam: string }> = [
  { id: "notebooks", label: "Notebooks", kindParam: "notebook" },
];

interface Props {
  profile: string;
  enabledKinds: string[];
  selection: Record<string, Set<string>>;
  onSelectionChange: (next: Record<string, Set<string>>) => void;
  disabled?: boolean;
}

interface NodeState {
  node: DiscoverTreeNode;
  expanded: boolean;
  loading: boolean;
  error: string | null;
  children: DiscoverTreeNode[] | null; // null = never fetched
}

export default function AssetBrowsePicker({
  profile,
  enabledKinds,
  selection,
  onSelectionChange,
  disabled,
}: Props) {
  const tabs = PICKABLE_KINDS.filter((k) => enabledKinds.includes(k.id));
  const [activeTabId, setActiveTabId] = useState<string>(
    tabs[0]?.id ?? "notebooks",
  );
  const activeTab =
    tabs.find((t) => t.id === activeTabId) ??
    tabs[0] ??
    PICKABLE_KINDS[0];

  // Cache the tree state per (profile, kind). Keys are the path
  // string; the root level is keyed by ''. ``children`` is null
  // until a fetch resolves.
  const [nodes, setNodes] = useState<Record<string, NodeState>>({});
  const [rootLoading, setRootLoading] = useState(false);
  const [rootError, setRootError] = useState<string | null>(null);
  const [rootChildren, setRootChildren] = useState<DiscoverTreeNode[] | null>(
    null,
  );
  const [rootRefreshing, setRootRefreshing] = useState(false);

  // Search
  const [filter, setFilter] = useState("");
  const [debouncedFilter, setDebouncedFilter] = useState("");
  const [walking, setWalking] = useState(false);
  const [walkError, setWalkError] = useState<string | null>(null);

  // Reset all state when the underlying profile/kind changes.
  useEffect(() => {
    setNodes({});
    setRootChildren(null);
    setRootError(null);
    setFilter("");
    setDebouncedFilter("");
  }, [profile, activeTab.kindParam]);

  // Debounce search.
  useEffect(() => {
    const t = setTimeout(() => setDebouncedFilter(filter.trim()), 200);
    return () => clearTimeout(t);
  }, [filter]);

  // Fetch root the first time the picker is shown for this
  // profile/kind.
  const fetchRoot = useCallback(
    async (force: boolean) => {
      if (force) setRootRefreshing(true);
      else setRootLoading(true);
      setRootError(null);
      try {
        const fn = force ? api.refreshDiscoverTree : api.discoverTree;
        const res = await fn({
          profile,
          kind: activeTab.kindParam,
          parent: "",
        });
        setRootChildren(res.items);
      } catch (err) {
        setRootError((err as Error).message || "Failed to load workspace.");
      } finally {
        setRootLoading(false);
        setRootRefreshing(false);
      }
    },
    [profile, activeTab.kindParam],
  );

  useEffect(() => {
    if (!profile) return;
    if (rootChildren !== null) return;
    fetchRoot(false);
  }, [profile, rootChildren, fetchRoot]);

  // Per-folder fetch helpers.
  const fetchChildren = useCallback(
    async (parentPath: string, force: boolean) => {
      setNodes((prev) => ({
        ...prev,
        [parentPath]: {
          ...(prev[parentPath] ?? {
            node: {
              path: parentPath,
              parent_path: "",
              name: parentPath.split("/").pop() ?? parentPath,
              is_directory: true,
              external_id: null,
              owner: null,
              last_modified: null,
              children_fetched_at: null,
              fetched_at: 0,
            },
            expanded: true,
            children: null,
          }),
          loading: true,
          error: null,
          expanded: true,
        },
      }));
      try {
        const fn = force ? api.refreshDiscoverTree : api.discoverTree;
        const res = await fn({
          profile,
          kind: activeTab.kindParam,
          parent: parentPath,
        });
        setNodes((prev) => ({
          ...prev,
          [parentPath]: {
            ...prev[parentPath]!,
            loading: false,
            error: null,
            children: res.items,
            expanded: true,
          },
        }));
      } catch (err) {
        setNodes((prev) => ({
          ...prev,
          [parentPath]: {
            ...prev[parentPath]!,
            loading: false,
            error: (err as Error).message ?? "Fetch failed.",
            expanded: true,
          },
        }));
      }
    },
    [profile, activeTab.kindParam],
  );

  const toggleFolder = useCallback(
    (folder: DiscoverTreeNode) => {
      const state = nodes[folder.path];
      if (state?.expanded) {
        setNodes((prev) => ({
          ...prev,
          [folder.path]: { ...prev[folder.path]!, expanded: false },
        }));
        return;
      }
      if (state?.children) {
        setNodes((prev) => ({
          ...prev,
          [folder.path]: { ...prev[folder.path]!, expanded: true },
        }));
        return;
      }
      // First open of this folder: register a state row + fetch.
      setNodes((prev) => ({
        ...prev,
        [folder.path]: {
          node: folder,
          expanded: true,
          loading: false,
          error: null,
          children: null,
        },
      }));
      void fetchChildren(folder.path, false);
    },
    [nodes, fetchChildren],
  );

  // ── Selection ────────────────────────────────────────────────
  const selectedSet = selection[activeTab.id] ?? new Set<string>();

  const toggleLeaf = (leaf: DiscoverTreeNode) => {
    if (!leaf.external_id) return;
    const next = new Set(selectedSet);
    if (next.has(leaf.external_id)) next.delete(leaf.external_id);
    else next.add(leaf.external_id);
    onSelectionChange({ ...selection, [activeTab.id]: next });
  };

  // ── Search ──────────────────────────────────────────────────
  const allLoadedLeaves = useCallback((): DiscoverTreeNode[] => {
    const acc: DiscoverTreeNode[] = [];
    const pushLeaf = (n: DiscoverTreeNode) => {
      if (!isDir(n)) acc.push(n);
    };
    (rootChildren ?? []).forEach(pushLeaf);
    Object.values(nodes).forEach((s) => {
      (s.children ?? []).forEach(pushLeaf);
    });
    return acc;
  }, [rootChildren, nodes]);

  const cacheHasAnyRow = (rootChildren?.length ?? 0) > 0;

  const onWalk = async () => {
    setWalking(true);
    setWalkError(null);
    try {
      await api.walkDiscoverTree({ profile, kind: activeTab.kindParam });
      // Re-seed UI by re-reading the root.
      await fetchRoot(false);
    } catch (err) {
      setWalkError((err as Error).message ?? "Walk failed.");
    } finally {
      setWalking(false);
    }
  };

  const matched = (() => {
    if (!debouncedFilter) return null;
    const needle = debouncedFilter.toLowerCase();
    return allLoadedLeaves().filter((l) => {
      const hay = `${l.name} ${l.path} ${l.owner ?? ""}`.toLowerCase();
      return hay.includes(needle);
    });
  })();

  // ── Render ──────────────────────────────────────────────────
  if (tabs.length === 0) {
    return (
      <p className="rounded-md border border-dashed border-border px-3 py-4 text-center text-sm text-ink-muted">
        Pick "Notebooks" above to browse individual assets.
      </p>
    );
  }

  return (
    <div className="space-y-2">
      {/* Tab strip — for now only Notebooks; the structure is in
          place so future kinds plug in without further rewrite. */}
      <div role="tablist" className="flex flex-wrap gap-1 border-b border-border">
        {tabs.map((tab) => {
          const isActive = tab.id === activeTabId;
          const count = (selection[tab.id] ?? new Set()).size;
          return (
            <button
              key={tab.id}
              role="tab"
              type="button"
              onClick={() => setActiveTabId(tab.id)}
              disabled={disabled}
              className={`-mb-px border-b-2 px-3 py-1.5 text-xs font-medium transition-colors ${
                isActive
                  ? "border-accent text-accent"
                  : "border-transparent text-ink-muted hover:text-ink"
              }`}
            >
              {tab.label}
              {count > 0 && (
                <span className="ml-1.5 rounded-full bg-accent/15 px-1.5 py-0.5 text-[10px] text-accent">
                  {count}
                </span>
              )}
            </button>
          );
        })}
      </div>

      {/* Search + global refresh */}
      <div className="flex items-center gap-2">
        <div className="relative flex-1">
          <Search
            size={14}
            className="pointer-events-none absolute left-2.5 top-1/2 -translate-y-1/2 text-ink-dim"
          />
          <input
            type="search"
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            placeholder="Search by name, path, or owner…"
            disabled={disabled}
            className="w-full rounded-md border border-border bg-surface-raised py-1.5 pl-7 pr-2 text-sm placeholder:text-ink-dim disabled:cursor-not-allowed disabled:opacity-50"
          />
        </div>
        <button
          type="button"
          title="Refresh root folder"
          onClick={() => fetchRoot(true)}
          disabled={disabled || rootRefreshing || rootLoading}
          className="inline-flex h-8 w-8 items-center justify-center rounded-md border border-border hover:bg-surface-subtle disabled:cursor-not-allowed disabled:opacity-50"
          aria-label="Refresh root level"
        >
          {rootRefreshing ? (
            <Loader2 size={14} className="animate-spin" />
          ) : (
            <RefreshCw size={14} />
          )}
        </button>
      </div>

      {/* Selection summary */}
      <div className="flex items-center justify-between gap-2 text-xs text-ink-muted">
        <span>
          {selectedSet.size > 0
            ? `${selectedSet.size} selected`
            : "None selected — every shown row will be skipped at submit."}
        </span>
        {selectedSet.size > 0 && (
          <button
            type="button"
            onClick={() =>
              onSelectionChange({ ...selection, [activeTab.id]: new Set() })
            }
            className="text-ink-muted hover:underline"
          >
            Clear
          </button>
        )}
      </div>

      {/* Content area */}
      <div className="max-h-[55vh] overflow-y-auto rounded-md border border-border">
        {rootLoading ? (
          <div className="px-3 py-6 text-center text-xs text-ink-dim">
            Loading workspace root…
          </div>
        ) : rootError ? (
          <div className="px-3 py-4 text-xs text-critical">{rootError}</div>
        ) : matched ? (
          <SearchResults
            matched={matched}
            cacheHasAnyRow={cacheHasAnyRow}
            walking={walking}
            walkError={walkError}
            onWalk={onWalk}
            selectedSet={selectedSet}
            onToggleLeaf={toggleLeaf}
            disabled={!!disabled}
          />
        ) : (
          <TreeList
            level={(rootChildren ?? []).filter(distinctByPath())}
            nodes={nodes}
            depth={0}
            onToggleFolder={toggleFolder}
            onRefreshFolder={(p) => fetchChildren(p, true)}
            onToggleLeaf={toggleLeaf}
            selectedSet={selectedSet}
            disabled={!!disabled}
          />
        )}
      </div>
    </div>
  );
}

function isDir(n: DiscoverTreeNode): boolean {
  return Boolean(n.is_directory);
}

function distinctByPath() {
  const seen = new Set<string>();
  return (n: DiscoverTreeNode) => {
    if (seen.has(n.path)) return false;
    seen.add(n.path);
    return true;
  };
}

interface TreeListProps {
  level: DiscoverTreeNode[];
  nodes: Record<string, NodeState>;
  depth: number;
  onToggleFolder: (folder: DiscoverTreeNode) => void;
  onRefreshFolder: (path: string) => void;
  onToggleLeaf: (leaf: DiscoverTreeNode) => void;
  selectedSet: Set<string>;
  disabled: boolean;
}

function TreeList({
  level,
  nodes,
  depth,
  onToggleFolder,
  onRefreshFolder,
  onToggleLeaf,
  selectedSet,
  disabled,
}: TreeListProps) {
  if (level.length === 0) {
    return (
      <div className="px-3 py-4 text-center text-xs text-ink-muted">
        Empty.
      </div>
    );
  }
  return (
    <ul className="divide-y divide-border/60">
      {level.map((entry) =>
        isDir(entry) ? (
          <FolderRow
            key={entry.path}
            folder={entry}
            state={nodes[entry.path]}
            depth={depth}
            onToggleFolder={onToggleFolder}
            onRefreshFolder={onRefreshFolder}
            onToggleLeaf={onToggleLeaf}
            selectedSet={selectedSet}
            disabled={disabled}
            nodes={nodes}
          />
        ) : (
          <LeafRow
            key={entry.path}
            leaf={entry}
            depth={depth}
            selectedSet={selectedSet}
            onToggleLeaf={onToggleLeaf}
            disabled={disabled}
          />
        ),
      )}
    </ul>
  );
}

function FolderRow({
  folder,
  state,
  depth,
  onToggleFolder,
  onRefreshFolder,
  onToggleLeaf,
  selectedSet,
  disabled,
  nodes,
}: {
  folder: DiscoverTreeNode;
  state: NodeState | undefined;
  depth: number;
  onToggleFolder: (folder: DiscoverTreeNode) => void;
  onRefreshFolder: (path: string) => void;
  onToggleLeaf: (leaf: DiscoverTreeNode) => void;
  selectedSet: Set<string>;
  disabled: boolean;
  nodes: Record<string, NodeState>;
}) {
  const expanded = state?.expanded ?? false;
  const loading = state?.loading ?? false;
  const error = state?.error ?? null;
  const children = state?.children ?? null;
  const [refreshing, setRefreshing] = useState(false);

  const handleRefresh = async (e: React.MouseEvent) => {
    e.stopPropagation();
    setRefreshing(true);
    try {
      await Promise.resolve(onRefreshFolder(folder.path));
    } finally {
      setRefreshing(false);
    }
  };

  return (
    <li>
      <div
        role="button"
        tabIndex={0}
        onClick={() => onToggleFolder(folder)}
        onKeyDown={(e) => {
          if (e.key === "Enter" || e.key === " ") {
            e.preventDefault();
            onToggleFolder(folder);
          }
        }}
        style={{ paddingLeft: 8 + depth * 16 }}
        className="flex cursor-pointer items-center gap-2 py-1.5 pr-2 text-sm hover:bg-surface-subtle"
      >
        {loading ? (
          <Loader2 size={14} className="shrink-0 animate-spin text-ink-dim" />
        ) : expanded ? (
          <ChevronDown size={14} className="shrink-0 text-ink-dim" />
        ) : (
          <ChevronRight size={14} className="shrink-0 text-ink-dim" />
        )}
        <span className="flex-1 truncate font-mono text-xs text-ink">
          {folder.path || "/"}
        </span>
        {children && (
          <span className="shrink-0 text-[11px] text-ink-dim">
            {children.length} items
          </span>
        )}
        <button
          type="button"
          title="Refresh this folder"
          onClick={handleRefresh}
          disabled={disabled || refreshing || loading}
          className="shrink-0 rounded p-0.5 text-ink-dim hover:bg-surface-raised disabled:cursor-not-allowed disabled:opacity-40"
          aria-label={`Refresh ${folder.path}`}
        >
          {refreshing ? (
            <Loader2 size={12} className="animate-spin" />
          ) : (
            <RefreshCw size={12} />
          )}
        </button>
      </div>
      {expanded && (
        <>
          {error && (
            <div
              style={{ paddingLeft: 24 + depth * 16 }}
              className="py-1 text-[11px] text-critical"
            >
              {error}
            </div>
          )}
          {children && (
            <TreeList
              level={children.filter(distinctByPath())}
              nodes={nodes}
              depth={depth + 1}
              onToggleFolder={onToggleFolder}
              onRefreshFolder={onRefreshFolder}
              onToggleLeaf={onToggleLeaf}
              selectedSet={selectedSet}
              disabled={disabled}
            />
          )}
        </>
      )}
    </li>
  );
}

function LeafRow({
  leaf,
  depth,
  selectedSet,
  onToggleLeaf,
  disabled,
}: {
  leaf: DiscoverTreeNode;
  depth: number;
  selectedSet: Set<string>;
  onToggleLeaf: (leaf: DiscoverTreeNode) => void;
  disabled: boolean;
}) {
  const checked = leaf.external_id ? selectedSet.has(leaf.external_id) : false;
  return (
    <li
      style={{ paddingLeft: 24 + depth * 16 }}
      className="flex items-center gap-2 py-1 pr-2 text-sm hover:bg-surface-subtle"
    >
      <input
        type="checkbox"
        checked={checked}
        onChange={() => onToggleLeaf(leaf)}
        disabled={disabled || !leaf.external_id}
        className="h-3.5 w-3.5 shrink-0 accent-accent"
        aria-label={`Select ${leaf.name}`}
      />
      <span className="flex-1 truncate text-ink">{leaf.name}</span>
      {leaf.owner && (
        <span className="shrink-0 text-[11px] text-ink-dim">{leaf.owner}</span>
      )}
    </li>
  );
}

function SearchResults({
  matched,
  cacheHasAnyRow,
  walking,
  walkError,
  onWalk,
  selectedSet,
  onToggleLeaf,
  disabled,
}: {
  matched: DiscoverTreeNode[];
  cacheHasAnyRow: boolean;
  walking: boolean;
  walkError: string | null;
  onWalk: () => void;
  selectedSet: Set<string>;
  onToggleLeaf: (leaf: DiscoverTreeNode) => void;
  disabled: boolean;
}) {
  if (!cacheHasAnyRow) {
    return (
      <div className="space-y-2 px-3 py-4 text-xs text-ink-muted">
        <p>
          The cache is empty. Run a full workspace walk once to enable search
          across every folder. Subsequent searches are instant.
        </p>
        <button
          type="button"
          onClick={onWalk}
          disabled={walking}
          className="inline-flex items-center gap-1.5 rounded border border-border px-2.5 py-1 text-xs font-medium hover:bg-surface-subtle disabled:cursor-not-allowed disabled:opacity-50"
        >
          {walking ? (
            <Loader2 size={12} className="animate-spin" />
          ) : (
            <RefreshCw size={12} />
          )}
          {walking ? "Walking workspace…" : "Walk workspace + search"}
        </button>
        {walkError && (
          <p className="text-critical">{walkError}</p>
        )}
      </div>
    );
  }
  if (matched.length === 0) {
    return (
      <div className="px-3 py-6 text-center text-xs text-ink-muted">
        No matches in the loaded folders. Tip: expand more folders or hit{" "}
        <button
          type="button"
          onClick={onWalk}
          className="underline hover:text-accent"
          disabled={walking}
        >
          walk workspace
        </button>{" "}
        to search the entire tree.
      </div>
    );
  }
  return (
    <ul className="divide-y divide-border/60">
      {matched.map((leaf) => (
        <li
          key={leaf.path}
          className="flex items-center gap-2 px-3 py-1 text-sm hover:bg-surface-subtle"
        >
          <input
            type="checkbox"
            checked={
              leaf.external_id ? selectedSet.has(leaf.external_id) : false
            }
            onChange={() => onToggleLeaf(leaf)}
            disabled={disabled || !leaf.external_id}
            className="h-3.5 w-3.5 shrink-0 accent-accent"
          />
          <span className="flex-1 truncate text-ink">{leaf.name}</span>
          <span className="shrink-0 truncate font-mono text-[11px] text-ink-dim">
            {leaf.path}
          </span>
        </li>
      ))}
    </ul>
  );
}
```

- [ ] **Step 2: Type-check**

Run: `cd frontend && npx tsc --noEmit`
Expected: no errors

- [ ] **Step 3: Build**

Run: `cd frontend && npm run build`
Expected: `✓ built in …`

- [ ] **Step 4: Commit**

```bash
git add frontend/src/components/assets/AssetBrowsePicker.tsx amx/web/static/
git commit -m "feat(assets/studio): AssetBrowsePicker lazy tree + per-folder refresh"
```

---

## Task 13: Full regression + deploy

**Files:** none new

- [ ] **Step 1: Run the full impacted test suite**

Run: `python -m pytest tests/storage/test_workspace_tree_schema.py tests/db/test_adapter_workspace_children.py tests/web/test_assets_discover_tree.py tests/web/test_assets_router.py tests/web/test_assets_router_pagination.py tests/cli/test_db_assets_commands.py tests/services/ tests/test_local_schema_comments.py -q`
Expected: all PASS (sentence_transformers skips are OK)

- [ ] **Step 2: Final lint + format pass**

Run: `ruff check . && ruff format --check .`
Expected: All checks passed!

- [ ] **Step 3: TypeScript check**

Run: `cd frontend && npx tsc --noEmit`
Expected: no output

- [ ] **Step 4: Deploy locally**

Run the user's local deploy script (path lives outside this repo).
Expected: Studio process restarted with the fresh dist + healthz OK.

- [ ] **Step 5: Manual verification in Studio**

* Open `/assets` → Ingest assets → check "Pick specific assets" → confirm root level loads sub-second.
* Click chevron on a folder → spinner briefly → expand shows children.
* Click the per-folder refresh icon → spinner → children re-fetched.
* Click the root refresh icon → top-level refresh.
* Type in the search input → expects:
  * Cached folders’ leaves filter immediately.
  * If cache is fresh and a search yields nothing in loaded folders, the "walk workspace" link is offered.
  * Click "walk workspace + search" → walks, results appear flat.

- [ ] **Step 6: No PR yet**

Per the active workflow constraint, do NOT open a PR or merge.
The user runs the verification on Studio first; PR comes after explicit "open PR" instruction.
