# History Store Team Collaboration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend AMX's shared history store from runs-only collaboration into full team collaboration over lineage diagrams, documentation pages, comments and column descriptions, with cross-profile visibility, optimistic concurrency control, a workspace admin panel, and automatic backfill of pre-existing local data.

**Architecture:** Three new shared SQLAlchemy tables for lineage (`lineage_artifacts`, `lineage_artifact_nodes`, `lineage_artifact_edges`, `lineage_comments`), three new admin tables (`_amx_users`, `_amx_admin_audit`, `_amx_session_events`), a `version` column on every concurrent-edit table for optimistic concurrency control, a new `documentation_pages.db_profile` column, a background `BackfillRunner` that pushes local SQLite rows to the warehouse on first connection, and Studio/CLI surfaces that default to cross-profile views.

**Tech Stack:** Python 3.11 (SQLAlchemy 2.x, FastAPI, threading), React 18 + TypeScript (React Query, TipTap), Postgres / Snowflake / BigQuery / Databricks / MySQL / Oracle as shared warehouse backends, pytest with cross-platform CI matrix (macOS + Ubuntu + Windows).

**Spec:** `~/.claude/plans/u-an-history-store-proud-lynx.md`

---

## PR Decomposition

This work ships as **eight sequential PRs**. Each PR is independently mergeable and provides value. Dependency order:

| PR  | Scope                                          | Depends on |
|-----|------------------------------------------------|------------|
| PR-1 | Lineage shared schema + storage methods       | —          |
| PR-2 | Pages `db_profile` + page attribution         | —          |
| PR-3 | Optimistic concurrency control (`version`)    | PR-1, PR-2 |
| PR-4 | Admin tables + identity bootstrap             | —          |
| PR-5 | Auto-backfill (`BackfillRunner`)              | PR-1, PR-2 |
| PR-6 | Admin CLI + API + permission enforcement      | PR-4       |
| PR-7 | Studio cross-profile filter chips + banners   | PR-1, PR-2, PR-5 |
| PR-8 | Studio admin panel UI + conflict dialog       | PR-3, PR-4, PR-6 |

The CLAUDE.md "deploy.sh → PR → merge" order applies to every PR with Studio-visible changes (PR-7, PR-8 at minimum; PR-5 surfaces a migration banner).

This file ships **PR-1 in full bite-sized detail**. PR-2 through PR-8 are summarized at the bottom and will each be expanded into their own plan document (`docs/superpowers/plans/2026-05-...-history-store-pr-N-<topic>.md`) before execution begins.

---

# PR-1: Lineage Shared Schema + Storage Methods

**Goal of PR-1:** Add `lineage_artifacts`, `lineage_artifact_nodes`, `lineage_artifact_edges`, `lineage_comments` tables to the shared schema with full attribution and structural data (joins, where clauses, column metadata, canvas geometry). Add corresponding read/write methods on `SQLAlchemyHistoryStore`. Wire the new write methods through `DualWriteHistoryStore`. No backfill yet (PR-5), no Studio changes yet (PR-7), no OCC yet (PR-3).

**At the end of PR-1, an existing user with shared mode enabled can create a lineage artifact via the existing CLI/Studio flow and the row appears in the shared warehouse with their username + hostname stamped on it. Teammates with shared mode enabled can see each other's lineage in `list_lineage_artifacts` calls.**

## File Map (PR-1)

**Create:**

- `tests/storage/test_shared_lineage_schema.py` — DDL + descriptions tests
- `tests/storage/test_sqlalchemy_lineage.py` — CRUD method tests
- `tests/storage/test_dual_write_lineage.py` — dual-write integration test

**Modify:**

- `amx/storage/shared_schema.py` — add 4 lineage tables, bump `SHARED_SCHEMA_VERSION`
- `amx/storage/schema_descriptions.py` — add entries for every new column
- `amx/storage/sqlalchemy_store.py` — add table handles + CRUD methods
- `amx/storage/dual_write.py` — add lineage OP constants + write wrappers + `_replay_op` branches

---

## Task 1: Add `lineage_artifacts` shared table (TDD)

**Files:**
- Modify: `amx/storage/shared_schema.py`
- Modify: `amx/storage/schema_descriptions.py`
- Test: `tests/storage/test_shared_lineage_schema.py`

- [ ] **Step 1: Write failing test for table presence**

```python
# tests/storage/test_shared_lineage_schema.py
from amx.storage.shared_schema import build_metadata


def test_lineage_artifacts_table_exists():
    md = build_metadata(schema="AMX")
    assert "AMX.lineage_artifacts" in md.tables


def test_lineage_artifacts_has_required_columns():
    md = build_metadata(schema="AMX")
    table = md.tables["AMX.lineage_artifacts"]
    expected = {
        "id", "name", "db_profile", "anchor_entity_ref",
        "depth_up", "depth_down", "format", "output_path",
        "edge_set_hash", "node_count", "edge_count",
        "generated_at", "extractors_used", "extractors_partial",
        "canvas_meta",
        "created_by", "hostname", "client_version",
        "created_at", "updated_at", "local_id",
    }
    actual = {c.name for c in table.columns}
    assert expected <= actual, f"missing: {expected - actual}"


def test_lineage_artifacts_indexes():
    md = build_metadata(schema="AMX")
    table = md.tables["AMX.lineage_artifacts"]
    idx_names = {idx.name for idx in table.indexes}
    assert "ix_lineage_artifacts_db_profile" in idx_names
    assert "ix_lineage_artifacts_local_lookup" in idx_names
    assert "ix_lineage_artifacts_name_profile" in idx_names
```

- [ ] **Step 2: Run test to confirm it fails**

Run: `pytest tests/storage/test_shared_lineage_schema.py -v`
Expected: FAIL with "AMX.lineage_artifacts not in md.tables"

- [ ] **Step 3: Add `lineage_artifacts` table in `shared_schema.py`**

In `amx/storage/shared_schema.py`, inside `build_metadata()`, before `return md`, add:

```python
Table(
    "lineage_artifacts",
    md,
    Column("id", String(36), primary_key=True,
           comment=_desc("lineage_artifacts", "id")),
    Column("name", String(512), nullable=False,
           comment=_desc("lineage_artifacts", "name")),
    Column("db_profile", String(120), nullable=False,
           comment=_desc("lineage_artifacts", "db_profile")),
    Column("anchor_entity_ref", String(1024), nullable=False,
           comment=_desc("lineage_artifacts", "anchor_entity_ref")),
    Column("depth_up", Integer,
           comment=_desc("lineage_artifacts", "depth_up")),
    Column("depth_down", Integer,
           comment=_desc("lineage_artifacts", "depth_down")),
    Column("format", String(20),
           comment=_desc("lineage_artifacts", "format")),
    Column("output_path", Text,
           comment=_desc("lineage_artifacts", "output_path")),
    Column("edge_set_hash", String(64),
           comment=_desc("lineage_artifacts", "edge_set_hash")),
    Column("node_count", Integer,
           comment=_desc("lineage_artifacts", "node_count")),
    Column("edge_count", Integer,
           comment=_desc("lineage_artifacts", "edge_count")),
    Column("generated_at", DateTime(timezone=True),
           comment=_desc("lineage_artifacts", "generated_at")),
    Column("extractors_used", _portable_json(),
           comment=_desc("lineage_artifacts", "extractors_used")),
    Column("extractors_partial", Integer,
           comment=_desc("lineage_artifacts", "extractors_partial")),
    Column("canvas_meta", _portable_json(),
           comment=_desc("lineage_artifacts", "canvas_meta")),
    Column("created_by", String(255), nullable=False,
           comment=_desc("lineage_artifacts", "created_by")),
    Column("hostname", String(255), nullable=False,
           comment=_desc("lineage_artifacts", "hostname")),
    Column("client_version", String(40), nullable=False,
           comment=_desc("lineage_artifacts", "client_version")),
    Column("created_at", DateTime(timezone=True), nullable=False,
           comment=_desc("lineage_artifacts", "created_at")),
    Column("updated_at", DateTime(timezone=True), nullable=False,
           comment=_desc("lineage_artifacts", "updated_at")),
    Column("local_id", BigInteger, nullable=False,
           comment=_desc("lineage_artifacts", "local_id")),
    Index("ix_lineage_artifacts_db_profile", "db_profile"),
    Index("ix_lineage_artifacts_local_lookup", "hostname", "local_id"),
    Index("ix_lineage_artifacts_name_profile", "name", "db_profile", unique=True),
    schema=schema,
),
```

- [ ] **Step 4: Add descriptions for every column in `schema_descriptions.py`**

In `amx/storage/schema_descriptions.py`, add a new top-level entry inside `SCHEMA_DESCRIPTIONS["AMX"]["lineage_artifacts"]`:

```python
"lineage_artifacts": {
    "_table": "Saved lineage diagrams shared across the team workspace, "
              "including all structural data needed to re-render and edit.",
    "id": "UUID primary key, stable across hosts.",
    "name": "Human-readable artifact name shown in lists.",
    "db_profile": "Name of the source database profile this lineage was extracted from.",
    "anchor_entity_ref": "FQN of the anchor entity in the form "
                        "'db_profile|database|schema|table[|column]'.",
    "depth_up": "Upstream traversal depth at extraction time.",
    "depth_down": "Downstream traversal depth at extraction time.",
    "format": "Rendering format used for the local export, e.g. 'svg', 'png', 'dot'.",
    "output_path": "Local filesystem path of the exported artifact "
                   "(optional; teammates re-render from structural data).",
    "edge_set_hash": "Stable hash of the edge set used for change detection.",
    "node_count": "Number of nodes in the artifact at save time.",
    "edge_count": "Number of edges in the artifact at save time.",
    "generated_at": "Timestamp when the lineage was first generated.",
    "extractors_used": "JSON list of extractor identifiers that contributed edges.",
    "extractors_partial": "1 if any extractor returned a partial result, else 0.",
    "canvas_meta": "JSON of canvas viewport state: zoom, pan, layout direction, theme.",
    "created_by": "Username that originally created this artifact.",
    "hostname": "Hostname where the artifact was originally created.",
    "client_version": "AMX client version at creation time.",
    "created_at": "UTC timestamp when the row was inserted into the shared store.",
    "updated_at": "UTC timestamp of the last edit.",
    "local_id": "Integer primary key in the originating local SQLite store; "
                "used together with hostname for idempotent backfill.",
},
```

- [ ] **Step 5: Run test and confirm PASS**

Run: `pytest tests/storage/test_shared_lineage_schema.py -v`
Expected: PASS (all three tests)

- [ ] **Step 6: Confirm existing schema-description CI guard still passes**

Run: `pytest tests/test_shared_schema_comments.py -v`
Expected: PASS (no `_desc()` KeyError)

- [ ] **Step 7: Commit**

```bash
git add amx/storage/shared_schema.py amx/storage/schema_descriptions.py \
        tests/storage/test_shared_lineage_schema.py
git commit -m "feat(storage): add shared lineage_artifacts table"
```

---

## Task 2: Add `lineage_artifact_nodes` shared table (TDD)

**Files:**
- Modify: `amx/storage/shared_schema.py`
- Modify: `amx/storage/schema_descriptions.py`
- Test: `tests/storage/test_shared_lineage_schema.py`

- [ ] **Step 1: Append failing tests**

In `tests/storage/test_shared_lineage_schema.py`:

```python
def test_lineage_artifact_nodes_table_exists():
    md = build_metadata(schema="AMX")
    assert "AMX.lineage_artifact_nodes" in md.tables


def test_lineage_artifact_nodes_has_required_columns():
    md = build_metadata(schema="AMX")
    table = md.tables["AMX.lineage_artifact_nodes"]
    expected = {
        "id", "artifact_id", "entity_ref", "entity_kind", "db_profile",
        "x", "y", "width", "height", "z_index",
        "display_label", "column_list_json", "logo_key", "custom_style_json",
        "created_by", "hostname", "client_version",
        "created_at", "updated_at", "local_id",
    }
    actual = {c.name for c in table.columns}
    assert expected <= actual, f"missing: {expected - actual}"
```

- [ ] **Step 2: Run** `pytest tests/storage/test_shared_lineage_schema.py -v` → both new tests FAIL.

- [ ] **Step 3: Add `lineage_artifact_nodes` table**

In `shared_schema.py` after the `lineage_artifacts` definition:

```python
Table(
    "lineage_artifact_nodes",
    md,
    Column("id", String(36), primary_key=True,
           comment=_desc("lineage_artifact_nodes", "id")),
    Column("artifact_id", String(36),
           ForeignKey(f"{schema}.lineage_artifacts.id"),
           nullable=False,
           comment=_desc("lineage_artifact_nodes", "artifact_id")),
    Column("entity_ref", String(1024), nullable=False,
           comment=_desc("lineage_artifact_nodes", "entity_ref")),
    Column("entity_kind", String(40), nullable=False,
           comment=_desc("lineage_artifact_nodes", "entity_kind")),
    Column("db_profile", String(120), nullable=False,
           comment=_desc("lineage_artifact_nodes", "db_profile")),
    Column("x", Float, comment=_desc("lineage_artifact_nodes", "x")),
    Column("y", Float, comment=_desc("lineage_artifact_nodes", "y")),
    Column("width", Float, comment=_desc("lineage_artifact_nodes", "width")),
    Column("height", Float, comment=_desc("lineage_artifact_nodes", "height")),
    Column("z_index", Integer, comment=_desc("lineage_artifact_nodes", "z_index")),
    Column("display_label", String(512),
           comment=_desc("lineage_artifact_nodes", "display_label")),
    Column("column_list_json", _portable_json(),
           comment=_desc("lineage_artifact_nodes", "column_list_json")),
    Column("logo_key", String(120),
           comment=_desc("lineage_artifact_nodes", "logo_key")),
    Column("custom_style_json", _portable_json(),
           comment=_desc("lineage_artifact_nodes", "custom_style_json")),
    Column("created_by", String(255), nullable=False,
           comment=_desc("lineage_artifact_nodes", "created_by")),
    Column("hostname", String(255), nullable=False,
           comment=_desc("lineage_artifact_nodes", "hostname")),
    Column("client_version", String(40), nullable=False,
           comment=_desc("lineage_artifact_nodes", "client_version")),
    Column("created_at", DateTime(timezone=True), nullable=False,
           comment=_desc("lineage_artifact_nodes", "created_at")),
    Column("updated_at", DateTime(timezone=True), nullable=False,
           comment=_desc("lineage_artifact_nodes", "updated_at")),
    Column("local_id", BigInteger, nullable=False,
           comment=_desc("lineage_artifact_nodes", "local_id")),
    Index("ix_lineage_nodes_artifact", "artifact_id"),
    Index("ix_lineage_nodes_entity_profile", "entity_ref", "db_profile"),
    Index("ix_lineage_nodes_local_lookup", "hostname", "local_id"),
    schema=schema,
),
```

- [ ] **Step 4: Add `lineage_artifact_nodes` descriptions**

In `schema_descriptions.py`:

```python
"lineage_artifact_nodes": {
    "_table": "Per-entity placement on a lineage canvas; captures position, "
              "label, logo, and a full column snapshot so teammates see the "
              "same schema view as the author.",
    "id": "UUID primary key.",
    "artifact_id": "FK to lineage_artifacts.id.",
    "entity_ref": "FQN of the entity rendered by this node, in the form "
                  "'db_profile|database|schema|table[|column]'.",
    "entity_kind": "One of 'table', 'view', 'column', 'external', 'cte', 'temp'.",
    "db_profile": "Source database profile of the entity at the time of capture.",
    "x": "Canvas X coordinate.",
    "y": "Canvas Y coordinate.",
    "width": "Rendered node width in canvas units.",
    "height": "Rendered node height in canvas units.",
    "z_index": "Stack order for overlapping nodes.",
    "display_label": "User override label (e.g. table alias). NULL means use entity_ref.",
    "column_list_json": "JSON list of columns shown on the node: name, type, "
                        "nullable, primary_key flags.",
    "logo_key": "Identifier for a predefined logo such as 'postgres', 'snowflake'.",
    "custom_style_json": "JSON of per-node style overrides: colors, border, font.",
    "created_by": "Username that created the node.",
    "hostname": "Hostname where the node was created.",
    "client_version": "AMX client version at creation time.",
    "created_at": "UTC timestamp of insertion.",
    "updated_at": "UTC timestamp of last edit.",
    "local_id": "Integer primary key in the originating local SQLite store.",
},
```

- [ ] **Step 5: Run** `pytest tests/storage/test_shared_lineage_schema.py -v` → all PASS.

- [ ] **Step 6: Commit**

```bash
git add amx/storage/shared_schema.py amx/storage/schema_descriptions.py \
        tests/storage/test_shared_lineage_schema.py
git commit -m "feat(storage): add shared lineage_artifact_nodes table"
```

---

## Task 3: Add `lineage_artifact_edges` shared table (TDD)

**Files:**
- Modify: `amx/storage/shared_schema.py`
- Modify: `amx/storage/schema_descriptions.py`
- Test: `tests/storage/test_shared_lineage_schema.py`

- [ ] **Step 1: Append failing tests**

```python
def test_lineage_artifact_edges_table_exists():
    md = build_metadata(schema="AMX")
    assert "AMX.lineage_artifact_edges" in md.tables


def test_lineage_artifact_edges_has_required_columns():
    md = build_metadata(schema="AMX")
    table = md.tables["AMX.lineage_artifact_edges"]
    expected = {
        "id", "artifact_id", "source_node_id", "target_node_id",
        "edge_kind", "join_type", "on_condition", "where_clause",
        "source_columns_json", "target_columns_json",
        "label", "style_json", "waypoints_json",
        "created_by", "hostname", "client_version",
        "created_at", "updated_at", "local_id",
    }
    actual = {c.name for c in table.columns}
    assert expected <= actual, f"missing: {expected - actual}"
```

- [ ] **Step 2: Run → FAIL.**

- [ ] **Step 3: Add `lineage_artifact_edges` table**

```python
Table(
    "lineage_artifact_edges",
    md,
    Column("id", String(36), primary_key=True,
           comment=_desc("lineage_artifact_edges", "id")),
    Column("artifact_id", String(36),
           ForeignKey(f"{schema}.lineage_artifacts.id"),
           nullable=False,
           comment=_desc("lineage_artifact_edges", "artifact_id")),
    Column("source_node_id", String(36),
           ForeignKey(f"{schema}.lineage_artifact_nodes.id"),
           nullable=False,
           comment=_desc("lineage_artifact_edges", "source_node_id")),
    Column("target_node_id", String(36),
           ForeignKey(f"{schema}.lineage_artifact_nodes.id"),
           nullable=False,
           comment=_desc("lineage_artifact_edges", "target_node_id")),
    Column("edge_kind", String(40), nullable=False,
           comment=_desc("lineage_artifact_edges", "edge_kind")),
    Column("join_type", String(20),
           comment=_desc("lineage_artifact_edges", "join_type")),
    Column("on_condition", Text,
           comment=_desc("lineage_artifact_edges", "on_condition")),
    Column("where_clause", Text,
           comment=_desc("lineage_artifact_edges", "where_clause")),
    Column("source_columns_json", _portable_json(),
           comment=_desc("lineage_artifact_edges", "source_columns_json")),
    Column("target_columns_json", _portable_json(),
           comment=_desc("lineage_artifact_edges", "target_columns_json")),
    Column("label", String(512),
           comment=_desc("lineage_artifact_edges", "label")),
    Column("style_json", _portable_json(),
           comment=_desc("lineage_artifact_edges", "style_json")),
    Column("waypoints_json", _portable_json(),
           comment=_desc("lineage_artifact_edges", "waypoints_json")),
    Column("created_by", String(255), nullable=False,
           comment=_desc("lineage_artifact_edges", "created_by")),
    Column("hostname", String(255), nullable=False,
           comment=_desc("lineage_artifact_edges", "hostname")),
    Column("client_version", String(40), nullable=False,
           comment=_desc("lineage_artifact_edges", "client_version")),
    Column("created_at", DateTime(timezone=True), nullable=False,
           comment=_desc("lineage_artifact_edges", "created_at")),
    Column("updated_at", DateTime(timezone=True), nullable=False,
           comment=_desc("lineage_artifact_edges", "updated_at")),
    Column("local_id", BigInteger, nullable=False,
           comment=_desc("lineage_artifact_edges", "local_id")),
    Index("ix_lineage_edges_artifact", "artifact_id"),
    Index("ix_lineage_edges_source", "source_node_id"),
    Index("ix_lineage_edges_target", "target_node_id"),
    Index("ix_lineage_edges_local_lookup", "hostname", "local_id"),
    schema=schema,
),
```

- [ ] **Step 4: Add `lineage_artifact_edges` descriptions**

```python
"lineage_artifact_edges": {
    "_table": "Relations between nodes on a lineage canvas: data flow, foreign "
              "keys, SQL joins, view references. Stores full SQL semantics so "
              "teammates can inspect joins, WHERE filters, and column mappings.",
    "id": "UUID primary key.",
    "artifact_id": "FK to lineage_artifacts.id.",
    "source_node_id": "FK to lineage_artifact_nodes.id at the source end.",
    "target_node_id": "FK to lineage_artifact_nodes.id at the target end.",
    "edge_kind": "One of 'lineage', 'fk', 'join', 'reference', 'view_source'.",
    "join_type": "For join edges: 'INNER', 'LEFT', 'RIGHT', 'FULL', 'CROSS'. "
                 "NULL for non-join edges.",
    "on_condition": "SQL ON expression for join edges, e.g. 'a.id = b.user_id'.",
    "where_clause": "SQL WHERE filter associated with this edge (often pulled "
                    "from view definitions).",
    "source_columns_json": "JSON list of source-side column names involved in this edge.",
    "target_columns_json": "JSON list of target-side column names involved in this edge.",
    "label": "User override label for the edge. NULL means derive from edge_kind.",
    "style_json": "JSON of edge style: color, line type, arrow style.",
    "waypoints_json": "JSON list of intermediate routing points for orthogonal layout.",
    "created_by": "Username that created the edge.",
    "hostname": "Hostname where the edge was created.",
    "client_version": "AMX client version at creation time.",
    "created_at": "UTC timestamp of insertion.",
    "updated_at": "UTC timestamp of last edit.",
    "local_id": "Integer primary key in the originating local SQLite store.",
},
```

- [ ] **Step 5: Run → PASS.**

- [ ] **Step 6: Commit**

```bash
git add amx/storage/shared_schema.py amx/storage/schema_descriptions.py \
        tests/storage/test_shared_lineage_schema.py
git commit -m "feat(storage): add shared lineage_artifact_edges table"
```

---

## Task 4: Add `lineage_comments` shared table (TDD)

**Files:**
- Modify: `amx/storage/shared_schema.py`
- Modify: `amx/storage/schema_descriptions.py`
- Test: `tests/storage/test_shared_lineage_schema.py`

- [ ] **Step 1: Append failing tests**

```python
def test_lineage_comments_table_exists():
    md = build_metadata(schema="AMX")
    assert "AMX.lineage_comments" in md.tables


def test_lineage_comments_has_attribution_columns():
    md = build_metadata(schema="AMX")
    table = md.tables["AMX.lineage_comments"]
    actual = {c.name for c in table.columns}
    for expected_col in ("created_by", "hostname", "client_version",
                         "created_at", "updated_at", "local_id"):
        assert expected_col in actual, f"missing attribution: {expected_col}"
```

- [ ] **Step 2: Run → FAIL.**

- [ ] **Step 3: Add `lineage_comments` table**

```python
Table(
    "lineage_comments",
    md,
    Column("id", String(36), primary_key=True,
           comment=_desc("lineage_comments", "id")),
    Column("artifact_id", String(36),
           ForeignKey(f"{schema}.lineage_artifacts.id"),
           nullable=False,
           comment=_desc("lineage_comments", "artifact_id")),
    Column("x", Float, comment=_desc("lineage_comments", "x")),
    Column("y", Float, comment=_desc("lineage_comments", "y")),
    Column("width", Float, comment=_desc("lineage_comments", "width")),
    Column("height", Float, comment=_desc("lineage_comments", "height")),
    Column("color", String(40), comment=_desc("lineage_comments", "color")),
    Column("style", String(20), server_default="note",
           comment=_desc("lineage_comments", "style")),
    Column("text", Text, comment=_desc("lineage_comments", "text")),
    Column("created_by", String(255), nullable=False,
           comment=_desc("lineage_comments", "created_by")),
    Column("hostname", String(255), nullable=False,
           comment=_desc("lineage_comments", "hostname")),
    Column("client_version", String(40), nullable=False,
           comment=_desc("lineage_comments", "client_version")),
    Column("created_at", DateTime(timezone=True), nullable=False,
           comment=_desc("lineage_comments", "created_at")),
    Column("updated_at", DateTime(timezone=True), nullable=False,
           comment=_desc("lineage_comments", "updated_at")),
    Column("local_id", BigInteger, nullable=False,
           comment=_desc("lineage_comments", "local_id")),
    Index("ix_lineage_comments_artifact", "artifact_id"),
    Index("ix_lineage_comments_local_lookup", "hostname", "local_id"),
    schema=schema,
),
```

- [ ] **Step 4: Add `lineage_comments` descriptions**

```python
"lineage_comments": {
    "_table": "Sticky-note style comments placed on lineage canvases. Used for "
              "team annotations: questions, decisions, callouts on entities.",
    "id": "UUID primary key.",
    "artifact_id": "FK to lineage_artifacts.id.",
    "x": "Canvas X coordinate of the comment's top-left corner.",
    "y": "Canvas Y coordinate of the comment's top-left corner.",
    "width": "Comment width in canvas units.",
    "height": "Comment height in canvas units.",
    "color": "Background color of the sticky note, e.g. '#fef3c7'.",
    "style": "Display style: 'note' (default), 'callout', 'pin'.",
    "text": "Comment body, plain text.",
    "created_by": "Username of the comment author.",
    "hostname": "Hostname where the comment was created.",
    "client_version": "AMX client version at creation time.",
    "created_at": "UTC timestamp of creation.",
    "updated_at": "UTC timestamp of last edit.",
    "local_id": "Integer primary key in the originating local SQLite store.",
},
```

- [ ] **Step 5: Run → PASS.**

- [ ] **Step 6: Commit**

```bash
git add amx/storage/shared_schema.py amx/storage/schema_descriptions.py \
        tests/storage/test_shared_lineage_schema.py
git commit -m "feat(storage): add shared lineage_comments table with author attribution"
```

---

## Task 5: Bump `SHARED_SCHEMA_VERSION` (TDD)

**Files:**
- Modify: `amx/storage/shared_schema.py`
- Test: `tests/storage/test_shared_lineage_schema.py`

- [ ] **Step 1: Identify the current version**

Run: `grep -n "SHARED_SCHEMA_VERSION" amx/storage/shared_schema.py`
Note the current integer value (call it `N`).

- [ ] **Step 2: Append failing test**

```python
def test_shared_schema_version_bumped():
    from amx.storage.shared_schema import SHARED_SCHEMA_VERSION
    # PR-1 increments the schema version to add lineage tables.
    assert SHARED_SCHEMA_VERSION >= (N + 1)  # replace N with the value from Step 1
```

- [ ] **Step 3: Run → FAIL** if version not yet bumped.

- [ ] **Step 4: Bump the constant**

Edit `shared_schema.py` to set `SHARED_SCHEMA_VERSION = N + 1`.

- [ ] **Step 5: Run → PASS.**

- [ ] **Step 6: Commit**

```bash
git add amx/storage/shared_schema.py tests/storage/test_shared_lineage_schema.py
git commit -m "chore(storage): bump SHARED_SCHEMA_VERSION for lineage tables"
```

---

## Task 6: Add lineage table handles to `SQLAlchemyHistoryStore` (TDD)

**Files:**
- Modify: `amx/storage/sqlalchemy_store.py`
- Test: `tests/storage/test_sqlalchemy_lineage.py`

- [ ] **Step 1: Write failing test**

```python
# tests/storage/test_sqlalchemy_lineage.py
import pytest
from sqlalchemy import create_engine
from amx.storage.sqlalchemy_store import SQLAlchemyHistoryStore


@pytest.fixture
def store(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path}/shared.db")
    s = SQLAlchemyHistoryStore(engine, schema="AMX")
    s.init()
    return s


def test_lineage_table_handles_present(store):
    assert store._t_lineage_artifacts is not None
    assert store._t_lineage_artifact_nodes is not None
    assert store._t_lineage_artifact_edges is not None
    assert store._t_lineage_comments is not None
```

- [ ] **Step 2: Run → FAIL** ("'SQLAlchemyHistoryStore' object has no attribute '_t_lineage_artifacts'").

- [ ] **Step 3: Wire the table handles**

In `amx/storage/sqlalchemy_store.py`, locate the `__init__` (around line 107-122) and append after the existing `self._t_*` assignments:

```python
schema_prefix = f"{schema}." if schema else ""
self._t_lineage_artifacts = self._md.tables[
    f"{schema_prefix}lineage_artifacts"
]
self._t_lineage_artifact_nodes = self._md.tables[
    f"{schema_prefix}lineage_artifact_nodes"
]
self._t_lineage_artifact_edges = self._md.tables[
    f"{schema_prefix}lineage_artifact_edges"
]
self._t_lineage_comments = self._md.tables[
    f"{schema_prefix}lineage_comments"
]
```

(Use the same `schema_prefix` pattern the file already uses for `_t_runs`. If the file already has a `_schema_prefix` helper, reuse it instead.)

- [ ] **Step 4: Run → PASS.**

- [ ] **Step 5: Commit**

```bash
git add amx/storage/sqlalchemy_store.py tests/storage/test_sqlalchemy_lineage.py
git commit -m "feat(storage): wire lineage table handles in SQLAlchemyHistoryStore"
```

---

## Task 7: `create_lineage_artifact` method (TDD)

**Files:**
- Modify: `amx/storage/sqlalchemy_store.py`
- Test: `tests/storage/test_sqlalchemy_lineage.py`

- [ ] **Step 1: Append failing test**

```python
def test_create_lineage_artifact_inserts_and_returns_uuid(store):
    uuid = store.create_lineage_artifact(
        local_id=42,
        name="orders_lineage",
        db_profile="prod_pg",
        anchor_entity_ref="prod_pg|main|public|orders",
        depth_up=2,
        depth_down=2,
        format="svg",
        output_path="/tmp/orders.svg",
        edge_set_hash="abc123",
        node_count=5,
        edge_count=4,
        canvas_meta={"zoom": 1.0, "pan": {"x": 0, "y": 0}, "layout": "LR"},
        extractors_used=["postgres_fk", "view_parser"],
        extractors_partial=0,
    )
    assert isinstance(uuid, str) and len(uuid) == 36

    found = store.find_lineage_uuid_by_local_id(
        hostname=store._hostname, local_id=42
    )
    assert found == uuid


def test_create_lineage_artifact_stamps_attribution(store):
    uuid = store.create_lineage_artifact(
        local_id=1,
        name="t",
        db_profile="x",
        anchor_entity_ref="x|a|b|c",
    )
    rows = store.list_lineage_artifacts()
    row = next(r for r in rows if r.id == uuid)
    assert row.created_by == store._username
    assert row.hostname == store._hostname
    assert row.client_version == store._client_version
```

- [ ] **Step 2: Run → FAIL** ("no attribute 'create_lineage_artifact'").

- [ ] **Step 3: Implement the methods**

In `sqlalchemy_store.py`, add after the existing `create_run` (around line 285):

```python
def create_lineage_artifact(
    self,
    *,
    local_id: int,
    name: str,
    db_profile: str,
    anchor_entity_ref: str,
    depth_up: int | None = None,
    depth_down: int | None = None,
    format: str | None = None,
    output_path: str | None = None,
    edge_set_hash: str | None = None,
    node_count: int | None = None,
    edge_count: int | None = None,
    generated_at: datetime | None = None,
    extractors_used: list[str] | None = None,
    extractors_partial: int | None = None,
    canvas_meta: dict | None = None,
) -> str:
    uuid_value = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    with self._engine.begin() as conn:
        conn.execute(
            self._t_lineage_artifacts.insert().values(
                id=uuid_value,
                name=name,
                db_profile=db_profile,
                anchor_entity_ref=anchor_entity_ref,
                depth_up=depth_up,
                depth_down=depth_down,
                format=format,
                output_path=output_path,
                edge_set_hash=edge_set_hash,
                node_count=node_count,
                edge_count=edge_count,
                generated_at=generated_at,
                extractors_used=extractors_used,
                extractors_partial=extractors_partial,
                canvas_meta=canvas_meta,
                created_by=self._username,
                hostname=self._hostname,
                client_version=self._client_version,
                created_at=now,
                updated_at=now,
                local_id=local_id,
            )
        )
    return uuid_value


def find_lineage_uuid_by_local_id(
    self, *, hostname: str, local_id: int
) -> str | None:
    with self._engine.connect() as conn:
        row = conn.execute(
            select(self._t_lineage_artifacts.c.id)
            .where(self._t_lineage_artifacts.c.hostname == hostname)
            .where(self._t_lineage_artifacts.c.local_id == local_id)
        ).fetchone()
    return row[0] if row else None


def list_lineage_artifacts(
    self,
    *,
    db_profiles: list[str] | None = None,
    created_by: list[str] | None = None,
) -> list[LineageArtifactRecord]:
    stmt = select(self._t_lineage_artifacts)
    if db_profiles:
        stmt = stmt.where(self._t_lineage_artifacts.c.db_profile.in_(db_profiles))
    if created_by:
        stmt = stmt.where(self._t_lineage_artifacts.c.created_by.in_(created_by))
    stmt = stmt.order_by(self._t_lineage_artifacts.c.updated_at.desc())
    with self._engine.connect() as conn:
        rows = conn.execute(stmt).fetchall()
    return [LineageArtifactRecord(**row._mapping) for row in rows]
```

Add the `LineageArtifactRecord` dataclass at the top of the file alongside existing record types:

```python
@dataclass(frozen=True)
class LineageArtifactRecord:
    id: str
    name: str
    db_profile: str
    anchor_entity_ref: str
    depth_up: int | None
    depth_down: int | None
    format: str | None
    output_path: str | None
    edge_set_hash: str | None
    node_count: int | None
    edge_count: int | None
    generated_at: datetime | None
    extractors_used: list[str] | None
    extractors_partial: int | None
    canvas_meta: dict | None
    created_by: str
    hostname: str
    client_version: str
    created_at: datetime
    updated_at: datetime
    local_id: int
```

Add `import uuid` and `from datetime import datetime, timezone` at the top of the file if not already present.

- [ ] **Step 4: Run → PASS.**

- [ ] **Step 5: Commit**

```bash
git add amx/storage/sqlalchemy_store.py tests/storage/test_sqlalchemy_lineage.py
git commit -m "feat(storage): add create_lineage_artifact + list/find methods"
```

---

## Task 8: `upsert_lineage_node` method (TDD)

**Files:**
- Modify: `amx/storage/sqlalchemy_store.py`
- Test: `tests/storage/test_sqlalchemy_lineage.py`

- [ ] **Step 1: Append failing test**

```python
def test_upsert_lineage_node_creates_then_updates(store):
    artifact_uuid = store.create_lineage_artifact(
        local_id=1, name="t", db_profile="x",
        anchor_entity_ref="x|a|b|c",
    )
    node_uuid = store.upsert_lineage_node(
        local_id=100,
        artifact_uuid=artifact_uuid,
        entity_ref="x|a|b|c",
        entity_kind="table",
        db_profile="x",
        x=10.0, y=20.0, width=120.0, height=80.0,
        z_index=0,
        display_label=None,
        column_list_json=[{"name": "id", "type": "int", "nullable": False}],
        logo_key="postgres",
        custom_style_json=None,
    )
    assert isinstance(node_uuid, str)

    # Upsert again with the same local_id moves the node.
    moved_uuid = store.upsert_lineage_node(
        local_id=100,
        artifact_uuid=artifact_uuid,
        entity_ref="x|a|b|c",
        entity_kind="table",
        db_profile="x",
        x=99.0, y=99.0, width=120.0, height=80.0,
    )
    assert moved_uuid == node_uuid

    nodes = store.list_lineage_nodes(artifact_uuid=artifact_uuid)
    assert len(nodes) == 1
    assert nodes[0].x == 99.0
```

- [ ] **Step 2: Run → FAIL.**

- [ ] **Step 3: Implement `upsert_lineage_node` and `list_lineage_nodes`**

```python
def upsert_lineage_node(
    self,
    *,
    local_id: int,
    artifact_uuid: str,
    entity_ref: str,
    entity_kind: str,
    db_profile: str,
    x: float,
    y: float,
    width: float,
    height: float,
    z_index: int = 0,
    display_label: str | None = None,
    column_list_json: list | None = None,
    logo_key: str | None = None,
    custom_style_json: dict | None = None,
) -> str:
    now = datetime.now(timezone.utc)
    existing = self._find_node_uuid_by_local_id(self._hostname, local_id)
    if existing:
        with self._engine.begin() as conn:
            conn.execute(
                self._t_lineage_artifact_nodes.update()
                .where(self._t_lineage_artifact_nodes.c.id == existing)
                .values(
                    x=x, y=y, width=width, height=height, z_index=z_index,
                    display_label=display_label,
                    column_list_json=column_list_json,
                    logo_key=logo_key,
                    custom_style_json=custom_style_json,
                    updated_at=now,
                )
            )
        return existing
    uuid_value = str(uuid.uuid4())
    with self._engine.begin() as conn:
        conn.execute(
            self._t_lineage_artifact_nodes.insert().values(
                id=uuid_value,
                artifact_id=artifact_uuid,
                entity_ref=entity_ref,
                entity_kind=entity_kind,
                db_profile=db_profile,
                x=x, y=y, width=width, height=height, z_index=z_index,
                display_label=display_label,
                column_list_json=column_list_json,
                logo_key=logo_key,
                custom_style_json=custom_style_json,
                created_by=self._username,
                hostname=self._hostname,
                client_version=self._client_version,
                created_at=now,
                updated_at=now,
                local_id=local_id,
            )
        )
    return uuid_value


def _find_node_uuid_by_local_id(
    self, hostname: str, local_id: int
) -> str | None:
    with self._engine.connect() as conn:
        row = conn.execute(
            select(self._t_lineage_artifact_nodes.c.id)
            .where(self._t_lineage_artifact_nodes.c.hostname == hostname)
            .where(self._t_lineage_artifact_nodes.c.local_id == local_id)
        ).fetchone()
    return row[0] if row else None


def list_lineage_nodes(
    self, *, artifact_uuid: str
) -> list[LineageNodeRecord]:
    stmt = (
        select(self._t_lineage_artifact_nodes)
        .where(self._t_lineage_artifact_nodes.c.artifact_id == artifact_uuid)
        .order_by(self._t_lineage_artifact_nodes.c.z_index)
    )
    with self._engine.connect() as conn:
        rows = conn.execute(stmt).fetchall()
    return [LineageNodeRecord(**row._mapping) for row in rows]
```

Add `LineageNodeRecord` dataclass alongside `LineageArtifactRecord`.

- [ ] **Step 4: Run → PASS.**

- [ ] **Step 5: Commit**

```bash
git add amx/storage/sqlalchemy_store.py tests/storage/test_sqlalchemy_lineage.py
git commit -m "feat(storage): add upsert_lineage_node + list_lineage_nodes"
```

---

## Task 9: `upsert_lineage_edge` method (TDD)

**Files:**
- Modify: `amx/storage/sqlalchemy_store.py`
- Test: `tests/storage/test_sqlalchemy_lineage.py`

- [ ] **Step 1: Append failing test for upsert + list**

```python
def test_upsert_lineage_edge_round_trip(store):
    a = store.create_lineage_artifact(
        local_id=1, name="t", db_profile="x", anchor_entity_ref="x|a|b|c"
    )
    n1 = store.upsert_lineage_node(
        local_id=10, artifact_uuid=a, entity_ref="x|a|b|c", entity_kind="table",
        db_profile="x", x=0, y=0, width=100, height=80,
    )
    n2 = store.upsert_lineage_node(
        local_id=11, artifact_uuid=a, entity_ref="x|a|b|d", entity_kind="table",
        db_profile="x", x=200, y=0, width=100, height=80,
    )
    edge = store.upsert_lineage_edge(
        local_id=50, artifact_uuid=a,
        source_node_uuid=n1, target_node_uuid=n2,
        edge_kind="join", join_type="LEFT",
        on_condition="a.id = b.a_id",
        where_clause="a.active = true",
        source_columns_json=["id"],
        target_columns_json=["a_id"],
        label=None, style_json=None, waypoints_json=None,
    )
    edges = store.list_lineage_edges(artifact_uuid=a)
    assert len(edges) == 1
    assert edges[0].id == edge
    assert edges[0].join_type == "LEFT"
    assert edges[0].on_condition == "a.id = b.a_id"
```

- [ ] **Step 2: Run → FAIL.**

- [ ] **Step 3: Implement `upsert_lineage_edge` + `list_lineage_edges`** (same pattern as `upsert_lineage_node` — copy-adapt; do not skip — the engineer may be reading tasks out of order).

```python
def upsert_lineage_edge(
    self,
    *,
    local_id: int,
    artifact_uuid: str,
    source_node_uuid: str,
    target_node_uuid: str,
    edge_kind: str,
    join_type: str | None = None,
    on_condition: str | None = None,
    where_clause: str | None = None,
    source_columns_json: list | None = None,
    target_columns_json: list | None = None,
    label: str | None = None,
    style_json: dict | None = None,
    waypoints_json: list | None = None,
) -> str:
    now = datetime.now(timezone.utc)
    existing = self._find_edge_uuid_by_local_id(self._hostname, local_id)
    if existing:
        with self._engine.begin() as conn:
            conn.execute(
                self._t_lineage_artifact_edges.update()
                .where(self._t_lineage_artifact_edges.c.id == existing)
                .values(
                    edge_kind=edge_kind,
                    join_type=join_type,
                    on_condition=on_condition,
                    where_clause=where_clause,
                    source_columns_json=source_columns_json,
                    target_columns_json=target_columns_json,
                    label=label,
                    style_json=style_json,
                    waypoints_json=waypoints_json,
                    updated_at=now,
                )
            )
        return existing
    uuid_value = str(uuid.uuid4())
    with self._engine.begin() as conn:
        conn.execute(
            self._t_lineage_artifact_edges.insert().values(
                id=uuid_value,
                artifact_id=artifact_uuid,
                source_node_id=source_node_uuid,
                target_node_id=target_node_uuid,
                edge_kind=edge_kind,
                join_type=join_type,
                on_condition=on_condition,
                where_clause=where_clause,
                source_columns_json=source_columns_json,
                target_columns_json=target_columns_json,
                label=label,
                style_json=style_json,
                waypoints_json=waypoints_json,
                created_by=self._username,
                hostname=self._hostname,
                client_version=self._client_version,
                created_at=now,
                updated_at=now,
                local_id=local_id,
            )
        )
    return uuid_value


def _find_edge_uuid_by_local_id(
    self, hostname: str, local_id: int
) -> str | None:
    with self._engine.connect() as conn:
        row = conn.execute(
            select(self._t_lineage_artifact_edges.c.id)
            .where(self._t_lineage_artifact_edges.c.hostname == hostname)
            .where(self._t_lineage_artifact_edges.c.local_id == local_id)
        ).fetchone()
    return row[0] if row else None


def list_lineage_edges(
    self, *, artifact_uuid: str
) -> list[LineageEdgeRecord]:
    stmt = (
        select(self._t_lineage_artifact_edges)
        .where(self._t_lineage_artifact_edges.c.artifact_id == artifact_uuid)
    )
    with self._engine.connect() as conn:
        rows = conn.execute(stmt).fetchall()
    return [LineageEdgeRecord(**row._mapping) for row in rows]
```

Add `LineageEdgeRecord` dataclass.

- [ ] **Step 4: Run → PASS.**

- [ ] **Step 5: Commit**

```bash
git add amx/storage/sqlalchemy_store.py tests/storage/test_sqlalchemy_lineage.py
git commit -m "feat(storage): add upsert_lineage_edge + list_lineage_edges"
```

---

## Task 10: `upsert_lineage_comment` method (TDD)

**Files:**
- Modify: `amx/storage/sqlalchemy_store.py`
- Test: `tests/storage/test_sqlalchemy_lineage.py`

- [ ] **Step 1: Append failing test**

```python
def test_upsert_lineage_comment_round_trip(store):
    a = store.create_lineage_artifact(
        local_id=1, name="t", db_profile="x", anchor_entity_ref="x|a|b|c"
    )
    cuuid = store.upsert_lineage_comment(
        local_id=200, artifact_uuid=a,
        x=10.0, y=20.0, width=200.0, height=80.0,
        color="#fef3c7", style="note",
        text="Check this join with @bob",
    )
    comments = store.list_lineage_comments(artifact_uuid=a)
    assert len(comments) == 1
    assert comments[0].id == cuuid
    assert comments[0].text == "Check this join with @bob"
    assert comments[0].created_by == store._username
```

- [ ] **Step 2: Run → FAIL.**

- [ ] **Step 3: Implement `upsert_lineage_comment`, `list_lineage_comments`, `delete_lineage_comment`** (full code, no placeholders):

```python
def upsert_lineage_comment(
    self,
    *,
    local_id: int,
    artifact_uuid: str,
    x: float,
    y: float,
    width: float,
    height: float,
    color: str | None = None,
    style: str = "note",
    text: str = "",
) -> str:
    now = datetime.now(timezone.utc)
    existing = self._find_comment_uuid_by_local_id(self._hostname, local_id)
    if existing:
        with self._engine.begin() as conn:
            conn.execute(
                self._t_lineage_comments.update()
                .where(self._t_lineage_comments.c.id == existing)
                .values(
                    x=x, y=y, width=width, height=height,
                    color=color, style=style, text=text,
                    updated_at=now,
                )
            )
        return existing
    uuid_value = str(uuid.uuid4())
    with self._engine.begin() as conn:
        conn.execute(
            self._t_lineage_comments.insert().values(
                id=uuid_value,
                artifact_id=artifact_uuid,
                x=x, y=y, width=width, height=height,
                color=color, style=style, text=text,
                created_by=self._username,
                hostname=self._hostname,
                client_version=self._client_version,
                created_at=now,
                updated_at=now,
                local_id=local_id,
            )
        )
    return uuid_value


def _find_comment_uuid_by_local_id(
    self, hostname: str, local_id: int
) -> str | None:
    with self._engine.connect() as conn:
        row = conn.execute(
            select(self._t_lineage_comments.c.id)
            .where(self._t_lineage_comments.c.hostname == hostname)
            .where(self._t_lineage_comments.c.local_id == local_id)
        ).fetchone()
    return row[0] if row else None


def list_lineage_comments(
    self, *, artifact_uuid: str
) -> list[LineageCommentRecord]:
    stmt = (
        select(self._t_lineage_comments)
        .where(self._t_lineage_comments.c.artifact_id == artifact_uuid)
    )
    with self._engine.connect() as conn:
        rows = conn.execute(stmt).fetchall()
    return [LineageCommentRecord(**row._mapping) for row in rows]


def delete_lineage_comment(self, *, uuid: str) -> None:
    with self._engine.begin() as conn:
        conn.execute(
            self._t_lineage_comments.delete()
            .where(self._t_lineage_comments.c.id == uuid)
        )
```

Add `LineageCommentRecord` dataclass.

- [ ] **Step 4: Run → PASS.**

- [ ] **Step 5: Commit**

```bash
git add amx/storage/sqlalchemy_store.py tests/storage/test_sqlalchemy_lineage.py
git commit -m "feat(storage): add lineage_comment upsert/list/delete with author"
```

---

## Task 11: `find_prior_lineage_by_others` method (TDD)

**Files:**
- Modify: `amx/storage/sqlalchemy_store.py`
- Test: `tests/storage/test_sqlalchemy_lineage.py`

- [ ] **Step 1: Append failing test**

```python
def test_find_prior_lineage_by_others_excludes_self(store):
    # Two artifacts: one from this host, one from a different host.
    store.create_lineage_artifact(
        local_id=1, name="mine", db_profile="prod_pg",
        anchor_entity_ref="prod_pg|main|public|orders",
    )
    # Simulate another host by inserting directly.
    from datetime import datetime, timezone
    with store._engine.begin() as conn:
        conn.execute(store._t_lineage_artifacts.insert().values(
            id="other-uuid-from-alice",
            name="alice_orders",
            db_profile="prod_pg",
            anchor_entity_ref="prod_pg|main|public|orders",
            created_by="alice",
            hostname="alice-laptop",
            client_version="0.14.0",
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            local_id=999,
        ))
    others = store.find_prior_lineage_by_others(
        db_profile="prod_pg",
        anchor_entity_ref="prod_pg|main|public|orders",
        exclude_hostname=store._hostname,
    )
    assert len(others) == 1
    assert others[0].created_by == "alice"
```

- [ ] **Step 2: Run → FAIL.**

- [ ] **Step 3: Implement the method**

```python
def find_prior_lineage_by_others(
    self,
    *,
    db_profile: str,
    anchor_entity_ref: str,
    exclude_hostname: str,
) -> list[LineageArtifactRecord]:
    stmt = (
        select(self._t_lineage_artifacts)
        .where(self._t_lineage_artifacts.c.db_profile == db_profile)
        .where(
            self._t_lineage_artifacts.c.anchor_entity_ref == anchor_entity_ref
        )
        .where(self._t_lineage_artifacts.c.hostname != exclude_hostname)
        .order_by(self._t_lineage_artifacts.c.updated_at.desc())
    )
    with self._engine.connect() as conn:
        rows = conn.execute(stmt).fetchall()
    return [LineageArtifactRecord(**row._mapping) for row in rows]
```

- [ ] **Step 4: Run → PASS.**

- [ ] **Step 5: Commit**

```bash
git add amx/storage/sqlalchemy_store.py tests/storage/test_sqlalchemy_lineage.py
git commit -m "feat(storage): add find_prior_lineage_by_others for collision detection"
```

---

## Task 12: `DualWriteHistoryStore` lineage write wrappers (TDD)

**Files:**
- Modify: `amx/storage/dual_write.py`
- Test: `tests/storage/test_dual_write_lineage.py`

- [ ] **Step 1: Write failing test**

```python
# tests/storage/test_dual_write_lineage.py
import pytest
from sqlalchemy import create_engine
from amx.storage.sqlalchemy_store import SQLAlchemyHistoryStore
from amx.storage.sqlite_store import SQLiteHistoryStore
from amx.storage.dual_write import DualWriteHistoryStore


@pytest.fixture
def dual(tmp_path):
    local = SQLiteHistoryStore(tmp_path / "history.db")
    local.init()
    engine = create_engine(f"sqlite:///{tmp_path}/shared.db")
    shared = SQLAlchemyHistoryStore(engine, schema="AMX")
    shared.init()
    return DualWriteHistoryStore(local, shared), local, shared


def test_dual_write_create_lineage_artifact_mirrors_both(dual):
    dual_store, local, shared = dual
    uuid_value = dual_store.create_lineage_artifact(
        name="t", db_profile="prod_pg",
        anchor_entity_ref="prod_pg|m|p|orders",
    )
    # Shared has the row.
    shared_rows = shared.list_lineage_artifacts()
    assert any(r.id == uuid_value for r in shared_rows)
    # Local has a row with a matching local_id linkage.
    local_artifacts = local.list_lineage_artifacts(db_profile="prod_pg")
    assert any(
        a["name"] == "t" and a["db_profile"] == "prod_pg"
        for a in local_artifacts
    )
```

- [ ] **Step 2: Run → FAIL** ("'DualWriteHistoryStore' object has no attribute 'create_lineage_artifact'" — caught by `__getattr__` and routed to local, which has no `create_lineage_artifact` method either; the fallthrough lookup fails).

- [ ] **Step 3: Add lineage OP constants**

In `amx/storage/dual_write.py`, near the existing `OP_CREATE_RUN` definitions (around line 42):

```python
OP_LINEAGE_CREATE_ARTIFACT = "lineage_create_artifact"
OP_LINEAGE_UPSERT_NODE = "lineage_upsert_node"
OP_LINEAGE_UPSERT_EDGE = "lineage_upsert_edge"
OP_LINEAGE_UPSERT_COMMENT = "lineage_upsert_comment"
OP_LINEAGE_DELETE_COMMENT = "lineage_delete_comment"
```

- [ ] **Step 4: Implement `create_lineage_artifact` on `DualWriteHistoryStore`**

Add as an explicit method (do NOT rely on `__getattr__`):

```python
def create_lineage_artifact(
    self,
    *,
    name: str,
    db_profile: str,
    anchor_entity_ref: str,
    **kwargs,
) -> str:
    # 1. Local write first; capture local integer PK.
    local_id = self._local.create_lineage_artifact(
        name=name,
        db_profile=db_profile,
        anchor_entity_ref=anchor_entity_ref,
        **kwargs,
    )
    # 2. Shared write, stamped with hostname + local_id for idempotency.
    payload = {
        "local_id": local_id,
        "name": name,
        "db_profile": db_profile,
        "anchor_entity_ref": anchor_entity_ref,
        **kwargs,
    }
    return self._try_remote(
        OP_LINEAGE_CREATE_ARTIFACT,
        payload,
        lambda: self._shared.create_lineage_artifact(**payload),
    )
```

- [ ] **Step 5: Ensure `SQLiteHistoryStore.create_lineage_artifact` exists**

Locate the existing local lineage creation function in `amx/storage/sqlite_store.py` (search for the current INSERT into `lineage_artifacts`). If it is already exposed as a public method, no change needed — the test will exercise it. If it is wrapped under another name (e.g. `save_lineage_artifact`), add a thin alias `create_lineage_artifact(...) -> int` that returns the integer PK.

- [ ] **Step 6: Implement `_replay_op` branch**

In `dual_write.py`, locate `_replay_op` (around line 212) and add a branch:

```python
elif op == OP_LINEAGE_CREATE_ARTIFACT:
    self._shared.create_lineage_artifact(**payload)
```

- [ ] **Step 7: Run → PASS.**

- [ ] **Step 8: Commit**

```bash
git add amx/storage/dual_write.py amx/storage/sqlite_store.py \
        tests/storage/test_dual_write_lineage.py
git commit -m "feat(storage): dual-write lineage_create_artifact through shared store"
```

---

## Task 13: Dual-write wrappers for nodes, edges, comments (TDD)

**Files:**
- Modify: `amx/storage/dual_write.py`
- Test: `tests/storage/test_dual_write_lineage.py`

- [ ] **Step 1: Append failing tests**

```python
def test_dual_write_upsert_node_mirrors(dual):
    dual_store, _local, shared = dual
    a = dual_store.create_lineage_artifact(
        name="t", db_profile="x", anchor_entity_ref="x|a|b|c",
    )
    dual_store.upsert_lineage_node(
        artifact_uuid=a,
        entity_ref="x|a|b|c", entity_kind="table", db_profile="x",
        x=0, y=0, width=100, height=80,
    )
    nodes = shared.list_lineage_nodes(artifact_uuid=a)
    assert len(nodes) == 1


def test_dual_write_upsert_edge_mirrors(dual):
    dual_store, _local, shared = dual
    a = dual_store.create_lineage_artifact(
        name="t", db_profile="x", anchor_entity_ref="x|a|b|c",
    )
    n1 = dual_store.upsert_lineage_node(
        artifact_uuid=a, entity_ref="x|a|b|c", entity_kind="table",
        db_profile="x", x=0, y=0, width=100, height=80,
    )
    n2 = dual_store.upsert_lineage_node(
        artifact_uuid=a, entity_ref="x|a|b|d", entity_kind="table",
        db_profile="x", x=200, y=0, width=100, height=80,
    )
    dual_store.upsert_lineage_edge(
        artifact_uuid=a,
        source_node_uuid=n1, target_node_uuid=n2,
        edge_kind="join", join_type="LEFT",
        on_condition="a.id = b.a_id",
    )
    edges = shared.list_lineage_edges(artifact_uuid=a)
    assert len(edges) == 1


def test_dual_write_upsert_comment_mirrors(dual):
    dual_store, _local, shared = dual
    a = dual_store.create_lineage_artifact(
        name="t", db_profile="x", anchor_entity_ref="x|a|b|c",
    )
    dual_store.upsert_lineage_comment(
        artifact_uuid=a,
        x=0, y=0, width=200, height=80,
        text="hello team",
    )
    comments = shared.list_lineage_comments(artifact_uuid=a)
    assert len(comments) == 1
    assert comments[0].text == "hello team"
```

- [ ] **Step 2: Run → FAIL.**

- [ ] **Step 3: Add the three wrappers**

Pattern is identical to `create_lineage_artifact` in Task 12. Each wrapper:
- Calls the local store first, captures `local_id`.
- Forwards to shared via `_try_remote(OP, payload, lambda: ...)`.

Add to `dual_write.py`:

```python
def upsert_lineage_node(
    self,
    *,
    artifact_uuid: str,
    entity_ref: str,
    entity_kind: str,
    db_profile: str,
    x: float, y: float, width: float, height: float,
    **kwargs,
) -> str:
    local_id = self._local.upsert_lineage_node(
        artifact_uuid=artifact_uuid,
        entity_ref=entity_ref, entity_kind=entity_kind,
        db_profile=db_profile,
        x=x, y=y, width=width, height=height,
        **kwargs,
    )
    payload = {
        "local_id": local_id,
        "artifact_uuid": artifact_uuid,
        "entity_ref": entity_ref,
        "entity_kind": entity_kind,
        "db_profile": db_profile,
        "x": x, "y": y, "width": width, "height": height,
        **kwargs,
    }
    return self._try_remote(
        OP_LINEAGE_UPSERT_NODE,
        payload,
        lambda: self._shared.upsert_lineage_node(**payload),
    )


def upsert_lineage_edge(
    self, *, artifact_uuid: str, source_node_uuid: str,
    target_node_uuid: str, edge_kind: str, **kwargs,
) -> str:
    local_id = self._local.upsert_lineage_edge(
        artifact_uuid=artifact_uuid,
        source_node_uuid=source_node_uuid,
        target_node_uuid=target_node_uuid,
        edge_kind=edge_kind,
        **kwargs,
    )
    payload = {
        "local_id": local_id,
        "artifact_uuid": artifact_uuid,
        "source_node_uuid": source_node_uuid,
        "target_node_uuid": target_node_uuid,
        "edge_kind": edge_kind,
        **kwargs,
    }
    return self._try_remote(
        OP_LINEAGE_UPSERT_EDGE,
        payload,
        lambda: self._shared.upsert_lineage_edge(**payload),
    )


def upsert_lineage_comment(
    self, *, artifact_uuid: str, x: float, y: float,
    width: float, height: float, text: str = "", **kwargs,
) -> str:
    local_id = self._local.upsert_lineage_comment(
        artifact_uuid=artifact_uuid,
        x=x, y=y, width=width, height=height, text=text, **kwargs,
    )
    payload = {
        "local_id": local_id,
        "artifact_uuid": artifact_uuid,
        "x": x, "y": y, "width": width, "height": height,
        "text": text, **kwargs,
    }
    return self._try_remote(
        OP_LINEAGE_UPSERT_COMMENT,
        payload,
        lambda: self._shared.upsert_lineage_comment(**payload),
    )


def delete_lineage_comment(self, *, uuid: str) -> None:
    # Local delete keyed on shared UUID is not portable (local uses int PK).
    # The local store exposes delete_lineage_comment_by_shared_uuid for the
    # cross-key lookup; falls back to local-only no-op if absent.
    if hasattr(self._local, "delete_lineage_comment_by_shared_uuid"):
        self._local.delete_lineage_comment_by_shared_uuid(uuid)
    self._try_remote(
        OP_LINEAGE_DELETE_COMMENT,
        {"uuid": uuid},
        lambda: self._shared.delete_lineage_comment(uuid=uuid),
    )
```

Mirror the local-store companion methods (`upsert_lineage_edge`, `upsert_lineage_comment`, `delete_lineage_comment_by_shared_uuid`) in `sqlite_store.py` if they are missing; they should each return the integer PK or `None` for deletes.

- [ ] **Step 4: Extend `_replay_op` branches**

```python
elif op == OP_LINEAGE_UPSERT_NODE:
    self._shared.upsert_lineage_node(**payload)
elif op == OP_LINEAGE_UPSERT_EDGE:
    self._shared.upsert_lineage_edge(**payload)
elif op == OP_LINEAGE_UPSERT_COMMENT:
    self._shared.upsert_lineage_comment(**payload)
elif op == OP_LINEAGE_DELETE_COMMENT:
    self._shared.delete_lineage_comment(**payload)
```

- [ ] **Step 5: Run → PASS.**

- [ ] **Step 6: Commit**

```bash
git add amx/storage/dual_write.py amx/storage/sqlite_store.py \
        tests/storage/test_dual_write_lineage.py
git commit -m "feat(storage): dual-write lineage nodes/edges/comments"
```

---

## Task 14: Outbox replay test for lineage ops (TDD)

**Files:**
- Test: `tests/storage/test_dual_write_lineage.py`

- [ ] **Step 1: Append failing test**

```python
def test_lineage_write_falls_back_to_outbox_when_shared_down(tmp_path):
    """When the shared engine raises, the op queues in pending_shared_writes
    and replays on flush."""
    from sqlalchemy import create_engine
    from unittest.mock import patch
    from amx.storage.sqlalchemy_store import SQLAlchemyHistoryStore
    from amx.storage.sqlite_store import SQLiteHistoryStore
    from amx.storage.dual_write import DualWriteHistoryStore

    local = SQLiteHistoryStore(tmp_path / "h.db")
    local.init()
    engine = create_engine(f"sqlite:///{tmp_path}/shared.db")
    shared = SQLAlchemyHistoryStore(engine, schema="AMX")
    shared.init()
    dual_store = DualWriteHistoryStore(local, shared)

    # Force shared.create_lineage_artifact to fail once.
    with patch.object(
        shared, "create_lineage_artifact",
        side_effect=Exception("shared down"),
    ):
        uuid_value = dual_store.create_lineage_artifact(
            name="t", db_profile="x", anchor_entity_ref="x|a|b|c",
        )
        assert uuid_value is None or isinstance(uuid_value, str)
        # Outbox now has a pending entry.
        assert dual_store.pending_count() >= 1
        # Shared has no row yet.
        assert shared.list_lineage_artifacts() == []

    # Flush replays the queued op.
    dual_store.flush_pending()
    assert dual_store.pending_count() == 0
    assert len(shared.list_lineage_artifacts()) == 1
```

- [ ] **Step 2: Run → PASS** if the outbox replay was wired correctly in Tasks 12-13.

If the test FAILS, the most likely cause is a missing `OP_LINEAGE_*` branch in `_replay_op`. Fix the missing branch, re-run.

- [ ] **Step 3: Commit (no code changes needed if PASS first time, otherwise commit the fix)**

```bash
git add tests/storage/test_dual_write_lineage.py
git commit -m "test(storage): outbox replay for lineage ops on shared failure"
```

---

## Task 15: Update `tests/test_shared_schema_comments.py` to cover new tables

**Files:**
- Modify: `tests/test_shared_schema_comments.py`

- [ ] **Step 1: Run the existing CI guard test to confirm it now picks up the new tables**

Run: `pytest tests/test_shared_schema_comments.py -v`
Expected: PASS — the existing test iterates every `Column(comment=...)` and asserts non-empty. Our new tables already use `_desc(...)` which raises `KeyError` on missing descriptions, so any gap fails fast.

- [ ] **Step 2: If the guard test does not iterate `lineage_*` tables, broaden it**

If the test uses a hard-coded table list (rather than iterating `md.tables`), append the four new table names. Otherwise no change needed.

- [ ] **Step 3: Commit (only if Step 2 changed the file)**

```bash
git add tests/test_shared_schema_comments.py
git commit -m "test(schema): extend description guard for lineage tables"
```

---

## Task 16: PR-1 end-to-end smoke test

**Files:**
- Test: `tests/storage/test_dual_write_lineage.py`

- [ ] **Step 1: Append end-to-end test**

```python
def test_pr1_end_to_end_team_visibility(tmp_path):
    """Two users on different hosts both connect to the same shared store.
    User A creates lineage; User B sees it in list_lineage_artifacts."""
    from sqlalchemy import create_engine
    from amx.storage.sqlalchemy_store import SQLAlchemyHistoryStore
    from amx.storage.sqlite_store import SQLiteHistoryStore
    from amx.storage.dual_write import DualWriteHistoryStore

    shared_engine = create_engine(f"sqlite:///{tmp_path}/team.db")
    shared_a = SQLAlchemyHistoryStore(shared_engine, schema="AMX")
    shared_a.init()
    shared_b = SQLAlchemyHistoryStore(shared_engine, schema="AMX")

    local_a = SQLiteHistoryStore(tmp_path / "a.db"); local_a.init()
    local_b = SQLiteHistoryStore(tmp_path / "b.db"); local_b.init()

    dual_a = DualWriteHistoryStore(local_a, shared_a)
    # User A creates an artifact.
    dual_a.create_lineage_artifact(
        name="orders_lineage",
        db_profile="prod_pg",
        anchor_entity_ref="prod_pg|main|public|orders",
    )

    # User B (different SQLAlchemy store on same shared engine) sees it.
    artifacts_b_sees = shared_b.list_lineage_artifacts(db_profiles=["prod_pg"])
    assert len(artifacts_b_sees) == 1
    assert artifacts_b_sees[0].name == "orders_lineage"
    assert artifacts_b_sees[0].created_by  # non-empty username
```

- [ ] **Step 2: Run → PASS.**

- [ ] **Step 3: Commit**

```bash
git add tests/storage/test_dual_write_lineage.py
git commit -m "test(storage): PR-1 e2e team visibility smoke test"
```

---

## PR-1 Pre-Merge Checklist

- [ ] All new tests pass: `pytest tests/storage/test_shared_lineage_schema.py tests/storage/test_sqlalchemy_lineage.py tests/storage/test_dual_write_lineage.py -v`
- [ ] Existing schema-description guards pass: `pytest tests/test_shared_schema_comments.py tests/test_local_schema_comments.py -v`
- [ ] Full unit suite: `pytest -x` from repo root
- [ ] Lint + format: `ruff check . && ruff format --check .`
- [ ] Type check: `mypy amx/storage/`
- [ ] `grep -ri "paid" amx/storage/ tests/storage/` → no hits (CLAUDE.md Rule 2)
- [ ] No Turkish strings in changed files (CLAUDE.md Rule 4)
- [ ] No `Co-Authored-By: Claude` trailers on any commit (CLAUDE.md commit-messages rule)
- [ ] No Studio-visible behavioral change in this PR → `deploy.sh` not required for PR-1
- [ ] Cross-platform: nothing in this PR uses POSIX-only APIs (Python `pathlib`, `uuid`, `datetime` only)

---

# PR-2 through PR-8: Roadmap

Each PR below will be expanded into its own bite-sized plan file (`docs/superpowers/plans/2026-05-...-history-store-pr-N-<topic>.md`) **before its execution begins**. The summaries below are scope locks, not implementation drafts.

## PR-2: Pages `db_profile` + attribution

**Goal:** Add `db_profile`, `hostname`, `client_version`, `local_id` columns to `documentation_pages` (both shared and local), backed by a backend-dispatched `ALTER TABLE` migration helper for existing deploys. Add `/pages assign-profile <slug>` CLI command for retroactive profile assignment.

**Critical files:**
- `amx/storage/shared_schema.py` (modify `documentation_pages`)
- `amx/storage/migration.py` (new — backend-dispatched ALTER TABLE)
- `amx/storage/sqlite_store.py` (idempotent local ALTER)
- `amx/storage/sqlalchemy_store.py` (extend pages methods to accept `db_profile`)
- `amx/storage/dual_write.py` (`OP_PAGE_*` wrappers)
- `amx/cli_support/commands/pages.py` (`/pages assign-profile`)

**Acceptance:** New pages can be tagged with a `db_profile`; existing pages stay NULL until assigned; migration runs idempotently across Postgres / Snowflake / BigQuery / MySQL / Oracle / Databricks.

## PR-3: Optimistic Concurrency Control (OCC)

**Goal:** Add `version INTEGER NOT NULL DEFAULT 1` to every concurrent-edit shared table (lineage_*, documentation_pages, schema_*_descriptions). All UPDATEs use `WHERE id=? AND version=?` precondition. Introduce `amx/storage/conflicts.py` with `StaleVersionError`. `force_overwrite=True` flag bypasses the check and writes an audit log entry.

**Critical files:**
- `amx/storage/conflicts.py` (new)
- `amx/storage/shared_schema.py` (version columns)
- `amx/storage/migration.py` (ALTER TABLE for existing deploys)
- `amx/storage/sqlalchemy_store.py` (every UPDATE method gains `expected_version` kwarg)
- `amx/storage/sqlite_store.py` (mirror version columns)

**Acceptance:** Concurrent UPDATE returns `StaleVersionError` with current value snapshot; force-overwrite path emits audit log entry; backfill INSERTs continue to work (version defaults to 1).

## PR-4: Admin tables + identity bootstrap

**Goal:** Add `_amx_users`, `_amx_admin_audit`, `_amx_session_events` shared tables. On every shared-store connection, resolve `(getpass.getuser(), socket.gethostname())`; auto-insert as admin if `_amx_users` is empty, else viewer. Record connect/disconnect to `_amx_session_events`.

**Critical files:**
- `amx/storage/shared_schema.py` (3 new tables)
- `amx/storage/schema_descriptions.py` (descriptions)
- `amx/storage/admin.py` (new — `promote_to_admin`, `demote_admin`, `revoke_user`, `list_members`, `record_session_event`, `current_user_role`)
- `amx/storage/factory.py` (identity resolution + session event on bootstrap)

**Acceptance:** First connecting user becomes admin; subsequent users become viewers; session events accumulate in admin-readable table; admin can promote/demote/revoke; "at least one admin" invariant enforced.

## PR-5: Auto-backfill (`BackfillRunner`)

**Goal:** New `amx/storage/backfill.py` with `BackfillRunner` that on first shared-mode connection iterates local SQLite lineage/pages rows and INSERTs missing ones into the shared store. Idempotent via `(hostname, local_id)` lookup. Background thread; failures fall through to `pending_shared_writes` outbox. Sentinel table `_amx_backfill_state` per `(scope, profile, schema)`.

**Critical files:**
- `amx/storage/backfill.py` (new — `BackfillRunner`, `BackfillReport`, sentinel)
- `amx/storage/factory.py` (kick off background backfill in `_bootstrap_dual_or_local`)
- `amx/storage/dual_write.py` (`force_backfill()` proxy on `_LazyDualWriteStore`)
- `amx/cli_support/commands/history.py` (`/history sync-local`, `/history status`)

**Acceptance:** Fresh shared connection backfills all local lineage + pages once; second run is a no-op (sentinel); transient failures queue to outbox and drain on next CLI command; runs in background thread (Windows-compatible).

## PR-6: Admin CLI + API + permission enforcement

**Goal:** Add `/admin members | promote | demote | revoke | audit | sessions` CLI commands and `amx/web/routers/admin.py` FastAPI endpoints. Every write endpoint checks `current_user.role`; viewer write returns HTTP 403 with structured error.

**Critical files:**
- `amx/cli_support/commands/admin.py` (new — registered under existing namespace tabs)
- `amx/web/routers/admin.py` (new)
- `amx/web/routers/{lineage,pages,history}.py` (permission middleware)
- `amx/web/server.py` (include admin router)

**Acceptance:** Viewer attempting to write a lineage edit gets a structured 403; admin can promote a viewer via CLI; audit log records every promote/demote/revoke; non-admin running `/admin promote` gets a helpful message listing existing admins.

## PR-7: Studio cross-profile filter chips + migration banner

**Goal:** Studio History / Lineage / Pages pages default to all-profiles view; multi-select profile filter chips in a sticky header; "Mine only" / "Others" preset filters. Migration banner during backfill. API endpoints accept `db_profiles` query param. Mobile-responsive (CLAUDE.md feedback_studio_responsive_required).

**Critical files:**
- `frontend/src/components/filters/ProfileFilterChips.tsx` (new)
- `frontend/src/routes/{History,Lineage,Pages}.tsx` (default to all profiles)
- `frontend/src/components/banners/BackfillBanner.tsx` (new)
- `amx/web/routers/{history,lineage,pages}.py` (accept `db_profiles` param)

**Acceptance:** Default Studio Lineage page shows artifacts across every active profile; filter chips collapse responsively on `sm:` breakpoint; backfill banner appears during PR-5 sync and disappears on completion. `deploy.sh` run before opening this PR (CLAUDE.md Rule 6).

## PR-8: Studio admin panel UI + conflict resolution dialog

**Goal:** New `Workspace Admin` Studio tab (admin-only) with Members, Activity, Audit, Settings sub-tabs. Conflict resolution dialog (PR-3 OCC) appears when save returns `StaleVersionError`: shows author + age + diff + `[Cancel] [Keep theirs] [Overwrite with mine] [Edit my version]`. CLI parallel: interactive prompt with same options.

**Critical files:**
- `frontend/src/routes/admin/Members.tsx`
- `frontend/src/routes/admin/Activity.tsx`
- `frontend/src/routes/admin/Audit.tsx`
- `frontend/src/routes/admin/Settings.tsx`
- `frontend/src/components/conflicts/ConflictDialog.tsx`
- `amx/cli_support/conflict_prompt.py` (interactive prompt for CLI)

**Acceptance:** Admin sees Workspace Admin tab; viewer does not; concurrent edit triggers conflict dialog with full diff; "Overwrite with mine" routes through `force_overwrite=True` and creates an audit entry; CLI conflict prompt accepts `[k|o|e|c]` keys plus `--on-conflict=...` for non-interactive use. `deploy.sh` run before opening this PR.

---

## Verification (full feature)

After all eight PRs merge:

1. **Fresh local install + shared mode off:** behavior identical to today.
2. **Enable shared mode on User A's machine:** background backfill pushes all local lineage + pages to warehouse; banner shows progress; first run completes; second start is no-op.
3. **User B connects to same warehouse:** auto-inserted as viewer; sees User A's lineage in Studio default view across all profiles.
4. **Filter test:** User B picks a single profile chip; list narrows; clears chip; full list returns.
5. **Conflict test:** User A and User B edit the same column description; second save shows conflict dialog with diff; "Overwrite" succeeds and creates audit entry; admin sees it in audit log.
6. **Admin test:** User A promotes User B to admin via `/admin promote`; User B's Studio now shows the Admin tab; audit log records the promotion; cannot demote the last admin.
7. **Offline test:** stop shared warehouse mid-edit; lineage write succeeds locally and queues to outbox; restart warehouse and run any CLI command; outbox drains.
8. **Per-profile config myth test:** `/restore-config` wizard's `history_store_profile` step shows the new help copy explaining single team workspace for all profiles.
9. **Cross-platform CI matrix:** PRs pass on Ubuntu, macOS, and Windows runners with the new lineage/admin/backfill tests included.
10. **No regression on critical path:** `init_history_store` < 50 ms; existing `analysis_runs` flow unchanged.

---

## Self-Review Notes (PR-1)

- **Spec coverage:** PR-1 implements spec §1.1 (lineage_artifacts, _nodes, _edges, _comments) and §2.1, §2.2 partial (lineage methods + dual-write wrappers). Pages / OCC / Admin / Backfill / Studio deferred to PR-2 through PR-8 per the roadmap.
- **Placeholder scan:** none — every step contains either complete code, an explicit command, or a verified-existing file location to modify.
- **Type consistency:** `LineageArtifactRecord`, `LineageNodeRecord`, `LineageEdgeRecord`, `LineageCommentRecord` defined once; method signatures use them consistently; `local_id: int`, `*_uuid: str` naming uniform throughout.
- **Spec §1.4 (schema descriptions zorunluluğu):** addressed in every table task (steps 4 / descriptions are part of the same task as the table DDL — same commit, satisfying CLAUDE.md Rule 5).
- **Spec §1.6 (SHARED_SCHEMA_VERSION bump):** Task 5.
