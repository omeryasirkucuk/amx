# Deepening Native Lineage Use in RUN and ASK — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `/analyze run` (RUN) and `/ask` (ASK) consume the materialized Databricks native-lineage graph deeply — name-resolved, canvas-free, and on both CLI and Studio — through one shared neighbour-query core, with zero new LLM round-trips.

**Architecture:** A single new module `amx/lineage/neighbors.py` owns the one-hop, name-resolved, profile-scoped walk over `catalog_relationships`. RUN's `resolve_lineage_context_for_run` and a new canvas-free ASK helper both call it. RUN additionally attaches neighbour descriptions and gains CLI parity; ASK renders neighbour **names** (not raw entity IDs) in its prompt appendix. All enrichment is assembled before the LLM call. A kill-switch env var (`AMX_LINEAGE_CONTEXT_DISABLED`) short-circuits the core.

**Tech Stack:** Python 3.12, SQLite (`SQLiteHistoryStore`), pytest. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-05-27-lineage-in-run-and-ask-design.md`

**Working agreement reminders (from repo conventions):**
- All tracked content English-only; cross-platform (no POSIX-only paths/shells/signals); no misleading feature-gating wording.
- Commit messages: single `-m` line, no co-author trailer, no agent attribution.
- Keep the files this branch touches green; do not chase preexisting `main` test failures.
- Work on a feature branch, not `main`.

---

## File Structure

- **Create** `amx/lineage/neighbors.py` — shared `Neighbor` value object + `lineage_neighbors()` walk + `enrichment_disabled()` kill-switch. Single responsibility: read one-hop named lineage neighbours for anchor entity ids.
- **Modify** `amx/analyze/lineage_context.py` — refactor `resolve_lineage_context_for_run` onto the core; add neighbour-description enrichment.
- **Modify** `amx/agents/profile_agent.py` — render the optional `detail` field on each lineage block.
- **Modify** `amx/cli_support/commands/_analyze/run_loop.py` — resolve lineage blocks once per run and attach to each per-schema orchestrator (CLI parity with the Studio worker).
- **Modify** `amx/lineage/evidence.py` — add canvas-free, name-resolved `build_native_lineage_neighbors()` (leaves `build_lineage_evidence` and its tests untouched).
- **Modify** `amx/search/_agent/retrieval.py` — fold named neighbours into `retrieval_details["lineage"]`.
- **Modify** `amx/search/tool_agent.py` — render neighbour **names** in `_format_lineage_pages_appendix`, dropping raw entity-id lines.
- **Create** tests under `tests/lineage/`, `tests/analyze/`, `tests/search/` per task.

---

## Task 1: Shared neighbour-query core

**Files:**
- Create: `amx/lineage/neighbors.py`
- Test: `tests/lineage/test_neighbors.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/lineage/test_neighbors.py`:

```python
"""Tests for the shared one-hop native-lineage neighbour query."""

from __future__ import annotations

import time
from pathlib import Path

from amx.lineage.neighbors import Neighbor, enrichment_disabled, lineage_neighbors
from amx.storage.sqlite_store import SQLiteHistoryStore


def _hs(tmp_path: Path) -> SQLiteHistoryStore:
    hs = SQLiteHistoryStore(tmp_path / "history.db")
    hs.init()
    return hs


def _entity(hs, *, schema, table, kind="table", search_text="", state="full") -> int:
    with hs._connect() as conn:
        cur = conn.execute(
            "INSERT INTO catalog_entities (db_profile, db_backend, database_name, "
            "schema_name, table_name, entity_kind, asset_kind, search_text, metadata_state) "
            "VALUES ('dbr','databricks','wh',?,?,?,?,?,?)",
            (schema, table, kind, kind, search_text, state),
        )
        return int(cur.lastrowid)


def _edge(hs, frm, to, rel) -> None:
    with hs._connect() as conn:
        conn.execute(
            "INSERT INTO catalog_relationships (from_entity_id, to_entity_id, "
            "relationship_type, score, source, details_json, last_seen, "
            "from_entity_kind, to_entity_kind) VALUES (?,?,?,1.0,'native','{}',?,?,?)",
            (frm, to, rel, time.time(), "table", "table"),
        )


def test_resolves_upstream_and_downstream_names(tmp_path: Path) -> None:
    hs = _hs(tmp_path)
    anchor = _entity(hs, schema="sales", table="orders")
    parent = _entity(hs, schema="sales", table="customers")
    nb = _entity(hs, schema="__assets", table="nb#1", kind="notebook", search_text="ETL nb")
    _edge(hs, parent, anchor, "lineage_native_table")  # parent feeds anchor → upstream
    _edge(hs, anchor, nb, "lineage_native_asset")       # anchor feeds nb → downstream

    with hs._connect() as conn:
        out = lineage_neighbors(conn, anchor_entity_ids=[anchor])

    nbs = out[anchor]
    assert ("upstream", "sales.customers", "table") in {
        (n.direction, n.name, n.kind) for n in nbs
    }
    assert ("downstream", "ETL nb", "notebook") in {
        (n.direction, n.name, n.kind) for n in nbs
    }
    assert all(isinstance(n, Neighbor) for n in nbs)


def test_dedup_and_fanout_cap(tmp_path: Path) -> None:
    hs = _hs(tmp_path)
    anchor = _entity(hs, schema="s", table="a")
    # Six distinct upstream parents; cap at fanout=3.
    for i in range(6):
        p = _entity(hs, schema="s", table=f"p{i}")
        _edge(hs, p, anchor, "foreign_key")
    with hs._connect() as conn:
        out = lineage_neighbors(conn, anchor_entity_ids=[anchor], fanout=3)
    assert len(out[anchor]) == 3


def test_empty_inputs_and_kill_switch(tmp_path: Path, monkeypatch) -> None:
    hs = _hs(tmp_path)
    anchor = _entity(hs, schema="s", table="a")
    parent = _entity(hs, schema="s", table="b")
    _edge(hs, parent, anchor, "foreign_key")
    with hs._connect() as conn:
        assert lineage_neighbors(conn, anchor_entity_ids=[]) == {}
        monkeypatch.setenv("AMX_LINEAGE_CONTEXT_DISABLED", "1")
        assert enrichment_disabled() is True
        assert lineage_neighbors(conn, anchor_entity_ids=[anchor]) == {}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/lineage/test_neighbors.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'amx.lineage.neighbors'`

- [ ] **Step 3: Write the implementation**

Create `amx/lineage/neighbors.py`:

```python
"""Shared one-hop native-lineage neighbour query for RUN and ASK.

Given a set of anchor catalog-entity ids and an open SQLite connection,
return each anchor's immediate upstream producers and downstream
consumers as name-resolved, bounded neighbour records read straight
from ``catalog_relationships`` — no saved lineage canvas required.

This is the single place the native-lineage graph walk lives: the
``/analyze run`` lineage-context resolver and the /ask retrieval
enrichment both call it instead of carrying near-duplicate queries.
The shape is one-hop on purpose (the performance-safe default); the
design doc records multi-hop as a measured follow-up.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

# Relationship types that carry lineage meaning. Native edges
# (``lineage_native_*``, from ``/lineage fetch``) plus the structural
# edges RUN/ASK already treated as lineage. ``join_inference`` is
# excluded — it carries thousands of speculative edges.
LINEAGE_REL_TYPES: tuple[str, ...] = (
    "foreign_key",
    "view_depends_on",
    "asset_references_table",
    "lineage_native_table",
    "lineage_native_column",
    "lineage_native_asset",
)

# Per-anchor cap on returned neighbours (token budget guard).
DEFAULT_FANOUT = 12

_DISABLE_ENV = "AMX_LINEAGE_CONTEXT_DISABLED"


@dataclass(frozen=True)
class Neighbor:
    """One name-resolved lineage neighbour of an anchor entity."""

    direction: str  # "upstream" | "downstream"
    kind: str  # neighbour entity_kind (table/notebook/job/...)
    name: str  # human name: "schema.table" or asset search_text
    relationship: str  # relationship_type of the edge
    entity_id: int  # neighbour catalog_entities.id
    metadata_state: str  # "full" | "name_only"


def enrichment_disabled() -> bool:
    """True when the field kill-switch env var is set to a truthy value."""
    return os.environ.get(_DISABLE_ENV, "").strip().lower() in {"1", "true", "yes", "on"}


def lineage_neighbors(
    conn: Any,
    *,
    anchor_entity_ids: list[int],
    rel_types: tuple[str, ...] = LINEAGE_REL_TYPES,
    fanout: int = DEFAULT_FANOUT,
) -> dict[int, list[Neighbor]]:
    """Return ``{anchor_id -> [Neighbor]}`` for the given anchors.

    One hop in each direction, name-resolved against
    ``catalog_entities``, deduped per anchor, capped at ``fanout``
    neighbours per anchor. Reads only local rows; never touches the
    network. Returns ``{}`` when the kill-switch is set or no anchors
    are given.
    """
    if enrichment_disabled() or not anchor_entity_ids:
        return {}
    ids = sorted({int(a) for a in anchor_entity_ids})
    anchor_ph = ",".join("?" for _ in ids)
    rel_ph = ",".join("?" for _ in rel_types)
    rows = conn.execute(
        f"""
        SELECT cr.from_entity_id, cr.to_entity_id, cr.relationship_type,
               nf.entity_kind, nf.schema_name, nf.table_name, nf.search_text,
               nf.metadata_state,
               nt.entity_kind, nt.schema_name, nt.table_name, nt.search_text,
               nt.metadata_state
        FROM catalog_relationships cr
        JOIN catalog_entities nf ON nf.id = cr.from_entity_id
        JOIN catalog_entities nt ON nt.id = cr.to_entity_id
        WHERE cr.relationship_type IN ({rel_ph})
          AND (cr.from_entity_id IN ({anchor_ph})
               OR cr.to_entity_id IN ({anchor_ph}))
        """,  # noqa: S608 — all interpolated fragments are placeholder lists
        (*rel_types, *ids, *ids),
    ).fetchall()

    anchor_set = set(ids)
    out: dict[int, list[Neighbor]] = {a: [] for a in ids}
    seen: dict[int, set[tuple[str, str, int]]] = {a: set() for a in ids}
    for row in rows:
        from_id = int(row[0])
        to_id = int(row[1])
        rel = str(row[2])
        # An edge can touch two anchors at once; record the neighbour
        # from each in-scope endpoint's viewpoint.
        if from_id in anchor_set:
            _add(out, seen, from_id, _neighbor(row, side="to", direction="downstream", rel=rel), fanout)
        if to_id in anchor_set:
            _add(out, seen, to_id, _neighbor(row, side="from", direction="upstream", rel=rel), fanout)
    return out


def _neighbor(row: Any, *, side: str, direction: str, rel: str) -> Neighbor:
    if side == "to":
        kind = str(row[8] or "table")
        name = _entity_name(row[9], row[10], row[11], kind)
        ent_id = int(row[1])
        state = str(row[12] or "full")
    else:
        kind = str(row[3] or "table")
        name = _entity_name(row[4], row[5], row[6], kind)
        ent_id = int(row[0])
        state = str(row[7] or "full")
    return Neighbor(
        direction=direction,
        kind=kind,
        name=name,
        relationship=rel,
        entity_id=ent_id,
        metadata_state=state,
    )


def _add(
    out: dict[int, list[Neighbor]],
    seen: dict[int, set[tuple[str, str, int]]],
    anchor_id: int,
    nb: Neighbor,
    fanout: int,
) -> None:
    if len(out[anchor_id]) >= fanout:
        return
    key = (nb.direction, nb.kind, nb.entity_id)
    if key in seen[anchor_id]:
        return
    seen[anchor_id].add(key)
    out[anchor_id].append(nb)


def _entity_name(schema: Any, table: Any, search_text: Any, kind: str) -> str:
    if kind != "table" and search_text:
        return str(search_text)
    parts = [str(p) for p in (schema, table) if p]
    return ".".join(parts) or str(table or kind)


__all__ = [
    "Neighbor",
    "lineage_neighbors",
    "enrichment_disabled",
    "LINEAGE_REL_TYPES",
    "DEFAULT_FANOUT",
]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/lineage/test_neighbors.py -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add amx/lineage/neighbors.py tests/lineage/test_neighbors.py
git commit -m "feat(lineage): shared one-hop native-lineage neighbour query core"
```

---

## Task 2: Refactor RUN resolver onto the core (behaviour-preserving)

**Files:**
- Modify: `amx/analyze/lineage_context.py`
- Test: `tests/analyze/test_lineage_context_for_run.py` (existing — must stay green)

- [ ] **Step 1: Run the existing test to confirm the current baseline passes**

Run: `pytest tests/analyze/test_lineage_context_for_run.py -v`
Expected: PASS (baseline before refactor)

- [ ] **Step 2: Refactor `resolve_lineage_context_for_run` to call the core**

In `amx/analyze/lineage_context.py`, replace the imports/constants block and the `resolve_lineage_context_for_run` + `_neighbours_for` + `_entity_name` functions. Keep `_anchor_tables`, `_MAX_ANCHOR_TABLES`, and `_MAX_BLOCKS_PER_TABLE`.

Replace the top-of-file constants (the `_LINEAGE_REL_TYPES` tuple) and add the import:

```python
from amx.lineage.neighbors import Neighbor, lineage_neighbors
from amx.utils.logging import get_logger

log = get_logger("analyze.lineage_context")

# Bound the work so a whole-schema run can't fan out unboundedly.
_MAX_ANCHOR_TABLES = 300
_MAX_BLOCKS_PER_TABLE = 12
```

Replace `resolve_lineage_context_for_run` with:

```python
def resolve_lineage_context_for_run(
    *,
    store: Any,
    profile: str,
    scope: dict[str, list[str]] | None = None,
) -> dict[tuple[str, str], list[dict[str, Any]]]:
    """Return ``{(schema, table) -> [lineage block]}`` for a run.

    ``scope`` is the run's schema → tables map (``{}`` / ``None`` means
    every reachable table). Each block is
    ``{"direction", "kind", "name", "relationship"}`` — the neighbour
    as seen from the anchor table. Built on the shared
    :func:`amx.lineage.neighbors.lineage_neighbors` core so RUN and ASK
    share one graph walk.
    """
    out: dict[tuple[str, str], list[dict[str, Any]]] = {}
    if store is None or not profile:
        return out
    with store._connect() as conn:  # noqa: SLF001
        anchors = _anchor_tables(conn, profile, scope)
        if not anchors:
            return out
        id_to_loc = {eid: (s.lower(), t.lower()) for eid, s, t in anchors}
        neighbours = lineage_neighbors(
            conn, anchor_entity_ids=list(id_to_loc), fanout=_MAX_BLOCKS_PER_TABLE
        )
    for anchor_id, nbs in neighbours.items():
        loc = id_to_loc.get(anchor_id)
        if loc and nbs:
            out[loc] = [_block(nb) for nb in nbs]
    return out


def _block(nb: Neighbor) -> dict[str, Any]:
    return {
        "direction": nb.direction,
        "kind": nb.kind,
        "name": nb.name,
        "relationship": nb.relationship,
    }
```

Delete the old `_neighbours_for` and `_entity_name` functions (the core now owns the walk and naming). Leave `_anchor_tables` unchanged.

- [ ] **Step 3: Run the existing test to verify behaviour is preserved**

Run: `pytest tests/analyze/test_lineage_context_for_run.py -v`
Expected: PASS (same assertions, now served by the shared core)

- [ ] **Step 4: Commit**

```bash
git add amx/analyze/lineage_context.py
git commit -m "refactor(lineage): RUN lineage resolver uses shared neighbour core"
```

---

## Task 3: RUN CLI parity — resolve lineage blocks in the CLI run loop

**Files:**
- Modify: `amx/cli_support/commands/_analyze/run_loop.py`
- Test: `tests/analyze/test_run_loop_lineage_blocks.py`

- [ ] **Step 1: Write the failing test**

Create `tests/analyze/test_run_loop_lineage_blocks.py`:

```python
"""The CLI run loop resolves lineage blocks (Studio-parity)."""

from __future__ import annotations

import time
from pathlib import Path
from types import SimpleNamespace

from amx.cli_support.commands._analyze.run_loop import resolve_run_lineage_blocks
from amx.storage.sqlite_store import SQLiteHistoryStore


def _hs(tmp_path: Path) -> SQLiteHistoryStore:
    hs = SQLiteHistoryStore(tmp_path / "history.db")
    hs.init()
    return hs


def _entity(hs, *, schema, table) -> int:
    with hs._connect() as conn:
        cur = conn.execute(
            "INSERT INTO catalog_entities (db_profile, db_backend, database_name, "
            "schema_name, table_name, entity_kind, asset_kind, search_text, metadata_state) "
            "VALUES ('dbr','databricks','wh',?,?,'table','table','','full')",
            (schema, table),
        )
        return int(cur.lastrowid)


def _edge(hs, frm, to, rel) -> None:
    with hs._connect() as conn:
        conn.execute(
            "INSERT INTO catalog_relationships (from_entity_id, to_entity_id, "
            "relationship_type, score, source, details_json, last_seen, "
            "from_entity_kind, to_entity_kind) VALUES (?,?,?,1.0,'native','{}',?,?,?)",
            (frm, to, rel, time.time(), "table", "table"),
        )


def test_resolves_blocks_for_scope(tmp_path: Path) -> None:
    hs = _hs(tmp_path)
    anchor = _entity(hs, schema="sales", table="orders")
    parent = _entity(hs, schema="sales", table="customers")
    _edge(hs, parent, anchor, "lineage_native_table")

    cfg = SimpleNamespace(active_db_profile="dbr")
    blocks = resolve_run_lineage_blocks(
        cfg=cfg, history_store_fn=lambda: hs, scope={"sales": ["orders"]}
    )
    assert ("sales", "orders") in blocks
    assert any(b["name"] == "sales.customers" for b in blocks[("sales", "orders")])


def test_returns_empty_without_profile_or_store(tmp_path: Path) -> None:
    cfg_noprofile = SimpleNamespace(active_db_profile="")
    assert resolve_run_lineage_blocks(
        cfg=cfg_noprofile, history_store_fn=lambda: None, scope={}
    ) == {}
    cfg = SimpleNamespace(active_db_profile="dbr")
    assert resolve_run_lineage_blocks(
        cfg=cfg, history_store_fn=lambda: None, scope={}
    ) == {}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `pytest tests/analyze/test_run_loop_lineage_blocks.py -v`
Expected: FAIL with `ImportError: cannot import name 'resolve_run_lineage_blocks'`

- [ ] **Step 3: Add the helper and wire it into the loop**

In `amx/cli_support/commands/_analyze/run_loop.py`, add the helper function above `run_per_schema_loop` (after the `PerSchemaLoopResult` dataclass):

```python
def resolve_run_lineage_blocks(
    *,
    cfg: Any,
    history_store_fn: Any,
    scope: Any,
) -> dict[tuple[str, str], list[dict[str, Any]]]:
    """Resolve per-table lineage context for the run (Studio-parity).

    Best-effort: returns ``{}`` on any failure, or when no active
    profile / history store is available, so it can never fail a run.
    Mirrors the Studio worker's lineage wiring in
    ``amx/web/routers/runs.py`` so CLI and Studio runs produce the same
    lineage-aware descriptions.
    """
    profile = getattr(cfg, "active_db_profile", "") or ""
    if not profile:
        return {}
    store = history_store_fn() if history_store_fn else None
    if store is None:
        return {}
    scope_map = {str(k): list(v) for k, v in (scope or {}).items()}
    try:
        from amx.analyze.lineage_context import resolve_lineage_context_for_run

        return resolve_lineage_context_for_run(store=store, profile=profile, scope=scope_map)
    except Exception:  # noqa: BLE001 — context is best-effort, never fail a run
        log.debug("CLI run lineage context resolution failed", exc_info=True)
        return {}
```

Then inside `run_per_schema_loop`, resolve the blocks once before the per-schema loop. Insert immediately after the `rag_llm = ...` line (currently line 82), before `for schema_name, assets in scope.items():`:

```python
    # Lineage-neighbour context (parity with the Studio run path):
    # resolved once for the whole scope and attached to every per-schema
    # orchestrator below. Best-effort — never fails the run.
    lineage_blocks = resolve_run_lineage_blocks(
        cfg=cfg, history_store_fn=history_store_fn, scope=scope
    )
```

Then attach it to each orchestrator. Inside the loop, immediately after the `orch = Orchestrator(...)` construction (currently ends at line 95) and before the `dedup_outcome` block, add:

```python
        if lineage_blocks:
            orch.lineage_context_by_table = lineage_blocks
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `pytest tests/analyze/test_run_loop_lineage_blocks.py -v`
Expected: PASS (2 tests)

- [ ] **Step 5: Run the surrounding run-loop tests to confirm no regression**

Run: `pytest tests/analyze/ -v -k "run_loop or analyze_flow"`
Expected: PASS (no regressions)

- [ ] **Step 6: Commit**

```bash
git add amx/cli_support/commands/_analyze/run_loop.py tests/analyze/test_run_loop_lineage_blocks.py
git commit -m "feat(analyze): CLI run resolves lineage context (Studio parity)"
```

---

## Task 4: RUN neighbour-description enrichment

**Files:**
- Modify: `amx/analyze/lineage_context.py`
- Modify: `amx/agents/profile_agent.py`
- Test: `tests/analyze/test_lineage_context_descriptions.py`
- Test: `tests/agents/test_profile_agent_lineage_detail.py`

- [ ] **Step 1: Write the failing resolver test**

Create `tests/analyze/test_lineage_context_descriptions.py`:

```python
"""RUN lineage blocks carry a truncated neighbour description."""

from __future__ import annotations

import time
from pathlib import Path

from amx.analyze.lineage_context import resolve_lineage_context_for_run
from amx.storage.sqlite_store import SQLiteHistoryStore


def _hs(tmp_path: Path) -> SQLiteHistoryStore:
    hs = SQLiteHistoryStore(tmp_path / "history.db")
    hs.init()
    return hs


def _entity(hs, *, schema, table) -> int:
    with hs._connect() as conn:
        cur = conn.execute(
            "INSERT INTO catalog_entities (db_profile, db_backend, database_name, "
            "schema_name, table_name, entity_kind, asset_kind, search_text, metadata_state) "
            "VALUES ('dbr','databricks','wh',?,?,'table','table','','full')",
            (schema, table),
        )
        return int(cur.lastrowid)


def _describe(hs, entity_id: int, text: str) -> None:
    with hs._connect() as conn:
        cur = conn.execute(
            "INSERT INTO catalog_descriptions (entity_id, description_text, source_kind, "
            "created_at) VALUES (?,?,?,?)",
            (entity_id, text, "agent", time.time()),
        )
        desc_id = int(cur.lastrowid)
        conn.execute(
            "UPDATE catalog_entities SET effective_description_id = ? WHERE id = ?",
            (desc_id, entity_id),
        )


def _edge(hs, frm, to, rel) -> None:
    with hs._connect() as conn:
        conn.execute(
            "INSERT INTO catalog_relationships (from_entity_id, to_entity_id, "
            "relationship_type, score, source, details_json, last_seen, "
            "from_entity_kind, to_entity_kind) VALUES (?,?,?,1.0,'native','{}',?,?,?)",
            (frm, to, rel, time.time(), "table", "table"),
        )


def test_block_includes_neighbour_description(tmp_path: Path) -> None:
    hs = _hs(tmp_path)
    anchor = _entity(hs, schema="sales", table="orders")
    parent = _entity(hs, schema="sales", table="customers")
    _describe(hs, parent, "Master list of customers, one row per account.")
    _edge(hs, parent, anchor, "lineage_native_table")

    out = resolve_lineage_context_for_run(store=hs, profile="dbr", scope={})
    block = next(b for b in out[("sales", "orders")] if b["name"] == "sales.customers")
    assert block["detail"].startswith("Master list of customers")


def test_block_without_description_has_no_detail(tmp_path: Path) -> None:
    hs = _hs(tmp_path)
    anchor = _entity(hs, schema="sales", table="orders")
    parent = _entity(hs, schema="sales", table="customers")
    _edge(hs, parent, anchor, "lineage_native_table")

    out = resolve_lineage_context_for_run(store=hs, profile="dbr", scope={})
    block = next(b for b in out[("sales", "orders")] if b["name"] == "sales.customers")
    assert "detail" not in block
```

- [ ] **Step 2: Run to verify it fails**

Run: `pytest tests/analyze/test_lineage_context_descriptions.py -v`
Expected: FAIL — `KeyError: 'detail'` (the block has no `detail` yet)

- [ ] **Step 3: Add description lookup to the resolver**

In `amx/analyze/lineage_context.py`, add a module constant and a `_descriptions_for` helper, and thread descriptions through `resolve_lineage_context_for_run` + `_block`.

Add near the other constants:

```python
# Truncate a neighbour's description so a fanned-out block list stays
# within the ProfileAgent prompt budget.
_MAX_DETAIL_CHARS = 200
```

Update the body of `resolve_lineage_context_for_run` — replace the `with store._connect()` block and the final loop with:

```python
    with store._connect() as conn:  # noqa: SLF001
        anchors = _anchor_tables(conn, profile, scope)
        if not anchors:
            return out
        id_to_loc = {eid: (s.lower(), t.lower()) for eid, s, t in anchors}
        neighbours = lineage_neighbors(
            conn, anchor_entity_ids=list(id_to_loc), fanout=_MAX_BLOCKS_PER_TABLE
        )
        neighbour_ids = {nb.entity_id for nbs in neighbours.values() for nb in nbs}
        descriptions = _descriptions_for(conn, neighbour_ids)
    for anchor_id, nbs in neighbours.items():
        loc = id_to_loc.get(anchor_id)
        if loc and nbs:
            out[loc] = [_block(nb, descriptions) for nb in nbs]
    return out
```

Update `_block` to accept descriptions:

```python
def _block(nb: Neighbor, descriptions: dict[int, str]) -> dict[str, Any]:
    block: dict[str, Any] = {
        "direction": nb.direction,
        "kind": nb.kind,
        "name": nb.name,
        "relationship": nb.relationship,
    }
    desc = descriptions.get(nb.entity_id)
    if desc:
        block["detail"] = desc[:_MAX_DETAIL_CHARS].rstrip()
    return block
```

Add the lookup helper:

```python
def _descriptions_for(conn: Any, entity_ids: set[int]) -> dict[int, str]:
    """Map ``entity_id -> effective description text`` for the ids given.

    Reads the catalog's chosen description via
    ``catalog_entities.effective_description_id``; entities without one
    are simply absent from the result. Tables and assets are treated
    the same — any entity with a generated description contributes one.
    """
    ids = sorted(int(e) for e in entity_ids)
    if not ids:
        return {}
    ph = ",".join("?" for _ in ids)
    rows = conn.execute(
        f"""
        SELECT ce.id, cd.description_text
        FROM catalog_entities ce
        JOIN catalog_descriptions cd ON cd.id = ce.effective_description_id
        WHERE ce.id IN ({ph})
        """,  # noqa: S608 — ids are integer placeholders
        tuple(ids),
    ).fetchall()
    return {int(r[0]): str(r[1]) for r in rows if r[1]}
```

- [ ] **Step 4: Run to verify the resolver test passes**

Run: `pytest tests/analyze/test_lineage_context_descriptions.py tests/analyze/test_lineage_context_for_run.py -v`
Expected: PASS (existing parity test still green; both new tests pass)

- [ ] **Step 5: Write the failing ProfileAgent rendering test**

Create `tests/agents/test_profile_agent_lineage_detail.py`:

```python
"""ProfileAgent renders the optional lineage-block ``detail`` field."""

from __future__ import annotations

from amx.agents.profile_agent import _render_lineage_section


def test_renders_detail_when_present() -> None:
    blocks = [
        {
            "direction": "upstream",
            "kind": "table",
            "name": "sales.customers",
            "relationship": "lineage_native_table",
            "detail": "Master list of customers.",
        }
    ]
    text = "\n".join(_render_lineage_section(blocks))
    assert "sales.customers" in text
    assert "Master list of customers." in text


def test_omits_detail_when_absent() -> None:
    blocks = [
        {
            "direction": "downstream",
            "kind": "notebook",
            "name": "ETL nb",
            "relationship": "lineage_native_asset",
        }
    ]
    text = "\n".join(_render_lineage_section(blocks))
    assert "ETL nb" in text
    assert "—" not in text  # no trailing detail separator
```

- [ ] **Step 6: Run to verify it fails**

Run: `pytest tests/agents/test_profile_agent_lineage_detail.py -v`
Expected: FAIL with `ImportError: cannot import name '_render_lineage_section'`

- [ ] **Step 7: Extract and extend the rendering in ProfileAgent**

In `amx/agents/profile_agent.py`, extract the existing inline lineage rendering (currently at lines 905-918) into a module-level helper and call it. Replace the inline block:

```python
        if ctx.lineage_context:
            lines.append("")
            lines.append(
                "Lineage context (upstream producers feed this table; "
                "downstream consumers read from it — use these relationships "
                "to describe the table's role in the data flow):"
            )
            for block in ctx.lineage_context:
                direction = str(block.get("direction") or "")
                kind = str(block.get("kind") or "table")
                name = str(block.get("name") or "")
                rel = str(block.get("relationship") or "")
                arrow = "←" if direction == "upstream" else "→"
                lines.append(f"  {arrow} [{direction} {kind}] {name} ({rel})")
```

with:

```python
        if ctx.lineage_context:
            lines.extend(_render_lineage_section(ctx.lineage_context))
```

Add the module-level helper (place it near the other module-level helpers in the file):

```python
def _render_lineage_section(blocks: list[dict[str, Any]]) -> list[str]:
    """Render lineage-neighbour blocks as ProfileAgent prompt lines.

    Each block is ``{direction, kind, name, relationship}`` plus an
    optional ``detail`` (a truncated description of the neighbour). The
    arrow marks data-flow direction relative to the table being
    described.
    """
    lines: list[str] = [
        "",
        "Lineage context (upstream producers feed this table; "
        "downstream consumers read from it — use these relationships "
        "to describe the table's role in the data flow):",
    ]
    for block in blocks:
        direction = str(block.get("direction") or "")
        kind = str(block.get("kind") or "table")
        name = str(block.get("name") or "")
        rel = str(block.get("relationship") or "")
        arrow = "←" if direction == "upstream" else "→"
        line = f"  {arrow} [{direction} {kind}] {name} ({rel})"
        detail = str(block.get("detail") or "").strip()
        if detail:
            line += f" — {detail}"
        lines.append(line)
    return lines
```

Confirm `Any` is imported at the top of `profile_agent.py` (it is used throughout); if the file imports `from typing import Any`, no change is needed.

- [ ] **Step 8: Run to verify the rendering test passes**

Run: `pytest tests/agents/test_profile_agent_lineage_detail.py -v`
Expected: PASS (2 tests)

- [ ] **Step 9: Run the ProfileAgent test module to confirm no regression**

Run: `pytest tests/agents/ -v -k profile_agent`
Expected: PASS (no regressions)

- [ ] **Step 10: Commit**

```bash
git add amx/analyze/lineage_context.py amx/agents/profile_agent.py tests/analyze/test_lineage_context_descriptions.py tests/agents/test_profile_agent_lineage_detail.py
git commit -m "feat(analyze): RUN lineage blocks carry neighbour descriptions"
```

---

## Task 5: ASK canvas-free, name-resolved neighbours

**Files:**
- Modify: `amx/lineage/evidence.py`
- Modify: `amx/search/_agent/retrieval.py`
- Test: `tests/lineage/test_native_neighbors_evidence.py`
- Test: `tests/search/test_retrieval_lineage_pages.py` (existing — must stay green)

- [ ] **Step 1: Write the failing evidence test**

Create `tests/lineage/test_native_neighbors_evidence.py`:

```python
"""Canvas-free, name-resolved native-lineage neighbours for ASK."""

from __future__ import annotations

import time
from pathlib import Path

from amx.lineage.evidence import NativeNeighbors, build_native_lineage_neighbors
from amx.storage.sqlite_store import SQLiteHistoryStore


def _hs(tmp_path: Path) -> SQLiteHistoryStore:
    hs = SQLiteHistoryStore(tmp_path / "history.db")
    hs.init()
    return hs


def _entity(hs, *, schema, table, kind="table", search_text="") -> int:
    with hs._connect() as conn:
        cur = conn.execute(
            "INSERT INTO catalog_entities (db_profile, db_backend, database_name, "
            "schema_name, table_name, entity_kind, asset_kind, search_text, metadata_state) "
            "VALUES ('dbr','databricks','wh',?,?,?,?,?,'full')",
            (schema, table, kind, kind, search_text),
        )
        return int(cur.lastrowid)


def _edge(hs, frm, to, rel) -> None:
    with hs._connect() as conn:
        conn.execute(
            "INSERT INTO catalog_relationships (from_entity_id, to_entity_id, "
            "relationship_type, score, source, details_json, last_seen, "
            "from_entity_kind, to_entity_kind) VALUES (?,?,?,1.0,'native','{}',?,?,?)",
            (frm, to, rel, time.time(), "table", "table"),
        )


def test_named_neighbours_without_any_canvas(tmp_path: Path) -> None:
    hs = _hs(tmp_path)
    anchor = _entity(hs, schema="sales", table="orders")
    parent = _entity(hs, schema="sales", table="customers")
    nb = _entity(hs, schema="__assets", table="nb#1", kind="notebook", search_text="ETL nb")
    _edge(hs, parent, anchor, "lineage_native_table")
    _edge(hs, anchor, nb, "lineage_native_asset")

    out = build_native_lineage_neighbors(store=hs, entity_ids=[anchor])
    assert isinstance(out, NativeNeighbors)
    assert out.has_neighbors
    assert {r["name"] for r in out.upstream} == {"sales.customers"}
    assert {r["name"] for r in out.downstream} == {"ETL nb"}


def test_off_switch_empty_filter(tmp_path: Path) -> None:
    hs = _hs(tmp_path)
    anchor = _entity(hs, schema="s", table="a")
    parent = _entity(hs, schema="s", table="b")
    _edge(hs, parent, anchor, "foreign_key")
    out = build_native_lineage_neighbors(store=hs, entity_ids=[anchor], artifact_filter=[])
    assert out.has_neighbors is False
```

- [ ] **Step 2: Run to verify it fails**

Run: `pytest tests/lineage/test_native_neighbors_evidence.py -v`
Expected: FAIL with `ImportError: cannot import name 'NativeNeighbors'`

- [ ] **Step 3: Add the canvas-free helper to evidence.py**

In `amx/lineage/evidence.py`, add the new dataclass and function (leave `LineageEvidence` and `build_lineage_evidence` untouched). Add the import at the top:

```python
from amx.lineage.neighbors import lineage_neighbors
```

Add after `build_lineage_evidence`:

```python
@dataclass(slots=True)
class NativeNeighbors:
    """Name-resolved, canvas-free lineage neighbours for ASK retrieval."""

    upstream: list[dict[str, str]] = field(default_factory=list)
    downstream: list[dict[str, str]] = field(default_factory=list)

    @property
    def has_neighbors(self) -> bool:
        return bool(self.upstream or self.downstream)


def build_native_lineage_neighbors(
    *,
    store: SQLiteHistoryStore,
    entity_ids: Iterable[int],
    artifact_filter: list[str] | None = None,
    max_each: int = 5,
) -> NativeNeighbors:
    """Return named upstream/downstream neighbours, no saved canvas needed.

    Reads ``catalog_relationships`` directly through the shared
    :func:`amx.lineage.neighbors.lineage_neighbors` core, so freshly
    fetched native lineage informs ASK answers immediately. ``[]`` for
    ``artifact_filter`` is the off-switch (mirrors
    :func:`build_lineage_evidence`).
    """
    if artifact_filter == []:
        return NativeNeighbors()
    ent_set = {int(e) for e in entity_ids}
    if not ent_set:
        return NativeNeighbors()
    with store._connect() as conn:  # noqa: SLF001
        neighbours = lineage_neighbors(conn, anchor_entity_ids=list(ent_set))
    up: list[dict[str, str]] = []
    down: list[dict[str, str]] = []
    for nbs in neighbours.values():
        for nb in nbs:
            rec = {"name": nb.name, "kind": nb.kind, "relationship": nb.relationship}
            (up if nb.direction == "upstream" else down).append(rec)
    out = NativeNeighbors()
    out.upstream = _dedup_dicts(up)[:max_each]
    out.downstream = _dedup_dicts(down)[:max_each]
    return out


def _dedup_dicts(xs: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[tuple[str, str, str]] = set()
    out: list[dict[str, str]] = []
    for x in xs:
        key = (x.get("name", ""), x.get("kind", ""), x.get("relationship", ""))
        if key not in seen:
            seen.add(key)
            out.append(x)
    return out
```

- [ ] **Step 4: Run to verify the evidence test passes**

Run: `pytest tests/lineage/test_native_neighbors_evidence.py tests/lineage/test_evidence.py -v`
Expected: PASS (new tests pass; existing `build_lineage_evidence` tests still green)

- [ ] **Step 5: Write the failing retrieval-wiring test**

Add this test to `tests/search/test_retrieval_lineage_pages.py` (append a new function; reuse the file's existing `_seed_minimal` / helpers and imports):

```python
def test_retrieval_includes_named_neighbours_without_canvas(tmp_path: Path) -> None:
    store = SQLiteHistoryStore(tmp_path / "history.db")
    store.init()
    anchor = _seed_minimal(store)

    # A native upstream edge with NO saved canvas at all.
    with store._connect() as conn:
        cur = conn.execute(
            "INSERT INTO catalog_entities (db_profile, db_backend, database_name, "
            "schema_name, table_name, entity_kind, asset_kind, search_text, metadata_state) "
            "VALUES ('p1','postgresql','db','s','suppliers','table','table','','full')"
        )
        parent = int(cur.lastrowid)
        conn.execute(
            "INSERT INTO catalog_relationships (from_entity_id, to_entity_id, "
            "relationship_type, score, source, details_json, last_seen, "
            "from_entity_kind, to_entity_kind) "
            "VALUES (?,?,?,1.0,'native','{}',?, 'table','table')",
            (parent, anchor, "lineage_native_table", __import__("time").time()),
        )

    rows = [{"id": anchor, "row_type": "table", "db_profile": "p1",
             "schema_name": "s", "table_name": "customers"}]
    enriched = enrich_retrieval_details_with_lineage_and_pages(
        store=store,
        rows=rows,
        retrieval_details={"evidence_sources": []},
        question="where does the customers table come from?",
        plan=_make_fake_plan(),
        lineage_profiles=None,
        pages_enabled=None,
    )
    lineage = enriched.get("lineage") or {}
    assert "lineage" in (enriched.get("evidence_sources") or [])
    names = {r["name"] for r in (lineage.get("upstream") or [])}
    assert "s.suppliers" in names
```

- [ ] **Step 6: Run to verify it fails**

Run: `pytest tests/search/test_retrieval_lineage_pages.py::test_retrieval_includes_named_neighbours_without_canvas -v`
Expected: FAIL — `lineage` block missing or has no `upstream` names (no canvas exists, so the current code emits no lineage block)

- [ ] **Step 7: Wire named neighbours into the retrieval enricher**

In `amx/search/_agent/retrieval.py`, in the module function `enrich_retrieval_details_with_lineage_and_pages`, find the `build_lineage_evidence` call site (around lines 216-237). Replace that block with:

```python
    from amx.lineage.evidence import build_lineage_evidence, build_native_lineage_neighbors

    lineage_payload = build_lineage_evidence(
        store=store,
        entity_ids=entity_ids,
        artifact_filter=lineage_profiles,
        max_upstream=5,
        max_downstream=5,
        max_comments=3,
    )
    # Canvas-free named neighbours: always available once /lineage fetch
    # has materialized native edges, even with no saved canvas. This is
    # the always-on path that replaces the raw entity-id rendering.
    named = build_native_lineage_neighbors(
        store=store, entity_ids=entity_ids, artifact_filter=lineage_profiles
    )
    if not lineage_payload.is_empty or named.has_neighbors:
        retrieval_details.setdefault("evidence_sources", [])
        if "lineage" not in retrieval_details["evidence_sources"]:
            retrieval_details["evidence_sources"].append("lineage")
        retrieval_details["lineage"] = {
            "kind": "lineage",
            "artifact_names": list(lineage_payload.artifact_names),
            "upstream": list(named.upstream),
            "downstream": list(named.downstream),
            "upstream_entity_ids": list(lineage_payload.upstream_entity_ids),
            "downstream_entity_ids": list(lineage_payload.downstream_entity_ids),
            "external_systems": list(lineage_payload.logo_keys),
            "comments": list(lineage_payload.comments),
        }
```

- [ ] **Step 8: Run to verify the retrieval tests pass**

Run: `pytest tests/search/test_retrieval_lineage_pages.py -v`
Expected: PASS (the existing `test_retrieval_emits_lineage_and_pages_sources_when_data_exists` still asserts ids 20/30 — kept — plus the new named-neighbours test passes)

- [ ] **Step 9: Commit**

```bash
git add amx/lineage/evidence.py amx/search/_agent/retrieval.py tests/lineage/test_native_neighbors_evidence.py tests/search/test_retrieval_lineage_pages.py
git commit -m "feat(search): ASK retrieval surfaces named canvas-free lineage neighbours"
```

---

## Task 6: ASK appendix renders neighbour names

**Files:**
- Modify: `amx/search/tool_agent.py`
- Test: `tests/search/test_tool_agent_appendix.py`

- [ ] **Step 1: Write the failing test**

Create `tests/search/test_tool_agent_appendix.py`:

```python
"""The lineage appendix renders neighbour names, not raw entity ids."""

from __future__ import annotations

from amx.search.tool_agent import _format_lineage_pages_appendix


def test_appendix_renders_names_not_ids() -> None:
    lineage = {
        "kind": "lineage",
        "artifact_names": ["orders-canvas"],
        "upstream": [
            {"name": "sales.customers", "kind": "table", "relationship": "lineage_native_table"}
        ],
        "downstream": [
            {"name": "ETL nb", "kind": "notebook", "relationship": "lineage_native_asset"}
        ],
        "upstream_entity_ids": [20],
        "downstream_entity_ids": [30],
        "external_systems": ["databricks"],
        "comments": [],
    }
    text = _format_lineage_pages_appendix(lineage, None)
    assert "sales.customers" in text
    assert "ETL nb" in text
    assert "orders-canvas" in text
    assert "databricks" in text
    # Raw entity-id lines must be gone.
    assert "entity ids" not in text
    assert "20" not in text and "30" not in text


def test_appendix_handles_missing_names_gracefully() -> None:
    # No upstream/downstream name lists (e.g. canvas-only data) — header
    # + canvas still render, no crash.
    lineage = {"kind": "lineage", "artifact_names": ["c"], "comments": []}
    text = _format_lineage_pages_appendix(lineage, None)
    assert "Lineage evidence" in text
    assert "c" in text
```

- [ ] **Step 2: Run to verify it fails**

Run: `pytest tests/search/test_tool_agent_appendix.py -v`
Expected: FAIL — names absent / `"20" in text` still true (current code prints entity ids)

- [ ] **Step 3: Rewrite the lineage half of `_format_lineage_pages_appendix`**

In `amx/search/tool_agent.py`, replace the lineage branch of `_format_lineage_pages_appendix` (currently lines ~382-399, the block starting `if lineage:` through the comments loop) with:

```python
    if lineage:
        artifact_names = lineage.get("artifact_names") or []
        upstream = lineage.get("upstream") or []
        downstream = lineage.get("downstream") or []
        external = lineage.get("external_systems") or []
        lines.append(
            "Lineage evidence (from the database's own lineage graph and "
            "any saved canvases anchored to these tables):"
        )
        if artifact_names:
            lines.append(f"  Canvases: {', '.join(str(n) for n in artifact_names)}")
        if upstream:
            lines.append("  Upstream (feeds these tables):")
            for rec in upstream:
                name = str(rec.get("name") or "")
                kind = str(rec.get("kind") or "table")
                rel = str(rec.get("relationship") or "")
                lines.append(f"    ← {name} [{kind}] ({rel})")
        if downstream:
            lines.append("  Downstream (reads from these tables):")
            for rec in downstream:
                name = str(rec.get("name") or "")
                kind = str(rec.get("kind") or "table")
                rel = str(rec.get("relationship") or "")
                lines.append(f"    → {name} [{kind}] ({rel})")
        if external:
            lines.append(f"  External systems: {', '.join(str(n) for n in external)}")
        comments = lineage.get("comments") or []
        for comment in comments[:3]:
            text = str(comment).strip()
            if text:
                lines.append(f"  Note: {text[:180]}")
```

Leave the `if pages:` branch and the final `return "\n".join(lines).rstrip()` unchanged.

- [ ] **Step 4: Run to verify the appendix test passes**

Run: `pytest tests/search/test_tool_agent_appendix.py -v`
Expected: PASS (2 tests)

- [ ] **Step 5: Run the existing tool-agent enrichment tests to confirm no regression**

Run: `pytest tests/search/test_tool_agent_enrichment.py -v`
Expected: PASS — its assertions (`"Lineage evidence" in ...`, `"customers-canvas" in ...`) still hold; the fake enrich payload has no `upstream`/`downstream` keys, so only the header + canvas line render.

- [ ] **Step 6: Commit**

```bash
git add amx/search/tool_agent.py tests/search/test_tool_agent_appendix.py
git commit -m "feat(search): ASK lineage appendix renders neighbour names"
```

---

## Task 7: Verification gate (full suites + performance sanity)

**Files:**
- Test: `tests/lineage/test_neighbors_perf.py`

- [ ] **Step 1: Write a performance-sanity test**

Create `tests/lineage/test_neighbors_perf.py`:

```python
"""Sanity bound: the shared core stays fast on a wide graph.

Not a micro-benchmark — a generous ceiling that catches accidental
N+1 / O(n^2) regressions without being timing-flaky.
"""

from __future__ import annotations

import time
from pathlib import Path

from amx.lineage.neighbors import lineage_neighbors
from amx.storage.sqlite_store import SQLiteHistoryStore


def test_wide_graph_under_generous_bound(tmp_path: Path) -> None:
    hs = SQLiteHistoryStore(tmp_path / "history.db")
    hs.init()
    with hs._connect() as conn:
        cur = conn.execute(
            "INSERT INTO catalog_entities (db_profile, db_backend, database_name, "
            "schema_name, table_name, entity_kind, asset_kind, search_text, metadata_state) "
            "VALUES ('dbr','databricks','wh','s','anchor','table','table','','full')"
        )
        anchor = int(cur.lastrowid)
        now = time.time()
        for i in range(200):
            cur = conn.execute(
                "INSERT INTO catalog_entities (db_profile, db_backend, database_name, "
                "schema_name, table_name, entity_kind, asset_kind, search_text, metadata_state) "
                "VALUES ('dbr','databricks','wh','s',?,'table','table','','full')",
                (f"p{i}",),
            )
            pid = int(cur.lastrowid)
            conn.execute(
                "INSERT INTO catalog_relationships (from_entity_id, to_entity_id, "
                "relationship_type, score, source, details_json, last_seen, "
                "from_entity_kind, to_entity_kind) VALUES (?,?,?,1.0,'native','{}',?, 'table','table')",
                (pid, anchor, "lineage_native_table", now),
            )

    with hs._connect() as conn:
        start = time.perf_counter()
        out = lineage_neighbors(conn, anchor_entity_ids=[anchor])
        elapsed = time.perf_counter() - start

    # Capped output (DEFAULT_FANOUT) and a comfortable time ceiling.
    assert len(out[anchor]) == 12
    assert elapsed < 0.5
```

- [ ] **Step 2: Run the performance-sanity test**

Run: `pytest tests/lineage/test_neighbors_perf.py -v`
Expected: PASS

- [ ] **Step 3: Run all touched test areas together**

Run:
```bash
pytest tests/lineage/ tests/analyze/ tests/agents/ tests/search/ -q
```
Expected: PASS for every test this branch created or touched. (Preexisting unrelated failures on `main`, if any, are out of scope — note them but do not fix here.)

- [ ] **Step 4: Lint and type-check the touched files**

Run:
```bash
ruff check amx/lineage/neighbors.py amx/analyze/lineage_context.py amx/agents/profile_agent.py amx/cli_support/commands/_analyze/run_loop.py amx/lineage/evidence.py amx/search/_agent/retrieval.py amx/search/tool_agent.py
ruff format --check amx/lineage/neighbors.py amx/analyze/lineage_context.py amx/lineage/evidence.py
```
Expected: no lint errors on the touched files; fix any that appear.

- [ ] **Step 5: Manual no-regression check (documented, run by the implementer)**

Confirm, by inspection, that no enrichment path added an LLM call:
- RUN: lineage context is assembled in `resolve_run_lineage_blocks` / `resolve_lineage_context_for_run` (pure SQLite) before the single ProfileAgent call — no extra round-trip.
- ASK: `enrich_retrieval_details_with_lineage_and_pages` is deterministic SQLite + string assembly — no LLM call; the tool path is unchanged.

Then run a real non-lineage `/ask` question with `amx-dev` before and after this branch and confirm the token count is not materially higher (target: equal or lower, since named lineage reduces lineage-tool calls). Record the two numbers in the PR description.

- [ ] **Step 6: Commit**

```bash
git add tests/lineage/test_neighbors_perf.py
git commit -m "test(lineage): performance-sanity bound for the neighbour core"
```

---

## Deployment

This change is Studio-visible (ASK answers and `/analyze run` surface in Studio). Per the standing deploy order for Studio-visible work: run the deploy script (`deploy.sh`) first, then open the PR, then merge. Confirm the order with the user before executing the deployment step.

## Notes / Out of Scope (do not implement here)

- Multi-hop traversal (depth ≥ 2). The core is anchor-id based so it can be added later behind the measured toggle; not built in this plan.
- Re-enabling column-level lineage (disabled upstream, unverified REST shape).
- Auto-linking lineage-discovered ingested assets into the user-attached `asset_context` body-excerpt path (separate follow-up).
- Live Databricks fetch in the RUN/ASK hot path (never — local materialized data only).
