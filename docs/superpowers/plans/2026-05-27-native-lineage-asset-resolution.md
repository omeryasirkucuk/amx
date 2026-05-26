# Native Lineage Asset Resolution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Databricks native-lineage notebook/job nodes show real names, be clickable to ingest-and-open inside AMX Assets (lazily, cached), and give every Databricks node a working "open in Databricks" deep-link.

**Architecture:** Resolve display names eagerly and cheaply at fetch time (per-node REST, no workspace scan); record assets as clickable name-only nodes carrying their `external_id`; ingest one asset's content only when the user clicks it, reusing `IngestAssetsService` with a single-id `selection`, and cache the result in `remote_*`. The header logo/asset node gains a deep-link built from the profile host.

**Tech Stack:** Python (FastAPI, sqlite3, pytest), TypeScript/React (reactflow, vitest).

---

## Spec

`docs/superpowers/specs/2026-05-27-native-lineage-asset-resolution-design.md`

## File Structure

**Backend (modify):**
- `amx/db/adapters/_databricks_workspace.py` — notebook id→name via `path_for_object_id`.
- `amx/lineage/native/service.py` — drop the workspace-scan name pass.
- `amx/web/routers/lineage.py` — surface `external_id`+`host` per node; add the lazy-ingest endpoint.

**Backend (create):**
- `amx/lineage/native/lazy_ingest.py` — single-asset on-demand ingest.

**Backend (delete):**
- `amx/lineage/native/workspace_index.py` — the 40s full-workspace scan (replaced by per-node resolution).

**Frontend (create):**
- `frontend/src/lineage-canvas/logos/databricksDeepLink.ts` — URL builder + test.

**Frontend (modify):**
- `frontend/src/lineage-canvas/types.ts` — `LoadedNode` + `AssetNodeData` fields.
- `frontend/src/lineage-canvas/amx-bridge/payload.ts` — map new fields into node data.
- `frontend/src/lineage-canvas/nodes/AssetNode.tsx` — lazy-ingest click + deep-link.
- `frontend/src/lineage-canvas/nodes/DataFrameNode.tsx` — table logo → deep-link.

**Tests:**
- `tests/lineage/test_native_lineage.py` (existing) — name resolution + no-eager-ingest.
- `tests/lineage/test_lazy_ingest.py` (new) — selection request mapping.
- `tests/web/test_lineage_router.py` (existing) — lazy-ingest endpoint + node fields.
- `frontend/src/lineage-canvas/logos/databricksDeepLink.test.ts` (new).

---

## Phase 0 — Verify the notebook id→path endpoint

- [ ] **Step 1: Confirm `get-status?object_id` works against a live workspace**

This is the one external unknown the whole notebook path depends on. With a real
Databricks profile configured, run a quick Python check (replace host/token/id):

```python
from amx.db.adapters._databricks_workspace import DatabricksWorkspaceClient
c = DatabricksWorkspaceClient(host="<host>", token="<token>")
print(c.path_for_object_id("<a notebook object_id from the lineage graph>"))
```

Expected: prints the notebook workspace path (e.g. `/Users/me/Folder/My Notebook`).

- If it prints a path → continue with this plan unchanged.
- If it errors (400/404) → the endpoint does not accept `object_id`. STOP and
  switch to the persisted-index fallback in the spec (build an `object_id→path`
  map once, persist to SQLite, refresh in the background) before Task 1. Flag
  this to the requester; do not reintroduce the 40s in-fetch budget.

---

## Phase 1 — Real notebook names, no workspace scan (problems 1 & 2a)

### Task 1: Resolve notebook id → name via `path_for_object_id`

**Files:**
- Modify: `amx/db/adapters/_databricks_workspace.py:124-130`
- Test: `tests/lineage/test_native_lineage.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/lineage/test_native_lineage.py`:

```python
def test_resolve_notebook_name_uses_object_id_path(monkeypatch):
    """notebook id resolves to the path basename via get-status?object_id."""
    from amx.db.adapters._databricks_workspace import DatabricksWorkspaceClient

    client = DatabricksWorkspaceClient(host="example.cloud.databricks.com", token="t")

    class _Resp:
        def json(self):
            return {"path": "/Users/me/Folder/My Notebook"}

    seen = {}

    def fake_get(path, *, params=None):
        seen["path"] = path
        seen["params"] = params
        return _Resp()

    monkeypatch.setattr(client, "_get", fake_get)
    name = client.resolve_entity_name(kind="notebook", external_id="2257615622929527")
    assert name == "My Notebook"
    assert seen["params"] == {"object_id": "2257615622929527"}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/lineage/test_native_lineage.py::test_resolve_notebook_name_uses_object_id_path -v`
Expected: FAIL — current notebook branch returns `None`.

- [ ] **Step 3: Implement notebook resolution**

In `_databricks_workspace.py`, replace the notebook branch in `resolve_entity_name`:

```python
            if kind == "notebook":
                # The lineage notebook id is the workspace object_id.
                # Resolve it to a path and take the basename — one cheap
                # REST get per notebook in this graph, no workspace scan.
                path = self.path_for_object_id(external_id)
                return path.rstrip("/").rsplit("/", 1)[-1] if path else None
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/lineage/test_native_lineage.py::test_resolve_notebook_name_uses_object_id_path -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add amx/db/adapters/_databricks_workspace.py tests/lineage/test_native_lineage.py
git commit -m "feat(lineage): resolve Databricks notebook id to name via get-status object_id"
```

### Task 2: Remove the full-workspace scan from the fetch path

**Files:**
- Modify: `amx/lineage/native/service.py:60-81`
- Delete: `amx/lineage/native/workspace_index.py`
- Test: `tests/lineage/test_native_lineage.py`

- [ ] **Step 1: Write the failing test**

Add a test asserting a fetch resolves notebook names without any workspace scan.
Use the existing `_FakeClient` style; give it a `path_for_object_id` and a
`table_lineage` response carrying a `notebookInfos` entry. Assert the notebook
node's name equals the basename and that no `list_workspace_objects` is called.

```python
def test_fetch_resolves_notebook_without_workspace_scan():
    table_resp = {
        "upstreams": [
            {"notebookInfos": [{"notebook_id": "2257615622929527"}]}
        ],
        "downstreams": [],
    }

    class _C:
        host = "example.cloud.databricks.com"

        def table_lineage(self, *, table_name, include_entity_lineage=True):
            return table_resp

        def resolve_entity_name(self, *, kind, external_id):
            assert kind == "notebook"
            return "My Notebook"  # provider path-resolves; mocked here

        def list_workspace_objects(self, *, path="/"):
            raise AssertionError("workspace scan must not run during fetch")

    prov = DatabricksLineageProvider(_C())
    result = prov.fetch_table_lineage("c.s.t", with_columns=False)
    names = {n.name for e in result.edges for n in (e.source, e.target)}
    assert "My Notebook" in names
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/lineage/test_native_lineage.py::test_fetch_resolves_notebook_without_workspace_scan -v`
Expected: PASS already for the provider (it never scans) — but Step 3 removes the
service-level scan that the spec targets. If it passes here, proceed to remove the
dead scan path so the service no longer imports it.

- [ ] **Step 3: Remove the scan wiring in `service.py`**

Delete the `resolve_notebook_names` import and its `try/except` block (lines
~76-81) so the service no longer calls the workspace index. The notebook names
now come from the provider's `_resolve_entity_names` pass (Task 1). Then delete
the file `amx/lineage/native/workspace_index.py` and remove any re-export of it
from `amx/lineage/native/__init__.py` (grep for `workspace_index` /
`resolve_notebook_names` and clear every reference).

```bash
grep -rn "workspace_index\|resolve_notebook_names" amx/ tests/
```

Expected after edits: only matches are in deleted/edited lines you are removing.

- [ ] **Step 4: Run the native suite**

Run: `pytest tests/lineage/test_native_lineage.py -v`
Expected: PASS (no import errors for the deleted module).

- [ ] **Step 5: Commit**

```bash
git add amx/lineage/native/service.py amx/lineage/native/__init__.py tests/lineage/test_native_lineage.py
git rm amx/lineage/native/workspace_index.py
git commit -m "refactor(lineage): drop 40s workspace scan; notebook names resolve per-node"
```

---

## Phase 2 — Lazy click-to-open assets (problem 2b)

> **No fetch-side change needed for notebooks/jobs.** The materializer already
> records them as `name_only` ghost rows keyed `"<kind>#ext:<external_id>"`, so
> their `external_id` is preserved with no content pulled at fetch time. The only
> eager content ingest on the fetch path is for saved queries, which already work
> and which the user did not flag — leave it untouched (removing it would regress
> query drill-in). This phase adds the on-demand path for notebook / job /
> pipeline.

### Task 3: Single-asset on-demand ingest module

**Files:**
- Create: `amx/lineage/native/lazy_ingest.py`
- Test: `tests/lineage/test_lazy_ingest.py`

- [ ] **Step 1: Write the failing test**

Create `tests/lineage/test_lazy_ingest.py`:

```python
"""Single-asset lazy ingest — selection-request mapping."""
from __future__ import annotations

import pytest

from amx.lineage.native.lazy_ingest import selection_request_for


def test_notebook_selection_request():
    req = selection_request_for(profile="db", kind="notebook", external_id="123")
    assert req is not None
    assert req.profile_name == "db"
    assert req.types == ["notebooks"]
    assert req.selection == {"notebooks": ["123"]}


def test_job_selection_request():
    req = selection_request_for(profile="db", kind="job", external_id="9")
    assert req.types == ["jobs"]
    assert req.selection == {"jobs": ["9"]}


def test_unknown_kind_or_blank_id_returns_none():
    assert selection_request_for(profile="db", kind="dashboard", external_id="1") is None
    assert selection_request_for(profile="db", kind="notebook", external_id="") is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/lineage/test_lazy_ingest.py -v`
Expected: FAIL — module does not exist.

- [ ] **Step 3: Implement the module**

Create `amx/lineage/native/lazy_ingest.py`:

```python
"""On-demand single-asset ingest for native-lineage canvas nodes.

When the user clicks a name-only notebook / job / pipeline on the
lineage canvas, this pulls just that one asset's content into the
local ``remote_*`` store (the Assets cache) so the node becomes a
full, drillable asset. Reuses :class:`IngestAssetsService` with a
single-id ``selection`` — no full re-ingest and no work at
lineage-fetch time. Reopening a cached asset never re-runs this.
"""

from __future__ import annotations

from typing import Any

from amx.services.ingest_assets import IngestAssetsService, IngestRequest

# Canvas asset kind -> (IngestAssetsService asset_type, remote table, id column).
# Only kinds that IngestAssetsService can select by external id are eligible;
# dashboards / vector indexes / external have no per-id ingest and stay name-only.
_KIND_SPEC: dict[str, tuple[str, str, str]] = {
    "notebook": ("notebooks", "remote_notebooks", "external_id"),
    "job": ("jobs", "remote_jobs", "job_id"),
    "pipeline": ("pipelines", "remote_pipelines", "pipeline_id"),
}


def selection_request_for(*, profile: str, kind: str, external_id: str) -> IngestRequest | None:
    """Build a single-asset ``IngestRequest``, or ``None`` if not ingestable."""
    spec = _KIND_SPEC.get(kind)
    if spec is None or not external_id:
        return None
    asset_type, _table, _id_col = spec
    return IngestRequest(
        profile_name=profile,
        types=[asset_type],
        selection={asset_type: [external_id]},
    )


def ingest_one_asset(
    *, connector: Any, catalog: Any, profile: str, kind: str, external_id: str
) -> int | None:
    """Ingest one asset and return its ``remote_<kind>s.id`` (cached), or None."""
    req = selection_request_for(profile=profile, kind=kind, external_id=external_id)
    if req is None:
        return None
    IngestAssetsService(connector=connector, catalog=catalog).run(req)
    _asset_type, table, id_col = _KIND_SPEC[kind]
    with catalog._connect() as conn:
        row = conn.execute(
            f"SELECT id FROM {table} WHERE profile_name = ? AND {id_col} = ?",  # noqa: S608
            (profile, external_id),
        ).fetchone()
    return int(row[0]) if row else None
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/lineage/test_lazy_ingest.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add amx/lineage/native/lazy_ingest.py tests/lineage/test_lazy_ingest.py
git commit -m "feat(lineage): single-asset on-demand ingest helper"
```

### Task 4: Lazy-ingest HTTP endpoint

**Files:**
- Modify: `amx/web/routers/lineage.py` (add endpoint near `post_fetch`, ~line 625)
- Test: `tests/web/test_lineage_router.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/web/test_lineage_router.py` a test that posts to the new endpoint
and asserts a `remote_id` comes back. Monkeypatch `ingest_one_asset` so the test
does not hit Databricks:

```python
def test_asset_ingest_endpoint_returns_remote_id(client, monkeypatch):
    import amx.web.routers.lineage as lineage_router

    monkeypatch.setattr(
        lineage_router, "_ingest_one_asset_for_profile",
        lambda *, profile, kind, external_id: 42,
    )
    resp = client.post(
        "/api/lineage/asset/ingest",
        json={"profile": "db", "kind": "notebook", "external_id": "123"},
    )
    assert resp.status_code == 200
    assert resp.json()["remote_id"] == 42
```

(Use the existing `client` fixture from `tests/web/conftest.py`.)

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/web/test_lineage_router.py::test_asset_ingest_endpoint_returns_remote_id -v`
Expected: FAIL — endpoint missing (404).

- [ ] **Step 3: Implement the endpoint**

In `amx/web/routers/lineage.py`, add a request model and route. Put the
connector/catalog wiring in a small private helper so the route stays thin:

```python
class AssetIngestBody(BaseModel):
    profile: str
    kind: str
    external_id: str


def _ingest_one_asset_for_profile(*, profile: str, kind: str, external_id: str) -> int | None:
    from amx.cli_support.commands.db_assets_impl import _open_catalog, _open_connector
    from amx.lineage.native.lazy_ingest import ingest_one_asset
    from amx.web.deps import get_cfg

    cfg = get_cfg()
    connector = _open_connector(cfg, profile)
    catalog = _open_catalog(cfg)
    return ingest_one_asset(
        connector=connector, catalog=catalog, profile=profile, kind=kind, external_id=external_id
    )


@router.post("/asset/ingest")
def post_asset_ingest(body: AssetIngestBody) -> dict[str, Any]:
    """Ingest one native-lineage asset on demand and return its remote id.

    Pulls only the clicked notebook / job / pipeline into the Assets
    cache so its canvas node becomes full and drillable. Cached, so a
    second open does no work.
    """
    remote_id = _ingest_one_asset_for_profile(
        profile=body.profile, kind=body.kind, external_id=body.external_id
    )
    if remote_id is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Could not ingest {body.kind} {body.external_id} for profile {body.profile}.",
        )
    return {"remote_id": remote_id, "kind": body.kind}
```

Confirm `BaseModel` is imported (pydantic) at the top of the file; add the import
if missing. `get_cfg` is already imported.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/web/test_lineage_router.py::test_asset_ingest_endpoint_returns_remote_id -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add amx/web/routers/lineage.py tests/web/test_lineage_router.py
git commit -m "feat(lineage): POST /api/lineage/asset/ingest for click-to-open assets"
```

### Task 5: Surface `external_id` + `host` on canvas nodes

**Files:**
- Modify: `amx/web/routers/lineage.py:855-895` (meta build) and `:950-986` (node_entry)
- Test: `tests/web/test_lineage_router.py`

- [ ] **Step 1: Write the failing test (pure helper)**

Add a pure helper `_asset_external_id_from_table_name` and unit-test it:

```python
def test_asset_external_id_parse():
    from amx.web.routers.lineage import _asset_external_id_from_table_name
    assert _asset_external_id_from_table_name("notebook", "notebook#ext:123") == "123"
    assert _asset_external_id_from_table_name("job", "job#ext:9") == "9"
    # name-slug ghosts (no external id) and full bridges return None
    assert _asset_external_id_from_table_name("notebook", "notebook#ext:name:Foo") is None
    assert _asset_external_id_from_table_name("notebook", "notebook#42") is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/web/test_lineage_router.py::test_asset_external_id_parse -v`
Expected: FAIL — helper missing.

- [ ] **Step 3: Implement the helper and wire the fields**

Add the helper:

```python
def _asset_external_id_from_table_name(kind: str, table_name: str) -> str | None:
    """Recover a ghost asset's platform external id from its bridge name.

    Ghost rows are keyed ``"<kind>#ext:<external_id>"`` (or
    ``"<kind>#ext:name:<slug>"`` when no id was known). Returns the id,
    or ``None`` when the row carries only a name slug or another shape.
    """
    prefix = f"{kind}#ext:"
    if not table_name.startswith(prefix):
        return None
    ref = table_name[len(prefix):]
    return None if ref.startswith("name:") else (ref or None)
```

In the meta-build loop (~888, the `elif kind in _ASSET_NODE_KINDS` branch) add:

```python
                meta["label"] = str(r[6] or "")
                meta["external_id"] = _asset_external_id_from_table_name(kind, str(r[3] or ""))
```

In the node_entry asset branch (~981) add `external_id` and `host`:

```python
        elif meta.get("kind") in _ASSET_NODE_KINDS:
            node_entry["label"] = meta.get("label", "")
            node_entry["source_remote_id"] = meta.get("source_remote_id")
            node_entry["external_id"] = meta.get("external_id")
            node_entry["host"] = _profile_host(cfg, str(row[1] or ""))
```

Add a `host` for table nodes too (the table deep-link target). In the `table`
branch (~986) set `node_entry["host"] = _profile_host(cfg, str(row[1] or ""))`.
Add the helper (reads the profile's bare host; returns "" for non-Databricks):

```python
def _profile_host(cfg: AMXConfig, profile: str) -> str:
    p = (getattr(cfg, "db_profiles", {}) or {}).get(profile)
    if p is None or (getattr(p, "backend", "") or "").lower() != "databricks":
        return ""
    return getattr(p, "host", "") or ""
```

Ensure `cfg` is in scope in the node-building function; if not, thread it in from
the route handler that already depends on `get_cfg`.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/web/test_lineage_router.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add amx/web/routers/lineage.py tests/web/test_lineage_router.py
git commit -m "feat(lineage): surface external_id and host on canvas nodes"
```

---

## Phase 3 — Frontend: click-to-open + deep-links (problems 2b & 3)

### Task 6: Databricks deep-link URL builder

**Files:**
- Create: `frontend/src/lineage-canvas/logos/databricksDeepLink.ts`
- Test: `frontend/src/lineage-canvas/logos/databricksDeepLink.test.ts`

- [ ] **Step 1: Write the failing test**

```typescript
import { describe, expect, it } from "vitest";
import { databricksDeepLink } from "./databricksDeepLink";

describe("databricksDeepLink", () => {
  const host = "example.cloud.databricks.com";
  it("builds a table link from fqn", () => {
    expect(databricksDeepLink({ kind: "table", host, fqn: "cat.sch.tbl" })).toBe(
      "https://example.cloud.databricks.com/explore/data/cat/sch/tbl",
    );
  });
  it("builds a notebook link from externalId", () => {
    expect(databricksDeepLink({ kind: "notebook", host, externalId: "123" })).toBe(
      "https://example.cloud.databricks.com/editor/notebooks/123",
    );
  });
  it("builds a job link", () => {
    expect(databricksDeepLink({ kind: "job", host, externalId: "9" })).toBe(
      "https://example.cloud.databricks.com/jobs/9",
    );
  });
  it("returns null when host missing or identifier absent", () => {
    expect(databricksDeepLink({ kind: "job", host: "", externalId: "9" })).toBeNull();
    expect(databricksDeepLink({ kind: "notebook", host, externalId: undefined })).toBeNull();
    expect(databricksDeepLink({ kind: "dashboard", host, externalId: "1" })).toBeNull();
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd frontend && npx vitest run src/lineage-canvas/logos/databricksDeepLink.test.ts`
Expected: FAIL — module missing.

- [ ] **Step 3: Implement the builder**

Create `frontend/src/lineage-canvas/logos/databricksDeepLink.ts`:

```typescript
/**
 * Builds an "open in Databricks" URL for a lineage canvas node.
 *
 * Tables link to Catalog Explorer by their 3-part FQN; assets link by
 * their platform external id. Returns null when the host is unknown or
 * the node kind has no workspace destination, so callers render no link
 * rather than a broken one.
 */

interface DeepLinkArgs {
  kind: string;
  host: string | undefined;
  fqn?: string;
  externalId?: string | undefined;
}

function normalizeHost(host: string): string {
  const h = host.trim().replace(/\/+$/, "");
  return /^https?:\/\//.test(h) ? h : `https://${h}`;
}

export function databricksDeepLink(args: DeepLinkArgs): string | null {
  const { kind, fqn, externalId } = args;
  if (!args.host) return null;
  const base = normalizeHost(args.host);

  if (kind === "table") {
    const parts = (fqn || "").split(".").filter(Boolean);
    if (parts.length !== 3) return null;
    return `${base}/explore/data/${parts[0]}/${parts[1]}/${parts[2]}`;
  }
  if (!externalId) return null;
  switch (kind) {
    case "notebook":
      return `${base}/editor/notebooks/${externalId}`;
    case "job":
      return `${base}/jobs/${externalId}`;
    case "pipeline":
      return `${base}/pipelines/${externalId}`;
    case "query":
      return `${base}/sql/editor/${externalId}`;
    default:
      return null;
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd frontend && npx vitest run src/lineage-canvas/logos/databricksDeepLink.test.ts`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add frontend/src/lineage-canvas/logos/databricksDeepLink.ts frontend/src/lineage-canvas/logos/databricksDeepLink.test.ts
git commit -m "feat(lineage-canvas): databricks deep-link URL builder"
```

### Task 7: Carry `externalId` + `host` through the payload mapper

**Files:**
- Modify: `frontend/src/lineage-canvas/types.ts`
- Modify: `frontend/src/lineage-canvas/amx-bridge/payload.ts:345-364`

- [ ] **Step 1: Extend the types**

In `types.ts`, add to the `LoadedNode` interface (the shape returned by the graph
API): `external_id?: string | null;` and `host?: string;`. Add to
`AssetNodeData`: `externalId?: string;` and `host?: string;` (mirror the existing
optional-field style and the JSDoc comment convention).

- [ ] **Step 2: Map them in `payload.ts`**

In `loadedNodeToCanvasNode`, inside the asset branch `data` object (after
`sourceRemoteId`), add:

```typescript
        externalId: n.external_id ?? undefined,
        host: n.host || undefined,
```

In the table branch `data` object, add `host: n.host || undefined` so the table
node can build its deep-link too.

- [ ] **Step 3: Type-check**

Run: `cd frontend && npx tsc --noEmit`
Expected: no new errors from these files.

- [ ] **Step 4: Commit**

```bash
git add frontend/src/lineage-canvas/types.ts frontend/src/lineage-canvas/amx-bridge/payload.ts
git commit -m "feat(lineage-canvas): thread externalId and host to canvas nodes"
```

### Task 8: AssetNode — lazy-ingest click + deep-link

**Files:**
- Modify: `frontend/src/lineage-canvas/nodes/AssetNode.tsx`

- [ ] **Step 1: Add the data fields and an ingestable check**

Extend `AssetNodeData` (already done in Task 8 types; mirror here in the local
interface) with `externalId?: string` and `host?: string`. Define the kinds that
support on-demand ingest:

```typescript
const INGESTABLE_KINDS = new Set<AssetKind>(["notebook", "job", "pipeline"]);
```

- [ ] **Step 2: Add the lazy-ingest handler**

```typescript
async function ingestAndOpen(
  kind: AssetKind,
  profile: string | undefined,
  externalId: string,
) {
  const resp = await fetch("/api/lineage/asset/ingest", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ profile, kind, external_id: externalId }),
  });
  if (!resp.ok) return;
  const { remote_id } = (await resp.json()) as { remote_id: number };
  openInAssets(kind, remote_id);
}
```

Wire it: when `nameOnly && data.externalId && INGESTABLE_KINDS.has(data.kind)`,
render a "Fetch & open" button (reuse the header-button styling of the existing
`openInAssets` control) that calls `ingestAndOpen` and shows a brief loading
state via `useState`. Keep the existing `sourceRemoteId` "Open in Assets" button
for already-ingested (full) nodes.

- [ ] **Step 3: Add the Databricks deep-link**

Import the builder and render a small ↗ link in the header when a link exists:

```typescript
import { ExternalLink } from "lucide-react";
import { databricksDeepLink } from "../logos/databricksDeepLink";
// ...
const href = databricksDeepLink({
  kind: data.kind,
  host: data.host,
  externalId: data.externalId,
});
// in the header row:
{href && (
  <a
    href={href}
    target="_blank"
    rel="noopener noreferrer"
    title="Open in Databricks"
    className="inline-flex h-5 w-5 items-center justify-center rounded text-ink-dim hover:bg-surface hover:text-ink"
    onClick={(e) => e.stopPropagation()}
  >
    <ExternalLink size={12} />
  </a>
)}
```

- [ ] **Step 4: Type-check + manual verify**

Run: `cd frontend && npx tsc --noEmit` (expect no new errors).
Manual: after `deploy.sh`, fetch lineage for a Databricks table; a name-only
notebook shows a "Fetch & open" control that ingests and opens it in Assets, and
every node shows a working ↗ to Databricks. (Deferred to the deploy step.)

- [ ] **Step 5: Commit**

```bash
git add frontend/src/lineage-canvas/nodes/AssetNode.tsx
git commit -m "feat(lineage-canvas): click-to-ingest assets and open-in-Databricks link"
```

### Task 9: DataFrameNode — table logo opens Databricks

**Files:**
- Modify: `frontend/src/lineage-canvas/nodes/DataFrameNode.tsx:230-235`
- Modify: `frontend/src/lineage-canvas/logos/LogoBadge.tsx`

- [ ] **Step 1: Give `LogoBadge` an optional href**

In `LogoBadge.tsx`, accept an optional `href?: string`. When present, render an
`<a target="_blank" rel="noopener noreferrer">` wrapping the logo image (opens
Databricks) instead of the picker button; when absent, keep the existing
picker-button behavior unchanged.

- [ ] **Step 2: Pass the deep-link from DataFrameNode**

In `DataFrameNode.tsx`, build the link and pass it to `LogoBadge`:

```typescript
import { databricksDeepLink } from "../logos/databricksDeepLink";
// ...
const dbxHref = databricksDeepLink({ kind: "table", host: data.host, fqn: data.fqn });
// ...
<LogoBadge logoKey={data.logoKey} href={dbxHref ?? undefined} onClick={() => setLogoPickerOpen(true)} />
```

Keep the picker reachable: when a logo is set, the picker still opens from the
selected-node toolbar control already present (the "Set header logo" affordance
shown when no logo, plus `onClear` in the picker). The badge click now prefers
the deep-link when one exists.

- [ ] **Step 3: Type-check**

Run: `cd frontend && npx tsc --noEmit`
Expected: no new errors.

- [ ] **Step 4: Commit**

```bash
git add frontend/src/lineage-canvas/nodes/DataFrameNode.tsx frontend/src/lineage-canvas/logos/LogoBadge.tsx
git commit -m "feat(lineage-canvas): table logo opens the table in Databricks"
```

---

## Final verification

- [ ] **Backend suite green for touched files**

Run: `pytest tests/lineage/test_native_lineage.py tests/lineage/test_lazy_ingest.py tests/web/test_lineage_router.py -v`
Expected: PASS.

- [ ] **Frontend unit + type check**

Run: `cd frontend && npx vitest run src/lineage-canvas/logos/databricksDeepLink.test.ts && npx tsc --noEmit`
Expected: PASS, no new type errors.

- [ ] **House-rule sweep**

Run the project's forbidden-wording sweep over the touched paths (`amx/lineage`,
`amx/web/routers/lineage.py`, `frontend/src/lineage-canvas`) and confirm no hits;
confirm no Turkish text in any touched file.

- [ ] **Integration (after deploy)**

On the test machine: fetch lineage for a Databricks table and confirm —
notebooks/jobs show real names; clicking a name-only asset ingests and opens it
in Assets; re-opening is instant (cached); the table logo and asset ↗ open the
right Databricks pages; the `hit 40s budget` log line no longer appears.

---

## Deployment

Studio-visible change. For this iteration the agreed order is **PR + merge → run
the Studio deploy script** (testing happens on a different machine). Confirm with
the requester before merging and before running the deploy step.
