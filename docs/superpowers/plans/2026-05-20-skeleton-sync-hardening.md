# Skeleton Sync Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the catalog skeleton sync (a) honor a profile's pinned default container as a hard limit so the cache never contains out-of-scope rows, and (b) accept cooperative cancellation from Studio, the REPL, and Python callers.

**Architecture:** Two new tiny stdlib-only modules (a cancel registry and a default-container helper) plus targeted edits to `drift.py`, `web/routers/catalog.py`, `cli_support/session.py`, and `storage/_history_caches.py`. No storage-schema change, no class refactor.

**Tech Stack:** Python 3.11+, FastAPI, threading (stdlib), SQLite (existing history store), pytest.

---

## File structure

| Path | Action |
|---|---|
| `amx/search/_skeleton_jobs.py` | **new** — module-level cancel registry |
| `amx/db/_default_scope.py` | **new** — backend-uniform default helper |
| `amx/search/drift.py` | edit — enumerator + cancel checkpoints |
| `amx/web/routers/catalog.py` | edit — register on start, new cancel endpoint |
| `amx/cli_support/commands/db.py` | edit — `cmd_sync_stop` wizard |
| `amx/cli_support/session.py` | edit — wire `sync-stop` into the `db` namespace |
| `amx/storage/_history_caches.py` | edit — `purge_out_of_scope` |
| `amx/search/__init__.py` | edit — re-export `cancel_skeleton_sync` |
| `tests/test_skeleton_jobs.py` | **new** — registry unit tests |
| `tests/test_default_scope.py` | **new** — helper unit tests |
| `tests/storage/test_history_caches_purge.py` | **new** — purge unit tests |
| `tests/test_skeleton_sync_pinned_default.py` | **new** — scope integration test |
| `tests/test_skeleton_sync_cancellation.py` | **new** — cancel integration test |
| `tests/web/test_catalog_sync_cancel.py` | **new** — HTTP endpoint test |

Test placement follows the existing repo layout: `tests/storage/` and `tests/web/` exist as subdirs; everything else is flat in `tests/`.

---

## Task 1: Cancel registry module

**Files:**
- Create: `amx/search/_skeleton_jobs.py`
- Test: `tests/test_skeleton_jobs.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_skeleton_jobs.py
"""Tests for the module-level skeleton-sync cancel registry."""

from __future__ import annotations

from amx.search import _skeleton_jobs


def setup_function() -> None:
    _skeleton_jobs._jobs.clear()


def test_register_returns_unset_event() -> None:
    event = _skeleton_jobs.register("prof")
    assert event.is_set() is False
    assert _skeleton_jobs.is_cancelled("prof") is False


def test_cancel_sets_event_and_returns_true() -> None:
    event = _skeleton_jobs.register("prof")
    assert _skeleton_jobs.cancel("prof") is True
    assert event.is_set() is True
    assert _skeleton_jobs.is_cancelled("prof") is True


def test_cancel_with_no_job_returns_false() -> None:
    assert _skeleton_jobs.cancel("missing") is False


def test_double_register_returns_same_event() -> None:
    first = _skeleton_jobs.register("prof")
    second = _skeleton_jobs.register("prof")
    assert first is second


def test_unregister_clears_job() -> None:
    _skeleton_jobs.register("prof")
    _skeleton_jobs.unregister("prof")
    assert _skeleton_jobs.is_cancelled("prof") is False
    assert _skeleton_jobs.cancel("prof") is False


def test_running_profiles_lists_registered() -> None:
    _skeleton_jobs.register("a")
    _skeleton_jobs.register("b")
    assert sorted(_skeleton_jobs.running_profiles()) == ["a", "b"]
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd ~/Desktop/omeryasirkucuk/Master/Thesis/AMX
pytest tests/test_skeleton_jobs.py -v
```

Expected: ModuleNotFoundError or all tests fail because `_skeleton_jobs` does not exist.

- [ ] **Step 3: Write minimal implementation**

```python
# amx/search/_skeleton_jobs.py
"""Module-level cancel registry for in-flight skeleton syncs.

One :class:`threading.Event` per profile; ``cancel(profile)`` sets it
so the running sync loop in :mod:`amx.search.drift` can break at its
next checkpoint. Cooperative cancel only — the loop must reach a
checkpoint for the cancel to take effect.
"""

from __future__ import annotations

import threading

_jobs: dict[str, threading.Event] = {}
_lock = threading.RLock()


def register(profile: str) -> threading.Event:
    """Return (or create) the cancel event for ``profile``.

    Re-entry returns the existing event so a restart racing a cancel
    doesn't lose the signal.
    """
    with _lock:
        event = _jobs.get(profile)
        if event is None:
            event = threading.Event()
            _jobs[profile] = event
        return event


def cancel(profile: str) -> bool:
    """Set the cancel event for ``profile``. Returns ``True`` when a job
    was registered, ``False`` otherwise (nothing to cancel)."""
    with _lock:
        event = _jobs.get(profile)
        if event is None:
            return False
        event.set()
        return True


def is_cancelled(profile: str) -> bool:
    """``True`` when ``cancel(profile)`` has been called for the
    currently registered job."""
    with _lock:
        event = _jobs.get(profile)
        return bool(event and event.is_set())


def unregister(profile: str) -> None:
    """Forget the cancel event for ``profile``. Safe to call when no
    job is registered."""
    with _lock:
        _jobs.pop(profile, None)


def running_profiles() -> list[str]:
    """Snapshot of profile names with a registered job."""
    with _lock:
        return list(_jobs.keys())
```

- [ ] **Step 4: Run test to verify it passes**

```bash
pytest tests/test_skeleton_jobs.py -v
```

Expected: all six tests PASS.

- [ ] **Step 5: Commit**

```bash
git add amx/search/_skeleton_jobs.py tests/test_skeleton_jobs.py
git commit -m "feat(search): add module-level cancel registry for skeleton sync"
```

---

## Task 2: Default-container helper

**Files:**
- Create: `amx/db/_default_scope.py`
- Test: `tests/test_default_scope.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_default_scope.py
"""Tests for the per-backend default-container helper."""

from __future__ import annotations

from types import SimpleNamespace

from amx.db._default_scope import profile_default_container


def test_databricks_catalog_wins() -> None:
    db = SimpleNamespace(backend="databricks", catalog="prod", database="", dataset="")
    assert profile_default_container(db) == "prod"


def test_bigquery_dataset() -> None:
    db = SimpleNamespace(backend="bigquery", catalog="", dataset="analytics", database="")
    assert profile_default_container(db) == "analytics"


def test_snowflake_database() -> None:
    db = SimpleNamespace(backend="snowflake", catalog="", dataset="", database="DW")
    assert profile_default_container(db) == "DW"


def test_postgres_database() -> None:
    db = SimpleNamespace(backend="postgres", catalog="", dataset="", database="app")
    assert profile_default_container(db) == "app"


def test_empty_profile_returns_none() -> None:
    db = SimpleNamespace(backend="postgres", catalog="", dataset="", database="")
    assert profile_default_container(db) is None


def test_none_input_returns_none() -> None:
    assert profile_default_container(None) is None


def test_catalog_beats_database_when_both_set() -> None:
    db = SimpleNamespace(backend="trino", catalog="hive", database="default", dataset="")
    assert profile_default_container(db) == "hive"
```

- [ ] **Step 2: Run test to verify it fails**

```bash
pytest tests/test_default_scope.py -v
```

Expected: ModuleNotFoundError or all tests fail.

- [ ] **Step 3: Write minimal implementation**

```python
# amx/db/_default_scope.py
"""Per-backend default-container helper.

A DB profile may pin a default container under one of three field
names — ``catalog`` (Databricks, Trino), ``dataset`` (BigQuery), or
``database`` (everything else). This helper normalizes the three so
the skeleton sync can ask one question: "what container is this
profile pinned to?".
"""

from __future__ import annotations

from typing import Any


def profile_default_container(db_cfg: Any) -> str | None:
    """Return the profile's pinned default container, or ``None``.

    Precedence: ``catalog`` (three-level backends), then ``dataset``
    (BigQuery), then ``database`` (two-level backends). Empty strings
    and ``None`` are treated as "unpinned".
    """
    if db_cfg is None:
        return None
    for attr in ("catalog", "dataset", "database"):
        value = getattr(db_cfg, attr, None)
        if value:
            text = str(value).strip()
            if text:
                return text
    return None
```

- [ ] **Step 4: Run test to verify it passes**

```bash
pytest tests/test_default_scope.py -v
```

Expected: all seven tests PASS.

- [ ] **Step 5: Commit**

```bash
git add amx/db/_default_scope.py tests/test_default_scope.py
git commit -m "feat(db): add backend-uniform default-container helper"
```

---

## Task 3: Purge function in history caches

**Files:**
- Modify: `amx/storage/_history_caches.py`
- Test: `tests/storage/test_history_caches_purge.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/storage/test_history_caches_purge.py
"""Tests for purge_out_of_scope — the migration helper that strips
cached rows belonging to containers outside the profile's pinned
default."""

from __future__ import annotations

import time

import pytest

from amx.storage._history_caches import (
    purge_out_of_scope,
    save_column_comments_cache,
    save_schemas_cache,
)
from amx.storage.sqlite_store import SQLiteHistoryStore


@pytest.fixture()
def hs(tmp_path) -> SQLiteHistoryStore:
    return SQLiteHistoryStore(tmp_path / "history.db")


def test_purge_removes_only_out_of_scope_rows(hs: SQLiteHistoryStore) -> None:
    # In-scope (kept):
    save_column_comments_cache(
        hs,
        db_profile="prof",
        database="prod",
        schema="public",
        entries={"orders": {"table_comment": "ok", "columns": {}, "kind": "TABLE"}},
    )
    # Out-of-scope (purged):
    save_column_comments_cache(
        hs,
        db_profile="prof",
        database="dev",
        schema="public",
        entries={"orders": {"table_comment": "stale", "columns": {}, "kind": "TABLE"}},
    )
    counts = purge_out_of_scope(hs, db_profile="prof", container="prod")
    assert counts["column_comments_cache"] == 1

    with hs._connect() as conn:
        rows = conn.execute(
            "SELECT database_name FROM column_comments_cache WHERE db_profile = ?",
            ("prof",),
        ).fetchall()
    assert [r["database_name"] for r in rows] == ["prod"]


def test_purge_is_idempotent(hs: SQLiteHistoryStore) -> None:
    save_column_comments_cache(
        hs,
        db_profile="prof",
        database="dev",
        schema="public",
        entries={"orders": {"table_comment": "x", "columns": {}, "kind": "TABLE"}},
    )
    first = purge_out_of_scope(hs, db_profile="prof", container="prod")
    second = purge_out_of_scope(hs, db_profile="prof", container="prod")
    assert first["column_comments_cache"] == 1
    assert second["column_comments_cache"] == 0


def test_purge_leaves_other_profiles_alone(hs: SQLiteHistoryStore) -> None:
    save_column_comments_cache(
        hs,
        db_profile="other",
        database="dev",
        schema="public",
        entries={"orders": {"table_comment": "x", "columns": {}, "kind": "TABLE"}},
    )
    counts = purge_out_of_scope(hs, db_profile="prof", container="prod")
    assert counts["column_comments_cache"] == 0
    with hs._connect() as conn:
        n = conn.execute(
            "SELECT COUNT(*) AS n FROM column_comments_cache WHERE db_profile = ?",
            ("other",),
        ).fetchone()["n"]
    assert n == 1


def test_purge_handles_schemas_cache(hs: SQLiteHistoryStore) -> None:
    save_schemas_cache(
        hs,
        db_profile="prof",
        database="prod",
        catalog="",
        entries={"public": {"comment": "ok"}},
    )
    save_schemas_cache(
        hs,
        db_profile="prof",
        database="dev",
        catalog="",
        entries={"public": {"comment": "stale"}},
    )
    counts = purge_out_of_scope(hs, db_profile="prof", container="prod")
    assert counts["schemas_cache"] == 1
```

- [ ] **Step 2: Run test to verify it fails**

```bash
pytest tests/storage/test_history_caches_purge.py -v
```

Expected: ImportError because `purge_out_of_scope` is not defined.

- [ ] **Step 3: Add `purge_out_of_scope` to `amx/storage/_history_caches.py`**

Append this function at the end of the module:

```python
def purge_out_of_scope(
    hs: "SQLiteHistoryStore",
    *,
    db_profile: str,
    container: str,
) -> dict[str, int]:
    """Delete cached rows for ``db_profile`` whose container does not
    match ``container``. Idempotent. Returns deletion counts per
    table for the audit log.

    Three cache tables are purged in one transaction:
    * ``catalog_entities`` — keyed by ``database_name``
    * ``schemas_cache`` — keyed by ``database`` (and ``catalog`` on
      three-level backends; the row matches when either field equals
      ``container``)
    * ``column_comments_cache`` — keyed by ``database_name``
    """
    container = str(container or "")
    counts = {
        "catalog_entities": 0,
        "schemas_cache": 0,
        "column_comments_cache": 0,
    }
    if not container:
        return counts
    with hs._lock, hs._connect() as conn:
        cur = conn.execute(
            "DELETE FROM catalog_entities "
            "WHERE db_profile = ? AND IFNULL(database_name, '') != ?",
            (db_profile, container),
        )
        counts["catalog_entities"] = int(cur.rowcount or 0)
        cur = conn.execute(
            "DELETE FROM schemas_cache "
            "WHERE db_profile = ? "
            "AND IFNULL(database, '') != ? "
            "AND IFNULL(catalog, '') != ?",
            (db_profile, container, container),
        )
        counts["schemas_cache"] = int(cur.rowcount or 0)
        cur = conn.execute(
            "DELETE FROM column_comments_cache "
            "WHERE db_profile = ? AND IFNULL(database_name, '') != ?",
            (db_profile, container),
        )
        counts["column_comments_cache"] = int(cur.rowcount or 0)
    return counts
```

- [ ] **Step 4: Run test to verify it passes**

```bash
pytest tests/storage/test_history_caches_purge.py -v
```

Expected: all four tests PASS.

- [ ] **Step 5: Commit**

```bash
git add amx/storage/_history_caches.py tests/storage/test_history_caches_purge.py
git commit -m "feat(storage): purge out-of-scope cached rows before skeleton sync"
```

---

## Task 4: Wire helpers into drift.py

**Files:**
- Modify: `amx/search/drift.py:243-506`
- Test: `tests/test_skeleton_sync_pinned_default.py` (added in Task 8)

- [ ] **Step 1: Read the current enumerator + sync function**

`_enumerate_containers` lives at `amx/search/drift.py:243-283`, `sync_profile_skeleton` at lines 286-506.

- [ ] **Step 2: Apply the hard-limit short-circuit to `_enumerate_containers`**

At the top of the function body (after the docstring), short-circuit when the profile pins a default container:

```python
def _enumerate_containers(
    cfg,
    profile: str,
    db_backend: str,
    default_container: str,
    is_three_level: bool,
) -> tuple[list[str], str | None]:
    """..."""
    # Hard-limit scoping: when the profile pins a default container,
    # walk only that one. Never enumerate the connector — even an
    # admin user's catalog/database list would be wrong to write into
    # the cache for a profile scoped out of those containers.
    if default_container:
        return [default_container], None
    # ... existing body unchanged
```

- [ ] **Step 3: Add purge + cancel checkpoints to `sync_profile_skeleton`**

Two edits in `sync_profile_skeleton`:

(a) After the `default_container` resolution block (right before the `# Step 1` comment, around current line 352), call purge and register the cancel event:

```python
    from amx.search import _skeleton_jobs
    from amx.storage._history_caches import purge_out_of_scope

    # Register a cancel slot so /api/catalog/sync/cancel can find us.
    # The caller (catalog router / worker) already registered when
    # invoked via HTTP; calling again returns the same event.
    cancel_event = _skeleton_jobs.register(profile)

    # Purge stale rows from any previous unscoped sync. Skip when
    # the profile is unpinned (legacy multi-container behavior).
    if default_container:
        history_store = getattr(catalog, "history_store", None) or getattr(
            catalog, "_hs", None
        )
        if history_store is not None:
            try:
                purge_out_of_scope(
                    history_store,
                    db_profile=profile,
                    container=default_container,
                )
            except Exception as exc:  # pragma: no cover - best-effort
                log.warning("Skeleton purge skipped for %s: %s", profile, exc)
```

(b) Inject cancel checkpoints at three loop heads — the two pass-1 loops (lines 385, 408) and the three pass-2 loops (lines 463, 465, 466). At each loop head, before doing work:

```python
        if cancel_event.is_set():
            break
```

And wrap the final return paths so the registry slot is freed. The cleanest shape is to wrap the whole function body in `try/finally`:

```python
    try:
        # ... existing body ...
    finally:
        _skeleton_jobs.unregister(profile)
```

When the cancel event fires, replace the normal `finish_skeleton_sync(ok=True)` with a cancelled finalizer. After the pass-2 loop, change:

```python
    catalog.finish_skeleton_sync(profile, ok=True)
    summary["state"] = "done"
    summary["processed"] = processed
    return summary
```

to:

```python
    if cancel_event.is_set():
        catalog.finish_skeleton_sync(profile, ok=False, error="cancelled")
        summary["state"] = "cancelled"
        summary["processed"] = processed
        return summary
    catalog.finish_skeleton_sync(profile, ok=True)
    summary["state"] = "done"
    summary["processed"] = processed
    return summary
```

- [ ] **Step 4: Run the existing drift tests to confirm nothing regressed**

```bash
pytest tests/ -k "drift or skeleton" -v
```

Expected: all green (or only the new not-yet-written ones fail).

- [ ] **Step 5: Commit**

```bash
git add amx/search/drift.py
git commit -m "feat(search): scope skeleton sync to pinned container + cooperative cancel"
```

---

## Task 5: Re-export `cancel_skeleton_sync`

**Files:**
- Modify: `amx/search/__init__.py`

- [ ] **Step 1: Add the public alias**

Append:

```python
from amx.search._skeleton_jobs import cancel as cancel_skeleton_sync  # noqa: F401
```

- [ ] **Step 2: Commit**

```bash
git add amx/search/__init__.py
git commit -m "feat(search): expose cancel_skeleton_sync as public helper"
```

---

## Task 6: Catalog router — register on start + cancel endpoint

**Files:**
- Modify: `amx/web/routers/catalog.py`
- Test: `tests/web/test_catalog_sync_cancel.py`

- [ ] **Step 1: Write the failing endpoint test**

```python
# tests/web/test_catalog_sync_cancel.py
"""POST /api/catalog/sync/cancel — cooperative cancel surface."""

from __future__ import annotations

from amx.search import _skeleton_jobs


def setup_function() -> None:
    _skeleton_jobs._jobs.clear()


def test_cancel_returns_true_when_job_registered(client, auth_headers) -> None:
    _skeleton_jobs.register("prod_dwh")
    response = client.post(
        "/api/catalog/sync/cancel",
        json={"profile": "prod_dwh"},
        headers=auth_headers,
    )
    assert response.status_code == 200
    assert response.json() == {"profile": "prod_dwh", "cancelled": True}


def test_cancel_returns_false_when_no_job(client, auth_headers) -> None:
    response = client.post(
        "/api/catalog/sync/cancel",
        json={"profile": "missing"},
        headers=auth_headers,
    )
    assert response.status_code == 200
    assert response.json() == {"profile": "missing", "cancelled": False}


def test_cancel_requires_profile(client, auth_headers) -> None:
    response = client.post(
        "/api/catalog/sync/cancel", json={}, headers=auth_headers
    )
    assert response.status_code == 422
```

- [ ] **Step 2: Run to verify it fails (404 or missing route)**

```bash
pytest tests/web/test_catalog_sync_cancel.py -v
```

- [ ] **Step 3: Wire the registry call into the existing `/sync` endpoint**

In `amx/web/routers/catalog.py:441-455`, inject `_skeleton_jobs.register(target_profile)` inside the runner so the slot is live for the entire thread lifetime:

```python
    from amx.search import _skeleton_jobs

    def _spawn(target_profile: str) -> None:
        _skeleton_jobs.register(target_profile)

        def _runner() -> None:
            try:
                sync_profile_skeleton(cfg, target_profile, catalog, databases=databases_arg)
            except Exception as exc:  # pragma: no cover - best-effort
                try:
                    catalog.finish_skeleton_sync(target_profile, ok=False, error=str(exc))
                except Exception:
                    pass

        threading.Thread(
            target=_runner,
            name=f"amx-catalog-skeleton-sync-{target_profile}",
            daemon=True,
        ).start()
```

- [ ] **Step 4: Add the cancel endpoint**

Add this Pydantic request model near the top of `catalog.py` (next to `CacheRefreshRequest`):

```python
class CatalogSyncCancelRequest(BaseModel):
    profile: str
```

Then append the endpoint after `trigger_catalog_sync`:

```python
@router.post("/sync/cancel")
def cancel_catalog_sync(body: CatalogSyncCancelRequest) -> dict[str, Any]:
    """Cooperatively cancel an in-flight skeleton sync for ``profile``.

    The running sync thread observes the cancel at its next loop
    checkpoint (per-container, per-schema, or per-table), finishes
    the in-flight table, then exits cleanly with
    ``finish_skeleton_sync(ok=False, error="cancelled")``. Rows
    already written remain in the cache.

    Returns ``{"cancelled": True}`` when a job was registered for
    ``profile``, ``{"cancelled": False}`` when nothing was running.
    """
    from amx.search import _skeleton_jobs

    profile = (body.profile or "").strip()
    if not profile:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="profile is required.",
        )
    cancelled = _skeleton_jobs.cancel(profile)
    return {"profile": profile, "cancelled": cancelled}
```

- [ ] **Step 5: Run to verify it passes**

```bash
pytest tests/web/test_catalog_sync_cancel.py -v
```

- [ ] **Step 6: Commit**

```bash
git add amx/web/routers/catalog.py tests/web/test_catalog_sync_cancel.py
git commit -m "feat(web): register skeleton sync and expose /sync/cancel endpoint"
```

---

## Task 7: REPL `/db sync-stop` command

**Files:**
- Modify: `amx/cli_support/commands/db.py`
- Modify: `amx/cli_support/session.py`

- [ ] **Step 1: Add `cmd_sync_stop` to `db.py`**

Append after `cmd_cache_clear` (around line 1732):

```python
def cmd_sync_stop(cfg: AMXConfig, rest: list[str]) -> None:
    """Cancel an in-flight skeleton sync.

    Bare ``/db sync-stop`` lists running syncs from the registry,
    user picks one, confirmation prompt, then cancel. The optional
    ``--profile <name>`` shortcut skips the picker for scripts.
    """
    from amx.search import _skeleton_jobs

    profile = ""
    for i, token in enumerate(rest):
        if token == "--profile" and i + 1 < len(rest):
            profile = rest[i + 1].strip()

    running = sorted(_skeleton_jobs.running_profiles())
    if not running:
        info("No skeleton sync is currently running.")
        return

    if not profile:
        from amx.cli_support._picker import pick_one

        profile = pick_one(
            "Pick a profile to cancel sync for",
            running,
        ) or ""
        if not profile:
            info("Cancelled.")
            return

    if profile not in running:
        warn(f"No active sync for profile {profile!r}.")
        return

    cancelled = _skeleton_jobs.cancel(profile)
    if cancelled:
        success(
            f"Cancellation requested for {profile!r}. "
            "The in-flight table will finish, then the sync exits."
        )
    else:
        info(f"No active sync to cancel for {profile!r}.")
```

If `pick_one` does not exist in `amx/cli_support/_picker.py`, replace the picker call with a numbered-prompt fallback using the existing input helpers in `db.py` (look for how `cmd_use` or `cmd_remove_profile` prompts).

- [ ] **Step 2: Wire into the session dispatcher**

In `amx/cli_support/session.py`, after the existing `cache-clear` handler (around line 629), add:

```python
    if head == "sync-stop":
        if not _require_namespace(head, namespace, "db", "sync-stop"):
            return True
        from amx.cli_support.commands.db import cmd_sync_stop as _cmd_sync_stop

        _cmd_sync_stop(cfg, parts[1:])
        return True
```

- [ ] **Step 3: Smoke check the import**

```bash
python -c "from amx.cli_support.commands.db import cmd_sync_stop; print(cmd_sync_stop)"
```

Expected: prints the function object.

- [ ] **Step 4: Commit**

```bash
git add amx/cli_support/commands/db.py amx/cli_support/session.py
git commit -m "feat(cli): /db sync-stop cancels a running skeleton sync"
```

---

## Task 8: Integration test — pinned default

**Files:**
- Create: `tests/test_skeleton_sync_pinned_default.py`

- [ ] **Step 1: Write the test**

```python
# tests/test_skeleton_sync_pinned_default.py
"""sync_profile_skeleton must walk only the pinned container."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture()
def fake_connector():
    conn = MagicMock()
    conn.list_catalogs.return_value = ["prod", "dev", "scratch"]
    conn.list_databases.return_value = ["prod", "dev", "scratch"]
    conn.list_schemas.return_value = ["public"]
    conn.list_assets.return_value = [{"name": "orders", "kind": "table"}]
    return conn


def test_pinned_catalog_short_circuits_enumeration(fake_connector) -> None:
    from amx.search import drift

    cfg = SimpleNamespace(
        db_profiles={
            "prof": SimpleNamespace(backend="databricks", catalog="prod", database=""),
        }
    )
    catalog = MagicMock()
    catalog._connect.return_value.__enter__ = MagicMock(return_value=MagicMock())
    catalog._connect.return_value.__exit__ = MagicMock(return_value=False)

    with patch.object(drift, "_scoped_connector", return_value=fake_connector):
        summary = drift.sync_profile_skeleton(cfg, "prof", catalog)

    assert summary["containers"] == ["prod"]
    fake_connector.list_catalogs.assert_not_called()
    fake_connector.list_databases.assert_not_called()


def test_unpinned_profile_falls_back_to_enumeration(fake_connector) -> None:
    from amx.search import drift

    cfg = SimpleNamespace(
        db_profiles={
            "prof": SimpleNamespace(backend="databricks", catalog="", database=""),
        }
    )
    catalog = MagicMock()
    catalog._connect.return_value.__enter__ = MagicMock(return_value=MagicMock())
    catalog._connect.return_value.__exit__ = MagicMock(return_value=False)

    with patch.object(drift, "_scoped_connector", return_value=fake_connector):
        summary = drift.sync_profile_skeleton(cfg, "prof", catalog)

    assert set(summary["containers"]) == {"prod", "dev", "scratch"}
    fake_connector.list_catalogs.assert_called()
```

- [ ] **Step 2: Run**

```bash
pytest tests/test_skeleton_sync_pinned_default.py -v
```

Expected: both PASS once Task 4 is in.

- [ ] **Step 3: Commit**

```bash
git add tests/test_skeleton_sync_pinned_default.py
git commit -m "test(search): skeleton sync honors pinned default container"
```

---

## Task 9: Integration test — cooperative cancel

**Files:**
- Create: `tests/test_skeleton_sync_cancellation.py`

- [ ] **Step 1: Write the test**

```python
# tests/test_skeleton_sync_cancellation.py
"""sync_profile_skeleton must observe a cancel event at its loop heads."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from amx.search import _skeleton_jobs, drift


def setup_function() -> None:
    _skeleton_jobs._jobs.clear()


def test_pre_set_cancel_exits_without_writing() -> None:
    fake = MagicMock()
    fake.list_schemas.return_value = ["public"]
    fake.list_assets.return_value = [{"name": "orders", "kind": "table"}]

    cfg = SimpleNamespace(
        db_profiles={
            "prof": SimpleNamespace(backend="postgres", catalog="", database="app"),
        }
    )
    catalog = MagicMock()
    catalog._connect.return_value.__enter__ = MagicMock(return_value=MagicMock())
    catalog._connect.return_value.__exit__ = MagicMock(return_value=False)

    event = _skeleton_jobs.register("prof")
    event.set()

    with patch.object(drift, "_scoped_connector", return_value=fake):
        summary = drift.sync_profile_skeleton(cfg, "prof", catalog)

    assert summary["state"] == "cancelled"
    catalog.finish_skeleton_sync.assert_called_with(
        "prof", ok=False, error="cancelled"
    )


def test_normal_run_does_not_trigger_cancel_path() -> None:
    fake = MagicMock()
    fake.list_schemas.return_value = ["public"]
    fake.list_assets.return_value = [{"name": "orders", "kind": "table"}]

    cfg = SimpleNamespace(
        db_profiles={
            "prof": SimpleNamespace(backend="postgres", catalog="", database="app"),
        }
    )
    catalog = MagicMock()
    catalog._connect.return_value.__enter__ = MagicMock(return_value=MagicMock())
    catalog._connect.return_value.__exit__ = MagicMock(return_value=False)

    with patch.object(drift, "_scoped_connector", return_value=fake):
        summary = drift.sync_profile_skeleton(cfg, "prof", catalog)

    assert summary["state"] in {"done", "syncing"}
    # `finish_skeleton_sync` called with ok=True at the happy path.
    args, kwargs = catalog.finish_skeleton_sync.call_args
    assert kwargs.get("ok") is True or (len(args) >= 2 and args[1] is True)
```

- [ ] **Step 2: Run + commit**

```bash
pytest tests/test_skeleton_sync_cancellation.py -v
git add tests/test_skeleton_sync_cancellation.py
git commit -m "test(search): cooperative skeleton-sync cancellation"
```

---

## Task 10: Verify whole suite + lint

- [ ] **Step 1: Run new + adjacent tests**

```bash
pytest tests/test_skeleton_jobs.py tests/test_default_scope.py tests/test_skeleton_sync_pinned_default.py tests/test_skeleton_sync_cancellation.py tests/storage/test_history_caches_purge.py tests/web/test_catalog_sync_cancel.py tests/test_db_cache_ops.py tests/test_catalog_cache_hardening.py -v
```

Expected: all green.

- [ ] **Step 2: Full pytest run**

```bash
pytest -q
```

Expected: green (or only pre-existing failures that exist on main).

- [ ] **Step 3: Lint + types**

```bash
ruff check amx/search/_skeleton_jobs.py amx/db/_default_scope.py amx/search/drift.py amx/web/routers/catalog.py amx/cli_support/commands/db.py amx/cli_support/session.py amx/storage/_history_caches.py amx/search/__init__.py
mypy amx/search amx/db amx/web/routers/catalog.py amx/storage/_history_caches.py 2>&1 | tail -20
```

Expected: zero new ruff/mypy errors.

---

## Task 11: Deploy → PR → Merge

Per AMX house rule 6: deploy first, then open the PR, then merge.

- [ ] **Step 1: Run the deploy script**

Run the local deploy script that ships the new code to the live
Studio so reviewers can verify the Stop button against the running
deployment. The script lives outside the public repo per the
"private infra stays private" policy; invoke whichever local path
you have configured.

- [ ] **Step 2: Push branch + open PR**

```bash
cd ~/Desktop/omeryasirkucuk/Master/Thesis/AMX
git push -u origin feat/skeleton-sync-hardening
gh pr create --title "feat(catalog): skeleton-sync hardening — scope + cancel" --body "$(cat <<'EOF'
## Summary
- Hard-limit cache scoping: profiles with a pinned default container now write only that container's rows. Out-of-scope rows from prior unscoped syncs are purged in a single idempotent transaction at the start of the next sync.
- Cooperative skeleton-sync cancellation via a module-level cancel registry (`amx/search/_skeleton_jobs.py`). Surfaces: `POST /api/catalog/sync/cancel`, REPL `/db sync-stop`, and a public `cancel_skeleton_sync(profile)` helper.

## Test plan
- [ ] `pytest tests/test_skeleton_jobs.py tests/test_default_scope.py tests/test_skeleton_sync_pinned_default.py tests/test_skeleton_sync_cancellation.py tests/storage/test_history_caches_purge.py tests/web/test_catalog_sync_cancel.py`
- [ ] `pytest tests/test_db_cache_ops.py tests/test_catalog_cache_hardening.py` (no-regression)
- [ ] Manual: pin a default container on a Databricks profile, kick off sync, verify only that catalog's rows land in `catalog_entities`.
- [ ] Manual: start sync, hit Stop in Studio, verify pill flips to `cancelled` and partial rows remain.
EOF
)"
```

- [ ] **Step 3: Merge**

```bash
gh pr merge --squash --delete-branch
```

If `gh pr merge --auto` triggers a harness permission prompt, run the explicit squash form above.

---

## Self-review checklist

- [ ] Every code block uses tabs/spaces consistent with the repo (4-space Python).
- [ ] Run `grep -ri "p<no-token>aid"` substitute check (the OSS-neutral token policy in AMX rule 2) — no hits in new files.
- [ ] No Turkish characters or words in any tracked file (per AMX rule 4).
- [ ] No agent attribution trailer (any co-author line referencing an automated tool) and no third-party tool attribution in commit messages or PR body.
- [ ] Every new column/table description gate (rule 5) is not triggered — storage schema unchanged.
- [ ] Cross-platform: only stdlib `threading` and standard SQLite — no POSIX-only paths or signals.
- [ ] All file paths in step bodies are absolute or repo-relative; no placeholders.
