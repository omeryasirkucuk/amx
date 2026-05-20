# Skeleton Sync Hardening — Cache Scoping + Cancellation

## Context

The catalog skeleton sync currently has two related defects in the same
subsystem (`amx/search/drift.py` + `amx/web/routers/catalog.py`):

1. **Cache scoping ignores pinned defaults.** When a DB profile pins a
   default container (`database` / `catalog` / `dataset`),
   `sync_profile_skeleton`'s container enumerator at
   `amx/search/drift.py:243-283` still calls the connector's
   `list_catalogs()` or `list_databases()` and walks every container on
   the server. The cache tables (`catalog_entities`, `schemas_cache`,
   `column_comments_cache`) then accumulate rows from containers the
   user explicitly scoped out of the profile. The cache keys themselves
   are shaped correctly — `(db_profile, database, catalog, schema,
   table)` per `amx/storage/_history_caches.py:176,424` — so the bug is
   in what gets *written*, not how it is *keyed*.

2. **Sync cannot be cancelled.** `amx/web/routers/catalog.py:445-462`
   spawns the sync as a bare daemon `threading.Thread` with no cancel
   handle, no event, and no UI-bound endpoint. Grepping
   `cancel|abort|stop|is_cancelled` across `drift.py`,
   `_catalog/sync.py`, and `db_cache.py` returns zero matches — there
   is no cancellation primitive at all. Once a sync starts, only a
   process restart stops it.

Both defects live in the same code path; the cancel checkpoints and the
scope-aware enumerator are fixed together so `drift.py` is touched once.

## Decisions

These were locked through brainstorming and are non-negotiable inputs
to the implementation plan.

| Axis | Decision |
|---|---|
| Scoping rule | **Hard limit.** If a profile has a pinned default container, sync walks only that container — no escape, no override flag. Users who want other catalogs create another profile. |
| Existing rows | **Idempotent purge.** The first hard-limit sync deletes out-of-scope rows from the three cache tables in a single transaction before the walk starts. No banner, no manual command required. |
| Cancellation model | **Cooperative cancel.** Checkpoints at the head of each container / schema / table loop. Cancel finishes the in-flight table, then exits cleanly with `finish_skeleton_sync(ok=False, error="cancelled")`. Partial rows are kept. |
| Cancel surfaces | All three: Studio HTTP `POST /api/catalog/sync/cancel`, REPL `/db sync stop`, and a Python helper `cancel_skeleton_sync(profile)` for tests and the worker. |
| Backend coverage | Uniform via a single helper `profile_default_container(db_cfg)` that normalizes per-backend field names (`catalog` for Databricks/Trino, `dataset` for BigQuery, `database` for Snowflake/Hive/Postgres/MySQL/MSSQL). |
| Implementation shape | **Minimal patch + module-level cancel registry.** No `SkeletonSyncJob` class refactor, no asyncio migration. Smallest surface change that fixes both defects. |

## Architecture

### New modules

- **`amx/search/_skeleton_jobs.py`** — module-level cancel registry.
  - `_jobs: dict[str, threading.Event]`, `_lock: threading.RLock`
  - `register(profile) -> threading.Event`
  - `cancel(profile) -> bool` (returns True if a job was registered)
  - `is_cancelled(profile) -> bool`
  - `unregister(profile) -> None`
  - Re-entry: if `register` is called for a profile that already has an
    event, return the existing event (race-safe with cancel-then-restart).
  - Zero external dependencies; pure stdlib threading.

- **`amx/db/_default_scope.py`** — backend-uniform pinned-default helper.
  - `profile_default_container(db_cfg) -> str | None`
  - Order of precedence: `catalog` (if non-empty), then `dataset`, then
    `database`. Returns `None` when all three are empty/None.
  - Pure function; no I/O. Easy to unit-test per backend config shape.

### Edited modules

- **`amx/search/drift.py`**
  - `_enumerate_containers()` (current lines ~243-283): if
    `profile_default_container()` returns a non-empty value, return
    `[default]` immediately. Do not call `connector.list_catalogs()` or
    `list_databases()`. When the helper returns `None`, fall back to the
    existing enumeration so single-container backends and unpinned
    profiles keep their current behavior.
  - `sync_profile_skeleton()` (current line ~286):
    1. Resolve `default` via `profile_default_container()` once at the
       top.
    2. If `default` is non-empty, call
       `purge_out_of_scope(db_profile=profile, container=default)`
       before `start_skeleton_sync`. Skip purge when unpinned
       (preserves legacy multi-container behavior).
    3. Inject `is_cancelled(profile)` checkpoints at three loop heads:
       outer `for container in containers`, middle
       `for schema in schemas`, inner `for asset in assets`.
    4. On cancellation, call
       `finish_skeleton_sync(profile, ok=False, error="cancelled")`
       and return; do not raise.
    5. Wrap normal/error/cancel exit in a `try/finally` that calls
       `_skeleton_jobs.unregister(profile)`.

- **`amx/web/routers/catalog.py`**
  - The existing `POST /sync` endpoint calls
    `_skeleton_jobs.register(profile)` for each target before spawning
    the daemon thread.
  - New endpoint: `POST /api/catalog/sync/cancel` with body
    `{"profile": "<name>"}`. Handler calls
    `_skeleton_jobs.cancel(profile)` and returns
    `{"cancelled": bool}` synchronously. Returns 404 when no job is
    registered for that profile.

- **`amx/cli_support/commands/db.py`**
  - New REPL command surface under the existing `db` namespace:
    `/db sync stop`. Wizard-first: bare invocation lists running syncs
    (from the registry), user picks one, confirmation prompt, then
    call the Python helper. `--profile <name>` is the optional
    power-user shortcut.
  - Python helper exposed from `amx/search/__init__.py` as
    `cancel_skeleton_sync(profile)` — thin wrapper over
    `_skeleton_jobs.cancel`.

- **`amx/storage/_history_caches.py`**
  - New function:
    ```python
    def purge_out_of_scope(
        *, db_profile: str, container: str
    ) -> dict[str, int]:
    ```
    Deletes rows from `catalog_entities`, `schemas_cache`, and
    `column_comments_cache` where the row's database/catalog does not
    match `container`. Single transaction. Returns deletion counts per
    table for the audit log. Idempotent (re-runs are no-ops).
  - Catalog descriptions / profile state side tables: if FK CASCADE is
    in place, no extra DELETE; otherwise add a second DELETE inside
    the same transaction.

### Storage schema

No schema change. No new columns. `amx/storage/schema_descriptions.py`
is not touched. The cache keys are already correctly shaped; we are
tightening write discipline, not changing storage.

## Data flow

### Sync start (e.g. `POST /api/catalog/sync?profile=prod_dwh`)

1. Handler calls `_skeleton_jobs.register("prod_dwh")` → cancel event.
2. `catalog.start_skeleton_sync(profile, total_tables=0)`.
3. Daemon thread spawn → `sync_profile_skeleton(cfg, "prod_dwh", catalog)`.
4. `sync_profile_skeleton` first resolves
   `default = profile_default_container(cfg.db_profiles[profile])`.
5. If `default` is non-empty:
   `purge_out_of_scope(db_profile=profile, container=default)` in one
   transaction. Audit log entry emitted.
6. `_enumerate_containers()` returns `[default]` (no connector
   enumeration). When `default` is `None`, legacy enumeration path
   runs.
7. Outer loop `for container in containers` — checkpoint
   `if is_cancelled(profile): break`.
8. Middle `for schema in schemas` — same checkpoint.
9. Inner `for asset in assets` — same checkpoint before
   `_upsert_entity`.
10. Normal end → `finish_skeleton_sync(ok=True)` →
    `_skeleton_jobs.unregister(profile)`.
11. Cancellation → `finish_skeleton_sync(ok=False, error="cancelled")`
    → `_skeleton_jobs.unregister(profile)`.
12. Exception → `finish_skeleton_sync(ok=False, error=str(exc))` →
    `_skeleton_jobs.unregister(profile)`.

### Cancel (e.g. Studio "Stop" button)

1. Studio sends `POST /api/catalog/sync/cancel {"profile":"prod_dwh"}`.
2. Handler calls `_skeleton_jobs.cancel("prod_dwh")` → event set.
3. Endpoint returns `{"cancelled": true}` immediately.
4. The running thread reaches the next checkpoint, finishes the
   in-flight table, exits the loop.
5. `finish_skeleton_sync(ok=False, error="cancelled")` flips
   `catalog_profile_state` so the freshness pill renders the
   cancelled state on the next poll.
6. Rows already written remain in the cache (cooperative semantics).

### Idempotency and races

- Re-running sync immediately after cancel: `register` returns the
  existing event if not yet `unregister`-ed; otherwise creates a fresh
  one. The new thread observes the right event.
- Double-clicking sync start while one is running is out of scope —
  the existing code does not guard against it; we keep that behavior.
- `purge_out_of_scope` is a `DELETE WHERE`, not a `DROP`, so repeated
  calls are no-ops once the cache is in scope.

## Critical files

- `amx/search/drift.py` — enumerator + main sync loop (heaviest edit)
- `amx/web/routers/catalog.py` — sync start + new cancel endpoint
- `amx/cli_support/commands/db.py` — `/db sync stop` wizard
- `amx/storage/_history_caches.py` — `purge_out_of_scope`
- `amx/search/_skeleton_jobs.py` — **new**, cancel registry
- `amx/db/_default_scope.py` — **new**, default-container helper
- `amx/search/__init__.py` — re-export `cancel_skeleton_sync`

## Reused existing pieces

- `catalog.start_skeleton_sync` / `finish_skeleton_sync` in
  `amx/search/_catalog/sync.py:100+` — state-machine API for the
  freshness pill. The cancel path reuses
  `finish_skeleton_sync(ok=False)` with a `"cancelled"` error string;
  no new state column is needed.
- `_history_caches.lookup_schemas_cache`
  (`amx/storage/_history_caches.py:490-510`) and
  `lookup_column_comments_cache` (`amx/db/connector.py:648-656`)
  already filter reads by `database` / `catalog` — they need no change.
- Connector field accessor `_cache_database_key()` at
  `amx/db/connector.py:634-636` already picks `database or catalog` —
  the new `profile_default_container()` helper mirrors and extends
  this for per-backend uniformity.

## Verification

End-to-end test plan once the implementation lands:

1. **Unit — default helper.** Parametrize `profile_default_container`
   over each backend's `DBConfig` shape (Databricks `catalog`,
   BigQuery `dataset`, Snowflake/Hive/Postgres/MySQL/MSSQL `database`,
   empty profile). Add to `tests/db/test_default_scope.py`.

2. **Unit — cancel registry.** `register`, `cancel`, `is_cancelled`,
   `unregister`, double-register, cancel-with-no-job. Add to
   `tests/search/test_skeleton_jobs.py`.

3. **Unit — purge.** Seed `catalog_entities`, `schemas_cache`,
   `column_comments_cache` with mixed-scope rows; call
   `purge_out_of_scope(db_profile=p, container=c)`; assert only
   matching rows survive. Idempotency: re-call is a no-op. Add to
   `tests/storage/test_history_caches_purge.py`.

4. **Integration — skeleton sync respects pinned default.** Using a
   fake connector that reports two catalogs, with a profile pinned to
   one, run `sync_profile_skeleton` and assert `catalog_entities`
   contains rows only for the pinned catalog. Add to
   `tests/search/test_drift_pinned_default.py`.

5. **Integration — cooperative cancel.** Fake connector that yields
   schemas with a deliberate delay; spawn sync; call
   `cancel_skeleton_sync(profile)`; assert thread exits within a
   bounded time, `catalog_profile_state` shows `error="cancelled"`,
   and partial rows remain in `catalog_entities`. Add to
   `tests/search/test_drift_cancel.py`.

6. **HTTP — cancel endpoint.** TestClient hits
   `POST /api/catalog/sync` then `POST /api/catalog/sync/cancel`.
   Assert 200 + `cancelled=true` and the registry shows no job
   afterwards. Add to `tests/web/routers/test_catalog_sync_cancel.py`.

7. **Manual smoke — REPL.** `/db profile add` a pinned
   Databricks/BigQuery profile, kick off a sync, then in a second
   terminal `/db sync stop`. Verify state pill, log, and
   `catalog_entities` contents in the dev history DB.

8. **Manual smoke — Studio.** Start sync from the `DbCache` page,
   click Stop, observe the freshness pill flip to "cancelled".
   Re-trigger to confirm the purge cleaned legacy out-of-scope rows.

9. **No-regression.**
   - Run the existing `tests/test_db_cache_ops.py` and
     `tests/test_catalog_cache_hardening.py` suites unchanged — they
     must stay green (cache key shape did not change).
   - Critical-path benchmark: time `sync_profile_skeleton` against a
     fixture profile before/after. Confirm under 5% regression.

10. **Studio deploy.** This change touches the Studio `DbCache` page
    (Stop button becomes functional). Run the project's deploy
    script before opening the PR so the remote Studio reflects the
    new endpoint when reviewers click through.

## Out of scope

- `SkeletonSyncJob` class refactor or async migration.
- Guarding against concurrent `start` for the same profile.
- Resume-from-checkpoint after cancel (deferred — covered as a
  follow-up if needed).
- Changes to `schemas_cache` / `column_comments_cache` key shape.
- The scheduler launchd job (`com.amx.scheduler`) — separate concern.
