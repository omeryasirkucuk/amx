# Parallel-run safety audit (Phase 0)

**Goal:** Establish that AMX can host two simultaneous run workers in the
same install without data corruption or deadlock — the prerequisite for
allowing concurrent scheduled runs.

**Scope:** SQLite history store (always-on, source of truth for the
local install) and the Chroma RAG store. The remote SQLAlchemy half of
the dual-write façade is intentionally out of scope here: it is
best-effort with an outbox, so any contention surfaces as queued
retries rather than data loss.

---

## SQLite history store

The local history store already enables a concurrency-friendly
configuration on every connection
(`amx/storage/sqlite_store.py:1977-1979`):

```python
conn.execute("PRAGMA journal_mode=WAL")
conn.execute("PRAGMA synchronous=NORMAL")
conn.execute("PRAGMA busy_timeout=30000")
```

WAL allows non-blocking readers and one writer; `busy_timeout=30000`
gives a contending writer 30 seconds to retry before raising
`database is locked`. The hot path scheduled-run workers will exercise
is `create_run` → many `increment_run_processed` → `finish_run`.

The smoke test
`tests/runtime/test_sqlite_concurrent_writes.py::test_two_workers_can_create_and_finish_runs_concurrently`
fires that exact path from two threads against the same database file
and asserts no exceptions are raised on either thread. It passes
reliably on the developer's machine. The intermediate
`increment_run_processed` writes are bounded `UPDATE` statements
holding the `self._lock` (process-wide threading lock) only for the
duration of the statement; no transaction is held across a slow
operation.

**Conclusion (SQLite):** Safe out of the box for the scope this feature
ships.

---

## Chroma RAG store

The smoke test `tests/runtime/test_chroma_concurrent_writes.py` covers
two access patterns:

1. **Distinct collections, two threads.** Both threads' writes land;
   no exceptions; the final counts on each collection are exact.
2. **Same collection, two threads.** Both threads' writes land; no
   exceptions; the final count is the sum of both batches.

Both cases pass on a stock `chromadb` install. The internal locking
inside Chroma's persistent client appears to serialise on the
collection without raising, which is exactly the behaviour we need.

The test is `pytest.importorskip`-guarded so a stripped CI install
without `chromadb` skips it rather than failing.

**Conclusion (Chroma):** Safe out of the box for the scope this feature
ships.

---

## Advisory lock — defence in depth

Even though both stores tolerate concurrent writes, the scheduler engine
will still wrap per-table work in a SQLite-backed advisory lock keyed by
`(db_profile, schema_name, table_name)`. Reasons:

* Two runs targeting *the same table* almost certainly shouldn't race
  on metadata writes (LLM-generated descriptions of the same column
  could collide and the last-write-wins outcome would be confusing).
  Serialising per-table costs nothing when distinct tables are in
  flight, because the lock is keyed at table granularity.
* The lock row carries `holder_pid`, `holder_thread`, `acquired_at`,
  so a future crash-recovery sweep can reclaim stale rows from a
  worker that died mid-acquisition. (Sweep not implemented in this
  phase; the schema supports it.)
* The lock is a single small helper
  (`amx/runtime/advisory_lock.py`), independently tested, and easily
  removed if it ever proves unnecessary.

The lock is verified by three behaviour tests:

* `test_lock_serialises_two_holders` — same key, two threads; second
  thread cannot enter the critical section before the first exits.
* `test_lock_distinct_keys_run_in_parallel` — two threads with
  different keys enter within ~150 ms of each other; no incidental
  serialisation.
* `test_lock_timeout_raises` — `acquire(..., timeout_sec=0.2)`
  raises `TimeoutError` when the lock is held elsewhere.

---

## Open items (deliberately deferred)

* **Advisory-lock sweep for crashed holders.** Schema is ready; the
  Phase 2 scheduler engine can call a sweep at tick start. Not in
  this PR.
* **Two AMX *processes* sharing one config dir.** The AMX single-
  instance config lock should prevent this; the audit assumed it does
  and did not stress that path. If a future incident shows otherwise,
  the advisory lock's PID column gives us cross-process serialisation
  for free, but the test coverage would need to grow.
* **High-end contention behaviour.** The smoke tests run two workers.
  Tens of concurrent workers writing the same hot column has not been
  measured. Scheduled runs in this design fire one schedule at a time
  via `claim_due_schedule` (Phase 1), so the realistic concurrent
  count stays small. We can revisit if usage demands it.
