"""Process-tree-wide advisory lock keyed by (db_profile, schema, table).

Backed by a SQLite file inside ``$AMX_CONFIG_DIR``. The lock table holds
one row per active holder; ``acquire()`` does an INSERT inside a
short-lived connection and lets the PRIMARY KEY on the 3-tuple block
contenders, polling until either it gets in or the supplied timeout
elapses.

Why SQLite and not ``threading.Lock``:

1. The same primitive must keep working if a second AMX process somehow
   targets the same config dir. The AMX single-instance config lock
   should normally prevent that, but defence-in-depth is cheap here.
2. The row holds the holder's PID + thread id + acquisition timestamp,
   which lets a future sweep reclaim a lock left behind by a crashed
   worker. The sweep is out of scope for the initial helper; the schema
   already supports it.

The lock is **per (db_profile, schema, table)**: two run workers writing
metadata for *different* tables run fully in parallel; two workers
targeting the *same* table serialise on that table only.
"""

from __future__ import annotations

import contextlib
import os
import sqlite3
import threading
import time
from collections.abc import Iterator

_SCHEMA = """
CREATE TABLE IF NOT EXISTS advisory_locks (
    db_profile TEXT NOT NULL,
    schema_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    holder_pid INTEGER NOT NULL,
    holder_thread INTEGER NOT NULL,
    acquired_at REAL NOT NULL,
    PRIMARY KEY (db_profile, schema_name, table_name)
);
"""


class AdvisoryLockStore:
    """File-backed advisory lock keyed by ``(db_profile, schema, table)``."""

    def __init__(self, path: str) -> None:
        self._path = path
        self._init()

    def _init(self) -> None:
        with sqlite3.connect(self._path, timeout=30) as conn:
            # WAL keeps readers non-blocking; the timeout matters only
            # when two would-be holders contend on the PRIMARY KEY.
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=30000")
            conn.executescript(_SCHEMA)

    @contextlib.contextmanager
    def acquire(
        self,
        key: tuple[str, str, str],
        *,
        timeout_sec: float = 60.0,
        poll_sec: float = 0.02,
    ) -> Iterator[None]:
        """Block until *key* is held by the current thread, then yield.

        Raises ``TimeoutError`` if *timeout_sec* elapses before the
        lock can be inserted. On exit the lock is released
        unconditionally (the caller's exception, if any, propagates).
        """
        db_profile, schema_name, table_name = key
        pid = os.getpid()
        thread = threading.get_ident()
        deadline = time.monotonic() + timeout_sec

        while True:
            try:
                with sqlite3.connect(self._path, timeout=5) as conn:
                    conn.execute(
                        "INSERT INTO advisory_locks "
                        "(db_profile, schema_name, table_name, "
                        "holder_pid, holder_thread, acquired_at) "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        (
                            db_profile,
                            schema_name,
                            table_name,
                            pid,
                            thread,
                            time.time(),
                        ),
                    )
                break
            except sqlite3.IntegrityError:
                if time.monotonic() >= deadline:
                    raise TimeoutError(
                        f"advisory lock {key!r} not acquired within {timeout_sec}s"
                    ) from None
                time.sleep(poll_sec)

        try:
            yield
        finally:
            with sqlite3.connect(self._path, timeout=5) as conn:
                conn.execute(
                    "DELETE FROM advisory_locks "
                    "WHERE db_profile=? AND schema_name=? "
                    "AND table_name=? AND holder_pid=? "
                    "AND holder_thread=?",
                    (db_profile, schema_name, table_name, pid, thread),
                )
