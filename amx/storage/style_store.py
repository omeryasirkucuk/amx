"""DAO for the `style_profiles` table.

Reads and writes happen through plain sqlite3, mirroring the
dependency surface of SQLiteHistoryStore. The shared SQLAlchemy
variant (history-store mode) is not wired here; style profiles are
local-only in v1.
"""

from __future__ import annotations

import os
import sqlite3
import time
from dataclasses import dataclass

from amx.llm.style.profile import StyleProfile


@dataclass(frozen=True)
class StoredStyleProfile:
    llm_profile: str
    source_ref: str
    source_db_kind: str
    profile: StyleProfile
    enabled: bool
    sample_count: int
    created_at: float
    updated_at: float


def _now() -> float:
    return time.time()


class StyleStore:
    def __init__(self, db_path: str | os.PathLike[str]) -> None:
        self._db_path = str(db_path)

    def _conn(self) -> sqlite3.Connection:
        c = sqlite3.connect(self._db_path)
        c.row_factory = sqlite3.Row
        return c

    def upsert(
        self,
        llm_profile: str,
        source_ref: str,
        source_db_kind: str,
        profile: StyleProfile,
        sample_count: int,
    ) -> None:
        now = _now()
        with self._conn() as c:
            existing = c.execute(
                "SELECT created_at FROM style_profiles WHERE llm_profile = ?",
                (llm_profile,),
            ).fetchone()
            created_at = float(existing["created_at"]) if existing else now
            c.execute(
                """
                INSERT INTO style_profiles
                    (llm_profile, source_ref, source_db_kind, profile_json,
                     enabled, sample_count, created_at, updated_at)
                VALUES (?, ?, ?, ?, 1, ?, ?, ?)
                ON CONFLICT(llm_profile) DO UPDATE SET
                    source_ref     = excluded.source_ref,
                    source_db_kind = excluded.source_db_kind,
                    profile_json   = excluded.profile_json,
                    sample_count   = excluded.sample_count,
                    updated_at     = excluded.updated_at
                """,
                (
                    llm_profile,
                    source_ref,
                    source_db_kind,
                    profile.to_json(),
                    sample_count,
                    str(created_at),
                    str(now),
                ),
            )

    def get(self, llm_profile: str) -> StoredStyleProfile | None:
        with self._conn() as c:
            row = c.execute(
                "SELECT * FROM style_profiles WHERE llm_profile = ?",
                (llm_profile,),
            ).fetchone()
        if not row:
            return None
        return StoredStyleProfile(
            llm_profile=row["llm_profile"],
            source_ref=row["source_ref"],
            source_db_kind=row["source_db_kind"],
            profile=StyleProfile.from_json(row["profile_json"]),
            enabled=bool(row["enabled"]),
            sample_count=int(row["sample_count"]),
            created_at=float(row["created_at"]),
            updated_at=float(row["updated_at"]),
        )

    def set_enabled(self, llm_profile: str, enabled: bool) -> None:
        with self._conn() as c:
            c.execute(
                "UPDATE style_profiles SET enabled = ?, updated_at = ? WHERE llm_profile = ?",
                (1 if enabled else 0, str(_now()), llm_profile),
            )

    def clear(self, llm_profile: str) -> None:
        with self._conn() as c:
            c.execute(
                "DELETE FROM style_profiles WHERE llm_profile = ?",
                (llm_profile,),
            )
