"""Optimistic concurrency control primitives for shared-store edits.

Every concurrent-edit table carries a ``version`` column. UPDATEs use
``WHERE id = ? AND version = ?`` so a stale writer cannot overwrite a
concurrent edit by another teammate. When the precondition matches no
rows, :class:`StaleVersionError` is raised so the caller can show a
conflict resolution UI (Studio diff dialog or CLI prompt).
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any


@dataclass(frozen=True)
class StaleVersionSnapshot:
    """Snapshot of the warehouse row when a stale-write attempt was made."""

    version: int
    updated_by: str
    updated_at: Any  # datetime
    current_value: dict[str, Any]


class StaleVersionError(Exception):
    """Raised when an UPDATE is attempted against an outdated row version."""

    def __init__(
        self,
        *,
        resource: str,
        expected_version: int,
        actual: StaleVersionSnapshot,
    ) -> None:
        self.resource = resource  # e.g. "lineage_comment:uuid:xyz"
        self.expected_version = expected_version
        self.actual = actual
        super().__init__(
            f"Stale version on {resource}: "
            f"expected={expected_version}, actual={actual.version}, "
            f"changed by {actual.updated_by}"
        )


class ConflictResolution(str, Enum):
    CANCEL = "cancel"
    OVERWRITE = "overwrite"  # force, version=actual.version
    MERGE = "merge"  # user-supplied merged value


__all__ = [
    "ConflictResolution",
    "StaleVersionError",
    "StaleVersionSnapshot",
]
