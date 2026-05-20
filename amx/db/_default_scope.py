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
