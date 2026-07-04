"""Clear a single table's review data across the three stores that hold it.

A "review" for a ``(schema, table)`` lives in three places:

* **Pending suggestions** — unapplied generated descriptions in
  ``~/.amx/pending_metadata.json`` (surfaced as "Pending review" pills).
* **Review state** — the accept / skip / custom decisions recorded on
  ``run_results`` rows in the local history DB.
* **Applied-description audit** — the ``apply_events`` rows that record
  descriptions previously written to the live database.

This module orchestrates clearing any subset of the three so the CLI
(``/review-clear``) and the Studio Table page share one code path. It is
deliberately transport-agnostic: no FastAPI / Click imports, just the
storage + pending-queue calls.
"""

from __future__ import annotations

from typing import Any

from amx.pending_review import clear_pending_for_table


def clear_table_reviews(
    store: Any,
    schema: str,
    table: str,
    *,
    pending: bool = True,
    review_state: bool = True,
    audit: bool = True,
) -> dict[str, int]:
    """Clear the selected review categories for one ``(schema, table)``.

    ``store`` is an initialized history store (local ``SQLiteHistoryStore``
    or the shared-mode façade). Each flag toggles one category; a flag left
    ``False`` skips that category and reports ``0``.

    Returns a per-category counts dict::

        {"pending": n, "review_state": m, "audit": k}

    Note: clearing the audit category removes AMX's *record* of what was
    written — it never touches the live-database COMMENTs themselves.
    """
    counts = {"pending": 0, "review_state": 0, "audit": 0}
    if pending:
        counts["pending"] = clear_pending_for_table(schema, table)
    if review_state:
        counts["review_state"] = store.reset_review_state_for_table(schema, table)
    if audit:
        counts["audit"] = store.delete_apply_events_for_table(schema, table)
    return counts
