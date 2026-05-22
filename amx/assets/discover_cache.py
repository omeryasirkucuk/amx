"""Lazy discover-tree cache reads + writes (PR-E).

Keeps the router thin. Three operations:

* :func:`read_children` — fetch immediate children for a parent
  from cache. Returns ``(rows, parent_fetched_at)`` where
  ``parent_fetched_at`` is ``None`` when children have never been
  listed (caller falls back to a fetch).
* :func:`refresh_parent` — atomic per-parent replace: drop old
  children + insert new ones + stamp the parent row's
  ``children_fetched_at``.
* :func:`walk_full` — drop all rows for (profile, kind), then
  insert every leaf yielded by a recursive walker plus the
  synthetic directory ancestors.
"""

from __future__ import annotations

import time
from collections.abc import Iterable
from typing import Any

from amx.db.adapters.remote_asset_types import WorkspaceEntry


def _row_dict(row: Any) -> dict[str, Any]:
    # ``sqlite3.Row`` iterates values when used in ``for k in row`` —
    # we need the column names, which only ``.keys()`` exposes. The
    # ``noqa: SIM118`` silences ruff's false positive (the lint is
    # only correct for dict-like objects, not for sqlite3.Row).
    if hasattr(row, "keys"):
        return {k: row[k] for k in row.keys()}  # noqa: SIM118
    return dict(row)


def read_children(
    conn: Any, *, profile: str, kind: str, parent_path: str
) -> tuple[list[dict[str, Any]], float | None]:
    """Return (rows, parent_fetched_at) when children are cached.

    ``parent_fetched_at`` is ``None`` when the parent row exists
    but its children have not been listed yet (treated as a cache
    miss). Root parent (``parent_path=''``) uses a synthetic marker
    row at ``path=''``.
    """
    # Root parent uses the synthetic marker row keyed by path=''.
    if parent_path == "":
        marker = conn.execute(
            "SELECT children_fetched_at FROM remote_workspace_tree "
            "WHERE profile_name = ? AND kind = ? AND path = '' AND is_directory = 1",
            (profile, kind),
        ).fetchone()
        if marker is None:
            return [], None
        fetched_at = marker["children_fetched_at"] if hasattr(marker, "keys") else marker[0]
        if fetched_at is None:
            return [], None
    else:
        parent_row = conn.execute(
            "SELECT children_fetched_at FROM remote_workspace_tree "
            "WHERE profile_name = ? AND kind = ? AND path = ?",
            (profile, kind, parent_path),
        ).fetchone()
        if parent_row is None:
            return [], None
        fetched_at = (
            parent_row["children_fetched_at"] if hasattr(parent_row, "keys") else parent_row[0]
        )
        if fetched_at is None:
            return [], None
    # The synthetic root-marker row lives at path='' parent_path=''
    # solely to carry the root-level ``children_fetched_at`` stamp.
    # Filter it out so callers never see a phantom empty-path child.
    rows = conn.execute(
        "SELECT path, parent_path, name, is_directory, external_id, "
        "owner, last_modified, children_fetched_at, fetched_at "
        "FROM remote_workspace_tree "
        "WHERE profile_name = ? AND kind = ? AND parent_path = ? AND path != ''",
        (profile, kind, parent_path),
    ).fetchall()
    return [_row_dict(r) for r in rows], float(fetched_at)


def refresh_parent(
    conn: Any,
    *,
    profile: str,
    kind: str,
    parent_path: str,
    entries: Iterable[WorkspaceEntry],
) -> int:
    """Atomically replace ``parent_path``'s immediate children."""
    now = time.time()
    materialised = list(entries)
    conn.execute(
        "DELETE FROM remote_workspace_tree WHERE profile_name = ? AND kind = ? AND parent_path = ?",
        (profile, kind, parent_path),
    )
    for entry in materialised:
        conn.execute(
            "INSERT OR REPLACE INTO remote_workspace_tree "
            "(profile_name, kind, path, parent_path, name, is_directory, "
            "external_id, owner, last_modified, children_fetched_at, fetched_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?)",
            (
                profile,
                kind,
                entry.path,
                parent_path,
                entry.name,
                1 if entry.is_directory else 0,
                entry.external_id,
                entry.owner,
                entry.last_modified.isoformat() if entry.last_modified else None,
                now,
            ),
        )
    if parent_path == "":
        conn.execute(
            "INSERT OR REPLACE INTO remote_workspace_tree "
            "(profile_name, kind, path, parent_path, name, is_directory, "
            "external_id, owner, last_modified, children_fetched_at, fetched_at) "
            "VALUES (?, ?, '', '', '', 1, NULL, NULL, NULL, ?, ?)",
            (profile, kind, now, now),
        )
    else:
        conn.execute(
            "UPDATE remote_workspace_tree "
            "SET children_fetched_at = ?, fetched_at = ? "
            "WHERE profile_name = ? AND kind = ? AND path = ?",
            (now, now, profile, kind, parent_path),
        )
    conn.commit()
    return len(materialised)


def walk_full(
    conn: Any,
    *,
    profile: str,
    kind: str,
    leaves: Iterable[Any],
) -> dict[str, int]:
    """Replace every row for (profile, kind) with the walk result.

    Synthesises ancestor directories so the tree picker can render
    after a walk without further fetches.
    """
    now = time.time()
    conn.execute(
        "DELETE FROM remote_workspace_tree WHERE profile_name = ? AND kind = ?",
        (profile, kind),
    )
    leaves_written = 0
    directories_written = 0
    dirs_seen: set[str] = set()
    for leaf in leaves:
        leaf_path = leaf.path or leaf.external_id
        if not leaf_path:
            continue
        if "/" in leaf_path:
            parts = leaf_path.split("/")
            for i in range(1, len(parts)):
                ancestor = "/".join(parts[:i]) or "/"
                if ancestor in dirs_seen:
                    continue
                dirs_seen.add(ancestor)
                ancestor_parent = "/".join(parts[: i - 1]) or ""
                if ancestor == "/":
                    ancestor_parent = ""
                ancestor_name = parts[i - 1] if i > 0 else "/"
                conn.execute(
                    "INSERT OR REPLACE INTO remote_workspace_tree "
                    "(profile_name, kind, path, parent_path, name, is_directory, "
                    "external_id, owner, last_modified, children_fetched_at, fetched_at) "
                    "VALUES (?, ?, ?, ?, ?, 1, NULL, NULL, NULL, ?, ?)",
                    (profile, kind, ancestor, ancestor_parent, ancestor_name, now, now),
                )
                directories_written += 1
        leaf_parent = leaf_path.rsplit("/", 1)[0] if "/" in leaf_path else ""
        conn.execute(
            "INSERT OR REPLACE INTO remote_workspace_tree "
            "(profile_name, kind, path, parent_path, name, is_directory, "
            "external_id, owner, last_modified, children_fetched_at, fetched_at) "
            "VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?, NULL, ?)",
            (
                profile,
                kind,
                leaf_path,
                leaf_parent,
                leaf.name,
                leaf.external_id,
                leaf.owner,
                leaf.last_modified.isoformat() if leaf.last_modified else None,
                now,
            ),
        )
        leaves_written += 1
    conn.execute(
        "INSERT OR REPLACE INTO remote_workspace_tree "
        "(profile_name, kind, path, parent_path, name, is_directory, "
        "external_id, owner, last_modified, children_fetched_at, fetched_at) "
        "VALUES (?, ?, '', '', '', 1, NULL, NULL, NULL, ?, ?)",
        (profile, kind, now, now),
    )
    conn.commit()
    return {
        "rows_written": leaves_written + directories_written,
        "directories": directories_written,
        "leaves": leaves_written,
    }


__all__ = ["read_children", "refresh_parent", "walk_full"]
