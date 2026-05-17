"""Run-variation tree + rerun-snapshot helpers for :class:`SQLiteHistoryStore`.

Extracted from :mod:`amx.storage.sqlite_store`. Eight methods drive
the genealogy/variation tree (``get_descendant_runs`` 178 LOC,
``get_result_chain``) and the rerun-context snapshot CRUD
(``next_rerun_seq`` / save / read / list / delete / gc).

Each function takes the store as ``hs`` and uses its ``_connect()``
plumbing. No DDL touched.
"""

from __future__ import annotations

import contextlib
import json
import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from amx.storage.sqlite_store import SQLiteHistoryStore


def get_descendant_runs(
    hs: SQLiteHistoryStore,
    run_id: int,
    *,
    variations_depth_cap: int = 3,
    rerun_depth_cap: int = 1,
) -> list[dict[str, Any]]:
    """Return the descendant Variations + Re-Run runs for *run_id*.

    Variations descendants are looked up via
    ``run_results.parent_run_id``; Re-Run descendants via the
    ``rerun`` ``analysis_runs.command`` plus
    ``settings_json.parent_run_id``. Variations recurse up to
    ``variations_depth_cap`` levels (default 3); deeper rows are
    flattened into the deepest visible parent with an
    ``over_max_depth`` flag so the frontend can render a "(nested)"
    indicator. Re-Run descend one level only (a re-run of a re-run
    is just another re-run of the original asset).

    The shape mirrors :func:`amx.web.routers.history.get_run_results`
    descendants block — both Studio (``GET /api/runs/{id}/results``)
    and CLI (``/history show``) consume the same tree.
    """
    out: list[dict[str, Any]] = []
    visited: set[int] = {int(run_id)}

    def _child_status(child_run_id: int) -> str | None:
        """Read ``analysis_runs.status`` for a descendant. Surfaced
        on the tree entry so the Studio can render a refresh-safe
        ``Generating variations…`` indicator after a page reload
        during execution — without this field the spinner is
        local-state only and a refresh wipes it."""
        with hs._connect() as conn:
            row = conn.execute(
                "SELECT status FROM analysis_runs WHERE id = ?",
                (int(child_run_id),),
            ).fetchone()
        return str(row["status"]) if row and row["status"] else None

    def _first_signal_in(rows_for_run: list[dict[str, Any]]) -> str | None:
        """Surface the confidence signal active when this descendant
        ran (e.g. ``self_consistency`` / ``logprob`` / ``judge``).
        Read from the first entry of the first row's
        ``alternatives_json`` since signal is per-alternative.
        Drives the version-group header label so a reviewer sees
        badge-type differences between v1 and vN at a glance."""
        for rr in rows_for_run:
            alts = rr.get("alternatives_json")
            if isinstance(alts, list):
                for entry in alts:
                    if isinstance(entry, dict) and entry.get("signal"):
                        return str(entry["signal"])
        return None

    def _collect_variations(parent_id: int, depth: int) -> None:
        with hs._connect() as conn:
            rows = conn.execute(
                "SELECT DISTINCT run_id FROM run_results WHERE parent_run_id = ? ORDER BY run_id",
                (int(parent_id),),
            ).fetchall()
        for r in rows:
            child_run_id = int(r["run_id"])
            if child_run_id in visited:
                continue
            visited.add(child_run_id)
            rows_for_run = hs.get_run_results(child_run_id)
            first_seed = next(
                (
                    rr.get("seed_alternative_id")
                    for rr in rows_for_run
                    if rr.get("seed_alternative_id")
                ),
                None,
            )
            # Surface the verbatim seed text + mode + model on the
            # tree entry so the Studio header chip + the inline
            # version groups can render the full lineage breadcrumb
            # without an extra per-row scan client-side.
            first_seed_text = next(
                (
                    rr.get("seed_alternative_text")
                    for rr in rows_for_run
                    if rr.get("seed_alternative_text")
                ),
                None,
            )
            first_mode = next(
                (rr.get("alternatives_mode") for rr in rows_for_run if rr.get("alternatives_mode")),
                None,
            )
            first_model = next(
                (rr.get("model") for rr in rows_for_run if rr.get("model")),
                None,
            )
            first_provider = next(
                (rr.get("provider") for rr in rows_for_run if rr.get("provider")),
                None,
            )
            entry = {
                "run_id": child_run_id,
                "kind": "variations",
                "seed_alternative_id": first_seed,
                "seed_alternative_text": first_seed_text,
                "mode": first_mode,
                "model": first_model,
                "provider": first_provider,
                "status": _child_status(child_run_id),
                "confidence_signal": _first_signal_in(rows_for_run),
                "depth": depth,
                "over_max_depth": depth > variations_depth_cap,
                "rows": rows_for_run,
            }
            out.append(entry)
            if depth < variations_depth_cap:
                _collect_variations(child_run_id, depth + 1)

    def _collect_reruns(parent_id: int) -> None:
        with hs._connect() as conn:
            rows = conn.execute(
                "SELECT id, settings_json FROM analysis_runs WHERE command = 'rerun' ORDER BY id",
            ).fetchall()
        for r in rows:
            child_run_id = int(r["id"])
            if child_run_id in visited:
                continue
            settings_raw = r["settings_json"]
            try:
                settings = json.loads(settings_raw) if isinstance(settings_raw, str) else {}
            except Exception:
                settings = {}
            if not isinstance(settings, dict):
                continue
            if int(settings.get("parent_run_id") or 0) != int(parent_id):
                continue
            visited.add(child_run_id)
            rows_for_run = hs.get_run_results(child_run_id)
            first_mode = next(
                (rr.get("alternatives_mode") for rr in rows_for_run if rr.get("alternatives_mode")),
                None,
            )
            first_model = next(
                (rr.get("model") for rr in rows_for_run if rr.get("model")),
                None,
            )
            first_provider = next(
                (rr.get("provider") for rr in rows_for_run if rr.get("provider")),
                None,
            )
            out.append(
                {
                    "run_id": child_run_id,
                    "kind": "rerun",
                    "seed_alternative_id": None,
                    "mode": first_mode,
                    "model": first_model,
                    "provider": first_provider,
                    "status": _child_status(child_run_id),
                    "confidence_signal": _first_signal_in(rows_for_run),
                    "depth": 1,
                    "over_max_depth": False,
                    "rows": rows_for_run,
                }
            )
            # Re-Run depth cap = 1; do not recurse.

    _collect_variations(int(run_id), depth=1)
    if rerun_depth_cap >= 1:
        _collect_reruns(int(run_id))
    return out


def get_result_chain(hs: SQLiteHistoryStore, result_id: int) -> list[dict[str, Any]]:
    """Return the full version chain (original + all re-runs) for an item.

    Walks ``parent_result_id`` upward to find the chain root, then
    fetches every row whose ``parent_result_id`` matches that root
    (plus the root itself), ordered by ``rerun_seq`` ASC. Used by
    the Studio history drawer + ``GET /api/history/runs/...?include_history=true``.
    """
    with hs._connect() as conn:
        row = conn.execute(
            "SELECT id, parent_result_id FROM run_results WHERE id = ?",
            (int(result_id),),
        ).fetchone()
        if row is None:
            return []
        root = int(row["id"])
        seen: set[int] = set()
        while True:
            if root in seen:
                break
            seen.add(root)
            parent_row = conn.execute(
                "SELECT parent_result_id FROM run_results WHERE id = ?",
                (root,),
            ).fetchone()
            parent = (
                int(parent_row["parent_result_id"])
                if parent_row and parent_row["parent_result_id"] is not None
                else None
            )
            if parent is None or parent == root:
                break
            root = parent
        chain_rows = conn.execute(
            """
            SELECT * FROM run_results
            WHERE id = ? OR parent_result_id = ?
            ORDER BY rerun_seq ASC, id ASC
            """,
            (root, root),
        ).fetchall()
    out: list[dict[str, Any]] = []
    for r in chain_rows:
        d = dict(r)
        raw = d.get("alternatives_json")
        if isinstance(raw, str) and raw:
            with contextlib.suppress(Exception):
                d["alternatives_json"] = json.loads(raw)
        cite_raw = d.get("citations_json")
        if isinstance(cite_raw, str) and cite_raw:
            with contextlib.suppress(Exception):
                d["citations_json"] = json.loads(cite_raw)
        out.append(d)
    return out


def next_rerun_seq(hs: SQLiteHistoryStore, parent_result_id: int) -> int:
    """Return the next ``rerun_seq`` to use for a re-run targeting this item.

    Looks at the chain root (parent_result_id, which already points at
    the original) and returns ``max(rerun_seq) + 1`` so concurrent
    re-runs receive monotonically increasing sequence numbers.
    """
    with hs._connect() as conn:
        row = conn.execute(
            """
            SELECT COALESCE(MAX(rerun_seq), 0) AS mx
            FROM run_results
            WHERE id = ? OR parent_result_id = ?
            """,
            (int(parent_result_id), int(parent_result_id)),
        ).fetchone()
    return int((row["mx"] if row else 0) or 0) + 1


def save_rerun_snapshot(
    hs: SQLiteHistoryStore,
    *,
    snapshot_id: str,
    job_id: str,
    target_result_id: int,
    payload: dict[str, Any],
) -> None:
    """Persist a frozen ``AgentContext`` for the re-run worker to read.

    Snapshots are short-lived: the worker deletes them in its
    ``finally`` block once the job terminates (done / failed /
    cancelled), and ``gc_orphan_rerun_snapshots`` sweeps anything
    left over from a crash on next process startup.
    """
    with hs._lock, hs._connect() as conn:
        conn.execute(
            """
            INSERT INTO rerun_context_snapshots
                (snapshot_id, job_id, target_result_id, payload_json, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                str(snapshot_id),
                str(job_id),
                int(target_result_id),
                json.dumps(payload, ensure_ascii=True),
                time.time(),
            ),
        )


def read_rerun_snapshot(hs: SQLiteHistoryStore, snapshot_id: str) -> dict[str, Any] | None:
    """Return the deserialized snapshot payload, or ``None`` when missing."""
    with hs._connect() as conn:
        row = conn.execute(
            "SELECT payload_json, target_result_id, job_id "
            "FROM rerun_context_snapshots WHERE snapshot_id = ?",
            (str(snapshot_id),),
        ).fetchone()
    if row is None:
        return None
    try:
        payload = json.loads(row["payload_json"])
    except Exception:
        return None
    return {
        "snapshot_id": str(snapshot_id),
        "job_id": str(row["job_id"]),
        "target_result_id": int(row["target_result_id"]),
        "payload": payload,
    }


def list_rerun_snapshots_for_job(hs: SQLiteHistoryStore, job_id: str) -> list[dict[str, Any]]:
    """Return all snapshot rows for one job (ordered by created_at)."""
    with hs._connect() as conn:
        rows = conn.execute(
            """
            SELECT snapshot_id, target_result_id, payload_json, created_at
            FROM rerun_context_snapshots
            WHERE job_id = ?
            ORDER BY created_at ASC
            """,
            (str(job_id),),
        ).fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        try:
            payload = json.loads(r["payload_json"])
        except Exception:
            continue
        out.append(
            {
                "snapshot_id": str(r["snapshot_id"]),
                "target_result_id": int(r["target_result_id"]),
                "payload": payload,
                "created_at": float(r["created_at"]),
            }
        )
    return out


def delete_rerun_snapshots_for_job(hs: SQLiteHistoryStore, job_id: str) -> int:
    """Drop every snapshot row owned by a finished job. Returns count."""
    with hs._lock, hs._connect() as conn:
        cur = conn.execute(
            "DELETE FROM rerun_context_snapshots WHERE job_id = ?",
            (str(job_id),),
        )
        return int(cur.rowcount or 0)


def gc_orphan_rerun_snapshots(hs: SQLiteHistoryStore, *, max_age_seconds: float = 3600.0) -> int:
    """Sweep snapshots older than ``max_age_seconds`` (default 1h).

    Called once at AMX Studio startup (and at CLI bootstrap) so
    any rows left behind by a crashed worker don't accumulate.
    Returns the number of rows deleted.
    """
    cutoff = time.time() - float(max_age_seconds)
    with hs._lock, hs._connect() as conn:
        cur = conn.execute(
            "DELETE FROM rerun_context_snapshots WHERE created_at < ?",
            (cutoff,),
        )
        return int(cur.rowcount or 0)
