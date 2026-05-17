"""CRUD + seed helpers for the lineage logo registry.

The registry is a single sqlite table (``lineage_logos``) with two row
flavours:

* ``source='default'`` — seeded from
  :mod:`amx.lineage.default_logos` on history-store init. Idempotent
  via the ``UNIQUE(key, source)`` index.
* ``source='custom'`` — user-uploaded via the Studio's logo picker.
  Either an inline base64 ``data_url`` or an external ``url``.

The frontend hits these helpers through ``amx/web/routers/lineage.py``:

* ``GET    /api/lineage/logos``      → :func:`list_logos`
* ``POST   /api/lineage/logos``      → :func:`create_custom_logo`
* ``DELETE /api/lineage/logos/{id}`` → :func:`delete_custom_logo`

Logo nodes (placements on a saved canvas) live in
``lineage_logo_nodes`` and are managed through the same router via
:func:`list_logo_nodes`, :func:`create_logo_node`,
:func:`update_logo_node`, :func:`delete_logo_node`.
"""

from __future__ import annotations

import base64
import re
import time
from typing import Any

from amx.lineage.default_logos import DEFAULT_LOGOS, render_logo_svg

# ── byte-size guard on custom uploads ────────────────────────────────────
#
# Anything above this lands in a 413 rather than bloating the local
# sqlite file. SVG / PNG icons larger than 200 KB are almost certainly
# a misclick — a normal brand mark is 1-30 KB.
MAX_DATA_URL_BYTES = 200 * 1024

# ── data URL mime allowlist ──────────────────────────────────────────────
#
# Anything outside this set is rejected with 415. PDF / video / etc.
# would render as a broken <img> on the canvas; we'd rather fail at the
# API boundary.
ALLOWED_MIMES: frozenset[str] = frozenset(
    {"image/svg+xml", "image/png", "image/jpeg", "image/jpg", "image/webp"}
)

# ── slug-safe key regex ──────────────────────────────────────────────────
#
# Logo keys end up on URL paths and are joined into canvas node ids,
# so we restrict them to a tight alphabet (lowercase + digits + ``-``
# and ``_``). 1-40 chars. Anything else is a 400.
_KEY_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,39}$")


class LogoStoreError(RuntimeError):
    """Raised for validation failures (bad mime, oversized payload, …)."""

    def __init__(self, message: str, status_code: int = 400):
        super().__init__(message)
        self.status_code = status_code


# ── seed ─────────────────────────────────────────────────────────────────


def seed_default_logos(hs: Any) -> int:
    """Insert the 20 bundled default logos into ``lineage_logos``.

    Idempotent: re-runs on every init are no-ops once seeded thanks to
    the ``UNIQUE(key, source)`` constraint. Returns the number of rows
    that were actually inserted on this call (0 on warm restarts).
    """
    inserted = 0
    now = time.time()
    with hs._connect() as conn:
        for logo in DEFAULT_LOGOS:
            svg = render_logo_svg(logo)
            data_url = "data:image/svg+xml;base64," + base64.b64encode(svg.encode("utf-8")).decode(
                "ascii"
            )
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO lineage_logos
                    (key, label, category, source, data_url, url, created_at)
                VALUES (?, ?, ?, 'default', ?, '', ?)
                """,
                (logo.key, logo.label, logo.category, data_url, now),
            )
            if cur.rowcount:
                inserted += 1
    return inserted


# ── read ─────────────────────────────────────────────────────────────────


def list_logos(hs: Any) -> list[dict[str, Any]]:
    """Return every logo (default + custom), ordered by category then key."""
    with hs._connect() as conn:
        rows = conn.execute(
            """
            SELECT id, key, label, category, source, data_url, url, created_at
            FROM lineage_logos
            ORDER BY
                CASE category
                    WHEN 'cloud' THEN 0
                    WHEN 'warehouse' THEN 1
                    WHEN 'bi' THEN 2
                    WHEN 'tooling' THEN 3
                    ELSE 4
                END,
                source,
                key
            """
        ).fetchall()
    return [
        {
            "id": int(r[0]),
            "key": str(r[1] or ""),
            "label": str(r[2] or ""),
            "category": str(r[3] or ""),
            "source": str(r[4] or ""),
            "data_url": str(r[5] or ""),
            "url": str(r[6] or ""),
            "created_at": float(r[7] or 0.0),
        }
        for r in rows
    ]


def lookup_logo_by_key(hs: Any, key: str) -> dict[str, Any] | None:
    """Resolve ``key`` to a logo row.

    Prefers ``source='custom'`` over ``'default'`` so a user's shadow
    upload (same key, source=custom) wins for any consumer that
    references the logo purely by key.
    """
    if not key:
        return None
    with hs._connect() as conn:
        row = conn.execute(
            """
            SELECT id, key, label, category, source, data_url, url
            FROM lineage_logos
            WHERE key = ?
            ORDER BY CASE source WHEN 'custom' THEN 0 ELSE 1 END
            LIMIT 1
            """,
            (key,),
        ).fetchone()
    if not row:
        return None
    return {
        "id": int(row[0]),
        "key": str(row[1] or ""),
        "label": str(row[2] or ""),
        "category": str(row[3] or ""),
        "source": str(row[4] or ""),
        "data_url": str(row[5] or ""),
        "url": str(row[6] or ""),
    }


# ── create ───────────────────────────────────────────────────────────────


def create_custom_logo(
    hs: Any,
    *,
    key: str,
    label: str,
    category: str = "custom",
    data_url: str = "",
    url: str = "",
) -> dict[str, Any]:
    """Validate + insert a custom logo. Returns the new row.

    Raises :class:`LogoStoreError` for any input the API should bounce.
    """
    key = (key or "").strip().lower()
    label = (label or "").strip()
    data_url = (data_url or "").strip()
    url = (url or "").strip()
    category = (category or "custom").strip() or "custom"
    if not key or not _KEY_RE.match(key):
        raise LogoStoreError(
            "key must be 1-40 chars of [a-z0-9_-] and start with [a-z0-9].",
            status_code=400,
        )
    if not label:
        raise LogoStoreError("label is required.", status_code=400)
    if not data_url and not url:
        raise LogoStoreError(
            "either data_url (file upload) or url must be provided.",
            status_code=400,
        )
    if data_url:
        if not data_url.startswith("data:"):
            raise LogoStoreError("data_url must be a data: URL.", status_code=415)
        mime = data_url.split(";", 1)[0][5:].lower()
        if mime not in ALLOWED_MIMES:
            raise LogoStoreError(
                f"unsupported mime {mime!r}; allowed: {sorted(ALLOWED_MIMES)}.",
                status_code=415,
            )
        if len(data_url.encode("utf-8")) > MAX_DATA_URL_BYTES:
            raise LogoStoreError(
                f"data_url exceeds {MAX_DATA_URL_BYTES} byte limit.",
                status_code=413,
            )

    now = time.time()
    try:
        with hs._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO lineage_logos
                    (key, label, category, source, data_url, url, created_at)
                VALUES (?, ?, ?, 'custom', ?, ?, ?)
                """,
                (key, label, category, data_url, url, now),
            )
            new_id = int(cur.lastrowid)
    except Exception as exc:
        raise LogoStoreError(
            f"could not insert logo (likely duplicate key for source='custom'): {exc}",
            status_code=409,
        ) from exc

    return {
        "id": new_id,
        "key": key,
        "label": label,
        "category": category,
        "source": "custom",
        "data_url": data_url,
        "url": url,
        "created_at": now,
    }


# ── delete ───────────────────────────────────────────────────────────────


def delete_custom_logo(hs: Any, logo_id: int) -> None:
    """Delete a custom logo. Raises if the row is a default or in use."""
    with hs._connect() as conn:
        row = conn.execute(
            "SELECT source FROM lineage_logos WHERE id = ?", (int(logo_id),)
        ).fetchone()
        if not row:
            raise LogoStoreError(f"logo {logo_id} not found.", status_code=404)
        if str(row[0]) == "default":
            raise LogoStoreError(
                "default logos cannot be deleted; shadow with a custom upload "
                "of the same key if you want to override.",
                status_code=403,
            )
        in_use = conn.execute(
            "SELECT 1 FROM lineage_logo_nodes WHERE logo_id = ? LIMIT 1",
            (int(logo_id),),
        ).fetchone()
        if in_use:
            raise LogoStoreError(
                "logo is referenced by one or more canvas nodes; remove "
                "those nodes (or replace their logo) before deleting.",
                status_code=409,
            )
        conn.execute("DELETE FROM lineage_logos WHERE id = ?", (int(logo_id),))


# ── logo-node CRUD ───────────────────────────────────────────────────────


def list_logo_nodes(hs: Any, artifact_id: int) -> list[dict[str, Any]]:
    """Return every logo node on the given artifact, joined to the registry."""
    with hs._connect() as conn:
        rows = conn.execute(
            """
            SELECT n.id, n.logo_id, n.label, n.x, n.y, n.width, n.height,
                   n.created_at, n.updated_at,
                   l.key, l.label, l.data_url, l.url, l.category
            FROM lineage_logo_nodes n
            JOIN lineage_logos l ON l.id = n.logo_id
            WHERE n.artifact_id = ?
            ORDER BY n.id
            """,
            (int(artifact_id),),
        ).fetchall()
    return [
        {
            "id": int(r[0]),
            "logo_id": int(r[1]),
            "label": str(r[2] or "") or str(r[10] or ""),
            "x": float(r[3] or 0.0),
            "y": float(r[4] or 0.0),
            "width": float(r[5] or 120.0),
            "height": float(r[6] or 120.0),
            "created_at": float(r[7] or 0.0),
            "updated_at": float(r[8] or 0.0),
            "logo_key": str(r[9] or ""),
            "logo_label": str(r[10] or ""),
            "data_url": str(r[11] or ""),
            "url": str(r[12] or ""),
            "category": str(r[13] or ""),
        }
        for r in rows
    ]


def create_logo_node(
    hs: Any,
    artifact_id: int,
    *,
    logo_id: int | None = None,
    logo_key: str | None = None,
    label: str = "",
    x: float = 0.0,
    y: float = 0.0,
    width: float = 120.0,
    height: float = 120.0,
) -> dict[str, Any]:
    """Create a logo placement on a canvas.

    Either ``logo_id`` or ``logo_key`` must be provided; key wins when
    both are present (frontend payloads prefer key for stability across
    history-store rebuilds).
    """
    if logo_key:
        resolved = lookup_logo_by_key(hs, logo_key)
        if not resolved:
            raise LogoStoreError(f"logo key {logo_key!r} not found.", status_code=404)
        logo_id = int(resolved["id"])
    if not logo_id:
        raise LogoStoreError("logo_id or logo_key is required.", status_code=400)
    now = time.time()
    with hs._connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO lineage_logo_nodes
                (artifact_id, logo_id, label, x, y, width, height,
                 created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(artifact_id),
                int(logo_id),
                str(label or ""),
                float(x),
                float(y),
                float(width),
                float(height),
                now,
                now,
            ),
        )
        new_id = int(cur.lastrowid)
    return {
        "id": new_id,
        "artifact_id": int(artifact_id),
        "logo_id": int(logo_id),
        "label": str(label or ""),
        "x": float(x),
        "y": float(y),
        "width": float(width),
        "height": float(height),
        "created_at": now,
        "updated_at": now,
    }


def update_logo_node(
    hs: Any,
    artifact_id: int,
    node_id: int,
    *,
    payload: dict[str, Any],
) -> int:
    """Patch x/y/width/height/label on a logo node. Returns rowcount."""
    sets: list[str] = []
    args: list[Any] = []
    for key in ("x", "y", "width", "height"):
        if key in payload:
            sets.append(f"{key} = ?")
            args.append(float(payload[key]))
    if "label" in payload:
        sets.append("label = ?")
        args.append(str(payload["label"]))
    if not sets:
        return 0
    now = time.time()
    sets.append("updated_at = ?")
    args.append(now)
    args.extend([int(artifact_id), int(node_id)])
    with hs._connect() as conn:
        cur = conn.execute(
            f"UPDATE lineage_logo_nodes SET {', '.join(sets)} WHERE artifact_id = ? AND id = ?",
            tuple(args),
        )
        return int(cur.rowcount or 0)


def delete_logo_node(hs: Any, artifact_id: int, node_id: int) -> None:
    with hs._connect() as conn:
        conn.execute(
            "DELETE FROM lineage_logo_nodes WHERE artifact_id = ? AND id = ?",
            (int(artifact_id), int(node_id)),
        )
