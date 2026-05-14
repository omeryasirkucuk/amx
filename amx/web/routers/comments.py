"""Per-asset comment write-back endpoints.

AMX Studio's table-detail page surfaces a per-column "edit
comment" UI; PR-E hooks the inline editor to these PUT endpoints.
The backend simply re-uses :class:`amx.db.connector.DatabaseConnector`'s
existing setters — nothing here is new ground.

Errors from the connector (driver missing, permission denied,
unsupported backend) get coerced to 400/500 with the same actionable
detail we use elsewhere.

Scope: every write endpoint REQUIRES ``?profile=``, optionally
narrowed with ``&database=`` / ``&catalog=``. The connector is built
per-request via :func:`_connector_for_scope` so a comment edited from
profile X never lands on profile Y — closes the silent-corruption
window the legacy single-active path had when the SPA's URL profile
disagreed with ``cfg.active_db_profile``.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel

from amx.config import AMXConfig
from amx.db.connector import AssetKind, DatabaseConnector
from amx.web.deps import get_cfg
from amx.web.routers.live_db import _connector_for_scope

router = APIRouter(prefix="/api/comments", tags=["comments"])


class CommentBody(BaseModel):
    """Common shape for every comment write-back endpoint."""

    comment: str


def _scoped_connector(
    cfg: AMXConfig,
    profile: str,
    database: str | None,
    catalog: str | None,
) -> DatabaseConnector:
    """Build a per-request connector for the named profile."""
    return _connector_for_scope(cfg, profile, database=database, catalog=catalog)


def _coerce_or_400(action: str, fn):
    try:
        return fn()
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"{action} failed: {exc.__class__.__name__}: {exc}",
        ) from exc


@router.put("/database")
def set_database_comment(
    body: CommentBody,
    profile: str = Query(...),
    database: str | None = Query(default=None),
    catalog: str | None = Query(default=None),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, str]:
    """COMMENT ON DATABASE for backends that support it. Falls back
    to a 400 with the connector's actionable message on backends that
    don't (e.g. SQLite)."""
    db = _scoped_connector(cfg, profile, database, catalog)
    _coerce_or_400("Setting database comment", lambda: db.set_database_comment(body.comment))
    # Wipe the cached column-comments for this profile so the next
    # read picks up the just-written value. Database-level writes are
    # rare enough that nuking the whole profile is cheaper than
    # tracking which schemas inherit the description.
    db.invalidate_column_comments_cache()
    return {"ok": "true"}


@router.put("/schemas/{schema}")
def set_schema_comment(
    schema: str,
    body: CommentBody,
    profile: str = Query(...),
    database: str | None = Query(default=None),
    catalog: str | None = Query(default=None),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, str]:
    db = _scoped_connector(cfg, profile, database, catalog)
    _coerce_or_400(
        f"Setting schema comment on {schema}",
        lambda: db.set_schema_comment(schema, body.comment),
    )
    db.invalidate_column_comments_cache(schema=schema)
    return {"ok": "true"}


@router.put("/schemas/{schema}/tables/{table}")
def set_table_comment(
    schema: str,
    table: str,
    body: CommentBody,
    profile: str = Query(...),
    database: str | None = Query(default=None),
    catalog: str | None = Query(default=None),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, str]:
    """COMMENT ON TABLE — used by the table-detail page's edit-table-
    description button. The asset kind defaults to TABLE; PR-E adds
    an explicit ``?kind=view`` so the SPA can edit view comments
    too."""
    db = _scoped_connector(cfg, profile, database, catalog)
    _coerce_or_400(
        f"Setting table comment on {schema}.{table}",
        lambda: db.set_table_comment(schema, table, body.comment, asset_kind=AssetKind.TABLE),
    )
    db.invalidate_column_comments_cache(schema=schema, table=table)
    return {"ok": "true"}


@router.put("/schemas/{schema}/tables/{table}/columns/{column}")
def set_column_comment(
    schema: str,
    table: str,
    column: str,
    body: CommentBody,
    profile: str = Query(...),
    database: str | None = Query(default=None),
    catalog: str | None = Query(default=None),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, str]:
    """COMMENT ON COLUMN — what the SPA's inline column editor calls
    on save. The connector handles per-backend SQL syntax + identifier
    quoting; we just pass the strings through verbatim."""
    db = _scoped_connector(cfg, profile, database, catalog)
    _coerce_or_400(
        f"Setting column comment on {schema}.{table}.{column}",
        lambda: db.set_column_comment(schema, table, column, body.comment),
    )
    # Column-level granularity isn't worth the bookkeeping — the next
    # fetch refreshes the entire table's column dict in one bulk call.
    db.invalidate_column_comments_cache(schema=schema, table=table)
    return {"ok": "true"}
