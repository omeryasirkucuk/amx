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
from pydantic import BaseModel, Field

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
    return {"ok": "true"}


class CleanupPlaceholdersBody(BaseModel):
    """Body for ``POST /api/comments/cleanup-placeholders``.

    Optional ``schema`` scopes the sweep; otherwise every schema the
    active DB profile can see is processed.
    """

    schema_: str | None = Field(default=None, alias="schema")

    model_config = {"populate_by_name": True}


@router.post("/cleanup-placeholders")
def cleanup_placeholders(
    body: CleanupPlaceholdersBody | None = None,
    profile: str = Query(...),
    database: str | None = Query(default=None),
    catalog: str | None = Query(default=None),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, object]:
    """Remove auto-inference fallback placeholder text from existing
    COMMENTs. Re-uses ``cleanup_placeholders_core`` from
    ``amx/cli_support/commands/db.py`` so the CLI command and the
    web button share one implementation. Returns the same payload
    shape (``schemas``, ``tables_cleared``, ``columns_cleared``,
    ``warnings``) the helper produces."""
    from amx.cli_support.commands.db import cleanup_placeholders_core

    db = _scoped_connector(cfg, profile, database, catalog)
    schema = (body.schema_ if body else None) or None
    try:
        result = cleanup_placeholders_core(db, schema=schema)
    except RuntimeError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc
    return result
