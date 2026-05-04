"""Per-asset comment write-back endpoints.

The visualizer's table-detail page surfaces a per-column "edit
comment" UI; PR-E hooks the inline editor to these PUT endpoints.
The backend simply re-uses :class:`amx.db.connector.DatabaseConnector`'s
existing setters — nothing here is new ground.

Errors from the connector (driver missing, permission denied,
unsupported backend) get coerced to 400/500 with the same actionable
detail we use elsewhere.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field

from amx.config import AMXConfig
from amx.db.connector import AssetKind, DatabaseConnector
from amx.web.deps import get_cfg

router = APIRouter(prefix="/api/comments", tags=["comments"])


class CommentBody(BaseModel):
    """Common shape for every comment write-back endpoint."""

    comment: str


def _connector(cfg: AMXConfig) -> DatabaseConnector:
    """Build a fresh DB connector — write paths intentionally don't
    share the read cache so a stale engine never holds an aborted
    transaction."""
    return DatabaseConnector(cfg.db)


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
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, str]:
    """COMMENT ON DATABASE for backends that support it. Falls back
    to a 400 with the connector's actionable message on backends that
    don't (e.g. SQLite)."""
    db = _connector(cfg)
    _coerce_or_400("Setting database comment", lambda: db.set_database_comment(body.comment))
    return {"ok": "true"}


@router.put("/schemas/{schema}")
def set_schema_comment(
    schema: str,
    body: CommentBody,
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, str]:
    db = _connector(cfg)
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
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, str]:
    """COMMENT ON TABLE — used by the table-detail page's edit-table-
    description button. The asset kind defaults to TABLE; PR-E adds
    an explicit ``?kind=view`` so the SPA can edit view comments
    too."""
    db = _connector(cfg)
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
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, str]:
    """COMMENT ON COLUMN — what the SPA's inline column editor calls
    on save. The connector handles per-backend SQL syntax + identifier
    quoting; we just pass the strings through verbatim."""
    db = _connector(cfg)
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
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, object]:
    """Remove auto-inference fallback placeholder text from existing
    COMMENTs. Re-uses ``cleanup_placeholders_core`` from
    ``amx/cli_support/commands/db.py`` so the CLI command and the
    web button share one implementation. Returns the same payload
    shape (``schemas``, ``tables_cleared``, ``columns_cleared``,
    ``warnings``) the helper produces."""
    from amx.cli_support.commands.db import cleanup_placeholders_core

    db = _connector(cfg)
    schema = (body.schema_ if body else None) or None
    try:
        result = cleanup_placeholders_core(db, schema=schema)
    except RuntimeError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc
    return result
