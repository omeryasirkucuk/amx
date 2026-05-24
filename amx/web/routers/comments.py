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

from collections.abc import Callable

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from amx.config import AMXConfig
from amx.core.errors import classify_write_error
from amx.db.connector import AssetKind, DatabaseConnector
from amx.web.deps import get_cfg
from amx.web.routers.live_db import _connector_for_scope

router = APIRouter(prefix="/api/comments", tags=["comments"])


class CommentBody(BaseModel):
    """Common shape for every comment write-back endpoint."""

    comment: str


class LocalCommentBody(BaseModel):
    """Body accepted by ``POST /api/comments/local``.

    ``column`` is optional: when omitted (or empty) the description
    is attached to the table-level entity row instead of a column.
    The local override never triggers a writeback to the source DB —
    that is the whole point of the surface.
    """

    profile: str = Field(min_length=1)
    schema_: str = Field(min_length=1, alias="schema")
    table: str = Field(min_length=1)
    column: str | None = None
    description: str = Field(min_length=1)

    model_config = {"populate_by_name": True}


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


def _classified_or_400(
    fn: Callable[[], object],
    *,
    db: DatabaseConnector,
    schema: str = "",
    table: str = "",
    column: str | None = None,
) -> object:
    """Run a single live-DB write through the same classifier the
    Apply pending queue path uses, so a permission-denied edit from
    the inline editor surfaces the same actionable banner the bulk
    apply does. The 400 detail is a JSON object the SPA reads
    structured fields off; ``error_kind`` is the stable slug.

    Falls back to a stringified detail when classification itself
    raises (defensive — the classifier currently never returns
    ``None``, but a future regression must not hide a write failure
    behind a stack trace)."""
    try:
        return fn()
    except Exception as exc:
        backend = ""
        try:
            backend = str(getattr(db, "backend", "") or "")
        except Exception:
            backend = ""
        try:
            cls = classify_write_error(
                exc,
                backend=backend,
                schema=schema,
                table=table,
                column=column,
            )
            # ``message`` mirrors the classifier title so the SPA's
            # generic toast handler (``api.ts`` reads ``detail.message``)
            # shows the user-friendly headline. Hint carries the
            # suggested action so existing toast UIs that render
            # ``hint`` get the DBA-grant string for free.
            detail = {
                "message": cls.title,
                "hint": cls.suggested_action,
                "error_kind": cls.kind,
                "error_title": cls.title,
                "error_text": cls.body,
                "error_action": cls.suggested_action,
                "raw": str(exc)[:1000],
            }
        except Exception:
            detail = {
                "message": "Write failed",
                "hint": "",
                "error_kind": "unknown",
                "error_title": "Write failed",
                "error_text": str(exc)[:1000],
                "error_action": "",
                "raw": str(exc)[:1000],
            }
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=detail,
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
    _classified_or_400(
        lambda: db.set_database_comment(body.comment),
        db=db,
    )
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
    _classified_or_400(
        lambda: db.set_schema_comment(schema, body.comment),
        db=db,
        schema=schema,
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
    _classified_or_400(
        lambda: db.set_table_comment(schema, table, body.comment, asset_kind=AssetKind.TABLE),
        db=db,
        schema=schema,
        table=table,
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
    _classified_or_400(
        lambda: db.set_column_comment(schema, table, column, body.comment),
        db=db,
        schema=schema,
        table=table,
        column=column,
    )
    # Column-level granularity isn't worth the bookkeeping — the next
    # fetch refreshes the entire table's column dict in one bulk call.
    db.invalidate_column_comments_cache(schema=schema, table=table)
    return {"ok": "true"}


@router.post("/local")
def save_local_comment(
    body: LocalCommentBody,
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, object]:
    """Create a local-only description override for one entity.

    Unlike the PUT endpoints above, this path never issues a
    ``COMMENT ON …`` to the source DB. The description is recorded
    as ``source_kind="user_local"`` in ``catalog_descriptions`` and
    immediately becomes the effective description for the entity
    (the new source outranks every existing one — see
    ``SOURCE_PRIORITY``). The Studio asset card / REPL inspect / FTS
    search will surface it on the next read.
    """
    from amx.db._default_scope import profile_default_container
    from amx.search.catalog import SearchCatalog

    profile = body.profile.strip()
    db_cfg = (cfg.db_profiles or {}).get(profile)
    if db_cfg is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"Unknown DB profile: {profile!r}. Save the profile "
                "via /db-profiles or pick an existing one."
            ),
        )

    catalog = SearchCatalog.from_history_store()
    if catalog is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "History store isn't initialised — activate any DB "
                "profile first so the local catalog can open."
            ),
        )

    column = (body.column or "").strip() or None
    entity_kind = "column" if column else "table"
    db_backend = str(getattr(db_cfg, "backend", "") or "")
    database_name = profile_default_container(db_cfg) or ""

    result = catalog.record_user_local_description(
        db_profile=profile,
        db_backend=db_backend,
        database_name=database_name,
        schema_name=body.schema_,
        table_name=body.table,
        column_name=column,
        entity_kind=entity_kind,
        asset_kind="table",
        description=body.description,
    )
    return {
        "ok": True,
        "profile": profile,
        "schema": body.schema_,
        "table": body.table,
        "column": column,
        "entity_id": result["entity_id"],
        "description_id": result["description_id"],
    }
