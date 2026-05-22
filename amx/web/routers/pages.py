"""FastAPI routes for documentation pages.

Transport-only layer: every endpoint delegates to
:class:`amx.pages.service.PagesService`. No SQL, no LLM calls, no
loaders live here.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Literal

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile, status
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, Field

from amx.docs.extensions import SUPPORTED_EXTENSIONS
from amx.pages.intent_templates import INTENT_TEMPLATES
from amx.pages.service import PagesService
from amx.pages.types import AssetRef
from amx.storage.conflicts import StaleVersionError
from amx.web.deps import get_pages_service
from amx.web.permissions import require_writer_role

router = APIRouter(prefix="/api/pages", tags=["pages"])


class AssetIn(BaseModel):
    kind: str
    ref: str


class PageCreateIn(BaseModel):
    title: str = Field(..., min_length=1)
    intent: str = ""
    assets: list[AssetIn] = Field(default_factory=list)


class PagePatchIn(BaseModel):
    markdown_body: str | None = None
    status: Literal["draft", "published"] | None = None
    note: str | None = None


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _ext_to_source_kind(saved_path: str) -> str:
    ext = os.path.splitext(saved_path)[1].lower()
    if ext == ".xlsx":
        return "excel"
    if ext == ".eml":
        return "email"
    return "upload"


@router.get("")
def list_pages(
    db_profiles: list[str] | None = Query(default=None),
    svc: PagesService = Depends(get_pages_service),
) -> list[dict]:
    """List active documentation pages.

    ``?db_profiles=a&db_profiles=b`` filters to pages owned by those DB
    profiles. ``None`` (the default) returns pages for all profiles,
    including pages with no profile association (shown as "unscoped").
    """
    pages = svc.store.list_active()
    if db_profiles:
        pages = [p for p in pages if p.get("db_profile") in db_profiles]
    return pages


@router.post("", status_code=status.HTTP_201_CREATED)
def create_page(
    body: PageCreateIn,
    svc: PagesService = Depends(get_pages_service),
    _: None = Depends(require_writer_role),
) -> dict:
    pid = svc.create_draft(
        title=body.title,
        intent=body.intent,
        assets=[AssetRef(a.kind, a.ref) for a in body.assets],  # type: ignore[arg-type]
        sources=[],
        created_by=None,
        now=_now(),
    )
    return {"id": pid}


@router.get("/intent-templates")
def list_intent_templates() -> list[dict]:
    """Return the preset intent templates as a list of pickable cards.

    Studio's New-page wizard renders these above the Intent textarea so
    a user can seed the intent string with a stock phrasing for the
    most common documentation shapes (single table, project overview,
    ...) instead of writing it from scratch.
    """
    return [
        {
            "slug": t.slug,
            "label": t.label,
            "required_assets": t.required_assets,
            "prompt_skeleton": t.prompt_skeleton,
        }
        for t in INTENT_TEMPLATES
    ]


# PR-C (scale): per-kind config powering ``/asset-options``.
# ``select`` is the projection used to build option rows; ``table``,
# ``name_col``, and ``path_col`` drive the WHERE / ORDER BY + the
# optional substring search axis. Wiring it into a config dict beats
# the previous per-kind SQL-string copy/paste and lets pagination
# threads through every kind uniformly.
_REMOTE_ASSET_OPTION_CONFIG: dict[str, dict[str, str]] = {
    "asset_notebook": {
        "select": "id, name, workspace_path, qualified_name, ingested_at",
        "table": "remote_notebooks",
        "name_col": "name",
        "path_col": "COALESCE(workspace_path, qualified_name, '')",
        "order_by": "name, id",
    },
    "asset_job": {
        "select": "id, name, '' AS workspace_path, '' AS qualified_name, ingested_at",
        "table": "remote_jobs",
        "name_col": "name",
        "path_col": "",
        "order_by": "name, id",
    },
    "asset_pipeline": {
        "select": "id, name, '' AS workspace_path, target_schema AS qualified_name, ingested_at",
        "table": "remote_pipelines",
        "name_col": "name",
        "path_col": "COALESCE(target_schema, '')",
        "order_by": "name, id",
    },
    "asset_query": {
        "select": "id, name, kind AS workspace_path, warehouse AS qualified_name, ingested_at",
        "table": "remote_queries",
        "name_col": "name",
        "path_col": "",
        "order_by": "executed_at DESC, id",
    },
    "asset_stream": {
        "select": (
            "id, qualified_name AS name, source_table_fqn AS workspace_path, "
            "mode AS qualified_name, ingested_at"
        ),
        "table": "remote_streams",
        "name_col": "qualified_name",
        "path_col": "",
        "order_by": "qualified_name, id",
    },
    "asset_streamlit": {
        "select": (
            "id, qualified_name AS name, main_file AS workspace_path, "
            "query_warehouse AS qualified_name, ingested_at"
        ),
        "table": "remote_streamlit_apps",
        "name_col": "qualified_name",
        "path_col": "",
        "order_by": "qualified_name, id",
    },
}


@router.get("/asset-options")
def list_asset_options(
    kind: str = Query(..., description="One of asset_notebook, asset_job, ..."),
    profile: str = Query(..., min_length=1),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    q: str = Query(default="", description="Substring filter on name + path."),
    svc: PagesService = Depends(get_pages_service),
) -> list[dict]:
    """List ingested remote assets a page can be anchored to.

    Feeds the Studio New-page wizard's "Ingested assets" tab. Rows are
    sourced from the local ``remote_*`` tables populated by
    ``/db ingest-assets``; the resolver later excerpts each one when
    the page is generated.

    PR-C (scale): the legacy ``LIMIT 500`` cap let the picker fall
    over silently past 500 assets. The endpoint now paginates
    explicitly (``limit`` / ``offset``) and accepts a substring
    filter ``q`` that hits both the name column and the kind's
    natural path column. Response shape stays list[dict] for
    backwards compatibility with the existing AssetPicker client;
    the page wizard tab simply pages with Prev/Next instead of
    relying on an invisible cap.
    """
    spec = _REMOTE_ASSET_OPTION_CONFIG.get(kind)
    if spec is None:
        raise HTTPException(status_code=400, detail=f"unknown asset kind: {kind}")
    where = "profile_name = ?"
    params: list = [profile]
    needle = (q or "").strip().lower()
    if needle:
        like = f"%{needle}%"
        if spec["path_col"]:
            where += f" AND (LOWER({spec['name_col']}) LIKE ? OR LOWER({spec['path_col']}) LIKE ?)"
            params.extend([like, like])
        else:
            where += f" AND LOWER({spec['name_col']}) LIKE ?"
            params.append(like)
    sql = (
        f"SELECT {spec['select']} FROM {spec['table']} "  # noqa: S608 — identifiers controlled above
        f"WHERE {where} ORDER BY {spec['order_by']} LIMIT ? OFFSET ?"
    )
    params.extend([int(limit), int(offset)])
    with svc.store.history._connect() as conn:
        cursor = conn.execute(sql, params)
        cols = [c[0] for c in cursor.description]
        rows = [dict(zip(cols, row, strict=False)) for row in cursor.fetchall()]
    return [
        {
            "kind": kind,
            "ref": f"{profile}:{r['id']}",
            "name": r.get("name") or r.get("id"),
            "location": r.get("workspace_path") or r.get("qualified_name") or "",
            "ingested_at": r.get("ingested_at") or "",
        }
        for r in rows
    ]


@router.get("/{page_id}")
def get_page(
    page_id: str,
    svc: PagesService = Depends(get_pages_service),
) -> dict:
    page = svc.store.get(page_id)
    if page is None:
        raise HTTPException(status_code=404, detail="page not found")
    return page


@router.post("/{page_id}/generate")
def generate_page(
    page_id: str,
    svc: PagesService = Depends(get_pages_service),
    _: None = Depends(require_writer_role),
) -> dict:
    try:
        svc.generate(page_id, now=_now())
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="page not found") from exc
    page = svc.store.get(page_id)
    if page is None:
        raise HTTPException(status_code=404, detail="page not found")
    return page


@router.patch("/{page_id}")
def patch_page(
    page_id: str,
    body: PagePatchIn,
    svc: PagesService = Depends(get_pages_service),
    _: None = Depends(require_writer_role),
) -> dict:
    page = svc.store.get(page_id)
    if page is None:
        raise HTTPException(status_code=404, detail="page not found")
    if body.markdown_body is not None:
        try:
            svc.save_revision(
                page_id,
                markdown_body=body.markdown_body,
                now=_now(),
                saved_by=None,
                note=body.note,
            )
        except StaleVersionError as exc:
            return JSONResponse(
                status_code=status.HTTP_409_CONFLICT,
                content={
                    "error": "stale_version",
                    "resource": exc.resource,
                    "expected_version": exc.expected_version,
                    "actual": {
                        "version": exc.actual.version,
                        "updated_by": exc.actual.updated_by,
                        "updated_at": (
                            exc.actual.updated_at.isoformat()
                            if hasattr(exc.actual.updated_at, "isoformat")
                            else str(exc.actual.updated_at)
                        ),
                        "current_value": exc.actual.current_value,
                    },
                },
            )
    updated = svc.store.get(page_id)
    if updated is None:
        raise HTTPException(status_code=404, detail="page not found")
    return updated


@router.delete("/{page_id}")
def delete_page(
    page_id: str,
    svc: PagesService = Depends(get_pages_service),
    _: None = Depends(require_writer_role),
) -> dict:
    if svc.store.get(page_id) is None:
        raise HTTPException(status_code=404, detail="page not found")
    svc.soft_delete(page_id, now=_now())
    return {"ok": True}


@router.post("/{page_id}/sources", status_code=status.HTTP_201_CREATED)
async def upload_source(
    page_id: str,
    file: UploadFile = File(...),
    svc: PagesService = Depends(get_pages_service),
    _: None = Depends(require_writer_role),
) -> dict:
    if svc.store.get(page_id) is None:
        raise HTTPException(status_code=404, detail="page not found")

    original_name = file.filename or "untitled"
    ext = os.path.splitext(original_name)[1].lower()
    if ext not in SUPPORTED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"unsupported file type {ext!r}",
        )

    data = await file.read()
    if not data:
        raise HTTPException(status_code=400, detail="empty file")

    from amx.docs.uploads import UploadError, save_uploaded_file

    try:
        # The doc-profile namespace is reused here so the upload helper
        # content-addresses the file under ``~/.amx/uploads/pages_<id>/``.
        # ``save_uploaded_file`` would normally also register the upload
        # root on a doc profile; that side-effect is harmless for the
        # pages flow and keeps the on-disk layout consistent with other
        # AMX uploads.
        saved = save_uploaded_file(
            svc.resolver.cfg,  # type: ignore[attr-defined]
            f"pages_{page_id}",
            original_name,
            data,
        )
    except UploadError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    kind = _ext_to_source_kind(saved.saved_path)
    now = _now()
    svc.store.history.attach_documentation_page_source(
        page_id,
        source_kind=kind,
        source_path=saved.saved_path,
        original_name=saved.original_name,
        created_at=now,
    )
    return {
        "page_id": page_id,
        "kind": kind,
        "path": saved.saved_path,
        "original_name": saved.original_name,
    }


@router.get("/{page_id}/export/md")
def export_md(
    page_id: str,
    svc: PagesService = Depends(get_pages_service),
) -> Response:
    try:
        body = svc.export(page_id, "md")
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="page not found") from exc
    text = body if isinstance(body, str) else body.decode("utf-8")
    return Response(
        content=text,
        media_type="text/markdown; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="page-{page_id}.md"'},
    )


@router.get("/{page_id}/export/pdf")
def export_pdf(
    page_id: str,
    svc: PagesService = Depends(get_pages_service),
) -> Response:
    try:
        pdf = svc.export(page_id, "pdf")
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="page not found") from exc
    payload = pdf if isinstance(pdf, bytes) else pdf.encode("utf-8")
    return Response(
        content=payload,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="page-{page_id}.pdf"'},
    )
