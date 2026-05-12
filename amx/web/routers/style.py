"""AMX Studio endpoints for the writing-style reference feature.

Each endpoint binds to one LLM profile by name. Heavy lifting
(distillation, DB metadata read) reuses the slash-command helpers in
amx.cli_support.commands.style so CLI and Studio stay in lockstep.
"""

from __future__ import annotations

import json
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from amx.cli_support.commands.style import _make_llm_caller, _open_connector
from amx.config import AMXConfig
from amx.llm.style.extractor import NoSamplesError, extract_style
from amx.storage.style_store import StyleStore
from amx.web.deps import get_cfg

router = APIRouter(prefix="/api/llm-profiles", tags=["style"])


class ExtractRequest(BaseModel):
    source_ref: str  # "db.schema.table"
    db_profile: str | None = None


class PatchStyleRequest(BaseModel):
    enabled: bool


def _history_db_path(cfg: AMXConfig) -> Path:
    config_dir = getattr(cfg, "CONFIG_DIR", None) or str(Path.home() / ".amx")
    return Path(config_dir) / "history.db"


def _serialize(row) -> dict:
    return {
        "llm_profile": row.llm_profile,
        "source_ref": row.source_ref,
        "source_db_kind": row.source_db_kind,
        "enabled": row.enabled,
        "sample_count": row.sample_count,
        "profile": json.loads(row.profile.to_json()),
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


@router.post("/{name}/style/extract")
def extract(name: str, body: ExtractRequest, cfg: AMXConfig = Depends(get_cfg)):
    if name not in cfg.llm_profiles:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"LLM profile {name!r} not found")
    parts = body.source_ref.split(".")
    if len(parts) != 3:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "source_ref must be db.schema.table")
    db, schema, table = parts

    db_profile = body.db_profile or cfg.active_db_profile
    if not db_profile or db_profile not in cfg.db_profiles:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "no DB profile available")

    try:
        conn = _open_connector(cfg, db_profile)
        conn.use(db)
        comments = conn.get_column_comments(schema, table)
    except Exception as e:
        raise HTTPException(status.HTTP_502_BAD_GATEWAY, f"reference table read failed: {e}")

    try:
        profile, n_samples = extract_style(comments, llm_call=_make_llm_caller(cfg, name))
    except NoSamplesError as e:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, str(e))
    except ValueError as e:
        raise HTTPException(status.HTTP_502_BAD_GATEWAY, f"distillation failed: {e}")

    StyleStore(_history_db_path(cfg)).upsert(
        llm_profile=name,
        source_ref=body.source_ref,
        source_db_kind=conn.backend,
        profile=profile,
        sample_count=n_samples,
    )
    return {"ok": True, "sample_count": n_samples}


@router.get("/{name}/style")
def get_style(name: str, cfg: AMXConfig = Depends(get_cfg)):
    row = StyleStore(_history_db_path(cfg)).get(name)
    if row is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND)
    return _serialize(row)


@router.patch("/{name}/style")
def patch_style(name: str, body: PatchStyleRequest, cfg: AMXConfig = Depends(get_cfg)):
    store = StyleStore(_history_db_path(cfg))
    if store.get(name) is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND)
    store.set_enabled(name, body.enabled)
    return {"ok": True}


@router.delete("/{name}/style")
def delete_style(name: str, cfg: AMXConfig = Depends(get_cfg)):
    StyleStore(_history_db_path(cfg)).clear(name)
    return {"ok": True}
