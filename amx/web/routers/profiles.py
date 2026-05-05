"""Profile management routes — DB / LLM / docs / code.

Mirrors the four CLI wizards (``/db-profiles``, ``/llm-profiles``,
``/docs-profiles``, ``/code-profiles``) without the interactive
prompts. AMX Studio's Settings page mutates the same
:class:`AMXConfig` the parent CLI is using, so an edit here is
visible the moment the user types a command in the terminal.

DB profiles are masked in responses — ``password`` and
``access_token`` are always replaced with a placeholder so the SPA
never accidentally renders the secret in a tooltip / inspector.
The PUT body, however, accepts the placeholder as "leave the
existing value alone" so editing one field doesn't blank the
secret.
"""

from __future__ import annotations

import logging
from dataclasses import asdict, fields, replace
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status

from amx.config import (
    AMXConfig,
    DBConfig,
    LLMConfig,
)
from amx.web.deps import get_cfg

router = APIRouter(prefix="/api/profiles", tags=["profiles"])

log = logging.getLogger("amx.web.routers.profiles")

#: Placeholder the SPA receives instead of the real secret. The PUT
#: body treats it as "no change" — the same idiom the CLI wizard
#: uses (Enter-to-keep).
SECRET_PLACEHOLDER = "********"

#: DBConfig fields the SPA must never read raw.
_DB_SECRET_FIELDS = frozenset({"password", "access_token"})

#: LLMConfig fields likewise.
_LLM_SECRET_FIELDS = frozenset({"api_key"})


# ── Backend / provider catalogs ────────────────────────────────────────


# Same backend list the CLI's ``/add-db-profile`` wizard exposes. Each
# entry hints at the fields the SPA's wizard should surface; the SPA
# can still send any DBConfig field via PUT — these are just the
# defaults that get rendered as labelled inputs.
_DB_BACKENDS: list[dict[str, Any]] = [
    {
        "id": "postgresql",
        "label": "PostgreSQL",
        "fields": ["host", "port", "user", "password", "database"],
        "default_port": 5432,
    },
    {
        "id": "mysql",
        "label": "MySQL / MariaDB",
        "fields": ["host", "port", "user", "password", "database"],
        "default_port": 3306,
    },
    {
        "id": "snowflake",
        "label": "Snowflake",
        "fields": ["account", "user", "password", "database", "warehouse", "role"],
    },
    {
        "id": "databricks",
        "label": "Databricks (Unity Catalog)",
        "fields": ["host", "http_path", "access_token", "catalog"],
        "supports_catalog": True,
    },
    {
        "id": "bigquery",
        "label": "BigQuery",
        "fields": ["project", "dataset", "credentials_path"],
    },
    {
        "id": "oracle",
        "label": "Oracle",
        "fields": ["host", "port", "user", "password", "database", "service_name"],
        "default_port": 1521,
    },
    {
        "id": "mssql",
        "label": "SQL Server",
        "fields": ["host", "port", "user", "password", "database", "driver"],
        "default_port": 1433,
    },
    {
        "id": "redshift",
        "label": "Redshift",
        "fields": ["host", "port", "user", "password", "database", "cluster_identifier"],
        "default_port": 5439,
    },
    {
        "id": "clickhouse",
        "label": "ClickHouse",
        "fields": ["host", "port", "user", "password", "database", "secure"],
        "default_port": 8123,
    },
    {
        "id": "duckdb",
        "label": "DuckDB",
        "fields": ["database"],
    },
]


_LLM_PROVIDERS: list[dict[str, Any]] = [
    {"id": "openai", "label": "OpenAI", "needs_key": True, "needs_base": False},
    {"id": "anthropic", "label": "Anthropic", "needs_key": True, "needs_base": False},
    {"id": "gemini", "label": "Gemini", "needs_key": True, "needs_base": False},
    {"id": "deepseek", "label": "DeepSeek", "needs_key": True, "needs_base": False},
    {"id": "openrouter", "label": "OpenRouter", "needs_key": True, "needs_base": False},
    {"id": "kimi", "label": "Kimi (Moonshot)", "needs_key": True, "needs_base": False},
    {
        "id": "databricks_serving",
        "label": "Databricks Serving",
        "needs_key": True,
        "needs_base": True,
    },
    {"id": "ollama", "label": "Ollama (local)", "needs_key": False, "needs_base": True},
    {"id": "local", "label": "Generic OpenAI-compatible", "needs_key": True, "needs_base": True},
]


@router.get("/db/backends")
def list_db_backends() -> dict[str, Any]:
    """Backends the wizard surfaces, with the canonical fields each
    one needs. Identical to the CLI's ``/add-db-profile`` picker —
    the SPA renders the right inputs based on the chosen backend."""
    return {"backends": _DB_BACKENDS}


@router.get("/llm/providers")
def list_llm_providers() -> dict[str, Any]:
    """Provider list for the LLM wizard. ``needs_base`` flags which
    providers require an explicit ``api_base`` (Ollama, Databricks
    serving, generic OpenAI-compatible local servers)."""
    return {"providers": _LLM_PROVIDERS}


# ── DB profiles ────────────────────────────────────────────────────────


@router.get("/db")
def list_db(cfg: AMXConfig = Depends(get_cfg)) -> dict[str, Any]:
    """Summary list — what the Settings page renders as a table.

    Each entry carries just enough to render the row (name, backend,
    target). Full secrets are NEVER included in the list response;
    callers must hit ``GET /api/profiles/db/{name}`` for the masked
    detail."""
    items: list[dict[str, Any]] = []
    for name, profile in sorted(cfg.db_profiles.items()):
        items.append(
            {
                "name": name,
                "backend": profile.backend or "",
                "host": profile.host or "",
                "database": profile.database or "",
                "catalog": profile.catalog or "",
                "project": profile.project or "",
                "is_active": name == (cfg.active_db_profile or ""),
            }
        )
    return {"profiles": items, "active": cfg.active_db_profile or None, "count": len(items)}


@router.get("/db/{name}")
def get_db(name: str, cfg: AMXConfig = Depends(get_cfg)) -> dict[str, Any]:
    profile = cfg.db_profiles.get(name)
    if profile is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No DB profile named {name!r}.",
        )
    return _mask_db(profile, name, is_active=name == (cfg.active_db_profile or ""))


@router.put("/db/{name}")
def upsert_db(
    name: str,
    body: dict[str, Any],
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Create or update one DB profile. Unknown keys are ignored
    (forwards-compatible)."""
    existing = cfg.db_profiles.get(name) or DBConfig()
    merged = _merge_db_patch(existing, body)
    cfg.upsert_db_profile(name, merged)
    cfg.save()
    return _mask_db(merged, name, is_active=name == (cfg.active_db_profile or ""))


@router.delete("/db/{name}")
def delete_db(name: str, cfg: AMXConfig = Depends(get_cfg)) -> dict[str, Any]:
    if name not in cfg.db_profiles:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No DB profile named {name!r}.",
        )
    if name == (cfg.active_db_profile or ""):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot delete the active DB profile. Activate another profile first.",
        )
    del cfg.db_profiles[name]
    cfg.save()
    return {"ok": True, "name": name, "remaining": len(cfg.db_profiles)}


@router.post("/db/{name}/activate")
def activate_db(name: str, cfg: AMXConfig = Depends(get_cfg)) -> dict[str, Any]:
    if name not in cfg.db_profiles:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No DB profile named {name!r}.",
        )
    cfg.set_active_db_profile(name)
    cfg.save()
    return {"active": name}


@router.post("/db/{name}/test")
def test_db(name: str, cfg: AMXConfig = Depends(get_cfg)) -> dict[str, Any]:
    """Open a connection against the saved profile and report
    whether AMX can talk to it. The Settings UI's "Test connection"
    button calls this before saving the form to surface broken
    credentials early."""
    profile = cfg.db_profiles.get(name)
    if profile is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No DB profile named {name!r}.",
        )
    from amx.db.connector import DatabaseConnector

    try:
        conn = DatabaseConnector(profile)
        result = conn.test_connection_result()
    except Exception as exc:  # pragma: no cover - belt-and-braces
        return {"ok": False, "message": f"{exc.__class__.__name__}: {exc}"}
    return {"ok": bool(result.ok), "message": result.message}


# ── LLM profiles ───────────────────────────────────────────────────────


@router.get("/llm")
def list_llm(cfg: AMXConfig = Depends(get_cfg)) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    for name, profile in sorted(cfg.llm_profiles.items()):
        items.append(
            {
                "name": name,
                "provider": profile.provider or "",
                "model": profile.model or "",
                "is_active": name == (cfg.active_llm_profile or ""),
            }
        )
    return {"profiles": items, "active": cfg.active_llm_profile or None, "count": len(items)}


@router.get("/llm/{name}")
def get_llm(name: str, cfg: AMXConfig = Depends(get_cfg)) -> dict[str, Any]:
    profile = cfg.llm_profiles.get(name)
    if profile is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No LLM profile named {name!r}.",
        )
    return _mask_llm(profile, name, is_active=name == (cfg.active_llm_profile or ""))


@router.put("/llm/{name}")
def upsert_llm(
    name: str,
    body: dict[str, Any],
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    existing = cfg.llm_profiles.get(name) or LLMConfig()
    merged = _merge_llm_patch(existing, body)
    cfg.upsert_llm_profile(name, merged)
    cfg.save()
    return _mask_llm(merged, name, is_active=name == (cfg.active_llm_profile or ""))


@router.delete("/llm/{name}")
def delete_llm(name: str, cfg: AMXConfig = Depends(get_cfg)) -> dict[str, Any]:
    if name not in cfg.llm_profiles:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No LLM profile named {name!r}.",
        )
    if name == (cfg.active_llm_profile or ""):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot delete the active LLM profile. Activate another first.",
        )
    del cfg.llm_profiles[name]
    cfg.save()
    return {"ok": True, "name": name, "remaining": len(cfg.llm_profiles)}


@router.post("/llm/{name}/activate")
def activate_llm(name: str, cfg: AMXConfig = Depends(get_cfg)) -> dict[str, Any]:
    if name not in cfg.llm_profiles:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No LLM profile named {name!r}.",
        )
    cfg.set_active_llm_profile(name)
    cfg.save()
    return {"active": name}


# ── Doc + Code profiles (path lists) ───────────────────────────────────


@router.get("/docs")
def list_docs(cfg: AMXConfig = Depends(get_cfg)) -> dict[str, Any]:
    """Doc profiles are simple ``dict[name -> list[path]]`` — surface
    the path list so the SPA can render each profile as a card with
    its source paths inline."""
    items = [
        {
            "name": name,
            "paths": list(paths or []),
            "is_active": name == (getattr(cfg, "active_doc_profile", "") or ""),
        }
        for name, paths in sorted(cfg.doc_profiles.items())
    ]
    return {"profiles": items, "active": getattr(cfg, "active_doc_profile", "") or None}


@router.get("/code")
def list_code(cfg: AMXConfig = Depends(get_cfg)) -> dict[str, Any]:
    """Code profiles are ``dict[name -> repo_path_or_url]``."""
    items = [
        {
            "name": name,
            "path": str(value or ""),
            "is_active": name == (getattr(cfg, "active_code_profile", "") or ""),
        }
        for name, value in sorted(cfg.code_profiles.items())
    ]
    return {"profiles": items, "active": getattr(cfg, "active_code_profile", "") or None}


@router.put("/docs/{name}")
def upsert_docs(
    name: str,
    body: dict[str, Any],
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Create / update a doc profile.

    Body shape: ``{"paths": ["/abs/dir1", "https://…", "s3://bucket/key", …]}``.
    Each path is normalised to a string; empty entries are dropped so
    accidental blank rows don't survive a save.
    """
    raw_paths = body.get("paths") if isinstance(body, dict) else None
    if not isinstance(raw_paths, list):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Body must include a 'paths' array of strings.",
        )
    cleaned = [str(p).strip() for p in raw_paths if str(p).strip()]
    cfg.doc_profiles[name] = cleaned
    cfg.save()
    return {
        "name": name,
        "paths": cleaned,
        "is_active": name == (getattr(cfg, "active_doc_profile", "") or ""),
    }


@router.delete("/docs/{name}")
def delete_docs(name: str, cfg: AMXConfig = Depends(get_cfg)) -> dict[str, Any]:
    if name not in cfg.doc_profiles:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No doc profile named {name!r}.",
        )
    if name == (getattr(cfg, "active_doc_profile", "") or ""):
        cfg.active_doc_profile = ""
    del cfg.doc_profiles[name]
    cfg.save()
    return {"ok": True, "remaining": len(cfg.doc_profiles)}


@router.post("/docs/{name}/activate")
def activate_docs(name: str, cfg: AMXConfig = Depends(get_cfg)) -> dict[str, Any]:
    if name not in cfg.doc_profiles:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No doc profile named {name!r}.",
        )
    cfg.active_doc_profile = name
    cfg.save()
    return {"active": name}


@router.put("/code/{name}")
def upsert_code(
    name: str,
    body: dict[str, Any],
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Create / update a code profile.

    Body shape: ``{"path": "/abs/dir or https://github.com/org/repo"}``.
    """
    raw_path = body.get("path") if isinstance(body, dict) else None
    if not isinstance(raw_path, str) or not raw_path.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Body must include a non-empty 'path' string.",
        )
    cfg.code_profiles[name] = raw_path.strip()
    cfg.save()
    return {
        "name": name,
        "path": cfg.code_profiles[name],
        "is_active": name == (getattr(cfg, "active_code_profile", "") or ""),
    }


@router.delete("/code/{name}")
def delete_code(name: str, cfg: AMXConfig = Depends(get_cfg)) -> dict[str, Any]:
    if name not in cfg.code_profiles:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No code profile named {name!r}.",
        )
    if name == (getattr(cfg, "active_code_profile", "") or ""):
        cfg.active_code_profile = ""
    del cfg.code_profiles[name]
    cfg.save()
    return {"ok": True, "remaining": len(cfg.code_profiles)}


@router.post("/code/{name}/activate")
def activate_code(name: str, cfg: AMXConfig = Depends(get_cfg)) -> dict[str, Any]:
    if name not in cfg.code_profiles:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No code profile named {name!r}.",
        )
    cfg.active_code_profile = name
    cfg.save()
    return {"active": name}


# ── Helpers ────────────────────────────────────────────────────────────


def _mask_db(profile: DBConfig, name: str, *, is_active: bool) -> dict[str, Any]:
    raw = asdict(profile)
    for secret in _DB_SECRET_FIELDS:
        if raw.get(secret):
            raw[secret] = SECRET_PLACEHOLDER
    raw["name"] = name
    raw["is_active"] = is_active
    return raw


def _mask_llm(profile: LLMConfig, name: str, *, is_active: bool) -> dict[str, Any]:
    raw = asdict(profile)
    for secret in _LLM_SECRET_FIELDS:
        if raw.get(secret):
            raw[secret] = SECRET_PLACEHOLDER
    raw["name"] = name
    raw["is_active"] = is_active
    return raw


def _merge_db_patch(existing: DBConfig, body: dict[str, Any]) -> DBConfig:
    """Apply *body* on top of *existing*. Unknown keys are silently
    dropped (so a future SPA build never breaks an older backend).
    Secret fields treat the placeholder as 'no change'."""
    valid = {f.name for f in fields(DBConfig)}
    diff: dict[str, Any] = {}
    for key, value in body.items():
        if key not in valid:
            continue
        if key in _DB_SECRET_FIELDS and value == SECRET_PLACEHOLDER:
            continue  # keep existing secret
        diff[key] = value
    return replace(existing, **diff)


def _merge_llm_patch(existing: LLMConfig, body: dict[str, Any]) -> LLMConfig:
    valid = {f.name for f in fields(LLMConfig)}
    diff: dict[str, Any] = {}
    for key, value in body.items():
        if key not in valid:
            continue
        if key in _LLM_SECRET_FIELDS and value == SECRET_PLACEHOLDER:
            continue
        diff[key] = value
    return replace(existing, **diff)


# Re-export so type-checkers see the symbol from the module's public
# surface even though we don't use it directly.
__all__ = [
    "router",
    "SECRET_PLACEHOLDER",
]
