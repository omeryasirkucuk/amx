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
    # ``is_active`` reads as ``True`` for every defined DB profile now
    # that Studio no longer has an Activate UI. Every Studio surface
    # (Run, Ask, Browse) picks a profile per-action; the legacy field
    # is kept on the row so older bundles cached in the user's browser
    # don't crash when they read it.
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
                "is_active": True,
            }
        )
    return {"profiles": items, "count": len(items)}


@router.get("/db/{name}")
def get_db(name: str, cfg: AMXConfig = Depends(get_cfg)) -> dict[str, Any]:
    profile = cfg.db_profiles.get(name)
    if profile is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No DB profile named {name!r}.",
        )
    return _mask_db(profile, name, is_active=True)


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
    return _mask_db(merged, name, is_active=True)


@router.delete("/db/{name}")
def delete_db(name: str, cfg: AMXConfig = Depends(get_cfg)) -> dict[str, Any]:
    """Delete a DB profile. The last remaining profile can be deleted —
    Studio surfaces the empty-config state via the browse sidebar's
    "no profiles yet" empty state and the /ask 412 ``configure-llm``
    flow. Refusing the deletion forced a roundabout reset; matching
    the CLI's ``/remove-db-profile`` behaviour means the SPA stops
    requiring users to add a decoy profile before they can clean up."""
    if name not in cfg.db_profiles:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No DB profile named {name!r}.",
        )
    cfg.remove_db_profile(name)
    cfg.save()
    return {
        "ok": True,
        "name": name,
        "remaining": len(cfg.db_profiles),
    }


@router.post("/db/{name}/activate")
def activate_db(name: str) -> dict[str, Any]:
    """Activation is no longer a user-facing concept for DB profiles.
    Every defined profile is selectable from every Studio surface
    (Run, Ask, Browse) per-action; nothing is "active" globally.
    The endpoint stays in the URL space so older Studio bundles get
    a clear 410 with a hint instead of silently 404-ing on a route
    the new server no longer registers."""
    raise HTTPException(
        status_code=status.HTTP_410_GONE,
        detail=(
            "DB profile activation removed in 0.13: every defined profile "
            "is selectable from Run, Ask, and Browse directly. The CLI "
            "fallback ('amx run' without --profile) uses the first defined "
            "profile; change the order in ~/.amx/config.yml or run "
            "/use-db <name> to override."
        ),
    )


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
    """Delete an LLM profile. The active profile and the last
    remaining profile can both be deleted — /ask gates on
    :func:`SearchAgent._llm_available` (CLI) and the configure-llm
    412 pre-flight (Studio), so an empty config surfaces a friendly
    "configure an LLM profile" prompt rather than failing silently.
    """
    if name not in cfg.llm_profiles:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No LLM profile named {name!r}.",
        )
    cfg.remove_llm_profile(name)
    cfg.save()
    return {
        "ok": True,
        "name": name,
        "remaining": len(cfg.llm_profiles),
        "active": cfg.active_llm_profile or None,
    }


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
    its source paths inline. ``linked_db_profiles`` (empty = global)
    drives doc retrieval scope for /ask."""
    items = [
        {
            "name": name,
            "paths": list(paths or []),
            "is_active": name == (getattr(cfg, "active_doc_profile", "") or ""),
            "linked_db_profiles": list(cfg.doc_profile_linked_dbs.get(name, []) or []),
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
            "linked_db_profiles": list(cfg.code_profile_linked_dbs.get(name, []) or []),
        }
        for name, value in sorted(cfg.code_profiles.items())
    ]
    return {"profiles": items, "active": getattr(cfg, "active_code_profile", "") or None}


def _validate_linked_dbs(cfg: AMXConfig, raw: Any, *, kind: str) -> list[str] | None:
    """Common validation for the optional ``linked_db_profiles`` field.

    Returns ``None`` when the body did not include the key at all (caller
    leaves links untouched). Returns a sanitised list otherwise. Raises
    HTTPException 400 when the value is malformed or names a missing DB.
    """
    if raw is None:
        return None
    if not isinstance(raw, list):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="'linked_db_profiles' must be an array of DB profile names.",
        )
    cleaned: list[str] = []
    for entry in raw:
        if not isinstance(entry, str):
            continue
        db = entry.strip()
        if not db:
            continue
        if db not in cfg.db_profiles:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unknown DB profile {db!r} on {kind} link.",
            )
        if db not in cleaned:
            cleaned.append(db)
    return cleaned


@router.put("/docs/{name}")
def upsert_docs(
    name: str,
    body: dict[str, Any],
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Create / update a doc profile.

    Body shape: ``{"paths": [...], "linked_db_profiles": [...]?}``.
    ``linked_db_profiles`` is optional — when absent the existing links
    are preserved, when present (even as ``[]``) the new value replaces
    them. ``[]`` flips the profile back to global scope.
    """
    raw_paths = body.get("paths") if isinstance(body, dict) else None
    if not isinstance(raw_paths, list):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Body must include a 'paths' array of strings.",
        )
    cleaned = [str(p).strip() for p in raw_paths if str(p).strip()]
    raw_links = body.get("linked_db_profiles") if isinstance(body, dict) else None
    links = _validate_linked_dbs(cfg, raw_links, kind="doc")
    cfg.doc_profiles[name] = cleaned
    if links is not None:
        if links:
            cfg.doc_profile_linked_dbs[name] = links
        else:
            cfg.doc_profile_linked_dbs.pop(name, None)
    cfg.save()
    return {
        "name": name,
        "paths": cleaned,
        "is_active": name == (getattr(cfg, "active_doc_profile", "") or ""),
        "linked_db_profiles": list(cfg.doc_profile_linked_dbs.get(name, []) or []),
    }


@router.delete("/docs/{name}")
def delete_docs(name: str, cfg: AMXConfig = Depends(get_cfg)) -> dict[str, Any]:
    if name not in cfg.doc_profiles:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No doc profile named {name!r}.",
        )
    # Goes through cfg.remove_doc_profile so the linked-DB map is also
    # cleaned (otherwise a stale ``contracts -> [prod_pg]`` entry would
    # hang around with the doc profile gone, only pruned the next time
    # config is reloaded).
    cfg.remove_doc_profile(name)
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

    Body shape: ``{"path": "...", "linked_db_profiles": [...]?}``.
    """
    raw_path = body.get("path") if isinstance(body, dict) else None
    if not isinstance(raw_path, str) or not raw_path.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Body must include a non-empty 'path' string.",
        )
    raw_links = body.get("linked_db_profiles") if isinstance(body, dict) else None
    links = _validate_linked_dbs(cfg, raw_links, kind="code")
    cfg.code_profiles[name] = raw_path.strip()
    if links is not None:
        if links:
            cfg.code_profile_linked_dbs[name] = links
        else:
            cfg.code_profile_linked_dbs.pop(name, None)
    cfg.save()
    return {
        "name": name,
        "path": cfg.code_profiles[name],
        "is_active": name == (getattr(cfg, "active_code_profile", "") or ""),
        "linked_db_profiles": list(cfg.code_profile_linked_dbs.get(name, []) or []),
    }


@router.delete("/code/{name}")
def delete_code(name: str, cfg: AMXConfig = Depends(get_cfg)) -> dict[str, Any]:
    if name not in cfg.code_profiles:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No code profile named {name!r}.",
        )
    cfg.remove_code_profile(name)
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
