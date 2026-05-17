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

import importlib.util
import logging
from dataclasses import asdict, fields, replace
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status

from amx.config import (
    SUPPORTED_EMBEDDING_KINDS,
    AMXConfig,
    DBConfig,
    EmbeddingConfig,
    LLMConfig,
)
from amx.db.profile_schema import (
    FieldSpec,
    ProfileValidationError,
    spec_for,
    supported_backends,
    validate_required_fields,
)
from amx.storage.secrets import get_default_store, is_secret_reference, parse_reference
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

#: EmbeddingConfig fields likewise.
_EMBEDDING_SECRET_FIELDS = frozenset({"api_key"})

#: Sides accepted by the embedding endpoints. The two sides are
#: independent — docs RAG and code RAG carry their own provider.
_EMBEDDING_SIDES = ("docs", "code")


# ── Backend / provider catalogs ────────────────────────────────────────


# Backend-label and default-port metadata. The per-field details
# (``fields``, ``field_specs``) are derived from
# :mod:`amx.db.profile_schema` so Studio and the CLI wizard share one
# source of truth — that is what stops the Databricks-TLS-style drift
# (URL builder reads a field, Studio never offers it, user can't reach
# the connection).
_DB_BACKEND_META: dict[str, dict[str, Any]] = {
    "postgresql": {"label": "PostgreSQL", "default_port": 5432},
    "mysql": {"label": "MySQL / MariaDB", "default_port": 3306},
    "snowflake": {"label": "Snowflake"},
    "databricks": {"label": "Databricks (Unity Catalog)", "supports_catalog": True},
    "bigquery": {"label": "BigQuery"},
    "oracle": {"label": "Oracle", "default_port": 1521},
    "mssql": {"label": "SQL Server", "default_port": 1433},
    "redshift": {"label": "Redshift", "default_port": 5439},
    "clickhouse": {"label": "ClickHouse", "default_port": 8123},
    "duckdb": {"label": "DuckDB"},
}


def _serialize_field(spec: FieldSpec) -> dict[str, Any]:
    """JSON shape of a FieldSpec for the Studio API.

    Frontend reads ``kind`` to pick a renderer (text / password / int /
    bool / select), ``group`` to bucket into basic vs. advanced, and
    ``help`` for the field tooltip. Older Studio bundles that only know
    about ``fields: list[str]`` keep working because that legacy array
    is still returned alongside this enrichment.
    """
    return {
        "name": spec.name,
        "kind": spec.kind,
        "label": spec.label or spec.name,
        "help": spec.help,
        "secret": spec.secret,
        "required": spec.required,
        "group": spec.group,
        "options": list(spec.options),
    }


# Backends that cannot host AMX's run-history schema. Sourced from each
# adapter's ``BackendCapabilities.supports_shared_history=False``;
# duplicated here so the Studio backends endpoint can answer without
# importing the adapter (which would force a driver load). The list is
# tiny and stable — DuckDB (local file) and ClickHouse (no row
# UPDATE for the finish_run lifecycle) — and lint guards in
# ``tests/test_history_store_capability_gating.py`` already prevent it
# from drifting.
_BACKENDS_WITHOUT_SHARED_HISTORY: frozenset[str] = frozenset({"duckdb", "clickhouse"})


def _backend_entry(backend: str) -> dict[str, Any]:
    meta = _DB_BACKEND_META.get(backend, {})
    specs = spec_for(backend)
    return {
        "id": backend,
        "label": meta.get("label", backend.title()),
        # Legacy list of field-name strings — kept for clients that
        # haven't migrated to ``field_specs`` yet.
        "fields": [s.name for s in specs],
        "field_specs": [_serialize_field(s) for s in specs],
        # ``supports_shared_history`` lets Studio render a non-blocking
        # info banner when the user saves a profile on a backend that
        # cannot host AMX's run-history schema. The CLI surfaces the
        # same hint via ``/history-store enable``.
        "supports_shared_history": backend not in _BACKENDS_WITHOUT_SHARED_HISTORY,
        **{k: v for k, v in meta.items() if k != "label"},
    }


_DB_BACKENDS: list[dict[str, Any]] = [_backend_entry(b) for b in supported_backends()]


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
    # Surgical Databricks-only check: reject a save that would leave
    # ``catalog`` blank, which is the specific misconfiguration that
    # produced the ``SHOW TABLES FROM `None`.<schema>`` Spark crash.
    # The full per-backend required-field sweep
    # (:func:`validate_required_fields`) is reserved for the CLI
    # wizard so the route stays permissive — Studio's profile form
    # already enforces field-level requirements before submit, and a
    # half-saved profile in localStorage shouldn't 400 here.
    backend_name = (merged.backend or existing.backend or "").strip().lower()
    if backend_name == "databricks":
        try:
            validate_required_fields(backend_name, asdict(merged))
        except ProfileValidationError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "message": str(exc),
                    "backend": exc.backend,
                    "missing": exc.missing,
                    "hint": "fill-required-fields",
                },
            ) from exc
    cfg.upsert_db_profile(name, merged)
    cfg.save()
    # A profile edit (host / catalog / database / password / token /
    # anything) leaves the existing cached connector in ``live_db``'s
    # _CONNECTOR_CACHE pointing at the OLD scope or OLD credentials.
    # Evicting by profile name forces the next request to build a
    # fresh connector against the just-saved DBConfig.
    from amx.web.routers.live_db import evict_connector_cache

    evict_connector_cache(name)
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
    # Close + drop any cached connector for the just-deleted profile
    # so its pool handles don't outlive the profile itself.
    from amx.web.routers.live_db import evict_connector_cache

    evict_connector_cache(name)
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
    _invalidate_credential_cache(name)
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
    _invalidate_credential_cache(name)
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


# ── Embedding providers (docs + code) ──────────────────────────────────
#
# Mirrors the CLI ``/embeddings`` slash command. The config carries two
# independent ``EmbeddingConfig`` blocks (``cfg.embedding_docs`` and
# ``cfg.embedding_code``); each endpoint targets one side via the
# ``{side}`` path parameter. After every PUT we reinstall both factory
# slots from the live cfg so subsequent ``/search`` and code-RAG queries
# pick up the new providers without restarting the server.


def _check_side(side: str) -> str:
    if side not in _EMBEDDING_SIDES:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Unknown embedding side {side!r}. Expected one of {_EMBEDDING_SIDES}.",
        )
    return side


def _embedding_for_side(cfg: AMXConfig, side: str) -> EmbeddingConfig:
    return getattr(cfg, f"embedding_{_check_side(side)}")


def _mask_embedding(emb: EmbeddingConfig) -> dict[str, Any]:
    raw = asdict(emb)
    for secret in _EMBEDDING_SECRET_FIELDS:
        if raw.get(secret):
            raw[secret] = SECRET_PLACEHOLDER
    raw["is_configured"] = emb.is_configured()
    return raw


def _merge_embedding_patch(existing: EmbeddingConfig, body: dict[str, Any]) -> EmbeddingConfig:
    valid = {f.name for f in fields(EmbeddingConfig)}
    diff: dict[str, Any] = {}
    for key, value in body.items():
        if key not in valid:
            continue
        if key in _EMBEDDING_SECRET_FIELDS and value == SECRET_PLACEHOLDER:
            continue  # keep existing secret
        diff[key] = value
    return replace(existing, **diff)


def _sentence_transformers_available() -> bool:
    """Cheap probe — no actual import, so the chromadb tax stays unpaid
    until the user opts into a non-default provider."""
    try:
        return importlib.util.find_spec("sentence_transformers") is not None
    except Exception:
        return False


def _kinds_payload() -> dict[str, Any]:
    """Static metadata for the Studio kind picker.

    ``available`` for ``sentence_transformers`` is probed at request
    time so a remote-Studio host that lacks the extra renders the
    option disabled rather than failing on save.
    """
    return {
        "kinds": [
            {
                "id": "minilm",
                "label": "MiniLM (--default)",
                "needs_model": False,
                "needs_key": False,
                "needs_base": False,
                "hint": "Chroma's bundled all-MiniLM-L6-v2 (offline, fastest, lowest quality).",
                "available": True,
            },
            {
                "id": "openai_compatible",
                "label": "OpenAI-compatible",
                "needs_model": True,
                "needs_key": True,
                "needs_base": True,
                "hint": (
                    "OpenAI / OpenRouter / Together / Mistral / Azure / vLLM / "
                    "LM Studio / llama.cpp server / any /embeddings endpoint."
                ),
                "available": True,
            },
            {
                "id": "sentence_transformers",
                "label": "Local sentence-transformers",
                "needs_model": True,
                "needs_key": False,
                "needs_base": False,
                "hint": (
                    "Any HuggingFace sentence-transformers model (offline). "
                    'Requires `pip install "amx-cli[local-embeddings]"` on this host.'
                ),
                "available": _sentence_transformers_available(),
            },
        ],
        "presets": [
            {"id": "openai", "label": "OpenAI", "base_url": "https://api.openai.com/v1"},
            {
                "id": "openrouter",
                "label": "OpenRouter",
                "base_url": "https://openrouter.ai/api/v1",
            },
            {"id": "together", "label": "Together", "base_url": "https://api.together.xyz/v1"},
            {"id": "mistral", "label": "Mistral", "base_url": "https://api.mistral.ai/v1"},
            {
                "id": "deepinfra",
                "label": "DeepInfra",
                "base_url": "https://api.deepinfra.com/v1/openai",
            },
            {
                "id": "azure",
                "label": "Azure OpenAI",
                "base_url": ("https://<resource>.openai.azure.com/openai/deployments/<deployment>"),
            },
            {
                "id": "vllm",
                "label": "vLLM / LM Studio / llama.cpp (local)",
                "base_url": "http://localhost:8000/v1",
            },
        ],
        "sides": list(_EMBEDDING_SIDES),
        "supported_kinds": list(SUPPORTED_EMBEDDING_KINDS),
    }


@router.get("/embedding/kinds")
def list_embedding_kinds() -> dict[str, Any]:
    return _kinds_payload()


@router.get("/embedding")
def get_embedding(cfg: AMXConfig = Depends(get_cfg)) -> dict[str, Any]:
    return {side: _mask_embedding(_embedding_for_side(cfg, side)) for side in _EMBEDDING_SIDES}


@router.put("/embedding/{side}")
def upsert_embedding(
    side: str,
    body: dict[str, Any],
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    side = _check_side(side)
    existing = _embedding_for_side(cfg, side)
    kind = (body.get("kind") or existing.kind or "").lower().strip()
    if kind and kind not in {"", "default", "minilm-l6-v2", *SUPPORTED_EMBEDDING_KINDS}:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unknown embedding kind {kind!r}. Expected one of {SUPPORTED_EMBEDDING_KINDS}.",
        )
    merged = _merge_embedding_patch(existing, body)
    setattr(cfg, f"embedding_{side}", merged)
    cfg.save()
    # Re-install both factory slots so subsequent retrieval calls pick
    # up the new provider without a server restart.
    try:
        from amx.search.embeddings import configure_from_amx_config

        configure_from_amx_config(cfg)
    except Exception as exc:  # pragma: no cover — defensive
        log.warning("Failed to reinstall embedding factory after PUT: %s", exc)
    return _mask_embedding(merged)


@router.post("/embedding/{side}/test")
def test_embedding(
    side: str,
    body: dict[str, Any],
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Embed a one-token sentinel with the merged-not-saved settings.

    Lets the Studio "Test" button surface a green/red pill plus the
    actual vector dimension before the user commits the change.
    """
    side = _check_side(side)
    existing = _embedding_for_side(cfg, side)
    merged = _merge_embedding_patch(existing, body)
    kind = (merged.kind or "").lower().strip()
    if kind in {"", "minilm", "default", "minilm-l6-v2"}:
        # MiniLM is the bundled default; nothing to round-trip beyond
        # confirming Chroma can construct it.
        try:
            from amx.search.embeddings import MiniLMEmbedding

            ef = MiniLMEmbedding()
            dim = int(getattr(ef, "dim", 384))
        except Exception as exc:
            return {"ok": False, "message": f"{exc.__class__.__name__}: {exc}"}
        return {"ok": True, "message": "MiniLM ready (Chroma bundled)", "dim": dim}

    if not merged.is_configured():
        return {
            "ok": False,
            "message": f"Provider {merged.kind!r} requires a model id before it can be tested.",
        }
    try:
        from amx.search.embeddings import make_embedding_function

        ef = make_embedding_function(
            merged.kind,
            model=merged.model,
            api_key=merged.api_key,
            base_url=merged.base_url,
        )
    except Exception as exc:
        return {"ok": False, "message": f"{exc.__class__.__name__}: {exc}"}
    if ef is None:
        return {"ok": True, "message": "Configured for bundled MiniLM.", "dim": 384}
    try:
        vectors = ef(["AMX embedding connection test."])
    except Exception as exc:
        return {"ok": False, "message": f"{exc.__class__.__name__}: {exc}"}
    if not vectors:
        return {"ok": False, "message": "Provider returned no vectors."}
    dim = len(list(vectors[0]) if vectors else [])
    return {"ok": True, "message": f"OK — embedded one sample in {dim} dimensions.", "dim": dim}


def _collection_status_for_side(side: str, cfg: AMXConfig) -> dict[str, Any]:
    """Inspect persisted collections to detect stale-vector state.

    Returns the per-collection ``provider``/``model``/``count`` plus a
    ``stale`` flag. Stale means the collection's metadata recorded a
    different provider/model than what ``cfg.embedding_{side}`` now
    resolves to, so vectors must be re-embedded.
    """
    from pathlib import Path

    try:
        import chromadb
    except Exception:
        return {"collections": [], "stale": False, "error": "chromadb not installed"}

    persist = str(Path.home() / ".amx" / "chroma_db")
    try:
        client = chromadb.PersistentClient(path=persist)
    except Exception as exc:
        return {"collections": [], "stale": False, "error": f"{exc.__class__.__name__}: {exc}"}

    if side == "docs":
        from amx.docs.rag import _resolve_docs_embedding

        active_provider, active_model, _ = _resolve_docs_embedding(cfg)
        prefix = "amx_search"
    else:
        from amx.codebase.code_rag import _resolve_code_embedding

        active_provider, active_model, _ = _resolve_code_embedding(cfg)
        prefix = "amx_code"

    collections: list[dict[str, Any]] = []
    stale = False
    for coll in client.list_collections():
        name = getattr(coll, "name", "")
        if not name.startswith(prefix):
            continue
        try:
            meta = dict(coll.metadata or {})
        except Exception:
            meta = {}
        recorded_provider = str(meta.get("embedding_provider") or "")
        recorded_model = str(meta.get("embedding_model") or "")
        try:
            count = int(coll.count())
        except Exception:
            count = 0
        is_stale = bool(
            recorded_provider
            and recorded_model
            and (recorded_provider != active_provider or recorded_model != active_model)
        )
        if is_stale:
            stale = True
        collections.append(
            {
                "name": name,
                "count": count,
                "embedding_provider": recorded_provider,
                "embedding_model": recorded_model,
                "stale": is_stale,
            }
        )
    return {
        "collections": collections,
        "stale": stale,
        "current_provider": active_provider,
        "current_model": active_model,
    }


@router.get("/embedding/status")
def get_embedding_status(cfg: AMXConfig = Depends(get_cfg)) -> dict[str, Any]:
    return {side: _collection_status_for_side(side, cfg) for side in _EMBEDDING_SIDES}


@router.post("/embedding/{side}/rebuild")
def rebuild_embedding(
    side: str,
    body: dict[str, Any] | None = None,
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Clear the per-side vector collections so the next query/ingest
    re-embeds with the active provider.

    Blocking — large catalogs take seconds to tens of seconds; jobs
    infrastructure is out of scope for the initial split.
    """
    side = _check_side(side)
    body = body or {}
    profile_filter = (body.get("profile") or "").strip() or None

    if side == "docs":
        try:
            from amx.search.catalog import SearchCatalog
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Docs catalog unavailable: {exc.__class__.__name__}: {exc}",
            ) from exc
        catalog = SearchCatalog.from_history_store()
        if catalog is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Docs catalog is not initialised; run /sync first.",
            )
        rebuilt: list[str] = []
        targets = [profile_filter] if profile_filter else sorted(cfg.db_profiles.keys()) or [""]
        for db_profile in targets:
            try:
                catalog.rebuild_profile(db_profile or "")
                rebuilt.append(db_profile or "(default)")
            except Exception as exc:
                log.warning("rebuild_profile failed for %r: %s", db_profile, exc)
        return {
            "ok": True,
            "side": "docs",
            "rebuilt": rebuilt,
            "message": f"Rebuilt {len(rebuilt)} docs collection(s).",
        }

    # side == "code"
    try:
        from amx.codebase.code_rag import delete_code_collection
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Code RAG unavailable: {exc.__class__.__name__}: {exc}",
        ) from exc
    cleared = bool(
        delete_code_collection(source_filters=[profile_filter] if profile_filter else None)
    )
    return {
        "ok": True,
        "side": "code",
        "cleared": cleared,
        "message": (
            "Cleared the code collection; the next /code query will re-embed "
            "with the active provider."
            if cleared
            else "Code collection did not exist; nothing to clear."
        ),
    }


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


@router.get("/docs/{name}/health")
def doc_profile_health(
    name: str,
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Per-doc-profile health card for Studio Settings.

    Combines on-disk Chroma stats (chunk count, embedding metadata)
    with config-side telemetry (last ingested timestamp, last error
    one-liner) so the user can see at a glance whether the profile
    is wired up and indexed without dropping to the CLI.

    Returns 404 when the profile is unknown so the SPA can distinguish
    "no telemetry yet" (200 with zeros) from a typo in the URL.
    """
    if name not in cfg.doc_profiles:
        from fastapi import HTTPException

        raise HTTPException(status_code=404, detail=f"Doc profile '{name}' not found.")
    paths = list(cfg.doc_profiles.get(name) or [])
    chunk_count = 0
    embedding_model: str | None = None
    embedding_provider: str | None = None
    try:
        from amx.docs.rag import RAGStore

        store = RAGStore(source_filters=cfg.effective_doc_paths(name) or None)
        chunk_count = int(store.filtered_doc_count())
        meta = dict(store.collection.metadata or {})
        embedding_model = str(meta.get("embedding_model")) if meta.get("embedding_model") else None
        embedding_provider = (
            str(meta.get("embedding_provider")) if meta.get("embedding_provider") else None
        )
    except Exception as exc:
        log.debug("doc_profile_health: RAGStore probe failed: %s", exc)
    last_ingested = cfg.doc_profiles_last_ingested_at.get(name, 0.0) or 0.0
    last_error = cfg.doc_profiles_last_error.get(name, "") or ""
    return {
        "name": name,
        "chunk_count": chunk_count,
        "last_ingested_at": float(last_ingested) if last_ingested else None,
        "last_error": last_error or None,
        "embedding_model": embedding_model,
        "embedding_provider": embedding_provider,
        "paths": paths,
        "linked_db_profiles": list(cfg.doc_profile_linked_dbs.get(name, []) or []),
        "local_files": _list_local_files_for_paths(paths),
    }


def _list_local_files_for_paths(paths: list[str]) -> list[dict[str, Any]]:
    """Walk each local entry in ``paths`` and return a flat file
    inventory ``[{path, name, size_bytes, modified_at, source_root}, …]``.

    Lets the doc-profile health card show the user which files are
    actually staged under a profile without forcing them to launch a
    scan job. Remote schemes (``http://``, ``s3://``, ``gs://``) are
    skipped — those need the full scan worker to enumerate. Result is
    capped at 200 entries with a trailing ``{"__truncated__": true}``
    marker so the UI can render a "more files exist" hint.
    """
    from pathlib import Path

    from amx.docs.extensions import INGEST_EXCLUDE_NAMES, SUPPORTED_EXTENSIONS
    from amx.docs.uploads import read_display_names_for_root

    remote_prefixes = ("http://", "https://", "s3://", "gs://")
    cap = 200
    out: list[dict[str, Any]] = []
    for raw in paths:
        spec = (raw or "").strip()
        if not spec:
            continue
        lowered = spec.lower()
        if any(lowered.startswith(p) for p in remote_prefixes):
            continue
        if lowered.startswith("file://"):
            spec = spec[len("file://") :]
        try:
            base = Path(spec).expanduser().resolve()
        except Exception:
            continue
        if base.is_file():
            try:
                st = base.stat()
            except Exception:
                continue
            # Look one level up for a manifest in case the path points
            # at an individual uploaded file (rare but possible).
            display_map = read_display_names_for_root(base.parent)
            out.append(
                {
                    "path": str(base),
                    "name": base.name,
                    "display_name": display_map.get(base.name, base.name),
                    "size_bytes": int(st.st_size),
                    "modified_at": float(st.st_mtime),
                    "source_root": str(base.parent),
                }
            )
            continue
        if not base.is_dir():
            continue
        # Read the upload manifest once per directory — cheap dict
        # lookup per file vs re-reading JSON for each entry.
        display_map = read_display_names_for_root(base)
        truncated = False
        for f in sorted(base.rglob("*")):
            if len(out) >= cap:
                truncated = True
                break
            if not f.is_file():
                continue
            if f.name in INGEST_EXCLUDE_NAMES:
                # Sidecar manifest (``.amx-manifest.json``) bookkeeps the
                # upload batch; never user content. Hiding it here keeps
                # the Studio file inventory honest — ``3 files`` should
                # mean three uploads, not "two uploads + an internal
                # manifest the scanner shouldn't have indexed either".
                continue
            if f.suffix.lower() not in SUPPORTED_EXTENSIONS:
                continue
            try:
                st = f.stat()
            except Exception:
                continue
            out.append(
                {
                    "path": str(f),
                    "name": f.name,
                    "display_name": display_map.get(f.name, f.name),
                    "size_bytes": int(st.st_size),
                    "modified_at": float(st.st_mtime),
                    "source_root": str(base),
                }
            )
        if truncated:
            out.append({"__truncated__": True, "source_root": str(base)})
    return out


@router.get("/code/{name}/health")
def code_profile_health(
    name: str,
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Per-code-profile health card for Studio Settings (PR δ).

    Parallel to :func:`doc_profile_health`. Combines on-disk Chroma
    stats (chunk count for this profile's paths, recorded embedding
    metadata) with config-side telemetry (last indexed timestamp,
    last error one-liner) so the user can tell at a glance whether
    the profile is wired up and indexed without dropping to the CLI.

    Returns 404 when the profile is unknown so the SPA can distinguish
    "no telemetry yet" (200 with zeros) from a typo in the URL.
    """
    if name not in cfg.code_profiles:
        from fastapi import HTTPException

        raise HTTPException(status_code=404, detail=f"Code profile '{name}' not found.")
    paths = cfg.effective_code_paths(name)
    chunk_count = 0
    embedding_model: str | None = None
    embedding_provider: str | None = None
    try:
        from amx.codebase.code_rag import code_collection_count, code_collection_metadata

        chunk_count = int(code_collection_count(source_filters=paths or None))
        meta = code_collection_metadata()
        embedding_model = str(meta.get("embedding_model")) if meta.get("embedding_model") else None
        embedding_provider = (
            str(meta.get("embedding_provider")) if meta.get("embedding_provider") else None
        )
    except Exception as exc:
        log.debug("code_profile_health: code RAG probe failed: %s", exc)
    last_indexed = cfg.code_profile_last_indexed_at.get(name, 0.0) or 0.0
    last_error = cfg.code_profile_last_error.get(name, "") or ""
    return {
        "name": name,
        "paths": list(paths or []),
        "chunk_count": chunk_count,
        "last_indexed_at": float(last_indexed) if last_indexed else None,
        "last_error": last_error or None,
        "embedding_model": embedding_model,
        "embedding_provider": embedding_provider,
        "linked_db_profiles": list(cfg.code_profile_linked_dbs.get(name, []) or []),
    }


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


@router.delete("/docs/{name}/files")
def delete_doc_profile_file(
    name: str,
    path: str,
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Remove a single file from a doc profile.

    Drops the file from disk, prunes its entry from the upload-root
    manifest, and deletes any chunks it produced from the Chroma
    collection so subsequent ``/search docs`` queries don't keep
    returning a ghost source. Pre-fix the only way to "drop one file"
    was to delete the whole profile and re-upload the remaining files,
    which got old fast for 10+ file profiles.

    The file MUST sit inside one of the profile's configured paths —
    refused otherwise so a malicious or accidental ``?path=/etc/passwd``
    can't escalate into an arbitrary-delete primitive. Path resolution
    happens with ``Path.resolve()`` on both sides, so a symlink hop or
    a ``..`` trick lands outside the allowed root and gets rejected.
    """
    from pathlib import Path as _Path

    from amx.docs.uploads import MANIFEST_FILENAME

    if name not in cfg.doc_profiles:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No doc profile named {name!r}.",
        )
    raw = (path or "").strip()
    if not raw:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="path query parameter is required.",
        )
    try:
        target = _Path(raw).expanduser().resolve()
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Could not resolve path {raw!r}: {exc}",
        ) from exc

    paths = list(cfg.doc_profiles.get(name) or [])
    allowed_roots: list[_Path] = []
    for spec in paths:
        if not spec or spec.lower().startswith(("http://", "https://", "s3://", "gs://")):
            continue
        try:
            allowed_roots.append(_Path(spec).expanduser().resolve())
        except Exception:
            continue

    def _inside_any_root(child: _Path) -> bool:
        for root in allowed_roots:
            try:
                if root == child or root in child.parents:
                    return True
            except Exception:
                continue
        return False

    if not _inside_any_root(target):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"{raw!r} is not inside any path registered on doc profile {name!r}.",
        )
    if not target.is_file():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"File not found: {target}",
        )

    target_path_str = str(target)
    try:
        target.unlink()
    except OSError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Could not delete {target}: {exc}",
        ) from exc

    # Strip the entry from the upload-root manifest so future file-list
    # responses don't keep showing a phantom display_name for the gone
    # file. Best-effort: a corrupted manifest is rebuilt on next upload.
    manifest_path = target.parent / MANIFEST_FILENAME
    if manifest_path.is_file():
        try:
            import json as _json

            payload = _json.loads(manifest_path.read_text())
            if isinstance(payload, dict):
                files_map = payload.get("files")
                if isinstance(files_map, dict) and target.name in files_map:
                    files_map.pop(target.name, None)
                    payload["files"] = files_map
                    tmp = manifest_path.with_suffix(manifest_path.suffix + ".part")
                    tmp.write_text(_json.dumps(payload, indent=2, sort_keys=True))
                    tmp.replace(manifest_path)
        except Exception as exc:
            log.warning("Could not prune manifest entry for %s: %s", target, exc)

    chunks_removed = 0
    try:
        from amx.docs.rag import RAGStore

        store = RAGStore(source_filters=cfg.effective_doc_paths(name) or None)
        chunks_removed = int(store.delete_chunks_for_sources([target_path_str]))
    except Exception as exc:
        log.warning("Could not delete chunks for %s: %s", target, exc)

    return {
        "ok": True,
        "path": target_path_str,
        "chunks_removed": chunks_removed,
    }


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


#: Lazy in-process cache for ``has_credentials`` results that require a
#: keyring lookup. Key: ``(profile_name, secret_ref)`` so renaming a
#: profile or rotating its keyring ref produces a fresh entry. Cleared
#: on every credential mutation via :func:`_invalidate_credential_cache`.
_CREDENTIAL_CACHE: dict[tuple[str, str], bool] = {}


def _check_credentials_cached(profile_name: str, llm: LLMConfig) -> bool:
    """Resolve whether *llm* has usable credentials, lazily + cached.

    Profiles whose ``api_key`` is a literal string take the fast path
    (no keyring call). Profiles whose ``api_key`` is a ``keyring:`` ref
    pay one keyring lookup per (name, ref) pair per process lifetime;
    subsequent reads hit :data:`_CREDENTIAL_CACHE`. Resolver failures
    (locked keychain, secret-service unavailable) cache ``False`` and
    log once — they MUST NOT propagate, otherwise a missing key would
    500 the ``/api/profiles/llm`` list and break Settings entirely.

    The cache is invalidated by :func:`_invalidate_credential_cache`
    from the PUT/DELETE handlers below.
    """
    api_key = (llm.api_key or "").strip()
    if not api_key:
        return False
    if not is_secret_reference(api_key):
        return True
    cache_key = (profile_name, api_key)
    cached = _CREDENTIAL_CACHE.get(cache_key)
    if cached is not None:
        return cached
    try:
        store = get_default_store()
        ref = parse_reference(api_key)
        resolved = store.get(ref)
        has_creds = bool(resolved)
    except Exception as exc:  # noqa: BLE001 — last-resort + logged
        log.warning(
            "has_credentials resolver failed for profile %r (ref=%r): %s; "
            "caching False for the session",
            profile_name,
            api_key,
            exc,
        )
        has_creds = False
    _CREDENTIAL_CACHE[cache_key] = has_creds
    return has_creds


def _invalidate_credential_cache(profile_name: str) -> None:
    """Drop every cache entry for *profile_name* (any ref shape).

    Called after PUT / DELETE on an LLM profile so the next read
    re-resolves against the live keyring.
    """
    stale = [key for key in _CREDENTIAL_CACHE if key[0] == profile_name]
    for key in stale:
        _CREDENTIAL_CACHE.pop(key, None)


def _mask_llm(profile: LLMConfig, name: str, *, is_active: bool) -> dict[str, Any]:
    raw = asdict(profile)
    for secret in _LLM_SECRET_FIELDS:
        if raw.get(secret):
            raw[secret] = SECRET_PLACEHOLDER
    raw["name"] = name
    raw["is_active"] = is_active
    raw["has_credentials"] = _check_credentials_cached(name, profile)
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
