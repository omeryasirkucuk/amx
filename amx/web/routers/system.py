"""System / context routes — what the SPA hits at boot.

Three endpoints:

* ``GET /api/health`` — liveness probe; the SPA pings this once at
  load to confirm the token works.
* ``GET /api/version`` — components shown in the About dialog.
* ``GET /api/context`` — active profiles + breadcrumbs for the top bar.

Everything here reads from the bound :class:`AMXConfig`; nothing
mutates state. PR-E adds the ``POST /api/context/{profile_kind}/{name}/activate``
write-side counterpart.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends

from amx import __version__ as AMX_VERSION
from amx.config import AMXConfig
from amx.web.deps import get_cfg
from amx.web.schemas import ContextResponse, HealthResponse, VersionResponse

router = APIRouter(prefix="/api", tags=["system"])

#: Bumped manually whenever AMX Studio's REST contract changes in
#: a backwards-incompatible way. The SPA reads it on boot to decide
#: whether to show an "AMX upgrade required" banner.
WEB_API_VERSION = "v1"

#: SQLite history-store schema marker. There's no migration-aware
#: counter in :mod:`amx.storage.sqlite_store` today (migrations are
#: tracked via column-existence checks), so we hard-code 1 and bump
#: it the first time AMX Studio needs to gate behaviour on a
#: schema-level migration.
HISTORY_SCHEMA_VERSION = 1


@router.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse(ok=True, version=AMX_VERSION)


@router.get("/version", response_model=VersionResponse)
def version() -> VersionResponse:
    return VersionResponse(amx=AMX_VERSION, schema=HISTORY_SCHEMA_VERSION, web=WEB_API_VERSION)


@router.get("/context", response_model=ContextResponse)
def context(cfg: AMXConfig = Depends(get_cfg)) -> ContextResponse:
    """Read-only snapshot of the active profile context.

    The SPA hits this on every route change so the top-bar pills
    (active DB / active LLM) stay current after the user activates
    a profile from the Settings page.
    """
    db = getattr(cfg, "db", None)
    llm = getattr(cfg, "llm", None)

    # Whether the active provider has a registered Batch API impl.
    # Drives the RunNew "Batch mode" toggle's disabled state — checking
    # the static provider list here saves the SPA from a second
    # round-trip and keeps the truth in the same place as
    # LLMProvider.supports_batch (amx.llm.batch).
    supports_batch = False
    provider = getattr(llm, "provider", "") or ""
    if provider:
        try:
            from amx.llm.batch import supported_providers

            supports_batch = provider in supported_providers()
        except Exception:  # pragma: no cover - defensive
            supports_batch = False

    # Surface the local OS user / hostname so the SPA can colour-code
    # apply events as "you" vs "{teammate}" without re-deriving it on
    # every Audit row. Failures are swallowed because the values are
    # display-only — a missing one shouldn't 500 the whole context
    # query that gates the top-bar pills.
    import getpass
    import socket

    try:
        current_user = getpass.getuser()
    except Exception:
        current_user = ""
    try:
        current_hostname = socket.gethostname()
    except Exception:
        current_hostname = ""

    return ContextResponse(
        active_db_profile=cfg.active_db_profile or None,
        active_db_profiles=list(getattr(cfg, "active_db_profiles", []) or []),
        active_llm_profile=cfg.active_llm_profile or None,
        active_doc_profile=getattr(cfg, "active_doc_profile", None) or None,
        active_code_profile=getattr(cfg, "active_code_profile", None) or None,
        current_schema=getattr(cfg, "current_schema", None) or None,
        current_table=getattr(cfg, "current_table", None) or None,
        db_backend=getattr(db, "backend", None) or None,
        llm_provider=provider or None,
        llm_model=getattr(llm, "model", None) or None,
        llm_supports_batch=supports_batch,
        current_user=current_user or None,
        current_hostname=current_hostname or None,
    )
