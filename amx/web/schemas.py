"""Pydantic DTOs returned by the AMX Studio API.

These are intentionally thin — most read endpoints pass the matching
:mod:`amx.config` / :mod:`amx.db.connector` / :mod:`amx.storage.sqlite_store`
shape through unchanged. We only declare a model when the response
needs to be different from the underlying dataclass (e.g. masking
secrets before serialization).
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class HealthResponse(BaseModel):
    """Basic liveness probe for the SPA's startup banner."""

    ok: bool
    version: str


class VersionResponse(BaseModel):
    """Component versions surfaced in the UI's About dialog."""

    model_config = ConfigDict(populate_by_name=True)

    amx: str = Field(..., description="amx-cli package version")
    schema_version: int = Field(
        ..., alias="schema", description="SQLite history-store schema version"
    )
    web: str = Field("v1", description="AMX Studio API version")


class LLMProfileDefaults(BaseModel):
    """Tuning values pulled straight from ``cfg.llm`` so the SPA can
    pre-fill the RunNew "Advanced LLM settings" disclosure with the
    active profile's current values. Everything is optional because the
    underlying dataclass field types are heterogeneous and we want the
    SPA to handle a "field not loaded yet" state without exploding.
    """

    temperature: float | None = None
    max_tokens: int | None = None
    n_alternatives: int | None = None
    column_batch_size: int | None = None
    prompt_detail: str | None = None
    description_verbosity: str | None = None
    thinking_budget: int | None = None
    logprob_high: float | None = None
    logprob_medium: float | None = None
    custom_input_cost_per_mtok: float | None = None
    custom_output_cost_per_mtok: float | None = None


class ContextResponse(BaseModel):
    """Active config snapshot the SPA shows in the top bar (active LLM
    pill, current schema/table breadcrumbs).

    DB activation was retired in 0.13: every defined DB profile is
    selectable from Run / Ask / Browse directly, so there is no
    "active DB" pill to render. The CLI keeps a default-fallback
    pointer internally (``cfg.active_db_profile``) for ``amx run``
    without ``--profile``; that's deliberately not surfaced here so
    SPA consumers can't grow a dependency on it again.
    """

    active_llm_profile: str | None = None
    active_doc_profile: str | None = None
    active_code_profile: str | None = None
    current_schema: str | None = None
    current_table: str | None = None
    db_backend: str | None = None
    llm_provider: str | None = None
    llm_model: str | None = None
    llm_supports_batch: bool = False
    # Tuning values from the active LLM profile, surfaced so the
    # /runs/new "Advanced LLM settings" disclosure can pre-fill from
    # what the user already configured. Read-only mirror of cfg.llm.
    llm_profile_defaults: LLMProfileDefaults | None = None
    # Identity of the local AMX user — read once at request time from
    # the OS (``getpass.getuser`` + ``socket.gethostname``). The SPA
    # uses these to tell *its* writes apart from those a teammate
    # made via a shared history store: "you applied this" vs "alice
    # applied this" on the Audit timeline + the pre-run conflict
    # banner.
    current_user: str | None = None
    current_hostname: str | None = None


class ErrorResponse(BaseModel):
    """Uniform error envelope so the SPA can render every failure with
    the same toast component."""

    detail: str
    hint: str | None = None
    extra: dict[str, Any] | None = None
