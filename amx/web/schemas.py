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


class ContextResponse(BaseModel):
    """Active config snapshot the SPA shows in the top bar (active DB +
    LLM pills, current schema/table breadcrumbs)."""

    active_db_profile: str | None = None
    active_db_profiles: list[str] = Field(default_factory=list)
    active_llm_profile: str | None = None
    active_doc_profile: str | None = None
    active_code_profile: str | None = None
    current_schema: str | None = None
    current_table: str | None = None
    db_backend: str | None = None
    llm_provider: str | None = None
    llm_model: str | None = None


class ErrorResponse(BaseModel):
    """Uniform error envelope so the SPA can render every failure with
    the same toast component."""

    detail: str
    hint: str | None = None
    extra: dict[str, Any] | None = None
