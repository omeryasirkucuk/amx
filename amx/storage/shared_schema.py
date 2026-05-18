"""SQLAlchemy schema for AMX's shared run-history store.

Every table in the shared (warehouse-hosted) AMX schema is declared
here as a portable SQLAlchemy ``Table`` against a single ``MetaData``
instance. ``MetaData.create_all(engine)`` is used to bootstrap the
schema on first connection in :func:`bootstrap_shared_history`.

Design notes:

* **Primary keys are UUID strings** (``String(36)``) because shared
  mode supports concurrent writers from multiple machines. INT
  autoincrement does not work in that setting.
* **Attribution columns** (``created_by``, ``hostname``, ``client_version``,
  ``local_id``) live on every row that originates from a specific
  machine, so ``/history-store list-team`` can show "who ran what".
* ``local_id`` records the local SQLite INT id, which lets the
  dual-write coordinator find the corresponding shared row when later
  ``UPDATE``-style methods (e.g. :meth:`finish_run`) are called.
* JSON columns use :class:`sqlalchemy.JSON` so the dialect chooses the
  best native type — ``JSONB`` on PostgreSQL, ``JSON`` on MySQL, ``VARIANT``
  on Snowflake (via dialect adapters), ``STRING`` everywhere else.
* ``DateTime(timezone=True)`` is used over float epoch seconds because
  warehouse query semantics for "last 7 days" expect actual timestamps.
* **Every table and column carries a ``comment=`` annotation, and the
  schema itself gets a comment via** :data:`DEFAULT_HISTORY_SCHEMA_COMMENT`.
  AMX is a metadata-generation tool — its own warehouse artifacts must
  meet the standard it enforces on user data. ``MetaData.create_all``
  emits the table/column comments as ``COMMENT ON`` statements after
  ``CREATE TABLE`` on every backend that supports them;
  :meth:`DatabaseAdapter.create_history_schema` emits the schema
  comment alongside ``CREATE SCHEMA`` for the same reason.
"""

from __future__ import annotations

import json as _json
from typing import Any

from sqlalchemy import (
    JSON,
    BigInteger,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    PrimaryKeyConstraint,
    String,
    Table,
    Text,
)
from sqlalchemy.types import TypeDecorator, TypeEngine

from amx.storage.schema_descriptions import (
    SCHEMA_DESCRIPTIONS,
    SHARED_SCHEMA_COMMENT,
)


class _JSONAsText(TypeDecorator):
    """JSON stored as TEXT for backends without a native JSON column type.

    ``databricks-sqlalchemy`` does not implement ``visit_JSON``, so the
    SQLAlchemy generic compiler raises *"can't render element of type
    JSON"* the moment ``MetaData.create_all`` tries to emit any JSON
    column on Databricks. The user-visible symptom (reported on
    2026-05-03 against 0.12.3) was a recurring warning on every
    ``amx`` startup:

        Shared history schema not initialised ((in table
        'analysis_runs', column 'scope_json'): Compiler … can't render
        element of type JSON). Run `/history-store enable` to bootstrap
        the AMX schema.

    This decorator stores the value as plain TEXT and round-trips it
    through ``json.dumps`` / ``json.loads`` so callers continue to see
    Python dicts/lists on read — no backend-aware code anywhere else.
    """

    impl = Text
    cache_ok = True

    def process_bind_param(self, value: Any, dialect: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, str):
            # Already serialised — just store as-is.
            return value
        return _json.dumps(value, default=str)

    def process_result_value(self, value: Any, dialect: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, (dict, list)):
            # Some dialects/drivers might pre-deserialise; trust them.
            return value
        try:
            return _json.loads(value)
        except (TypeError, ValueError):
            return value


def _portable_json() -> TypeEngine:
    """Return a JSON column type that compiles on every supported backend.

    Native ``JSON`` (or ``JSONB`` / ``VARIANT``) where the dialect
    knows how to render it; ``_JSONAsText`` on Databricks where
    ``databricks-sqlalchemy`` lacks a ``visit_JSON`` implementation.
    Use this everywhere in this module instead of bare ``JSON`` so a
    new JSON column added later automatically inherits the fix.
    """
    return JSON().with_variant(_JSONAsText(), "databricks")


# Schema name is configurable per-deployment — the user can pick a
# different name in ``/history-store enable``. Default is ``AMX`` which
# matches the user-facing nomenclature in docs/CLI prompts.
DEFAULT_HISTORY_SCHEMA = "AMX"

# Schema comment text written to the namespace itself via
# ``COMMENT ON SCHEMA``. ``MetaData`` does not carry schema-level
# annotations natively, so :meth:`DatabaseAdapter.create_history_schema`
# emits this explicitly after ``CREATE SCHEMA``. Lives in the shared
# source-of-truth module so the local SQLite sidecar can stay aligned.
DEFAULT_HISTORY_SCHEMA_COMMENT = SHARED_SCHEMA_COMMENT

# All client versions writing into a shared store record this as their
# ``schema_version`` so an older client refuses to write into a schema
# bumped by a newer client (avoids losing columns the new client added).
SHARED_SCHEMA_VERSION = 1


def _desc(table: str, column: str | None = None) -> str:
    """Look up the canonical description for a table or column.

    Single accessor for ``SCHEMA_DESCRIPTIONS`` so every ``comment=``
    site in this module reads from the same source the local SQLite
    store reads. Raises ``KeyError`` on a missing entry — that failure
    surfaces in CI via :mod:`tests.test_shared_schema_comments` rather
    than silently shipping an empty comment.
    """
    if column is None:
        return SCHEMA_DESCRIPTIONS[table]["__table__"]
    return SCHEMA_DESCRIPTIONS[table][column]


def build_metadata(schema: str | None = None) -> MetaData:
    """Build a fresh ``MetaData`` bound to *schema*.

    A function (rather than a module-level singleton) is used because
    different deployments may pick different schema names, and SQLAlchemy
    bakes the schema into each ``Table`` at construction time. Tests and
    the bootstrap path each get their own ``MetaData`` for isolation.

    All ``comment=`` strings here are looked up from
    :mod:`amx.storage.schema_descriptions` so the shared schema cannot
    drift from the local SQLite sidecar — see ``CONTRIBUTING.md``.
    """
    md = MetaData(schema=schema or DEFAULT_HISTORY_SCHEMA)

    # ── analysis_runs: one row per /run or /ask invocation ────────────────
    Table(
        "analysis_runs",
        md,
        Column("id", String(36), primary_key=True, comment=_desc("analysis_runs", "id")),
        Column(
            "started_at",
            DateTime(timezone=True),
            nullable=False,
            index=True,
            comment=_desc("analysis_runs", "started_at"),
        ),
        Column("ended_at", DateTime(timezone=True), comment=_desc("analysis_runs", "ended_at")),
        Column("duration_sec", Float, comment=_desc("analysis_runs", "duration_sec")),
        Column("status", String(40), nullable=False, comment=_desc("analysis_runs", "status")),
        Column("command", String(80), nullable=False, comment=_desc("analysis_runs", "command")),
        Column("mode", String(40), comment=_desc("analysis_runs", "mode")),
        Column("db_backend", String(40), comment=_desc("analysis_runs", "db_backend")),
        Column("db_profile", String(120), comment=_desc("analysis_runs", "db_profile")),
        Column("llm_provider", String(40), comment=_desc("analysis_runs", "llm_provider")),
        Column("llm_model", String(120), comment=_desc("analysis_runs", "llm_model")),
        Column("scope_json", _portable_json(), comment=_desc("analysis_runs", "scope_json")),
        Column("metrics_json", _portable_json(), comment=_desc("analysis_runs", "metrics_json")),
        Column("tokens_json", _portable_json(), comment=_desc("analysis_runs", "tokens_json")),
        Column("results_json", _portable_json(), comment=_desc("analysis_runs", "results_json")),
        Column("error_text", Text, comment=_desc("analysis_runs", "error_text")),
        Column(
            "selected_count",
            Integer,
            nullable=False,
            default=0,
            comment=_desc("analysis_runs", "selected_count"),
        ),
        Column(
            "planned_count",
            Integer,
            nullable=False,
            default=0,
            comment=_desc("analysis_runs", "planned_count"),
        ),
        Column(
            "processed_count",
            Integer,
            nullable=False,
            default=0,
            comment=_desc("analysis_runs", "processed_count"),
        ),
        Column(
            "applied_count",
            Integer,
            nullable=False,
            default=0,
            comment=_desc("analysis_runs", "applied_count"),
        ),
        Column("review_strategy", String(40), comment=_desc("analysis_runs", "review_strategy")),
        Column("llm_profile", String(120), comment=_desc("analysis_runs", "llm_profile")),
        Column("doc_profile", String(120), comment=_desc("analysis_runs", "doc_profile")),
        Column("code_profile", String(120), comment=_desc("analysis_runs", "code_profile")),
        Column("settings_json", _portable_json(), comment=_desc("analysis_runs", "settings_json")),
        Column("created_by", String(120), comment=_desc("analysis_runs", "created_by")),
        Column("hostname", String(255), comment=_desc("analysis_runs", "hostname")),
        Column("client_version", String(40), comment=_desc("analysis_runs", "client_version")),
        Column("local_id", BigInteger, comment=_desc("analysis_runs", "local_id")),
        Index("ix_analysis_runs_started_at", "started_at"),
        Index("ix_analysis_runs_local_lookup", "hostname", "local_id"),
        comment=_desc("analysis_runs"),
    )

    # ── run_results: per-asset LLM alternatives + review state ─────────────
    Table(
        "run_results",
        md,
        Column("id", String(36), primary_key=True, comment=_desc("run_results", "id")),
        Column(
            "run_id",
            String(36),
            nullable=False,
            index=True,
            comment=_desc("run_results", "run_id"),
        ),
        Column(
            "saved_at",
            DateTime(timezone=True),
            nullable=False,
            comment=_desc("run_results", "saved_at"),
        ),
        Column(
            "schema_name",
            String(255),
            nullable=False,
            comment=_desc("run_results", "schema_name"),
        ),
        Column(
            "table_name",
            String(255),
            nullable=False,
            comment=_desc("run_results", "table_name"),
        ),
        Column("column_name", String(255), comment=_desc("run_results", "column_name")),
        Column(
            "asset_kind",
            String(40),
            nullable=False,
            default="table",
            comment=_desc("run_results", "asset_kind"),
        ),
        Column("source", String(40), nullable=False, comment=_desc("run_results", "source")),
        Column(
            "confidence",
            String(20),
            nullable=False,
            comment=_desc("run_results", "confidence"),
        ),
        Column("logprob_score", Float, comment=_desc("run_results", "logprob_score")),
        Column("raw_logprob", Float, comment=_desc("run_results", "raw_logprob")),
        Column("token_count", Integer, comment=_desc("run_results", "token_count")),
        Column(
            "model_version",
            String(120),
            nullable=False,
            default="",
            comment=_desc("run_results", "model_version"),
        ),
        Column("reasoning", Text, comment=_desc("run_results", "reasoning")),
        Column(
            "alternatives_json",
            _portable_json(),
            nullable=False,
            comment=_desc("run_results", "alternatives_json"),
        ),
        Column(
            "evaluated_at", DateTime(timezone=True), comment=_desc("run_results", "evaluated_at")
        ),
        Column("applied_at", DateTime(timezone=True), comment=_desc("run_results", "applied_at")),
        Column("chosen_description", Text, comment=_desc("run_results", "chosen_description")),
        Column("evaluation", String(40), comment=_desc("run_results", "evaluation")),
        Column(
            "catalog_status",
            String(40),
            nullable=False,
            default="",
            comment=_desc("run_results", "catalog_status"),
        ),
        Column(
            "catalog_indexed_at",
            DateTime(timezone=True),
            comment=_desc("run_results", "catalog_indexed_at"),
        ),
        Column(
            "db_applied_status",
            String(40),
            nullable=False,
            default="",
            comment=_desc("run_results", "db_applied_status"),
        ),
        Column(
            "effective_source_kind",
            String(40),
            nullable=False,
            default="",
            comment=_desc("run_results", "effective_source_kind"),
        ),
        Column(
            "superseded_at",
            DateTime(timezone=True),
            comment=_desc("run_results", "superseded_at"),
        ),
        Column(
            "rejection_reason",
            Text,
            nullable=False,
            default="",
            comment=_desc("run_results", "rejection_reason"),
        ),
        Column("hostname", String(255), comment=_desc("run_results", "hostname")),
        Column("local_id", BigInteger, comment=_desc("run_results", "local_id")),
        Index("ix_run_results_asset", "schema_name", "table_name", "column_name"),
        Index("ix_run_results_local_lookup", "hostname", "local_id"),
        comment=_desc("run_results"),
    )

    # ── app_events: append-only event log ─────────────────────────────────
    Table(
        "app_events",
        md,
        Column("id", String(36), primary_key=True, comment=_desc("app_events", "id")),
        Column(
            "created_at",
            DateTime(timezone=True),
            nullable=False,
            index=True,
            comment=_desc("app_events", "created_at"),
        ),
        Column("event_type", String(80), nullable=False, comment=_desc("app_events", "event_type")),
        Column("status", String(40), nullable=False, comment=_desc("app_events", "status")),
        Column("command", String(120), nullable=False, comment=_desc("app_events", "command")),
        Column("details_json", _portable_json(), comment=_desc("app_events", "details_json")),
        Column("created_by", String(120), comment=_desc("app_events", "created_by")),
        Column("hostname", String(255), comment=_desc("app_events", "hostname")),
        Column("client_version", String(40), comment=_desc("app_events", "client_version")),
        Index("ix_app_events_created_at", "created_at"),
        comment=_desc("app_events"),
    )

    # ── session_state: namespaced key/value storage ───────────────────────
    Table(
        "session_state",
        md,
        Column(
            "namespace",
            String(120),
            primary_key=True,
            comment=_desc("session_state", "namespace"),
        ),
        Column(
            "key_name",
            String(255),
            primary_key=True,
            comment=_desc("session_state", "key_name"),
        ),
        Column(
            "hostname",
            String(255),
            primary_key=True,
            default="",
            comment=_desc("session_state", "hostname"),
        ),
        Column(
            "value_json",
            _portable_json(),
            nullable=False,
            comment=_desc("session_state", "value_json"),
        ),
        Column(
            "updated_at",
            DateTime(timezone=True),
            nullable=False,
            comment=_desc("session_state", "updated_at"),
        ),
        Column("created_by", String(120), comment=_desc("session_state", "created_by")),
        comment=_desc("session_state"),
    )

    # ── schema_meta: single-row version stamp ────────────────────────────
    Table(
        "schema_meta",
        md,
        Column("id", Integer, primary_key=True, comment=_desc("schema_meta", "id")),
        Column(
            "schema_version",
            Integer,
            nullable=False,
            comment=_desc("schema_meta", "schema_version"),
        ),
        Column(
            "created_at",
            DateTime(timezone=True),
            nullable=False,
            comment=_desc("schema_meta", "created_at"),
        ),
        Column(
            "created_by_client_version",
            String(40),
            comment=_desc("schema_meta", "created_by_client_version"),
        ),
        comment=_desc("schema_meta"),
    )

    # ── style_profiles: derived description-style profiles ────────────────
    Table(
        "style_profiles",
        md,
        Column(
            "id",
            Integer,
            primary_key=True,
            autoincrement=True,
            comment=_desc("style_profiles", "id"),
        ),
        Column(
            "llm_profile",
            String(256),
            nullable=False,
            unique=True,
            comment=_desc("style_profiles", "llm_profile"),
        ),
        Column(
            "source_ref",
            String(1024),
            nullable=False,
            comment=_desc("style_profiles", "source_ref"),
        ),
        Column(
            "source_db_kind",
            String(64),
            nullable=False,
            comment=_desc("style_profiles", "source_db_kind"),
        ),
        Column(
            "profile_json",
            Text,
            nullable=False,
            comment=_desc("style_profiles", "profile_json"),
        ),
        Column(
            "enabled",
            Integer,
            nullable=False,
            default=1,
            comment=_desc("style_profiles", "enabled"),
        ),
        Column(
            "sample_count",
            Integer,
            nullable=False,
            comment=_desc("style_profiles", "sample_count"),
        ),
        Column(
            "created_at",
            Text,
            nullable=False,
            comment=_desc("style_profiles", "created_at"),
        ),
        Column(
            "updated_at",
            Text,
            nullable=False,
            comment=_desc("style_profiles", "updated_at"),
        ),
        comment=_desc("style_profiles"),
    )

    # ── documentation_pages: LLM-composed narrative pages ────────────────
    pages_schema = schema or DEFAULT_HISTORY_SCHEMA
    fq_pages = f"{pages_schema}.documentation_pages"
    Table(
        "documentation_pages",
        md,
        Column(
            "id",
            String(36),
            primary_key=True,
            comment=_desc("documentation_pages", "id"),
        ),
        Column(
            "title",
            String(512),
            nullable=False,
            comment=_desc("documentation_pages", "title"),
        ),
        Column(
            "slug",
            String(512),
            nullable=False,
            unique=True,
            comment=_desc("documentation_pages", "slug"),
        ),
        Column(
            "markdown_body",
            Text,
            nullable=False,
            comment=_desc("documentation_pages", "markdown_body"),
        ),
        Column(
            "rendered_html",
            Text,
            comment=_desc("documentation_pages", "rendered_html"),
        ),
        Column(
            "status",
            String(40),
            nullable=False,
            default="draft",
            comment=_desc("documentation_pages", "status"),
        ),
        Column(
            "created_at",
            DateTime(timezone=True),
            nullable=False,
            comment=_desc("documentation_pages", "created_at"),
        ),
        Column(
            "updated_at",
            DateTime(timezone=True),
            nullable=False,
            comment=_desc("documentation_pages", "updated_at"),
        ),
        Column(
            "created_by",
            String(120),
            comment=_desc("documentation_pages", "created_by"),
        ),
        Column(
            "generation_prompt",
            Text,
            comment=_desc("documentation_pages", "generation_prompt"),
        ),
        Column(
            "model_used",
            String(120),
            comment=_desc("documentation_pages", "model_used"),
        ),
        Index("ix_documentation_pages_status", "status"),
        Index("ix_documentation_pages_updated_at", "updated_at"),
        comment=_desc("documentation_pages"),
    )

    # ── documentation_page_assets: per-page asset list ───────────────────
    Table(
        "documentation_page_assets",
        md,
        Column(
            "id",
            Integer,
            primary_key=True,
            autoincrement=True,
            comment=_desc("documentation_page_assets", "id"),
        ),
        Column(
            "page_id",
            String(36),
            ForeignKey(fq_pages + ".id"),
            nullable=False,
            index=True,
            comment=_desc("documentation_page_assets", "page_id"),
        ),
        Column(
            "asset_kind",
            String(40),
            nullable=False,
            comment=_desc("documentation_page_assets", "asset_kind"),
        ),
        Column(
            "asset_ref",
            String(1024),
            nullable=False,
            comment=_desc("documentation_page_assets", "asset_ref"),
        ),
        Column(
            "included",
            Integer,
            nullable=False,
            default=1,
            comment=_desc("documentation_page_assets", "included"),
        ),
        comment=_desc("documentation_page_assets"),
    )

    # ── documentation_page_sources: per-page upload list ─────────────────
    Table(
        "documentation_page_sources",
        md,
        Column(
            "id",
            Integer,
            primary_key=True,
            autoincrement=True,
            comment=_desc("documentation_page_sources", "id"),
        ),
        Column(
            "page_id",
            String(36),
            ForeignKey(fq_pages + ".id"),
            nullable=False,
            index=True,
            comment=_desc("documentation_page_sources", "page_id"),
        ),
        Column(
            "source_kind",
            String(40),
            nullable=False,
            comment=_desc("documentation_page_sources", "source_kind"),
        ),
        Column(
            "source_path",
            String(1024),
            nullable=False,
            comment=_desc("documentation_page_sources", "source_path"),
        ),
        Column(
            "original_name",
            String(512),
            nullable=False,
            comment=_desc("documentation_page_sources", "original_name"),
        ),
        Column(
            "created_at",
            DateTime(timezone=True),
            nullable=False,
            comment=_desc("documentation_page_sources", "created_at"),
        ),
        comment=_desc("documentation_page_sources"),
    )

    # ── documentation_page_versions: per-save snapshots ──────────────────
    Table(
        "documentation_page_versions",
        md,
        Column(
            "page_id",
            String(36),
            ForeignKey(fq_pages + ".id"),
            nullable=False,
            comment=_desc("documentation_page_versions", "page_id"),
        ),
        Column(
            "version_no",
            Integer,
            nullable=False,
            comment=_desc("documentation_page_versions", "version_no"),
        ),
        Column(
            "markdown_body",
            Text,
            nullable=False,
            comment=_desc("documentation_page_versions", "markdown_body"),
        ),
        Column(
            "saved_at",
            DateTime(timezone=True),
            nullable=False,
            comment=_desc("documentation_page_versions", "saved_at"),
        ),
        Column(
            "saved_by",
            String(120),
            comment=_desc("documentation_page_versions", "saved_by"),
        ),
        Column(
            "note",
            Text,
            comment=_desc("documentation_page_versions", "note"),
        ),
        PrimaryKeyConstraint("page_id", "version_no"),
        comment=_desc("documentation_page_versions"),
    )

    return md


__all__ = [
    "DEFAULT_HISTORY_SCHEMA",
    "DEFAULT_HISTORY_SCHEMA_COMMENT",
    "SHARED_SCHEMA_VERSION",
    "build_metadata",
]
