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
    UniqueConstraint,
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
SHARED_SCHEMA_VERSION = 7


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
        Column(
            "db_profile",
            String(120),
            comment=_desc("documentation_pages", "db_profile"),
        ),
        Column(
            "hostname",
            String(255),
            comment=_desc("documentation_pages", "hostname"),
        ),
        Column(
            "client_version",
            String(40),
            comment=_desc("documentation_pages", "client_version"),
        ),
        Column(
            "local_id",
            BigInteger,
            comment=_desc("documentation_pages", "local_id"),
        ),
        Column(
            "version",
            Integer,
            nullable=False,
            default=1,
            server_default="1",
            comment=_desc("documentation_pages", "version"),
        ),
        Index("ix_documentation_pages_status", "status"),
        Index("ix_documentation_pages_updated_at", "updated_at"),
        Index("ix_documentation_pages_db_profile", "db_profile"),
        Index("ix_documentation_pages_local_lookup", "hostname", "local_id"),
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

    # ── lineage_artifacts: saved lineage diagrams shared across the team ─────
    Table(
        "lineage_artifacts",
        md,
        Column("id", String(36), primary_key=True, comment=_desc("lineage_artifacts", "id")),
        Column("name", String(512), nullable=False, comment=_desc("lineage_artifacts", "name")),
        Column(
            "db_profile",
            String(120),
            nullable=False,
            comment=_desc("lineage_artifacts", "db_profile"),
        ),
        Column(
            "anchor_entity_ref",
            String(1024),
            nullable=False,
            comment=_desc("lineage_artifacts", "anchor_entity_ref"),
        ),
        Column("depth_up", Integer, comment=_desc("lineage_artifacts", "depth_up")),
        Column("depth_down", Integer, comment=_desc("lineage_artifacts", "depth_down")),
        Column("format", String(20), comment=_desc("lineage_artifacts", "format")),
        Column("output_path", Text, comment=_desc("lineage_artifacts", "output_path")),
        Column("edge_set_hash", String(64), comment=_desc("lineage_artifacts", "edge_set_hash")),
        Column("node_count", Integer, comment=_desc("lineage_artifacts", "node_count")),
        Column("edge_count", Integer, comment=_desc("lineage_artifacts", "edge_count")),
        Column(
            "generated_at",
            DateTime(timezone=True),
            comment=_desc("lineage_artifacts", "generated_at"),
        ),
        Column(
            "extractors_used",
            _portable_json(),
            comment=_desc("lineage_artifacts", "extractors_used"),
        ),
        Column(
            "extractors_partial", Integer, comment=_desc("lineage_artifacts", "extractors_partial")
        ),
        Column("canvas_meta", _portable_json(), comment=_desc("lineage_artifacts", "canvas_meta")),
        Column(
            "created_by",
            String(120),
            nullable=False,
            comment=_desc("lineage_artifacts", "created_by"),
        ),
        Column(
            "hostname", String(255), nullable=False, comment=_desc("lineage_artifacts", "hostname")
        ),
        Column(
            "client_version",
            String(40),
            nullable=False,
            comment=_desc("lineage_artifacts", "client_version"),
        ),
        Column(
            "created_at",
            DateTime(timezone=True),
            nullable=False,
            comment=_desc("lineage_artifacts", "created_at"),
        ),
        Column(
            "updated_at",
            DateTime(timezone=True),
            nullable=False,
            comment=_desc("lineage_artifacts", "updated_at"),
        ),
        Column(
            "local_id", BigInteger, nullable=False, comment=_desc("lineage_artifacts", "local_id")
        ),
        Column(
            "version",
            Integer,
            nullable=False,
            default=1,
            server_default="1",
            comment=_desc("lineage_artifacts", "version"),
        ),
        Index("ix_lineage_artifacts_db_profile", "db_profile"),
        Index("ix_lineage_artifacts_local_lookup", "hostname", "local_id"),
        Index("ix_lineage_artifacts_name_profile", "name", "db_profile", unique=True),
        comment=_desc("lineage_artifacts"),
    )

    # ── lineage_artifact_nodes: per-entity canvas placement ──────────────────
    Table(
        "lineage_artifact_nodes",
        md,
        Column("id", String(36), primary_key=True, comment=_desc("lineage_artifact_nodes", "id")),
        Column(
            "artifact_id",
            String(36),
            ForeignKey(f"{schema}.lineage_artifacts.id"),
            nullable=False,
            comment=_desc("lineage_artifact_nodes", "artifact_id"),
        ),
        Column(
            "entity_ref",
            String(1024),
            nullable=False,
            comment=_desc("lineage_artifact_nodes", "entity_ref"),
        ),
        Column(
            "entity_kind",
            String(40),
            nullable=False,
            comment=_desc("lineage_artifact_nodes", "entity_kind"),
        ),
        Column(
            "db_profile",
            String(120),
            nullable=False,
            comment=_desc("lineage_artifact_nodes", "db_profile"),
        ),
        Column("x", Float, comment=_desc("lineage_artifact_nodes", "x")),
        Column("y", Float, comment=_desc("lineage_artifact_nodes", "y")),
        Column("width", Float, comment=_desc("lineage_artifact_nodes", "width")),
        Column("height", Float, comment=_desc("lineage_artifact_nodes", "height")),
        Column("z_index", Integer, comment=_desc("lineage_artifact_nodes", "z_index")),
        Column(
            "display_label", String(512), comment=_desc("lineage_artifact_nodes", "display_label")
        ),
        Column(
            "column_list_json",
            _portable_json(),
            comment=_desc("lineage_artifact_nodes", "column_list_json"),
        ),
        Column("logo_key", String(120), comment=_desc("lineage_artifact_nodes", "logo_key")),
        Column(
            "custom_style_json",
            _portable_json(),
            comment=_desc("lineage_artifact_nodes", "custom_style_json"),
        ),
        Column(
            "created_by",
            String(255),
            nullable=False,
            comment=_desc("lineage_artifact_nodes", "created_by"),
        ),
        Column(
            "hostname",
            String(255),
            nullable=False,
            comment=_desc("lineage_artifact_nodes", "hostname"),
        ),
        Column(
            "client_version",
            String(40),
            nullable=False,
            comment=_desc("lineage_artifact_nodes", "client_version"),
        ),
        Column(
            "created_at",
            DateTime(timezone=True),
            nullable=False,
            comment=_desc("lineage_artifact_nodes", "created_at"),
        ),
        Column(
            "updated_at",
            DateTime(timezone=True),
            nullable=False,
            comment=_desc("lineage_artifact_nodes", "updated_at"),
        ),
        Column(
            "local_id",
            BigInteger,
            nullable=False,
            comment=_desc("lineage_artifact_nodes", "local_id"),
        ),
        Column(
            "version",
            Integer,
            nullable=False,
            default=1,
            server_default="1",
            comment=_desc("lineage_artifact_nodes", "version"),
        ),
        Index("ix_lineage_nodes_artifact", "artifact_id"),
        Index("ix_lineage_nodes_entity_profile", "entity_ref", "db_profile"),
        Index("ix_lineage_nodes_local_lookup", "hostname", "local_id"),
        comment=_desc("lineage_artifact_nodes"),
    )

    # ── lineage_artifact_edges: relations between canvas nodes ───────────────
    Table(
        "lineage_artifact_edges",
        md,
        Column("id", String(36), primary_key=True, comment=_desc("lineage_artifact_edges", "id")),
        Column(
            "artifact_id",
            String(36),
            ForeignKey(f"{schema}.lineage_artifacts.id"),
            nullable=False,
            comment=_desc("lineage_artifact_edges", "artifact_id"),
        ),
        Column(
            "source_node_id",
            String(36),
            ForeignKey(f"{schema}.lineage_artifact_nodes.id"),
            nullable=False,
            comment=_desc("lineage_artifact_edges", "source_node_id"),
        ),
        Column(
            "target_node_id",
            String(36),
            ForeignKey(f"{schema}.lineage_artifact_nodes.id"),
            nullable=False,
            comment=_desc("lineage_artifact_edges", "target_node_id"),
        ),
        Column(
            "edge_kind",
            String(40),
            nullable=False,
            comment=_desc("lineage_artifact_edges", "edge_kind"),
        ),
        Column("join_type", String(20), comment=_desc("lineage_artifact_edges", "join_type")),
        Column("on_condition", Text, comment=_desc("lineage_artifact_edges", "on_condition")),
        Column("where_clause", Text, comment=_desc("lineage_artifact_edges", "where_clause")),
        Column(
            "source_columns_json",
            _portable_json(),
            comment=_desc("lineage_artifact_edges", "source_columns_json"),
        ),
        Column(
            "target_columns_json",
            _portable_json(),
            comment=_desc("lineage_artifact_edges", "target_columns_json"),
        ),
        Column("label", String(512), comment=_desc("lineage_artifact_edges", "label")),
        Column(
            "style_json", _portable_json(), comment=_desc("lineage_artifact_edges", "style_json")
        ),
        Column(
            "waypoints_json",
            _portable_json(),
            comment=_desc("lineage_artifact_edges", "waypoints_json"),
        ),
        Column(
            "created_by",
            String(255),
            nullable=False,
            comment=_desc("lineage_artifact_edges", "created_by"),
        ),
        Column(
            "hostname",
            String(255),
            nullable=False,
            comment=_desc("lineage_artifact_edges", "hostname"),
        ),
        Column(
            "client_version",
            String(40),
            nullable=False,
            comment=_desc("lineage_artifact_edges", "client_version"),
        ),
        Column(
            "created_at",
            DateTime(timezone=True),
            nullable=False,
            comment=_desc("lineage_artifact_edges", "created_at"),
        ),
        Column(
            "updated_at",
            DateTime(timezone=True),
            nullable=False,
            comment=_desc("lineage_artifact_edges", "updated_at"),
        ),
        Column(
            "local_id",
            BigInteger,
            nullable=False,
            comment=_desc("lineage_artifact_edges", "local_id"),
        ),
        Column(
            "version",
            Integer,
            nullable=False,
            default=1,
            server_default="1",
            comment=_desc("lineage_artifact_edges", "version"),
        ),
        Index("ix_lineage_edges_artifact", "artifact_id"),
        Index("ix_lineage_edges_source", "source_node_id"),
        Index("ix_lineage_edges_target", "target_node_id"),
        Index("ix_lineage_edges_local_lookup", "hostname", "local_id"),
        comment=_desc("lineage_artifact_edges"),
    )

    # ── lineage_comments: sticky-note annotations on lineage canvases ────────
    Table(
        "lineage_comments",
        md,
        Column("id", String(36), primary_key=True, comment=_desc("lineage_comments", "id")),
        Column(
            "artifact_id",
            String(36),
            ForeignKey(f"{schema}.lineage_artifacts.id"),
            nullable=False,
            comment=_desc("lineage_comments", "artifact_id"),
        ),
        Column("x", Float, comment=_desc("lineage_comments", "x")),
        Column("y", Float, comment=_desc("lineage_comments", "y")),
        Column("width", Float, comment=_desc("lineage_comments", "width")),
        Column("height", Float, comment=_desc("lineage_comments", "height")),
        Column("color", String(40), comment=_desc("lineage_comments", "color")),
        Column(
            "style", String(20), server_default="note", comment=_desc("lineage_comments", "style")
        ),
        Column("text", Text, comment=_desc("lineage_comments", "text")),
        Column(
            "created_by",
            String(255),
            nullable=False,
            comment=_desc("lineage_comments", "created_by"),
        ),
        Column(
            "hostname", String(255), nullable=False, comment=_desc("lineage_comments", "hostname")
        ),
        Column(
            "client_version",
            String(40),
            nullable=False,
            comment=_desc("lineage_comments", "client_version"),
        ),
        Column(
            "created_at",
            DateTime(timezone=True),
            nullable=False,
            comment=_desc("lineage_comments", "created_at"),
        ),
        Column(
            "updated_at",
            DateTime(timezone=True),
            nullable=False,
            comment=_desc("lineage_comments", "updated_at"),
        ),
        Column(
            "local_id", BigInteger, nullable=False, comment=_desc("lineage_comments", "local_id")
        ),
        Column(
            "version",
            Integer,
            nullable=False,
            default=1,
            server_default="1",
            comment=_desc("lineage_comments", "version"),
        ),
        Index("ix_lineage_comments_artifact", "artifact_id"),
        Index("ix_lineage_comments_local_lookup", "hostname", "local_id"),
        comment=_desc("lineage_comments"),
    )

    # ── _amx_users: workspace member registry ────────────────────────────────
    fq_users = f"{schema or DEFAULT_HISTORY_SCHEMA}._amx_users"
    Table(
        "_amx_users",
        md,
        Column("id", String(36), primary_key=True, comment=_desc("_amx_users", "id")),
        Column(
            "username",
            String(255),
            nullable=False,
            comment=_desc("_amx_users", "username"),
        ),
        Column(
            "hostname",
            String(255),
            nullable=False,
            comment=_desc("_amx_users", "hostname"),
        ),
        Column(
            "display_name",
            String(512),
            comment=_desc("_amx_users", "display_name"),
        ),
        Column("email", String(255), comment=_desc("_amx_users", "email")),
        Column(
            "role",
            String(20),
            nullable=False,
            comment=_desc("_amx_users", "role"),
        ),
        Column(
            "first_seen_at",
            DateTime(timezone=True),
            nullable=False,
            comment=_desc("_amx_users", "first_seen_at"),
        ),
        Column(
            "last_seen_at",
            DateTime(timezone=True),
            nullable=False,
            comment=_desc("_amx_users", "last_seen_at"),
        ),
        Column(
            "client_version",
            String(40),
            comment=_desc("_amx_users", "client_version"),
        ),
        Column(
            "created_by",
            String(36),
            comment=_desc("_amx_users", "created_by"),
        ),
        Column(
            "revoked_at",
            DateTime(timezone=True),
            comment=_desc("_amx_users", "revoked_at"),
        ),
        Column(
            "revoked_by",
            String(36),
            comment=_desc("_amx_users", "revoked_by"),
        ),
        Index("uq_amx_users_username_hostname", "username", "hostname", unique=True),
        Index("ix_amx_users_role", "role"),
        comment=_desc("_amx_users"),
    )

    # ── _amx_admin_audit: permission + sensitive-action log ──────────────────
    Table(
        "_amx_admin_audit",
        md,
        Column("id", String(36), primary_key=True, comment=_desc("_amx_admin_audit", "id")),
        Column(
            "event_at",
            DateTime(timezone=True),
            nullable=False,
            comment=_desc("_amx_admin_audit", "event_at"),
        ),
        Column(
            "actor_user_id",
            String(36),
            ForeignKey(fq_users + ".id"),
            comment=_desc("_amx_admin_audit", "actor_user_id"),
        ),
        Column(
            "actor_username",
            String(255),
            comment=_desc("_amx_admin_audit", "actor_username"),
        ),
        Column(
            "actor_hostname",
            String(255),
            comment=_desc("_amx_admin_audit", "actor_hostname"),
        ),
        Column(
            "action",
            String(40),
            nullable=False,
            comment=_desc("_amx_admin_audit", "action"),
        ),
        Column(
            "target_user_id",
            String(36),
            ForeignKey(fq_users + ".id"),
            comment=_desc("_amx_admin_audit", "target_user_id"),
        ),
        Column(
            "target_resource",
            String(1024),
            comment=_desc("_amx_admin_audit", "target_resource"),
        ),
        Column(
            "details_json",
            _portable_json(),
            comment=_desc("_amx_admin_audit", "details_json"),
        ),
        Index("ix_amx_admin_audit_event_at", "event_at"),
        Index("ix_amx_admin_audit_actor_user_id", "actor_user_id"),
        comment=_desc("_amx_admin_audit"),
    )

    # ── _amx_session_events: connect/disconnect log ──────────────────────────
    Table(
        "_amx_session_events",
        md,
        Column("id", String(36), primary_key=True, comment=_desc("_amx_session_events", "id")),
        Column(
            "event_at",
            DateTime(timezone=True),
            nullable=False,
            comment=_desc("_amx_session_events", "event_at"),
        ),
        Column(
            "user_id",
            String(36),
            ForeignKey(fq_users + ".id"),
            comment=_desc("_amx_session_events", "user_id"),
        ),
        Column(
            "username",
            String(255),
            comment=_desc("_amx_session_events", "username"),
        ),
        Column(
            "hostname",
            String(255),
            comment=_desc("_amx_session_events", "hostname"),
        ),
        Column(
            "event_kind",
            String(40),
            nullable=False,
            comment=_desc("_amx_session_events", "event_kind"),
        ),
        Column(
            "client_version",
            String(40),
            comment=_desc("_amx_session_events", "client_version"),
        ),
        Column(
            "os_platform",
            String(40),
            comment=_desc("_amx_session_events", "os_platform"),
        ),
        Column(
            "db_profiles_seen",
            _portable_json(),
            comment=_desc("_amx_session_events", "db_profiles_seen"),
        ),
        Index("ix_amx_session_events_event_at", "event_at"),
        Index("ix_amx_session_events_user_id", "user_id"),
        comment=_desc("_amx_session_events"),
    )

    # ── remote_notebooks: notebooks ingested from remote platforms ────────
    Table(
        "remote_notebooks",
        md,
        Column(
            "id",
            Integer,
            primary_key=True,
            autoincrement=True,
            comment=_desc("remote_notebooks", "id"),
        ),
        Column(
            "profile_name",
            String,
            nullable=False,
            comment=_desc("remote_notebooks", "profile_name"),
        ),
        Column(
            "platform",
            String,
            nullable=False,
            comment=_desc("remote_notebooks", "platform"),
        ),
        Column(
            "external_id",
            String,
            nullable=False,
            comment=_desc("remote_notebooks", "external_id"),
        ),
        Column("name", String, nullable=False, comment=_desc("remote_notebooks", "name")),
        Column(
            "workspace_path",
            String,
            comment=_desc("remote_notebooks", "workspace_path"),
        ),
        Column(
            "qualified_name",
            String,
            comment=_desc("remote_notebooks", "qualified_name"),
        ),
        Column(
            "language",
            String,
            nullable=False,
            comment=_desc("remote_notebooks", "language"),
        ),
        Column(
            "source_text",
            Text,
            nullable=False,
            comment=_desc("remote_notebooks", "source_text"),
        ),
        Column(
            "source_hash",
            String,
            nullable=False,
            comment=_desc("remote_notebooks", "source_hash"),
        ),
        Column(
            "last_modified_at",
            DateTime(timezone=True),
            comment=_desc("remote_notebooks", "last_modified_at"),
        ),
        Column(
            "last_modified_by",
            String,
            comment=_desc("remote_notebooks", "last_modified_by"),
        ),
        Column("owner", String, comment=_desc("remote_notebooks", "owner")),
        Column("cell_count", Integer, comment=_desc("remote_notebooks", "cell_count")),
        Column(
            "ingested_at",
            DateTime(timezone=True),
            nullable=False,
            comment=_desc("remote_notebooks", "ingested_at"),
        ),
        UniqueConstraint(
            "profile_name",
            "platform",
            "external_id",
            name="uq_remote_notebooks_profile_platform_ext",
        ),
        Index("idx_remote_notebooks_profile_platform", "profile_name", "platform"),
        comment=_desc("remote_notebooks"),
    )

    # ── remote_jobs: Databricks jobs/workflows ────────────────────────────
    Table(
        "remote_jobs",
        md,
        Column(
            "id",
            Integer,
            primary_key=True,
            autoincrement=True,
            comment=_desc("remote_jobs", "id"),
        ),
        Column(
            "profile_name",
            String,
            nullable=False,
            comment=_desc("remote_jobs", "profile_name"),
        ),
        Column(
            "job_id",
            Integer,
            nullable=False,
            comment=_desc("remote_jobs", "job_id"),
        ),
        Column("name", String, nullable=False, comment=_desc("remote_jobs", "name")),
        Column(
            "creator_user_name",
            String,
            comment=_desc("remote_jobs", "creator_user_name"),
        ),
        Column("schedule_cron", String, comment=_desc("remote_jobs", "schedule_cron")),
        Column(
            "schedule_timezone",
            String,
            comment=_desc("remote_jobs", "schedule_timezone"),
        ),
        Column(
            "schedule_pause_status",
            String,
            comment=_desc("remote_jobs", "schedule_pause_status"),
        ),
        Column(
            "max_concurrent_runs",
            Integer,
            comment=_desc("remote_jobs", "max_concurrent_runs"),
        ),
        Column(
            "email_notifications_json",
            String,
            comment=_desc("remote_jobs", "email_notifications_json"),
        ),
        Column("tags_json", String, comment=_desc("remote_jobs", "tags_json")),
        Column(
            "last_run_status",
            String,
            comment=_desc("remote_jobs", "last_run_status"),
        ),
        Column(
            "last_run_started_at",
            DateTime(timezone=True),
            comment=_desc("remote_jobs", "last_run_started_at"),
        ),
        Column(
            "success_rate_30d",
            Float,
            comment=_desc("remote_jobs", "success_rate_30d"),
        ),
        Column(
            "ingested_at",
            DateTime(timezone=True),
            nullable=False,
            comment=_desc("remote_jobs", "ingested_at"),
        ),
        UniqueConstraint("profile_name", "job_id", name="uq_remote_jobs_profile_job"),
        Index("idx_remote_jobs_profile", "profile_name"),
        comment=_desc("remote_jobs"),
    )

    # ── remote_job_tasks: per-task definitions within a job ───────────────
    Table(
        "remote_job_tasks",
        md,
        Column(
            "id",
            Integer,
            primary_key=True,
            autoincrement=True,
            comment=_desc("remote_job_tasks", "id"),
        ),
        Column(
            "job_id_fk",
            Integer,
            ForeignKey("remote_jobs.id"),
            nullable=False,
            comment=_desc("remote_job_tasks", "job_id_fk"),
        ),
        Column(
            "task_key",
            String,
            nullable=False,
            comment=_desc("remote_job_tasks", "task_key"),
        ),
        Column(
            "task_type",
            String,
            nullable=False,
            comment=_desc("remote_job_tasks", "task_type"),
        ),
        Column(
            "notebook_path",
            String,
            comment=_desc("remote_job_tasks", "notebook_path"),
        ),
        Column(
            "notebook_id_fk",
            Integer,
            ForeignKey("remote_notebooks.id"),
            comment=_desc("remote_job_tasks", "notebook_id_fk"),
        ),
        Column(
            "sql_query_id",
            String,
            comment=_desc("remote_job_tasks", "sql_query_id"),
        ),
        Column(
            "sql_warehouse_id",
            String,
            comment=_desc("remote_job_tasks", "sql_warehouse_id"),
        ),
        Column(
            "pipeline_id_fk",
            Integer,
            comment=_desc("remote_job_tasks", "pipeline_id_fk"),
        ),
        Column(
            "depends_on_json",
            String,
            comment=_desc("remote_job_tasks", "depends_on_json"),
        ),
        Column(
            "raw_definition_json",
            Text,
            nullable=False,
            comment=_desc("remote_job_tasks", "raw_definition_json"),
        ),
        UniqueConstraint("job_id_fk", "task_key", name="uq_remote_job_tasks_job_key"),
        Index("idx_remote_job_tasks_job", "job_id_fk"),
        comment=_desc("remote_job_tasks"),
    )

    # ── remote_job_runs: recent run history for each job ─────────────────
    Table(
        "remote_job_runs",
        md,
        Column(
            "id",
            Integer,
            primary_key=True,
            autoincrement=True,
            comment=_desc("remote_job_runs", "id"),
        ),
        Column(
            "job_id_fk",
            Integer,
            ForeignKey("remote_jobs.id"),
            nullable=False,
            comment=_desc("remote_job_runs", "job_id_fk"),
        ),
        Column(
            "run_id",
            Integer,
            nullable=False,
            comment=_desc("remote_job_runs", "run_id"),
        ),
        Column(
            "state_result",
            String,
            nullable=False,
            comment=_desc("remote_job_runs", "state_result"),
        ),
        Column(
            "start_time",
            DateTime(timezone=True),
            nullable=False,
            comment=_desc("remote_job_runs", "start_time"),
        ),
        Column(
            "end_time",
            DateTime(timezone=True),
            comment=_desc("remote_job_runs", "end_time"),
        ),
        Column(
            "setup_duration_ms",
            Integer,
            comment=_desc("remote_job_runs", "setup_duration_ms"),
        ),
        Column(
            "execution_duration_ms",
            Integer,
            comment=_desc("remote_job_runs", "execution_duration_ms"),
        ),
        UniqueConstraint("job_id_fk", "run_id", name="uq_remote_job_runs_job_run"),
        Index("idx_remote_job_runs_job", "job_id_fk"),
        comment=_desc("remote_job_runs"),
    )

    # ── remote_pipelines: Databricks DLT pipeline definitions ────────────
    Table(
        "remote_pipelines",
        md,
        Column(
            "id",
            Integer,
            primary_key=True,
            autoincrement=True,
            comment=_desc("remote_pipelines", "id"),
        ),
        Column(
            "profile_name",
            String,
            nullable=False,
            comment=_desc("remote_pipelines", "profile_name"),
        ),
        Column(
            "pipeline_id",
            String,
            nullable=False,
            comment=_desc("remote_pipelines", "pipeline_id"),
        ),
        Column(
            "name",
            String,
            nullable=False,
            comment=_desc("remote_pipelines", "name"),
        ),
        Column(
            "target_schema",
            String,
            comment=_desc("remote_pipelines", "target_schema"),
        ),
        Column(
            "edition",
            String,
            comment=_desc("remote_pipelines", "edition"),
        ),
        Column(
            "continuous",
            Integer,
            nullable=False,
            comment=_desc("remote_pipelines", "continuous"),
        ),
        Column(
            "photon",
            Integer,
            nullable=False,
            comment=_desc("remote_pipelines", "photon"),
        ),
        Column(
            "libraries_json",
            String,
            nullable=False,
            comment=_desc("remote_pipelines", "libraries_json"),
        ),
        Column(
            "latest_update_state",
            String,
            comment=_desc("remote_pipelines", "latest_update_state"),
        ),
        Column(
            "latest_update_creation_time",
            DateTime(timezone=True),
            comment=_desc("remote_pipelines", "latest_update_creation_time"),
        ),
        Column(
            "ingested_at",
            DateTime(timezone=True),
            nullable=False,
            comment=_desc("remote_pipelines", "ingested_at"),
        ),
        UniqueConstraint(
            "profile_name", "pipeline_id", name="uq_remote_pipelines_profile_pipeline"
        ),
        Index("idx_remote_pipelines_profile", "profile_name"),
        comment=_desc("remote_pipelines"),
    )

    # ── remote_streamlit_apps: Snowflake STREAMLIT objects ───────────────
    Table(
        "remote_streamlit_apps",
        md,
        Column(
            "id",
            Integer,
            primary_key=True,
            autoincrement=True,
            comment=_desc("remote_streamlit_apps", "id"),
        ),
        Column(
            "profile_name",
            String,
            nullable=False,
            comment=_desc("remote_streamlit_apps", "profile_name"),
        ),
        Column(
            "qualified_name",
            String,
            nullable=False,
            comment=_desc("remote_streamlit_apps", "qualified_name"),
        ),
        Column(
            "main_file",
            String,
            nullable=False,
            comment=_desc("remote_streamlit_apps", "main_file"),
        ),
        Column(
            "query_warehouse",
            String,
            comment=_desc("remote_streamlit_apps", "query_warehouse"),
        ),
        Column(
            "root_location",
            String,
            nullable=False,
            comment=_desc("remote_streamlit_apps", "root_location"),
        ),
        Column(
            "owner",
            String,
            comment=_desc("remote_streamlit_apps", "owner"),
        ),
        Column(
            "last_altered_at",
            DateTime(timezone=True),
            comment=_desc("remote_streamlit_apps", "last_altered_at"),
        ),
        Column(
            "ingested_at",
            DateTime(timezone=True),
            nullable=False,
            comment=_desc("remote_streamlit_apps", "ingested_at"),
        ),
        UniqueConstraint(
            "profile_name", "qualified_name", name="uq_remote_streamlit_apps_profile_name"
        ),
        Index("idx_remote_streamlit_apps_profile", "profile_name"),
        comment=_desc("remote_streamlit_apps"),
    )

    # ── remote_streams: Snowflake CDC streams ────────────────────────────
    Table(
        "remote_streams",
        md,
        Column(
            "id",
            Integer,
            primary_key=True,
            autoincrement=True,
            comment=_desc("remote_streams", "id"),
        ),
        Column(
            "profile_name",
            String,
            nullable=False,
            comment=_desc("remote_streams", "profile_name"),
        ),
        Column(
            "qualified_name",
            String,
            nullable=False,
            comment=_desc("remote_streams", "qualified_name"),
        ),
        Column(
            "source_table_fqn",
            String,
            nullable=False,
            comment=_desc("remote_streams", "source_table_fqn"),
        ),
        Column(
            "source_entity_id",
            Integer,
            comment=_desc("remote_streams", "source_entity_id"),
        ),
        Column(
            "mode",
            String,
            nullable=False,
            comment=_desc("remote_streams", "mode"),
        ),
        Column(
            "stale_after",
            DateTime(timezone=True),
            comment=_desc("remote_streams", "stale_after"),
        ),
        Column(
            "owner",
            String,
            comment=_desc("remote_streams", "owner"),
        ),
        Column(
            "ingested_at",
            DateTime(timezone=True),
            nullable=False,
            comment=_desc("remote_streams", "ingested_at"),
        ),
        UniqueConstraint("profile_name", "qualified_name", name="uq_remote_streams_profile_name"),
        Index("idx_remote_streams_profile", "profile_name"),
        comment=_desc("remote_streams"),
    )

    # ── remote_task_dependencies: Snowflake task DAG edges ───────────────
    Table(
        "remote_task_dependencies",
        md,
        Column(
            "profile_name",
            String,
            nullable=False,
            comment=_desc("remote_task_dependencies", "profile_name"),
        ),
        Column(
            "parent_task_fqn",
            String,
            nullable=False,
            comment=_desc("remote_task_dependencies", "parent_task_fqn"),
        ),
        Column(
            "child_task_fqn",
            String,
            nullable=False,
            comment=_desc("remote_task_dependencies", "child_task_fqn"),
        ),
        PrimaryKeyConstraint(
            "profile_name",
            "parent_task_fqn",
            "child_task_fqn",
            name="pk_remote_task_dependencies",
        ),
        comment=_desc("remote_task_dependencies"),
    )

    # ── remote_queries: saved queries and execution history ──────────────
    Table(
        "remote_queries",
        md,
        Column(
            "id",
            Integer,
            primary_key=True,
            autoincrement=True,
            comment=_desc("remote_queries", "id"),
        ),
        Column(
            "profile_name",
            String,
            nullable=False,
            comment=_desc("remote_queries", "profile_name"),
        ),
        Column(
            "platform",
            String,
            nullable=False,
            comment=_desc("remote_queries", "platform"),
        ),
        Column(
            "kind",
            String,
            nullable=False,
            comment=_desc("remote_queries", "kind"),
        ),
        Column(
            "external_id",
            String,
            nullable=False,
            comment=_desc("remote_queries", "external_id"),
        ),
        Column(
            "name",
            String,
            comment=_desc("remote_queries", "name"),
        ),
        Column(
            "sql_text",
            Text,
            nullable=False,
            comment=_desc("remote_queries", "sql_text"),
        ),
        Column(
            "sql_hash",
            String,
            nullable=False,
            comment=_desc("remote_queries", "sql_hash"),
        ),
        Column(
            "warehouse",
            String,
            comment=_desc("remote_queries", "warehouse"),
        ),
        Column(
            "user_name",
            String,
            comment=_desc("remote_queries", "user_name"),
        ),
        Column(
            "executed_at",
            DateTime(timezone=True),
            comment=_desc("remote_queries", "executed_at"),
        ),
        Column(
            "duration_ms",
            Integer,
            comment=_desc("remote_queries", "duration_ms"),
        ),
        Column(
            "ingested_at",
            DateTime(timezone=True),
            nullable=False,
            comment=_desc("remote_queries", "ingested_at"),
        ),
        UniqueConstraint(
            "profile_name",
            "platform",
            "kind",
            "external_id",
            name="uq_remote_queries_profile_platform_kind_id",
        ),
        Index("idx_remote_queries_profile_platform", "profile_name", "platform"),
        comment=_desc("remote_queries"),
    )

    # ── catalog_entities: shared structural table/column metadata ─────────
    # Mirrors the LOCAL catalog_entities structural columns so a deep
    # sync's expensive COUNT(*) pass runs once per team and propagates.
    # Descriptions are NOT shared here — they flow via run_results /
    # analysis_runs; only structure (columns, dtypes, row counts, key
    # flags) lives in this table. The natural key
    # (db_profile, database_name, schema_name, table_name, column_name)
    # plus last_synced_at drives last-write-wins upserts in both
    # directions (push + pull).
    Table(
        "catalog_entities",
        md,
        Column("id", String(36), primary_key=True, comment=_desc("catalog_entities", "id")),
        Column(
            "db_profile",
            String(120),
            nullable=False,
            comment=_desc("catalog_entities", "db_profile"),
        ),
        Column("db_backend", String(40), comment=_desc("catalog_entities", "db_backend")),
        Column(
            "database_name",
            String(255),
            nullable=False,
            default="",
            comment=_desc("catalog_entities", "database_name"),
        ),
        Column(
            "schema_name",
            String(255),
            nullable=False,
            comment=_desc("catalog_entities", "schema_name"),
        ),
        Column(
            "table_name",
            String(255),
            nullable=False,
            comment=_desc("catalog_entities", "table_name"),
        ),
        Column(
            "column_name",
            String(255),
            nullable=False,
            default="",
            comment=_desc("catalog_entities", "column_name"),
        ),
        Column(
            "entity_kind",
            String(20),
            nullable=False,
            comment=_desc("catalog_entities", "entity_kind"),
        ),
        Column("asset_kind", String(40), comment=_desc("catalog_entities", "asset_kind")),
        Column("dtype", String(255), comment=_desc("catalog_entities", "dtype")),
        Column("nullable", Integer, comment=_desc("catalog_entities", "nullable")),
        Column("pk_flag", Integer, comment=_desc("catalog_entities", "pk_flag")),
        Column("fk_flag", Integer, comment=_desc("catalog_entities", "fk_flag")),
        Column("row_count", BigInteger, comment=_desc("catalog_entities", "row_count")),
        Column(
            "last_synced_at",
            DateTime(timezone=True),
            index=True,
            comment=_desc("catalog_entities", "last_synced_at"),
        ),
        Column(
            "first_synced_at",
            DateTime(timezone=True),
            comment=_desc("catalog_entities", "first_synced_at"),
        ),
        Column("created_by", String(120), comment=_desc("catalog_entities", "created_by")),
        Column("hostname", String(255), comment=_desc("catalog_entities", "hostname")),
        Column("client_version", String(40), comment=_desc("catalog_entities", "client_version")),
        UniqueConstraint(
            "db_profile",
            "database_name",
            "schema_name",
            "table_name",
            "column_name",
            name="uq_catalog_entities_natural_key",
        ),
        Index(
            "ix_catalog_entities_profile_schema",
            "db_profile",
            "database_name",
            "schema_name",
        ),
        comment=_desc("catalog_entities"),
    )

    return md


__all__ = [
    "DEFAULT_HISTORY_SCHEMA",
    "DEFAULT_HISTORY_SCHEMA_COMMENT",
    "SHARED_SCHEMA_VERSION",
    "build_metadata",
]
