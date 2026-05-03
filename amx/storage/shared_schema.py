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
"""

from __future__ import annotations

from sqlalchemy import (
    JSON,
    BigInteger,
    Column,
    DateTime,
    Float,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Text,
)

# Schema name is configurable per-deployment — the user can pick a
# different name in ``/history-store enable``. Default is ``AMX`` which
# matches the user-facing nomenclature in docs/CLI prompts.
DEFAULT_HISTORY_SCHEMA = "AMX"

# All client versions writing into a shared store record this as their
# ``schema_version`` so an older client refuses to write into a schema
# bumped by a newer client (avoids losing columns the new client added).
SHARED_SCHEMA_VERSION = 1


def build_metadata(schema: str | None = None) -> MetaData:
    """Build a fresh ``MetaData`` bound to *schema*.

    A function (rather than a module-level singleton) is used because
    different deployments may pick different schema names, and SQLAlchemy
    bakes the schema into each ``Table`` at construction time. Tests and
    the bootstrap path each get their own ``MetaData`` for isolation.
    """
    md = MetaData(schema=schema or DEFAULT_HISTORY_SCHEMA)

    # ── analysis_runs: one row per /run or /ask invocation ────────────────
    Table(
        "analysis_runs",
        md,
        Column("id", String(36), primary_key=True),  # UUID
        Column("started_at", DateTime(timezone=True), nullable=False, index=True),
        Column("ended_at", DateTime(timezone=True)),
        Column("duration_sec", Float),
        Column("status", String(40), nullable=False),
        Column("command", String(80), nullable=False),
        Column("mode", String(40)),
        Column("db_backend", String(40)),
        Column("db_profile", String(120)),
        Column("llm_provider", String(40)),
        Column("llm_model", String(120)),
        Column("scope_json", JSON),
        Column("metrics_json", JSON),
        Column("tokens_json", JSON),
        Column("results_json", JSON),
        Column("error_text", Text),
        Column("selected_count", Integer, nullable=False, default=0),
        Column("planned_count", Integer, nullable=False, default=0),
        Column("processed_count", Integer, nullable=False, default=0),
        Column("applied_count", Integer, nullable=False, default=0),
        Column("review_strategy", String(40)),
        Column("llm_profile", String(120)),
        Column("doc_profile", String(120)),
        Column("code_profile", String(120)),
        Column("settings_json", JSON),
        # Attribution — only meaningful in shared mode.
        Column("created_by", String(120)),
        Column("hostname", String(255)),
        Column("client_version", String(40)),
        # Provenance back to the local row, scoped by hostname so two
        # machines can both have ``local_id=5`` without collision.
        Column("local_id", BigInteger),
        Index("ix_analysis_runs_started_at", "started_at"),
        Index("ix_analysis_runs_local_lookup", "hostname", "local_id"),
    )

    # ── run_results: per-asset LLM alternatives + review state ─────────────
    Table(
        "run_results",
        md,
        Column("id", String(36), primary_key=True),
        Column("run_id", String(36), nullable=False, index=True),
        Column("saved_at", DateTime(timezone=True), nullable=False),
        Column("schema_name", String(255), nullable=False),
        Column("table_name", String(255), nullable=False),
        Column("column_name", String(255)),
        Column("asset_kind", String(40), nullable=False, default="table"),
        Column("source", String(40), nullable=False),
        Column("confidence", String(20), nullable=False),
        Column("logprob_score", Float),
        Column("raw_logprob", Float),
        Column("token_count", Integer),
        Column("model_version", String(120), nullable=False, default=""),
        Column("reasoning", Text),
        Column("alternatives_json", JSON, nullable=False),
        Column("evaluated_at", DateTime(timezone=True)),
        Column("applied_at", DateTime(timezone=True)),
        Column("chosen_description", Text),
        Column("evaluation", String(40)),
        Column("catalog_status", String(40), nullable=False, default=""),
        Column("catalog_indexed_at", DateTime(timezone=True)),
        Column("db_applied_status", String(40), nullable=False, default=""),
        Column("effective_source_kind", String(40), nullable=False, default=""),
        Column("superseded_at", DateTime(timezone=True)),
        Column("rejection_reason", Text, nullable=False, default=""),
        Column("hostname", String(255)),
        Column("local_id", BigInteger),
        Index("ix_run_results_asset", "schema_name", "table_name", "column_name"),
        Index("ix_run_results_local_lookup", "hostname", "local_id"),
    )

    # ── app_events: append-only event log ─────────────────────────────────
    Table(
        "app_events",
        md,
        Column("id", String(36), primary_key=True),
        Column("created_at", DateTime(timezone=True), nullable=False, index=True),
        Column("event_type", String(80), nullable=False),
        Column("status", String(40), nullable=False),
        Column("command", String(120), nullable=False),
        Column("details_json", JSON),
        Column("created_by", String(120)),
        Column("hostname", String(255)),
        Column("client_version", String(40)),
        Index("ix_app_events_created_at", "created_at"),
    )

    # ── session_state: namespaced key/value storage ───────────────────────
    # Used by ``StateManager`` for inter-turn agent memory. In shared
    # mode it lets the team (or the same user across machines) share
    # session checkpoints. ``hostname`` is part of the PK so two
    # machines can hold independent state under the same namespace/key.
    Table(
        "session_state",
        md,
        Column("namespace", String(120), primary_key=True),
        Column("key_name", String(255), primary_key=True),
        Column("hostname", String(255), primary_key=True, default=""),
        Column("value_json", JSON, nullable=False),
        Column("updated_at", DateTime(timezone=True), nullable=False),
        Column("created_by", String(120)),
    )

    # ── schema_meta: version stamp so older clients refuse to write ──────
    # Single-row table. The /history-store enable bootstrap inserts
    # version=SHARED_SCHEMA_VERSION; a newer client bumps it on its
    # first write; an older client refuses to write when it sees a
    # higher version than it knows about (mirrors the AMXConfig
    # schema_version guard).
    Table(
        "schema_meta",
        md,
        Column("id", Integer, primary_key=True),  # always 1
        Column("schema_version", Integer, nullable=False),
        Column("created_at", DateTime(timezone=True), nullable=False),
        Column("created_by_client_version", String(40)),
    )

    return md


__all__ = [
    "DEFAULT_HISTORY_SCHEMA",
    "SHARED_SCHEMA_VERSION",
    "build_metadata",
]
