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
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Text,
)
from sqlalchemy.types import TypeDecorator, TypeEngine


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

# Comment text written to the schema (namespace) itself via
# ``COMMENT ON SCHEMA``. ``MetaData`` does not carry schema-level
# annotations natively, so :meth:`DatabaseAdapter.create_history_schema`
# emits this explicitly after ``CREATE SCHEMA``.
DEFAULT_HISTORY_SCHEMA_COMMENT = (
    "AMX shared run-history schema. Created by AMX (Agentic Metadata "
    "Extractor) via /history-store enable. Holds cross-machine analysis "
    "history, per-asset LLM alternatives, app events, agent session "
    "state, and a schema version stamp so multiple AMX clients can "
    "share run history under one warehouse. See "
    "https://github.com/omeryasirkucuk/amx for details."
)

# All client versions writing into a shared store record this as their
# ``schema_version`` so an older client refuses to write into a schema
# bumped by a newer client (avoids losing columns the new client added).
SHARED_SCHEMA_VERSION = 1


# ── Per-column comment text ───────────────────────────────────────────────
# Pulled out as constants so the same string isn't duplicated across
# attribution columns on every table.

_ATTRIBUTION_CREATED_BY = (
    "OS username (or AMX_USER override) of the principal that wrote this row. "
    "Populated on every shared-mode write so '/history-store list-team' can answer "
    "'who ran what?'."
)
_ATTRIBUTION_HOSTNAME = (
    "Machine that wrote this row. Part of the (hostname, local_id) provenance pair "
    "that lets the dual-write coordinator re-find the shared row when a later UPDATE "
    "(e.g. finish_run) fires from the same machine."
)
_ATTRIBUTION_CLIENT_VERSION = (
    "AMX version string (e.g. '0.12.1') of the client that wrote this row. Used by "
    "/doctor and post-mortems to correlate row shape changes with client upgrades."
)
_ATTRIBUTION_LOCAL_ID = (
    "Corresponding INT id in the writer machine's local SQLite history.db. Scoped "
    "by hostname so two machines can both have local_id=5 without collision; lets "
    "the dual-write coordinator locate the shared row for in-flight UPDATEs."
)


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
        Column(
            "id",
            String(36),
            primary_key=True,
            comment=(
                "UUID v4 primary key. Surfaced as a short prefix in CLI output "
                "(/list, /show, /results, /review, /compare). UUID rather than "
                "INT autoincrement because shared mode admits concurrent writers "
                "from multiple machines."
            ),
        ),
        Column(
            "started_at",
            DateTime(timezone=True),
            nullable=False,
            index=True,
            comment=(
                "UTC timestamp when the agent run began. Indexed for "
                "last-N-days filters in /list and /stats."
            ),
        ),
        Column(
            "ended_at",
            DateTime(timezone=True),
            comment=(
                "UTC timestamp when the run finished. NULL while a run is in "
                "flight; also NULL for runs killed mid-flight by Ctrl-C."
            ),
        ),
        Column(
            "duration_sec",
            Float,
            comment=(
                "Wall-clock seconds between started_at and ended_at. Convenience "
                "column so /stats does not have to recompute it on every read."
            ),
        ),
        Column(
            "status",
            String(40),
            nullable=False,
            comment=(
                "Lifecycle state of the run: running | completed | failed | "
                "cancelled. Drives /list filters and the colored status badge."
            ),
        ),
        Column(
            "command",
            String(80),
            nullable=False,
            comment=(
                "Top-level CLI command that triggered the run: run | run-apply | "
                "ask | doc-analyze | code-analyze. Distinguishes batch metadata "
                "generation from one-off Q&A in /stats breakdowns."
            ),
        ),
        Column(
            "mode",
            String(40),
            comment=(
                "Sub-mode chosen at the run picker: human-review | auto-apply | "
                "confidence-threshold | dry-run."
            ),
        ),
        Column(
            "db_backend",
            String(40),
            comment=(
                "Backend of the analyzed DB profile: postgresql | snowflake | "
                "bigquery | databricks | mssql | mysql | oracle | redshift. "
                "Enables /compare --by db_backend cross-backend audits."
            ),
        ),
        Column(
            "db_profile",
            String(120),
            comment=(
                "Named DB profile used for this run (see /db-profiles). Multi-"
                "profile runs (0.11+) record the first profile here and the "
                "full list in scope_json.profiles."
            ),
        ),
        Column(
            "llm_provider",
            String(40),
            comment=(
                "LLM vendor: openai | anthropic | gemini | openrouter | "
                "deepseek | ollama | …. Distinct from llm_model which records "
                "the specific model id."
            ),
        ),
        Column(
            "llm_model",
            String(120),
            comment=(
                "Specific model id served by llm_provider — e.g. 'gpt-4o', "
                "'claude-sonnet-4-20250514', 'openai/gpt-4o-mini' on OpenRouter."
            ),
        ),
        Column(
            "scope_json",
            _portable_json(),
            comment=(
                "JSON describing the analyzed scope: {schemas, tables, columns, "
                "asset_kinds, profiles}. /compare reads this to find prior runs "
                "of the same assets for side-by-side pivots."
            ),
        ),
        Column(
            "metrics_json",
            _portable_json(),
            comment=(
                "JSON of run metrics — counts, per-stage timings, retries, "
                "skipped assets. Free-form so newer agents can add fields "
                "without a schema bump."
            ),
        ),
        Column(
            "tokens_json",
            _portable_json(),
            comment=(
                "JSON of token usage broken down by phase: "
                "{prompt, completion, cached, reasoning, total}. Drives /stats "
                "cost reporting and /compare --by tokens."
            ),
        ),
        Column(
            "results_json",
            _portable_json(),
            comment=(
                "JSON summary of run outputs (counts, top-level rollups). "
                "Per-asset detail lives in run_results joined on run_id."
            ),
        ),
        Column(
            "error_text",
            Text,
            comment=("Stack trace or error message when status='failed'. NULL on successful runs."),
        ),
        Column(
            "selected_count",
            Integer,
            nullable=False,
            default=0,
            comment=(
                "Assets the user selected at the run picker. First step of the "
                "selected → planned → processed → applied funnel."
            ),
        ),
        Column(
            "planned_count",
            Integer,
            nullable=False,
            default=0,
            comment=(
                "Assets that survived post-selection filtering (already-good "
                "comments skipped, unsupported asset kinds dropped, etc.)."
            ),
        ),
        Column(
            "processed_count",
            Integer,
            nullable=False,
            default=0,
            comment=(
                "Assets the LLM successfully produced descriptions for. "
                "planned_count - processed_count = LLM/network failures."
            ),
        ),
        Column(
            "applied_count",
            Integer,
            nullable=False,
            default=0,
            comment=(
                "Assets whose chosen description was written to the live DB "
                "via COMMENT ON. processed_count - applied_count = approved-"
                "but-not-yet-applied (when running without --apply)."
            ),
        ),
        Column(
            "review_strategy",
            String(40),
            comment=(
                "How alternatives were chosen: human | auto-best | "
                "confidence-threshold. Affects how to read evaluated_at on "
                "joined run_results rows."
            ),
        ),
        Column(
            "llm_profile",
            String(120),
            comment=(
                "Named LLM profile used for this run (see /llm-profiles). "
                "Captures the user-facing handle; concrete provider/model "
                "values are mirrored in llm_provider/llm_model."
            ),
        ),
        Column(
            "doc_profile",
            String(120),
            comment=(
                "Named document profile that supplied RAG evidence (see "
                "/doc-profiles). NULL when /docs was not used in this run."
            ),
        ),
        Column(
            "code_profile",
            String(120),
            comment=(
                "Named code profile that supplied code-evidence (see "
                "/code-profiles). NULL when /code was not used in this run."
            ),
        ),
        Column(
            "settings_json",
            _portable_json(),
            comment=(
                "Snapshot of LLM settings at run time — temperature, "
                "prompt_detail, n_alternatives, llm_batch_size, "
                "description_verbosity, logprob_thresholds. Drives "
                "/compare --by settings pivots so changes can be A/B-attributed."
            ),
        ),
        Column("created_by", String(120), comment=_ATTRIBUTION_CREATED_BY),
        Column("hostname", String(255), comment=_ATTRIBUTION_HOSTNAME),
        Column("client_version", String(40), comment=_ATTRIBUTION_CLIENT_VERSION),
        Column("local_id", BigInteger, comment=_ATTRIBUTION_LOCAL_ID),
        Index("ix_analysis_runs_started_at", "started_at"),
        Index("ix_analysis_runs_local_lookup", "hostname", "local_id"),
        comment=(
            "One row per AMX analysis run (/run, /run-apply, /ask, "
            "doc-analyze, code-analyze). Captures the inputs (scope, "
            "profiles, settings) and outputs (results, metrics, errors) of "
            "an LLM-driven metadata generation. Joined to run_results for "
            "per-asset alternatives. Read by /list, /show, /stats, /compare."
        ),
    )

    # ── run_results: per-asset LLM alternatives + review state ─────────────
    Table(
        "run_results",
        md,
        Column(
            "id",
            String(36),
            primary_key=True,
            comment="UUID v4 primary key for this (asset, alternative) row.",
        ),
        Column(
            "run_id",
            String(36),
            nullable=False,
            index=True,
            comment=(
                "Foreign-key-by-convention to analysis_runs.id. Not a hard FK "
                "because shared mode admits replication lag — a result row can "
                "land before its parent run row when two writers race."
            ),
        ),
        Column(
            "saved_at",
            DateTime(timezone=True),
            nullable=False,
            comment=(
                "UTC timestamp when this alternative was persisted. Distinct "
                "from analysis_runs.started_at when the LLM streams alternatives "
                "across the run window."
            ),
        ),
        Column(
            "schema_name",
            String(255),
            nullable=False,
            comment="Schema (or dataset/database) of the asset described.",
        ),
        Column(
            "table_name",
            String(255),
            nullable=False,
            comment="Table or view name of the asset described.",
        ),
        Column(
            "column_name",
            String(255),
            comment=(
                "Column name when asset_kind='column'. NULL when asset_kind='table' "
                "(the alternative describes the table itself)."
            ),
        ),
        Column(
            "asset_kind",
            String(40),
            nullable=False,
            default="table",
            comment=(
                "What this row describes: table | view | materialized_view | "
                "column. Drives which COMMENT ON variant is emitted on apply."
            ),
        ),
        Column(
            "source",
            String(40),
            nullable=False,
            comment=(
                "Which agent produced this alternative: profile | doc | code | "
                "combined | manual. Lets /compare and /review filter by source "
                "of evidence."
            ),
        ),
        Column(
            "confidence",
            String(20),
            nullable=False,
            comment=(
                "Bucketed quality label derived from logprob_score against the "
                "/logprob-thresholds settings: high | medium | low. Used by "
                "/review filters and the human-review UI."
            ),
        ),
        Column(
            "logprob_score",
            Float,
            comment=(
                "Normalized average log-probability of the description tokens "
                "(-1..0). Higher = the LLM was more confident in the wording."
            ),
        ),
        Column(
            "raw_logprob",
            Float,
            comment="Sum of log-probabilities (unnormalized) backing logprob_score.",
        ),
        Column(
            "token_count",
            Integer,
            comment="Token length of the alternative description.",
        ),
        Column(
            "model_version",
            String(120),
            nullable=False,
            default="",
            comment=(
                "Specific model id that produced this alternative (e.g. "
                "'gpt-4o-2024-11-20'). May differ from analysis_runs.llm_model "
                "when the user switches mid-run."
            ),
        ),
        Column(
            "reasoning",
            Text,
            comment=(
                "Optional reasoning trace from a reasoning model (o-series, "
                "claude with thinking, deepseek-reasoner). NULL for normal "
                "chat models."
            ),
        ),
        Column(
            "alternatives_json",
            _portable_json(),
            nullable=False,
            comment=(
                "Ordered JSON list of alternative description strings the LLM "
                "produced for this asset. The /review picker presents these; "
                "chosen_description records which one was selected."
            ),
        ),
        Column(
            "evaluated_at",
            DateTime(timezone=True),
            comment=(
                "UTC timestamp when a human (or auto-best) picked one of the "
                "alternatives. NULL = pending review."
            ),
        ),
        Column(
            "applied_at",
            DateTime(timezone=True),
            comment=(
                "UTC timestamp when chosen_description was written to the live "
                "DB via COMMENT ON. NULL = approved but not yet applied (or "
                "never approved)."
            ),
        ),
        Column(
            "chosen_description",
            Text,
            comment=(
                "The specific alternative selected at evaluation time. Empty "
                "string when evaluation='rejected'."
            ),
        ),
        Column(
            "evaluation",
            String(40),
            comment=(
                "Outcome of human/auto review: approved | rejected | edited. "
                "'edited' means the user accepted an alternative but modified "
                "the wording before apply."
            ),
        ),
        Column(
            "catalog_status",
            String(40),
            nullable=False,
            default="",
            comment=(
                "Sync state with the /search catalog: pending | indexed | stale "
                "| skipped. Drives /search rebuild incremental updates."
            ),
        ),
        Column(
            "catalog_indexed_at",
            DateTime(timezone=True),
            comment="UTC timestamp of the last /search-catalog index for this row.",
        ),
        Column(
            "db_applied_status",
            String(40),
            nullable=False,
            default="",
            comment=(
                "Result of the COMMENT ON write to the live DB: success | "
                "skipped | failed. Empty string until apply is attempted."
            ),
        ),
        Column(
            "effective_source_kind",
            String(40),
            nullable=False,
            default="",
            comment=(
                "What actually became the column's description after evaluation: "
                "same labels as `source` plus 'manual-edit'. Distinct from "
                "`source` because a user may approve a doc-sourced alternative "
                "after a manual edit."
            ),
        ),
        Column(
            "superseded_at",
            DateTime(timezone=True),
            comment=(
                "Set when a newer run produces a better description for the same "
                "asset. Lets /history filters hide stale rows without deleting "
                "them — full audit trail preserved."
            ),
        ),
        Column(
            "rejection_reason",
            Text,
            nullable=False,
            default="",
            comment=(
                "Free-text reason captured at evaluation time when "
                "evaluation='rejected'. Surfaced by /review for retrospectives."
            ),
        ),
        Column("hostname", String(255), comment=_ATTRIBUTION_HOSTNAME),
        Column("local_id", BigInteger, comment=_ATTRIBUTION_LOCAL_ID),
        Index("ix_run_results_asset", "schema_name", "table_name", "column_name"),
        Index("ix_run_results_local_lookup", "hostname", "local_id"),
        comment=(
            "Per-asset LLM alternatives generated during an analysis_runs "
            "invocation. One row per (asset, alternative) — a column with "
            "3 alternatives produces 3 rows. Captures the alternative payload, "
            "confidence, evaluation/apply state, and a back-pointer to the run."
        ),
    )

    # ── app_events: append-only event log ─────────────────────────────────
    Table(
        "app_events",
        md,
        Column(
            "id",
            String(36),
            primary_key=True,
            comment="UUID v4 primary key for the event.",
        ),
        Column(
            "created_at",
            DateTime(timezone=True),
            nullable=False,
            index=True,
            comment="UTC timestamp the event fired. Indexed for /events recent-first ordering.",
        ),
        Column(
            "event_type",
            String(80),
            nullable=False,
            comment=(
                "Coarse event family: cli | db | llm | doc | code | error | "
                "history-store. Lets /events filter by subsystem."
            ),
        ),
        Column(
            "status",
            String(40),
            nullable=False,
            comment="Severity of the event: info | warn | error.",
        ),
        Column(
            "command",
            String(120),
            nullable=False,
            comment="CLI command (or sub-action) that was running when the event fired.",
        ),
        Column(
            "details_json",
            _portable_json(),
            comment=(
                "Free-form JSON payload — varies per event_type. Examples: "
                "{profile, backend} for db connect, {model, latency_ms} for "
                "llm calls, {error_class, traceback} for errors."
            ),
        ),
        Column("created_by", String(120), comment=_ATTRIBUTION_CREATED_BY),
        Column("hostname", String(255), comment=_ATTRIBUTION_HOSTNAME),
        Column("client_version", String(40), comment=_ATTRIBUTION_CLIENT_VERSION),
        Index("ix_app_events_created_at", "created_at"),
        comment=(
            "Append-only structured event log surfaced by /events. Records CLI "
            "lifecycle events (connection tests, syncs, doctor checks, errors) "
            "for audit and debugging. Distinct from analysis_runs which logs "
            "LLM agent invocations."
        ),
    )

    # ── session_state: namespaced key/value storage ───────────────────────
    # Used by ``StateManager`` for inter-turn agent memory. In shared
    # mode it lets the team (or the same user across machines) share
    # session checkpoints. ``hostname`` is part of the PK so two
    # machines can hold independent state under the same namespace/key.
    Table(
        "session_state",
        md,
        Column(
            "namespace",
            String(120),
            primary_key=True,
            comment=(
                "Logical grouping for related keys (e.g. 'ask_session_42' or "
                "'review_state'). Lets multiple StateManager instances share a "
                "table without colliding."
            ),
        ),
        Column(
            "key_name",
            String(255),
            primary_key=True,
            comment="Key within the namespace.",
        ),
        Column(
            "hostname",
            String(255),
            primary_key=True,
            default="",
            comment=(
                "Writer machine. Part of the composite PK so a teammate's state "
                "under the same (namespace, key_name) does not clobber yours in "
                "shared mode. Empty string when running in single-user mode."
            ),
        ),
        Column(
            "value_json",
            _portable_json(),
            nullable=False,
            comment="JSON-serialized value associated with (namespace, key_name, hostname).",
        ),
        Column(
            "updated_at",
            DateTime(timezone=True),
            nullable=False,
            comment="UTC timestamp of the last write to this row.",
        ),
        Column("created_by", String(120), comment=_ATTRIBUTION_CREATED_BY),
        comment=(
            "Namespaced key/value storage used by StateManager for inter-turn "
            "agent memory within /ask conversational sessions. Composite primary "
            "key (namespace, key_name, hostname) so a teammate's state under "
            "the same namespace does not clobber yours in shared mode."
        ),
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
        Column(
            "id",
            Integer,
            primary_key=True,
            comment=(
                "Singleton sentinel — always 1. The PK exists only because every "
                "table needs one; this table holds at most one row."
            ),
        ),
        Column(
            "schema_version",
            Integer,
            nullable=False,
            comment=(
                "Current version of the AMX shared-store schema. Older clients "
                "refuse to write when this is higher than what they were built "
                "against, mirroring the AMXConfig schema_version compatibility "
                "guard."
            ),
        ),
        Column(
            "created_at",
            DateTime(timezone=True),
            nullable=False,
            comment="UTC timestamp when /history-store enable first bootstrapped this schema.",
        ),
        Column(
            "created_by_client_version",
            String(40),
            comment="AMX version string of the client that ran the bootstrap.",
        ),
        comment=(
            "Single-row version stamp written at /history-store enable bootstrap. "
            "Newer AMX clients bump schema_version on first write; older clients "
            "refuse to write into a schema bumped beyond what they know about, "
            "mirroring the AMXConfig schema_version guard."
        ),
    )

    return md


__all__ = [
    "DEFAULT_HISTORY_SCHEMA",
    "DEFAULT_HISTORY_SCHEMA_COMMENT",
    "SHARED_SCHEMA_VERSION",
    "build_metadata",
]
