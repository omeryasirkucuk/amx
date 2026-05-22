"""SQLite-backed local history store for AMX runs."""

from __future__ import annotations

import contextlib
import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

from amx.storage.schema_descriptions import (
    LOCAL_DATABASE_DESCRIPTION,
    SCHEMA_DESCRIPTIONS,
)
from amx.utils.logging import get_logger

log = get_logger("storage.sqlite")

# Sidecar table that holds the description of every table and column in the
# local history DB. SQLite has no native ``COMMENT ON``; this table is the
# queryable equivalent of ``pg_description``. Populated idempotently at the
# end of :meth:`SQLiteHistoryStore.init` from
# :data:`amx.storage.schema_descriptions.SCHEMA_DESCRIPTIONS` so descriptions
# stay in lock-step with the schema across upgrades.
_SCHEMA_DESCRIPTIONS_TABLE = "_amx_schema_descriptions"


def parse_alternatives_json(raw):
    """Parse ``run_results.alternatives_json`` into a normalised list of dicts.

    Three on-disk shapes are accepted:

    1. **Legacy** flat ``list[str]`` from pre-confidence rows
       (``["alt one.", "alt two."]``). Each entry normalises to
       ``{text, signal=None, score=None, band=None}``.
    2. **Old ensemble** structured shape from the four-signal aggregate
       phase (``[{"text": …, "scores": {...}, "ensemble": …, "band": …},
       …]``). The per-signal scores are dropped — only the band label
       survives so legacy Studio runs still show a coloured pill — and
       ``signal``/``score`` come back as ``None``.
    3. **Current** single-signal shape
       (``[{"text": …, "signal": …, "score": …, "band": …}, …]``).
       Returned as-is.

    Anything malformed yields ``[]`` plus a warning log, never an
    exception, so the API endpoints and Studio UI keep rendering.
    """
    if not raw:
        return []
    try:
        data = json.loads(raw) if isinstance(raw, str) else raw
    except Exception:
        log.warning("alternatives_json parse failed; treating as empty")
        return []
    if not isinstance(data, list):
        return []
    out: list[dict] = []
    for entry in data:
        if isinstance(entry, str):
            out.append(
                {
                    "text": entry,
                    "signal": None,
                    "score": None,
                    "band": None,
                }
            )
        elif isinstance(entry, dict) and "text" in entry:
            # Current single-signal shape uses ``signal`` + ``score``.
            # Old ensemble shape uses ``scores`` (dict) + ``ensemble``.
            if "signal" in entry or "score" in entry:
                signal = entry.get("signal")
                score = entry.get("score")
                band = entry.get("band")
            else:
                # Legacy ensemble row — preserve the band label only;
                # per-signal data no longer fits the schema.
                signal = None
                score = None
                band = entry.get("band")
            out.append(
                {
                    "text": str(entry["text"]),
                    "signal": signal if isinstance(signal, str) or signal is None else None,
                    "score": float(score) if isinstance(score, (int, float)) else None,
                    "band": band if isinstance(band, str) or band is None else None,
                }
            )
    return out


def build_alternatives_json(suggestion) -> str:
    """Serialise a suggestion (or suggestion-like duck) into JSON for storage.

    When the suggestion carries ``suggestion_scores`` (Phase 1 confidence
    feature), the structured shape ``[{text, scores, ensemble, band}, ...]``
    is emitted. Otherwise the legacy ``list[str]`` shape is preserved so
    existing readers continue to function on rows produced before the
    feature shipped.

    ``suggestion`` is duck-typed and accepts:
      * A dataclass / ``SimpleNamespace`` with ``.suggestions`` and
        ``.suggestion_scores`` attributes.
      * A dict with ``alternatives`` (legacy text list) and optional
        ``alternative_scores`` (list of dicts or ``AlternativeScore``
        instances) — used at the orchestrator → storage boundary.
    """
    if isinstance(suggestion, dict):
        alternatives = list(suggestion.get("alternatives") or [])
        scores = suggestion.get("alternative_scores")
    else:
        alternatives = list(getattr(suggestion, "suggestions", []) or [])
        scores = getattr(suggestion, "suggestion_scores", None)

    if not scores:
        return json.dumps(alternatives, ensure_ascii=True)
    out = []
    for score in scores:
        if hasattr(score, "to_json"):
            out.append(score.to_json())
        elif isinstance(score, dict):
            out.append(score)
        else:
            out.append({"text": str(score)})
    return json.dumps(out, ensure_ascii=True)


class SQLiteHistoryStore:
    """Persist run history and metadata in a local SQLite database."""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._lock = threading.Lock()

    def init(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS analysis_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    started_at REAL NOT NULL,
                    ended_at REAL,
                    duration_sec REAL,
                    status TEXT NOT NULL,
                    command TEXT NOT NULL,
                    mode TEXT,
                    db_backend TEXT,
                    db_profile TEXT,
                    llm_provider TEXT,
                    llm_model TEXT,
                    scope_json TEXT,
                    metrics_json TEXT,
                    tokens_json TEXT,
                    results_json TEXT,
                    error_text TEXT,
                    -- Reporting columns added in 0.5.2 to make /history honest
                    -- about partial / interrupted runs:
                    -- ``selected_count``: assets the user originally selected
                    -- (pre missing-only filter).
                    -- ``planned_count``: assets remaining after the missing-
                    --   only filter — what we actually intended to process.
                    -- ``processed_count``: assets that have started processing
                    --   (incremented per-table; survives Ctrl+C).
                    -- ``applied_count``: results successfully written to live
                    --   DB via apply_review_results_to_db.
                    -- ``review_strategy``: individual / deferred / auto-apply,
                    --   used by the status logic so auto-apply runs never
                    --   land in 'ready_for_review'.
                    selected_count INTEGER NOT NULL DEFAULT 0,
                    planned_count INTEGER NOT NULL DEFAULT 0,
                    processed_count INTEGER NOT NULL DEFAULT 0,
                    applied_count INTEGER NOT NULL DEFAULT 0,
                    review_strategy TEXT
                )
                """
            )
            # Migration runs on every init() AND defensively at the top
            # of create_run (see ``_ensure_run_columns``). This catches the
            # case where users upgraded AMX while ``_store`` was already
            # initialised in a long-running process, or where init() ran
            # before the upgraded code was loaded (pipx editable mode
            # quirks).
            self._ensure_run_columns(conn)
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_analysis_runs_started_at "
                "ON analysis_runs(started_at DESC)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS app_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at REAL NOT NULL,
                    event_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    command TEXT NOT NULL,
                    details_json TEXT
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_app_events_created_at "
                "ON app_events(created_at DESC)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS session_state (
                    namespace TEXT NOT NULL,
                    key_name TEXT NOT NULL,
                    value_json TEXT NOT NULL,
                    updated_at REAL NOT NULL,
                    PRIMARY KEY (namespace, key_name)
                )
                """
            )
            # ── run_results: all alternatives produced per column per run ──────
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS run_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL,
                    saved_at REAL NOT NULL,
                    schema_name TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    column_name TEXT,
                    asset_kind TEXT NOT NULL DEFAULT 'table',
                    source TEXT NOT NULL,
                    confidence TEXT NOT NULL,
                    logprob_score REAL,
                    raw_logprob REAL,
                    token_count INTEGER,
                    model_version TEXT NOT NULL DEFAULT '',
                    reasoning TEXT,
                    alternatives_json TEXT NOT NULL,
                    evaluated_at REAL,
                    applied_at REAL,
                    chosen_description TEXT,
                    evaluation TEXT,
                    catalog_status TEXT NOT NULL DEFAULT '',
                    catalog_indexed_at REAL,
                    db_applied_status TEXT NOT NULL DEFAULT '',
                    effective_source_kind TEXT NOT NULL DEFAULT '',
                    superseded_at REAL,
                    rejection_reason TEXT NOT NULL DEFAULT '',
                    alternatives_mode TEXT,
                    seed_alternative_id TEXT,
                    seed_alternative_text TEXT,
                    parent_run_id INTEGER,
                    model TEXT,
                    provider TEXT,
                    production_warning TEXT,
                    FOREIGN KEY (run_id) REFERENCES analysis_runs(id)
                )
                """
            )
            # Backward-compatible migration for older history DBs.
            with contextlib.suppress(sqlite3.OperationalError):
                conn.execute(
                    "ALTER TABLE run_results ADD COLUMN asset_kind TEXT NOT NULL DEFAULT 'table'"
                )
            with contextlib.suppress(sqlite3.OperationalError):
                conn.execute("ALTER TABLE run_results ADD COLUMN applied_at REAL")
            with contextlib.suppress(sqlite3.OperationalError):
                conn.execute("ALTER TABLE run_results ADD COLUMN logprob_score REAL")
            for stmt in (
                "ALTER TABLE run_results ADD COLUMN raw_logprob REAL",
                "ALTER TABLE run_results ADD COLUMN token_count INTEGER",
                "ALTER TABLE run_results ADD COLUMN model_version TEXT NOT NULL DEFAULT ''",
            ):
                with contextlib.suppress(sqlite3.OperationalError):
                    conn.execute(stmt)
            for stmt in (
                "ALTER TABLE run_results ADD COLUMN catalog_status TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE run_results ADD COLUMN catalog_indexed_at REAL",
                "ALTER TABLE run_results ADD COLUMN db_applied_status TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE run_results ADD COLUMN effective_source_kind TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE run_results ADD COLUMN superseded_at REAL",
                "ALTER TABLE run_results ADD COLUMN rejection_reason TEXT NOT NULL DEFAULT ''",
                # 0.12.x — attribution + shared-store provenance on
                # run_results so a row pulled down from the shared
                # store via /history-store pull-from-shared can carry
                # the originating user/host through to /history show
                # and dedupe on re-pull via shared_uuid.
                "ALTER TABLE run_results ADD COLUMN created_by TEXT",
                "ALTER TABLE run_results ADD COLUMN hostname TEXT",
                "ALTER TABLE run_results ADD COLUMN shared_uuid TEXT",
                # Re-Run versioning (v0.13). ``parent_result_id`` links a
                # re-run row back to the original run_results row it was
                # spawned from; ``rerun_seq`` is 0 for originals, 1+ for
                # successive re-runs in the chain. ``user_instructions``
                # records the optional free-text addendum the user typed
                # in the re-run modal so the audit trail / history drawer
                # can show it next to the alternatives.
                "ALTER TABLE run_results ADD COLUMN parent_result_id INTEGER",
                "ALTER TABLE run_results ADD COLUMN rerun_seq INTEGER NOT NULL DEFAULT 0",
                "ALTER TABLE run_results ADD COLUMN user_instructions TEXT",
                # PR C (citation chain): structured provenance for
                # RAG-derived suggestions. JSON-encoded
                # ``list[{source, chunk_idx, score, snippet}]`` so the
                # CLI run summary and Studio Run detail page can
                # render which document chunks informed each
                # suggestion. NULL / empty list on legacy rows and
                # non-RAG sources -- callers treat both as "no
                # citations" with no UI fallout.
                "ALTER TABLE run_results ADD COLUMN citations_json TEXT",
                # Diversity mode active when the row's alternatives
                # were generated, per Definition 1 (NLP standard):
                # ``semantic`` (default) ⇒ paraphrases of DESCRIPTION_1
                # (same meaning, different wording); ``lexical`` ⇒
                # shared vocabulary with DESCRIPTION_1 while allowing the
                # meaning to drift through added nuances or reframing.
                # Captured row-level (not just run-level) so a rerun
                # that switches mode shows an accurate audit per row in
                # the review UI. NB: rows written before commit ``<sha>``
                # used the inverted definitions — see CHANGELOG.
                "ALTER TABLE run_results ADD COLUMN alternatives_mode TEXT",
                # Variations feature (v0.15). When a row was generated as a
                # seeded variation from one specific alternative of an earlier
                # run, ``seed_alternative_id`` is the string
                # ``"{parent_result_id}:{alt_index}"`` identifying that source
                # alt; ``seed_alternative_text`` carries the verbatim seed so
                # the audit trail survives even if the parent row is later
                # rewritten. ``parent_run_id`` is the seed's owning
                # ``analysis_runs.id`` (distinct from the row-level
                # ``parent_result_id`` used by Re-Run chains; both can coexist).
                # ``model`` / ``provider`` capture the LLM identity that
                # produced the row — needed when a per-run model override was
                # in effect since the run-level ``analysis_runs.llm_model`` /
                # ``analysis_runs.llm_provider`` would still report the base
                # profile's values. NULL on every legacy row.
                "ALTER TABLE run_results ADD COLUMN seed_alternative_id TEXT",
                "ALTER TABLE run_results ADD COLUMN seed_alternative_text TEXT",
                "ALTER TABLE run_results ADD COLUMN parent_run_id INTEGER",
                "ALTER TABLE run_results ADD COLUMN model TEXT",
                "ALTER TABLE run_results ADD COLUMN provider TEXT",
                # Under-production audit. When the LLM (or the
                # parser) produces fewer alternatives than the active
                # profile's ``n_alternatives``, this column captures
                # a one-line summary — e.g. ``"produced 2 of 3
                # requested"`` or ``"produced 2 of 3 requested (after
                # seed echo)"`` on Variations rows where the model
                # echoed the seed verbatim. NULL on the success path
                # so absence-of-warning is meaningful.
                "ALTER TABLE run_results ADD COLUMN production_warning TEXT",
            ):
                with contextlib.suppress(sqlite3.OperationalError):
                    conn.execute(stmt)
            with contextlib.suppress(sqlite3.OperationalError):
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_run_results_parent "
                    "ON run_results(parent_result_id)"
                )
            with contextlib.suppress(sqlite3.OperationalError):
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_run_results_parent_run "
                    "ON run_results(parent_run_id)"
                )
            # ── rerun_context_snapshots: short-lived, GC'd when the worker
            # finishes (job.done / failed / cancelled). One row per target
            # item per re-run job; payload_json is the AgentContext frozen
            # at job start so all parallel agents see identical inputs.
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS rerun_context_snapshots (
                    snapshot_id      TEXT PRIMARY KEY,
                    job_id           TEXT NOT NULL,
                    target_result_id INTEGER NOT NULL,
                    payload_json     TEXT NOT NULL,
                    created_at       REAL NOT NULL
                )
                """
            )
            with contextlib.suppress(sqlite3.OperationalError):
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_rerun_snap_job "
                    "ON rerun_context_snapshots(job_id)"
                )
            with contextlib.suppress(sqlite3.OperationalError):
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_rerun_snap_created "
                    "ON rerun_context_snapshots(created_at)"
                )
            with contextlib.suppress(sqlite3.OperationalError):
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_run_results_shared_uuid "
                    "ON run_results(shared_uuid)"
                )
            # ── run_context_cache: persistent table-level context produced
            # at first run, reused on subsequent re-runs to skip live
            # ``profile_table`` introspection. Keyed on
            # (db_profile, database, schema, table) so a re-run of any
            # column on the same table can hit the same row. The cache
            # is dropped from ``_record_audit`` after the row's COMMENT
            # lands on the live database (i.e. we trust the table the
            # user just touched is no longer a re-run target). A 24h
            # TTL guards against silently serving stale schema after
            # the user altered the table out-of-band.
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS run_context_cache (
                    cache_key       TEXT PRIMARY KEY,
                    db_profile      TEXT NOT NULL,
                    database_name   TEXT NOT NULL DEFAULT '',
                    schema_name     TEXT NOT NULL,
                    table_name      TEXT NOT NULL,
                    payload_json    TEXT NOT NULL,
                    source_run_id   INTEGER,
                    created_at      REAL NOT NULL,
                    expires_at      REAL
                )
                """
            )
            with contextlib.suppress(sqlite3.OperationalError):
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_run_context_cache_table "
                    "ON run_context_cache(db_profile, database_name, schema_name, table_name)"
                )
            with contextlib.suppress(sqlite3.OperationalError):
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_run_context_cache_expires "
                    "ON run_context_cache(expires_at)"
                )
            # ── column_comments_cache: per-table existing-comment cache ──
            # On large warehouses (Databricks especially) the per-table
            # DESCRIBE EXTENDED loop the sidebar and CLI inspect flows used
            # to hit became 30s+. The connector now folds the whole schema
            # into one bulk INFORMATION_SCHEMA-style query and stashes the
            # result here, keyed per-table so a single COMMENT write can
            # invalidate just the row that changed. TTL is the second line
            # of defence for DBA-edited comments that AMX never sees.
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS column_comments_cache (
                    cache_key      TEXT PRIMARY KEY,
                    db_profile     TEXT NOT NULL,
                    database_name  TEXT NOT NULL DEFAULT '',
                    schema_name    TEXT NOT NULL,
                    table_name     TEXT NOT NULL,
                    table_comment  TEXT,
                    columns_json   TEXT NOT NULL,
                    kind           TEXT NOT NULL DEFAULT 'TABLE',
                    fetched_at     REAL NOT NULL,
                    expires_at     REAL NOT NULL
                )
                """
            )
            with contextlib.suppress(sqlite3.OperationalError):
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_ccc_profile_schema "
                    "ON column_comments_cache(db_profile, database_name, schema_name)"
                )
            with contextlib.suppress(sqlite3.OperationalError):
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_ccc_expires "
                    "ON column_comments_cache(expires_at)"
                )
            # ``bulk_filled`` differentiates two write paths:
            #   - ``1`` — entry came from a successful
            #     ``adapter.bulk_schema_metadata`` call, which by
            #     contract returns EVERY table in the schema. Presence
            #     of any such row for a schema means the cache covers
            #     the whole schema and ``list_assets`` can read from it
            #     directly without re-issuing SHOW TABLES.
            #   - ``0`` — entry came from the per-table inspector
            #     fallback. The schema may have other uncached tables;
            #     ``list_assets`` must NOT trust this state.
            # Added as a migration so existing histories pick it up.
            with contextlib.suppress(sqlite3.OperationalError):
                conn.execute(
                    "ALTER TABLE column_comments_cache "
                    "ADD COLUMN bulk_filled INTEGER NOT NULL DEFAULT 0"
                )
            # ── schemas_cache: per-catalog schema-level metadata ──
            # Catalog expand in the sidebar fires ``list_schemas`` (one
            # query — fast) and then ``get_schema_comment`` per schema
            # (DESCRIBE SCHEMA / pg_namespace lookup — slow loop).
            # This table absorbs both: a single ``bulk_catalog_metadata``
            # query fills schema names + comments for the whole catalog
            # in one round-trip, and the result lives here under a
            # ``(profile, database, catalog)`` scope. The freshness
            # marker on individual rows mirrors the column cache.
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS schemas_cache (
                    cache_key       TEXT PRIMARY KEY,
                    db_profile      TEXT NOT NULL,
                    database_name   TEXT NOT NULL DEFAULT '',
                    catalog_name    TEXT NOT NULL DEFAULT '',
                    schema_name     TEXT NOT NULL,
                    schema_comment  TEXT,
                    bulk_filled     INTEGER NOT NULL DEFAULT 0,
                    fetched_at      REAL NOT NULL,
                    expires_at      REAL NOT NULL
                )
                """
            )
            with contextlib.suppress(sqlite3.OperationalError):
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_sc_profile_catalog "
                    "ON schemas_cache(db_profile, database_name, catalog_name)"
                )
            with contextlib.suppress(sqlite3.OperationalError):
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_sc_expires ON schemas_cache(expires_at)"
                )
            # ── view_definitions_cache: per-view DDL + pre-parsed lineage ──
            # /lineage extractors stay off the wire by default. A single
            # row per (profile, database, schema, view) caches the raw
            # CREATE VIEW text and its sqlglot-parsed column lineage. TTL
            # mirrors column_comments_cache so the cache-first guarantee
            # is automatic on stable warehouses.
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS view_definitions_cache (
                    cache_key            TEXT PRIMARY KEY,
                    db_profile           TEXT NOT NULL,
                    database_name        TEXT NOT NULL DEFAULT '',
                    schema_name          TEXT NOT NULL,
                    view_name            TEXT NOT NULL,
                    ddl_text             TEXT NOT NULL,
                    dialect              TEXT NOT NULL,
                    parsed_lineage_json  TEXT,
                    parse_status         TEXT NOT NULL,
                    parse_error          TEXT NOT NULL DEFAULT '',
                    fetched_at           REAL NOT NULL,
                    expires_at           REAL NOT NULL
                )
                """
            )
            with contextlib.suppress(sqlite3.OperationalError):
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_vdc_scope "
                    "ON view_definitions_cache(db_profile, database_name, schema_name)"
                )
            with contextlib.suppress(sqlite3.OperationalError):
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_vdc_expires "
                    "ON view_definitions_cache(expires_at)"
                )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_run_results_run_id ON run_results(run_id)")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_run_results_asset "
                "ON run_results(schema_name, table_name, column_name)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS catalog_entities (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    db_profile TEXT NOT NULL,
                    db_backend TEXT NOT NULL DEFAULT '',
                    database_name TEXT NOT NULL DEFAULT '',
                    schema_name TEXT NOT NULL DEFAULT '',
                    table_name TEXT NOT NULL DEFAULT '',
                    column_name TEXT,
                    entity_kind TEXT NOT NULL,
                    asset_kind TEXT NOT NULL DEFAULT 'table',
                    dtype TEXT NOT NULL DEFAULT '',
                    nullable INTEGER NOT NULL DEFAULT 1,
                    pk_flag INTEGER NOT NULL DEFAULT 0,
                    fk_flag INTEGER NOT NULL DEFAULT 0,
                    row_count INTEGER NOT NULL DEFAULT 0,
                    search_text TEXT NOT NULL DEFAULT '',
                    current_confidence TEXT NOT NULL DEFAULT '',
                    effective_status TEXT NOT NULL DEFAULT '',
                    effective_source_kind TEXT NOT NULL DEFAULT '',
                    effective_description_id INTEGER,
                    updated_at REAL NOT NULL DEFAULT 0,
                    last_synced_at REAL NOT NULL DEFAULT 0,
                    last_code_sync_at REAL
                )
                """
            )
            # Identity index migration: pre-v0.16 the unique key didn't
            # include ``database_name``, so a profile with three databases
            # that each held a ``public.users`` table clobbered each other
            # on insert and the cache leaked across databases. Drop the
            # narrow index (if present from the legacy bootstrap) and
            # recreate with ``database_name`` in the tuple. Existing rows
            # that violate the new uniqueness — the survivor of the old
            # collision — are tolerated; the next skeleton sync re-fills
            # whatever was lost.
            conn.execute("DROP INDEX IF EXISTS idx_catalog_entities_identity")
            conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_catalog_entities_identity
                ON catalog_entities(
                    db_profile,
                    database_name,
                    schema_name,
                    table_name,
                    COALESCE(column_name, ''),
                    entity_kind
                )
                """
            )
            # Bridge column for ingested remote assets: holds the
            # ``remote_<kind>s.id`` that this catalog_entities row mirrors
            # so the lineage canvas can render notebook / job / pipeline /
            # query / stream / streamlit_app nodes while the asset content
            # itself lives in the ``remote_*`` tables. NULL on every
            # database table / column row.
            with contextlib.suppress(sqlite3.OperationalError):
                conn.execute("ALTER TABLE catalog_entities ADD COLUMN source_remote_id INTEGER")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_catalog_entities_source_remote "
                "ON catalog_entities(source_remote_id, entity_kind) "
                "WHERE source_remote_id IS NOT NULL"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS catalog_descriptions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entity_id INTEGER NOT NULL,
                    description_text TEXT NOT NULL,
                    source_kind TEXT NOT NULL,
                    source_agent TEXT NOT NULL DEFAULT '',
                    confidence TEXT NOT NULL DEFAULT '',
                    logprob_score REAL,
                    reasoning TEXT NOT NULL DEFAULT '',
                    run_id INTEGER,
                    result_id INTEGER,
                    created_at REAL NOT NULL,
                    superseded INTEGER NOT NULL DEFAULT 0,
                    indexed INTEGER NOT NULL DEFAULT 0,
                    indexed_at REAL,
                    applied_to_db INTEGER NOT NULL DEFAULT 0,
                    applied_at REAL,
                    chosen_description INTEGER NOT NULL DEFAULT 0,
                    FOREIGN KEY (entity_id) REFERENCES catalog_entities(id),
                    FOREIGN KEY (run_id) REFERENCES analysis_runs(id),
                    FOREIGN KEY (result_id) REFERENCES run_results(id)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_catalog_descriptions_entity_id ON catalog_descriptions(entity_id, created_at DESC)"
            )
            # FTS5 virtual table for concept search. Mirrors the four
            # search-relevant columns from ``catalog_entities``:
            # ``search_text`` carries the synthesized blob the resolver
            # built (path + dtype + descriptions + relationships) and
            # the three name columns let MATCH queries weight a hit on
            # column / table name above a hit in the description body.
            # Stored as ``content_rowid=id`` so each FTS row tracks the
            # owning entity row by primary key — sync.py mirrors INSERT
            # / UPDATE / DELETE alongside the entity write path. The
            # ``content=''`` shape (contentless FTS) avoids needing
            # triggers to keep the FTS in sync with arbitrary external
            # writes; we own every catalog_entities write site already.
            conn.execute(
                """
                CREATE VIRTUAL TABLE IF NOT EXISTS catalog_entities_fts
                USING fts5(
                    db_profile UNINDEXED,
                    column_name,
                    table_name,
                    schema_name,
                    search_text,
                    tokenize='unicode61 remove_diacritics 2'
                )
                """
            )
            # One-shot backfill on upgrade. If the catalog already holds
            # rows but the FTS table is empty (existing install jumping
            # to v0.15+), populate every entity now so concept search
            # works on the first ``/ask`` after upgrade — without this
            # the legacy O(n) scan fallback handles correctness but the
            # latency win only kicks in after the next ``/search sync``.
            try:
                pending = conn.execute(
                    "SELECT COUNT(*) AS n FROM catalog_entities WHERE search_text != ''"
                ).fetchone()
                fts_count = conn.execute(
                    "SELECT COUNT(*) AS n FROM catalog_entities_fts"
                ).fetchone()
                if pending and fts_count and int(pending["n"]) > 0 and int(fts_count["n"]) == 0:
                    conn.execute(
                        """
                        INSERT INTO catalog_entities_fts (
                            rowid, db_profile, column_name, table_name, schema_name, search_text
                        )
                        SELECT id, db_profile, COALESCE(column_name, ''), table_name,
                               schema_name, search_text
                        FROM catalog_entities
                        WHERE search_text != ''
                        """
                    )
            except sqlite3.OperationalError:
                # FTS5 missing on the host SQLite; the search path
                # falls back to the legacy scan automatically.
                pass
            # Per-profile completeness state. The cache-first read path
            # (sidebar, schedule/run scope pickers, Ask agent tools)
            # gates on ``state = 'done'`` so a partially-synced catalog
            # never gets surfaced to the user as the full picture. The
            # row is upserted by ``sync_profile_skeleton`` at start /
            # progress / finish.
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS catalog_profile_state (
                    db_profile TEXT PRIMARY KEY,
                    state TEXT NOT NULL DEFAULT 'none',
                    total_tables INTEGER NOT NULL DEFAULT 0,
                    processed_tables INTEGER NOT NULL DEFAULT 0,
                    started_at REAL,
                    finished_at REAL,
                    last_full_sync_at REAL,
                    last_error TEXT NOT NULL DEFAULT ''
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS catalog_relationships (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    from_entity_id INTEGER NOT NULL,
                    to_entity_id INTEGER NOT NULL,
                    relationship_type TEXT NOT NULL,
                    score REAL NOT NULL DEFAULT 0,
                    source TEXT NOT NULL DEFAULT '',
                    details_json TEXT NOT NULL DEFAULT '{}',
                    last_seen REAL NOT NULL DEFAULT 0,
                    FOREIGN KEY (from_entity_id) REFERENCES catalog_entities(id),
                    FOREIGN KEY (to_entity_id) REFERENCES catalog_entities(id)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_catalog_relationships_from_to ON catalog_relationships(from_entity_id, to_entity_id, relationship_type)"
            )
            # v3 S4 — authoring metadata. Optional verdict + audit
            # columns added via additive ALTERs so existing databases
            # upgrade in place without losing rows. Values:
            #   verdict ∈ {'', 'approved', 'rejected', 'pending'}
            #   audit_actor: OS user / studio actor who last touched it
            #   audit_at: UTC epoch seconds of last touch
            for _ddl in (
                "ALTER TABLE catalog_relationships ADD COLUMN verdict TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE catalog_relationships ADD COLUMN audit_actor TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE catalog_relationships ADD COLUMN audit_at REAL",
                # v4 — column-level lineage. Empty string keeps the
                # row at table grain (legacy behaviour); non-empty
                # promotes it to a column→column edge. Operator nodes
                # (entity_kind='operator') hang off the same edge
                # rows.
                "ALTER TABLE catalog_relationships ADD COLUMN from_column TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE catalog_relationships ADD COLUMN to_column TEXT NOT NULL DEFAULT ''",
                # v5 — Studio-canvas style overrides. All nullable so
                # an unedited edge reads as ``NULL`` → frontend
                # defaults take over. Style overrides live on the edge
                # itself (not the artifact), so the same relationship
                # carries its user-chosen color / cardinality across
                # every canvas that surfaces it.
                "ALTER TABLE catalog_relationships ADD COLUMN style_color TEXT",
                "ALTER TABLE catalog_relationships ADD COLUMN style_dashed INTEGER",
                "ALTER TABLE catalog_relationships ADD COLUMN cardinality TEXT",
                # v6 — polymorphic-FK support for remote-asset lineage.
                # 'table' is the backward-compatible default; new rows
                # for notebook/job/pipeline/streamlit/stream/query edges
                # set the matching kind so callers know which remote_*
                # table the id belongs to.
                "ALTER TABLE catalog_relationships ADD COLUMN from_entity_kind TEXT NOT NULL DEFAULT 'table'",
                "ALTER TABLE catalog_relationships ADD COLUMN to_entity_kind TEXT NOT NULL DEFAULT 'table'",
            ):
                with contextlib.suppress(sqlite3.OperationalError):
                    conn.execute(_ddl)
            with contextlib.suppress(sqlite3.OperationalError):
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_catalog_relationships_column_grain "
                    "ON catalog_relationships(from_entity_id, from_column, to_entity_id, to_column) "
                    "WHERE from_column != '' OR to_column != ''"
                )
            # ── lineage_artifacts: registry of rendered lineage diagrams ──
            # Each row binds a focal entity (anchor) to a rendered image
            # on disk plus the edge-set hash that produced it. Drives
            # /lineage open|refresh|delete|list and lets /open detect
            # drift without re-running extractors.
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS lineage_artifacts (
                    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                    name                TEXT NOT NULL,
                    db_profile          TEXT NOT NULL,
                    anchor_entity_id    INTEGER NOT NULL,
                    depth_up            INTEGER NOT NULL DEFAULT 1,
                    depth_down          INTEGER NOT NULL DEFAULT 1,
                    format              TEXT NOT NULL,
                    output_path         TEXT NOT NULL,
                    edge_set_hash       TEXT NOT NULL,
                    node_count          INTEGER NOT NULL,
                    edge_count          INTEGER NOT NULL,
                    generated_at        REAL NOT NULL,
                    extractors_used     TEXT NOT NULL,
                    extractors_partial  INTEGER NOT NULL DEFAULT 0,
                    FOREIGN KEY (anchor_entity_id) REFERENCES catalog_entities(id)
                )
                """
            )
            with contextlib.suppress(sqlite3.OperationalError):
                conn.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS idx_lineage_artifacts_name "
                    "ON lineage_artifacts(name)"
                )
            with contextlib.suppress(sqlite3.OperationalError):
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_lineage_artifacts_anchor "
                    "ON lineage_artifacts(anchor_entity_id)"
                )
            # ── lineage_artifact_nodes: per-canvas node placement w/ profile ──
            # One row per node on a saved canvas. Carries its own db_profile
            # so a single canvas can host nodes from multiple profiles
            # (cross-profile lineage). x/y persist the user's manual layout
            # so re-open restores the same arrangement instead of re-running
            # dagre. ``entity_id`` is the catalog_entities row for the
            # table or operator the node represents.
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS lineage_artifact_nodes (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    artifact_id     INTEGER NOT NULL,
                    entity_id       INTEGER NOT NULL,
                    db_profile      TEXT NOT NULL,
                    x               REAL NOT NULL DEFAULT 0,
                    y               REAL NOT NULL DEFAULT 0,
                    width           REAL NOT NULL DEFAULT 240,
                    height          REAL NOT NULL DEFAULT 120,
                    z_index         INTEGER NOT NULL DEFAULT 0,
                    FOREIGN KEY (artifact_id) REFERENCES lineage_artifacts(id) ON DELETE CASCADE,
                    FOREIGN KEY (entity_id) REFERENCES catalog_entities(id)
                )
                """
            )
            with contextlib.suppress(sqlite3.OperationalError):
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_lineage_artifact_nodes_artifact "
                    "ON lineage_artifact_nodes(artifact_id)"
                )
            with contextlib.suppress(sqlite3.OperationalError):
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_lineage_artifact_nodes_entity "
                    "ON lineage_artifact_nodes(entity_id, db_profile)"
                )
            # ── lineage_logos: registry of available logos for canvas use ──
            # Two flavours:
            #   * ``source='default'`` — seeded once at init from the
            #     bundled SVG library in ``amx/lineage/default_logos/``.
            #     Read-only from the API; cannot be deleted by users so
            #     a fresh install always has the well-known brands
            #     (aws, gcp, powerbi, …).
            #   * ``source='custom'`` — user-uploaded via POST. Stored as
            #     either an inline base64 data URL (file upload) or a
            #     pasted external URL.
            # Same (key, source) is unique so the seed is idempotent and
            # users can shadow a default by adding a custom row with the
            # same key without colliding.
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS lineage_logos (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    key         TEXT NOT NULL,
                    label       TEXT NOT NULL,
                    category    TEXT NOT NULL,
                    source      TEXT NOT NULL,
                    data_url    TEXT NOT NULL DEFAULT '',
                    url         TEXT NOT NULL DEFAULT '',
                    created_at  REAL NOT NULL,
                    UNIQUE(key, source)
                )
                """
            )
            with contextlib.suppress(sqlite3.OperationalError):
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_lineage_logos_category "
                    "ON lineage_logos(category)"
                )
            # ── lineage_logo_nodes: standalone logo nodes on a canvas ──
            # Each row places a logo (from the registry) on a saved
            # artifact's canvas. Cascades on artifact delete.
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS lineage_logo_nodes (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    artifact_id INTEGER NOT NULL,
                    logo_id     INTEGER NOT NULL,
                    label       TEXT NOT NULL DEFAULT '',
                    x           REAL NOT NULL DEFAULT 0,
                    y           REAL NOT NULL DEFAULT 0,
                    width       REAL NOT NULL DEFAULT 120,
                    height      REAL NOT NULL DEFAULT 120,
                    created_at  REAL NOT NULL,
                    updated_at  REAL NOT NULL,
                    FOREIGN KEY (artifact_id) REFERENCES lineage_artifacts(id) ON DELETE CASCADE,
                    FOREIGN KEY (logo_id) REFERENCES lineage_logos(id)
                )
                """
            )
            with contextlib.suppress(sqlite3.OperationalError):
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_lineage_logo_nodes_artifact "
                    "ON lineage_logo_nodes(artifact_id)"
                )
            # ── lineage_artifact_nodes.logo_key (header badge override) ──
            # Optional per-table logo badge that overrides the
            # backend-derived auto-bind. Empty string = no override (the
            # frontend falls back to the auto-bound logo for the node's
            # profile.backend, or no badge if no backend match).
            with contextlib.suppress(sqlite3.OperationalError):
                conn.execute(
                    "ALTER TABLE lineage_artifact_nodes ADD COLUMN logo_key TEXT DEFAULT ''"
                )
            # ── lineage_comments: sticky-note annotations on a canvas ──
            # Free-floating notes are not lineage entities — they live
            # alongside a saved canvas and never participate in edge
            # resolution. Stored separately from catalog_entities so the
            # entity model stays clean (it's about tables/columns/operators
            # only). One artifact owns N comments; deleting the artifact
            # cascades.
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS lineage_comments (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    artifact_id     INTEGER NOT NULL,
                    x               REAL NOT NULL DEFAULT 0,
                    y               REAL NOT NULL DEFAULT 0,
                    width           REAL NOT NULL DEFAULT 240,
                    height          REAL NOT NULL DEFAULT 140,
                    color           TEXT NOT NULL DEFAULT 'amber',
                    text            TEXT NOT NULL DEFAULT '',
                    created_at      REAL NOT NULL,
                    updated_at      REAL NOT NULL,
                    FOREIGN KEY (artifact_id) REFERENCES lineage_artifacts(id) ON DELETE CASCADE
                )
                """
            )
            with contextlib.suppress(sqlite3.OperationalError):
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_lineage_comments_artifact "
                    "ON lineage_comments(artifact_id)"
                )
            # Two render modes share the comments table:
            #   * ``style='note'`` (default) — colored sticky note with
            #     a header band and color picker.
            #   * ``style='text'`` — minimal plain-text label, no
            #     background, no border; for canvas section headings
            #     and free-form annotations.
            # Shape is identical (text + x/y/w/h) so an ALTER is enough
            # rather than a second table.
            with contextlib.suppress(sqlite3.OperationalError):
                conn.execute("ALTER TABLE lineage_comments ADD COLUMN style TEXT DEFAULT 'note'")
            # PR-3: OCC version columns on concurrent-edit lineage tables.
            # Note: lineage_artifact_edges is shared-only and not created locally.
            for _occ_tbl in (
                "lineage_artifacts",
                "lineage_artifact_nodes",
                "lineage_comments",
            ):
                with contextlib.suppress(sqlite3.OperationalError):
                    conn.execute(f"ALTER TABLE {_occ_tbl} ADD COLUMN version INTEGER DEFAULT 1")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS catalog_usage_evidence (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    db_profile TEXT NOT NULL DEFAULT '',
                    entity_id INTEGER,
                    source_kind TEXT NOT NULL,
                    evidence_type TEXT NOT NULL,
                    source_path TEXT NOT NULL DEFAULT '',
                    count_value INTEGER NOT NULL DEFAULT 0,
                    score_value REAL NOT NULL DEFAULT 0,
                    sample_snippets_json TEXT NOT NULL DEFAULT '[]',
                    last_seen REAL NOT NULL DEFAULT 0,
                    FOREIGN KEY (entity_id) REFERENCES catalog_entities(id)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_catalog_usage_entity ON catalog_usage_evidence(db_profile, entity_id, source_kind, evidence_type)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS catalog_sync_jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    db_profile TEXT NOT NULL,
                    job_type TEXT NOT NULL,
                    scope_json TEXT NOT NULL DEFAULT '{}',
                    started_at REAL NOT NULL,
                    completed_at REAL,
                    status TEXT NOT NULL,
                    inserted_count INTEGER NOT NULL DEFAULT 0,
                    updated_count INTEGER NOT NULL DEFAULT 0,
                    error_text TEXT NOT NULL DEFAULT ''
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_catalog_sync_jobs_profile_started ON catalog_sync_jobs(db_profile, started_at DESC)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS search_settings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    db_profile TEXT NOT NULL,
                    key_name TEXT NOT NULL,
                    value_text TEXT NOT NULL,
                    updated_at REAL NOT NULL
                )
                """
            )
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_search_settings_profile_key ON search_settings(db_profile, key_name)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS chat_sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    db_profile TEXT NOT NULL,
                    llm_profile TEXT NOT NULL,
                    started_at REAL NOT NULL,
                    last_active_at REAL NOT NULL,
                    ended_at REAL,
                    title TEXT,
                    turn_count INTEGER NOT NULL DEFAULT 0,
                    total_tokens INTEGER NOT NULL DEFAULT 0,
                    compaction_state_json TEXT,
                    scope_profiles_json TEXT,
                    focus_profile TEXT
                )
                """
            )
            # Backwards-compatible migration for older history DBs created
            # before multi-profile ask shipped (PR ask-multi-profile-A).
            for stmt in (
                "ALTER TABLE chat_sessions ADD COLUMN scope_profiles_json TEXT",
                "ALTER TABLE chat_sessions ADD COLUMN focus_profile TEXT",
            ):
                with contextlib.suppress(sqlite3.OperationalError):
                    conn.execute(stmt)
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_chat_sessions_profile_active "
                "ON chat_sessions(db_profile, llm_profile, last_active_at DESC)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS chat_turns (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id INTEGER NOT NULL,
                    run_id INTEGER,
                    turn_index INTEGER NOT NULL,
                    role TEXT NOT NULL,
                    question TEXT,
                    answer_summary TEXT,
                    intent TEXT,
                    topic TEXT,
                    confidence TEXT,
                    tables_json TEXT NOT NULL DEFAULT '[]',
                    columns_json TEXT NOT NULL DEFAULT '[]',
                    plan_json TEXT,
                    tokens_json TEXT,
                    request_id TEXT,
                    created_at REAL NOT NULL,
                    estimated_tokens INTEGER NOT NULL DEFAULT 0,
                    compacted_at REAL,
                    FOREIGN KEY (session_id) REFERENCES chat_sessions(id) ON DELETE CASCADE,
                    FOREIGN KEY (run_id) REFERENCES analysis_runs(id)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_chat_turns_session_index "
                "ON chat_turns(session_id, turn_index)"
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_chat_turns_run ON chat_turns(run_id)")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS style_profiles (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    llm_profile     TEXT    NOT NULL,
                    source_ref      TEXT    NOT NULL,
                    source_db_kind  TEXT    NOT NULL,
                    profile_json    TEXT    NOT NULL,
                    enabled         INTEGER NOT NULL DEFAULT 1,
                    sample_count    INTEGER NOT NULL,
                    created_at      TEXT    NOT NULL,
                    updated_at      TEXT    NOT NULL,
                    UNIQUE (llm_profile)
                )
                """
            )
            # ── scheduled_runs: one-shot scheduled metadata runs ──
            #
            # Created and managed via the `amx schedule` command group
            # (Phase 3) and the Studio Schedules page (Phase 5). The
            # tick engine (Phase 2) reads pending rows whose
            # ``fire_at_utc`` has elapsed and transitions them through
            # the state machine documented in
            # docs/superpowers/specs/2026-05-13-scheduled-runs-design.md.
            #
            # ``fire_at_utc`` is always canonical UTC; ``fire_at_tz``
            # is the IANA tz id the user picked for display and DST
            # handling. ``scope_json`` stores a high-level reference
            # (schema/table names) and is resolved against the live DB
            # at fire time -- new tables under a scheduled schema are
            # picked up automatically; missing entities surface as a
            # clean failure with last_error populated.
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS scheduled_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    fire_at_utc REAL NOT NULL,
                    fire_at_tz TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    db_profile TEXT NOT NULL,
                    database TEXT,
                    catalog TEXT,
                    scope_json TEXT NOT NULL,
                    llm_profile TEXT NOT NULL,
                    review_strategy TEXT NOT NULL,
                    extra_args_json TEXT,
                    kind TEXT NOT NULL DEFAULT 'analyze',
                    cron_expr TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    fired_at REAL,
                    triggered_run_id INTEGER,
                    last_error TEXT
                )
                """
            )
            self._ensure_scheduled_columns(conn)
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_scheduled_runs_status_fireat "
                "ON scheduled_runs(status, fire_at_utc)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_scheduled_runs_db_profile "
                "ON scheduled_runs(db_profile)"
            )
            # ── documentation_pages: LLM-composed narrative pages ───────
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS documentation_pages (
                    id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    slug TEXT NOT NULL UNIQUE,
                    markdown_body TEXT NOT NULL,
                    rendered_html TEXT,
                    status TEXT NOT NULL DEFAULT 'draft',
                    created_at TIMESTAMP NOT NULL,
                    updated_at TIMESTAMP NOT NULL,
                    created_by TEXT,
                    generation_prompt TEXT,
                    model_used TEXT
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_documentation_pages_status "
                "ON documentation_pages(status)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_documentation_pages_updated_at "
                "ON documentation_pages(updated_at DESC)"
            )
            # PR-2: db_profile + attribution columns (idempotent ALTERs for
            # existing installs; CREATE TABLE above handles fresh databases).
            for _col_spec in (
                "db_profile TEXT",
                "hostname TEXT",
                "client_version TEXT",
                "local_id INTEGER",
            ):
                with contextlib.suppress(sqlite3.OperationalError):
                    conn.execute(f"ALTER TABLE documentation_pages ADD COLUMN {_col_spec}")
            # ── documentation_page_assets: per-page asset list ──────────
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS documentation_page_assets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    page_id TEXT NOT NULL,
                    asset_kind TEXT NOT NULL,
                    asset_ref TEXT NOT NULL,
                    included INTEGER NOT NULL DEFAULT 1,
                    FOREIGN KEY (page_id) REFERENCES documentation_pages(id)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_documentation_page_assets_page_id "
                "ON documentation_page_assets(page_id)"
            )
            # ── documentation_page_sources: per-page upload list ────────
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS documentation_page_sources (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    page_id TEXT NOT NULL,
                    source_kind TEXT NOT NULL,
                    source_path TEXT NOT NULL,
                    original_name TEXT NOT NULL,
                    created_at TIMESTAMP NOT NULL,
                    FOREIGN KEY (page_id) REFERENCES documentation_pages(id)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_documentation_page_sources_page_id "
                "ON documentation_page_sources(page_id)"
            )
            # ── documentation_page_versions: per-save snapshots ─────────
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS documentation_page_versions (
                    page_id TEXT NOT NULL,
                    version_no INTEGER NOT NULL,
                    markdown_body TEXT NOT NULL,
                    saved_at TIMESTAMP NOT NULL,
                    saved_by TEXT,
                    note TEXT,
                    PRIMARY KEY (page_id, version_no),
                    FOREIGN KEY (page_id) REFERENCES documentation_pages(id)
                )
                """
            )
            # ── remote_notebooks: notebooks ingested from remote platforms ──
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS remote_notebooks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    profile_name TEXT NOT NULL,
                    platform TEXT NOT NULL,
                    external_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    workspace_path TEXT,
                    qualified_name TEXT,
                    language TEXT NOT NULL,
                    source_text TEXT NOT NULL,
                    source_hash TEXT NOT NULL,
                    last_modified_at TIMESTAMP,
                    last_modified_by TEXT,
                    owner TEXT,
                    cell_count INTEGER,
                    ingested_at TIMESTAMP NOT NULL,
                    UNIQUE(profile_name, platform, external_id)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_remote_notebooks_profile_platform "
                "ON remote_notebooks(profile_name, platform)"
            )

            # ── remote_jobs: Databricks jobs/workflows ──────────────────────
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS remote_jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    profile_name TEXT NOT NULL,
                    job_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    creator_user_name TEXT,
                    schedule_cron TEXT,
                    schedule_timezone TEXT,
                    schedule_pause_status TEXT,
                    max_concurrent_runs INTEGER,
                    email_notifications_json TEXT,
                    tags_json TEXT,
                    last_run_status TEXT,
                    last_run_started_at TIMESTAMP,
                    success_rate_30d REAL,
                    ingested_at TIMESTAMP NOT NULL,
                    UNIQUE(profile_name, job_id)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_remote_jobs_profile ON remote_jobs(profile_name)"
            )

            # ── remote_job_tasks: per-task definitions within a job ───────
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS remote_job_tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id_fk INTEGER NOT NULL,
                    task_key TEXT NOT NULL,
                    task_type TEXT NOT NULL,
                    notebook_path TEXT,
                    notebook_id_fk INTEGER,
                    sql_query_id TEXT,
                    sql_warehouse_id TEXT,
                    pipeline_id_fk INTEGER,
                    depends_on_json TEXT,
                    raw_definition_json TEXT NOT NULL,
                    UNIQUE(job_id_fk, task_key),
                    FOREIGN KEY (job_id_fk) REFERENCES remote_jobs(id),
                    FOREIGN KEY (notebook_id_fk) REFERENCES remote_notebooks(id)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_remote_job_tasks_job ON remote_job_tasks(job_id_fk)"
            )

            # ── remote_job_runs: recent run history for each job ─────────
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS remote_job_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id_fk INTEGER NOT NULL,
                    run_id INTEGER NOT NULL,
                    state_result TEXT NOT NULL,
                    start_time TIMESTAMP NOT NULL,
                    end_time TIMESTAMP,
                    setup_duration_ms INTEGER,
                    execution_duration_ms INTEGER,
                    UNIQUE(job_id_fk, run_id),
                    FOREIGN KEY (job_id_fk) REFERENCES remote_jobs(id)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_remote_job_runs_job ON remote_job_runs(job_id_fk)"
            )

            # ── remote_pipelines: Databricks DLT pipeline definitions ────────
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS remote_pipelines (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    profile_name TEXT NOT NULL,
                    pipeline_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    target_schema TEXT,
                    edition TEXT,
                    continuous INTEGER NOT NULL,
                    photon INTEGER NOT NULL,
                    libraries_json TEXT NOT NULL,
                    latest_update_state TEXT,
                    latest_update_creation_time TIMESTAMP,
                    ingested_at TIMESTAMP NOT NULL,
                    UNIQUE(profile_name, pipeline_id)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_remote_pipelines_profile ON remote_pipelines(profile_name)"
            )

            # ── remote_streamlit_apps: Snowflake STREAMLIT objects ────────────
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS remote_streamlit_apps (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    profile_name TEXT NOT NULL,
                    qualified_name TEXT NOT NULL,
                    main_file TEXT NOT NULL,
                    query_warehouse TEXT,
                    root_location TEXT NOT NULL,
                    owner TEXT,
                    last_altered_at TIMESTAMP,
                    ingested_at TIMESTAMP NOT NULL,
                    UNIQUE(profile_name, qualified_name)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_remote_streamlit_apps_profile ON remote_streamlit_apps(profile_name)"
            )

            # ── remote_streams: Snowflake CDC streams ────────────────────────
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS remote_streams (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    profile_name TEXT NOT NULL,
                    qualified_name TEXT NOT NULL,
                    source_table_fqn TEXT NOT NULL,
                    source_entity_id INTEGER,
                    mode TEXT NOT NULL,
                    stale_after TIMESTAMP,
                    owner TEXT,
                    ingested_at TIMESTAMP NOT NULL,
                    UNIQUE(profile_name, qualified_name),
                    FOREIGN KEY (source_entity_id) REFERENCES catalog_entities(id)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_remote_streams_profile ON remote_streams(profile_name)"
            )

            # ── remote_task_dependencies: Snowflake task DAG edges ───────────
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS remote_task_dependencies (
                    profile_name TEXT NOT NULL,
                    parent_task_fqn TEXT NOT NULL,
                    child_task_fqn TEXT NOT NULL,
                    PRIMARY KEY(profile_name, parent_task_fqn, child_task_fqn)
                )
                """
            )

            # ── remote_queries: saved queries and execution history ───────────
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS remote_queries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    profile_name TEXT NOT NULL,
                    platform TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    external_id TEXT NOT NULL,
                    name TEXT,
                    sql_text TEXT NOT NULL,
                    sql_hash TEXT NOT NULL,
                    warehouse TEXT,
                    user_name TEXT,
                    executed_at TIMESTAMP,
                    duration_ms INTEGER,
                    ingested_at TIMESTAMP NOT NULL,
                    UNIQUE(profile_name, platform, kind, external_id)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_remote_queries_profile_platform ON remote_queries(profile_name, platform)"
            )

            # ── asset_chunking_overrides: per-asset chunking strategy ─────────
            # Studio's per-row "Chunk" button writes here so an individual
            # notebook / query / pipeline can carry a chunking strategy
            # that differs from the global ``cfg.assets_chunking``
            # default. NULL columns (chunk_chars / chunk_overlap) mean
            # "inherit the default for the active strategy".
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS asset_chunking_overrides (
                    profile_name TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    remote_id INTEGER NOT NULL,
                    strategy TEXT NOT NULL,
                    chunk_chars INTEGER,
                    chunk_overlap INTEGER,
                    updated_at REAL NOT NULL,
                    PRIMARY KEY (profile_name, kind, remote_id)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_asset_chunking_overrides_profile "
                "ON asset_chunking_overrides(profile_name, kind)"
            )

            # Seed the bundled default logos into ``lineage_logos`` if
            # they aren't there yet. Idempotent via the UNIQUE(key,
            # source) index — re-runs on every init are no-ops after
            # the first.
            try:
                from amx.lineage.logo_store import seed_default_logos
            except Exception:  # pragma: no cover - import guard
                seed_default_logos = None
            if seed_default_logos is not None:
                with contextlib.suppress(Exception):
                    seed_default_logos(self)
            # Last step: populate the metadata sidecar so every table created
            # above carries a queryable description. Must run after all
            # CREATE TABLE / ALTER TABLE statements so PRAGMA table_info
            # returns the final column set.
            self._populate_schema_descriptions(conn)

    def _populate_schema_descriptions(self, conn: Any) -> None:
        """Create and idempotently populate the metadata sidecar table.

        Creates ``_amx_schema_descriptions`` (if missing) and writes one row
        per (object_kind, schema, table, column) using ``INSERT OR REPLACE``.
        Descriptions are sourced from
        :data:`amx.storage.schema_descriptions.SCHEMA_DESCRIPTIONS`, which is
        also the single source of truth read by
        :mod:`amx.storage.shared_schema`.

        Runs on every :meth:`init` call so new strings authored in the SoT
        propagate to existing installs on next boot. The PK on
        (object_kind, schema_name, table_name, column_name) guarantees the
        operation is O(rows) and free of duplicates.

        Tables that are missing on this install (older schemas, partial
        migrations) are silently skipped — the next ``init`` after the
        ``CREATE TABLE`` lands will pick them up.
        """
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {_SCHEMA_DESCRIPTIONS_TABLE} (
                object_kind TEXT NOT NULL,
                schema_name TEXT NOT NULL DEFAULT '',
                table_name  TEXT NOT NULL DEFAULT '',
                column_name TEXT NOT NULL DEFAULT '',
                description TEXT NOT NULL,
                updated_at  REAL NOT NULL,
                PRIMARY KEY (object_kind, schema_name, table_name, column_name)
            )
            """
        )
        now = time.time()
        # Database-level row.
        conn.execute(
            f"INSERT OR REPLACE INTO {_SCHEMA_DESCRIPTIONS_TABLE} "
            f"(object_kind, schema_name, table_name, column_name, description, updated_at) "
            f"VALUES (?, ?, ?, ?, ?, ?)",
            ("database", "", "", "", LOCAL_DATABASE_DESCRIPTION, now),
        )
        # Set of tables that actually exist in this DB. Includes virtual
        # tables (FTS5) but excludes SQLite shadow tables and tables not
        # yet created on this install.
        existing_names: set[str] = set()
        try:
            existing_names = {
                str(r[0])
                for r in conn.execute(
                    "SELECT name FROM sqlite_master "
                    "WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%'"
                ).fetchall()
            }
        except Exception as exc:
            log.warning("Could not enumerate sqlite_master for description sync: %s", exc)
        for table_name, fields in SCHEMA_DESCRIPTIONS.items():
            if table_name not in existing_names:
                continue
            table_desc = fields.get("__table__")
            if table_desc:
                conn.execute(
                    f"INSERT OR REPLACE INTO {_SCHEMA_DESCRIPTIONS_TABLE} "
                    f"(object_kind, schema_name, table_name, column_name, description, updated_at) "
                    f"VALUES (?, ?, ?, ?, ?, ?)",
                    ("table", "main", table_name, "", table_desc, now),
                )
            try:
                live_cols = [
                    str(r[1]) for r in conn.execute(f"PRAGMA table_info({table_name})").fetchall()
                ]
            except Exception as exc:
                log.warning("Could not PRAGMA table_info(%s): %s", table_name, exc)
                live_cols = []
            for col_name in live_cols:
                col_desc = fields.get(col_name)
                if not col_desc:
                    # Missing description for a real column is a contract
                    # violation enforced by tests/test_local_schema_comments.py.
                    # Skip silently here so the boot path stays resilient; CI
                    # will fail the offending PR before it lands.
                    continue
                conn.execute(
                    f"INSERT OR REPLACE INTO {_SCHEMA_DESCRIPTIONS_TABLE} "
                    f"(object_kind, schema_name, table_name, column_name, description, updated_at) "
                    f"VALUES (?, ?, ?, ?, ?, ?)",
                    ("column", "main", table_name, col_name, col_desc, now),
                )

    def _ensure_run_columns(self, conn: Any) -> None:
        """Idempotently add the v0.5.2 reporting columns to analysis_runs.

        Called from both ``init()`` (the normal path) and the top of
        ``create_run`` (safety net for users who upgraded AMX without a
        clean restart, or whose init() ran on stale code under a pipx
        editable install). Each ALTER is wrapped so per-column failures
        are isolated and visible in the logs.
        """
        try:
            rows = conn.execute("PRAGMA table_info(analysis_runs)").fetchall()
            existing_cols = {str(r[1]) for r in rows}
        except Exception as exc:
            log.warning("Could not introspect analysis_runs schema: %s", exc)
            existing_cols = set()
        for col_name, col_type in (
            ("selected_count", "INTEGER NOT NULL DEFAULT 0"),
            ("planned_count", "INTEGER NOT NULL DEFAULT 0"),
            ("processed_count", "INTEGER NOT NULL DEFAULT 0"),
            ("applied_count", "INTEGER NOT NULL DEFAULT 0"),
            ("review_strategy", "TEXT"),
            # 0.11.x — profile names captured for /compare so runs that
            # differ only by LLM/doc/code profile can be told apart.
            # Older rows stay NULL and render as '—' in comparisons.
            ("llm_profile", "TEXT"),
            ("doc_profile", "TEXT"),
            ("code_profile", "TEXT"),
            # 0.11.x — full LLM/run config snapshot at run-start time
            # (prompt_detail, language, column_batch_size, n_alternatives,
            # completion_mode, description_verbosity, temperature,
            # batch_context_column_names, dedup_used, missing_only).
            # Stored as JSON so future settings can be added without
            # another schema migration. /history compare surfaces these
            # so users can see exactly which knobs varied between runs.
            ("settings_json", "TEXT"),
            # 0.12.x — attribution + shared-store provenance. Populated
            # for runs created on this machine (so /history list can
            # render "by alice@laptop-A" once shared mode is on) AND
            # for runs pulled down from the team's shared store via
            # /history-store pull-from-shared. ``shared_uuid`` is NULL
            # for runs created locally; for pulled rows it's the UUID
            # PK of the corresponding shared row, so re-running pull
            # is idempotent (we look up by shared_uuid before inserting).
            ("created_by", "TEXT"),
            ("hostname", "TEXT"),
            ("client_version", "TEXT"),
            ("shared_uuid", "TEXT"),
            # Scheduled-runs feature (Phase 1a). ``triggered_by_schedule_id``
            # links a run back to the scheduled_runs row that fired it;
            # ``last_heartbeat_at`` is bumped by the orchestrator while a
            # run is in flight and consumed by the stale-run recovery
            # path (Phase 2). Both are NULL for runs not driven by the
            # scheduler and for runs created before this migration.
            ("triggered_by_schedule_id", "INTEGER"),
            ("last_heartbeat_at", "REAL"),
            # Cold-load progress signal for the Studio run-detail page.
            # The worker rewrites this as it walks startup phases
            # (connect / LLM / RAG / orchestrator / per-table); the
            # SPA seeds its "current step" label from it so refresh
            # doesn't fall back to "Waiting for the worker to begin…".
            ("current_step_label", "TEXT"),
        ):
            if col_name in existing_cols:
                continue
            try:
                conn.execute(f"ALTER TABLE analysis_runs ADD COLUMN {col_name} {col_type}")
                log.info(
                    "Migrated analysis_runs: added column %s %s",
                    col_name,
                    col_type,
                )
            except Exception as exc:
                log.warning(
                    "Could not add analysis_runs.%s: %s  --  partial-progress "
                    "reporting in /history will show '—'.",
                    col_name,
                    exc,
                )
        # Index for the dedup lookup on pull-from-shared. SQLite skips
        # creation when it already exists. (run_results indexes live
        # next to its CREATE TABLE in init().)
        with contextlib.suppress(sqlite3.OperationalError):
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_analysis_runs_shared_uuid "
                "ON analysis_runs(shared_uuid)"
            )

    def _ensure_scheduled_columns(self, conn: Any) -> None:
        """Idempotently add ``database`` / ``catalog`` to scheduled_runs.

        Schedules created before this column landed are missing the
        per-schedule DB / catalog overlay; the picker fetched live
        schemas via ``/api/live/schemas?profile=…&database=…`` but the
        ``database`` half was dropped on the way to the store, so at
        fire time ``production_run_executor`` connected to the profile
        default and ``airline.<table>`` resolutions raised
        ``NoSuchTableError``. The two nullable columns let existing
        rows survive untouched; on next Edit the user picks a
        database and the next fire connects to the right DB.
        """
        try:
            rows = conn.execute("PRAGMA table_info(scheduled_runs)").fetchall()
            existing_cols = {str(r[1]) for r in rows}
        except Exception as exc:
            log.warning("Could not introspect scheduled_runs schema: %s", exc)
            existing_cols = set()
        for col_name, col_type in (
            ("database", "TEXT"),
            ("catalog", "TEXT"),
            # ``kind`` discriminates run-type schedules from cache-refresh
            # schedules. Cache-refresh rows ignore LLM-profile fields and
            # invalidate the catalog cache via the connector path instead
            # of spawning an analysis_runs row. Defaults to 'analyze' so
            # every legacy row keeps its current behaviour.
            ("kind", "TEXT NOT NULL DEFAULT 'analyze'"),
            # ``cron_expr`` turns a one-shot schedule into a recurring
            # one. NULL keeps the legacy single-fire semantics; a valid
            # croniter expression (e.g. ``0 */6 * * *``) re-arms the row
            # to ``status='pending'`` with a fresh ``fire_at_utc`` every
            # time it fires.
            ("cron_expr", "TEXT"),
        ):
            if col_name in existing_cols:
                continue
            try:
                conn.execute(f"ALTER TABLE scheduled_runs ADD COLUMN {col_name} {col_type}")
                log.info(
                    "Migrated scheduled_runs: added column %s %s",
                    col_name,
                    col_type,
                )
            except Exception as exc:
                log.warning("Could not add scheduled_runs.%s: %s", col_name, exc)
        # ── apply_events: audit trail of every COMMENT actually written ──
        #
        # ``run_results.applied_at`` already says "this row was applied"
        # but cannot answer "what was the comment before we overwrote
        # it" or "who applied it on which host". The apply_events table
        # records one row per successful COMMENT write so /history
        # rollback (PR-12b) and Studio's Recent Applies panel (PR-12c)
        # have a stable replay log. Old comments are stored verbatim so
        # rollback can restore them character-for-character.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS apply_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                applied_at REAL NOT NULL,
                run_id INTEGER,
                result_id INTEGER,
                profile_name TEXT NOT NULL DEFAULT '',
                schema_name TEXT NOT NULL,
                table_name TEXT NOT NULL DEFAULT '',
                column_name TEXT,
                asset_kind TEXT NOT NULL DEFAULT 'table',
                old_comment TEXT,
                new_comment TEXT NOT NULL,
                applied_by TEXT NOT NULL DEFAULT '',
                hostname TEXT NOT NULL DEFAULT '',
                sql_template TEXT NOT NULL DEFAULT '',
                FOREIGN KEY (run_id) REFERENCES analysis_runs(id),
                FOREIGN KEY (result_id) REFERENCES run_results(id)
            )
            """
        )
        with contextlib.suppress(sqlite3.OperationalError):
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_apply_events_applied_at "
                "ON apply_events(applied_at DESC)"
            )
        with contextlib.suppress(sqlite3.OperationalError):
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_apply_events_run_id ON apply_events(run_id)"
            )
        with contextlib.suppress(sqlite3.OperationalError):
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_apply_events_asset "
                "ON apply_events(profile_name, schema_name, table_name, column_name)"
            )

    # ── Run lifecycle delegators (extracted to amx.storage._history_runs) ──
    def create_run(self, *args, **kwargs):
        from amx.storage._history_runs import create_run

        return create_run(self, *args, **kwargs)

    def update_run_planned_count(self, *args, **kwargs):
        from amx.storage._history_runs import update_run_planned_count

        return update_run_planned_count(self, *args, **kwargs)

    def increment_run_processed(self, *args, **kwargs):
        from amx.storage._history_runs import increment_run_processed

        return increment_run_processed(self, *args, **kwargs)

    def increment_run_applied(self, *args, **kwargs):
        from amx.storage._history_runs import increment_run_applied

        return increment_run_applied(self, *args, **kwargs)

    def finish_run(self, *args, **kwargs):
        from amx.storage._history_runs import finish_run

        return finish_run(self, *args, **kwargs)

    def update_run_status(self, *args, **kwargs):
        from amx.storage._history_runs import update_run_status

        return update_run_status(self, *args, **kwargs)

    def list_recent_runs(self, *args, **kwargs):
        from amx.storage._history_runs import list_recent_runs

        return list_recent_runs(self, *args, **kwargs)

    def find_runs_for_scope(self, *args, **kwargs):
        from amx.storage._history_runs import find_runs_for_scope

        return find_runs_for_scope(self, *args, **kwargs)

    def get_run(self, *args, **kwargs):
        from amx.storage._history_runs import get_run

        return get_run(self, *args, **kwargs)

    def stats(self, *args, **kwargs):
        from amx.storage._history_runs import stats

        return stats(self, *args, **kwargs)

    def count_pending_review_runs(self, *args, **kwargs):
        from amx.storage._history_runs import count_pending_review_runs

        return count_pending_review_runs(self, *args, **kwargs)

    def list_recent_events(self, *args, **kwargs):
        from amx.storage._history_runs import list_recent_events

        return list_recent_events(self, *args, **kwargs)

    def recover_stale_runs(self, *args, **kwargs):
        from amx.storage._history_runs import recover_stale_runs

        return recover_stale_runs(self, *args, **kwargs)

    def update_run_heartbeat(self, *args, **kwargs):
        from amx.storage._history_runs import update_run_heartbeat

        return update_run_heartbeat(self, *args, **kwargs)

    def update_run_current_step(self, *args, **kwargs):
        from amx.storage._history_runs import update_run_current_step

        return update_run_current_step(self, *args, **kwargs)

    # ── Audit + scheduled delegators (extracted to amx.storage._history_apply_audit / _history_scheduled) ──
    def save_run_results(self, *args, **kwargs):
        from amx.storage._history_apply_audit import save_run_results

        return save_run_results(self, *args, **kwargs)

    def record_evaluation(self, *args, **kwargs):
        from amx.storage._history_apply_audit import record_evaluation

        return record_evaluation(self, *args, **kwargs)

    def record_applied(self, *args, **kwargs):
        from amx.storage._history_apply_audit import record_applied

        return record_applied(self, *args, **kwargs)

    def record_db_apply_failure(self, *args, **kwargs):
        from amx.storage._history_apply_audit import record_db_apply_failure

        return record_db_apply_failure(self, *args, **kwargs)

    def record_apply_event(self, *args, **kwargs):
        from amx.storage._history_apply_audit import record_apply_event

        return record_apply_event(self, *args, **kwargs)

    def list_apply_events(self, *args, **kwargs):
        from amx.storage._history_apply_audit import list_apply_events

        return list_apply_events(self, *args, **kwargs)

    def create_scheduled_run(self, *args, **kwargs):
        from amx.storage._history_scheduled import create_scheduled_run

        return create_scheduled_run(self, *args, **kwargs)

    def get_scheduled_run(self, *args, **kwargs):
        from amx.storage._history_scheduled import get_scheduled_run

        return get_scheduled_run(self, *args, **kwargs)

    def list_scheduled_runs(self, *args, **kwargs):
        from amx.storage._history_scheduled import list_scheduled_runs

        return list_scheduled_runs(self, *args, **kwargs)

    def list_due_pending_schedules(self, *args, **kwargs):
        from amx.storage._history_scheduled import list_due_pending_schedules

        return list_due_pending_schedules(self, *args, **kwargs)

    def update_scheduled_run(self, *args, **kwargs):
        from amx.storage._history_scheduled import update_scheduled_run

        return update_scheduled_run(self, *args, **kwargs)

    def set_scheduled_run_status(self, *args, **kwargs):
        from amx.storage._history_scheduled import set_scheduled_run_status

        return set_scheduled_run_status(self, *args, **kwargs)

    def delete_scheduled_run(self, *args, **kwargs):
        from amx.storage._history_scheduled import delete_scheduled_run

        return delete_scheduled_run(self, *args, **kwargs)

    def claim_due_schedule(self, *args, **kwargs):
        from amx.storage._history_scheduled import claim_due_schedule

        return claim_due_schedule(self, *args, **kwargs)

    def arm_next_fire(self, *args, **kwargs):
        from amx.storage._history_scheduled import arm_next_fire

        return arm_next_fire(self, *args, **kwargs)

    def profile_has_active_cache_refresh_schedule(self, *args, **kwargs):
        from amx.storage._history_scheduled import profile_has_active_cache_refresh_schedule

        return profile_has_active_cache_refresh_schedule(self, *args, **kwargs)

    def set_run_schedule_link(self, *args, **kwargs):
        from amx.storage._history_scheduled import set_run_schedule_link

        return set_run_schedule_link(self, *args, **kwargs)

    def set_session_state(self, namespace: str, key: str, value: Any) -> None:
        from amx.storage._history_session_state import set_session_state

        set_session_state(self, namespace, key, value)

    def get_session_state(self, namespace: str, key: str, default: Any = None) -> Any:
        from amx.storage._history_session_state import get_session_state

        return get_session_state(self, namespace, key, default)

    def log_event(
        self,
        *,
        event_type: str,
        status: str,
        command: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        from amx.storage._history_event_log import log_event

        log_event(
            self,
            event_type=event_type,
            status=status,
            command=command,
            details=details,
        )

    # ── Variations + result-query delegators ──
    def get_run_results(self, *args, **kwargs):
        from amx.storage._history_results import get_run_results

        return get_run_results(self, *args, **kwargs)

    def get_run_result(self, *args, **kwargs):
        from amx.storage._history_results import get_run_result

        return get_run_result(self, *args, **kwargs)

    def get_descendant_runs(self, *args, **kwargs):
        from amx.storage._history_variations import get_descendant_runs

        return get_descendant_runs(self, *args, **kwargs)

    def get_result_chain(self, *args, **kwargs):
        from amx.storage._history_variations import get_result_chain

        return get_result_chain(self, *args, **kwargs)

    def next_rerun_seq(self, *args, **kwargs):
        from amx.storage._history_variations import next_rerun_seq

        return next_rerun_seq(self, *args, **kwargs)

    def save_rerun_snapshot(self, *args, **kwargs):
        from amx.storage._history_variations import save_rerun_snapshot

        return save_rerun_snapshot(self, *args, **kwargs)

    def read_rerun_snapshot(self, *args, **kwargs):
        from amx.storage._history_variations import read_rerun_snapshot

        return read_rerun_snapshot(self, *args, **kwargs)

    def list_rerun_snapshots_for_job(self, *args, **kwargs):
        from amx.storage._history_variations import list_rerun_snapshots_for_job

        return list_rerun_snapshots_for_job(self, *args, **kwargs)

    def delete_rerun_snapshots_for_job(self, *args, **kwargs):
        from amx.storage._history_variations import delete_rerun_snapshots_for_job

        return delete_rerun_snapshots_for_job(self, *args, **kwargs)

    def gc_orphan_rerun_snapshots(self, *args, **kwargs):
        from amx.storage._history_variations import gc_orphan_rerun_snapshots

        return gc_orphan_rerun_snapshots(self, *args, **kwargs)

    def list_runs_with_result_counts(self, *args, **kwargs):
        from amx.storage._history_results import list_runs_with_result_counts

        return list_runs_with_result_counts(self, *args, **kwargs)

    # ── Cache delegators (extracted to amx.storage._history_caches) ──
    @staticmethod
    def _context_cache_key(*args, **kwargs):
        from amx.storage._history_caches import _context_cache_key

        return _context_cache_key(*args, **kwargs)

    def save_run_context_cache(self, *args, **kwargs):
        from amx.storage._history_caches import save_run_context_cache

        return save_run_context_cache(self, *args, **kwargs)

    def lookup_run_context_cache(self, *args, **kwargs):
        from amx.storage._history_caches import lookup_run_context_cache

        return lookup_run_context_cache(self, *args, **kwargs)

    def delete_run_context_cache(self, *args, **kwargs):
        from amx.storage._history_caches import delete_run_context_cache

        return delete_run_context_cache(self, *args, **kwargs)

    def gc_run_context_cache(self, *args, **kwargs):
        from amx.storage._history_caches import gc_run_context_cache

        return gc_run_context_cache(self, *args, **kwargs)

    @staticmethod
    def _ccc_key(*args, **kwargs):
        from amx.storage._history_caches import _ccc_key

        return _ccc_key(*args, **kwargs)

    def save_column_comments_cache(self, *args, **kwargs):
        from amx.storage._history_caches import save_column_comments_cache

        return save_column_comments_cache(self, *args, **kwargs)

    def schema_has_bulk_filled_cache(self, *args, **kwargs):
        from amx.storage._history_caches import schema_has_bulk_filled_cache

        return schema_has_bulk_filled_cache(self, *args, **kwargs)

    def lookup_column_comments_cache(self, *args, **kwargs):
        from amx.storage._history_caches import lookup_column_comments_cache

        return lookup_column_comments_cache(self, *args, **kwargs)

    def lookup_column_comments_cache_bulk(self, *args, **kwargs):
        from amx.storage._history_caches import lookup_column_comments_cache_bulk

        return lookup_column_comments_cache_bulk(self, *args, **kwargs)

    def invalidate_column_comments_cache(self, *args, **kwargs):
        from amx.storage._history_caches import invalidate_column_comments_cache

        return invalidate_column_comments_cache(self, *args, **kwargs)

    def gc_column_comments_cache(self, *args, **kwargs):
        from amx.storage._history_caches import gc_column_comments_cache

        return gc_column_comments_cache(self, *args, **kwargs)

    @staticmethod
    def _sc_key(*args, **kwargs):
        from amx.storage._history_caches import _sc_key

        return _sc_key(*args, **kwargs)

    def save_schemas_cache(self, *args, **kwargs):
        from amx.storage._history_caches import save_schemas_cache

        return save_schemas_cache(self, *args, **kwargs)

    def lookup_schemas_cache(self, *args, **kwargs):
        from amx.storage._history_caches import lookup_schemas_cache

        return lookup_schemas_cache(self, *args, **kwargs)

    def catalog_has_bulk_filled_cache(self, *args, **kwargs):
        from amx.storage._history_caches import catalog_has_bulk_filled_cache

        return catalog_has_bulk_filled_cache(self, *args, **kwargs)

    def list_schemas_from_cache(self, *args, **kwargs):
        from amx.storage._history_caches import list_schemas_from_cache

        return list_schemas_from_cache(self, *args, **kwargs)

    def invalidate_schemas_cache(self, *args, **kwargs):
        from amx.storage._history_caches import invalidate_schemas_cache

        return invalidate_schemas_cache(self, *args, **kwargs)

    def gc_schemas_cache(self, *args, **kwargs):
        from amx.storage._history_caches import gc_schemas_cache

        return gc_schemas_cache(self, *args, **kwargs)

    # ── documentation_pages CRUD (delegators) ───────────────────────────
    def create_documentation_page(self, *args, **kwargs):
        from amx.storage._history_pages import create_documentation_page

        return create_documentation_page(self, *args, **kwargs)

    def get_documentation_page(self, *args, **kwargs):
        from amx.storage._history_pages import get_documentation_page

        return get_documentation_page(self, *args, **kwargs)

    def list_documentation_pages(self, *args, **kwargs):
        from amx.storage._history_pages import list_documentation_pages

        return list_documentation_pages(self, *args, **kwargs)

    def update_documentation_page_body(self, *args, **kwargs):
        from amx.storage._history_pages import update_documentation_page_body

        return update_documentation_page_body(self, *args, **kwargs)

    def soft_delete_documentation_page(self, *args, **kwargs):
        from amx.storage._history_pages import soft_delete_documentation_page

        return soft_delete_documentation_page(self, *args, **kwargs)

    def append_documentation_page_version(self, *args, **kwargs):
        from amx.storage._history_pages import append_documentation_page_version

        return append_documentation_page_version(self, *args, **kwargs)

    def attach_documentation_page_asset(self, *args, **kwargs):
        from amx.storage._history_pages import attach_documentation_page_asset

        return attach_documentation_page_asset(self, *args, **kwargs)

    def attach_documentation_page_source(self, *args, **kwargs):
        from amx.storage._history_pages import attach_documentation_page_source

        return attach_documentation_page_source(self, *args, **kwargs)

    def list_documentation_page_assets(self, *args, **kwargs):
        from amx.storage._history_pages import list_documentation_page_assets

        return list_documentation_page_assets(self, *args, **kwargs)

    def list_documentation_page_sources(self, *args, **kwargs):
        from amx.storage._history_pages import list_documentation_page_sources

        return list_documentation_page_sources(self, *args, **kwargs)

    def list_documentation_page_versions(self, *args, **kwargs):
        from amx.storage._history_pages import list_documentation_page_versions

        return list_documentation_page_versions(self, *args, **kwargs)

    def update_documentation_page_db_profile(self, *args, **kwargs):
        from amx.storage._history_pages import update_documentation_page_db_profile

        return update_documentation_page_db_profile(self, *args, **kwargs)

    def _connect(self) -> sqlite3.Connection:
        # ``timeout=30`` and the matching ``PRAGMA busy_timeout`` both
        # bump the lock-wait budget so concurrent Studio + CLI writers
        # don't surface ``database is locked`` warnings under realistic
        # contention (Studio worker writing run rows while the chat
        # session is mid-LLM call, plus catalog scans landing alongside).
        # WAL keeps readers non-blocking; the timeout only matters for
        # writer-vs-writer overlap. The PRAGMA is the authoritative
        # knob — sqlite3's ``timeout=`` arg is occasionally ignored
        # depending on the platform's libsqlite version.
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    # ── scheduled_runs CRUD ─────────────────────────────────────────────
    #
    # The state machine governs which transitions are legal. See the
    # design spec for the full diagram; the dict below is its
    # source-of-truth implementation. Terminal states (failed,
    # completed, cancelled) intentionally have empty sets -- once a
    # schedule is terminal it stays terminal; the user "re-arms" by
    # cloning into a fresh schedule.
    _SCHEDULE_TRANSITIONS: dict[str, set[str]] = {
        "pending": {"paused", "running", "missed", "cancelled"},
        "paused": {"pending", "cancelled"},
        "running": {"completed", "failed"},
        "missed": {"running", "cancelled", "pending"},
        "failed": set(),
        "completed": set(),
        "cancelled": set(),
    }

    _SCHEDULE_UPDATABLE_FIELDS: frozenset[str] = frozenset(
        {
            "name",
            "fire_at_utc",
            "fire_at_tz",
            "db_profile",
            "database",
            "catalog",
            "scope_json",
            "llm_profile",
            "review_strategy",
            "extra_args_json",
            "kind",
            "cron_expr",
        }
    )


# The singleton is typed as ``Any`` so v0.12.0+ shared-history mode
# can store a :class:`amx.storage.dual_write.DualWriteHistoryStore`
# (which implements :class:`amx.storage.protocol.IHistoryStore` but is
# not a SQLiteHistoryStore subclass). All call sites use Protocol-
# compatible methods, so the loose typing does not hurt them.
_store: Any | None = None


def init_history_store(config_dir: str) -> SQLiteHistoryStore:
    """Initialize the local-only history store (legacy entry point).

    .. deprecated:: 0.12.0
        Prefer :func:`amx.storage.factory.init_history_store(cfg)` —
        the new entry takes an :class:`AMXConfig` and dispatches to
        the dual-write store when shared mode is enabled. This shim
        is kept so the headless application path (which never calls
        the CLI directly) continues to work.
    """
    global _store
    if _store is None:
        db_path = Path(config_dir) / "history.db"
        _store = SQLiteHistoryStore(db_path)
        try:
            _store.init()
            log.info("SQLite history initialized at %s", db_path)
        except Exception as exc:
            log.warning("Could not initialize SQLite history store: %s", exc)
    return _store


def history_store() -> Any | None:
    """Return the active singleton.

    May be ``SQLiteHistoryStore`` (local-only) or ``DualWriteHistoryStore``
    (shared mode) — both implement the :class:`IHistoryStore` Protocol.
    """
    return _store
