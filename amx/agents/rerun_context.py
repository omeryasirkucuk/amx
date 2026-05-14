"""Re-Run snapshot builder.

When the user clicks "Re-Run this item" (CLI or Studio), the worker
freezes the original run's inputs into a short-lived row in
``rerun_context_snapshots`` so every parallel agent sees identical
context. The snapshot is deleted in the worker's ``finally`` block —
storage cost is zero outside the live re-run window.

Snapshot payload (one per target item):

* ``schema`` / ``table`` / ``column`` / ``asset_kind`` — the addressable
  coordinates of the item being re-run.
* ``db_profile`` — the live ``AgentContext.db_profile`` dict produced by
  :meth:`amx.agents.orchestrator.Orchestrator._build_context`. For
  *column* re-runs ``columns`` is filtered down to just the target so
  the agents don't waste tokens on siblings.
* ``rag_context`` / ``code_context`` — placeholders today (Profile-only
  re-run). Populated when the doc/code agent wiring is ported into the
  re-run path in a follow-up.
* ``existing_metadata`` — passed through verbatim from the orchestrator.
* ``user_instructions`` — the optional free-text addendum from the
  re-run modal. Empty string means "no additional guidance".
* ``original`` — book-keeping for the executor: the parent ``run_id`` /
  ``result_id``, the original asset coordinates, the active LLM/DB/Doc/
  Code profile names from the source run.

The functions in this module never write to ``run_results`` themselves
— that's the executor's job (:mod:`amx.agents._orchestrator.rerun`).
This module only owns context assembly + snapshot read/write/delete.
"""

from __future__ import annotations

import uuid
from dataclasses import asdict
from typing import Any

from amx.agents.base import AgentContext
from amx.config import AMXConfig
from amx.db.connector import AssetKind, DatabaseConnector
from amx.storage.sqlite_store import history_store
from amx.utils.logging import get_logger

log = get_logger("agents.rerun_context")


class RerunContextError(RuntimeError):
    """Snapshot build failed (missing target row, no DB profile, etc.).

    The web router translates this to a 4xx HTTPException so the SPA
    surfaces the underlying message instead of "Internal Server Error".
    """


def _serialize_code_hit(hit: dict[str, Any]) -> dict[str, Any]:
    """Lean JSON projection of one ``query_code_snippets`` hit.

    PR δ (C8): code hits carry richer metadata than docs hits — the
    chunker stamps ``source``, ``source_root``, ``rel_path``,
    ``chunk_id``, ``kind``, and 1-based ``start_line`` / ``end_line``
    so citations can render ``file.py:42-58`` precisely. We preserve
    every one of those keys so the re-run replay reproduces the same
    prompt and the citations layer still attributes alternatives to
    the same chunks. Distance / score round-trip as floats; missing
    bounds collapse to ``0`` so the renderer can show "unknown line".
    """
    meta = hit.get("metadata") or {}

    def _coerce_int(value: Any) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    return {
        "text": str(hit.get("text") or ""),
        "metadata": {
            "source": str(meta.get("source") or ""),
            "source_root": str(meta.get("source_root") or ""),
            "rel_path": str(meta.get("rel_path") or ""),
            "chunk_id": str(meta.get("chunk_id") or ""),
            "kind": str(meta.get("kind") or ""),
            "start_line": _coerce_int(meta.get("start_line")),
            "end_line": _coerce_int(meta.get("end_line")),
        },
        "distance": float(hit.get("distance") or 0.0) if hit.get("distance") is not None else None,
        "score": float(hit.get("score") or 0.0),
    }


def _serialize_rag_hit(hit: dict[str, Any]) -> dict[str, Any]:
    """Lean JSON projection of one ``RAGStore.query`` hit for the snapshot.

    Keeps ``text``, ``metadata`` (the per-chunk provenance dict
    written at ingest time — ``source``, ``source_root``,
    ``source_type``, ``chunk_idx``), ``distance``, and ``score`` so
    the re-run replay reproduces the same prompt and the citations
    layer can still attribute alternatives to their original
    chunks. Other keys (loader-specific debug fields, embedding
    vectors if any caller ever sticks one in) are intentionally
    dropped to keep the snapshot small.
    """
    meta = hit.get("metadata") or {}
    return {
        "text": str(hit.get("text") or ""),
        "metadata": {
            "source": str(meta.get("source") or ""),
            "source_root": str(meta.get("source_root") or ""),
            "source_type": str(meta.get("source_type") or ""),
            "chunk_idx": int(meta.get("chunk_idx") or 0)
            if str(meta.get("chunk_idx") or "0").lstrip("-").isdigit()
            else 0,
        },
        "distance": float(hit.get("distance") or 0.0) if hit.get("distance") is not None else None,
        "score": float(hit.get("score") or 0.0),
    }


def _connector_for_db_profile(
    cfg: AMXConfig,
    profile_name: str,
    *,
    database: str | None = None,
    catalog: str | None = None,
) -> DatabaseConnector:
    """Open a fresh ``DatabaseConnector`` against a named DB profile.

    Mirrors the resolution logic the run worker uses: look up the
    profile in ``cfg.db_profiles`` and instantiate a connector. When
    *database* / *catalog* are provided (always the case from the
    re-run path, since the parent run captured its scope) they
    override the profile's defaults via :func:`dataclasses.replace`
    so the connector points at the same database the original run
    targeted.

    Without this override, a profile whose ``database`` field is
    blank falls back to the engine-specific default (Postgres'
    ``postgres`` system DB, Databricks' default catalog, …) and the
    re-run profiles the wrong database entirely — producing
    ``sqlalchemy.exc.NoSuchTableError: cars.data`` even though the
    table exists in ``bird_train`` where the original /run worked.
    """
    base = cfg.db_profiles.get((profile_name or "").strip())
    if base is None:
        raise RerunContextError(
            f"DB profile '{profile_name}' is not defined in this AMXConfig — "
            "the original run referenced a profile that no longer exists."
        )
    patch: dict[str, Any] = {}
    if database:
        patch["database"] = database
    if catalog:
        patch["catalog"] = catalog
    if patch:
        try:
            from dataclasses import replace as _dc_replace

            base = _dc_replace(base, **patch)
        except Exception as exc:  # noqa: BLE001 - defensive
            log.warning(
                "Could not override db_profile %s with database=%r catalog=%r: %s; "
                "falling back to profile defaults — re-run may target the wrong "
                "database.",
                profile_name,
                database,
                catalog,
                exc,
            )
    return DatabaseConnector(base)


def _table_profile_to_dicts(
    profile: Any,
    db: DatabaseConnector,
    *,
    only_column: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Convert a freshly-built ``TableProfile`` to the snapshot dict shape.

    Factored out of :func:`_build_db_profile_dict` so callers that
    already have a ``TableProfile`` in memory (e.g. the first-run
    orchestrator) can re-use the conversion without re-introspecting
    the live database.
    """
    db_name = db.cfg.database or db.cfg.project or db.cfg.catalog or "N/A"
    all_cols = [
        {
            "name": c.name,
            "dtype": c.dtype,
            "nullable": c.nullable,
            "row_count": c.row_count,
            "null_count": c.null_count,
            "distinct_count": c.distinct_count,
            "cardinality_ratio": c.cardinality_ratio,
            "min_val": c.min_val,
            "max_val": c.max_val,
            "samples": c.samples,
            "existing_comment": c.existing_comment,
        }
        for c in profile.columns
    ]
    if only_column:
        target_cols = [c for c in all_cols if c["name"] == only_column]
        sibling_names = [c["name"] for c in all_cols if c["name"] != only_column]
    else:
        target_cols = all_cols
        sibling_names = []

    db_profile: dict[str, Any] = {
        "row_count": profile.row_count,
        "existing_comment": profile.existing_comment,
        "primary_key": profile.primary_key,
        "foreign_keys": profile.foreign_keys,
        "referenced_by": profile.referenced_by,
        "unique_constraints": profile.unique_constraints,
        "check_constraints": profile.check_constraints,
        "stats_seq_scan": profile.stats_seq_scan,
        "stats_idx_scan": profile.stats_idx_scan,
        "stats_n_live_tup": profile.stats_n_live_tup,
        "stats_source": db.stats_label,
        "schema_comment": profile.schema_comment,
        "database_comment": profile.database_comment,
        "related_comments": profile.related_comments,
        "query_usage": {},
        "columns": target_cols,
    }
    if sibling_names:
        db_profile["context_column_names"] = sibling_names
    existing_metadata = {
        "database": db_name,
        "backend": db.backend,
        "table_comment": profile.existing_comment,
        "schema_comment": profile.schema_comment,
        "database_comment": profile.database_comment,
    }
    return db_profile, existing_metadata


def _slice_cached_profile(
    db_profile: dict[str, Any],
    *,
    only_column: str | None,
) -> dict[str, Any]:
    """Return a copy of a cached ``db_profile`` narrowed to one column.

    The cache stores the full table profile (every column) so a
    subsequent re-run targeting a *different* column can still use the
    same row. This helper rebuilds the column-specific slice the
    agents expect at call time.
    """
    if not only_column:
        return dict(db_profile)
    cols = list(db_profile.get("columns") or [])
    target_cols = [c for c in cols if c.get("name") == only_column]
    sibling_names = [c.get("name") for c in cols if c.get("name") != only_column]
    out = dict(db_profile)
    out["columns"] = target_cols
    if sibling_names:
        out["context_column_names"] = sibling_names
    return out


def _build_db_profile_dict(
    db: DatabaseConnector,
    schema: str,
    table: str,
    *,
    asset_kind: AssetKind | None = None,
    only_column: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Assemble the ``db_profile`` slice the agents read from.

    Returns ``(db_profile_dict, existing_metadata_dict)`` matching the
    shape :meth:`Orchestrator._build_context` produces, so the agent
    prompt builders work without any branching for the re-run path.
    When ``only_column`` is provided, ``db_profile['columns']`` is
    narrowed to that single column — sibling columns are kept as
    ``context_column_names`` so the LLM still sees the table shape.
    """
    profile = db.profile_table(schema, table, asset_kind=asset_kind)
    return _table_profile_to_dicts(profile, db, only_column=only_column)


def _resolve_asset_kind(raw: str | None) -> AssetKind | None:
    """Best-effort coercion of stored ``asset_kind`` strings to the enum."""
    if not raw:
        return None
    try:
        return AssetKind(raw)
    except ValueError:
        return None


def build_context_snapshot(
    cfg: AMXConfig,
    *,
    target_result_id: int,
    job_id: str,
    user_instructions: str | None = None,
) -> str:
    """Build + persist a frozen ``AgentContext`` for one target item.

    Returns the generated ``snapshot_id`` (uuid hex). The caller (the
    re-run worker) reads it back via
    :func:`amx.storage.sqlite_store.SQLiteHistoryStore.read_rerun_snapshot`
    and feeds it into the agent fan-out.

    Re-uses live indexes for everything except DB schema (which is
    profiled fresh — the snapshot freezes that fresh read so parallel
    agents agree). RAG/code context populating is intentionally a
    follow-up: the user's free-text addendum is the v1 lever for
    biasing the second pass.
    """
    hs = history_store()
    if hs is None:
        raise RerunContextError(
            "History store is not initialised — cannot resolve original run "
            "metadata. Open Settings → History to enable it."
        )

    target = hs.get_run_result(int(target_result_id))
    if target is None:
        raise RerunContextError(
            f"Target run_result {target_result_id} not found. Was the row deleted?"
        )

    parent_run = hs.get_run(int(target.get("run_id") or 0))
    if parent_run is None:
        raise RerunContextError(
            f"Original run {target.get('run_id')} not found for result {target_result_id}."
        )

    schema = str(target.get("schema_name") or "")
    table = str(target.get("table_name") or "")
    column = target.get("column_name")
    asset_kind_raw = str(target.get("asset_kind") or "table")
    asset_kind = _resolve_asset_kind(asset_kind_raw)

    # Prefer the profile that was active during the original run.
    # Fall back to the currently active profile so a re-run still
    # works after the user rotated profiles between sessions.
    db_profile_name = str(parent_run.get("db_profile") or "") or (cfg.active_db_profile or "")

    payload: dict[str, Any] = {
        "schema": schema,
        "table": table,
        "column": column,
        "asset_kind": asset_kind_raw,
        "db_profile": {},
        "rag_context": [],
        "rag_hits": [],
        "code_hits": [],
        "code_context": [],
        "existing_metadata": {},
        "user_instructions": (user_instructions or "").strip(),
        "original": {
            "run_id": int(target.get("run_id") or 0),
            "result_id": int(target_result_id),
            "rerun_seq": int(target.get("rerun_seq") or 0),
            "parent_result_id": target.get("parent_result_id"),
            "db_profile": db_profile_name or None,
            "llm_profile": parent_run.get("llm_profile"),
            "doc_profile": parent_run.get("doc_profile"),
            "code_profile": parent_run.get("code_profile"),
            "llm_provider": parent_run.get("llm_provider"),
            "llm_model": parent_run.get("llm_model"),
            "db_backend": parent_run.get("db_backend"),
            "alternatives": list(target.get("alternatives_json") or []),
            "chosen_description": target.get("chosen_description") or "",
        },
    }

    # Schema/database-level re-runs aggregate from the run's already-
    # produced table descriptions; no live profiling needed. The
    # executor reads ``original.run_id`` to fetch peer rows when it
    # builds the meta prompt.
    if asset_kind in (AssetKind.SCHEMA, AssetKind.DATABASE):
        payload["existing_metadata"] = {
            "database": "",
            "backend": parent_run.get("db_backend"),
        }
        snapshot_id = uuid.uuid4().hex
        hs.save_rerun_snapshot(
            snapshot_id=snapshot_id,
            job_id=job_id,
            target_result_id=int(target_result_id),
            payload=payload,
        )
        return snapshot_id

    # Table / column re-run: prefer the cached first-run profile when
    # the user hasn't touched the table out-of-band. Saves a 5-30s
    # ``profile_table`` round-trip per re-run target. Cache miss /
    # expiry / cross-database edge case all fall through to the live
    # profile rebuild below.
    if not db_profile_name:
        raise RerunContextError(
            "Original run has no db_profile recorded and no active profile is set — "
            "cannot rebuild the database context for this re-run."
        )

    # The parent run records its database / catalog inside
    # ``settings_json`` — analysis_runs has no top-level ``database``
    # or ``catalog`` column. Reading the top-level field alone returns
    # ``None`` for every existing run, so the connector falls back to
    # the engine default and the inspector raises ``NoSuchTableError``.
    # Mirror the resolution pattern used by ``rerun_items`` in
    # ``_orchestrator/rerun.py`` so both code paths agree on where to
    # find the scope: top-level first (forward-compat with a future
    # schema migration that lifts the field out), then
    # ``settings_json``. ``hs.get_run`` parses settings_json into a
    # dict for us; the defensive str-parse below survives the
    # DualWriteHistoryStore + shared-mode reader paths.
    parent_settings_raw = (
        (parent_run.get("settings_json") or parent_run.get("settings") or {}) if parent_run else {}
    )
    if isinstance(parent_settings_raw, str):
        try:
            import json as _json

            parent_settings_raw = _json.loads(parent_settings_raw) or {}
        except Exception:
            parent_settings_raw = {}
    parent_settings: dict[str, Any] = (
        parent_settings_raw if isinstance(parent_settings_raw, dict) else {}
    )
    parent_database = (
        (parent_run.get("database") or parent_settings.get("database") or "") if parent_run else ""
    )
    parent_catalog = (
        (parent_run.get("catalog") or parent_settings.get("catalog") or "") if parent_run else ""
    )
    cached = hs.lookup_run_context_cache(
        db_profile=db_profile_name,
        database=parent_database,
        schema=schema,
        table=table,
    )
    db_profile_dict: dict[str, Any] | None = None
    existing_metadata: dict[str, Any] | None = None
    cached_rag_hits: list[dict[str, Any]] = []
    cached_code_hits: list[dict[str, Any]] = []
    if cached and isinstance(cached.get("payload"), dict):
        cached_payload = cached["payload"]
        cached_db_profile = cached_payload.get("db_profile")
        cached_existing = cached_payload.get("existing_metadata")
        cached_rag = cached_payload.get("rag_hits")
        if isinstance(cached_rag, list):
            cached_rag_hits = [h for h in cached_rag if isinstance(h, dict)]
        cached_code = cached_payload.get("code_hits")
        if isinstance(cached_code, list):
            cached_code_hits = [h for h in cached_code if isinstance(h, dict)]
        if isinstance(cached_db_profile, dict) and isinstance(cached_existing, dict):
            db_profile_dict = _slice_cached_profile(
                cached_db_profile, only_column=column if column else None
            )
            existing_metadata = dict(cached_existing)
            log.info(
                "rerun: cache hit for %s.%s -- skipping live profile_table",
                schema,
                table,
            )

    if db_profile_dict is None or existing_metadata is None:
        # Forward the parent run's database + catalog (resolved above
        # via the settings_json fallback) so the connector points at
        # the same scope the original /run targeted.
        db = _connector_for_db_profile(
            cfg,
            db_profile_name,
            database=parent_database or None,
            catalog=parent_catalog or None,
        )
        try:
            db_profile_dict, existing_metadata = _build_db_profile_dict(
                db,
                schema,
                table,
                asset_kind=asset_kind,
                only_column=column if column else None,
            )
        finally:
            try:
                db.close()
            except Exception:
                pass

    payload["db_profile"] = db_profile_dict
    payload["existing_metadata"] = existing_metadata
    if cached_rag_hits:
        payload["rag_hits"] = cached_rag_hits
    if cached_code_hits:
        payload["code_hits"] = cached_code_hits

    snapshot_id = uuid.uuid4().hex
    hs.save_rerun_snapshot(
        snapshot_id=snapshot_id,
        job_id=job_id,
        target_result_id=int(target_result_id),
        payload=payload,
    )
    return snapshot_id


def hydrate_context(payload: dict[str, Any]) -> AgentContext:
    """Re-inflate an ``AgentContext`` from a snapshot payload dict.

    Keeps ``user_instructions`` populated so the agent prompt suffixes
    fire automatically without any re-run-specific code in the agents.
    """
    return AgentContext(
        schema=str(payload.get("schema") or ""),
        table=str(payload.get("table") or ""),
        column=payload.get("column"),
        asset_kind=str(payload.get("asset_kind") or "table"),
        db_profile=dict(payload.get("db_profile") or {}),
        rag_context=list(payload.get("rag_context") or []),
        rag_hits=[h for h in (payload.get("rag_hits") or []) if isinstance(h, dict)],
        code_hits=[h for h in (payload.get("code_hits") or []) if isinstance(h, dict)],
        code_context=list(payload.get("code_context") or []),
        existing_metadata=dict(payload.get("existing_metadata") or {}),
        user_instructions=str(payload.get("user_instructions") or ""),
    )


def serialize_context(ctx: AgentContext) -> dict[str, Any]:
    """JSON-serializable dict for storing an ``AgentContext`` in a snapshot.

    Used by tests and any path that wants to round-trip a context the
    same way the live snapshot writer does.
    """
    return asdict(ctx)


def cache_table_profile(
    *,
    profile: Any,
    db: DatabaseConnector,
    db_profile_name: str,
    database: str,
    run_id: int | None = None,
    rag_hits: list[dict[str, Any]] | None = None,
    code_hits: list[dict[str, Any]] | None = None,
) -> bool:
    """Persist the freshly-built table profile for re-use on re-run.

    Called from the analyze path right after ``db.profile_table``
    succeeds. Cache hits in :func:`build_context_snapshot` skip the
    live profile rebuild — saving 5-30s per re-run target.

    Returns ``True`` when the row was written, ``False`` on any
    exception (best-effort: a failed cache write must never break the
    analyze loop).
    """
    hs = history_store()
    if hs is None:
        return False
    try:
        db_profile_dict, existing_metadata = _table_profile_to_dicts(profile, db)
        payload: dict[str, Any] = {
            "db_profile": db_profile_dict,
            "existing_metadata": existing_metadata,
        }
        if rag_hits:
            # Persist a lean projection of each hit — just the fields
            # the re-run replay path needs to reconstruct the prompt.
            # Dropping the raw distance/score keeps the JSON column
            # small without losing provenance.
            payload["rag_hits"] = [_serialize_rag_hit(h) for h in rag_hits if h]
        if code_hits:
            payload["code_hits"] = [_serialize_code_hit(h) for h in code_hits if h]
        hs.save_run_context_cache(
            db_profile=str(db_profile_name or ""),
            database=str(database or ""),
            schema=str(profile.schema or ""),
            table=str(profile.name or ""),
            payload=payload,
            source_run_id=run_id,
        )
        return True
    except Exception as exc:  # pragma: no cover - best-effort
        log.debug(
            "cache_table_profile failed for %s.%s: %s",
            getattr(profile, "schema", "?"),
            getattr(profile, "name", "?"),
            exc,
        )
        return False


__all__ = [
    "RerunContextError",
    "build_context_snapshot",
    "cache_table_profile",
    "hydrate_context",
    "serialize_context",
    "_serialize_code_hit",
    "_serialize_rag_hit",
]
