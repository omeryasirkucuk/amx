"""Per-asset LLM-driven generate endpoints.

Each route runs ONE focused LLM call for a single asset
(database/schema/table/column) and routes the result through the
same history + pending-review path the bulk ``analyze.run`` worker
uses. The generated description does NOT land on the live database
until the user approves it from the Pending page — every generate,
single-shot or bulk, goes through human-in-the-loop.

The four user-configurable knobs from Settings → LLM are honoured:

* ``temperature`` — passed straight to the LLM call.
* ``n_alternatives`` (1–5) — when >1, the model is asked for the
  requested number of alternatives in a structured response and each
  one becomes a row in ``run_results.alternatives_json``. The Pending
  page renders them as A/B/C buttons; the user picks before approving.
* ``description_verbosity`` — controls the length-rule appended to
  the system prompt (``brief | detailed | comprehensive | exhaustive``)
  via the shared :mod:`amx.llm.prompts.length` helper, so single-shot
  and bulk runs produce equally long descriptions for the same preset.
* ``prompt_detail`` (``PromptDetail`` flags) — gates which
  schema/table/column metadata is folded into the user prompt.
  Metadata-only signals (existing comments, PK/FK, schema/db comments)
  are honoured fast. Data-scan signals (samples / min-max / cardinality
  / null counts / usage stats) trigger a one-shot
  :meth:`DatabaseConnector.profile_table` call; if that fails or is
  unavailable the prompt falls back to the metadata-only path so
  generation never hard-blocks on profiling.

Response shape: ``{description, run_id, result_id, alternatives_count, verbosity}``.
- ``description`` is the chosen alternative (the first one) for an instant preview.
- ``alternatives_count`` is how many alternatives landed in the queue —
  the SPA uses it to render a different toast when N>1.
- ``verbosity`` echoes the active preset so the toast can mention it.
- ``run_id`` ties the call back to ``analysis_runs`` so the user can
  find it on the Runs page.
- ``result_id`` is the matching ``run_results`` row id and the same
  id stored on the pending queue entry — the SPA uses it to scroll
  the Pending page to the freshly-queued row.
"""

from __future__ import annotations

import re
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, status

from amx.agents.base import Confidence
from amx.agents.orchestrator import ReviewResult
from amx.config import AMXConfig, PromptDetail
from amx.db.connector import ColumnProfile, DatabaseConnector, TableProfile
from amx.llm.prompts import length_rule
from amx.llm.provider import LLMProvider
from amx.pending_review import load_pending, save_pending
from amx.storage.sqlite_store import history_store
from amx.utils.logging import get_logger
from amx.web.deps import get_cfg
from amx.web.routers.live_db import _connector_for_scope

router = APIRouter(prefix="/api/generate", tags=["generate"])
log = get_logger("web.generate")

_SYSTEM_BASE = (
    "You write factual database asset descriptions. State what the "
    "asset stores or represents, grounded in the provided evidence. "
    "Reply with no preamble, no quotes, no markdown — just the "
    "description text."
)

_DESCRIPTION_LINE_RE = re.compile(r"^\s*DESCRIPTION_\d+\s*:\s*(.+?)\s*$", re.IGNORECASE)


def _llm(cfg: AMXConfig) -> LLMProvider:
    if not cfg.llm or not cfg.llm.provider or not cfg.llm.model:
        raise HTTPException(
            status_code=status.HTTP_412_PRECONDITION_FAILED,
            detail="No active LLM profile. Activate one under Settings → LLM first.",
        )
    return LLMProvider(cfg.llm)


def _resolve_generate_connector(
    cfg: AMXConfig,
    profile: str,
    database: str | None,
    catalog: str | None,
) -> tuple[DatabaseConnector, str, str]:
    """Resolve a connector + return the profile name + DB-backend label
    used in prompts so generated descriptions reference the requested
    profile (never ``cfg.active_db_profile``).
    """
    conn = _connector_for_scope(cfg, profile, database=database, catalog=catalog)
    base = cfg.db_profiles.get(profile.strip())
    backend = getattr(base, "backend", "") if base else ""
    return conn, profile.strip(), backend


def _settings(cfg: AMXConfig) -> tuple[int, str, float, PromptDetail]:
    """Read the four user-configurable knobs that shape generation.

    Defaults match a fresh install: 1 alternative, brief, temp 0.2,
    standard prompt_detail. Values are clamped to safe ranges so an
    accidentally-edited config can't break the LLM call.
    """
    llm = cfg.llm
    n = max(1, min(5, int(getattr(llm, "n_alternatives", 1) or 1)))
    verbosity = (getattr(llm, "description_verbosity", "brief") or "brief").lower().strip()
    temperature = float(getattr(llm, "temperature", 0.2) or 0.2)
    pd = cfg.llm.prompt_detail_cfg
    return n, verbosity, temperature, pd


def _build_system_prompt(n: int, verbosity: str) -> str:
    rule = length_rule(verbosity)
    base = f"{_SYSTEM_BASE} {rule}"
    if n <= 1:
        return base
    label_lines = "\n".join(f"DESCRIPTION_{i}: <text>" for i in range(1, n + 1))
    return (
        f"{base}\n"
        f"Provide exactly {n} alternative descriptions ranked by likelihood. "
        "Use this format, one alternative per line, no preamble:\n"
        f"{label_lines}"
    )


def _clean_description(text: str) -> str:
    """Trim quotes/prefixes/whitespace from a single-description response."""
    text = (text or "").strip().strip('"').strip("'").strip()
    for prefix in ("Description:", "DESCRIPTION:", "description:"):
        if text.startswith(prefix):
            text = text[len(prefix) :].strip()
    text = text.replace("\n", " ").strip()
    if len(text) > 600:
        text = text[:600].rstrip() + "…"
    return text


def _parse_alternatives(text: str, n: int) -> list[str]:
    """Extract up to N alternatives from an LLM response.

    For ``n == 1`` the response is treated as a single description and
    cleaned in-place. For ``n >= 2`` the response is scanned for
    ``DESCRIPTION_<digit>:`` lines; if at least one is found that's
    what we use. If the model ignores the format and returns a single
    blob, we degrade gracefully and keep that blob as the only
    alternative — generation never hard-fails when only the format is
    off.

    Empty / duplicate alternatives are dropped. Order is preserved.
    """
    if n <= 1:
        cleaned = _clean_description(text)
        return [cleaned] if cleaned else []

    found: list[str] = []
    for line in (text or "").splitlines():
        m = _DESCRIPTION_LINE_RE.match(line)
        if not m:
            continue
        cleaned = _clean_description(m.group(1))
        if cleaned and cleaned not in found:
            found.append(cleaned)
        if len(found) >= n:
            break

    if found:
        return found

    cleaned = _clean_description(text)
    return [cleaned] if cleaned else []


def _generate(
    llm: LLMProvider,
    user_prompt: str,
    *,
    n: int = 1,
    verbosity: str = "brief",
    temperature: float = 0.2,
) -> list[str]:
    """Run the LLM and return up to N description alternatives."""
    system_prompt = _build_system_prompt(n, verbosity)
    try:
        result = llm.chat(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=temperature,
            use_logprobs=False,
        )
    except Exception as exc:  # provider-specific exceptions are noisy
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"LLM call failed: {exc.__class__.__name__}: {exc}",
        ) from exc
    alternatives = _parse_alternatives(result.content or "", n)
    if not alternatives:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="LLM returned an empty description.",
        )
    return alternatives


# ── Prompt-detail helpers ──────────────────────────────────────────────


def _wants_data_signals(pd: PromptDetail) -> bool:
    """Whether the prompt_detail preset asks for any signal that
    requires a table profiling pass (samples / min-max / cardinality /
    null counts / usage stats). Used to decide whether a single-shot
    generate should call the heavier ``profile_table`` path.
    """
    return bool(
        pd.include_samples
        or pd.include_min_max
        or pd.include_cardinality
        or pd.include_null_counts
        or pd.include_usage_stats
    )


def _safe_profile_table(db: DatabaseConnector, schema: str, table: str) -> TableProfile | None:
    """Profile a single table for prompt enrichment, swallowing errors.

    Single-shot generate must not hard-fail because of a profiling
    glitch (permissions, slow scan, backend quirk). On any exception
    we fall back to the metadata-only path.
    """
    try:
        return db.profile_table(schema, table)
    except Exception as exc:  # pragma: no cover — defensive
        log.warning("profile_table(%s.%s) failed; falling back: %s", schema, table, exc)
        return None


def _format_samples(values: list[Any], max_n: int) -> str:
    if not values:
        return ""
    sample_strs: list[str] = []
    for v in values[:max_n]:
        s = str(v)
        if len(s) > 60:
            s = s[:57] + "…"
        sample_strs.append(s)
    return ", ".join(sample_strs)


# ── User-prompt builders (one per endpoint) ────────────────────────────


def _build_database_prompt(
    db: DatabaseConnector,
    db_label: str,
    pd: PromptDetail,
) -> str:
    try:
        schemas = db.list_schemas()
    except Exception:
        schemas = []
    schema_summary = ", ".join(schemas[:20]) or "(no schemas reachable)"
    parts = [f"Database '{db_label}' contains the schemas: {schema_summary}."]

    if pd.include_schema_db_comments:
        try:
            db_comment = (db.get_database_comment() or "").strip()
        except Exception:
            db_comment = ""
        if db_comment:
            parts.append(f"Existing database comment: {db_comment}.")

    if pd.include_related_comments and schemas:
        schema_comments: list[str] = []
        for s in schemas[:10]:
            try:
                c = (db.get_schema_comment(s) or "").strip()
            except Exception:
                c = ""
            if c:
                schema_comments.append(f"{s}: {c}")
        if schema_comments:
            parts.append("Schema comments — " + "; ".join(schema_comments) + ".")

    parts.append("Describe the database's overall purpose.")
    return " ".join(parts)


def _build_schema_prompt(
    db: DatabaseConnector,
    schema: str,
    pd: PromptDetail,
) -> str:
    try:
        assets = db.list_assets(schema)
    except Exception:
        assets = []
    table_names = [name if isinstance(name, str) else name[0] for name in assets[:20]]
    table_summary = ", ".join(table_names) or "(no tables reachable)"
    parts = [f"Schema '{schema}' contains the tables: {table_summary}."]

    if pd.include_schema_db_comments:
        try:
            schema_comment = (db.get_schema_comment(schema) or "").strip()
        except Exception:
            schema_comment = ""
        if schema_comment:
            parts.append(f"Existing schema comment: {schema_comment}.")

    if pd.include_existing_col_comment and table_names:
        table_comments: list[str] = []
        for t in table_names[:10]:
            try:
                c = (db.get_table_comment(schema, t) or "").strip()
            except Exception:
                c = ""
            if c:
                table_comments.append(f"{t}: {c}")
        if table_comments:
            parts.append("Table comments — " + "; ".join(table_comments) + ".")

    parts.append("Describe the schema's purpose.")
    return " ".join(parts)


def _build_table_prompt(
    db: DatabaseConnector,
    schema: str,
    table: str,
    pd: PromptDetail,
) -> str:
    try:
        cols = db.list_column_profiles(schema, table)
    except Exception:
        cols = []

    profile: TableProfile | None = None
    if _wants_data_signals(pd) or pd.include_pk_fk or pd.include_unique_check:
        profile = _safe_profile_table(db, schema, table)
        if profile is not None and profile.columns:
            cols = profile.columns

    col_summary = (
        ", ".join(f"{c.name} ({c.dtype})" for c in cols[:25]) or "(no introspectable columns)"
    )
    parts = [f"Table '{schema}.{table}' has the columns: {col_summary}."]

    if pd.include_existing_col_comment:
        try:
            table_comment = (db.get_table_comment(schema, table) or "").strip()
        except Exception:
            table_comment = ""
        if table_comment:
            parts.append(f"Existing table comment: {table_comment}.")

    if profile is not None:
        if pd.include_pk_fk:
            if profile.primary_key:
                parts.append(f"Primary key: {', '.join(profile.primary_key)}.")
            if profile.foreign_keys:
                fk_lines = []
                for fk in profile.foreign_keys[:5]:
                    cols_ = ",".join(fk.get("constrained_columns") or [])
                    target = fk.get("referred_table") or ""
                    if cols_ and target:
                        fk_lines.append(f"{cols_}→{target}")
                if fk_lines:
                    parts.append("Foreign keys: " + "; ".join(fk_lines) + ".")
        if pd.include_unique_check and profile.unique_constraints:
            uc = ["(" + ", ".join(c) + ")" for c in profile.unique_constraints[:3]]
            parts.append("Unique constraints: " + "; ".join(uc) + ".")
        if pd.include_usage_stats and (profile.stats_seq_scan or profile.stats_idx_scan):
            parts.append(
                f"Usage stats: seq_scan={profile.stats_seq_scan}, "
                f"idx_scan={profile.stats_idx_scan}, "
                f"row_count≈{profile.stats_n_live_tup}."
            )
        if pd.include_samples or pd.include_min_max or pd.include_cardinality:
            evidence_lines: list[str] = []
            for col in profile.columns[:10]:
                bits: list[str] = []
                if pd.include_samples and col.samples:
                    bits.append(f"samples: {_format_samples(col.samples, pd.max_samples)}")
                if pd.include_min_max and col.min_val is not None and col.max_val is not None:
                    bits.append(f"range: {col.min_val} → {col.max_val}")
                if pd.include_cardinality and col.distinct_count:
                    bits.append(
                        f"distinct={col.distinct_count} (ratio {col.cardinality_ratio:.2f})"
                    )
                if bits:
                    evidence_lines.append(f"{col.name} — " + "; ".join(bits))
            if evidence_lines:
                parts.append("Column evidence: " + " | ".join(evidence_lines) + ".")

    parts.append("Describe what one row in this table represents.")
    return " ".join(parts)


def _build_column_prompt(
    db: DatabaseConnector,
    schema: str,
    table: str,
    column: str,
    pd: PromptDetail,
) -> str:
    try:
        cols = db.list_column_profiles(schema, table)
    except Exception:
        cols = []

    profile: TableProfile | None = None
    profiled_col: ColumnProfile | None = None
    if _wants_data_signals(pd) or pd.include_pk_fk:
        profile = _safe_profile_table(db, schema, table)
        if profile is not None:
            profiled_col = next((c for c in profile.columns if c.name == column), None)

    base_col = profiled_col or next((c for c in cols if c.name == column), None)
    dtype = base_col.dtype if base_col else "unknown"
    nullable = "nullable" if (base_col and base_col.nullable) else "required"
    parts = [f"Column '{schema}.{table}.{column}' (type {dtype}, {nullable})."]

    if pd.include_existing_col_comment:
        try:
            table_comment = (db.get_table_comment(schema, table) or "").strip()
        except Exception:
            table_comment = ""
        if table_comment:
            parts.append(f"The table is: {table_comment}.")

    sibling_summary = ", ".join(c.name for c in cols if c.name != column)[:200] if cols else ""
    if sibling_summary:
        parts.append(f"Other columns: {sibling_summary}.")

    if profile is not None and profiled_col is not None:
        if pd.include_pk_fk:
            if column in (profile.primary_key or []):
                parts.append("This column is part of the primary key.")
            for fk in profile.foreign_keys or []:
                if column in (fk.get("constrained_columns") or []):
                    target = fk.get("referred_table") or ""
                    if target:
                        parts.append(f"This column references {target}.")
                    break
        if pd.include_samples and profiled_col.samples:
            parts.append(f"Sample values: {_format_samples(profiled_col.samples, pd.max_samples)}.")
        if (
            pd.include_min_max
            and profiled_col.min_val is not None
            and profiled_col.max_val is not None
        ):
            parts.append(f"Range: {profiled_col.min_val} → {profiled_col.max_val}.")
        if pd.include_null_counts and profile.row_count:
            parts.append(f"Null count: {profiled_col.null_count} / {profile.row_count} rows.")
        if pd.include_cardinality and profiled_col.distinct_count:
            parts.append(
                f"Distinct values: {profiled_col.distinct_count} "
                f"(cardinality ratio {profiled_col.cardinality_ratio:.2f})."
            )

    parts.append("Describe what this column stores.")
    return " ".join(parts)


# ── Persistence ────────────────────────────────────────────────────────


def _record_and_queue(
    cfg: AMXConfig,
    *,
    command: str,
    alternatives: list[str],
    verbosity: str,
    schema: str,
    table: str,
    column: str | None,
    asset_kind: str,
    db_profile: str | None = None,
    db_backend: str | None = None,
) -> dict[str, Any]:
    """Persist generated alternatives through history + pending queue.

    Mirrors what ``_run_worker`` does for the bulk path: open a fresh
    ``analysis_runs`` row, save the LLM output (with its full
    alternatives list) as a single ``run_results`` entry, mark the run
    ``ready_for_review``, and append a :class:`ReviewResult` to
    ``~/.amx/pending_metadata.json`` so the SPA's Pending page picks
    it up.

    The first alternative is the default chosen one; the user can
    swap to a different alternative on the Pending page (which fires
    ``PATCH /api/pending/{idx}`` to update ``final_description``)
    before approving. The live database is NOT touched until that
    approval — the existing apply worker handles the writeback.
    """
    if not alternatives:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="LLM returned an empty description.",
        )
    primary = alternatives[0]

    response: dict[str, Any] = {
        "description": primary,
        "run_id": None,
        "result_id": None,
        "alternatives_count": len(alternatives),
        "verbosity": verbosity,
    }
    scope = {schema: [table] if table else []} if schema else {}
    selected = 1
    hs = history_store()
    if hs is None:
        # No history store available — surface a clear error rather
        # than silently dropping the generation. Callers can decide
        # whether to retry or fall back, but on a fresh install this
        # path effectively means "history-store wasn't initialised",
        # which is a configuration bug worth knowing about.
        log.warning("history_store() returned None; generated text not recorded")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=(
                "History store is not initialised. The generated description "
                "was not persisted. Open Settings → History to enable it, then "
                "retry."
            ),
        )

    effective_profile = (db_profile or cfg.active_db_profile) or None
    effective_backend = db_backend or (cfg.db.backend if cfg.db else None)
    try:
        run_id = hs.create_run(
            command=command,
            mode="chat",
            db_backend=effective_backend,
            db_profile=effective_profile,
            llm_provider=cfg.llm.provider,
            llm_model=cfg.llm.model,
            scope=scope,
            selected_count=selected,
            planned_count=selected,
            review_strategy="individual",
            llm_profile=cfg.active_llm_profile,
            doc_profile=cfg.active_doc_profile or None,
            code_profile=cfg.active_code_profile or None,
            settings={"trigger": "studio.generate.singleshot"},
        )
    except Exception as exc:  # pragma: no cover — DB-layer failure
        log.warning("Could not record generate run: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Could not persist run history: {exc.__class__.__name__}: {exc}",
        ) from exc

    response["run_id"] = int(run_id)

    try:
        result_ids = hs.save_run_results(
            run_id,
            [
                {
                    "schema": schema,
                    "table": table,
                    "column": column,
                    "asset_kind": asset_kind,
                    "source": "generate.singleshot",
                    "confidence": Confidence.MEDIUM.value,
                    "alternatives": alternatives,
                    "reasoning": "",
                }
            ],
        )
    except Exception as exc:  # pragma: no cover
        log.warning("Could not save run_results for run %s: %s", run_id, exc)
        result_ids = []

    if result_ids:
        response["result_id"] = int(result_ids[0])

    try:
        hs.update_run_status(int(run_id), "ready_for_review")
    except Exception as exc:  # pragma: no cover
        log.warning("Could not flip run %s to ready_for_review: %s", run_id, exc)

    rr = ReviewResult(
        schema=schema,
        table=table,
        column=column,
        final_description=primary,
        confidence=Confidence.MEDIUM,
        source="generate.singleshot",
        applied=True,
        asset_kind=asset_kind,
        result_id=response["result_id"],
    )
    try:
        rows = load_pending()
        rows.append(rr)
        save_pending(rows)
    except Exception as exc:  # pragma: no cover
        log.warning("Could not append generate result to pending queue: %s", exc)

    return response


# ── Endpoints ──────────────────────────────────────────────────────────


@router.post("/database")
def generate_database(
    profile: str = Query(...),
    database: str | None = Query(default=None),
    catalog: str | None = Query(default=None),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    db, db_label, backend = _resolve_generate_connector(cfg, profile, database, catalog)
    n, verbosity, temperature, pd = _settings(cfg)
    user_prompt = _build_database_prompt(db, db_label, pd)
    alternatives = _generate(
        _llm(cfg), user_prompt, n=n, verbosity=verbosity, temperature=temperature
    )
    return _record_and_queue(
        cfg,
        command="generate.database",
        alternatives=alternatives,
        verbosity=verbosity,
        schema="",
        table="",
        column=None,
        asset_kind="database",
        db_profile=db_label,
        db_backend=backend or None,
    )


@router.post("/schema/{schema}")
def generate_schema(
    schema: str,
    profile: str = Query(...),
    database: str | None = Query(default=None),
    catalog: str | None = Query(default=None),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    db, db_label, backend = _resolve_generate_connector(cfg, profile, database, catalog)
    n, verbosity, temperature, pd = _settings(cfg)
    user_prompt = _build_schema_prompt(db, schema, pd)
    alternatives = _generate(
        _llm(cfg), user_prompt, n=n, verbosity=verbosity, temperature=temperature
    )
    return _record_and_queue(
        cfg,
        command="generate.schema",
        alternatives=alternatives,
        verbosity=verbosity,
        schema=schema,
        table="",
        column=None,
        asset_kind="schema",
        db_profile=db_label,
        db_backend=backend or None,
    )


@router.post("/table/{schema}/{table}")
def generate_table(
    schema: str,
    table: str,
    profile: str = Query(...),
    database: str | None = Query(default=None),
    catalog: str | None = Query(default=None),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    db, db_label, backend = _resolve_generate_connector(cfg, profile, database, catalog)
    n, verbosity, temperature, pd = _settings(cfg)
    user_prompt = _build_table_prompt(db, schema, table, pd)
    alternatives = _generate(
        _llm(cfg), user_prompt, n=n, verbosity=verbosity, temperature=temperature
    )
    return _record_and_queue(
        cfg,
        command="generate.table",
        alternatives=alternatives,
        verbosity=verbosity,
        schema=schema,
        table=table,
        column=None,
        asset_kind="table",
        db_profile=db_label,
        db_backend=backend or None,
    )


@router.post("/column/{schema}/{table}/{column}")
def generate_column(
    schema: str,
    table: str,
    column: str,
    profile: str = Query(...),
    database: str | None = Query(default=None),
    catalog: str | None = Query(default=None),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    db, db_label, backend = _resolve_generate_connector(cfg, profile, database, catalog)
    n, verbosity, temperature, pd = _settings(cfg)
    user_prompt = _build_column_prompt(db, schema, table, column, pd)
    alternatives = _generate(
        _llm(cfg), user_prompt, n=n, verbosity=verbosity, temperature=temperature
    )
    return _record_and_queue(
        cfg,
        command="generate.column",
        alternatives=alternatives,
        verbosity=verbosity,
        schema=schema,
        table=table,
        column=column,
        asset_kind="column",
        db_profile=db_label,
        db_backend=backend or None,
    )


__all__ = ["router"]
