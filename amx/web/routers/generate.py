"""Per-asset LLM-driven generate endpoints.

Each route runs ONE focused LLM call for a single asset
(database/schema/table/column) and routes the result through the
same history + pending-review path the bulk ``analyze.run`` worker
uses. The generated description does NOT land on the live database
until the user approves it from the Pending page — every generate,
single-shot or bulk, goes through human-in-the-loop.

Response shape: ``{description, run_id, result_id}``.
- ``description`` is what was generated (for an instant preview).
- ``run_id`` ties the call back to ``analysis_runs`` so the user can
  find it on the Runs page.
- ``result_id`` is the matching ``run_results`` row id and the same
  id stored on the pending queue entry — the SPA uses it to scroll
  the Pending page to the freshly-queued row.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status

from amx.agents.base import Confidence
from amx.agents.orchestrator import ReviewResult
from amx.config import AMXConfig
from amx.db.connector import DatabaseConnector
from amx.llm.provider import LLMProvider
from amx.pending_review import load_pending, save_pending
from amx.storage.sqlite_store import history_store
from amx.utils.logging import get_logger
from amx.web.deps import get_cfg

router = APIRouter(prefix="/api/generate", tags=["generate"])
log = get_logger("web.generate")

_SYSTEM = (
    "You write concise, factual database asset descriptions. Reply "
    "with one sentence, 8-22 words, no preamble, no quotes. State "
    "what the asset stores or represents — nothing else."
)


def _llm(cfg: AMXConfig) -> LLMProvider:
    if not cfg.llm or not cfg.llm.provider or not cfg.llm.model:
        raise HTTPException(
            status_code=status.HTTP_412_PRECONDITION_FAILED,
            detail="No active LLM profile. Activate one under Settings → LLM first.",
        )
    return LLMProvider(cfg.llm)


def _connector(cfg: AMXConfig) -> DatabaseConnector:
    return DatabaseConnector(cfg.db)


def _generate(llm: LLMProvider, prompt: str) -> str:
    """Run the LLM, strip surrounding quotes/whitespace, return one
    line. Falls back to a 502 if the provider returns nothing."""
    try:
        result = llm.chat(
            messages=[
                {"role": "system", "content": _SYSTEM},
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
            use_logprobs=False,
        )
    except Exception as exc:  # provider-specific exceptions are noisy
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"LLM call failed: {exc.__class__.__name__}: {exc}",
        ) from exc
    text = (result.content or "").strip().strip('"').strip("'").strip()
    # Some models still wrap their reply in a "Description: …" prefix.
    for prefix in ("Description:", "DESCRIPTION:", "description:"):
        if text.startswith(prefix):
            text = text[len(prefix) :].strip()
    if not text:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="LLM returned an empty description.",
        )
    # Guarantee a single line and a sane upper bound.
    text = text.replace("\n", " ").strip()
    if len(text) > 600:
        text = text[:600].rstrip() + "…"
    return text


def _record_and_queue(
    cfg: AMXConfig,
    *,
    command: str,
    description: str,
    schema: str,
    table: str,
    column: str | None,
    asset_kind: str,
) -> dict[str, Any]:
    """Persist a generated description through history + pending queue.

    Mirrors what ``_run_worker`` does for the bulk path: open a fresh
    ``analysis_runs`` row, save the LLM output as a single
    ``run_results`` entry, mark the run ``ready_for_review``, and append
    a :class:`ReviewResult` to ``~/.amx/pending_metadata.json`` so the
    SPA's Pending page picks it up. The live database is NOT touched —
    the user approves from /pending and the existing apply worker does
    the writeback.
    """
    response: dict[str, Any] = {
        "description": description,
        "run_id": None,
        "result_id": None,
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

    try:
        run_id = hs.create_run(
            command=command,
            mode="chat",
            db_backend=cfg.db.backend,
            db_profile=cfg.active_db_profile,
            llm_provider=cfg.llm.provider,
            llm_model=cfg.llm.model,
            scope=scope,
            selected_count=selected,
            planned_count=selected,
            review_strategy="individual",
            llm_profile=cfg.active_llm_profile,
            doc_profile=cfg.active_doc_profile or None,
            code_profile=cfg.active_code_profile or None,
            settings={"trigger": "visualizer.generate.singleshot"},
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
                    "alternatives": [description],
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
        final_description=description,
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


@router.post("/database")
def generate_database(cfg: AMXConfig = Depends(get_cfg)) -> dict[str, Any]:
    db = _connector(cfg)
    try:
        schemas = db.list_schemas()
    except Exception:
        schemas = []
    schema_summary = ", ".join(schemas[:20]) or "(no schemas reachable)"
    db_label = cfg.active_db_profile or "this database"
    prompt = (
        f"Database '{db_label}' contains the schemas: {schema_summary}. "
        "Describe the database's overall purpose."
    )
    description = _generate(_llm(cfg), prompt)
    return _record_and_queue(
        cfg,
        command="generate.database",
        description=description,
        schema="",
        table="",
        column=None,
        asset_kind="database",
    )


@router.post("/schema/{schema}")
def generate_schema(
    schema: str,
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    db = _connector(cfg)
    try:
        assets = db.list_assets(schema)
    except Exception:
        assets = []
    table_names = [name if isinstance(name, str) else name[0] for name in assets[:20]]
    table_summary = ", ".join(table_names) or "(no tables reachable)"
    prompt = (
        f"Schema '{schema}' contains the tables: {table_summary}. Describe the schema's purpose."
    )
    description = _generate(_llm(cfg), prompt)
    return _record_and_queue(
        cfg,
        command="generate.schema",
        description=description,
        schema=schema,
        table="",
        column=None,
        asset_kind="schema",
    )


@router.post("/table/{schema}/{table}")
def generate_table(
    schema: str,
    table: str,
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    db = _connector(cfg)
    try:
        cols = db.list_column_profiles(schema, table)
    except Exception:
        cols = []
    col_summary = (
        ", ".join(f"{c.name} ({c.dtype})" for c in cols[:25]) or "(no introspectable columns)"
    )
    prompt = (
        f"Table '{schema}.{table}' has the columns: {col_summary}. "
        "Describe what one row in this table represents."
    )
    description = _generate(_llm(cfg), prompt)
    return _record_and_queue(
        cfg,
        command="generate.table",
        description=description,
        schema=schema,
        table=table,
        column=None,
        asset_kind="table",
    )


@router.post("/column/{schema}/{table}/{column}")
def generate_column(
    schema: str,
    table: str,
    column: str,
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    db = _connector(cfg)
    try:
        cols = db.list_column_profiles(schema, table)
    except Exception:
        cols = []
    col = next((c for c in cols if c.name == column), None)
    dtype = col.dtype if col else "unknown"
    nullable = "nullable" if (col and col.nullable) else "required"
    table_comment = ""
    try:
        table_comment = (db.get_table_comment(schema, table) or "").strip()
    except Exception:
        table_comment = ""
    sibling_summary = ", ".join(c.name for c in cols if c.name != column)[:200] if cols else ""
    parts = [f"Column '{schema}.{table}.{column}' (type {dtype}, {nullable})."]
    if table_comment:
        parts.append(f"The table is: {table_comment}.")
    if sibling_summary:
        parts.append(f"Other columns: {sibling_summary}.")
    parts.append("Describe what this column stores.")
    prompt = " ".join(parts)
    description = _generate(_llm(cfg), prompt)
    return _record_and_queue(
        cfg,
        command="generate.column",
        description=description,
        schema=schema,
        table=table,
        column=column,
        asset_kind="column",
    )


__all__ = ["router"]
