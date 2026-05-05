"""Per-asset LLM-driven generate endpoints.

Each route runs ONE focused LLM call for a single asset
(database/schema/table/column), writes the resulting description
back via the existing connector setters, and returns the new text
synchronously. Mirrors what the analyze.run worker would do for a
single asset, minus the run-history bookkeeping and the pending
queue indirection — the user is asking AMX to generate one piece
of metadata, so we just generate it and write it.

Bulk generation (``POST /api/runs`` with a wide scope) remains the
canonical path for filling in many assets at once. These endpoints
exist so a user reviewing one row can hit "Generate" without
spawning a multi-asset job.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status

from amx.config import AMXConfig
from amx.db.connector import AssetKind, DatabaseConnector
from amx.llm.provider import LLMProvider
from amx.web.deps import get_cfg

router = APIRouter(prefix="/api/generate", tags=["generate"])

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
            text = text[len(prefix):].strip()
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


def _write_or_400(action: str, fn) -> None:
    try:
        fn()
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"{action} failed: {exc.__class__.__name__}: {exc}",
        ) from exc


@router.post("/database")
def generate_database(cfg: AMXConfig = Depends(get_cfg)) -> dict[str, str]:
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
    _write_or_400("Setting database comment", lambda: db.set_database_comment(description))
    return {"description": description}


@router.post("/schema/{schema}")
def generate_schema(
    schema: str,
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, str]:
    db = _connector(cfg)
    try:
        assets = db.list_assets(schema)
    except Exception:
        assets = []
    table_names = [name if isinstance(name, str) else name[0] for name in assets[:20]]
    table_summary = ", ".join(table_names) or "(no tables reachable)"
    prompt = (
        f"Schema '{schema}' contains the tables: {table_summary}. "
        "Describe the schema's purpose."
    )
    description = _generate(_llm(cfg), prompt)
    _write_or_400(
        f"Setting schema comment on {schema}",
        lambda: db.set_schema_comment(schema, description),
    )
    return {"description": description}


@router.post("/table/{schema}/{table}")
def generate_table(
    schema: str,
    table: str,
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, str]:
    db = _connector(cfg)
    try:
        cols = db.list_column_profiles(schema, table)
    except Exception:
        cols = []
    col_summary = (
        ", ".join(f"{c.name} ({c.dtype})" for c in cols[:25])
        or "(no introspectable columns)"
    )
    prompt = (
        f"Table '{schema}.{table}' has the columns: {col_summary}. "
        "Describe what one row in this table represents."
    )
    description = _generate(_llm(cfg), prompt)
    _write_or_400(
        f"Setting table comment on {schema}.{table}",
        lambda: db.set_table_comment(
            schema, table, description, asset_kind=AssetKind.TABLE
        ),
    )
    return {"description": description}


@router.post("/column/{schema}/{table}/{column}")
def generate_column(
    schema: str,
    table: str,
    column: str,
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, str]:
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
    sibling_summary = (
        ", ".join(c.name for c in cols if c.name != column)[:200]
        if cols
        else ""
    )
    parts = [f"Column '{schema}.{table}.{column}' (type {dtype}, {nullable})."]
    if table_comment:
        parts.append(f"The table is: {table_comment}.")
    if sibling_summary:
        parts.append(f"Other columns: {sibling_summary}.")
    parts.append("Describe what this column stores.")
    prompt = " ".join(parts)
    description = _generate(_llm(cfg), prompt)
    _write_or_400(
        f"Setting column comment on {schema}.{table}.{column}",
        lambda: db.set_column_comment(schema, table, column, description),
    )
    return {"description": description}


__all__ = ["router"]
