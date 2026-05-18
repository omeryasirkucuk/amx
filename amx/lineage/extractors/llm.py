"""On-demand LLM-inferred lineage edges.

The extractor never fires automatically. The service layer calls it
with ``mode="llm_suggest"`` after the user explicitly opts in (CLI
``/lineage suggest`` or Studio "AI suggest" button). The ``cache_only``
mode reads back the previously persisted ``lineage_llm`` edges from
``catalog_relationships`` — no LLM call, no cost. Persistence happens
through :func:`amx.lineage.store.upsert_llm_edges`.
"""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Callable
from typing import Any

from amx.lineage import llm_prompt as prompt_mod
from amx.lineage.types import (
    ColumnRef,
    Edge,
    ExtractMode,
    ExtractResult,
    Scope,
)

# Maximum number of candidate tables fed into the LLM prompt. Bigger
# prompts cost more tokens with diminishing returns — name-prefix +
# co-occurrence ranking already surfaces the relevant subset.
_MAX_CANDIDATES = 30
# Marginal hallucinations clustered around 0.4-0.5 in user testing
# (LLM emitting plausible-sounding but unsupported edges). 0.6 is the
# new floor for an edge to land on the canvas; the prompt now also
# asks the model to honestly calibrate confidence rather than
# default-stamp 0.7+ on every guess.
_MIN_CONFIDENCE = 0.6


# The LLM client surface the extractor depends on. Keeps the extractor
# decoupled from the concrete :class:`LLMProvider` — tests pass a
# fake callable, the CLI/Studio wire up the real provider.
LLMCallable = Callable[[list[dict[str, str]]], str]


class LLMExtractor:
    name = "llm"

    def __init__(
        self,
        *,
        llm_callable: LLMCallable | None = None,
        model_name: str = "",
    ) -> None:
        self._llm = llm_callable
        self._model_name = model_name

    def extract(
        self,
        *,
        hs: Any,
        scope: Scope,
        mode: ExtractMode = "cache_only",
    ) -> ExtractResult:
        anchor = scope.anchor
        if not anchor.schema or not anchor.table:
            return ExtractResult()

        if mode == "cache_only":
            edges = _read_cached_llm_edges(hs, scope)
            return ExtractResult(edges=edges, cache_status="hit")

        # mode == "llm_suggest" — on-demand LLM call.
        if self._llm is None:
            return ExtractResult(
                edges=[],
                cache_status="miss",
            )

        anchor_ctx = _build_anchor_context(hs, scope)
        candidates = _build_candidate_list(hs, scope)
        if not candidates:
            return ExtractResult(edges=[], cache_status="hit")

        approved, rejected = _verdict_examples(hs, scope)
        messages = prompt_mod.build_messages(
            anchor_ctx,
            candidates,
            max_candidates=_MAX_CANDIDATES,
            approved_examples=approved,
            rejected_examples=rejected,
        )
        prompt_hash = hashlib.sha256(
            json.dumps(messages, ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()
        raw_reply = self._llm(messages)
        valid_fqns = {c.fqn for c in candidates}
        suggested = prompt_mod.parse_response(
            raw_reply,
            anchor_fqn=anchor_ctx.fqn,
            valid_candidate_fqns=valid_fqns,
            min_confidence=_MIN_CONFIDENCE,
        )
        edges = list(
            _persist_and_emit_edges(
                hs, scope, suggested, model_name=self._model_name, prompt_hash=prompt_hash
            )
        )
        return ExtractResult(edges=edges, cache_status="hit")


def _build_anchor_context(hs: Any, scope: Scope) -> prompt_mod.AnchorContext:
    """Assemble the full anchor context via the rich-context module.

    Delegates to :mod:`amx.lineage._anchor_context`, which collects
    table/column descriptions, FK partners, view co-mentions, and
    query-log co-occurrence — all signals the LLM needs to ground
    its suggestions instead of guessing from bare column names.
    """
    from amx.lineage._anchor_context import build_rich_context

    rich = build_rich_context(hs, scope)
    fk_partners = [
        {
            "direction": fk.direction,
            "other_fqn": fk.other_fqn,
            "from_column": fk.from_column,
            "to_column": fk.to_column,
        }
        for fk in rich.fk_partners
    ]
    view_references = [
        {"view_fqn": v.view_fqn, "other_tables": v.other_tables} for v in rich.view_references
    ]
    co_occurrence = [
        {"other_fqn": p.other_fqn, "count": p.count} for p in rich.co_occurrence_partners
    ]
    return prompt_mod.AnchorContext(
        fqn=rich.fqn,
        columns=rich.columns,
        description=rich.table_description,
        fk_partners=fk_partners,
        view_references=view_references,
        co_occurrence_partners=co_occurrence,
    )


def _build_candidate_list(hs: Any, scope: Scope) -> list[prompt_mod.CandidateTable]:
    """Return the top ``_MAX_CANDIDATES`` candidates ranked by signal.

    Replaces the prior name-prefix + alphabetical ordering — which on
    SAP-style schemas filled the prompt with sibling tables — with a
    weighted-sum score over FK partnership, view co-mentions, query
    co-occurrence, column-name overlap, and prefix similarity. Each
    candidate carries its score + reason list into the prompt so the
    LLM sees the evidence behind every entry, not just the name.
    """
    from amx.lineage._candidate_ranker import score_candidates

    ranked = score_candidates(hs, scope, max_count=_MAX_CANDIDATES)
    out: list[prompt_mod.CandidateTable] = []
    for c in ranked:
        out.append(
            prompt_mod.CandidateTable(
                fqn=c.fqn,
                columns=_columns_for_table(
                    hs, scope.profile, scope.anchor.database, c.schema, c.table
                ),
                description=_table_description(
                    hs, scope.profile, scope.anchor.database, c.schema, c.table
                ),
                score=c.score,
                reasons=c.reasons,
            )
        )
    return out


def _name_prefix(name: str) -> str:
    """First 3 alpha chars uppercased — heuristic for SAP-style naming."""
    head = "".join(ch for ch in (name or "")[:6] if ch.isalpha())
    return head[:3].upper()


def _columns_for_table(
    hs: Any, profile: str, database: str, schema: str, table: str
) -> list[dict[str, str]]:
    """Read columns from column_comments_cache. Returns [] when uncached."""
    cache_key = f"{profile}|{database}|{schema}|{table}"
    with hs._connect() as conn:
        row = conn.execute(
            "SELECT columns_json FROM column_comments_cache WHERE cache_key = ?",
            (cache_key,),
        ).fetchone()
    if not row or not row[0]:
        return []
    try:
        payload = json.loads(row[0])
    except (TypeError, ValueError):
        return []
    out: list[dict[str, str]] = []
    if isinstance(payload, dict):
        for name, meta in payload.items():
            entry: dict[str, str] = {"name": str(name)}
            if isinstance(meta, dict):
                for src_key, dst_key in (("type", "dtype"), ("description", "description")):
                    val = meta.get(src_key)
                    if val:
                        entry[dst_key] = str(val)
            out.append(entry)
    return out


def _table_description(hs: Any, profile: str, database: str, schema: str, table: str) -> str:
    """Pull the effective table description if AMX has one persisted."""
    with hs._connect() as conn:
        row = conn.execute(
            """
            SELECT cd.description_text
            FROM catalog_entities ce
            JOIN catalog_descriptions cd ON cd.id = ce.effective_description_id
            WHERE ce.db_profile = ? AND ce.database_name = ?
              AND ce.schema_name = ? AND ce.table_name = ?
              AND ce.entity_kind = 'table'
            LIMIT 1
            """,
            (profile, database, schema, table),
        ).fetchone()
    return str(row[0]) if row and row[0] else ""


def _read_cached_llm_edges(hs: Any, scope: Scope) -> list[Edge]:
    """Return previously-persisted ``lineage_llm`` edges touching the anchor."""
    anchor_table_id = _resolve_anchor_table_id(hs, scope)
    if anchor_table_id is None:
        return []
    with hs._connect() as conn:
        rows = conn.execute(
            """
            SELECT cr.from_entity_id, cr.to_entity_id, cr.score, cr.details_json,
                   src.schema_name AS src_schema, src.table_name AS src_table,
                   tgt.schema_name AS tgt_schema, tgt.table_name AS tgt_table
            FROM catalog_relationships cr
            JOIN catalog_entities src ON src.id = cr.from_entity_id
            JOIN catalog_entities tgt ON tgt.id = cr.to_entity_id
            WHERE cr.relationship_type = 'lineage_llm'
              AND (cr.from_entity_id = ? OR cr.to_entity_id = ?)
            """,
            (anchor_table_id, anchor_table_id),
        ).fetchall()
    edges: list[Edge] = []
    for row in rows:
        details = _safe_json(row[3])
        reasoning = (details.get("reasoning") if isinstance(details, dict) else "") or ""
        src_ref = ColumnRef(
            database=scope.anchor.database,
            schema=str(row[4]),
            table=str(row[5]),
            column="",
        )
        tgt_ref = ColumnRef(
            database=scope.anchor.database,
            schema=str(row[6]),
            table=str(row[7]),
            column="",
        )
        edges.append(
            Edge(
                source=src_ref,
                target=tgt_ref,
                relationship_type="lineage_llm",
                extractor="llm",
                confidence=float(row[2] or 0.0),
                evidence=str(reasoning)[:160],
            )
        )
    return edges


def _persist_and_emit_edges(
    hs: Any,
    scope: Scope,
    suggested: list[prompt_mod.SuggestedEdge],
    *,
    model_name: str,
    prompt_hash: str,
):
    anchor_table_id = _resolve_anchor_table_id(hs, scope)
    if anchor_table_id is None:
        return
    now = time.time()
    with hs._lock, hs._connect() as conn:
        for sugg in suggested:
            from_id = _resolve_table_id(conn, scope.profile, scope.anchor.database, sugg.from_fqn)
            to_id = _resolve_table_id(conn, scope.profile, scope.anchor.database, sugg.to_fqn)
            if from_id is None or to_id is None:
                continue
            details = {
                "reasoning": sugg.reasoning,
                "column_pairs": [list(p) for p in sugg.column_pairs],
                "model": model_name,
                "prompt_hash": prompt_hash,
                "ts": now,
            }
            # Upsert by (from, to, type): delete any prior LLM edge for the
            # same endpoints so we don't accumulate duplicates across runs.
            conn.execute(
                """
                DELETE FROM catalog_relationships
                WHERE from_entity_id = ? AND to_entity_id = ?
                  AND relationship_type = 'lineage_llm'
                """,
                (from_id, to_id),
            )
            conn.execute(
                """
                INSERT INTO catalog_relationships
                    (from_entity_id, to_entity_id, relationship_type, score,
                     source, details_json, last_seen)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    from_id,
                    to_id,
                    "lineage_llm",
                    float(sugg.confidence),
                    "llm",
                    json.dumps(details, ensure_ascii=False),
                    now,
                ),
            )
            yield Edge(
                source=prompt_mod.to_column_ref(scope.anchor, sugg.from_fqn),
                target=prompt_mod.to_column_ref(scope.anchor, sugg.to_fqn),
                relationship_type="lineage_llm",
                extractor="llm",
                confidence=float(sugg.confidence),
                evidence=str(sugg.reasoning)[:160],
            )


def _resolve_table_id(conn: Any, profile: str, database: str, fqn: str) -> int | None:
    schema, _, table = fqn.partition(".")
    row = conn.execute(
        """
        SELECT id FROM catalog_entities
        WHERE db_profile = ? AND database_name = ? AND schema_name = ?
          AND table_name = ? AND entity_kind = 'table'
        LIMIT 1
        """,
        (profile, database, schema, table),
    ).fetchone()
    return int(row[0]) if row else None


def _resolve_anchor_table_id(hs: Any, scope: Scope) -> int | None:
    with hs._connect() as conn:
        row = conn.execute(
            """
            SELECT id FROM catalog_entities
            WHERE db_profile = ? AND database_name = ? AND schema_name = ?
              AND table_name = ? AND entity_kind = 'table'
            LIMIT 1
            """,
            (
                scope.profile,
                scope.anchor.database,
                scope.anchor.schema,
                scope.anchor.table,
            ),
        ).fetchone()
    return int(row[0]) if row else None


def _safe_json(raw: Any) -> Any:
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return {}


def _verdict_examples(
    hs: Any, scope: Scope, *, limit: int = 10
) -> tuple[list[prompt_mod.FeedbackExample], list[prompt_mod.FeedbackExample]]:
    """Pull recent approved + rejected edges from the same profile.

    These ride into the next LLM prompt as positive / negative few-shot
    examples (see :func:`amx.lineage.llm_prompt.build_messages`). Limit
    is generous because the prompt builder hard-caps per side.
    """
    with hs._connect() as conn:
        rows = conn.execute(
            """
            SELECT cr.verdict, cr.details_json,
                   src.schema_name, src.table_name,
                   tgt.schema_name, tgt.table_name,
                   COALESCE(cr.from_column, ''), COALESCE(cr.to_column, '')
            FROM catalog_relationships cr
            JOIN catalog_entities src ON src.id = cr.from_entity_id
            JOIN catalog_entities tgt ON tgt.id = cr.to_entity_id
            WHERE cr.verdict IN ('approved', 'rejected')
              AND src.db_profile = ?
            ORDER BY cr.audit_at DESC NULLS LAST
            LIMIT ?
            """,
            (scope.profile, limit * 2),
        ).fetchall()
    approved: list[prompt_mod.FeedbackExample] = []
    rejected: list[prompt_mod.FeedbackExample] = []
    for row in rows:
        verdict = str(row[0] or "")
        details = _safe_json(row[1])
        note = ""
        if isinstance(details, dict):
            note = str(details.get("notes") or details.get("reasoning") or "")[:80]
        ex = prompt_mod.FeedbackExample(
            from_fqn=f"{row[2]}.{row[3]}",
            to_fqn=f"{row[4]}.{row[5]}",
            from_column=str(row[6] or ""),
            to_column=str(row[7] or ""),
            note=note,
        )
        if verdict == "approved" and len(approved) < limit:
            approved.append(ex)
        elif verdict == "rejected" and len(rejected) < limit:
            rejected.append(ex)
        if len(approved) >= limit and len(rejected) >= limit:
            break
    return approved, rejected


__all__ = ["LLMExtractor", "LLMCallable"]
