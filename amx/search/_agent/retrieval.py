"""Retrieval + live-DB probe orchestration for ``SearchAgent``.

The cluster turns a planned :class:`SearchPlan` into concrete catalog
queries and live-DB probes, then merges the rows for the answer
synthesizer. It owns:

* The big retrieval orchestrator (``_retrieve``) — the single largest
  method in the class, dispatches to ``catalog.search_*``, live-DB
  helpers, and the live-probe pipeline depending on plan shape.
* Live-DB row builders (``_inventory_db``, ``_known_database_rows``,
  ``_live_schema_rows``, ``_live_table_count``, ``_live_joinable_tables``).
* Live-probe planning + execution (``_should_plan_live_probe``,
  ``_default_live_probe_operations``, ``_merge_probe_operations``,
  ``_plan_live_probe``, ``_execute_live_probe``,
  ``_should_use_llm_probe_planner``).
* Row post-processing for the prompt (``_merge_join_rows``,
  ``_merge_joinable_rows``, ``_verify_rows``, ``_rows_for_prompt``,
  ``_normalize_rows``, ``_suppress_rows``).
* ``_context_detail`` — small helper that reads the active LLM profile.

Reads ``self.catalog``, ``self.db``, ``self.cfg``, ``self.db_profile``.
Calls back into planning + resolution mixins via ``self.``.
"""

from __future__ import annotations

import re
from typing import Any

from amx.agents.tools import SchemaExplorer
from amx.db.connector import DatabaseConnector, ProfilingError
from amx.search._agent._types import LiveProbePlan, SearchPlan, SearchPolicy
from amx.utils.logging import get_logger

log = get_logger("search.agent.retrieval")


# Tokens removed from question_terms before they reach the BM25-lite
# scorer in ``amx.pages.evidence``. They are too generic to be useful
# as keywords and would otherwise dominate the relevance score on
# pages that mention them in passing.
_PAGES_STOPWORDS = frozenset(
    {
        "what",
        "which",
        "where",
        "when",
        "show",
        "list",
        "tell",
        "give",
        "find",
        "table",
        "tables",
        "column",
        "columns",
        "row",
        "rows",
        "value",
        "values",
        "data",
        "from",
        "with",
        "have",
        "this",
        "that",
        "these",
        "those",
        "into",
        "about",
        "the",
        "and",
        "but",
        "for",
        "are",
        "was",
        "were",
        "has",
        "had",
        "you",
        "your",
        "our",
    }
)


def _asset_refs_for_entities(store: Any, entity_ids: list[int]) -> list[str]:
    """Convert ``catalog_entities`` ids to ``documentation_page_assets``
    ``asset_ref`` strings.

    Emits ``profile:schema:table`` for table entities and additionally
    ``profile:schema:table.column`` for column entities, matching the
    asset-ref convention used by the pages composer.
    """
    if not entity_ids:
        return []
    placeholders = ",".join("?" for _ in entity_ids)
    with store._connect() as conn:  # noqa: SLF001
        rows = conn.execute(
            f"""
            SELECT db_profile, schema_name, table_name, column_name
            FROM catalog_entities WHERE id IN ({placeholders})
            """,
            tuple(entity_ids),
        ).fetchall()
    refs: list[str] = []
    seen: set[str] = set()
    for prof, schema, table, col in rows:
        base = f"{prof}:{schema}:{table}"
        if base not in seen:
            seen.add(base)
            refs.append(base)
        if col:
            scoped = f"{base}.{col}"
            if scoped not in seen:
                seen.add(scoped)
                refs.append(scoped)
    return refs


def _question_terms_for_pages(question: str, plan: SearchPlan | None) -> list[str]:
    """Extract de-noised keyword tokens for pages BM25-lite scoring.

    Pulls the question's normalized form plus the planner's entity
    hints, lowercases everything, splits on word boundaries, and drops
    short tokens / stopwords so the scorer keys on meaningful nouns.
    """
    sources: list[str] = []
    if question:
        sources.append(question)
    if plan is not None:
        if plan.normalized_question:
            sources.append(plan.normalized_question)
        for hint in plan.entity_hints:
            if hint:
                sources.append(str(hint))
    seen: set[str] = set()
    out: list[str] = []
    for chunk in sources:
        for token in re.findall(r"[A-Za-z0-9_]+", chunk):
            normalized = token.lower()
            if len(normalized) <= 2:
                continue
            if normalized in _PAGES_STOPWORDS:
                continue
            if normalized in seen:
                continue
            seen.add(normalized)
            out.append(normalized)
    return out


def _entity_ids_from_rows(rows: list[dict[str, Any]]) -> list[int]:
    """Pull catalog_entities row ids out of retrieval rows, in order, deduped."""
    seen: set[int] = set()
    out: list[int] = []
    for row in rows or []:
        raw = row.get("id")
        if raw is None:
            continue
        try:
            eid = int(raw)
        except (TypeError, ValueError):
            continue
        if eid <= 0 or eid in seen:
            continue
        seen.add(eid)
        out.append(eid)
    return out


def enrich_retrieval_details_with_lineage_and_pages(
    *,
    store: Any,
    rows: list[dict[str, Any]],
    retrieval_details: dict[str, Any],
    question: str,
    plan: SearchPlan | None,
    lineage_profiles: list[str] | None,
    pages_enabled: bool | None,
    asset_kinds: list[str] | None = None,
) -> dict[str, Any]:
    """Fold lineage, pages, and asset evidence into ``retrieval_details``.

    Anchor entity ids are derived from ``rows`` (the catalog_entity ids
    surfaced by the catalog search). When supporting data exists the
    function appends ``"lineage"`` / ``"pages"`` / ``"assets"`` to
    ``retrieval_details["evidence_sources"]`` and stores the structured
    payload under ``retrieval_details["lineage"]`` / ``["pages"]`` /
    ``["assets"]``.

    ``lineage_profiles``:
      * ``None`` — Auto: include every saved canvas touching the anchors.
      * non-empty list — restrict to canvases whose name is in the list.
      * empty list — lineage retrieval off.

    ``pages_enabled``:
      * ``None`` — Auto (treated as enabled here; gating per-question is
        handled by the policy layer upstream).
      * ``True`` / ``False`` — explicit override.

    ``asset_kinds``:
      * ``None`` — Auto: include every ingested kind (notebooks,
        queries, streams, pipelines) that references a resolved entity.
      * non-empty list — restrict to the listed kinds (subset of
        ``notebooks``, ``queries``, ``streams``, ``pipelines``).
      * empty list — asset retrieval off.
    """
    if store is None:
        return retrieval_details

    entity_ids = _entity_ids_from_rows(rows)

    # Anchor-based lineage retrieval. Skips silently when there are no
    # saved canvases anchored to the resolved entities.
    from amx.lineage.evidence import build_lineage_evidence

    lineage_payload = build_lineage_evidence(
        store=store,
        entity_ids=entity_ids,
        artifact_filter=lineage_profiles,
        max_upstream=5,
        max_downstream=5,
        max_comments=3,
    )
    if not lineage_payload.is_empty:
        retrieval_details.setdefault("evidence_sources", [])
        if "lineage" not in retrieval_details["evidence_sources"]:
            retrieval_details["evidence_sources"].append("lineage")
        retrieval_details["lineage"] = {
            "kind": "lineage",
            "artifact_names": list(lineage_payload.artifact_names),
            "upstream_entity_ids": list(lineage_payload.upstream_entity_ids),
            "downstream_entity_ids": list(lineage_payload.downstream_entity_ids),
            "external_systems": list(lineage_payload.logo_keys),
            "comments": list(lineage_payload.comments),
        }

    # Published-pages retrieval. Anchor-scoped, no semantic index.
    from amx.pages.evidence import build_pages_evidence

    pages_payload = build_pages_evidence(
        store=store,
        asset_refs=_asset_refs_for_entities(store, entity_ids),
        question_terms=_question_terms_for_pages(question, plan),
        max_pages=3,
        max_excerpt_chars=400,
        enabled=True if pages_enabled is None else pages_enabled,
    )
    if not pages_payload.is_empty:
        retrieval_details.setdefault("evidence_sources", [])
        if "pages" not in retrieval_details["evidence_sources"]:
            retrieval_details["evidence_sources"].append("pages")
        retrieval_details["pages"] = {
            "kind": "pages",
            "items": [
                {"title": it.title, "slug": it.slug, "excerpt": it.excerpt}
                for it in pages_payload.items
            ],
        }

    # Ingested-asset evidence (notebooks, queries, streams, pipelines)
    # that reference the resolved tables via catalog_relationships
    # edges of type ``asset_references_table``. Studio's Assets pill
    # forwards the wire-plural kinds (``notebooks`` / ``queries`` / …);
    # the retriever's SQL uses the singular ``from_entity_kind`` values
    # (``notebook`` / ``query`` / …), so translate before calling.
    from amx.search._agent.asset_evidence import build_assets_evidence

    _ASSET_KIND_PLURAL_TO_SINGULAR = {
        "notebooks": "notebook",
        "queries": "query",
        "streams": "stream",
        "pipelines": "pipeline",
    }
    assets_enabled = asset_kinds is None or len(asset_kinds) > 0
    kinds_filter: list[str] | None
    if asset_kinds is None:
        kinds_filter = None
    else:
        kinds_filter = [
            _ASSET_KIND_PLURAL_TO_SINGULAR[k]
            for k in asset_kinds
            if k in _ASSET_KIND_PLURAL_TO_SINGULAR
        ]
        if not kinds_filter:
            # Empty override (Off) OR a list that mapped to nothing
            # known — either way skip the evidence step.
            assets_enabled = False

    assets_payload = build_assets_evidence(
        store=store,
        entity_ids=entity_ids,
        question_terms=_question_terms_for_pages(question, plan),
        max_assets=3,
        max_excerpt_chars=400,
        enabled=assets_enabled,
        kinds=kinds_filter,
    )
    if not assets_payload.is_empty:
        retrieval_details.setdefault("evidence_sources", [])
        if "assets" not in retrieval_details["evidence_sources"]:
            retrieval_details["evidence_sources"].append("assets")
        retrieval_details["assets"] = {
            "kind": "assets",
            "items": [
                {
                    "kind": it.kind,
                    "name": it.name,
                    "profile": it.profile,
                    "location": it.location,
                    "excerpt": it.excerpt,
                }
                for it in assets_payload.items
            ],
        }

    return retrieval_details


class RetrievalMixin:
    """Retrieval + live-DB probe methods for ``SearchAgent``."""

    def _should_use_llm_probe_planner(self) -> bool:
        return False

    def _context_detail(self) -> str:
        value = str(self.settings.get("context_detail", "standard") or "standard").strip().lower()
        return value if value in {"minimal", "standard", "rich", "deep"} else "standard"

    def _inventory_db(self) -> DatabaseConnector:
        return self._inventory_db_factory()

    def _known_database_rows(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        for profile_name, db_cfg in sorted(self.cfg.db_profiles.items()):
            database_name = db_cfg.database or db_cfg.catalog or db_cfg.project or ""
            key = (profile_name, database_name)
            if key in seen:
                continue
            seen.add(key)
            rows.append(
                {
                    "row_type": "database",
                    "db_profile": profile_name,
                    "database_name": database_name,
                    "backend": db_cfg.backend,
                    "source": "config",
                }
            )
        if not rows:
            rows.append(
                {
                    "row_type": "database",
                    "db_profile": self.db_profile,
                    "database_name": self.cfg.db.database
                    or self.cfg.db.catalog
                    or self.cfg.db.project
                    or "",
                    "backend": self.cfg.db.backend,
                    "source": "config",
                }
            )
        return rows

    def _live_schema_rows(self) -> list[dict[str, Any]]:
        db = self._inventory_db()
        rows: list[dict[str, Any]] = []
        for schema_name in db.list_schemas():
            try:
                table_count = len(db.list_tables(schema_name))
            except Exception:
                table_count = 0
            rows.append(
                {
                    "row_type": "schema",
                    "database_name": self.cfg.db.database
                    or self.cfg.db.catalog
                    or self.cfg.db.project
                    or "",
                    "schema_name": schema_name,
                    "table_count": table_count,
                    "source": "live_db",
                }
            )
        return rows

    def _live_table_count(self, schema_name: str | None) -> tuple[int, dict[str, Any]]:
        db = self._inventory_db()
        schemas = db.list_schemas()
        schema_lookup = {str(item).lower(): str(item) for item in schemas}
        if schema_name:
            resolved = schema_lookup.get(str(schema_name).strip().lower())
            if resolved:
                count = len(db.list_tables(resolved))
                return count, {
                    "scope_kind": "schema",
                    "schema_name": resolved,
                    "database_name": self.cfg.db.database
                    or self.cfg.db.catalog
                    or self.cfg.db.project
                    or "",
                    "scope_assumption": "current_schema",
                }
        total = 0
        for item in schemas:
            try:
                total += len(db.list_tables(item))
            except Exception:
                continue
        return total, {
            "scope_kind": "database",
            "database_name": self.cfg.db.database
            or self.cfg.db.catalog
            or self.cfg.db.project
            or "",
            "schema_count": len(schemas),
            "scope_assumption": "active_database"
            if not schema_name
            else "invalid_current_schema_fallback",
        }

    def _live_joinable_tables(self, table_path: str, limit: int) -> list[dict[str, Any]]:
        if "." not in table_path:
            return []
        schema_name, table_name = table_path.split(".", 1)
        db = self._inventory_db()
        try:
            profile = db.profile_table(schema_name, table_name, sample_size=0)
        except ProfilingError:
            return []
        rows: list[dict[str, Any]] = []
        for fk in profile.foreign_keys:
            rows.append(
                {
                    "row_type": "joinable_table",
                    "schema_name": schema_name,
                    "table_name": table_name,
                    "target_schema_name": str(fk.get("referred_schema") or schema_name),
                    "target_table_name": str(fk.get("referred_table") or ""),
                    "left_column": ", ".join(
                        str(item) for item in (fk.get("constrained_columns") or []) if str(item)
                    ),
                    "right_column": ", ".join(
                        str(item) for item in (fk.get("referred_columns") or []) if str(item)
                    ),
                    "relationship_type": "foreign_key",
                    "source": "live_db",
                    "score": 10.0,
                    "confidence_band": "verified",
                    "verified_live": True,
                }
            )
        for fk in profile.referenced_by:
            rows.append(
                {
                    "row_type": "joinable_table",
                    "schema_name": schema_name,
                    "table_name": table_name,
                    "target_schema_name": str(fk.get("source_schema") or schema_name),
                    "target_table_name": str(fk.get("source_table") or ""),
                    "left_column": ", ".join(
                        str(item) for item in (fk.get("referred_columns") or []) if str(item)
                    ),
                    "right_column": ", ".join(
                        str(item)
                        for item in (
                            fk.get("source_columns") or fk.get("constrained_columns") or []
                        )
                        if str(item)
                    ),
                    "relationship_type": "incoming_foreign_key",
                    "source": "live_db",
                    "score": 10.0,
                    "confidence_band": "verified",
                    "verified_live": True,
                }
            )
        seen: set[tuple[str, str, str, str]] = set()
        out: list[dict[str, Any]] = []
        for row in rows:
            key = (
                str(row.get("target_schema_name") or "").lower(),
                str(row.get("target_table_name") or "").lower(),
                str(row.get("relationship_type") or "").lower(),
                f"{row.get('left_column', '')}|{row.get('right_column', '')}",
            )
            if key in seen or not row.get("target_table_name"):
                continue
            seen.add(key)
            out.append(row)
        return out[:limit]

    def _should_plan_live_probe(
        self, question: str, plan: SearchPlan, table_paths: list[str]
    ) -> bool:
        if not table_paths or plan.search_mode in {
            "list_databases",
            "list_schemas",
            "count_tables",
            "join_candidates",
            "joinable_tables",
        }:
            return False
        if plan.search_mode == "table_explain" or plan.question_class == "table_understanding":
            return True
        if not self._explicit_table_mentions_for_question(question):
            return False
        sample = (question or "").strip().lower()
        metadata_terms = {
            "comment",
            "comments",
            "description",
            "descriptions",
            "metadata",
        }
        verification_terms = {
            "all",
            "every",
            "whether",
            "has",
            "have",
            "exists",
            "complete",
            "coverage",
        }
        return any(term in sample for term in metadata_terms) or any(
            term in sample for term in verification_terms
        )

    def _default_live_probe_operations(
        self, question: str, table_paths: list[str]
    ) -> list[dict[str, str]]:
        if not table_paths:
            return []
        sample = (question or "").strip().lower()
        comments_question = any(
            term in sample
            for term in (
                "comment",
                "comments",
                "description",
                "descriptions",
            )
        )
        operation = "column_comments" if comments_question else "table_metadata_snapshot"
        return [
            {
                "operation": operation,
                "table_path": table_path,
                "rationale": "Default live probe for a table-scoped factual metadata question.",
            }
            for table_path in table_paths[:2]
        ]

    def _merge_probe_operations(self, *groups: list[dict[str, str]]) -> list[dict[str, str]]:
        merged: list[dict[str, str]] = []
        seen: set[tuple[str, str]] = set()
        for group in groups:
            for op in group:
                operation = str(op.get("operation") or "").strip()
                table_path = str(op.get("table_path") or "").strip()
                key = (operation, table_path)
                if not operation or key in seen:
                    continue
                seen.add(key)
                merged.append(
                    {
                        "operation": operation,
                        "table_path": table_path,
                        "rationale": str(op.get("rationale") or "").strip(),
                    }
                )
        return merged[:4]

    def _plan_live_probe(
        self,
        question: str,
        plan: SearchPlan,
        policy: SearchPolicy,
        rows: list[dict[str, Any]],
        retrieval_details: dict[str, Any],
    ) -> tuple[LiveProbePlan, dict[str, Any]]:
        target_resolution = retrieval_details.get("target_resolution") or {}
        resolved_targets = [
            str(item.get("resolved_path") or "")
            for item in target_resolution.get("targets", [])
            if isinstance(item, dict) and str(item.get("resolved_path") or "")
        ]
        if target_resolution.get("unresolved_explicit") and not resolved_targets:
            return LiveProbePlan(
                False, "Explicit table target was not found in live metadata.", []
            ), {}
        explicit_table_paths = self._explicit_table_paths_for_question(question)
        if plan.search_mode == "table_explain" or plan.question_class == "table_understanding":
            table_paths = (
                resolved_targets
                or explicit_table_paths
                or self._candidate_table_paths_for_question(plan.entity_hints, question)
            )
        else:
            table_paths = resolved_targets or explicit_table_paths
            if not table_paths:
                seen_paths: set[str] = set()
                for mention in self._explicit_table_mentions_for_question(question):
                    requested = str(mention.get("requested") or "").strip()
                    if not requested:
                        continue
                    for path in self._table_candidate_paths(requested, limit=2):
                        if path.lower() not in seen_paths:
                            seen_paths.add(path.lower())
                            table_paths.append(path)
        if not self._should_plan_live_probe(question, plan, table_paths):
            return LiveProbePlan(False, "", []), {}
        default_ops = self._default_live_probe_operations(
            question, explicit_table_paths or table_paths
        )
        already_verified_comments = any(
            row.get("row_type") == "live_probe" and row.get("probe_operation") == "column_comments"
            for row in rows
        )
        already_verified_snapshot = any(
            row.get("row_type") == "live_probe"
            and row.get("probe_operation") == "table_metadata_snapshot"
            for row in rows
        )
        ops = default_ops
        if already_verified_comments or already_verified_snapshot:
            ops = []
        elif plan.search_mode == "table_explain" and rows:
            table_row = next((row for row in rows if row.get("row_type") == "table"), None)
            if table_row and table_row.get("effective_description"):
                ops = self._merge_probe_operations(
                    [
                        {
                            "operation": "table_metadata_snapshot",
                            "table_path": path,
                            "rationale": "Verify structural table facts from live metadata before answering.",
                        }
                        for path in (resolved_targets or explicit_table_paths or table_paths)[:1]
                    ]
                )
        merged_ops = self._merge_probe_operations(ops)
        return (
            LiveProbePlan(
                needs_live_probe=bool(merged_ops),
                reason="Deterministic live probe selected for a table-scoped factual metadata question."
                if merged_ops
                else "",
                operations=merged_ops,
            ),
            {},
        )

    def _execute_live_probe(
        self, probe_plan: LiveProbePlan
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        if not probe_plan.needs_live_probe:
            return [], {"executed": False, "reason": probe_plan.reason, "operations": []}
        db = self._inventory_db()
        rows: list[dict[str, Any]] = []
        executed: list[dict[str, Any]] = []
        for op in probe_plan.operations:
            operation = op.get("operation", "")
            table_path = op.get("table_path", "")
            if operation in {"column_comments", "table_metadata_snapshot"} and "." in table_path:
                schema_name, table_name = table_path.split(".", 1)
                query_text = (
                    db.column_comments_probe_query(schema_name, table_name)
                    if operation == "column_comments"
                    else db.table_metadata_probe_query(schema_name, table_name)
                )
                snapshot = (
                    {
                        "columns": [
                            {"name": name, "comment": comment or ""}
                            for name, comment in db.get_column_comments(
                                schema_name, table_name
                            ).items()
                        ],
                        "table_comment": "",
                    }
                    if operation == "column_comments"
                    else db.get_table_metadata_snapshot(schema_name, table_name)
                )
                columns = list(snapshot.get("columns") or [])
                comments = {
                    str(col.get("name") or ""): str(col.get("comment") or "")
                    for col in columns
                    if str(col.get("name") or "")
                }
                total = len(comments)
                filled = sum(1 for value in comments.values() if str(value or "").strip())
                missing = [name for name, value in comments.items() if not str(value or "").strip()]
                rows.append(
                    {
                        "row_type": "live_probe",
                        "probe_operation": operation,
                        "schema_name": schema_name,
                        "table_name": table_name,
                        "metric": "table_metadata_snapshot"
                        if operation == "table_metadata_snapshot"
                        else "column_comment_coverage",
                        "value": filled,
                        "total_columns": total,
                        "commented_columns": filled,
                        "missing_columns": missing,
                        "all_columns_commented": total > 0 and filled == total,
                        "table_comment": snapshot.get("table_comment", ""),
                        "source": "live_db",
                        "verified_live": True,
                        "executed_query": query_text,
                    }
                )
                for col in columns:
                    column_name = str(col.get("name") or "")
                    comment = str(col.get("comment") or "")
                    if not column_name:
                        continue
                    rows.append(
                        {
                            "row_type": "live_column_comment",
                            "schema_name": schema_name,
                            "table_name": table_name,
                            "column_name": column_name,
                            "dtype": col.get("dtype", ""),
                            "nullable": col.get("nullable", ""),
                            "effective_description": comment,
                            "has_comment": bool(comment.strip()),
                            "source": "live_db",
                            "verified_live": True,
                        }
                    )
                executed.append(
                    {
                        "operation": operation,
                        "table_path": table_path,
                        "query": query_text,
                        "rationale": op.get("rationale", ""),
                    }
                )
        return rows, {
            "executed": bool(executed),
            "reason": probe_plan.reason,
            "operations": executed,
        }

    def _retrieve(
        self, question: str, plan: SearchPlan, policy: SearchPolicy
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        limit = self._candidate_limit(policy.question_class)
        details: dict[str, Any] = {
            "search_mode": plan.search_mode,
            "question_class": plan.question_class,
            "retrieval_policy": policy.retrieval_policy,
            "entity_hints": list(plan.entity_hints),
            "search_queries": list(plan.search_queries),
            "evidence_sources": [],
            "resolved_scope": {},
        }
        if plan.search_mode == "join_candidates":
            table_paths = self._resolve_table_paths(plan.entity_hints, question)
            details["resolved_tables"] = table_paths[:2]
            if len(table_paths) < 2:
                details["ambiguity_flags"] = ["missing_join_table"]
                return [], details
            verified = self.catalog.join_candidates(
                self.db_profile, table_paths[0], table_paths[1], limit=limit
            )
            semantic = self.catalog.semantic_join_candidates(
                self.db_profile, table_paths[0], table_paths[1], limit=limit
            )
            details["evidence_sources"] = ["catalog_relationships", "semantic_join_inference"]
            rows = self._merge_join_rows(verified, semantic, limit)
            return rows, details
        if plan.search_mode == "joinable_tables":
            table_paths = self._resolve_table_paths(plan.entity_hints, question)
            details["resolved_tables"] = table_paths[:1]
            if not table_paths:
                details["ambiguity_flags"] = ["missing_join_base_table"]
                return [], details
            live_rows = self._live_joinable_tables(table_paths[0], limit=limit)
            catalog_rows = self.catalog.joinable_tables(
                self.db_profile, table_paths[0], limit=limit
            )
            semantic_rows = self.catalog.semantic_joinable_tables(
                self.db_profile, table_paths[0], limit=limit
            )
            details["display_rows"] = True
            details["evidence_sources"] = [
                "live_db",
                "catalog_relationships",
                "semantic_join_inference",
            ]
            rows = self._merge_joinable_rows(live_rows, catalog_rows, semantic_rows, limit)
            return rows, details
        if plan.search_mode == "table_explain":
            targets = self._resolve_table_targets(plan.entity_hints, question)
            details["target_resolution"] = self._target_resolution_details(targets)
            resolved_paths = [target.resolved_path for target in targets if target.resolved_path]
            details["resolved_tables"] = resolved_paths[:1]
            if targets and not resolved_paths:
                details["ambiguity_flags"] = ["explicit_table_not_found_live"]
                details["evidence_sources"] = ["live_target_resolution"]
                return [], details
            if not resolved_paths:
                details["ambiguity_flags"] = ["missing_table_scope"]
                return [], details
            explained = self.catalog.explain_table(self.db_profile, resolved_paths[0])
            if not explained:
                details["evidence_sources"] = ["live_target_resolution"]
                return [], details
            table_row = dict(explained["table"])
            table_row["row_type"] = "table"
            table_row["relationship_count"] = len(explained["relationships"])
            table_row["column_count"] = len(explained["columns"])
            top_columns = [dict(row) for row in explained["columns"][:limit]]
            for row in top_columns:
                row["row_type"] = "column"
            details["table_context"] = {
                "table": table_row,
                "relationship_count": len(explained["relationships"]),
                "column_count": len(explained["columns"]),
            }
            details["evidence_sources"] = ["effective_metadata", "catalog_relationships"]
            return [table_row, *top_columns], details
        if plan.search_mode == "name_lookup":
            query_text = plan.entity_hints[0] if plan.entity_hints else plan.normalized_question
            rows = self.catalog.name_search_columns(self.db_profile_filter, query_text, limit=limit)
            details["query_text"] = query_text
            details["evidence_sources"] = ["lexical_index"]
            return rows, details
        if plan.search_mode == "list_databases":
            rows = self._known_database_rows()
            details["display_rows"] = False
            details["result_kind"] = "catalog_overview"
            details["evidence_sources"] = ["config", "catalog_entities"]
            return rows, details
        if plan.search_mode == "list_schemas":
            rows = self._live_schema_rows()
            details["database_name"] = (
                self.cfg.db.database or self.cfg.db.catalog or self.cfg.db.project or ""
            )
            details["display_rows"] = False
            details["result_kind"] = "catalog_overview"
            details["evidence_sources"] = ["live_db"]
            return rows, details
        if plan.search_mode == "count_tables":
            schema_name = ""
            database_name = ""
            schema_lookup = {
                str(item).lower(): str(item) for item in self._inventory_db().list_schemas()
            }
            db_lookup = {
                str(row.get("database_name") or "").lower(): str(row.get("database_name") or "")
                for row in self._known_database_rows()
            }
            explicit_scope = False
            for hint in plan.entity_hints:
                normalized = str(hint or "").strip().lower()
                if not normalized or "." in normalized:
                    continue
                if normalized in schema_lookup and not schema_name:
                    schema_name = schema_lookup[normalized]
                    explicit_scope = True
                    continue
                if normalized in db_lookup and not database_name:
                    database_name = db_lookup[normalized]
                    explicit_scope = True
                    continue
                if self.catalog.find_table_candidates(self.db_profile_filter, normalized, limit=1):
                    table_paths = self._resolve_table_paths([normalized], question)
                    if table_paths and not schema_name:
                        schema_name = table_paths[0].split(".", 1)[0]
                        explicit_scope = True
            if not schema_name and self.cfg.current_schema:
                normalized_current = str(self.cfg.current_schema).strip().lower()
                if normalized_current in schema_lookup:
                    schema_name = schema_lookup[normalized_current]
            count, scope_meta = self._live_table_count(schema_name or None)
            if explicit_scope:
                scope_meta["scope_assumption"] = ""
            details.update(scope_meta)
            details["display_rows"] = False
            details["result_kind"] = "aggregate"
            details["evidence_sources"] = ["live_db"]
            return [
                {
                    "row_type": "aggregate",
                    "metric": "table_count",
                    "value": count,
                    "schema_name": scope_meta.get("schema_name", ""),
                    "database_name": scope_meta.get("database_name", ""),
                    "source": "live_db",
                    "verified_live": True,
                }
            ], details
        if plan.search_mode == "schema_inventory":
            schema_name = self.cfg.current_schema or ""
            database_name = self.cfg.db.database or self.cfg.db.catalog or self.cfg.db.project or ""
            try:
                schema_lookup = {
                    str(item).lower(): str(item) for item in self._inventory_db().list_schemas()
                }
            except Exception:
                schema_lookup = {}
            normalized_current = str(schema_name).strip().lower()
            if normalized_current and normalized_current in schema_lookup:
                schema_name = schema_lookup[normalized_current]
            elif normalized_current:
                schema_name = ""
            for hint in plan.entity_hints:
                normalized = str(hint or "").strip().lower()
                if normalized in schema_lookup:
                    schema_name = schema_lookup[normalized]
                    break
            explorer = SchemaExplorer(self.cfg, self.catalog, db_factory=self._inventory_db_factory)
            inventory = explorer.explore(
                schema_name=schema_name or None,
                database_name=database_name or None,
                limit=max(limit * 20, 500),
            )
            rows = [dict(row) for row in inventory.get("rows", [])]
            summary = dict(inventory.get("summary") or {})
            scope = dict(inventory.get("scope") or {})
            details["display_rows"] = True
            details["result_kind"] = "schema_inventory"
            details["tool"] = "SchemaExplorer"
            details["schema_explorer_summary"] = summary
            details["schema_name"] = str(scope.get("schema_name") or schema_name or "")
            details["database_name"] = str(scope.get("database_name") or database_name or "")
            details["evidence_sources"] = [
                "schema_explorer",
                str(inventory.get("source") or "effective_metadata"),
            ]
            details["gap_fill_operations"] = int(summary.get("gap_fill_operations") or 0)
            return rows, details
        if plan.search_mode == "compare_entities":
            rows = self.catalog.search_columns(
                self.db_profile_filter,
                plan.normalized_question or question,
                limit=limit,
                entity_hints=plan.entity_hints,
                query_variants=plan.search_queries,
            )
            details["evidence_sources"] = ["effective_metadata", "vector_support"]
            return rows, details
        if self._asks_column_name_listing(question, plan):
            lookup_limit = max(limit, 50)
            merged: list[dict[str, Any]] = []
            seen_ids: set[int] = set()
            for term in self._column_name_lookup_terms(question, plan):
                candidates = self.catalog.name_search_columns(
                    self.db_profile_filter, term, limit=lookup_limit
                )
                strict = [
                    row for row in candidates if term in str(row.get("column_name") or "").lower()
                ]
                for row in strict or candidates:
                    entity_id = int(row.get("id") or 0)
                    if entity_id and entity_id in seen_ids:
                        continue
                    if entity_id:
                        seen_ids.add(entity_id)
                    merged.append(row)
            if merged:
                details["display_rows"] = True
                details["result_kind"] = "exact_column_name_matches"
                details["evidence_sources"] = ["lexical_index", "effective_metadata"]
                return merged[:lookup_limit], details
        if plan.question_class == "semantic_discovery" and plan.target_entity == "table":
            rows = self.catalog.search_tables(
                self.db_profile_filter,
                plan.normalized_question or question,
                limit=limit,
                entity_hints=plan.entity_hints,
                query_variants=plan.search_queries,
            )
            details["display_rows"] = True
            details["result_kind"] = "table_matches"
            details["evidence_sources"] = [
                "effective_metadata",
                "aggregated_column_metadata",
                "vector_support",
            ]
            return rows, details
        rows = self.catalog.search_columns(
            self.db_profile_filter,
            plan.normalized_question or question,
            limit=limit,
            entity_hints=plan.entity_hints,
            query_variants=plan.search_queries,
        )
        details["evidence_sources"] = ["effective_metadata", "vector_support"]
        return rows, details

    def _enrich_with_lineage_and_pages(
        self,
        question: str,
        plan: SearchPlan,
        rows: list[dict[str, Any]],
        retrieval_details: dict[str, Any],
        *,
        lineage_profiles: list[str] | None = None,
        pages_enabled: bool | None = None,
    ) -> dict[str, Any]:
        """Resolve the active history store and fold lineage/pages evidence in.

        Looked up through the catalog rather than imported eagerly so
        unit tests that pass a hand-rolled catalog (no live history
        store) skip the enrichment instead of crashing.
        """
        store = None
        try:
            from amx.storage.sqlite_store import history_store

            store = history_store()
        except Exception as exc:  # pragma: no cover - defensive
            log.debug("history_store() unavailable for retrieval enrichment: %s", exc)
            return retrieval_details
        if store is None:
            return retrieval_details
        return enrich_retrieval_details_with_lineage_and_pages(
            store=store,
            rows=rows,
            retrieval_details=retrieval_details,
            question=question,
            plan=plan,
            lineage_profiles=lineage_profiles,
            pages_enabled=pages_enabled,
        )

    def _merge_join_rows(
        self,
        verified_rows: list[dict[str, Any]],
        semantic_rows: list[dict[str, Any]],
        limit: int,
    ) -> list[dict[str, Any]]:
        merged: list[dict[str, Any]] = []
        seen: set[tuple[str, str, str]] = set()
        for row in verified_rows + semantic_rows:
            key = (
                str(row.get("left_column") or "").lower(),
                str(row.get("right_column") or "").lower(),
                str(row.get("relationship_type") or "").lower(),
            )
            if key in seen:
                continue
            seen.add(key)
            merged.append(dict(row))
        merged.sort(
            key=lambda item: (
                -float(item.get("score") or 0.0),
                str(item.get("relationship_type") or ""),
            )
        )
        return merged[:limit]

    def _merge_joinable_rows(
        self,
        live_rows: list[dict[str, Any]],
        catalog_rows: list[dict[str, Any]],
        semantic_rows: list[dict[str, Any]],
        limit: int,
    ) -> list[dict[str, Any]]:
        merged: list[dict[str, Any]] = []
        seen: set[tuple[str, str, str, str]] = set()
        for row in live_rows + catalog_rows + semantic_rows:
            key = (
                str(row.get("target_schema_name") or "").lower(),
                str(row.get("target_table_name") or "").lower(),
                str(row.get("left_column") or "").lower(),
                str(row.get("right_column") or "").lower(),
            )
            if key in seen:
                continue
            seen.add(key)
            merged.append(dict(row))
        merged.sort(
            key=lambda item: (
                {"verified": 0, "high_likelihood": 1, "possible": 2, "weak_hypothesis": 3}.get(
                    str(item.get("confidence_band") or ""), 4
                ),
                -float(item.get("score") or 0.0),
            )
        )
        return merged[:limit]

    def _verify_rows(
        self,
        plan: SearchPlan,
        policy: SearchPolicy,
        rows: list[dict[str, Any]],
        retrieval_details: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        verification: dict[str, Any] = {
            "live_verified": False,
            "checks": [],
        }
        if any(row.get("row_type") == "live_probe" and row.get("verified_live") for row in rows):
            verification["live_verified"] = True
            verification["checks"].append("agent_planned_live_metadata_probe")
        if not policy.verify_live:
            return rows, verification
        if plan.question_class == "inventory":
            verification["live_verified"] = True
            verification["checks"].append("inventory_live_truth")
            return rows, verification
        if plan.question_class == "join_discovery":
            verification["checks"].append("join_relationship_classification")
            verified = False
            for row in rows:
                if str(row.get("relationship_type") or "") in {
                    "foreign_key",
                    "incoming_foreign_key",
                }:
                    row["verified_live"] = bool(
                        row.get("source") == "live_db" or row.get("verified_live")
                    )
                    row["confidence_band"] = "verified"
                    verified = verified or bool(row.get("verified_live"))
                elif not row.get("confidence_band"):
                    score = float(row.get("score") or 0.0)
                    row["confidence_band"] = (
                        "high_likelihood"
                        if score >= 8.0
                        else "possible"
                        if score >= 6.0
                        else "weak_hypothesis"
                    )
            verification["live_verified"] = verified
            return rows, verification
        if plan.search_mode == "table_explain" and retrieval_details.get("resolved_tables"):
            verification["checks"].append("table_resolution")
        return rows, verification

    def _rows_for_prompt(
        self, rows: list[dict[str, Any]], policy: SearchPolicy
    ) -> list[dict[str, Any]]:
        detail = self._context_detail()
        base_cap = {"minimal": 8, "standard": 16, "rich": 24, "deep": 32}.get(detail, 16)
        cap = min(max(base_cap, len(rows)), 40)
        payload: list[dict[str, Any]] = []
        for idx, row in enumerate(rows[:cap], 1):
            item = {
                "result_index": idx,
                "total_results": len(rows),
                "schema": row.get("schema_name", ""),
                "table": row.get("table_name", ""),
                "column": row.get("column_name", ""),
                "target_schema": row.get("target_schema_name", ""),
                "target_table": row.get("target_table_name", ""),
                "left_column": row.get("left_column", ""),
                "right_column": row.get("right_column", ""),
                "source": row.get("effective_source_kind", row.get("source", "")),
                "confidence": row.get("current_confidence", row.get("confidence", "")),
                "rank_score": row.get("rank_score", row.get("score", 0)),
                "description": row.get("effective_description", ""),
                "relationship_type": row.get("relationship_type", ""),
                "row_type": row.get("row_type", "column"),
                "metric": row.get("metric", ""),
                "value": row.get("value", ""),
                "database_name": row.get("database_name", ""),
                "table_count": row.get("table_count", ""),
                "confidence_band": row.get("confidence_band", ""),
                "verified_live": bool(row.get("verified_live")),
                "probe_operation": row.get("probe_operation", ""),
                "total_columns": row.get("total_columns", ""),
                "commented_columns": row.get("commented_columns", ""),
                "missing_columns": row.get("missing_columns", []),
                "all_columns_commented": row.get("all_columns_commented", ""),
                "executed_query": row.get("executed_query", ""),
                "has_comment": row.get("has_comment", ""),
                "dtype": row.get("dtype", ""),
                "nullable": row.get("nullable", ""),
                "table_comment": row.get("table_comment", ""),
            }
            payload.append(item)
        return payload

    def _normalize_rows(self, plan: SearchPlan, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        for idx, raw in enumerate(rows):
            row = dict(raw)
            reason = "semantic_match"
            tier = "strong"
            role = "supporting"
            if row.get("row_type") == "schema_explorer_table":
                tier = "strong"
                reason = "schema_explorer_inventory"
                role = "primary" if idx == 0 else "supporting"
            elif bool(row.get("verified_live")) or row.get("row_type") == "live_probe":
                tier = "verified"
                reason = "live_verified"
                role = (
                    "primary" if idx == 0 or row.get("row_type") == "live_probe" else "supporting"
                )
            elif str(row.get("relationship_type") or "") in {"foreign_key", "incoming_foreign_key"}:
                tier = "verified"
                reason = "verified_relationship"
                role = "primary"
            elif row.get("vector_only"):
                tier = "weak"
                reason = "vector_only_match"
                role = "diagnostic"
            elif plan.search_mode == "name_lookup":
                tier = (
                    "strong"
                    if float(row.get("rank_score") or row.get("match_score") or 0.0) >= 8.0
                    else "weak"
                )
                reason = "lexical_name_match"
                role = "primary" if idx == 0 else "supporting"
            elif (
                float(row.get("rank_score") or row.get("match_score") or row.get("score") or 0.0)
                < 4.5
            ):
                tier = "weak"
                reason = "low_score_match"
                role = "diagnostic"
            elif idx == 0:
                role = "primary"
                reason = "top_ranked_match"
            row["evidence_tier"] = tier
            row["answer_role"] = role
            row["match_reason"] = reason
            normalized.append(row)
        return normalized

    def _suppress_rows(
        self, plan: SearchPlan, rows: list[dict[str, Any]]
    ) -> tuple[list[dict[str, Any]], int]:
        visible: list[dict[str, Any]] = []
        suppressed = 0
        for idx, row in enumerate(rows):
            if plan.search_mode == "schema_inventory":
                visible.append(row)
                continue
            if plan.question_class == "join_discovery":
                visible.append(row)
                continue
            if row.get("answer_role") == "diagnostic":
                if (
                    row.get("vector_only")
                    and float(row.get("rank_score") or row.get("match_score") or 0.0) < 3.2
                ):
                    suppressed += 1
                    continue
                if idx >= 3:
                    suppressed += 1
                    continue
            visible.append(row)
        return visible, suppressed


__all__ = ["RetrievalMixin"]
