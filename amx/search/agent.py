"""Production-oriented search agent orchestration for AMX /search."""

from __future__ import annotations

import json
import re
import time
from dataclasses import asdict, dataclass
from typing import Any, Callable

from amx.config import AMXConfig
from amx.db.connector import DatabaseConnector, ProfilingError
from amx.llm.provider import LLMProvider
from amx.search.catalog import SearchAnswer, SearchCatalog
from amx.utils.console import step_spinner

_SESSION_MEMORY: dict[str, list[dict[str, Any]]] = {}


@dataclass
class SearchPlan:
    intent: str
    out_of_domain: bool
    normalized_question: str
    search_mode: str
    question_class: str
    target_entity: str
    entity_hints: list[str]
    search_queries: list[str]
    needs_typo_recovery: bool
    answer_language: str
    ambiguity_flags: list[str]
    reason: str


@dataclass
class SearchPolicy:
    question_class: str
    retrieval_policy: str
    requires_catalog: bool
    deterministic_answer: bool
    verify_live: bool
    allow_vector: bool
    allow_code: bool
    answer_format: str
    fallback_behavior: str


@dataclass
class SearchActionSuggestion:
    action: str
    reason: str


@dataclass
class LiveProbePlan:
    needs_live_probe: bool
    reason: str
    operations: list[dict[str, str]]


def _question_language_hint(text: str) -> str:
    sample = (text or "").strip().lower()
    if not sample:
        return "english"
    turkish_markers = {
        "hangi",
        "kaç",
        "kac",
        "tablo",
        "tablolar",
        "schema",
        "şema",
        "sema",
        "kolon",
        "kolonlar",
        "joinleyebilirim",
        "nedir",
        "var",
        "içinde",
        "icinde",
    }
    if any(ch in sample for ch in "çğıöşü") or any(token in sample for token in turkish_markers):
        return "turkish"
    return "english"


def _json_block(text: str) -> dict[str, Any]:
    raw = (text or "").strip()
    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
    start = raw.find("{")
    end = raw.rfind("}")
    if start >= 0 and end > start:
        raw = raw[start : end + 1]
    return json.loads(raw)


def _merge_usage(*payloads: dict[str, Any] | None) -> dict[str, Any]:
    merged = {
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "total_tokens": 0,
        "model_processing_sec": 0.0,
    }
    for payload in payloads:
        if not payload:
            continue
        merged["prompt_tokens"] += int(payload.get("prompt_tokens") or 0)
        merged["completion_tokens"] += int(payload.get("completion_tokens") or 0)
        merged["total_tokens"] += int(payload.get("total_tokens") or 0)
        merged["model_processing_sec"] += float(payload.get("model_processing_sec") or 0.0)
    return merged


class SearchAgent:
    """Multi-step metadata reasoning agent for /search."""

    def __init__(
        self,
        cfg: AMXConfig,
        catalog: SearchCatalog,
        *,
        llm_factory: type[LLMProvider] = LLMProvider,
        inventory_db_factory: Callable[[], DatabaseConnector] | None = None,
    ) -> None:
        self.cfg = cfg
        self.catalog = catalog
        self.settings = catalog.get_settings(cfg.active_db_profile or "default")
        self.db_profile = cfg.active_db_profile or "default"
        self._llm_factory = llm_factory
        self._inventory_db_factory = inventory_db_factory or (lambda: DatabaseConnector(self.cfg.db))
        self._llm: LLMProvider | None = None
        self._memory_key = f"{self.db_profile}:{cfg.active_llm_profile or 'default'}"

    def _llm_available(self) -> bool:
        if self.settings.get("llm_enabled", "true").lower() != "true":
            return False
        return bool(getattr(self.cfg.llm, "provider", "") and getattr(self.cfg.llm, "model", ""))

    def _llm_provider(self) -> LLMProvider:
        if self._llm is None:
            self._llm = self._llm_factory(self.cfg.llm)
        return self._llm

    def _memory_turns(self) -> int:
        try:
            return max(0, int(self.settings.get("conversation_memory_turns", "4")))
        except Exception:
            return 4

    def _memory(self) -> list[dict[str, Any]]:
        return list(_SESSION_MEMORY.get(self._memory_key, []))

    def _remember(self, turn: dict[str, Any]) -> None:
        turns = self._memory()
        turns.append(turn)
        max_turns = self._memory_turns()
        if max_turns > 0:
            turns = turns[-max_turns:]
        _SESSION_MEMORY[self._memory_key] = turns

    def _memory_summary(self) -> list[dict[str, Any]]:
        summary: list[dict[str, Any]] = []
        for turn in self._memory():
            summary.append(
                {
                    "question": turn.get("question", ""),
                    "intent": turn.get("intent", ""),
                    "topic": turn.get("topic", ""),
                    "tables": turn.get("tables", []),
                    "columns": turn.get("columns", []),
                }
            )
        return summary

    def _last_tables(self) -> list[str]:
        tables: list[str] = []
        for turn in reversed(self._memory()):
            for table in turn.get("tables", []) or []:
                if table and table not in tables:
                    tables.append(str(table))
            if tables:
                break
        return tables

    def _catalog_ready(self) -> tuple[bool, dict[str, Any]]:
        status = self.catalog.sync_status(self.db_profile)
        total = int((status.get("entities") or {}).get("total_entities") or 0)
        return total > 0, status

    def _interpret_question(self, question: str) -> tuple[SearchPlan, dict[str, Any]]:
        llm = self._llm_provider()
        memory = self._memory_summary()
        metadata_language = self.cfg.llm.language or "english"
        system = (
            "You interpret metadata-search questions for an AMX /search agent.\n"
            "Return JSON only.\n"
            "Classify the request; do not answer it.\n"
            "Allowed search_mode values: semantic_concept, name_lookup, join_candidates, joinable_tables, "
            "table_explain, list_databases, list_schemas, count_tables, compare_entities, unsupported.\n"
            "Allowed question_class values: inventory, entity_lookup, semantic_discovery, join_discovery, "
            "table_understanding, comparative_reasoning, unsupported.\n"
            "Allowed target_entity values: column, table, schema, database, aggregate, join_path, unknown.\n"
            "Allowed intent values: find_columns, join_candidates, explain_table, list_databases, list_schemas, "
            "count_tables, compare_entities, unsupported.\n"
            "Set out_of_domain=true for greetings, small talk, or requests unrelated to database metadata.\n"
            "Use entity_hints for schema.table names, table names, schemas, databases, and important field names.\n"
            "Preserve the original spelling of entity hints even if they look like typos.\n"
            "Always include search_queries as a short list of retrieval phrases. Include the user's original wording. "
            "For non-English questions, also include an English canonical retrieval phrase.\n"
            "normalized_question should be the best canonical retrieval phrase in English when useful.\n"
            "If the question looks like a field/code lookup such as MANDT, VBELN, mangdt, bukrs, choose name_lookup and entity_lookup.\n"
            "If the question asks how to join two tables, choose join_candidates and join_discovery.\n"
            "If the question asks which tables can join with one table, choose joinable_tables and join_discovery.\n"
            "If the question asks what a table does, choose table_explain and table_understanding.\n"
            "If the question asks which databases are known, choose list_databases and inventory.\n"
            "If the question asks which schemas exist, choose list_schemas and inventory.\n"
            "If the question asks how many tables or columns exist, choose count_tables and inventory.\n"
            "If the question compares tables or asks for equivalents, choose compare_entities and comparative_reasoning.\n"
            "If the question asks which tables contain a concept such as address details, price data, customer identifiers, "
            "or date-related fields, keep question_class=semantic_discovery, choose search_mode=semantic_concept, and set target_entity=table.\n"
            "For field or concept questions about columns, set target_entity=column.\n"
            "Set answer_language to the language of the user's question. "
            "Return ambiguity_flags as a list, for example missing_scope, ambiguous_table_name, or cross_schema_risk.\n"
            "Otherwise choose semantic_concept and semantic_discovery."
        )
        user = json.dumps(
            {
                "question": question,
                "session_memory": memory,
                "current_schema": self.cfg.current_schema or "",
                "current_table": self.cfg.current_table or "",
                "metadata_generation_language": metadata_language,
                "active_db_profile": self.db_profile,
            },
            ensure_ascii=True,
        )
        result = llm.chat(
            [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            temperature=0.0,
            max_tokens=900,
            use_logprobs=False,
        )
        payload = _json_block(result.content)
        search_mode = str(payload.get("search_mode") or "semantic_concept")
        question_class = str(payload.get("question_class") or "").strip() or self._class_from_mode(search_mode)
        return (
            SearchPlan(
                intent=str(payload.get("intent") or "find_columns"),
                out_of_domain=bool(payload.get("out_of_domain")),
                normalized_question=str(payload.get("normalized_question") or question).strip() or question,
                search_mode=search_mode,
                question_class=question_class,
                target_entity=str(payload.get("target_entity") or "unknown").strip() or "unknown",
                entity_hints=[str(item).strip() for item in (payload.get("entity_hints") or []) if str(item).strip()],
                search_queries=[
                    str(item).strip()
                    for item in (payload.get("search_queries") or [])
                    if str(item).strip()
                ]
                or [str(payload.get("normalized_question") or question).strip() or question],
                needs_typo_recovery=bool(payload.get("needs_typo_recovery")),
                answer_language=str(payload.get("answer_language") or _question_language_hint(question)).strip() or _question_language_hint(question),
                ambiguity_flags=[
                    str(item).strip()
                    for item in (payload.get("ambiguity_flags") or [])
                    if str(item).strip()
                ],
                reason=str(payload.get("reason") or "").strip(),
            ),
            result.usage or {},
        )

    def _class_from_mode(self, search_mode: str) -> str:
        if search_mode in {"list_databases", "list_schemas", "count_tables"}:
            return "inventory"
        if search_mode == "name_lookup":
            return "entity_lookup"
        if search_mode in {"join_candidates", "joinable_tables"}:
            return "join_discovery"
        if search_mode == "table_explain":
            return "table_understanding"
        if search_mode == "compare_entities":
            return "comparative_reasoning"
        if search_mode == "unsupported":
            return "unsupported"
        return "semantic_discovery"

    def _align_answer_language(self, plan: SearchPlan, question_language: str) -> SearchPlan:
        normalized = (question_language or "english").strip().lower() or "english"
        if plan.answer_language == normalized:
            return plan
        return SearchPlan(
            intent=plan.intent,
            out_of_domain=plan.out_of_domain,
            normalized_question=plan.normalized_question,
            search_mode=plan.search_mode,
            question_class=plan.question_class,
            target_entity=plan.target_entity,
            entity_hints=list(plan.entity_hints),
            search_queries=list(plan.search_queries),
            needs_typo_recovery=plan.needs_typo_recovery,
            answer_language=normalized,
            ambiguity_flags=list(plan.ambiguity_flags),
            reason=plan.reason,
        )

    def _align_plan_shape(self, plan: SearchPlan, question: str) -> SearchPlan:
        sample = (question or "").strip().lower()
        asks_count = any(token in sample for token in ("kaç", "kac", "how many", "count"))
        asks_table_word = any(token in sample for token in ("tablo", "tablolar", "table", "tables"))
        asks_listing = any(token in sample for token in ("hangi", "tüm", "tum", "list", "show", "söyle", "soyle", "tell"))
        asks_semantic_table_concept = any(
            token in sample
            for token in (
                "içinde",
                "icinde",
                "alak",
                "related",
                "detail",
                "detay",
                "contain",
                "containing",
                "with",
                "olan",
            )
        )
        if (
            plan.search_mode == "count_tables"
            and asks_table_word
            and asks_listing
            and asks_semantic_table_concept
            and not asks_count
        ):
            return SearchPlan(
                intent="find_tables",
                out_of_domain=plan.out_of_domain,
                normalized_question=plan.normalized_question,
                search_mode="semantic_concept",
                question_class="semantic_discovery",
                target_entity="table",
                entity_hints=list(plan.entity_hints),
                search_queries=list(plan.search_queries),
                needs_typo_recovery=plan.needs_typo_recovery,
                answer_language=plan.answer_language,
                ambiguity_flags=list(plan.ambiguity_flags),
                reason=(plan.reason + "; rerouted to table semantic discovery").strip("; "),
            )
        if plan.question_class == "semantic_discovery" and plan.target_entity in {"", "unknown"} and asks_table_word and asks_listing:
            return SearchPlan(
                intent=plan.intent,
                out_of_domain=plan.out_of_domain,
                normalized_question=plan.normalized_question,
                search_mode=plan.search_mode,
                question_class=plan.question_class,
                target_entity="table",
                entity_hints=list(plan.entity_hints),
                search_queries=list(plan.search_queries),
                needs_typo_recovery=plan.needs_typo_recovery,
                answer_language=plan.answer_language,
                ambiguity_flags=list(plan.ambiguity_flags),
                reason=plan.reason,
            )
        return plan

    def _context_detail(self) -> str:
        value = str(self.settings.get("context_detail", "standard") or "standard").strip().lower()
        return value if value in {"minimal", "standard", "rich", "deep"} else "standard"

    def _policy_for_plan(self, plan: SearchPlan) -> SearchPolicy:
        context_detail = self._context_detail()
        allow_vector = self.settings.get("allow_vector_support", "true").lower() == "true" and context_detail != "minimal"
        allow_code = self.settings.get("allow_code_evidence", "true").lower() == "true"
        if plan.question_class == "inventory":
            return SearchPolicy(plan.question_class, "live_inventory_first", False, True, True, False, False, "aggregate", "disclose_scope")
        if plan.question_class == "entity_lookup":
            return SearchPolicy(plan.question_class, "lexical_name_first", True, False, True, False, False, "ranked_matches", "suggest_narrow_scope")
        if plan.question_class == "join_discovery":
            return SearchPolicy(plan.question_class, "verified_fk_then_semantic_join", True, False, True, allow_vector, allow_code, "join_candidates", "return_confidence_bands")
        if plan.question_class == "table_understanding":
            return SearchPolicy(plan.question_class, "table_context_plus_neighbors", True, False, True, allow_vector, allow_code, "table_summary", "suggest_sync_if_sparse")
        if plan.question_class == "comparative_reasoning":
            return SearchPolicy(plan.question_class, "semantic_then_structural_compare", True, False, True, allow_vector, allow_code, "comparative", "ask_follow_up")
        if plan.question_class == "semantic_discovery" and plan.target_entity == "table":
            return SearchPolicy(plan.question_class, "semantic_table_search", True, False, False, allow_vector, allow_code, "table_matches", "suggest_sync_if_sparse")
        return SearchPolicy(plan.question_class, "semantic_catalog_search", True, False, False, allow_vector, allow_code, "ranked_matches", "suggest_sync_if_sparse")

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
                    "database_name": self.cfg.db.database or self.cfg.db.catalog or self.cfg.db.project or "",
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
                    "database_name": self.cfg.db.database or self.cfg.db.catalog or self.cfg.db.project or "",
                    "schema_name": schema_name,
                    "table_count": table_count,
                    "source": "live_db",
                }
            )
        return rows

    def _live_table_count(self, schema_name: str | None) -> tuple[int, dict[str, Any]]:
        db = self._inventory_db()
        if schema_name:
            count = len(db.list_tables(schema_name))
            return count, {
                "scope_kind": "schema",
                "schema_name": schema_name,
                "database_name": self.cfg.db.database or self.cfg.db.catalog or self.cfg.db.project or "",
                "scope_assumption": "current_schema",
            }
        total = 0
        schemas = db.list_schemas()
        for item in schemas:
            total += len(db.list_tables(item))
        return total, {
            "scope_kind": "database",
            "database_name": self.cfg.db.database or self.cfg.db.catalog or self.cfg.db.project or "",
            "schema_count": len(schemas),
            "scope_assumption": "active_database",
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
                    "left_column": ", ".join(str(item) for item in (fk.get("constrained_columns") or []) if str(item)),
                    "right_column": ", ".join(str(item) for item in (fk.get("referred_columns") or []) if str(item)),
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
                    "left_column": ", ".join(str(item) for item in (fk.get("referred_columns") or []) if str(item)),
                    "right_column": ", ".join(str(item) for item in (fk.get("source_columns") or fk.get("constrained_columns") or []) if str(item)),
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
                f"{row.get('left_column','')}|{row.get('right_column','')}",
            )
            if key in seen or not row.get("target_table_name"):
                continue
            seen.add(key)
            out.append(row)
        return out[:limit]

    def _candidate_table_paths_for_question(self, hints: list[str], question: str) -> list[str]:
        candidates = self._resolve_table_paths(hints, question)
        seen = {item.lower() for item in candidates}
        explicit_table_tokens = [
            item
            for item in re.findall(
                r"\b([A-Za-z_][A-Za-z0-9_]{1,127})\s+(?:table|tablo|tablosu|tablosunda)\b",
                question or "",
                flags=re.IGNORECASE,
            )
        ]
        explicit_table_tokens.extend(
            item
            for item in re.findall(
                r"\b(?:table|tablo)\s+([A-Za-z_][A-Za-z0-9_]{1,127})\b",
                question or "",
                flags=re.IGNORECASE,
            )
        )
        if self.cfg.current_schema:
            for token in explicit_table_tokens:
                path = f"{self.cfg.current_schema}.{token}"
                if path.lower() not in seen:
                    seen.add(path.lower())
                    candidates.append(path)
        tokens = [
            token
            for token in re.findall(r"\b[A-Za-z_][A-Za-z0-9_]{1,127}\b", question or "")
            if token.lower()
            not in {
                "table",
                "tablo",
                "tablosu",
                "tablosunda",
                "column",
                "columns",
                "kolon",
                "kolonlar",
                "comment",
                "commentler",
                "yorum",
                "yorumlar",
            }
        ]
        for token in tokens:
            for candidate in self.catalog.find_table_candidates(self.db_profile, token, limit=2):
                path = f"{candidate.get('schema_name', '')}.{candidate.get('table_name', '')}"
                if path == "." or path.lower() in seen:
                    continue
                seen.add(path.lower())
                candidates.append(path)
        return candidates[:6]

    def _resolve_table_paths(self, hints: list[str], question: str) -> list[str]:
        resolved: list[str] = []
        seen: set[str] = set()
        inline = re.findall(r"\b([A-Za-z0-9_]+\.[A-Za-z0-9_]+)\b", question)
        for item in inline + hints + self._last_tables():
            value = str(item or "").strip()
            if not value:
                continue
            if "." in value:
                parts = value.split(".")
                if len(parts) == 2:
                    path = f"{parts[0]}.{parts[1]}"
                    if path not in seen:
                        seen.add(path)
                        resolved.append(path)
                    continue
            for candidate in self.catalog.find_table_candidates(self.db_profile, value, limit=3):
                path = f"{candidate.get('schema_name', '')}.{candidate.get('table_name', '')}"
                if path == "." or path in seen:
                    continue
                seen.add(path)
                resolved.append(path)
                break
        return resolved

    def _candidate_limit(self, question_class: str) -> int:
        configured = str(self.settings.get("max_retrieved_entities", self.settings.get("max_results", "8")) or "8")
        try:
            base = max(1, int(configured))
        except Exception:
            base = 8
        detail = self._context_detail()
        if detail == "minimal":
            return min(base, 6)
        if detail == "rich":
            return max(base, 10)
        if detail == "deep":
            return max(base, 14)
        if question_class == "join_discovery":
            return max(base, 10)
        return base

    def _should_plan_live_probe(self, question: str, plan: SearchPlan, table_paths: list[str]) -> bool:
        if not table_paths or plan.search_mode in {"list_databases", "list_schemas", "count_tables", "join_candidates", "joinable_tables", "table_explain"}:
            return False
        sample = (question or "").strip().lower()
        metadata_terms = {
            "comment",
            "comments",
            "commentler",
            "yorum",
            "yorumlar",
            "description",
            "descriptions",
            "açıklama",
            "aciklama",
            "metadata",
        }
        short_verification_tokens = {"mi", "mı", "mu", "mü"}
        verification_terms = {
            "var mı",
            "girili",
            "dolu",
            "tüm",
            "tum",
            "all",
            "every",
            "whether",
            "has",
            "have",
            "exists",
            "complete",
            "coverage",
        }
        tokens = set(re.findall(r"\w+", sample, flags=re.UNICODE))
        return (
            any(term in sample for term in metadata_terms)
            or any(term in sample for term in verification_terms)
            or bool(tokens.intersection(short_verification_tokens))
        )

    def _default_live_probe_operations(self, question: str, table_paths: list[str]) -> list[dict[str, str]]:
        if not table_paths:
            return []
        sample = (question or "").strip().lower()
        comments_question = any(
            term in sample
            for term in (
                "comment",
                "comments",
                "commentler",
                "yorum",
                "yorumlar",
                "description",
                "descriptions",
                "açıklama",
                "aciklama",
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
        table_paths = self._candidate_table_paths_for_question(plan.entity_hints, question)
        if not self._should_plan_live_probe(question, plan, table_paths):
            return LiveProbePlan(False, "", []), {}
        default_ops = self._default_live_probe_operations(question, table_paths)
        llm = self._llm_provider()
        system = (
            "You are the evidence planner inside AMX /search.\n"
            "Decide whether the current retrieved metadata is enough to answer, or whether AMX should run a safe live metadata probe.\n"
            "Return JSON only.\n"
            "Allowed operations are: table_metadata_snapshot, column_comments, table_exists, schema_tables.\n"
            "Use table_metadata_snapshot for table-scoped factual questions about columns, types, nullability, comments, or structure.\n"
            "Use column_comments when the user asks whether table columns have comments/descriptions or asks comment coverage.\n"
            "Only choose operations for the candidate table paths provided. Do not invent schemas or tables.\n"
            "Set needs_live_probe=false only when retrieved rows already directly prove the answer."
        )
        user = json.dumps(
            {
                "question": question,
                "plan": asdict(plan),
                "policy": asdict(policy),
                "candidate_table_paths": table_paths,
                "default_operations": default_ops,
                "retrieval_details": retrieval_details,
                "retrieved_rows": self._rows_for_prompt(rows, policy),
            },
            ensure_ascii=True,
        )
        try:
            result = llm.chat(
                [
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
                temperature=0.0,
                max_tokens=700,
                use_logprobs=False,
            )
            payload = _json_block(result.content)
            usage = result.usage or {}
        except Exception:
            return (
                LiveProbePlan(
                    needs_live_probe=bool(default_ops),
                    reason="Default live probe selected for a table-scoped factual metadata question.",
                    operations=default_ops,
                ),
                {},
            )
        ops: list[dict[str, str]] = []
        allowed = {"table_metadata_snapshot", "column_comments", "table_exists", "schema_tables"}
        for item in payload.get("operations") or []:
            if not isinstance(item, dict):
                continue
            operation = str(item.get("operation") or "").strip()
            table_path = str(item.get("table_path") or "").strip()
            if operation not in allowed:
                continue
            if table_path and table_path not in table_paths:
                continue
            ops.append(
                {
                    "operation": operation,
                    "table_path": table_path,
                    "rationale": str(item.get("rationale") or "").strip(),
                }
            )
        merged_ops = self._merge_probe_operations(default_ops, ops)
        return (
            LiveProbePlan(
                needs_live_probe=bool(merged_ops),
                reason=str(payload.get("reason") or "").strip(),
                operations=merged_ops,
            ),
            usage,
        )

    def _execute_live_probe(self, probe_plan: LiveProbePlan) -> tuple[list[dict[str, Any]], dict[str, Any]]:
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
                    {"columns": [{"name": name, "comment": comment or ""} for name, comment in db.get_column_comments(schema_name, table_name).items()], "table_comment": ""}
                    if operation == "column_comments"
                    else db.get_table_metadata_snapshot(schema_name, table_name)
                )
                columns = list(snapshot.get("columns") or [])
                comments = {str(col.get("name") or ""): str(col.get("comment") or "") for col in columns if str(col.get("name") or "")}
                total = len(comments)
                filled = sum(1 for value in comments.values() if str(value or "").strip())
                missing = [name for name, value in comments.items() if not str(value or "").strip()]
                rows.append(
                    {
                        "row_type": "live_probe",
                        "probe_operation": operation,
                        "schema_name": schema_name,
                        "table_name": table_name,
                        "metric": "table_metadata_snapshot" if operation == "table_metadata_snapshot" else "column_comment_coverage",
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
        return rows, {"executed": bool(executed), "reason": probe_plan.reason, "operations": executed}

    def _retrieve(self, question: str, plan: SearchPlan, policy: SearchPolicy) -> tuple[list[dict[str, Any]], dict[str, Any]]:
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
            verified = self.catalog.join_candidates(self.db_profile, table_paths[0], table_paths[1], limit=limit)
            semantic = self.catalog.semantic_join_candidates(self.db_profile, table_paths[0], table_paths[1], limit=limit)
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
            catalog_rows = self.catalog.joinable_tables(self.db_profile, table_paths[0], limit=limit)
            semantic_rows = self.catalog.semantic_joinable_tables(self.db_profile, table_paths[0], limit=limit)
            details["display_rows"] = True
            details["evidence_sources"] = ["live_db", "catalog_relationships", "semantic_join_inference"]
            rows = self._merge_joinable_rows(live_rows, catalog_rows, semantic_rows, limit)
            return rows, details
        if plan.search_mode == "table_explain":
            table_paths = self._resolve_table_paths(plan.entity_hints, question)
            details["resolved_tables"] = table_paths[:1]
            if not table_paths:
                details["ambiguity_flags"] = ["missing_table_scope"]
                return [], details
            explained = self.catalog.explain_table(self.db_profile, table_paths[0])
            if not explained:
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
            rows = self.catalog.name_search_columns(self.db_profile, query_text, limit=limit)
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
            details["database_name"] = self.cfg.db.database or self.cfg.db.catalog or self.cfg.db.project or ""
            details["display_rows"] = False
            details["result_kind"] = "catalog_overview"
            details["evidence_sources"] = ["live_db"]
            return rows, details
        if plan.search_mode == "count_tables":
            schema_name = ""
            database_name = ""
            schema_lookup = {str(item).lower(): str(item) for item in self._inventory_db().list_schemas()}
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
                if self.catalog.find_table_candidates(self.db_profile, normalized, limit=1):
                    table_paths = self._resolve_table_paths([normalized], question)
                    if table_paths and not schema_name:
                        schema_name = table_paths[0].split(".", 1)[0]
                        explicit_scope = True
            if not schema_name and self.cfg.current_schema:
                schema_name = self.cfg.current_schema
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
        if plan.search_mode == "compare_entities":
            rows = self.catalog.search_columns(
                self.db_profile,
                plan.normalized_question or question,
                limit=limit,
                entity_hints=plan.entity_hints,
                query_variants=plan.search_queries,
            )
            details["evidence_sources"] = ["effective_metadata", "vector_support"]
            return rows, details
        if plan.question_class == "semantic_discovery" and plan.target_entity == "table":
            rows = self.catalog.search_tables(
                self.db_profile,
                plan.normalized_question or question,
                limit=limit,
                entity_hints=plan.entity_hints,
                query_variants=plan.search_queries,
            )
            details["display_rows"] = True
            details["result_kind"] = "table_matches"
            details["evidence_sources"] = ["effective_metadata", "aggregated_column_metadata", "vector_support"]
            return rows, details
        rows = self.catalog.search_columns(
            self.db_profile,
            plan.normalized_question or question,
            limit=limit,
            entity_hints=plan.entity_hints,
            query_variants=plan.search_queries,
        )
        details["evidence_sources"] = ["effective_metadata", "vector_support"]
        return rows, details

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
        merged.sort(key=lambda item: (-float(item.get("score") or 0.0), str(item.get("relationship_type") or "")))
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
                {"verified": 0, "high_likelihood": 1, "possible": 2, "weak_hypothesis": 3}.get(str(item.get("confidence_band") or ""), 4),
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
                if str(row.get("relationship_type") or "") in {"foreign_key", "incoming_foreign_key"}:
                    row["verified_live"] = bool(row.get("source") == "live_db" or row.get("verified_live"))
                    row["confidence_band"] = "verified"
                    verified = verified or bool(row.get("verified_live"))
                elif not row.get("confidence_band"):
                    score = float(row.get("score") or 0.0)
                    row["confidence_band"] = (
                        "high_likelihood" if score >= 8.0 else "possible" if score >= 6.0 else "weak_hypothesis"
                    )
            verification["live_verified"] = verified
            return rows, verification
        if plan.search_mode == "table_explain" and retrieval_details.get("resolved_tables"):
            verification["checks"].append("table_resolution")
            verification["live_verified"] = True
        return rows, verification

    def _rows_for_prompt(self, rows: list[dict[str, Any]], policy: SearchPolicy) -> list[dict[str, Any]]:
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

    def _synthesize_answer(
        self,
        question: str,
        plan: SearchPlan,
        policy: SearchPolicy,
        rows: list[dict[str, Any]],
        retrieval_details: dict[str, Any],
        verification: dict[str, Any],
        actions: list[SearchActionSuggestion],
    ) -> tuple[str, dict[str, Any]]:
        llm = self._llm_provider()
        target_language = plan.answer_language or _question_language_hint(question)
        system = (
            "You are AMX /search, a grounded metadata copilot.\n"
            "Answer only from the retrieved metadata evidence you are given.\n"
            "If evidence is weak or empty, say so explicitly and suggest a narrower follow-up.\n"
            "Do not invent table names, joins, counts, or column meanings not present in the evidence.\n"
            "Use every row in the provided rows array; if there are many rows, group them but do not ignore tail results.\n"
            "When join evidence includes confidence bands, explain them.\n"
            "When scope was assumed, state that assumption.\n"
            "If action suggestions exist, mention only the most relevant one briefly.\n"
            f"Write the final answer in {target_language}."
        )
        user = json.dumps(
            {
                "question": question,
                "plan": asdict(plan),
                "policy": asdict(policy),
                "session_memory": self._memory_summary() if self._context_detail() in {"rich", "deep"} else self._memory_summary()[-2:],
                "retrieval_details": retrieval_details,
                "verification": verification,
                "result_count": len(rows),
                "rows": self._rows_for_prompt(rows, policy),
                "actions": [asdict(item) for item in actions],
            },
            ensure_ascii=True,
        )
        result = llm.chat(
            [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            temperature=0.1,
            max_tokens=1600 if len(rows) > 12 else 1200,
            use_logprobs=False,
        )
        return result.content.strip(), result.usage or {}

    def _deterministic_inventory_answer(
        self,
        plan: SearchPlan,
        rows: list[dict[str, Any]],
        retrieval_details: dict[str, Any],
    ) -> str | None:
        lang = (plan.answer_language or "english").lower()
        if plan.search_mode == "count_tables" and rows:
            value = int(rows[0].get("value") or 0)
            schema_name = str(retrieval_details.get("schema_name") or rows[0].get("schema_name") or "")
            database_name = str(retrieval_details.get("database_name") or rows[0].get("database_name") or "")
            assumption = str(retrieval_details.get("scope_assumption") or "").strip()
            if lang == "turkish":
                if schema_name:
                    answer = f"`{schema_name}` schema'sinda toplam **{value}** tablo var."
                elif database_name:
                    answer = f"`{database_name}` veritabaninda toplam **{value}** tablo var."
                else:
                    answer = f"Toplam **{value}** tablo var."
                if assumption == "current_schema":
                    answer += f" Acik scope verilmedigi icin aktif schema `{schema_name}` varsayildi."
                elif assumption == "active_database":
                    answer += f" Acik schema verilmedigi icin aktif veritabani/profil `{self.db_profile}` kullanildi."
                return answer
            if schema_name:
                answer = f"There are **{value}** tables in the `{schema_name}` schema."
            elif database_name:
                answer = f"There are **{value}** tables in the `{database_name}` database."
            else:
                answer = f"There are **{value}** tables."
            if assumption == "current_schema":
                answer += f" No explicit scope was given, so the current schema `{schema_name}` was used."
            elif assumption == "active_database":
                answer += f" No explicit schema was given, so the active database/profile `{self.db_profile}` was used."
            return answer
        if plan.search_mode == "list_databases":
            names = [str(row.get("database_name") or "").strip() for row in rows if str(row.get("database_name") or "").strip()]
            if not names:
                return "Bilinen veritabani bulunamadi." if lang == "turkish" else "No known databases were found."
            joined = ", ".join(f"`{name}`" for name in names)
            return (
                f"Su anda su veritabanlari hakkinda bilgi var: {joined}."
                if lang == "turkish"
                else f"I currently have information about these databases: {joined}."
            )
        if plan.search_mode == "list_schemas":
            names = [str(row.get("schema_name") or "").strip() for row in rows if str(row.get("schema_name") or "").strip()]
            if not names:
                return "Schema bulunamadi." if lang == "turkish" else "No schemas were found."
            database_name = str(retrieval_details.get("database_name") or "").strip()
            joined = ", ".join(f"`{name}`" for name in names[:25])
            if lang == "turkish":
                lead = f"`{database_name}` veritabanindaki schemalar" if database_name else "Bulunan schemalar"
                return f"{lead}: {joined}."
            lead = f"Schemas in `{database_name}`" if database_name else "Schemas found"
            return f"{lead}: {joined}."
        return None

    def _deterministic_live_probe_answer(
        self,
        plan: SearchPlan,
        rows: list[dict[str, Any]],
        live_probe: dict[str, Any],
    ) -> str | None:
        lang = (plan.answer_language or "english").lower()
        coverage = next(
            (
                row
                for row in rows
                if row.get("row_type") == "live_probe"
                and row.get("probe_operation") == "column_comments"
            ),
            None,
        )
        if not coverage:
            return None
        schema_name = str(coverage.get("schema_name") or "")
        table_name = str(coverage.get("table_name") or "")
        total = int(coverage.get("total_columns") or 0)
        filled = int(coverage.get("commented_columns") or 0)
        missing = [str(item) for item in (coverage.get("missing_columns") or []) if str(item)]
        query_text = str(coverage.get("executed_query") or "")
        all_done = bool(coverage.get("all_columns_commented"))
        table_path = f"{schema_name}.{table_name}" if schema_name and table_name else table_name
        if lang == "turkish":
            if all_done:
                answer = f"Evet. `{table_path}` tablosundaki **{total}/{total}** kolonun comment'i live DB metadata'sinda girili gorunuyor."
            else:
                answer = f"Hayir. `{table_path}` tablosunda **{filled}/{total}** kolonun comment'i girili; **{len(missing)}** kolon eksik."
                if missing:
                    answer += " Eksik kolonlar: " + ", ".join(f"`{name}`" for name in missing[:25]) + "."
            if query_text:
                answer += f" Kontrol icin kullanilan probe: `{query_text}`."
            return answer
        if all_done:
            answer = f"Yes. Live DB metadata shows comments for **{total}/{total}** columns on `{table_path}`."
        else:
            answer = f"No. Live DB metadata shows comments for **{filled}/{total}** columns on `{table_path}`; **{len(missing)}** columns are missing comments."
            if missing:
                answer += " Missing columns: " + ", ".join(f"`{name}`" for name in missing[:25]) + "."
        if query_text:
            answer += f" Probe used: `{query_text}`."
        return answer

    def _provenance(self, plan: SearchPlan, rows: list[dict[str, Any]], verification: dict[str, Any]) -> list[str]:
        labels: list[str] = []
        if any((row.get("source") or "") == "live_db" for row in rows):
            labels.append("live database introspection")
        if any((row.get("row_type") or "") == "live_probe" for row in rows):
            labels.append("agent-planned live metadata probe")
        if any((row.get("source") or "") == "config" for row in rows):
            labels.append("configured database profiles")
        if plan.question_class == "inventory":
            labels.append("live structural truth")
        if plan.question_class == "join_discovery":
            labels.append("structural relationships")
            if any(str(row.get("confidence_band") or "") in {"high_likelihood", "possible", "weak_hypothesis"} for row in rows):
                labels.append("semantic join inference")
        if plan.search_mode == "table_explain":
            labels.append("effective table metadata")
        if plan.search_mode == "name_lookup":
            labels.append("exact or fuzzy field-name matching")
        elif plan.question_class in {"semantic_discovery", "comparative_reasoning"}:
            labels.append("effective metadata")
        if any((row.get("source") or "") == "code" for row in rows):
            labels.append("behavioral code evidence")
        if verification.get("live_verified"):
            labels.append("live verification")
        if self.settings.get("allow_vector_support", "true").lower() == "true" and plan.search_mode in {"semantic_concept", "compare_entities"}:
            labels.append("vector support")
        out: list[str] = []
        for label in labels:
            if label not in out:
                out.append(label)
        return out

    def _confidence(self, plan: SearchPlan, rows: list[dict[str, Any]], verification: dict[str, Any]) -> str:
        if plan.out_of_domain or not rows:
            return "low"
        if plan.question_class == "inventory":
            return "high" if verification.get("live_verified") else "medium"
        if any(bool(row.get("verified_live")) for row in rows):
            return "high"
        top = rows[0]
        if plan.question_class == "join_discovery":
            band = str(top.get("confidence_band") or "")
            if band == "verified":
                return "high"
            if band == "high_likelihood":
                return "medium"
            return "low"
        score = float(top.get("rank_score") or top.get("score") or 0.0)
        if plan.search_mode == "name_lookup":
            return "high" if score >= 8.0 else "medium"
        if score >= 6.0:
            return "high"
        if score >= 4.0:
            return "medium"
        return "low"

    def _action_suggestions(
        self,
        plan: SearchPlan,
        rows: list[dict[str, Any]],
        ready: bool,
        retrieval_details: dict[str, Any],
        confidence: str,
    ) -> list[SearchActionSuggestion]:
        actions: list[SearchActionSuggestion] = []
        if not ready and plan.question_class in {"semantic_discovery", "join_discovery", "table_understanding", "comparative_reasoning"}:
            actions.append(SearchActionSuggestion("sync_catalog", "Search catalog is empty for semantic reasoning."))
        if ready and not rows and plan.question_class in {"semantic_discovery", "entity_lookup", "table_understanding", "comparative_reasoning"}:
            actions.append(SearchActionSuggestion("sync_catalog", "Refresh catalog structure and comments, then retry the question."))
            actions.append(SearchActionSuggestion("refresh_code_evidence", "Refresh code evidence so practical table and column usage can support retrieval."))
        if confidence == "low" and plan.question_class == "join_discovery":
            actions.append(SearchActionSuggestion("refresh_code_evidence", "Code evidence may reveal practical joins that are not explicit in metadata."))
        if confidence == "low" and plan.question_class in {"semantic_discovery", "table_understanding"}:
            resolved = retrieval_details.get("resolved_tables") or []
            if resolved:
                actions.append(SearchActionSuggestion("analyze_table", f"Generate richer metadata for `{resolved[0]}` to improve search quality."))
        if retrieval_details.get("scope_assumption") in {"current_schema", "active_database"}:
            actions.append(SearchActionSuggestion("narrow_scope", "Specify a schema or table to avoid scope assumptions."))
        return actions[:3]

    def ask(self, question: str) -> SearchAnswer:
        clean_question = (question or "").strip()
        question_language = _question_language_hint(clean_question)
        if not clean_question:
            return SearchAnswer(
                intent="unsupported",
                question=question,
                rows=[],
                confidence="low",
                summary="Ask a metadata question inside /search.",
                provenance=[],
                details={"reason": "empty_question"},
            )
        if not self._llm_available():
            return SearchAnswer(
                intent="unsupported",
                question=question,
                rows=[],
                confidence="low",
                summary=(
                    "`/search` tartisma modu aktif bir LLM profili gerektirir. Once `/llm` altinda bir profil tanimlayin."
                    if question_language == "turkish"
                    else "`/search` discussion requires an active LLM profile. Configure one under `/llm` first."
                ),
                provenance=[],
                details={"reason": "no_llm"},
            )

        stage_metrics: list[dict[str, Any]] = []
        try:
            t0 = time.monotonic()
            with step_spinner("Search Agent: interpreting question"):
                plan, interpretation_usage = self._interpret_question(clean_question)
                plan = self._align_answer_language(plan, question_language)
                plan = self._align_plan_shape(plan, clean_question)
            stage_metrics.append({"stage": "interpretation", "duration_sec": round(time.monotonic() - t0, 4)})
        except Exception as exc:
            return SearchAnswer(
                intent="unsupported",
                question=question,
                rows=[],
                confidence="low",
                summary=f"`/search` could not interpret the question with the active LLM profile: {exc}",
                provenance=[],
                details={"reason": "llm_failure", "stage": "interpretation"},
            )

        if plan.out_of_domain or plan.search_mode == "unsupported":
            return SearchAnswer(
                intent="unsupported",
                question=question,
                rows=[],
                confidence="low",
                summary=(
                    "Bu soru metadata sorusu gibi gorunmuyor. `/search`; database, schema, tablo, kolon, join ve metadata anlami icin kullanilir."
                    if plan.answer_language == "turkish"
                    else "This does not look like a metadata question. `/search` is for discussing databases, schemas, tables, columns, joins, and metadata meaning."
                ),
                provenance=[],
                details={
                    "reason": "out_of_domain",
                    "plan": asdict(plan),
                    "question_class": plan.question_class,
                    "tokens": interpretation_usage,
                    "stage_metrics": stage_metrics,
                },
            )

        t0 = time.monotonic()
        with step_spinner("Search Agent: planning retrieval"):
            policy = self._policy_for_plan(plan)
            ready, status = self._catalog_ready()
        stage_metrics.append({"stage": "planning", "duration_sec": round(time.monotonic() - t0, 4)})

        if policy.requires_catalog and not ready:
            actions = [SearchActionSuggestion("sync_catalog", "Search catalog is empty for semantic reasoning.")]
            return SearchAnswer(
                intent=plan.intent,
                question=question,
                rows=[],
                confidence="low",
                summary=(
                    "`/search` katalogu bos. Metadata sorulari icin once `/search sync` veya `/search rebuild` calistirin."
                    if plan.answer_language == "turkish"
                    else "`/search` catalog is empty. Run `/search sync` or `/search rebuild` before asking metadata questions."
                ),
                provenance=[],
                details={
                    "reason": "catalog_not_ready",
                    "status": status,
                    "plan": asdict(plan),
                    "policy": asdict(policy),
                    "actions": [asdict(item) for item in actions],
                    "ambiguity_flags": list(plan.ambiguity_flags),
                    "evidence_sources": [],
                    "stage_metrics": stage_metrics,
                },
            )

        t0 = time.monotonic()
        with step_spinner("Search Agent: retrieving grounded evidence"):
            rows, retrieval_details = self._retrieve(clean_question, plan, policy)
        stage_metrics.append({"stage": "retrieval", "duration_sec": round(time.monotonic() - t0, 4)})

        live_probe_usage: dict[str, Any] = {}
        live_probe: dict[str, Any] = {"executed": False, "operations": []}
        try:
            t0 = time.monotonic()
            with step_spinner("Search Agent: checking evidence gaps"):
                probe_plan, live_probe_usage = self._plan_live_probe(clean_question, plan, policy, rows, retrieval_details)
                live_rows, live_probe = self._execute_live_probe(probe_plan)
                if live_rows:
                    rows = live_rows + rows
                    retrieval_details.setdefault("evidence_sources", [])
                    if "live_db" not in retrieval_details["evidence_sources"]:
                        retrieval_details["evidence_sources"].append("live_db")
                    if "agent_planned_live_probe" not in retrieval_details["evidence_sources"]:
                        retrieval_details["evidence_sources"].append("agent_planned_live_probe")
                    retrieval_details["live_probe"] = live_probe
            stage_metrics.append({"stage": "live_probe", "duration_sec": round(time.monotonic() - t0, 4)})
        except Exception as exc:
            live_probe = {"executed": False, "error": str(exc), "operations": []}
            retrieval_details["live_probe"] = live_probe
            retrieval_details.setdefault("ambiguity_flags", [])
            retrieval_details["ambiguity_flags"].append("live_probe_failed")

        t0 = time.monotonic()
        with step_spinner("Search Agent: verifying high-risk claims"):
            rows, verification = self._verify_rows(plan, policy, rows, retrieval_details)
        stage_metrics.append({"stage": "verification", "duration_sec": round(time.monotonic() - t0, 4)})

        answer_usage: dict[str, Any] = {}
        answer_text = self._deterministic_inventory_answer(plan, rows, retrieval_details) if policy.deterministic_answer else None
        if answer_text is None and live_probe.get("executed"):
            answer_text = self._deterministic_live_probe_answer(plan, rows, live_probe)
        confidence = self._confidence(plan, rows, verification)
        actions = self._action_suggestions(plan, rows, ready, retrieval_details, confidence)
        if answer_text is None:
            try:
                t0 = time.monotonic()
                with step_spinner("Search Agent: synthesizing answer"):
                    answer_text, answer_usage = self._synthesize_answer(clean_question, plan, policy, rows, retrieval_details, verification, actions)
                stage_metrics.append({"stage": "synthesis", "duration_sec": round(time.monotonic() - t0, 4)})
            except Exception as exc:
                return SearchAnswer(
                    intent=plan.intent,
                    question=question,
                    rows=rows,
                    confidence="low",
                    summary=f"`/search` could not synthesize an answer with the active LLM profile: {exc}",
                    provenance=self._provenance(plan, rows, verification),
                    details={
                        "reason": "llm_failure",
                        "stage": "answer",
                        "plan": asdict(plan),
                        "policy": asdict(policy),
                        "retrieval": retrieval_details,
                        "verification": verification,
                        "stage_metrics": stage_metrics,
                    },
                )

        provenance = self._provenance(plan, rows, verification)
        tables = retrieval_details.get("resolved_tables") or []
        if not tables:
            seen_tables: list[str] = []
            for row in rows:
                schema_name = str(row.get("schema_name") or "")
                table_name = str(row.get("table_name") or "")
                if schema_name and table_name:
                    path = f"{schema_name}.{table_name}"
                    if path not in seen_tables:
                        seen_tables.append(path)
            tables = seen_tables[:4]
        columns = [str(row.get("column_name") or "") for row in rows if row.get("column_name")][:6]
        self._remember(
            {
                "question": clean_question,
                "intent": plan.intent,
                "topic": plan.normalized_question or clean_question,
                "tables": tables,
                "columns": [col for col in columns if col],
            }
        )
        return SearchAnswer(
            intent=plan.intent,
            question=question,
            rows=rows,
            confidence=confidence,
            summary=answer_text,
            provenance=provenance,
            details={
                "plan": asdict(plan),
                "policy": asdict(policy),
                "question_class": plan.question_class,
                "retrieval": retrieval_details,
                "verification": verification,
                "display_rows": bool(retrieval_details.get("display_rows", True)),
                "scope": self._scope_from_tables(tables),
                "actions": [asdict(item) for item in actions],
                "ambiguity_flags": list(plan.ambiguity_flags) + list(retrieval_details.get("ambiguity_flags") or []),
                "evidence_sources": retrieval_details.get("evidence_sources", []),
                "stage_metrics": stage_metrics,
                "tokens": _merge_usage(interpretation_usage, live_probe_usage, answer_usage),
                "llm_usage": {
                    "interpretation": interpretation_usage,
                    "live_probe": live_probe_usage,
                    "answer": answer_usage,
                },
            },
        )

    def _scope_from_tables(self, tables: list[str]) -> dict[str, list[str]]:
        scope: dict[str, list[str]] = {}
        for path in tables:
            if "." not in path:
                continue
            schema_name, table_name = path.split(".", 1)
            scope.setdefault(schema_name, [])
            if table_name not in scope[schema_name]:
                scope[schema_name].append(table_name)
        return scope
