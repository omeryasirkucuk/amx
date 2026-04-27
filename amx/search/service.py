"""LLM-backed search orchestration and answer formatting."""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from typing import Any

from amx.config import AMXConfig
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
    entity_hints: list[str]
    search_queries: list[str]
    needs_typo_recovery: bool
    reason: str


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


class SearchService:
    """High-level /search operations."""

    def __init__(self, cfg: AMXConfig, catalog: SearchCatalog):
        self.cfg = cfg
        self.catalog = catalog
        self.db_profile = cfg.active_db_profile or "default"
        self.settings = catalog.get_settings(self.db_profile)
        self._llm: LLMProvider | None = None
        self._memory_key = f"{self.db_profile}:{cfg.active_llm_profile or 'default'}"

    def _llm_available(self) -> bool:
        if self.settings.get("llm_enabled", "true").lower() != "true":
            return False
        return bool(getattr(self.cfg.llm, "provider", "") and getattr(self.cfg.llm, "model", ""))

    def _llm_provider(self) -> LLMProvider:
        if self._llm is None:
            self._llm = LLMProvider(self.cfg.llm)
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
        target_language = self.cfg.llm.language or "english"
        system = (
            "You interpret metadata-search questions for an AMX /search copilot.\n"
            "Return JSON only.\n"
            "You must classify the request and never answer the question itself.\n"
            "Allowed search_mode values: semantic_concept, name_lookup, join_candidates, joinable_tables, table_explain, list_databases, list_schemas, count_tables, unsupported.\n"
            "Allowed intent values: find_columns, join_candidates, explain_table, list_databases, list_schemas, count_tables, unsupported.\n"
            "Set out_of_domain=true for greetings, small talk, or requests not about database metadata.\n"
            "Use entity_hints for schema.table, table names, or important field names. Preserve original spelling even if it looks like a typo.\n"
            "Always include search_queries as a short list of retrieval phrases. For semantic questions, include the user's original wording plus an English canonical retrieval phrase. "
            "If the preferred output language is not English, also include one phrase in that preferred language when useful.\n"
            "normalized_question should be the best canonical English retrieval phrase when the question is not already English.\n"
            "If the question looks like a field/code lookup such as MANDT, VBELN, mangdt, bukrs, choose name_lookup.\n"
            "If the question asks how to join two tables, choose join_candidates.\n"
            "If the question asks which tables can be joined with one table, choose joinable_tables.\n"
            "If the question asks what a table does, choose table_explain.\n"
            "If the question asks which databases are known, choose list_databases.\n"
            "If the question asks which schemas exist in a database or profile, choose list_schemas.\n"
            "If the question asks how many tables exist in a schema or database, choose count_tables.\n"
            "Otherwise choose semantic_concept."
        )
        user = json.dumps(
            {
                "question": question,
                "session_memory": memory,
                "current_schema": self.cfg.current_schema or "",
                "current_table": self.cfg.current_table or "",
                "preferred_output_language": target_language,
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
        plan = SearchPlan(
            intent=str(payload.get("intent") or "find_columns"),
            out_of_domain=bool(payload.get("out_of_domain")),
            normalized_question=str(payload.get("normalized_question") or question).strip() or question,
            search_mode=str(payload.get("search_mode") or "semantic_concept"),
            entity_hints=[str(item).strip() for item in (payload.get("entity_hints") or []) if str(item).strip()],
            search_queries=[
                str(item).strip()
                for item in (payload.get("search_queries") or [])
                if str(item).strip()
            ]
            or [str(payload.get("normalized_question") or question).strip() or question],
            needs_typo_recovery=bool(payload.get("needs_typo_recovery")),
            reason=str(payload.get("reason") or "").strip(),
        )
        return plan, result.usage or {}

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

    def _retrieve(self, question: str, plan: SearchPlan) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        try:
            limit = max(1, int(self.settings.get("max_retrieved_entities", self.settings.get("max_results", "8"))))
        except Exception:
            limit = 8
        details: dict[str, Any] = {
            "search_mode": plan.search_mode,
            "entity_hints": list(plan.entity_hints),
            "search_queries": list(plan.search_queries),
        }
        if plan.search_mode == "join_candidates":
            table_paths = self._resolve_table_paths(plan.entity_hints, question)
            details["resolved_tables"] = table_paths[:2]
            if len(table_paths) < 2:
                return [], details
            rows = self.catalog.join_candidates(self.db_profile, table_paths[0], table_paths[1], limit=limit)
            return rows, details
        if plan.search_mode == "joinable_tables":
            table_paths = self._resolve_table_paths(plan.entity_hints, question)
            details["resolved_tables"] = table_paths[:1]
            if not table_paths:
                return [], details
            rows = self.catalog.joinable_tables(self.db_profile, table_paths[0], limit=limit)
            details["display_rows"] = True
            return rows, details
        if plan.search_mode == "table_explain":
            table_paths = self._resolve_table_paths(plan.entity_hints, question)
            details["resolved_tables"] = table_paths[:1]
            if not table_paths:
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
            return [table_row, *top_columns], details
        if plan.search_mode == "name_lookup":
            query_text = plan.entity_hints[0] if plan.entity_hints else plan.normalized_question
            rows = self.catalog.name_search_columns(self.db_profile, query_text, limit=limit)
            details["query_text"] = query_text
            return rows, details
        if plan.search_mode == "list_databases":
            rows = self.catalog.known_databases(self.db_profile)
            details["display_rows"] = False
            details["result_kind"] = "catalog_overview"
            return rows, details
        if plan.search_mode == "list_schemas":
            database_name = ""
            known_databases = {
                str(row.get("database_name") or "").lower(): str(row.get("database_name") or "")
                for row in self.catalog.known_databases(self.db_profile)
            }
            for hint in plan.entity_hints:
                normalized = str(hint or "").strip().lower()
                if normalized and "." not in normalized and normalized in known_databases:
                    database_name = known_databases[normalized]
                    break
            rows = self.catalog.known_schemas(self.db_profile, database_name=database_name or None)
            details["database_name"] = database_name
            details["display_rows"] = False
            details["result_kind"] = "catalog_overview"
            return rows, details
        if plan.search_mode == "count_tables":
            schema_name = ""
            database_name = ""
            schema_lookup = {
                str(row.get("schema_name") or "").lower(): str(row.get("schema_name") or "")
                for row in self.catalog.known_schemas(self.db_profile)
            }
            db_lookup = {
                str(row.get("database_name") or "").lower(): str(row.get("database_name") or "")
                for row in self.catalog.known_databases(self.db_profile)
            }
            for hint in plan.entity_hints:
                normalized = str(hint or "").strip().lower()
                if not normalized or "." in normalized:
                    continue
                if normalized in schema_lookup and not schema_name:
                    schema_name = schema_lookup[normalized]
                    continue
                if normalized in db_lookup and not database_name:
                    database_name = db_lookup[normalized]
                    continue
                if self.catalog.find_table_candidates(self.db_profile, normalized, limit=1):
                    table_paths = self._resolve_table_paths([normalized], question)
                    if table_paths and not schema_name:
                        schema_name = table_paths[0].split(".", 1)[0]
                        continue
            if not schema_name and self.cfg.current_schema:
                schema_name = self.cfg.current_schema
            count = self.catalog.count_tables(
                self.db_profile,
                schema_name=schema_name or None,
                database_name=database_name or None,
            )
            details["schema_name"] = schema_name
            details["database_name"] = database_name
            details["display_rows"] = False
            details["result_kind"] = "aggregate"
            return [{"row_type": "aggregate", "metric": "table_count", "value": count}], details
        rows = self.catalog.search_columns(
            self.db_profile,
            plan.normalized_question or question,
            limit=limit,
            entity_hints=plan.entity_hints,
            query_variants=plan.search_queries,
        )
        return rows, details

    def _rows_for_prompt(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        payload: list[dict[str, Any]] = []
        for row in rows[:10]:
            item = {
                "schema": row.get("schema_name", ""),
                "table": row.get("table_name", ""),
                "column": row.get("column_name", ""),
                "target_schema": row.get("target_schema_name", ""),
                "target_table": row.get("target_table_name", ""),
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
            }
            payload.append(item)
        return payload

    def _synthesize_answer(
        self,
        question: str,
        plan: SearchPlan,
        rows: list[dict[str, Any]],
        retrieval_details: dict[str, Any],
    ) -> tuple[str, dict[str, Any]]:
        llm = self._llm_provider()
        target_language = self.cfg.llm.language or "english"
        system = (
            "You are AMX /search, a metadata copilot.\n"
            "Answer only from the retrieved metadata evidence you are given.\n"
            "If evidence is weak or empty, say so explicitly and suggest a narrower follow-up.\n"
            "Reject small-talk or out-of-domain input by saying /search is for metadata discussion.\n"
            "Keep the answer concise, practical, and grounded.\n"
            "Do not invent table names, joins, or column meanings that are not in the evidence.\n"
            f"Write the final answer in {target_language}."
        )
        user = json.dumps(
            {
                "question": question,
                "plan": asdict(plan),
                "session_memory": self._memory_summary(),
                "retrieval_details": retrieval_details,
                "rows": self._rows_for_prompt(rows),
            },
            ensure_ascii=True,
        )
        result = llm.chat(
            [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            temperature=0.1,
            max_tokens=1200,
            use_logprobs=False,
        )
        return result.content.strip(), result.usage or {}

    def _provenance(self, plan: SearchPlan, rows: list[dict[str, Any]]) -> list[str]:
        labels: list[str] = []
        if plan.search_mode == "join_candidates":
            labels.append("structural relationships")
        if plan.search_mode == "joinable_tables":
            labels.append("structural relationships")
            labels.append("catalog relationships")
        if plan.search_mode == "table_explain":
            labels.append("effective table metadata")
        if plan.search_mode in {"list_databases", "list_schemas", "count_tables"}:
            labels.append("catalog inventory")
        if plan.search_mode == "name_lookup":
            labels.append("exact or fuzzy field-name matching")
        elif plan.search_mode not in {"list_databases", "list_schemas", "count_tables", "joinable_tables"}:
            labels.append("effective metadata")
        if any((row.get("source") or "") == "code" for row in rows):
            labels.append("behavioral code evidence")
        if any(str(row.get("relationship_type") or "").startswith("foreign_key") or str(row.get("relationship_type") or "") == "incoming_foreign_key" for row in rows):
            labels.append("foreign key relationships")
        if self.settings.get("allow_vector_support", "true").lower() == "true" and plan.search_mode == "semantic_concept":
            labels.append("vector support")
        return labels

    def _confidence(self, plan: SearchPlan, rows: list[dict[str, Any]]) -> str:
        if plan.out_of_domain or not rows:
            return "low"
        if plan.search_mode in {"list_databases", "list_schemas", "count_tables"}:
            return "high"
        top = rows[0]
        if plan.search_mode in {"join_candidates", "joinable_tables"}:
            if str(top.get("relationship_type") or "") in {"foreign_key", "incoming_foreign_key"}:
                return "high"
            return "medium"
        score = float(top.get("rank_score") or top.get("score") or 0.0)
        if plan.search_mode == "name_lookup":
            return "high" if score >= 8.0 else "medium"
        if score >= 5.0:
            return "high"
        if score >= 4.0:
            return "medium"
        return "low"

    def ask(self, question: str) -> SearchAnswer:
        clean_question = (question or "").strip()
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
                summary="`/search` discussion requires an active LLM profile. Configure one under `/llm` first.",
                provenance=[],
                details={"reason": "no_llm"},
            )
        ready, status = self._catalog_ready()
        if not ready:
            return SearchAnswer(
                intent="unsupported",
                question=question,
                rows=[],
                confidence="low",
                summary="`/search` catalog is empty. Run `/search sync` or `/search rebuild` before asking metadata questions.",
                provenance=[],
                details={"reason": "catalog_not_ready", "status": status},
            )
        try:
            with step_spinner("Search Copilot: interpreting question"):
                plan, interpretation_usage = self._interpret_question(clean_question)
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
                summary="This does not look like a metadata question. `/search` is for discussing databases, schemas, tables, columns, joins, and metadata meaning.",
                provenance=[],
                details={
                    "reason": "out_of_domain",
                    "plan": asdict(plan),
                    "tokens": interpretation_usage,
                },
            )
        with step_spinner("Search Copilot: retrieving grounded evidence"):
            rows, retrieval_details = self._retrieve(clean_question, plan)
        try:
            with step_spinner("Search Copilot: synthesizing answer"):
                answer_text, answer_usage = self._synthesize_answer(clean_question, plan, rows, retrieval_details)
        except Exception as exc:
            return SearchAnswer(
                intent=plan.intent,
                question=question,
                rows=rows,
                confidence="low",
                summary=f"`/search` could not synthesize an answer with the active LLM profile: {exc}",
                provenance=self._provenance(plan, rows),
                details={
                    "reason": "llm_failure",
                    "stage": "answer",
                    "plan": asdict(plan),
                    "retrieval": retrieval_details,
                },
            )
        confidence = self._confidence(plan, rows)
        provenance = self._provenance(plan, rows)
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
                "retrieval": retrieval_details,
                "display_rows": bool(retrieval_details.get("display_rows", True)),
                "scope": self._scope_from_tables(tables),
                "tokens": _merge_usage(interpretation_usage, answer_usage),
                "llm_usage": {
                    "interpretation": interpretation_usage,
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

    def explain(self, question: str) -> dict[str, Any]:
        answer = self.ask(question)
        return {
            "intent": answer.intent,
            "question": answer.question,
            "confidence": answer.confidence,
            "summary": answer.summary,
            "provenance": answer.provenance,
            "rows": answer.rows,
            "details": answer.details,
        }
