"""Search orchestration and answer formatting."""

from __future__ import annotations

from typing import Any

from amx.config import AMXConfig
from amx.search.catalog import SearchAnswer, SearchCatalog


class SearchService:
    """High-level /search operations."""

    def __init__(self, cfg: AMXConfig, catalog: SearchCatalog):
        self.cfg = cfg
        self.catalog = catalog
        self.db_profile = cfg.active_db_profile or "default"

    def infer_intent(self, question: str) -> str:
        text = question.lower()
        if " join " in f" {text} " or "hangi kolon" in text and "join" in text:
            return "join_candidates"
        if "joinlerken" in text or "join" in text and "tabl" in text:
            return "join_candidates"
        return "find_columns"

    def ask(self, question: str) -> SearchAnswer:
        intent = self.infer_intent(question)
        if intent == "join_candidates":
            table_paths = [token.strip(" ,.;") for token in question.split() if token.count(".") == 1]
            if len(table_paths) < 2:
                return SearchAnswer(
                    intent=intent,
                    question=question,
                    rows=[],
                    confidence="low",
                    summary="Join-oriented questions need two explicit `schema.table` names.",
                    provenance=[],
                    details={"reason": "missing_table_paths"},
                )
            rows = self.catalog.join_candidates(self.db_profile, table_paths[0], table_paths[1])
            provenance = ["foreign key relationship", "heuristic name/type compatibility", "behavioral code evidence"]
            summary = (
                f"Top join candidates between {table_paths[0]} and {table_paths[1]}."
                if rows
                else f"No join candidates found for {table_paths[0]} and {table_paths[1]}."
            )
            confidence = "high" if rows and rows[0].get("relationship_type") in {"foreign_key", "incoming_foreign_key"} else ("medium" if rows else "low")
            return SearchAnswer(intent, question, rows, confidence, summary, provenance, {"tables": table_paths[:2]})

        rows = self.catalog.search_columns(self.db_profile, question)
        provenance = ["effective metadata", "vector similarity", "exact token overlap", "behavioral code evidence"]
        summary = (
            f"Top column matches for: {question}"
            if rows
            else f"No catalog matches found for: {question}"
        )
        confidence = "high" if rows and float(rows[0].get("rank_score") or 0.0) >= 6.0 else ("medium" if rows else "low")
        return SearchAnswer(intent, question, rows, confidence, summary, provenance, {})

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
