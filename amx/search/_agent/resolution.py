"""Target / explicit-mention resolution for ``SearchAgent``.

Methods that translate a free-form question into concrete catalog
identifiers (schema.table[.column]) before retrieval. The cluster
covers:

* Explicit table-name extraction from the question text
  (``_explicit_table_mentions_for_question``,
  ``_explicit_table_paths_for_question``).
* Live-DB existence checks (``_live_table_exists``).
* Candidate-path generation (``_table_candidate_paths``,
  ``_candidate_table_paths_for_question``,
  ``_resolve_table_paths``, ``_candidate_limit``).
* Final target resolution + ambiguity detection
  (``_resolve_table_targets``, ``_target_resolution_details``).
* Column-name listing detection (``_asks_column_name_listing``,
  ``_column_name_lookup_terms``).
* Catalog-resolvability check (``_catalog_resolvable_subject``).

Reads from ``self.catalog``, ``self.db``, ``self.cfg``,
``self.db_profile``. Calls back into planning helpers
(``_align_answer_language``) and itself.
"""

from __future__ import annotations

import re
from typing import Any

from amx.utils.logging import get_logger

ResolvedTarget = Any  # Forward reference to dataclass in agent.py.

log = get_logger("search.agent.resolution")


class ResolutionMixin:
    """Target / explicit-mention resolution methods for ``SearchAgent``."""

    def _catalog_resolvable_subject(self, question: str) -> str | None:
        """Return the first explicit subject token we can confirm is a
        table — either because the user explicitly called it a "table"
        ("vbrk table" / "tablo X"), or because the catalog / live DB has
        it under that exact name.

        Used to ground-truth re-routing: we override the LLM's mode to
        ``table_explain`` when the user named a real table.
        Column-shaped tokens like "vbrk_id" don't reach a strong-mention
        branch and won't be confirmed by the catalog as a table, so they
        skip the override.
        """
        for mention in self._explicit_table_mentions_for_question(question):
            requested = str(mention.get("requested") or "").strip()
            if not requested:
                continue
            # Strong mentions (user said "X table" / "table X" / "schema.table")
            # don't need extra confirmation. The user explicitly called the
            # noun a table, so we trust the route. ``_resolve_table_targets``
            # will still surface "not found" cleanly if the catalog and
            # live DB both come up empty.
            if str(mention.get("strength") or "") == "strong":
                return requested
            try:
                rows = self.catalog.find_tables_by_exact_name(self.db_profile, requested, limit=2)
            except Exception:
                rows = []
            if rows:
                return requested
            # Weak mention not in catalog — last chance is the live DB.
            # Cheap when ``current_schema`` is set; we skip the schema
            # iteration when it isn't (would require N HEAD queries).
            current_schema = (self.cfg.current_schema or "").strip()
            if current_schema:
                try:
                    if self._live_table_exists(current_schema, requested) is True:
                        return requested
                except Exception:
                    pass
        return None
    def _explicit_table_mentions_for_question(self, question: str) -> list[dict[str, str]]:
        mentions: list[dict[str, str]] = []
        seen: set[str] = set()
        # Inline ``schema.table`` references — strongest possible signal.
        for inline in re.findall(r"\b([A-Za-z0-9_]+\.[A-Za-z0-9_]+)\b", question or ""):
            parts = inline.split(".", 1)
            if len(parts) != 2:
                continue
            path = f"{parts[0]}.{parts[1]}"
            if path.lower() not in seen:
                seen.add(path.lower())
                mentions.append(
                    {
                        "requested": inline,
                        "path": path,
                        "source": "explicit_schema_table",
                        # ``strength`` distinguishes catch-strength so the
                        # alignment guard knows when the user explicitly
                        # called the noun a "table" (high — override LLM
                        # unconditionally) vs. just named a subject in a
                        # "what's the X" form (medium — require catalog or
                        # live confirmation before overriding).
                        "strength": "strong",
                    }
                )
        # Strong-signal tokens: user explicitly says "X table" / "table X" /
        # "X tablo" / "tablo X". User CALLED IT A TABLE, so we don't need
        # extra catalog or live confirmation to trust the routing.
        strong_tokens: list[str] = []
        strong_tokens.extend(
            item
            for item in re.findall(
                r"\b([A-Za-z_][A-Za-z0-9_]{1,127})\s+(?:table|tablo|tablolar|tablosu|tablosunda|tablosuna|tablosundan|tabloları|tablosunu)\b",
                question or "",
                flags=re.IGNORECASE,
            )
        )
        strong_tokens.extend(
            item
            for item in re.findall(
                r"\b(?:table|tables|tablo|tablolar|tablosu)\s+([A-Za-z_][A-Za-z0-9_]{1,127})\b",
                question or "",
                flags=re.IGNORECASE,
            )
        )
        # Weak-signal tokens: user said "what's the X" / "describe X" /
        # "X nedir" — the noun MIGHT be a column or a generic entity, so
        # the alignment guard should require catalog / live confirmation
        # before overriding the LLM's chosen mode.
        weak_tokens: list[str] = []
        subject_patterns = (
            # English: what's/what is/describe/explain/tell me about/show me X
            r"\b(?:what'?s|what is|what are|whats|describe|explain|define|tell\s+me\s+about|"
            r"show\s+me|info\s+(?:on|about)|details?\s+(?:on|about)|definition\s+of|"
            r"meaning\s+of|purpose\s+of)\s+(?:the\s+|a\s+|an\s+)?"
            r"`?([A-Za-z_][A-Za-z0-9_]{1,127})`?\b",
            # English: what does X do/store/contain/mean
            r"\b(?:what\s+does|what\s+do)\s+`?([A-Za-z_][A-Za-z0-9_]{1,127})`?\s+"
            r"(?:do|mean|store|contain|hold|represent)\b",
            # Turkish: <X> nedir / hakkında / hakkinda / ne işe yarar / ne demek
            r"\b`?([A-Za-z_][A-Za-z0-9_]{1,127})`?\s+(?:nedir|ne\s+demek|"
            r"hakk[ıi]nda|ne\s+i[şs]e\s+yarar|ne\s+i[şs]\s+yapar)\b",
            # Turkish: bana <X> hakkında bilgi ver / <X>'i anlat / <X>'i açıkla
            r"\b(?:bana\s+)?(?:bahset|anlat|a[çc][ıi]kla|tan[ıi]t)\s+"
            r"(?:bana\s+)?`?([A-Za-z_][A-Za-z0-9_]{1,127})`?\b",
        )
        for pattern in subject_patterns:
            weak_tokens.extend(
                item
                for item in re.findall(pattern, question or "", flags=re.IGNORECASE)
            )
        table_token_stopwords = {
            "nedir",
            "ne",
            "what",
            "is",
            "are",
            "hangi",
            "hangileri",
            "var",
            "mi",
            "mı",
            "mu",
            "mü",
            # Question/quantifier words that precede "table" without naming one.
            # Without these, a question like "which table has the most rows"
            # is misread as a request for a literal table named "which".
            "which",
            "this",
            "that",
            "these",
            "those",
            "each",
            "every",
            "any",
            "all",
            "some",
            "no",
            "many",
            "much",
            # Superlatives often paired with "table" in aggregations.
            "biggest",
            "largest",
            "smallest",
            "best",
            "worst",
            "top",
            "bottom",
            "first",
            "last",
            "primary",
            "main",
            "the",
            # English verbs/prepositions that commonly follow "table" without
            # being a table name (e.g., "the table has the most rows" -> "has").
            "has",
            "have",
            "had",
            "with",
            "in",
            "on",
            "of",
            "for",
            "by",
            "from",
            "to",
            "into",
            "and",
            "or",
            "but",
            "contains",
            "contain",
            "shows",
            "show",
            "named",
            "called",
            # The new subject-form regex captures the noun that follows
            # "what's the / describe / explain". Filter generic meta-words
            # so e.g. "describe table" does not extract "table" as a name.
            "table",
            "tables",
            "tablo",
            "tablolar",
            # All inflected Turkish forms of "tablo" we already accept in the
            # other regex branch — they must also drop out of subject capture.
            "tablosu",
            "tablosunda",
            "tablosuna",
            "tablosundan",
            "tablosunu",
            "tabloları",
            "tablolarını",
            "tablolardan",
            "column",
            "columns",
            "kolon",
            "kolonlar",
            "field",
            "fields",
            "alan",
            "alanlar",
            "data",
            "info",
            "information",
            "metadata",
            "veri",
            "bilgi",
            "schema",
            "schemas",
            "sema",
            "şema",
            "şemalar",
            "semalar",
            "database",
            "databases",
            "veritaban",
            "veritabani",
            "veritabanı",
            # Generic adjectives that might land after "what's the".
            "most",
            "least",
            "popular",
            "common",
            "single",
            "multiple",
            "total",
            "average",
            "newest",
            "oldest",
            "recent",
            "older",
            "newer",
        }
        strong_tokens = [t for t in strong_tokens if t.lower() not in table_token_stopwords]
        weak_tokens = [t for t in weak_tokens if t.lower() not in table_token_stopwords]
        # Emit strong tokens first so they appear before weak ones — both
        # the alignment guard and ``_resolve_table_targets`` walk this list
        # in order and we want the high-confidence match to win when both
        # branches captured the same noun.
        for tokens, strength, source_qualified, source_unqualified in (
            (strong_tokens, "strong", "explicit_current_schema", "explicit_unqualified_table"),
            (weak_tokens, "weak", "subject_form_current_schema", "subject_form_unqualified"),
        ):
            for token in tokens:
                if self.cfg.current_schema:
                    path = f"{self.cfg.current_schema}.{token}"
                    key = path.lower()
                    if key in seen:
                        continue
                    seen.add(key)
                    mentions.append(
                        {"requested": token, "path": path, "source": source_qualified, "strength": strength}
                    )
                else:
                    key = token.lower()
                    if key in seen:
                        continue
                    seen.add(key)
                    mentions.append(
                        {"requested": token, "path": "", "source": source_unqualified, "strength": strength}
                    )
        return mentions
    def _explicit_table_paths_for_question(self, question: str) -> list[str]:
        paths: list[str] = []
        seen: set[str] = set()
        for mention in self._explicit_table_mentions_for_question(question):
            path = str(mention.get("path") or "")
            if path and path.lower() not in seen:
                seen.add(path.lower())
                paths.append(path)
        return paths
    def _live_table_exists(self, schema_name: str, table_name: str) -> bool | None:
        """Return exact live existence when cheap metadata APIs are available."""
        db = self._inventory_db()
        target = table_name.lower()
        try:
            if hasattr(db, "list_assets"):
                return any(str(name).lower() == target for name, _kind in db.list_assets(schema_name))
        except Exception:
            pass
        checks = ("list_tables", "list_views", "list_materialized_views")
        found_any_api = False
        for method_name in checks:
            method = getattr(db, method_name, None)
            if not callable(method):
                continue
            found_any_api = True
            try:
                if any(str(name).lower() == target for name in method(schema_name)):
                    return True
            except Exception:
                return None
        return False if found_any_api else None
    def _table_candidate_paths(self, hint: str, *, limit: int = 5) -> list[str]:
        paths: list[str] = []
        seen: set[str] = set()
        for candidate in self.catalog.find_table_candidates(self.db_profile, hint, limit=limit):
            schema_name = str(candidate.get("schema_name") or "")
            table_name = str(candidate.get("table_name") or "")
            path = f"{schema_name}.{table_name}" if schema_name and table_name else ""
            if path and path.lower() not in seen:
                seen.add(path.lower())
                paths.append(path)
        return paths
    def _resolve_table_targets(self, hints: list[str], question: str) -> list[ResolvedTarget]:
        targets: list[ResolvedTarget] = []
        seen: set[str] = set()
        explicit_mentions = self._explicit_table_mentions_for_question(question)
        for mention in explicit_mentions:
            path = str(mention.get("path") or "")
            requested = str(mention.get("requested") or path)
            if "." not in path:
                # Unqualified mention (e.g. user typed "what's the vbrk"
                # without a current_schema). Look up the bare token in the
                # catalog: if it lives in exactly one schema, resolve to it;
                # if it lives in several, surface them as ambiguity
                # candidates instead of silently picking one; if it lives
                # in none, mark as "explicit_table_not_found_live" so the
                # deterministic answer template explains that to the user.
                bare = requested.strip()
                if not bare:
                    continue
                exact_rows = self.catalog.find_tables_by_exact_name(self.db_profile, bare, limit=20)
                exact_paths = [
                    f"{str(row.get('schema_name') or '')}.{str(row.get('table_name') or '')}".strip(".")
                    for row in exact_rows
                    if str(row.get("schema_name") or "") and str(row.get("table_name") or "")
                ]
                if len(exact_paths) == 1:
                    schema_name, table_name = exact_paths[0].split(".", 1)
                    exists = self._live_table_exists(schema_name, table_name)
                    target = ResolvedTarget(
                        requested=requested,
                        resolved_path=exact_paths[0],
                        source=str(mention.get("source") or "explicit_unqualified_table"),
                        is_exact=True,
                        confidence="high" if exists is True else "medium",
                        warnings=[] if exists is True else ["live_table_existence_unknown"],
                        candidates=[],
                    )
                elif len(exact_paths) >= 2:
                    target = ResolvedTarget(
                        requested=requested,
                        resolved_path="",
                        source=str(mention.get("source") or "explicit_unqualified_table"),
                        is_exact=False,
                        confidence="medium",
                        warnings=["ambiguous_unqualified_table"],
                        candidates=exact_paths[:5],
                    )
                else:
                    fuzzy = self._table_candidate_paths(bare, limit=3)
                    target = ResolvedTarget(
                        requested=requested,
                        resolved_path="",
                        source=str(mention.get("source") or "explicit_unqualified_table"),
                        is_exact=False,
                        confidence="low",
                        warnings=["explicit_table_not_found_live"],
                        candidates=fuzzy,
                    )
                key = (target.resolved_path or target.requested).lower()
                if key not in seen:
                    seen.add(key)
                    targets.append(target)
                continue
            schema_name, table_name = path.split(".", 1)
            exists = self._live_table_exists(schema_name, table_name)
            candidates = self._table_candidate_paths(table_name, limit=3)
            if exists is False:
                target = ResolvedTarget(
                    requested=requested,
                    resolved_path="",
                    source=str(mention.get("source") or "explicit"),
                    is_exact=False,
                    confidence="low",
                    warnings=["explicit_table_not_found_live"],
                    candidates=candidates,
                )
            else:
                warnings = [] if exists is True else ["live_table_existence_unknown"]
                target = ResolvedTarget(
                    requested=requested,
                    resolved_path=path,
                    source=str(mention.get("source") or "explicit"),
                    is_exact=True,
                    confidence="high" if exists is True else "medium",
                    warnings=warnings,
                    candidates=[],
                )
            key = (target.resolved_path or target.requested).lower()
            if key not in seen:
                seen.add(key)
                targets.append(target)
        if targets:
            return targets

        for path in self._resolve_table_paths(hints, question):
            if "." not in path or path.lower() in seen:
                continue
            schema_name, table_name = path.split(".", 1)
            exists = self._live_table_exists(schema_name, table_name)
            targets.append(
                ResolvedTarget(
                    requested=table_name,
                    resolved_path=path,
                    source="hint_or_memory",
                    is_exact=exists is not False,
                    confidence="medium" if exists is not False else "low",
                    warnings=[] if exists is True else ["live_table_existence_unknown" if exists is None else "resolved_table_not_found_live"],
                    candidates=[],
                )
            )
            seen.add(path.lower())
        return targets
    def _target_resolution_details(self, targets: list[ResolvedTarget]) -> dict[str, Any]:
        has_resolved = any(bool(target.resolved_path) for target in targets)
        has_unresolved_explicit = any(
            not target.resolved_path
            and (
                "explicit_table_not_found_live" in target.warnings
                or "ambiguous_unqualified_table" in target.warnings
            )
            for target in targets
        )
        has_ambiguous = any(
            not target.resolved_path and "ambiguous_unqualified_table" in target.warnings
            for target in targets
        )
        return {
            "targets": [asdict(target) for target in targets],
            "unresolved_explicit": has_unresolved_explicit and not has_resolved,
            "ambiguous_unqualified": has_ambiguous and not has_resolved,
        }
    def _candidate_table_paths_for_question(self, hints: list[str], question: str) -> list[str]:
        candidates = self._explicit_table_paths_for_question(question)
        seen = {item.lower() for item in candidates}
        for path in self._resolve_table_paths(hints, question):
            if path.lower() not in seen:
                seen.add(path.lower())
                candidates.append(path)
        explicit_tokens = {
            path.split(".", 1)[1].lower()
            for path in candidates
            if "." in path
        }
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
            and token.lower() not in explicit_tokens
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
    def _asks_column_name_listing(self, question: str, plan: SearchPlan) -> bool:
        sample = (question or "").strip().lower()
        asks_column = any(token in sample for token in ("kolon", "kolonlar", "column", "columns", "field", "fields"))
        asks_names = any(token in sample for token in ("isim", "isimleri", "name", "names", "getir", "listele", "list"))
        asks_comment_coverage = any(token in sample for token in ("comment", "comments", "commentler", "yorum", "yorumlar", "girili", "coverage"))
        return asks_column and asks_names and not asks_comment_coverage and plan.question_class == "semantic_discovery" and plan.target_entity in {"column", "unknown", ""}
    def _column_name_lookup_terms(self, question: str, plan: SearchPlan) -> list[str]:
        stopwords = {
            "ile",
            "alakali",
            "alakalı",
            "ilgili",
            "tüm",
            "tum",
            "kolon",
            "kolonlar",
            "kolonu",
            "column",
            "columns",
            "field",
            "fields",
            "isim",
            "isimleri",
            "name",
            "names",
            "getir",
            "listele",
            "list",
            "all",
            "which",
            "hangi",
            "related",
            "with",
        }
        terms: list[str] = []
        for token in re.findall(r"\b[A-Za-z_][A-Za-z0-9_]{1,127}\b", question or ""):
            normalized = token.lower()
            if normalized in stopwords or normalized in terms:
                continue
            terms.append(normalized)
        return terms[:4]


__all__ = ["ResolutionMixin"]
