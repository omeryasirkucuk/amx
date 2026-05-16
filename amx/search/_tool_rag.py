"""Docs/code RAG tools for :class:`ToolBox`.

``search_docs`` and ``search_code`` query the user's local documentation
and code embedding stores. They share two needs from the host
``ToolBox``:

* ``self.cfg`` — for resolving doc/code profile scope.
* ``self.db_profiles`` — the active db-profile set for scoping.

The mixin is compose-only — it never overrides ``ToolBox.__init__``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from amx.config import AMXConfig


class _RagToolsMixin:
    """Docs and code embedding-search tool implementations."""

    # Provided by the host ``ToolBox`` instance.
    cfg: AMXConfig
    db_profiles: list[str]

    def _tool_search_docs(self, query: str, n_results: int = 5) -> dict[str, Any]:
        from amx.search._agent.scope import resolve_doc_profiles_for_scope

        q = (query or "").strip()
        if not q:
            return {"hits": [], "count": 0, "reason": "empty_query"}
        n = max(1, min(int(n_results or 5), 10))

        doc_override = getattr(self, "_doc_profiles_override", None)
        if doc_override is not None:
            # Explicit user pick from the Studio dropdown or the CLI
            # ``--doc-profile`` flag. Empty list = "skip doc retrieval
            # for this question" — honoured without falling back to
            # the auto-resolved set.
            profiles = [p for p in doc_override if p in self.cfg.doc_profiles]
            override_in_effect = True
        else:
            profiles = resolve_doc_profiles_for_scope(self.cfg, self.db_profiles)
            override_in_effect = False
        if not profiles:
            return {
                "hits": [],
                "count": 0,
                "reason": "no_docs_selected" if override_in_effect else "no_docs_for_scope",
                "scope_dbs": list(self.db_profiles),
            }

        # Build the source-filter list from every in-scope doc profile's
        # configured source paths. A single RAGStore handles the union;
        # source_filters scopes ``query()`` to chunks whose ``source`` /
        # ``source_root`` metadata starts with one of these prefixes.
        source_paths: list[str] = []
        for prof in profiles:
            for path in self.cfg.doc_profiles.get(prof, []) or []:
                if path and path not in source_paths:
                    source_paths.append(path)

        try:
            from amx.docs.rag import RAGStore

            store = RAGStore(source_filters=source_paths)
            if store.filtered_doc_count() == 0:
                return {
                    "hits": [],
                    "count": 0,
                    "reason": "no_docs_for_scope",
                    "doc_profiles": profiles,
                    "scope_dbs": list(self.db_profiles),
                }
            raw_hits = store.query(q, n_results=n)
        except Exception as exc:
            return {"hits": [], "count": 0, "error": f"rag_query_failed: {exc}"}

        hits: list[dict[str, Any]] = []
        for h in raw_hits:
            meta = h.get("metadata") or {}
            text = str(h.get("text") or "")
            # Token-budget hygiene: every snippet capped at ~1.2K chars
            # so a /ask question pulling 5 hits never blows past 6KB —
            # well inside the 60K floor budget.
            if len(text) > 1200:
                text = text[:1200] + "…"
            hits.append(
                {
                    "source": meta.get("source") or meta.get("source_root") or "",
                    "source_type": meta.get("source_type") or "",
                    "snippet": text,
                    "distance": h.get("distance"),
                    # PR E: chunk_idx round-trips from RAGStore's ingest
                    # metadata so the /ask citation summary renders
                    # ``pdf.pdf:5`` exactly like PR C's RunDetail rows.
                    # Falls back to 0 when missing so older collections
                    # without the metadata key don't blow up the parse.
                    "chunk_idx": int(meta.get("chunk_idx") or 0),
                }
            )
        return {
            "hits": hits,
            "count": len(hits),
            "doc_profiles": profiles,
            "scope_dbs": list(self.db_profiles),
        }

    def _tool_search_code(
        self,
        query: str,
        n_results: int = 5,
        table_filter: str | None = None,
    ) -> dict[str, Any]:
        from amx.search._agent.scope import resolve_code_profiles_for_scope

        q = (query or "").strip()
        tbl = (table_filter or "").strip()
        if not q and not tbl:
            return {"hits": [], "count": 0, "reason": "empty_query"}
        n = max(1, min(int(n_results or 5), 10))

        code_override = getattr(self, "_code_profiles_override", None)
        if code_override is not None:
            profiles = [p for p in code_override if p in self.cfg.code_profiles]
            code_override_in_effect = True
        else:
            profiles = resolve_code_profiles_for_scope(self.cfg, self.db_profiles)
            code_override_in_effect = False
        if not profiles:
            return {
                "hits": [],
                "count": 0,
                "reason": "no_code_selected" if code_override_in_effect else "no_code_for_scope",
                "scope_dbs": list(self.db_profiles),
            }

        source_paths: list[str] = []
        for prof in profiles:
            path = self.cfg.code_profiles.get(prof, "") or ""
            if path and path not in source_paths:
                source_paths.append(path)

        # Bias the query string with the table name when the LLM wants
        # callsite-style results — the underlying Chroma collection is
        # text-only, so concatenating ``"<query> <table>"`` is the
        # cheapest way to lift table-mentioning chunks without a where
        # clause (codebase metadata is path-shaped, not table-shaped).
        composite = f"{q} {tbl}".strip() if tbl else q

        try:
            from amx.codebase.code_rag import code_collection_count, query_code_snippets

            if code_collection_count(source_filters=source_paths) == 0:
                return {
                    "hits": [],
                    "count": 0,
                    "reason": "no_code_for_scope",
                    "code_profiles": profiles,
                    "scope_dbs": list(self.db_profiles),
                }
            raw_hits = query_code_snippets(composite, n_results=n, source_filters=source_paths)
        except Exception as exc:
            return {"hits": [], "count": 0, "error": f"code_query_failed: {exc}"}

        hits: list[dict[str, Any]] = []
        for h in raw_hits:
            meta = h.get("metadata") or {}
            text = str(h.get("text") or "")
            if len(text) > 1200:
                text = text[:1200] + "…"
            # PR γ: surface the chunk's line range + chunk_id from
            # metadata so ``_summarise_tool_call`` can build citations
            # that render as ``src/foo.py:120-145`` in /ask. Falls back
            # to ``0`` / ``None`` for chunks indexed before PR γ — the
            # renderer special-cases the missing-line case to show
            # ``path`` only.
            start_line_raw = meta.get("start_line")
            end_line_raw = meta.get("end_line")
            try:
                start_line = int(start_line_raw) if start_line_raw is not None else 0
            except (TypeError, ValueError):
                start_line = 0
            try:
                end_line = int(end_line_raw) if end_line_raw is not None else 0
            except (TypeError, ValueError):
                end_line = 0
            chunk_id_raw = meta.get("chunk_id") or 0
            try:
                # ``chunk_id`` is the string-ish key (e.g. ``"func_42"``)
                # the indexer produced. Numeric coercion is best-effort —
                # callers should rely on ``line_range`` for provenance.
                chunk_idx = int(chunk_id_raw)
            except (TypeError, ValueError):
                chunk_idx = 0
            hits.append(
                {
                    "source": meta.get("source") or meta.get("rel_path") or "",
                    "rel_path": meta.get("rel_path") or "",
                    "symbol": meta.get("symbol") or meta.get("kind") or "",
                    "snippet": text,
                    "distance": h.get("distance"),
                    "chunk_idx": chunk_idx,
                    "start_line": start_line,
                    "end_line": end_line,
                }
            )
        return {
            "hits": hits,
            "count": len(hits),
            "code_profiles": profiles,
            "scope_dbs": list(self.db_profiles),
            "deep_analysis_hint": (
                "If the user asks for a comprehensive review of how a table is "
                "used across the codebase, recommend `/code-analyze --tables <X>` "
                "(CLI) or the Code Analyze page in Studio rather than "
                "summarising every snippet here."
            ),
        }
