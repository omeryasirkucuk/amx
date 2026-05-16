"""Join-inference tools for :class:`ToolBox`.

The three join-discovery tools (``get_join_candidates``,
``find_joinable_tables``, ``find_joinable_across_profiles``) score
catalog-recorded and live-DB column overlaps to surface foreign-key
relationships the user has not explicitly declared. They share a
handful of state attributes from the host ``ToolBox``:

* ``self.cfg`` / ``self.catalog`` / ``self.db_profile`` /
  ``self.db_profiles`` — scope and catalog access.
* ``self._live_db`` / ``self._connector_for_profile`` /
  ``self._scoped_catalog`` — live-DB connector helpers.
* ``self._user_catalogs`` / ``self._count_database_assets`` /
  ``self._LIVE_FANOUT_TIMEOUT_SEC`` — supporting helpers and
  fan-out tuning constants.

Two private helpers (``_fetch_live_column_names`` and
``_compute_value_overlap_rows``) live here because they are used only
by the join-inference tools.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from amx.search._agent_tools_helpers import (
    _description_proximity,
    _dtype_compat_score,
    _name_overlap_score,
    _sample_distinct_values,
    _ToolError,
)

if TYPE_CHECKING:
    from amx.config import AMXConfig
    from amx.search.catalog import SearchCatalog


class _JoinInferenceMixin:
    """Join-candidate discovery tool implementations."""

    # Provided by the host ``ToolBox`` instance.
    cfg: AMXConfig
    catalog: SearchCatalog
    db_profile: str
    db_profiles: list[str]

    _VALID_JOIN_STRATEGIES: frozenset[str] = frozenset(
        {"auto", "foreign_key", "name_overlap", "semantic", "value_overlap", "all"}
    )

    def _tool_get_join_candidates(self, left: str, right: str) -> dict[str, Any]:
        verified = self.catalog.join_candidates(self.db_profile, left, right, limit=8)
        return {
            "left": left,
            "right": right,
            "candidates": [
                {
                    "left_column": str(r.get("left_column") or ""),
                    "right_column": str(r.get("right_column") or ""),
                    "type": str(r.get("relationship_type") or ""),
                    "score": float(r.get("score") or 0.0),
                }
                for r in verified
            ],
        }

    def _tool_find_joinable_across_profiles(self, table: str, k: int = 12) -> dict[str, Any]:
        """Cross-profile join finder.

        Given a source ``profile::schema.table`` (or ``schema.table``
        on the anchor profile), find columns on OTHER profiles in scope
        whose name + dtype + semantic similarity + FK pattern suggest a
        join key. Aggressive scoring on purpose — the user picked
        "high recall" over "low BYO-LLM cost"; a few false positives
        are fine because each candidate carries a confidence score the
        LLM can caveat with.

        Output rows:
            ``{source: {profile, schema, table, column, dtype}, target:
            {profile, schema, table, column, dtype}, score, signals:
            {name, dtype, vector, fk}}``

        Performance: 1 SQL pass per source column to find compatible
        target columns + 1 vector index query for semantic matches.
        For 5 profiles × 200 schemas total, target wall-clock < 400ms.
        """
        from amx.search._catalog._db_profile_clause import build_db_profile_clause as _bdp

        target = (table or "").strip()
        if not target:
            raise _ToolError("Argument 'table' is required.")
        limit = max(1, min(int(k or 12), 50))

        # ── 1. Resolve the source (profile, schema, table) ──
        # Accept ``profile::schema.table`` (strict) or ``schema.table``
        # / ``table`` (resolve via find_tables_by_exact_name on the
        # anchor profile or scope-wide).
        source_profile: str | None = None
        source_schema: str | None = None
        source_table: str | None = None
        if "::" in target:
            head, rest = target.split("::", 1)
            source_profile = head.strip() or None
            target = rest.strip()
        if source_profile and source_profile not in self.cfg.db_profiles:
            return {
                "found": False,
                "error": f"Unknown source profile {source_profile!r}.",
                "candidates": [],
            }
        if "." in target:
            source_schema, source_table = target.split(".", 1)
            source_schema = source_schema.strip()
            source_table = source_table.strip()
        else:
            source_table = target.strip()

        # ── 2. Look up source columns ──
        # The source profile defaults to anchor when not given.
        resolved_source = source_profile or self.db_profile
        source_cols_clause, source_cols_binds = _bdp(resolved_source, column="ce.db_profile")
        with self.catalog._connect() as conn:  # noqa: SLF001
            where = [source_cols_clause, "ce.entity_kind = 'column'"]
            params: list[Any] = list(source_cols_binds)
            if source_schema:
                where.append("LOWER(ce.schema_name) = LOWER(?)")
                params.append(source_schema)
            where.append("LOWER(ce.table_name) = LOWER(?)")
            params.append(str(source_table or ""))
            source_rows = conn.execute(
                f"""
                    SELECT ce.db_profile, ce.schema_name, ce.table_name,
                           ce.column_name, ce.dtype, ce.pk_flag, ce.fk_flag,
                           cd.description_text AS effective_description
                    FROM catalog_entities ce
                    LEFT JOIN catalog_descriptions cd
                           ON cd.id = ce.effective_description_id
                    WHERE {" AND ".join(where)}
                    ORDER BY ce.column_name
                    """,
                tuple(params),
            ).fetchall()
        if not source_rows:
            return {
                "found": False,
                "error": (
                    f"Source table {target!r} not found on profile "
                    f"{resolved_source!r}. Try find_table_by_name first."
                ),
                "candidates": [],
            }
        # Lock the source profile to whatever the row reports (handles
        # case where the user didn't specify and the table only lives
        # in one profile).
        source_profile = str(source_rows[0]["db_profile"]).strip()
        source_schema = str(source_rows[0]["schema_name"]).strip()
        source_table = str(source_rows[0]["table_name"]).strip()

        # ── 3. Find candidate columns on OTHER profiles ──
        target_profiles = [p for p in self.db_profiles if p and p != source_profile]
        if not target_profiles:
            return {
                "found": True,
                "source": {
                    "profile": source_profile,
                    "schema": source_schema,
                    "table": source_table,
                },
                "candidates": [],
                "message": (
                    "Scope only includes one profile — there are no other "
                    "profiles to join against. Add another profile to "
                    "scope (or expand /ask-scope) and re-ask."
                ),
            }

        target_clause, target_binds = _bdp(target_profiles, column="ce.db_profile")

        # Collect ALL columns from target profiles in one SQL pass —
        # we score in Python after. This avoids N+1 queries when the
        # source has many columns. Capped at 5000 rows for safety.
        with self.catalog._connect() as conn:  # noqa: SLF001
            target_rows = conn.execute(
                f"""
                    SELECT ce.db_profile, ce.schema_name, ce.table_name,
                           ce.column_name, ce.dtype, ce.pk_flag, ce.fk_flag,
                           cd.description_text AS effective_description
                    FROM catalog_entities ce
                    LEFT JOIN catalog_descriptions cd
                           ON cd.id = ce.effective_description_id
                    WHERE {target_clause} AND ce.entity_kind = 'column'
                      AND ce.column_name IS NOT NULL
                    LIMIT 5000
                    """,
                tuple(target_binds),
            ).fetchall()

        # ── 4. Score every (source col, target col) pair ──
        candidates: list[dict[str, Any]] = []
        for s_row in source_rows:
            s_name = str(s_row["column_name"] or "")
            s_dtype = str(s_row["dtype"] or "")
            s_pk = bool(s_row["pk_flag"])
            s_fk_pattern = s_name.lower().endswith(("_id", "id"))
            for t_row in target_rows:
                t_name = str(t_row["column_name"] or "")
                t_dtype = str(t_row["dtype"] or "")
                t_pk = bool(t_row["pk_flag"])
                t_table = str(t_row["table_name"] or "")
                if not t_name:
                    continue
                # ── Signal 1: name overlap (token + Levenshtein ratio) ──
                name_score = _name_overlap_score(s_name, t_name)
                if name_score == 0.0:
                    continue  # not even loosely related — skip
                # ── Signal 2: dtype compatibility ──
                dtype_score = _dtype_compat_score(s_dtype, t_dtype)
                # ── Signal 3: vector similarity (deferred — same SQL ──
                # we use description text proximity here as a cheap
                # proxy; PR-D will swap in a real index query when
                # the catalog has descriptions populated).
                s_desc = str(s_row["effective_description"] or "")
                t_desc = str(t_row["effective_description"] or "")
                vector_score = _description_proximity(s_desc, t_desc)
                # ── Signal 4: FK pattern ──
                fk_score = 0.0
                if s_fk_pattern and t_pk:
                    fk_score = 1.0
                elif s_pk and t_name.lower().endswith(("_id", "id")):
                    fk_score = 0.7
                # ── Combine ──
                total = (
                    0.30 * name_score + 0.20 * dtype_score + 0.40 * vector_score + 0.10 * fk_score
                )
                if total < 0.20:
                    continue  # cull very weak matches
                candidates.append(
                    {
                        "source": {
                            "profile": source_profile,
                            "schema": source_schema,
                            "table": source_table,
                            "column": s_name,
                            "dtype": s_dtype,
                        },
                        "target": {
                            "profile": str(t_row["db_profile"] or ""),
                            "schema": str(t_row["schema_name"] or ""),
                            "table": t_table,
                            "column": t_name,
                            "dtype": t_dtype,
                        },
                        "score": round(total, 3),
                        "signals": {
                            "name": round(name_score, 3),
                            "dtype": round(dtype_score, 3),
                            "vector": round(vector_score, 3),
                            "fk": round(fk_score, 3),
                        },
                    }
                )

        # ── 5. Rank + truncate ──
        candidates.sort(key=lambda c: c["score"], reverse=True)
        candidates = candidates[:limit]

        return {
            "found": True,
            "source": {
                "profile": source_profile,
                "schema": source_schema,
                "table": source_table,
            },
            "scope": list(self.db_profiles),
            "candidates": candidates,
            "candidate_count": len(candidates),
            "scoring_note": (
                "Score weights: name=0.30, dtype=0.20, vector=0.40, fk=0.10. "
                "Treat scores ≥0.65 as confident, 0.40-0.65 as weak (caveat "
                "explicitly), <0.40 as coincidental (do NOT recommend)."
            ),
        }

    def _tool_find_joinable_tables(
        self,
        table: str,
        strategy: str = "auto",
    ) -> dict[str, Any]:
        target = (table or "").strip()
        if not target:
            raise _ToolError("Argument 'table' is required.")
        strategy = (strategy or "auto").strip().lower()
        if strategy not in self._VALID_JOIN_STRATEGIES:
            raise _ToolError(
                f"strategy must be one of {sorted(self._VALID_JOIN_STRATEGIES)}; got {strategy!r}."
            )
        # Resolve to schema.table when only the table name was provided.
        # Multi-profile scope: search across every configured profile;
        # if the table exists in only one profile we anchor there. The
        # cross-profile join expansion (joinable across profile X and
        # profile Y) lands in PR-C as a dedicated tool.
        if "." not in target:
            exact = self.catalog.find_tables_by_exact_name(self.db_profile_filter, target, limit=5)
            if not exact:
                return {
                    "table": target,
                    "found": False,
                    "message": (
                        f"No table named '{target}' is in the catalog. Try find_table_by_name "
                        "first, or qualify the target as schema.table."
                    ),
                    "joinable_tables": [],
                }
            if len(exact) > 1:
                paths = [
                    f"{str(r.get('schema_name') or '')}.{str(r.get('table_name') or '')}"
                    for r in exact
                ]
                return {
                    "table": target,
                    "found": False,
                    "ambiguous": True,
                    "candidates": paths,
                    "message": (
                        f"'{target}' lives in multiple schemas: {', '.join(paths)}. "
                        "Re-call with the fully-qualified schema.table."
                    ),
                    "joinable_tables": [],
                }
            row = exact[0]
            target = f"{row.get('schema_name') or ''}.{row.get('table_name') or ''}"

        # Four-tier strategy palette (v0.14):
        # 1. ``foreign_key``    — declared FK relationships from the catalog.
        # 2. ``name_overlap``   — rarity-weighted shared column names,
        #                          with a live ``information_schema``
        #                          rescue when ``catalog_entities`` is
        #                          missing rows for the target table.
        # 3. ``semantic``       — vector similarity over column
        #                          descriptions (requires /run).
        # 4. ``value_overlap``  — opt-in data-touching strategy:
        #                          samples distinct values from both
        #                          sides of each name-overlap candidate
        #                          and scores by Jaccard intersection.
        #                          Bounded at 12 candidates × 200
        #                          distinct values per side; opt-in
        #                          to keep the default hot path free
        #                          of extra DB hits.
        # ``strategy="auto"`` cascades 1→2→3 (today's behavior, unchanged
        # for default callers). ``"all"`` runs every strategy and merges
        # results by (target_schema, target_table), keeping the highest
        # per-row score. Individual strategy names run only that tier.
        strategies_tried: list[str] = []
        source_was_live = False

        def _run_fk() -> list[dict[str, Any]]:
            strategies_tried.append("foreign_key")
            return self.catalog.joinable_tables(self.db_profile, target, limit=12)

        def _run_name_overlap() -> list[dict[str, Any]]:
            nonlocal source_was_live
            strategies_tried.append("name_overlap")
            r = self.catalog.name_overlap_joinable_tables(
                self.db_profile,
                target,
                limit=12,
            )
            if r:
                return r
            # Live rescue: catalog wasn't synced for this target yet, so
            # we have no base column list to compare against peers.
            # Fetch column names directly from the live backend (one
            # cheap ``get_columns`` call) and retry with the override.
            if self.catalog.target_has_catalog_columns(self.db_profile, target):
                return []
            live_cols = self._fetch_live_column_names(target)
            if not live_cols:
                return []
            source_was_live = True
            return self.catalog.name_overlap_joinable_tables(
                self.db_profile,
                target,
                limit=12,
                base_cols_override=live_cols,
            )

        def _run_semantic() -> list[dict[str, Any]]:
            strategies_tried.append("semantic_similarity")
            try:
                return self.catalog.semantic_joinable_tables(
                    self.db_profile,
                    target,
                    limit=12,
                )
            except Exception:
                return []

        def _run_value_overlap() -> list[dict[str, Any]]:
            # Seed candidates from name_overlap (with the live rescue
            # path) so we only sample values for plausible joins. Pure
            # name_overlap may not return enough candidates on its own;
            # we don't try to widen — value_overlap is meant to *verify*
            # name overlap with real data, not to discover joins from
            # scratch.
            seeds = _run_name_overlap()
            # We routed through name_overlap purely to get seeds — the
            # user asked for value_overlap, so drop that label.
            if "name_overlap" in strategies_tried:
                strategies_tried.remove("name_overlap")
            strategies_tried.append("value_overlap")
            if not seeds:
                return []
            return self._compute_value_overlap_rows(target, seeds)

        rows: list[dict[str, Any]] = []
        inference_source: str | None = None
        per_strategy_results: list[tuple[str, list[dict[str, Any]]]] = []

        if strategy in ("auto", "foreign_key", "all"):
            fk_rows = _run_fk()
            per_strategy_results.append(("foreign_key", fk_rows))
            if not rows and fk_rows:
                rows = fk_rows
                inference_source = "foreign_key"
            if strategy == "foreign_key":
                pass  # nothing else to run
            elif strategy == "auto" and rows:
                pass  # cascade stops on first hit

        need_name_overlap = (
            (strategy == "auto" and not rows) or strategy == "name_overlap" or strategy == "all"
        )
        if need_name_overlap:
            no_rows = _run_name_overlap()
            per_strategy_results.append(("name_overlap", no_rows))
            if not rows and no_rows:
                rows = no_rows
                inference_source = "name_overlap"

        need_semantic = (
            (strategy == "auto" and not rows) or strategy == "semantic" or strategy == "all"
        )
        if need_semantic:
            s_rows = _run_semantic()
            per_strategy_results.append(("semantic_similarity", s_rows))
            if not rows and s_rows:
                rows = s_rows
                inference_source = "semantic_similarity"

        if strategy in ("value_overlap", "all"):
            v_rows = _run_value_overlap()
            per_strategy_results.append(("value_overlap", v_rows))
            if not rows and v_rows:
                rows = v_rows
                inference_source = "value_overlap"

        if strategy == "all":
            # Merge per-strategy results by (target_schema, target_table),
            # keeping the highest-score row and tagging each with its
            # source so the LLM can see why each candidate landed in
            # the list.
            merged: dict[tuple[str, str], dict[str, Any]] = {}
            for label, batch in per_strategy_results:
                for r in batch:
                    key = (
                        str(r.get("target_schema_name") or "").lower(),
                        str(r.get("target_table_name") or "").lower(),
                    )
                    if not key[0] or not key[1]:
                        continue
                    enriched = dict(r)
                    enriched.setdefault("inference_sources", [])
                    if label not in enriched["inference_sources"]:
                        enriched["inference_sources"].append(label)
                    existing = merged.get(key)
                    if existing is None or float(enriched.get("score") or 0.0) > float(
                        existing.get("score") or 0.0
                    ):
                        # Carry over any sources already merged into the
                        # previous best so we don't lose history.
                        if existing is not None:
                            for src in existing.get("inference_sources", []):
                                if src not in enriched["inference_sources"]:
                                    enriched["inference_sources"].append(src)
                        merged[key] = enriched
                    else:
                        for src in enriched["inference_sources"]:
                            if src not in existing.get("inference_sources", []):
                                existing.setdefault("inference_sources", []).append(src)
            rows = sorted(
                merged.values(),
                key=lambda r: -float(r.get("score") or 0.0),
            )[:12]
            inference_source = "all" if rows else None

        joinable: list[dict[str, Any]] = []
        for r in rows:
            entry: dict[str, Any] = {
                "target_schema": str(r.get("target_schema_name") or ""),
                "target_table": str(r.get("target_table_name") or ""),
                "left_column": str(r.get("left_column") or ""),
                "right_column": str(r.get("right_column") or ""),
                "type": str(r.get("relationship_type") or ""),
                "score": float(r.get("score") or 0.0),
                "shared_column_count": int(r.get("shared_column_count") or 0),
            }
            # value_overlap (or "all" carrying value_overlap rows)
            # surfaces per-row data signals so the LLM can cite the
            # intersection count and Jaccard ratio in its answer.
            if "overlap_count" in r:
                entry["overlap_count"] = int(r.get("overlap_count") or 0)
            if "overlap_ratio" in r:
                entry["overlap_ratio"] = float(r.get("overlap_ratio") or 0.0)
            if "sample_size_per_side" in r:
                entry["sample_size_per_side"] = int(r.get("sample_size_per_side") or 0)
            if "inference_sources" in r:
                entry["inference_sources"] = list(r.get("inference_sources") or [])
            joinable.append(entry)

        response: dict[str, Any] = {
            "table": target,
            "found": True,
            "strategy": strategy,
            "joinable_tables": joinable,
            "count": len(joinable),
            "inference_source": inference_source,
            "strategies_tried": strategies_tried,
        }
        if source_was_live:
            response["source_was_live"] = True
            response["note"] = (
                "Catalog had no column rows for this table; column "
                "names were fetched live from the backend. Run "
                "`/search sync` to refresh the catalog for faster "
                "subsequent calls."
            )
        return response

    def _fetch_live_column_names(self, target: str) -> list[str]:
        """Return live column names for ``schema.table`` from the active
        backend's information_schema (or adapter equivalent).

        Used by the name_overlap rescue path: when the catalog has
        no column rows for the target, we still want to discover
        joinable peers from the catalog by feeding in the live
        column list. Cheap (one ``get_columns`` round-trip via the
        SQLAlchemy inspector) and soft-fails to an empty list so
        the caller can give up gracefully.
        """
        if "." not in target:
            return []
        schema_name, table_name = target.split(".", 1)
        try:
            from sqlalchemy import inspect as _inspect

            db = self._connector_for_profile(self.db_profile)
            insp = _inspect(db.engine)
            cols = insp.get_columns(table_name, schema=schema_name)
        except Exception:
            return []
        out: list[str] = []
        for c in cols or []:
            name = str(c.get("name") or "").strip() if isinstance(c, dict) else ""
            if name:
                out.append(name)
        return out

    def _compute_value_overlap_rows(
        self,
        target: str,
        seeds: list[dict[str, Any]],
        *,
        sample_n: int = 200,
        min_intersection: int = 3,
        candidate_limit: int = 12,
    ) -> list[dict[str, Any]]:
        """Run the value_overlap strategy against a list of name-overlap
        candidates.

        For each seed, samples up to *sample_n* distinct values from
        the highest-rarity shared column on both sides of the join,
        then scores by Jaccard intersection. Drops candidates whose
        intersection is below *min_intersection* — too few common
        values is noise (collisions on flag values like ``''`` or
        ``'X'`` shouldn't drive a join recommendation).

        Bounded at *candidate_limit* seeds × 2 sides × 2 queries
        (``SELECT DISTINCT`` + ``COUNT(DISTINCT)``) so an answer
        never costs more than ~48 short reads per call.
        """
        if "." not in target:
            return []
        schema_name, table_name = target.split(".", 1)
        db = self._connector_for_profile(self.db_profile)
        out: list[dict[str, Any]] = []
        for seed in seeds[: max(1, int(candidate_limit))]:
            target_schema = str(seed.get("target_schema_name") or "")
            target_table = str(seed.get("target_table_name") or "")
            # ``left_column`` from name_overlap is comma-separated when
            # multiple columns are shared; the first entry is the
            # highest-rarity (the function sorts by weight desc inside
            # each candidate). Sampling on the rarest shared column is
            # the most informative single check.
            raw_left = str(seed.get("left_column") or "")
            join_col = raw_left.split(",")[0].strip()
            if not target_schema or not target_table or not join_col:
                continue
            try:
                left_samples, _ = _sample_distinct_values(
                    db,
                    schema_name,
                    table_name,
                    join_col,
                    sample_n,
                )
                right_samples, _ = _sample_distinct_values(
                    db,
                    target_schema,
                    target_table,
                    join_col,
                    sample_n,
                )
            except Exception:
                # Skip this candidate rather than failing the whole
                # strategy — common reasons: column missing on the
                # right side (catalog out-of-date), permissions, or
                # type mismatch that breaks the SELECT.
                continue
            left_set = {v for v in left_samples if v != ""}
            right_set = {v for v in right_samples if v != ""}
            if not left_set or not right_set:
                continue
            inter = left_set & right_set
            if len(inter) < min_intersection:
                continue
            union = left_set | right_set
            jaccard = len(inter) / len(union) if union else 0.0
            name_weight = float(seed.get("score") or 0.0)
            row = dict(seed)
            row.update(
                {
                    "relationship_type": "value_overlap",
                    "source": "value_overlap",
                    "left_column": join_col,
                    "right_column": join_col,
                    "score": round(name_weight * jaccard, 4),
                    "overlap_count": len(inter),
                    "overlap_ratio": round(jaccard, 4),
                    "sample_size_per_side": max(len(left_set), len(right_set)),
                }
            )
            out.append(row)
        out.sort(key=lambda r: -float(r.get("score") or 0.0))
        return out
