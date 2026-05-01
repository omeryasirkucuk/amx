"""Deterministic answer composers for ``SearchAgent``.

These methods produce a final natural-language answer WITHOUT calling
the LLM. They run for question types where the retrieval step already
contains enough structured data to render a sentence (and a small
markdown table where useful) — saving a synthesis LLM round-trip and
guaranteeing a deterministic, citation-aligned response.

The cluster covers six question shapes:

* ``_deterministic_ranked_answer`` — top-1 / top-K table or column
  match for ranked-list answers.
* ``_deterministic_aggregate_inventory_answer`` — superlative /
  aggregate ("which table has the most rows") over schema inventory.
* ``_deterministic_inventory_answer`` — schema dump, table count,
  list_databases, list_schemas.
* ``_deterministic_column_name_answer`` — exact column-name lookup
  result rendering.
* ``_deterministic_live_probe_answer`` — comment coverage + table
  metadata snapshot summaries from live-DB probes.
* ``_deterministic_target_resolution_answer`` — "I couldn't find this
  table" / ambiguity disambiguation messages from target resolution.

Each method returns ``str`` (the answer) or ``None`` when the input
shape doesn't match — caller falls back to the LLM synthesizer.

The mixin reads only from ``self.cfg``, ``self.db_profile`` and the
mixin-shared answering helpers (``_synthesize_answer``,
``_rows_for_prompt`` etc. live in agent.py for now). It does not
call the LLM.
"""

from __future__ import annotations

from typing import Any

from amx.utils.logging import get_logger

# Forward references for type hints only — these dataclasses are
# defined in ``amx.search.agent``. Importing them at runtime would
# create a circular dependency since ``agent.py`` imports this mixin.
SearchPlan = Any
SearchPolicy = Any
SearchActionSuggestion = Any

log = get_logger("search.agent.deterministic")


def _question_language_hint(text: str) -> str:
    """Light-weight language detector used by deterministic ranked answers.

    Mirrors the helper in ``agent.py``; redefined here so the mixin
    has no import-time dependency on the parent module.
    """
    import re

    sample = (text or "").strip()
    if not sample:
        return "english"
    lower = sample.lower()
    if re.search(r"[؀-ۿ]", sample):
        return "arabic"
    if re.search(r"[぀-ヿ一-鿿]", sample):
        return "japanese"
    if re.search(r"[가-힯]", sample):
        return "korean"
    if re.search(r"[Ѐ-ӿ]", sample):
        return "russian"
    if any(ch in lower for ch in "çğıöşü"):
        return "turkish"
    return "english"


class DeterministicAnswersMixin:
    """Bag of deterministic answer composers mixed into ``SearchAgent``."""

    def _deterministic_ranked_answer(
        self,
        question: str,
        plan: SearchPlan,
        rows: list[dict[str, Any]],
        retrieval_details: dict[str, Any],
        actions: list[SearchActionSuggestion],
    ) -> str | None:
        if not rows:
            return None
        lang = (plan.answer_language or _question_language_hint(question)).lower()
        primary = rows[0]
        if plan.question_class == "join_discovery":
            if plan.search_mode == "joinable_tables":
                target = f"{primary.get('target_schema_name')}.{primary.get('target_table_name')}".strip(".")
                left = str(primary.get("left_column") or "").strip()
                right = str(primary.get("right_column") or "").strip()
                if lang == "turkish":
                    answer = f"En guclu join adayi `{target}`."
                    if left or right:
                        answer += f" Ana kolonlar: `{left}` -> `{right}`."
                    return answer
                answer = f"The strongest join target is `{target}`."
                if left or right:
                    answer += f" Primary columns: `{left}` -> `{right}`."
                return answer
            left = str(primary.get("left_column") or "").strip()
            right = str(primary.get("right_column") or "").strip()
            band = str(primary.get("confidence_band") or "").strip()
            if left or right:
                if lang == "turkish":
                    return f"En guclu join kolonu eslesmesi `{left}` -> `{right}`. Guven seviyesi: `{band or 'unknown'}`."
                return f"The strongest join-column match is `{left}` -> `{right}`. Confidence: `{band or 'unknown'}`."
        if plan.search_mode == "table_explain" and retrieval_details.get("resolved_tables"):
            table_path = retrieval_details["resolved_tables"][0]
            column_count = primary.get("column_count") or retrieval_details.get("table_context", {}).get("column_count")
            if lang == "turkish":
                answer = f"`{table_path}` tablosu icin en guclu aciklama bulundu."
                if column_count:
                    answer += f" Katalogda **{int(column_count)}** kolon gorunuyor."
                return answer
            answer = f"The strongest match is the table `{table_path}`."
            if column_count:
                answer += f" The catalog shows **{int(column_count)}** columns."
            return answer
        if plan.target_entity == "table":
            table_path = ".".join(part for part in (str(primary.get("schema_name") or ""), str(primary.get("table_name") or "")) if part)
            if not table_path:
                return None
            if lang == "turkish":
                answer = f"En guclu tablo eslesmesi `{table_path}`."
                if primary.get("effective_description"):
                    answer += f" Kisa anlam: {str(primary.get('effective_description')).strip()}."
                return answer
            answer = f"The strongest table match is `{table_path}`."
            if primary.get("effective_description"):
                answer += f" Summary: {str(primary.get('effective_description')).strip()}."
            return answer
        column_path = ".".join(
            part
            for part in (
                str(primary.get("schema_name") or ""),
                str(primary.get("table_name") or ""),
                str(primary.get("column_name") or ""),
            )
            if part
        )
        if not column_path:
            return None
        if lang == "turkish":
            answer = f"En guclu kolon eslesmesi `{column_path}`."
            if primary.get("effective_description"):
                answer += f" Kisa anlam: {str(primary.get('effective_description')).strip()}."
            elif actions:
                answer += f" Sonraki adim: {actions[0].reason}"
            return answer
        answer = f"The strongest column match is `{column_path}`."
        if primary.get("effective_description"):
            answer += f" Summary: {str(primary.get('effective_description')).strip()}."
        elif actions:
            answer += f" Next step: {actions[0].reason}"
        return answer

    def _deterministic_aggregate_inventory_answer(
        self,
        plan: SearchPlan,
        rows: list[dict[str, Any]],
        retrieval_details: dict[str, Any],
    ) -> str | None:
        """Headline answer for superlative/top-K questions over schema_inventory.

        Returns None when the request isn't an aggregation or the field isn't
        usable, so the caller can fall back to the broad inventory dump.
        """
        if plan.search_mode != "schema_inventory":
            return None
        op = (plan.aggregation_op or "").lower()
        if op not in {"max", "min", "top_k", "bottom_k"}:
            return None
        field = (plan.aggregation_field or "").lower()
        if field not in {"row_count", "column_count"}:
            # table_count or "" don't index into per-table rows; let dump path handle.
            return None
        usable_rows = [row for row in rows if row.get(field) is not None]
        if not usable_rows:
            return None
        descending = op in {"max", "top_k"}
        ordered = sorted(
            usable_rows,
            key=lambda row: (int(row.get(field) or 0), str(row.get("table_name") or "")),
            reverse=descending,
        )
        limit = plan.aggregation_limit if plan.aggregation_limit > 0 else 1
        limit = min(limit, len(ordered))
        top = ordered[:limit]
        lang = (plan.answer_language or "english").lower()
        schema_name = str(retrieval_details.get("schema_name") or top[0].get("schema_name") or "").strip()
        database_name = str(retrieval_details.get("database_name") or top[0].get("database_name") or "").strip()
        scope_label = (
            f"`{schema_name}`" if schema_name
            else f"`{database_name}`" if database_name
            else ""
        )
        # Single-fact branch: one headline sentence, no table.
        if limit <= 1:
            row = top[0]
            table_name = str(row.get("table_name") or "")
            value = int(row.get(field) or 0)
            column_count = int(row.get("column_count") or 0)
            cluster = str(row.get("semantic_cluster") or "Unclustered")
            if lang == "turkish":
                facet = "satira" if field == "row_count" else "kolona"
                facet_unit = "satir" if field == "row_count" else "kolon"
                superlative = "en cok" if op == "max" else "en az"
                scope_phrase = f"{scope_label} icinde " if scope_label else ""
                value_fmt = f"{value:,}".replace(",", ".")
                # For column_count answers, do not duplicate the column count in trailing context.
                if field == "row_count":
                    return (
                        f"{scope_phrase}{superlative} {facet} sahip tablo `{table_name}`: "
                        f"**{value_fmt} {facet_unit}**, {column_count} kolon (kume: `{cluster}`)."
                    )
                return (
                    f"{scope_phrase}{superlative} {facet} sahip tablo `{table_name}`: "
                    f"**{value_fmt} {facet_unit}** (kume: `{cluster}`)."
                )
            facet = "rows" if field == "row_count" else "columns"
            superlative = "the most" if op == "max" else "the fewest"
            scope_phrase = f" in {scope_label}" if scope_label else ""
            value_fmt = f"{value:,}"
            if field == "row_count":
                return (
                    f"`{table_name}` has {superlative} {facet}{scope_phrase}: "
                    f"**{value_fmt}** rows, {column_count} columns, cluster `{cluster}`."
                )
            return (
                f"`{table_name}` has {superlative} {facet}{scope_phrase}: "
                f"**{value_fmt}** columns, cluster `{cluster}`."
            )
        # Short-table branch: headline + tiny markdown table of top K.
        facet_label_en = "Rows" if field == "row_count" else "Columns"
        facet_label_tr = "Satir" if field == "row_count" else "Kolon"
        if lang == "turkish":
            superlative = "en cok" if op in {"max", "top_k"} else "en az"
            scope_phrase = f" {scope_label} icinde" if scope_label else ""
            facet_word = "satira" if field == "row_count" else "kolona"
            header = f"{superlative} {facet_word} sahip ilk **{limit}** tablo{scope_phrase}:"
        else:
            superlative = "most" if op in {"max", "top_k"} else "fewest"
            scope_phrase = f" in {scope_label}" if scope_label else ""
            facet_word = "rows" if field == "row_count" else "columns"
            header = f"Top **{limit}** tables by {superlative} {facet_word}{scope_phrase}:"
        col_label = facet_label_tr if lang == "turkish" else facet_label_en
        table_lines = [
            f"| Schema | Table | {col_label} | Cluster |",
            "|---|---|---:|---|",
        ]
        for row in top:
            value = int(row.get(field) or 0)
            value_fmt = f"{value:,}".replace(",", ".") if lang == "turkish" else f"{value:,}"
            table_lines.append(
                "| {schema} | {table} | {value} | {cluster} |".format(
                    schema=str(row.get("schema_name") or ""),
                    table=str(row.get("table_name") or ""),
                    value=value_fmt,
                    cluster=str(row.get("semantic_cluster") or "Unclustered"),
                )
            )
        return header + "\n\n" + "\n".join(table_lines)

    def _deterministic_inventory_answer(
        self,
        plan: SearchPlan,
        rows: list[dict[str, Any]],
        retrieval_details: dict[str, Any],
    ) -> str | None:
        lang = (plan.answer_language or "english").lower()
        if plan.search_mode == "schema_inventory":
            aggregate = self._deterministic_aggregate_inventory_answer(plan, rows, retrieval_details)
            if aggregate is not None:
                return aggregate
            summary = dict(retrieval_details.get("schema_explorer_summary") or {})
            table_count = int(summary.get("table_count") or len(rows))
            total_columns = int(summary.get("total_columns") or sum(int(row.get("column_count") or 0) for row in rows))
            schema_name = str(retrieval_details.get("schema_name") or "").strip()
            database_name = str(retrieval_details.get("database_name") or "").strip()
            scope_label = f"`{schema_name}` schema" if schema_name else f"`{database_name}` database" if database_name else "the active namespace"
            cluster_counts: dict[str, int] = {}
            for row in rows:
                cluster = str(row.get("semantic_cluster") or "Unclustered")
                cluster_counts[cluster] = cluster_counts.get(cluster, 0) + 1
            cluster_summary = ", ".join(
                f"{cluster}: {count}" for cluster, count in sorted(cluster_counts.items(), key=lambda item: (-item[1], item[0]))[:8]
            )
            header = (
                f"{scope_label} icin **{table_count}** tablo ve toplam **{total_columns}** kolon bulundu."
                if lang == "turkish"
                else f"SchemaExplorer found **{table_count}** tables and **{total_columns}** total columns for {scope_label}."
            )
            if cluster_summary:
                header += (
                    f" Semantik kumeler: {cluster_summary}."
                    if lang == "turkish"
                    else f" Semantic clusters: {cluster_summary}."
                )
            table_lines = [
                "| Schema | Table | Columns | Rows | Cluster |",
                "|---|---:|---:|---:|---|",
            ]
            for row in rows[:50]:
                table_lines.append(
                    "| {schema} | {table} | {columns} | {rows_count} | {cluster} |".format(
                        schema=str(row.get("schema_name") or ""),
                        table=str(row.get("table_name") or ""),
                        columns=int(row.get("column_count") or 0),
                        rows_count=int(row.get("row_count") or 0),
                        cluster=str(row.get("semantic_cluster") or "Unclustered"),
                    )
                )
            if len(rows) > 50:
                table_lines.append(f"| ... | {len(rows) - 50} more tables |  |  |  |")
            return header + "\n\n" + "\n".join(table_lines)
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

    def _deterministic_column_name_answer(
        self,
        plan: SearchPlan,
        rows: list[dict[str, Any]],
        retrieval_details: dict[str, Any],
    ) -> str | None:
        if retrieval_details.get("result_kind") != "exact_column_name_matches" or not rows:
            return None
        lang = (plan.answer_language or "english").lower()
        names: list[str] = []
        for row in rows:
            schema_name = str(row.get("schema_name") or "")
            table_name = str(row.get("table_name") or "")
            column_name = str(row.get("column_name") or "")
            if not column_name:
                continue
            label = f"{schema_name}.{table_name}.{column_name}" if schema_name and table_name else column_name
            if label not in names:
                names.append(label)
        if not names:
            return None
        joined = ", ".join(f"`{name}`" for name in names)
        if lang == "turkish":
            return f"Kolon adi eslesmelerine gore bulunan kolonlar: {joined}."
        return f"Column-name matches found: {joined}."

    def _deterministic_live_probe_answer(
        self,
        plan: SearchPlan,
        rows: list[dict[str, Any]],
        live_probe: dict[str, Any],
    ) -> str | None:
        lang = (plan.answer_language or "english").lower()
        snapshot = next(
            (
                row
                for row in rows
                if row.get("row_type") == "live_probe"
                and row.get("probe_operation") == "table_metadata_snapshot"
            ),
            None,
        )
        if snapshot and (plan.search_mode == "table_explain" or plan.question_class == "table_understanding"):
            schema_name = str(snapshot.get("schema_name") or "")
            table_name = str(snapshot.get("table_name") or "")
            table_path = f"{schema_name}.{table_name}" if schema_name and table_name else table_name
            total = int(snapshot.get("total_columns") or 0)
            table_comment = str(snapshot.get("table_comment") or "").strip()
            columns = [
                row
                for row in rows
                if row.get("row_type") == "live_column_comment"
                and row.get("schema_name") == schema_name
                and row.get("table_name") == table_name
            ]
            preview = ", ".join(
                f"`{str(row.get('column_name') or '')}`"
                + (f" ({str(row.get('dtype') or '')})" if str(row.get("dtype") or "") else "")
                for row in columns[:12]
                if str(row.get("column_name") or "")
            )
            if lang == "turkish":
                answer = f"Canli DB metadata'sina gore `{table_path}` tablosunda **{total}** kolon var."
                if table_comment:
                    answer += f" Tablo comment'i: {table_comment}."
                else:
                    answer += " Tablo comment'i live metadata'da bos gorunuyor; bu yuzden is anlamini kesinlestirmiyorum."
                if preview:
                    answer += f" Ilk kolonlar: {preview}."
                return answer
            answer = f"Live DB metadata shows **{total}** columns on `{table_path}`."
            if table_comment:
                answer += f" Table comment: {table_comment}."
            else:
                answer += " The table comment is empty in live metadata, so I am not inferring a business meaning."
            if preview:
                answer += f" First columns: {preview}."
            return answer

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

    def _deterministic_target_resolution_answer(
        self,
        plan: SearchPlan,
        retrieval_details: dict[str, Any],
        live_probe: dict[str, Any],
    ) -> str | None:
        target_resolution = retrieval_details.get("target_resolution") or {}
        if not target_resolution.get("unresolved_explicit"):
            if live_probe.get("error"):
                if (plan.answer_language or "english").lower() == "turkish":
                    return f"Canli metadata kontrolu calistirilamadi: {live_probe.get('error')}. Bu nedenle kesin cevap vermiyorum."
                return f"The live metadata check could not run: {live_probe.get('error')}. I am not returning a definitive answer."
            return None
        targets = [item for item in target_resolution.get("targets", []) if isinstance(item, dict)]
        target = targets[0] if targets else {}
        requested = str(target.get("requested") or "").strip() or "requested table"
        candidates = [str(item) for item in (target.get("candidates") or []) if str(item)]
        warnings = [str(w) for w in (target.get("warnings") or [])]
        is_ambiguous = "ambiguous_unqualified_table" in warnings
        is_turkish = (plan.answer_language or "english").lower() == "turkish"
        if is_ambiguous:
            if is_turkish:
                if candidates:
                    return (
                        f"`{requested}` adında bir tablo birden fazla şemada mevcut. "
                        "Hangisini kastettiğinizi netleştirir misiniz? Adaylar: "
                        + ", ".join(f"`{item}`" for item in candidates[:5])
                        + "."
                    )
                return f"`{requested}` adı birden fazla şemada geçiyor; lütfen tam yolu belirtin (schema.table)."
            if candidates:
                return (
                    f"`{requested}` exists as a table in more than one schema. "
                    "Could you clarify which one you mean? Candidates: "
                    + ", ".join(f"`{item}`" for item in candidates[:5])
                    + "."
                )
            return (
                f"`{requested}` is the name of more than one table; please qualify it as `schema.table`."
            )
        if is_turkish:
            answer = (
                f"`{requested}` adında bir tablo bu DB profili için katalog veya canlı metadata'da bulunamadı."
            )
            if candidates:
                answer += " Benzer adlar (kesin değil, öneri): " + ", ".join(f"`{item}`" for item in candidates[:5]) + "."
            else:
                answer += " Önce `/search sync` çalıştırarak katalogu güncellemeyi deneyebilirsiniz."
            return answer
        answer = (
            f"I could not find a table named `{requested}` in this DB profile's catalog or live metadata."
        )
        if candidates:
            answer += " Similar names (suggestions, not confirmed): " + ", ".join(f"`{item}`" for item in candidates[:5]) + "."
        else:
            answer += " You may want to run `/search sync` to refresh the catalog first."
        return answer


__all__ = [
    "DeterministicAnswersMixin",
]
