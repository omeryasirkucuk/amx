"""SCD detection + dimensional-role classification tools for :class:`ToolBox`.

Extracted from :mod:`amx.search.agent_tools` so the two heaviest live-DB
classification tools (``detect_scd_pattern`` ~338 LOC,
``detect_dimensional_role`` ~152 LOC) plus their 11 naming-pattern
constants and three private helpers (``_name_role_signal``,
``_count_column_shape``, ``_classify_table_role``) live in one focused
module.

The mixin is compose-only — it never overrides ``ToolBox.__init__`` —
and reads ``self._live_db`` off the host ``ToolBox`` for the SQL
samples it needs.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from amx.search._agent_tools_helpers import _ToolError

if TYPE_CHECKING:
    from amx.db.connector import DatabaseConnector


class _ScdAndRoleMixin:
    """SCD-pattern and dimensional-role tool implementations."""

    # Provided by the host ``ToolBox`` instance.
    def _live_db(self) -> DatabaseConnector:  # pragma: no cover - host method
        raise NotImplementedError

    _SCD_VALID_FROM_NAMES: tuple[str, ...] = (
        "valid_from",
        "valid_start",
        "effective_from",
        "effective_start",
        "start_date",
        "start_dt",
        "begin_date",
        "begda",
        "from_date",
        "active_from",
        "row_start",
    )

    _SCD_VALID_TO_NAMES: tuple[str, ...] = (
        "valid_to",
        "valid_end",
        "effective_to",
        "effective_end",
        "end_date",
        "end_dt",
        "endda",
        "to_date",
        "active_to",
        "row_end",
    )

    _SCD_CURRENT_FLAG_NAMES: tuple[str, ...] = (
        "is_current",
        "is_active",
        "current_flag",
        "active_flag",
        "is_latest",
        "current_record",
        "is_current_version",
    )

    _SCD_VERSION_NAMES: tuple[str, ...] = (
        "version",
        "revision",
        "rev_no",
        "seq_no",
        "row_version",
        "scd_version",
        "history_seq",
    )

    _SCD_PREV_PREFIXES: tuple[str, ...] = (
        "prev_",
        "previous_",
        "old_",
        "former_",
        "before_",
        "last_",
    )

    _SCD_NEW_PREFIXES: tuple[str, ...] = (
        "new_",
        "current_",
        "now_",
        "after_",
    )

    _SCD_HISTORY_SUFFIXES: tuple[str, ...] = (
        "_history",
        "_hist",
        "_audit",
        "_log",
        "_archive",
        "_versions",
        "_changes",
        "_snapshot",
    )

    _DIM_ROLE_NAMING: dict[str, tuple[str, ...]] = {
        # Each role lists name patterns; matched as substring on the
        # lowered table name. Order doesn't matter — every role
        # contributes to a separate naming-signal bucket.
        "fact": (
            "fact_",
            "_fact",
            "_facts",
            "fact",
            "f_",
            "_evt",
            "_event",
            "_events",
            "transactions",
            "_trans",
            "_txn",
            "_orders",
            "_sales",
            "_invoice",
            "_invoices",
        ),
        "dimension": (
            "dim_",
            "_dim",
            "_dimension",
            "dimension_",
            "_lookup",
            "lookup_",
        ),
        "staging": (
            "stg_",
            "staging_",
            "_staging",
            "_landing",
            "raw_",
            "_raw",
            "src_",
            "_src",
        ),
        "bridge": (
            "bridge_",
            "_bridge",
            "xref_",
            "_xref",
            "link_",
            "_link",
            "rel_",
            "_rel",
        ),
        "audit": (
            "_audit",
            "audit_",
            "_log",
            "log_",
            "_history",
            "history_",
            "_archive",
            "archive_",
        ),
    }

    _MEASURE_NAME_PATTERNS: tuple[str, ...] = (
        "_amt",
        "_amount",
        "amount_",
        "_value",
        "_qty",
        "_quantity",
        "_total",
        "_sum",
        "_price",
        "_cost",
        "_fee",
        "_rate",
        "_count",
        "_brutto",
        "_netto",
        "_revenue",
        "_profit",
        "_margin",
        "_balance",
        "_credit",
        "_debit",
        "_tax",
        # SAP-specific currency / quantity columns
        "netwr",
        "brtwr",
        "mwsbp",
        "mwsbk",
        "kbetr",
        "kwert",
        "fkimg",
        "fklmg",
        "kpein",
        "kzwi",
        "wavwr",
    )

    _ID_NAME_PATTERNS: tuple[str, ...] = (
        "_id",
        "id_",
        "_key",
        "_no",
        "_num",
        "_code",
        "_nr",
        "_kod",
        # SAP-specific keys appearing in many tables
        "mandt",
        "vbeln",
        "vgbel",
        "kunag",
        "kunrg",
        "kunwe",
        "lifnr",
        "vkorg",
        "vtweg",
        "spart",
        "matnr",
        "werks",
        "lgort",
        "bukrs",
        "gjahr",
        "belnr",
        "buzei",
        "fkart",
        "auart",
    )

    _DESCRIPTIVE_NAME_PATTERNS: tuple[str, ...] = (
        "_name",
        "name_",
        "_desc",
        "_description",
        "description_",
        "_label",
        "_text",
        "text_",
        "_title",
        "_remark",
        "_note",
        "_comment",
        "_addr",
        "address_",
        "_street",
        "_city",
        # SAP-specific descriptive columns
        "ktokd",
        "kdgrp",
        "klabc",
        "konzs",
        "name1",
        "name2",
    )

    def _name_role_signal(self, table_name: str) -> str:
        low = table_name.lower()
        for role, patterns in self._DIM_ROLE_NAMING.items():
            for pat in patterns:
                if pat in low:
                    return role
        return ""

    def _count_column_shape(self, profile: Any) -> dict[str, int]:
        """Count measure-like / id-like / descriptive-like columns.

        Used by ``_classify_table_role`` as a structural fallback when
        naming + FK signals are weak. Returns counts by category;
        decision logic lives in the caller.
        """
        measures = 0
        ids = 0
        descriptives = 0
        for c in profile.columns or []:
            name_low = str(c.name).lower()
            dtype_low = str(c.dtype).lower()
            is_numeric = any(
                token in dtype_low
                for token in (
                    "int",
                    "numeric",
                    "decimal",
                    "double",
                    "float",
                    "real",
                    "money",
                )
            )
            is_string = any(token in dtype_low for token in ("char", "varchar", "text", "string"))
            # Measure: numeric AND name suggests value/quantity.
            if is_numeric and any(p in name_low for p in self._MEASURE_NAME_PATTERNS):
                measures += 1
                continue
            # ID-like: any dtype, name suggests key/code (numeric or
            # short-fixed-width strings both count).
            if any(
                p == name_low or name_low.endswith(p) or name_low.startswith(p) or p in name_low
                for p in self._ID_NAME_PATTERNS
            ):
                ids += 1
                continue
            # Descriptive: string AND name suggests label/description.
            if is_string and any(p in name_low for p in self._DESCRIPTIVE_NAME_PATTERNS):
                descriptives += 1
        return {
            "measures": measures,
            "ids": ids,
            "descriptives": descriptives,
        }

    def _classify_table_role(
        self,
        profile: Any,
        peer_row_counts: list[int] | None = None,
    ) -> dict[str, Any]:
        """Classify ONE table's dimensional role from its profile.

        Combines naming signals with structural signals. ``peer_row_counts``
        is the row-count distribution of sibling tables in the same
        schema — used to compute the row-count percentile (high
        percentile → likely fact). When omitted (single-table call
        without schema context), the structural heuristic falls back
        to absolute thresholds.
        """
        from statistics import median

        evidence: list[str] = []
        indicators: dict[str, Any] = {}

        table_name = str(profile.name)
        row_count = int(profile.row_count or 0)
        fk_out = len(profile.foreign_keys or [])
        fk_in = len(profile.referenced_by or [])
        col_count = len(profile.columns or [])
        is_partitioned = bool(getattr(profile.analytics, "partition_keys", []) or [])
        has_clustering = bool(getattr(profile.analytics, "clustering_keys", []) or [])

        indicators["row_count"] = row_count
        indicators["fk_outgoing"] = fk_out
        indicators["fk_incoming"] = fk_in
        indicators["column_count"] = col_count
        indicators["is_partitioned"] = is_partitioned
        indicators["has_clustering"] = has_clustering

        # Has temporal column? (any column with date/timestamp dtype family)
        has_temporal = any(
            any(token in str(c.dtype).lower() for token in ("date", "timestamp", "datetime"))
            for c in profile.columns
        )
        indicators["has_temporal_column"] = has_temporal

        # Naming signal
        naming = self._name_role_signal(table_name)
        if naming:
            indicators["naming_signal"] = naming
            evidence.append(f"Naming pattern matches `{naming}` role.")

        # ── Column-shape signal ──
        # Counts measure-like (numeric financial / quantity) columns,
        # ID-like (key / code) columns, and descriptive (label / name)
        # columns. Lets the classifier handle SAP-style schemas with
        # opaque table names AND no declared FKs — vbrk has no naming
        # signal AND no FK constraints, but it has many numeric measures
        # (netwr / mwsbk / fkimg) + many keys (mandt / vbeln / kunag),
        # which is the column-shape signature of a fact table.
        shape = self._count_column_shape(profile)
        indicators["measure_columns"] = shape["measures"]
        indicators["id_columns"] = shape["ids"]
        indicators["descriptive_columns"] = shape["descriptives"]
        if shape["measures"] >= 3:
            evidence.append(
                f"{shape['measures']} measure-like numeric column(s) "
                "(amount / value / qty / SAP currency or quantity field) "
                "— fact-like column shape."
            )
        if shape["ids"] >= 4:
            evidence.append(
                f"{shape['ids']} ID / key / code column(s) — joins out "
                "to many entities (fact-like) or composite-key (bridge-like)."
            )
        if shape["descriptives"] >= 5 and shape["measures"] == 0:
            evidence.append(
                f"{shape['descriptives']} descriptive (name / label / "
                "description) column(s) and no measures — dimension / "
                "reference shape."
            )

        # Row-count percentile vs peers (if peers provided)
        rc_percentile: float | None = None
        if peer_row_counts and len(peer_row_counts) >= 3:
            sorted_peers = sorted(peer_row_counts)
            rank = sum(1 for n in sorted_peers if n <= row_count)
            rc_percentile = rank / len(sorted_peers)
            indicators["row_count_percentile"] = round(rc_percentile, 3)
            med = median(sorted_peers)
            indicators["peer_row_count_median"] = int(med)
            if row_count > med * 5 and row_count > 1000:
                evidence.append(
                    f"Row count {row_count:,} is >5× the schema median "
                    f"({int(med):,}) — likely fact / transactional."
                )
            elif row_count <= 1000 and col_count <= 10:
                evidence.append(
                    f"Small table ({row_count} rows, {col_count} cols) — likely lookup / reference."
                )

        # FK fan-out / fan-in
        if fk_out >= 3:
            evidence.append(
                f"{fk_out} outgoing FK(s) — likely fact (joins out to many dimensions)."
            )
        if fk_in >= 3:
            evidence.append(
                f"{fk_in} incoming FK(s) — likely dimension (referenced by many tables)."
            )

        # Bridge: roughly equal in/out, both ≥ 2
        is_bridge = fk_out >= 2 and fk_in >= 2 and abs(fk_out - fk_in) <= 1

        # Decide the hypothesis. Naming wins for staging / audit / bridge
        # (strong intent); structural wins for fact / dimension / lookup.
        hypothesis = "unknown"
        confidence = "low"

        if naming == "staging":
            hypothesis = "staging"
            confidence = "high"
        elif naming == "audit":
            hypothesis = "audit"
            confidence = "high"
        elif naming == "bridge" or is_bridge:
            hypothesis = "bridge"
            confidence = "medium" if naming == "bridge" else "low"
            if is_bridge and naming != "bridge":
                evidence.append(
                    f"Roughly equal FK fan-out ({fk_out}) and fan-in "
                    f"({fk_in}) — bridge / link table shape."
                )
        elif naming == "fact":
            hypothesis = "fact"
            confidence = (
                "high"
                if (fk_out >= 2 or rc_percentile is not None and rc_percentile >= 0.75)
                else "medium"
            )
        elif naming == "dimension":
            hypothesis = "dimension"
            confidence = "high" if fk_in >= 1 else "medium"
        else:
            # Pure structural inference. Order matters — the
            # column-shape signal (measures + ids) wins for SAP /
            # FK-free schemas because that's the only ground truth
            # left when naming is opaque AND constraints aren't
            # declared.
            if (
                fk_out >= 3
                and (rc_percentile is None or rc_percentile >= 0.6)
                and (is_partitioned or has_temporal)
            ):
                hypothesis = "fact"
                confidence = "medium"
                evidence.append(
                    "No naming signal; classified by structure (high FK "
                    "fan-out + temporal/partitioned)."
                )
            elif (
                shape["measures"] >= 3
                and shape["ids"] >= 4
                and (has_temporal or row_count >= 10_000)
            ):
                # Column-shape fact heuristic — fires when FK
                # constraints are absent (typical SAP) but the column
                # mix screams "transactional with measures + foreign
                # keys at the application layer".
                hypothesis = "fact"
                confidence = "medium"
                evidence.append(
                    f"No FK / naming signal; column-shape shows "
                    f"{shape['measures']} measure(s) + {shape['ids']} "
                    f"key(s) + temporal — fact-shaped row."
                )
            elif fk_in >= 3 and fk_out <= 1:
                hypothesis = "dimension"
                confidence = "medium"
                evidence.append(
                    "No naming signal; classified by structure (high FK fan-in, low fan-out)."
                )
            elif shape["descriptives"] >= 5 and shape["measures"] == 0 and row_count <= 100_000:
                # Column-shape dimension heuristic — many descriptive
                # columns + no measures + moderate row count.
                hypothesis = "dimension"
                confidence = "medium"
                evidence.append(
                    f"Column-shape dimension: {shape['descriptives']} "
                    "descriptive column(s) + no measures + moderate row "
                    "count."
                )
            elif row_count <= 1000 and col_count <= 12 and fk_in >= 1:
                hypothesis = "lookup"
                confidence = "medium"
                evidence.append("Small + referenced — likely lookup / reference table.")
            elif has_temporal and not (is_partitioned or fk_out):
                hypothesis = "transactional"
                confidence = "low"
                evidence.append(
                    "Temporal column present but no partitioning / FKs out "
                    "— likely raw transactional / event log."
                )

        if not evidence:
            evidence.append(
                "No strong signals — naming, FK structure, and row count "
                "are all ambiguous. Try providing the schema context "
                "(rank-all-tables mode) or run the SCD detector if "
                "history shape matters."
            )

        return {
            "schema": str(profile.schema),
            "table": table_name,
            "role_hypothesis": hypothesis,
            "confidence": confidence,
            "evidence": evidence,
            "indicators": indicators,
        }

    def _tool_detect_scd_pattern(
        self,
        schema: str,
        table: str,
        business_key: list[str] | None = None,
    ) -> dict[str, Any]:
        """Infer SCD type from column-name patterns + sibling tables + key cardinality.

        The heuristic stack:

        1. Column-name patterns ⇒ Type 2 / Type 3 hints.
        2. Sibling-table lookup (``X_history`` / ``X_hist`` / ``X_audit``
           / ``X_log``) ⇒ Type 4 hint.
        3. When ``business_key`` is provided: row-per-key avg count ⇒
           Type 1 vs Type 2 (current-only vs history-rows).

        The hypothesis is the strongest signal that fired; ``evidence``
        captures every detected signal so the LLM can quote them
        verbatim instead of asserting the type without justification.
        """
        from sqlalchemy import text as _text

        schema_name = (schema or "").strip()
        table_name = (table or "").strip()
        if not schema_name or not table_name:
            raise _ToolError("Both 'schema' and 'table' are required.")

        # Profile the table once to get column names + dtypes + PK.
        try:
            profile = self._live_db().profile_table(
                schema_name,
                table_name,
                sample_size=0,
            )
        except Exception as exc:
            return {
                "schema": schema_name,
                "table": table_name,
                "found": False,
                "error": str(exc),
                "hint": ("If schema/table didn't resolve, call find_table_by_name first."),
            }

        col_names_lower = [str(c.name).lower() for c in profile.columns]
        col_lookup = {n: profile.columns[i] for i, n in enumerate(col_names_lower)}

        evidence: list[str] = []
        indicators: dict[str, Any] = {}

        # ── Type 2 — temporal row-validity pair ──
        valid_from_hits = [
            n for n in col_names_lower if any(p in n for p in self._SCD_VALID_FROM_NAMES)
        ]
        valid_to_hits = [
            n for n in col_names_lower if any(p in n for p in self._SCD_VALID_TO_NAMES)
        ]
        if valid_from_hits and valid_to_hits:
            indicators["type2_temporal_pair"] = [valid_from_hits[0], valid_to_hits[0]]
            evidence.append(f"Type 2 temporal pair: `{valid_from_hits[0]}` + `{valid_to_hits[0]}`.")
        elif valid_from_hits:
            indicators["type2_open_ended_temporal"] = valid_from_hits[0]
            evidence.append(
                f"Type 2 partial signal: `{valid_from_hits[0]}` exists "
                "but no matching end-of-validity column."
            )

        # ── Type 2 — current/active flag ──
        flag_hits = [
            n
            for n in col_names_lower
            if any(p == n or n.endswith("_" + p) or n == p for p in self._SCD_CURRENT_FLAG_NAMES)
            or n in self._SCD_CURRENT_FLAG_NAMES
        ]
        # Restrict to boolean-shape dtypes so a regular int isn't tagged.
        flag_hits = [
            n
            for n in flag_hits
            if any(
                token in str(col_lookup[n].dtype).lower()
                for token in ("bool", "char(1)", "varchar(1)")
            )
        ]
        if flag_hits:
            indicators["type2_current_flag"] = flag_hits[0]
            evidence.append(
                f"Type 2 current-flag column: `{flag_hits[0]}` "
                f"(dtype={col_lookup[flag_hits[0]].dtype})."
            )

        # ── Type 2 — version / revision column ──
        version_hits = [n for n in col_names_lower if n in self._SCD_VERSION_NAMES]
        if version_hits:
            indicators["type2_version_col"] = version_hits[0]
            evidence.append(f"Type 2 version column: `{version_hits[0]}`.")

        # ── Type 3 — paired (current_X, prev_X) columns ──
        prev_pairs: list[tuple[str, str]] = []
        for col in col_names_lower:
            for prev_p in self._SCD_PREV_PREFIXES:
                if col.startswith(prev_p):
                    base = col[len(prev_p) :]
                    # Look for the canonical sibling in the same table.
                    if base in col_names_lower:
                        prev_pairs.append((base, col))
                        break
                    # Or a new_/current_ prefix sibling.
                    for new_p in self._SCD_NEW_PREFIXES:
                        if (new_p + base) in col_names_lower:
                            prev_pairs.append((new_p + base, col))
                            break
                    break
        if prev_pairs:
            indicators["type3_prev_pairs"] = [
                {"current": cur, "previous": prev} for cur, prev in prev_pairs[:5]
            ]
            evidence.append(
                "Type 3 column pair(s): "
                + ", ".join(f"`{prev}`↔`{cur}`" for cur, prev in prev_pairs[:3])
                + "."
            )

        # ── Type 4 — sibling history table in same schema ──
        sibling_path = ""
        try:
            db = self._live_db()
            assets = (
                db.list_assets(schema_name)
                if hasattr(db, "list_assets")
                else ((n, "table") for n in db.list_tables(schema_name))
            )
            for name, _kind in assets:
                low = str(name).lower()
                for suffix in self._SCD_HISTORY_SUFFIXES:
                    if low == table_name.lower() + suffix:
                        sibling_path = f"{schema_name}.{name}"
                        break
                if sibling_path:
                    break
        except Exception:
            pass
        if sibling_path:
            indicators["type4_history_sibling"] = sibling_path
            evidence.append(
                f"Type 4 sibling history table: `{sibling_path}` exists next to the base table."
            )

        # ── Type 1 vs 2 — row-per-key probe (only if business_key given) ──
        rows_per_key: float | None = None
        if business_key:
            try:
                db = self._live_db()
                adapter = db._adapter  # noqa: SLF001
                fqn = adapter.fully_qualified_name(schema_name, table_name)
                q_cols = ", ".join(adapter.quote_identifier(c) for c in business_key)
                with db.engine.connect() as conn:
                    row = conn.execute(
                        _text(
                            f"SELECT COUNT(*) AS total, "
                            f"COUNT(DISTINCT ({q_cols})) AS distinct_keys "
                            f"FROM {fqn}"
                        ),
                    ).fetchone()
                if row and row[1]:
                    total = int(row[0] or 0)
                    distinct_keys = int(row[1])
                    rows_per_key = total / distinct_keys if distinct_keys else 0.0
                    indicators["business_key"] = list(business_key)
                    indicators["rows_per_key_avg"] = round(rows_per_key, 3)
                    indicators["total_rows"] = total
                    indicators["distinct_business_keys"] = distinct_keys
                    if rows_per_key <= 1.05:
                        evidence.append(
                            f"Avg rows-per-business-key = {rows_per_key:.2f} "
                            "→ current-only (Type 1)."
                        )
                    elif rows_per_key > 1.5:
                        evidence.append(
                            f"Avg rows-per-business-key = {rows_per_key:.2f} "
                            "→ multiple rows per key (likely Type 2)."
                        )
                    else:
                        evidence.append(
                            f"Avg rows-per-business-key = {rows_per_key:.2f} "
                            "→ ambiguous; could be Type 1 with rare history."
                        )
            except Exception as exc:
                evidence.append(f"Could not run rows-per-key probe: {exc}")

        # ── Decide hypothesis ──
        # Strongest signals win; sibling history table is the most
        # specific but we still surface other signals because real
        # systems often combine types (Type 6 = 1+2+3).
        type2_hits = (
            ("type2_temporal_pair" in indicators)
            + ("type2_current_flag" in indicators)
            + ("type2_version_col" in indicators)
            + (1 if rows_per_key is not None and rows_per_key > 1.5 else 0)
        )
        type3_hits = 1 if "type3_prev_pairs" in indicators else 0
        type4_hits = 1 if sibling_path else 0
        type1_signal = (
            rows_per_key is not None
            and rows_per_key <= 1.05
            and type2_hits == 0
            and type3_hits == 0
        )

        if type2_hits >= 2 or (type2_hits >= 1 and rows_per_key is not None and rows_per_key > 1.5):
            hypothesis = "type_2"
            confidence = "high" if type2_hits >= 2 else "medium"
        elif type3_hits and type2_hits == 0:
            hypothesis = "type_3"
            confidence = "medium"
        elif type4_hits and type2_hits == 0 and type3_hits == 0:
            hypothesis = "type_4"
            confidence = "medium"
        elif type1_signal:
            hypothesis = "type_1"
            confidence = "medium"
        elif type2_hits >= 1:
            hypothesis = "type_2"
            confidence = "low"
        else:
            hypothesis = "unknown"
            confidence = "low"
            if not evidence:
                evidence.append(
                    "No SCD-style signals found in column names or sibling "
                    "tables. The table may be append-only, fully overwritten "
                    "(Type 1), or use a custom convention."
                )

        # Alternative hypotheses — surface co-existing signals so the
        # LLM can mention "primarily Type 2 but a sibling history "
        # table also exists (so this is closer to Type 6)".
        alternatives: list[str] = []
        if hypothesis == "type_2" and type4_hits:
            alternatives.append("type_6 (Type 2 in main + Type 4 sibling = hybrid)")
        if hypothesis == "type_4" and type2_hits:
            alternatives.append("type_6 (history sibling + in-table type 2 signals = hybrid)")
        if hypothesis == "type_2" and type3_hits:
            alternatives.append("type_6 (in-table previous-value columns alongside row-history)")

        recommendation = ""
        if hypothesis == "type_2" and "type2_temporal_pair" not in indicators:
            recommendation = (
                "Type 2 inferred without an explicit valid_from/valid_to "
                "pair. To replay history at a point in time you'll need "
                "the version / current_flag column; consider asking for "
                "the load logic from your data team."
            )
        elif hypothesis == "type_1":
            recommendation = (
                "Type 1 inferred — only current values are kept. To get "
                "history you'd need a separate audit log or CDC stream."
            )
        elif hypothesis == "unknown" and not business_key:
            recommendation = (
                "No SCD signals from names/siblings. Re-call this tool "
                "with a candidate ``business_key`` so the rows-per-key "
                "probe can disambiguate Type 1 vs Type 2."
            )

        return {
            "schema": schema_name,
            "table": table_name,
            "found": True,
            "scd_type_hypothesis": hypothesis,
            "confidence": confidence,
            "evidence": evidence,
            "indicators": indicators,
            "alternative_hypotheses": alternatives,
            "recommendation": recommendation,
        }

    def _tool_detect_dimensional_role(
        self,
        schema: str,
        table: str | None = None,
    ) -> dict[str, Any]:
        """Single-table or schema-wide dimensional-role classifier.

        See the tool description for the full contract; this body just
        dispatches between per-table and schema-level classification.
        """
        schema_name = (schema or "").strip()
        if not schema_name:
            raise _ToolError("Argument 'schema' is required.")

        # ── Single-table mode ──
        if table:
            try:
                profile = self._live_db().profile_table(
                    schema_name,
                    table.strip(),
                    sample_size=0,
                )
            except Exception as exc:
                return {
                    "schema": schema_name,
                    "table": table,
                    "found": False,
                    "error": str(exc),
                }
            return {**self._classify_table_role(profile), "found": True}

        # ── Schema-level mode ──
        # Walk every asset in the schema, profile cheaply (no samples,
        # no large stats), classify, then derive the schema-level
        # pattern (star vs snowflake) from FK relationships among the
        # classified dimensions.
        db = self._live_db()
        try:
            if hasattr(db, "list_assets"):
                assets = [(str(n), str(k)) for n, k in db.list_assets(schema_name)]
            else:
                assets = [(str(n), "table") for n in db.list_tables(schema_name)]
        except Exception as exc:
            return {
                "schema": schema_name,
                "found": False,
                "error": f"Could not list tables in schema: {exc}",
            }
        if not assets:
            return {
                "schema": schema_name,
                "found": False,
                "table_count": 0,
                "tables_by_role": {},
                "pattern_hypothesis": "unknown",
                "evidence": [
                    "Schema has no tables to classify.",
                ],
            }

        # First pass: profile all tables to collect row counts (for
        # percentile) + FK info. Profiles WITHOUT samples are cheap.
        per_table: list[Any] = []
        peer_row_counts: list[int] = []
        for name, _kind in assets:
            try:
                p = db.profile_table(schema_name, name, sample_size=0)
                per_table.append(p)
                peer_row_counts.append(int(p.row_count or 0))
            except Exception:
                continue

        # Second pass: classify each with peer-row-count context.
        classifications: list[dict[str, Any]] = []
        # Build a (schema, table) → role lookup so we can later check
        # whether a dimension references another dimension (snowflake).
        for p in per_table:
            classifications.append(self._classify_table_role(p, peer_row_counts))

        role_to_paths: dict[str, list[str]] = {}
        for c in classifications:
            role = c["role_hypothesis"]
            role_to_paths.setdefault(role, []).append(f"{c['schema']}.{c['table']}")

        # Star vs snowflake — only meaningful if BOTH facts and
        # dimensions exist. Snowflake = at least one dimension references
        # another dimension. Star = dimensions are flat (only referenced
        # by facts, no FKs to other dimensions).
        pattern = "unknown"
        pattern_evidence: list[str] = []
        fact_paths = set(role_to_paths.get("fact", []))
        dim_paths = set(role_to_paths.get("dimension", []))
        if fact_paths and dim_paths:
            dim_to_dim_links = 0
            dim_to_dim_examples: list[str] = []
            for p in per_table:
                if f"{p.schema}.{p.name}" not in dim_paths:
                    continue
                for fk in p.foreign_keys or []:
                    target = (
                        f"{fk.get('referred_schema') or p.schema}.{fk.get('referred_table') or ''}"
                    )
                    if target in dim_paths and target != f"{p.schema}.{p.name}":
                        dim_to_dim_links += 1
                        if len(dim_to_dim_examples) < 3:
                            dim_to_dim_examples.append(f"{p.schema}.{p.name} → {target}")
            if dim_to_dim_links:
                pattern = "snowflake_schema"
                pattern_evidence.append(
                    f"{dim_to_dim_links} dimension-to-dimension FK link(s) "
                    "found (snowflake): " + ", ".join(dim_to_dim_examples)
                )
            else:
                pattern = "star_schema"
                pattern_evidence.append(
                    f"{len(fact_paths)} fact table(s) and "
                    f"{len(dim_paths)} dimension table(s); no "
                    "dimension-to-dimension FKs (star layout)."
                )
        elif not fact_paths and dim_paths:
            pattern = "flat"
            pattern_evidence.append(
                "No fact-shaped tables; the schema looks like a "
                "denormalised dim-only or reference layout."
            )
        elif fact_paths and not dim_paths:
            pattern = "fact_only"
            pattern_evidence.append(
                "Fact tables present but no dimension-shaped tables "
                "found — possibly an OBT (one-big-table) layout."
            )

        return {
            "schema": schema_name,
            "found": True,
            "table_count": len(per_table),
            "pattern_hypothesis": pattern,
            "pattern_evidence": pattern_evidence,
            "tables_by_role": role_to_paths,
            "fact_tables": role_to_paths.get("fact", []),
            "dimension_tables": role_to_paths.get("dimension", []),
            "bridge_tables": role_to_paths.get("bridge", []),
            "lookup_tables": role_to_paths.get("lookup", []),
            "staging_tables": role_to_paths.get("staging", []),
            "audit_tables": role_to_paths.get("audit", []),
            "transactional_tables": role_to_paths.get("transactional", []),
            "unknown_tables": role_to_paths.get("unknown", []),
            "classifications": classifications,
        }
