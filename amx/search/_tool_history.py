"""History, chat-session, and read-only schedule tools for :class:`ToolBox`.

The six methods in :class:`_HistoryToolsMixin` answer LLM questions about
past ``/ask`` runs, chat sessions, and scheduled jobs. They share two
needs from the host ``ToolBox``:

* ``self.cfg`` — the active :class:`amx.config.AMXConfig` for resolving
  scope and profile names.
* ``self._history_store()`` — the lazy :class:`amx.storage.protocol.IHistoryStore`
  singleton accessor.

The mixin is compose-only — it never overrides ``ToolBox.__init__`` or
any property; consumers mix it into ``ToolBox`` for tool dispatch.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from amx.config import AMXConfig


class _HistoryToolsMixin:
    """Read-only history, chat-session, and schedule tool implementations."""

    # Provided by the host ``ToolBox`` instance.
    cfg: AMXConfig

    def _history_store(self):  # pragma: no cover - host method
        raise NotImplementedError

    def _tool_list_past_runs(
        self,
        schema: str = "",
        table: str = "",
        command: str = "",
        limit: int = 10,
    ) -> dict[str, Any]:
        """List the user's past ``/run`` invocations from the local SQLite history.

        Defaults to ``analyze.run`` only — matches the user's mental
        model where "runs" means data-analysis invocations and "asks"
        are conversational chats listed by ``list_chat_sessions``.
        Each row carries human-friendly fields (``started_at`` ISO
        string, ``duration_human``) plus the raw epoch / float for
        machine consumption, so the LLM can produce a clean answer
        without doing arithmetic on ``1777675166.705911``.
        """
        import datetime as _dt

        from amx.storage.sqlite_store import history_store

        hs = history_store()
        if hs is None:
            return {
                "runs": [],
                "count": 0,
                "note": (
                    "No local history store is initialised in this process. "
                    "This usually means /ask was invoked outside the standard CLI; "
                    "tell the user we can't introspect past runs in that context."
                ),
            }
        clamped_limit = max(1, min(int(limit) if limit else 10, 50))
        # Default to /run history. The LLM (or the user) must explicitly
        # ask for ``search.ask`` or ``all`` to widen the filter — see the
        # tool description and the system prompt's routing rule.
        raw_cmd = command.strip().lower() if command else ""
        if raw_cmd in ("", "analyze.run", "run"):
            cmd: str | None = "analyze.run"
        elif raw_cmd in ("search.ask", "ask"):
            cmd = "search.ask"
        elif raw_cmd == "all":
            cmd = None
        else:
            return {
                "error": (
                    f"Invalid 'command' filter {command!r} — must be 'analyze.run', "
                    "'search.ask', or 'all'. Omit the parameter for /run history "
                    "(the most common case)."
                )
            }

        try:
            rows = hs.find_runs_for_scope(
                schema=(schema.strip() or None) if schema else None,
                table=(table.strip() or None) if table else None,
                command_filter=cmd,
                limit=clamped_limit,
            )
        except Exception as exc:
            return {"error": f"Could not query history: {exc}"}

        def _human_duration(sec: float) -> str:
            if sec is None or sec <= 0:
                return "—"
            s = float(sec)
            if s < 60:
                return f"{s:.1f}s"
            m, rem = divmod(s, 60)
            return f"{int(m)}m {rem:0.0f}s"

        def _iso(epoch: float) -> str:
            try:
                return _dt.datetime.fromtimestamp(float(epoch or 0)).strftime("%Y-%m-%d %H:%M")
            except Exception:
                return "—"

        compact: list[dict[str, Any]] = []
        for r in rows:
            metrics = r.get("metrics_json") if isinstance(r, dict) else None
            if not isinstance(metrics, dict):
                metrics = {}
            tokens = r.get("tokens_json") if isinstance(r, dict) else None
            total_tokens = 0
            if isinstance(tokens, dict):
                try:
                    total_tokens = int(tokens.get("total_tokens") or 0)
                except (TypeError, ValueError):
                    total_tokens = 0
            duration = float(r.get("duration_sec") or 0.0)
            model_proc = float(metrics.get("model_processing_sec") or 0.0)
            compact.append(
                {
                    "run_id": int(r.get("id") or 0),
                    "started_at": _iso(r.get("started_at")),
                    "started_at_epoch": float(r.get("started_at") or 0.0),
                    "duration_human": _human_duration(duration),
                    "duration_sec": round(duration, 2),
                    "model_processing_human": _human_duration(model_proc),
                    "model_processing_sec": round(model_proc, 2),
                    "status": r.get("status") or "",
                    "command": r.get("command") or "",
                    "scope": r.get("scope_json") or {},
                    "db_profile": r.get("db_profile") or "",
                    "llm_profile": r.get("llm_profile") or "",
                    "llm_model": r.get("llm_model") or "",
                    "doc_profile": r.get("doc_profile") or "",
                    "code_profile": r.get("code_profile") or "",
                    "settings": r.get("settings_json") or {},
                    "selected_count": int(r.get("selected_count") or 0),
                    "processed_count": int(r.get("processed_count") or 0),
                    "applied_count": int(r.get("applied_count") or 0),
                    "total_tokens": total_tokens,
                }
            )

        return {
            "runs": compact,
            "count": len(compact),
            "filter": {
                "schema": schema or "",
                "table": table or "",
                "command": cmd or "all",
                "limit": clamped_limit,
            },
            "presentation_hint": (
                "When the user asks for a table, render a Rich-friendly compact table "
                "with at most 6 columns: Run ID, Started, Duration, Status, LLM model, "
                "Total tokens. Use the human-readable fields (started_at, "
                "duration_human) — never the raw epoch or raw float seconds. Quote "
                "longer fields (scope, settings) inline as text below the table."
            ),
        }

    def _tool_describe_run(
        self,
        run_id: int,
        include_results: bool = True,
        include_variations: bool = True,
    ) -> dict[str, Any]:
        """Return the full record for one past run, optionally with results.

        When ``include_variations`` is True (the default) each result row
        in ``results`` gains a ``variations`` sub-array listing the v2,
        v3, ... rows generated by Re-Run / Variations against that v1
        asset, with the variation's ``mode`` (``semantic`` |
        ``lexical``), the ``seed_alternative_text`` the user picked, the
        ``descendant_run_id``, and the variation's own chosen description
        + alternatives. The v1 result also carries an
        ``alternatives_mode`` field at the top level so the assistant
        can reason about the original row's mode without diving into
        ``alternatives_json``.

        Semantic mode means the variation is a *paraphrase* of the seed
        (same factual content, different surface form). Lexical mode
        means the variation re-uses the seed's key vocabulary but
        proposes a DISTINCT CANDIDATE MEANING the reviewer can tell
        apart. Use these fields whenever the user asks about a
        column's history, asks for an evaluation of the alternatives,
        or wants commentary on how semantic vs lexical variations
        differ.
        """
        from amx.storage.sqlite_store import history_store

        hs = history_store()
        if hs is None:
            return {
                "error": (
                    "No local history store is initialised in this process. "
                    "Cannot describe past runs."
                )
            }
        try:
            rid = int(run_id)
        except (TypeError, ValueError):
            return {"error": f"Invalid run_id {run_id!r} — must be an integer."}

        try:
            row = hs.get_run(rid)
        except Exception as exc:
            return {"error": f"Failed to load run #{rid}: {exc}"}
        if row is None:
            return {"error": f"Run #{rid} not found in history.db."}

        out: dict[str, Any] = {
            "run_id": rid,
            "started_at_epoch": float(row.get("started_at") or 0.0),
            "ended_at_epoch": float(row.get("ended_at") or 0.0),
            "duration_sec": float(row.get("duration_sec") or 0.0),
            "status": row.get("status") or "",
            "command": row.get("command") or "",
            "mode": row.get("mode") or "",
            "scope": row.get("scope_json") or {},
            "db_profile": row.get("db_profile") or "",
            "db_backend": row.get("db_backend") or "",
            "llm_profile": row.get("llm_profile") or "",
            "llm_provider": row.get("llm_provider") or "",
            "llm_model": row.get("llm_model") or "",
            "doc_profile": row.get("doc_profile") or "",
            "code_profile": row.get("code_profile") or "",
            "settings": row.get("settings_json") or {},
            "metrics": row.get("metrics_json") or {},
            "tokens": row.get("tokens_json") or {},
            "selected_count": int(row.get("selected_count") or 0),
            "planned_count": int(row.get("planned_count") or 0),
            "processed_count": int(row.get("processed_count") or 0),
            "applied_count": int(row.get("applied_count") or 0),
            "review_strategy": row.get("review_strategy") or "",
            "error_text": row.get("error_text") or "",
        }

        if include_results:
            try:
                results = hs.get_run_results(rid)
            except Exception as exc:
                results = []
                out["results_warning"] = f"Could not load run_results: {exc}"

            # Variations / Re-Run descendants for this run, indexed by
            # the v1 asset key so each result row can carry its own
            # ``variations`` block. Built once per call so a wide run
            # (200 columns) still hits ``get_descendant_runs`` exactly
            # once. Variations descendants stack chronologically as
            # v2, v3, ... per-asset; the rule mirrors the run-detail
            # page's version labelling so what the LLM sees and what
            # the Studio renders agree.
            variations_by_asset: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
            if include_variations:
                try:
                    descendants = hs.get_descendant_runs(rid)
                except Exception as exc:
                    descendants = []
                    out["descendants_warning"] = f"Could not load descendant runs: {exc}"
                for entry in descendants:
                    for dr in entry.get("rows", []) or []:
                        key = (
                            str(dr.get("schema_name") or ""),
                            str(dr.get("table_name") or ""),
                            str(dr.get("column_name") or ""),
                        )
                        variations_by_asset.setdefault(key, []).append(
                            {
                                "kind": entry.get("kind"),
                                "mode": dr.get("alternatives_mode") or entry.get("mode"),
                                "seed_alternative_text": dr.get("seed_alternative_text")
                                or entry.get("seed_alternative_text"),
                                "descendant_run_id": entry.get("run_id"),
                                "chosen_description": dr.get("chosen_description") or "",
                                "alternatives": (
                                    dr.get("alternatives_json")
                                    if isinstance(dr.get("alternatives_json"), list)
                                    else []
                                ),
                                "confidence": dr.get("confidence") or "",
                                "confidence_signal": entry.get("confidence_signal"),
                                "_run_id": entry.get("run_id"),
                            }
                        )
                # Chronological order per asset (mirrors run-detail's
                # v2..vN rule). Then stamp ``version_label`` in-place
                # and drop the internal ``_run_id`` sort key.
                for variants in variations_by_asset.values():
                    variants.sort(key=lambda v: int(v.get("_run_id") or 0))
                    for idx, v in enumerate(variants, start=2):
                        v["version_label"] = f"v{idx}"
                        v.pop("_run_id", None)

            # Compact the results for LLM consumption — drop heavy raw
            # fields the model rarely needs. ``applied`` / ``applied_at``
            # let the assistant answer "which columns applied?" without
            # guessing, and the precomputed ``applied_columns`` summary
            # below is what it should quote verbatim for partial-apply runs.
            results_out = []
            for r in results:
                row_out = {
                    "schema": r.get("schema_name") or "",
                    "table": r.get("table_name") or "",
                    "column": r.get("column_name") or "",
                    "asset_kind": r.get("asset_kind") or "table",
                    "source": r.get("source") or "",
                    "confidence": r.get("confidence") or "",
                    "logprob_score": r.get("logprob_score"),
                    "token_count": r.get("token_count"),
                    "model_version": r.get("model_version") or "",
                    "chosen_description": r.get("chosen_description") or "",
                    "evaluation": r.get("evaluation") or "",
                    "applied": (r.get("db_applied_status") or "") == "applied",
                    "applied_at": r.get("applied_at"),
                    "alternatives": (
                        r.get("alternatives_json")
                        if isinstance(r.get("alternatives_json"), list)
                        else []
                    ),
                    "alternatives_mode": r.get("alternatives_mode"),
                }
                if include_variations:
                    asset_key = (
                        row_out["schema"],
                        row_out["table"],
                        row_out["column"],
                    )
                    row_out["variations"] = variations_by_asset.get(asset_key, [])
                results_out.append(row_out)
            out["results"] = results_out
            out["results_count"] = len(results_out)
            out["applied_columns"] = [
                {
                    "schema": r["schema"],
                    "table": r["table"],
                    "column": r["column"],
                    "chosen_description": r["chosen_description"],
                    "applied_at": r["applied_at"],
                }
                for r in results_out
                if r["applied"]
            ]
        return out

    def _tool_compare_runs(
        self,
        run_ids: list[int] | None = None,
        include_per_column: bool = False,
        column_filter: str = "",
        quality_tier: int = 0,
        ground_truth_run_id: int | None = None,
    ) -> dict[str, Any]:
        """Side-by-side comparison of two or more past runs.

        Wraps the pure ``compare_runs`` helper that already powers the
        CLI ``/history compare`` and the Studio Compare modal. Defaults
        to a SUMMARY payload (runs + summary_rows + aggregates + 3-row
        sample) so the model doesn't blow its context window on an
        8-run × 200-column pivot; the LLM can re-call with
        ``include_per_column=true`` or ``column_filter="<col>"`` when
        the user actually wants the per-column descriptions.
        """
        from amx.cli_support.commands.compare import compare_runs

        # Validate the input set before delegating so the LLM gets a
        # crisp error message instead of a deeper TypeError.
        if not run_ids:
            return {
                "error": (
                    "compare_runs needs at least 2 run IDs. Pass run_ids "
                    "as an array of integers, or call list_past_runs "
                    "first to resolve them from a scope hint."
                )
            }
        try:
            normalized_ids = [int(r) for r in run_ids]
        except (TypeError, ValueError) as exc:
            return {"error": f"Invalid run_ids: {exc}"}
        if len(normalized_ids) < 2:
            return {
                "error": ("compare_runs needs at least 2 run IDs to make a comparison meaningful.")
            }

        # Clamp tier so a malformed LLM call (tier=99) doesn't pull
        # the LLM judge unexpectedly. Tier 2 needs an active
        # LLMProvider — build one lazily so every Tier 0/1 call stays
        # cheap.
        try:
            tier = int(quality_tier)
        except (TypeError, ValueError):
            tier = 0
        tier = max(0, min(2, tier))
        llm_provider = None
        db_connector = None
        if tier >= 2:
            try:
                from amx.llm.provider import LLMProvider

                if self.cfg.llm.provider and self.cfg.llm.model:
                    llm_provider = LLMProvider(self.cfg.llm)
                else:
                    # No active LLM → silently demote to Tier 1 so the
                    # caller still gets useful metrics.
                    tier = 1
            except Exception:
                tier = 1
        if tier > 0:
            try:
                from amx.db.connector import DatabaseConnector

                db_connector = DatabaseConnector(self.cfg.db)
            except Exception:
                db_connector = None

        try:
            payload = compare_runs(
                normalized_ids,
                quality_tier=tier,
                ground_truth_run_id=ground_truth_run_id,
                db_connector=db_connector,
                llm_provider=llm_provider,
            )
        except RuntimeError as exc:
            # ``compare_runs`` raises RuntimeError when the history store
            # isn't initialised — surface it verbatim so the LLM can
            # tell the user to activate a profile.
            return {"error": str(exc)}

        per_column = list(payload.get("per_column") or [])
        if column_filter:
            needle = column_filter.strip()
            filtered = [r for r in per_column if str(r.get("column") or "").strip() == needle]
        else:
            filtered = per_column

        result: dict[str, Any] = {
            "runs": payload.get("runs") or [],
            "summary_rows": payload.get("summary_rows") or [],
            "aggregates": payload.get("aggregates") or [],
            "missing": payload.get("missing") or [],
            "per_column_count": len(filtered),
        }
        if include_per_column or column_filter:
            # Caller asked for the full pivot (or for a single-column
            # slice, which is small by definition).
            result["per_column"] = filtered
        else:
            # Cheap-context default: a 3-row sample so the model can
            # eyeball the shape and decide whether to drill in.
            result["per_column_sample"] = filtered[:3]
        if column_filter:
            result["column_filter"] = column_filter
        # Surface quality_metrics back to the LLM when the caller
        # opted into Tier ≥ 1. The metrics dict is already shaped for
        # rendering (per_run rollups + per_asset cells + citations);
        # the LLM is expected to read per_run + citations and explain
        # WHY each winner wins (system-prompt routing rule).
        if "quality_metrics" in payload:
            result["quality_metrics"] = payload["quality_metrics"]
        return result

    def _tool_list_chat_sessions(
        self,
        limit: int = 20,
        include_ended: bool = True,
    ) -> dict[str, Any]:
        """List the user's past ``/ask`` chat sessions (resumable conversations).

        ``/ask`` invocations form a stateful conversation thread (the
        chat_sessions / chat_turns SQLite tables). Each row here
        carries the session id, when it started, last activity time,
        whether it's still open, turn count, total tokens, and the
        first user question as a preview. Tell the user they can
        resume any ended session via ``/session resume <id>``.

        Use this — NOT ``list_past_runs(command="search.ask")`` —
        when the user asks "show me my past chats" / "my ask history"
        / "previous /ask conversations". The two surfaces store the
        same conceptual data differently: ``analysis_runs`` rows for
        ``search.ask`` are PER-TURN audit log entries (one per
        question asked); ``chat_sessions`` rows are PER-CONVERSATION
        threads. Users almost always want the latter.
        """
        import datetime as _dt

        from amx.search.session_store import ChatSessionStore
        from amx.storage.sqlite_store import history_store

        hs = history_store()
        if hs is None:
            return {
                "sessions": [],
                "count": 0,
                "note": "No local history store is initialised in this process.",
            }

        clamped_limit = max(1, min(int(limit) if limit else 20, 100))
        try:
            rows = ChatSessionStore(hs).list_sessions(
                limit=clamped_limit,
                include_ended=bool(include_ended),
            )
        except Exception as exc:
            return {"error": f"Could not query chat sessions: {exc}"}

        def _iso(epoch: Any) -> str:
            try:
                v = float(epoch or 0)
                if v <= 0:
                    return "—"
                return _dt.datetime.fromtimestamp(v).strftime("%Y-%m-%d %H:%M")
            except Exception:
                return "—"

        sessions: list[dict[str, Any]] = []
        for row in rows:
            ended_epoch = row.get("ended_at")
            sessions.append(
                {
                    "session_id": int(row.get("id") or 0),
                    "db_profile": row.get("db_profile") or "",
                    "llm_profile": row.get("llm_profile") or "",
                    "started_at": _iso(row.get("started_at")),
                    "last_active_at": _iso(row.get("last_active_at")),
                    "ended_at": _iso(ended_epoch) if ended_epoch else "",
                    "is_active": ended_epoch is None,
                    "title": row.get("title") or "",
                    "turn_count": int(row.get("turn_count") or 0),
                    "total_tokens": int(row.get("total_tokens") or 0),
                    "first_question": row.get("first_question") or "",
                }
            )

        return {
            "sessions": sessions,
            "count": len(sessions),
            "note": (
                "Resume any ended session in the CLI with `/session resume <id>`. "
                "Active sessions (is_active=true) are the currently-open thread."
            ),
        }

    def _tool_list_schedules(
        self,
        filter: str = "active",  # noqa: A002 - matches the LLM tool schema
        db_profile: str | None = None,
    ) -> dict[str, Any]:
        store = self._history_store()
        if store is None:
            return {"error": "history store not initialized", "schedules": []}
        if filter == "past":
            statuses = ["completed", "failed", "cancelled"]
        elif filter == "all":
            statuses = None
        else:
            # active is the default; everything that isn't terminal.
            statuses = ["pending", "paused", "missed", "running"]
        rows = store.list_scheduled_runs(statuses=statuses, db_profile=db_profile, limit=500)
        return {
            "filter": filter,
            "db_profile": db_profile,
            "count": len(rows),
            "schedules": [
                {
                    "id": r["id"],
                    "name": r["name"],
                    "fire_at_utc": r["fire_at_utc"],
                    "fire_at_tz": r["fire_at_tz"],
                    "status": r["status"],
                    "db_profile": r["db_profile"],
                    "scope_json": r["scope_json"],
                    "llm_profile": r["llm_profile"],
                    "review_strategy": r["review_strategy"],
                    "triggered_run_id": r.get("triggered_run_id"),
                    "last_error": r.get("last_error"),
                }
                for r in rows
            ],
        }

    def _tool_get_schedule(self, schedule_id: int) -> dict[str, Any]:
        store = self._history_store()
        if store is None:
            return {"error": "history store not initialized"}
        row = store.get_scheduled_run(int(schedule_id))
        if row is None:
            return {"error": f"no scheduled_runs row with id={schedule_id}"}
        return {"schedule": row}
