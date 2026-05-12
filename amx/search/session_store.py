"""SQLite-backed conversational session store for `/ask` chat.

Composes :class:`amx.storage.sqlite_store.SQLiteHistoryStore` (does *not* open
its own connection) so all chat persistence lives in the same `~/.amx/history.db`
as run history. Two tables back this store: ``chat_sessions`` and ``chat_turns``.

Compaction is triggered from :meth:`SearchAgent.ask` via
:meth:`maybe_compact`. Old turns are summarised by the active LLM into a
single synthetic ``role='summary'`` turn; if no LLM is available the older
turns are still soft-deleted but with a stub summary so the read path stays
honest.
"""

from __future__ import annotations

import json
import time
from typing import Any

from amx.storage.sqlite_store import SQLiteHistoryStore
from amx.utils.logging import get_logger
from amx.utils.token_tracker import estimate_tokens

log = get_logger("search.session_store")


_COMPACTION_RATIO = 0.40  # of model input budget — when exceeded, compact.
_KEEP_TAIL_RATIO = 0.70  # post-compaction kept tail size, relative to threshold.
_SUMMARY_MAX_TOKENS = 400


def _input_budget_for(model: str | None) -> int:
    # Mirrors amx.search.agent._input_token_budget_for to avoid a circular
    # import — both functions are intentionally tiny.
    if not model:
        return 60_000
    name = model.lower()
    if any(
        token in name
        for token in (
            "claude-3-5",
            "claude-sonnet-4",
            "claude-opus-4",
            "claude-3-opus",
            "claude-haiku-4",
        )
    ):
        return 150_000
    if any(
        token in name
        for token in (
            "gemini-1.5-pro",
            "gemini-2.0-pro",
            "gemini-2.0-flash",
            "gemini-1.5-flash",
        )
    ):
        return 250_000
    return 60_000


def _estimate_turn_tokens(question: str | None, answer_summary: str | None) -> int:
    msgs: list[dict[str, str]] = []
    if question:
        msgs.append({"role": "user", "content": question})
    if answer_summary:
        msgs.append({"role": "assistant", "content": answer_summary})
    if not msgs:
        return 1
    return estimate_tokens(msgs)


class ChatSessionStore:
    """Persist conversational sessions and turns.

    The store reuses ``SQLiteHistoryStore``'s connection helpers and lock so
    chat I/O participates in the same WAL-mode database as run history.
    """

    def __init__(self, history: SQLiteHistoryStore) -> None:
        self._history = history

    # ── lifecycle ────────────────────────────────────────────────────────

    def start_session(
        self,
        *,
        db_profile: str,
        llm_profile: str,
        title: str | None = None,
        scope_profiles: list[str] | None = None,
    ) -> int:
        """Open a new chat session row.

        ``scope_profiles`` is the multi-profile ask scope sticky for this
        session — Studio's dropdown and CLI's ``/ask-scope`` write through
        :meth:`update_scope`. Empty / ``None`` means "use config default
        (every saved DB profile)".
        """
        now = time.time()
        scope_json = json.dumps(list(scope_profiles)) if scope_profiles else None
        with self._history._lock, self._history._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO chat_sessions
                    (db_profile, llm_profile, started_at, last_active_at, title,
                     scope_profiles_json)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (str(db_profile), str(llm_profile), now, now, title, scope_json),
            )
            return int(cur.lastrowid or 0)

    def end_session(self, session_id: int) -> None:
        with self._history._lock, self._history._connect() as conn:
            conn.execute(
                "UPDATE chat_sessions SET ended_at = ?, last_active_at = ? WHERE id = ?",
                (time.time(), time.time(), int(session_id)),
            )

    def delete_session(self, session_id: int) -> bool:
        """Drop a chat session and every turn it owns.

        Returns ``True`` when a row was deleted, ``False`` when the id
        was unknown. ``end_session`` marks a session inactive but
        preserves history; delete is the hard-remove the Studio left
        rail and the CLI need so a user can clean up stale chats they
        no longer want to see in the picker.
        """
        sid = int(session_id)
        with self._history._lock, self._history._connect() as conn:
            cur = conn.execute("DELETE FROM chat_turns WHERE session_id = ?", (sid,))
            cur = conn.execute("DELETE FROM chat_sessions WHERE id = ?", (sid,))
            return bool(cur.rowcount)

    def get_session(self, session_id: int) -> dict[str, Any] | None:
        with self._history._connect() as conn:
            row = conn.execute(
                "SELECT * FROM chat_sessions WHERE id = ?",
                (int(session_id),),
            ).fetchone()
        return dict(row) if row else None

    def update_scope(
        self,
        session_id: int,
        *,
        scope_profiles: list[str] | None,
        focus_profile: str | None = None,
    ) -> None:
        """Replace the sticky scope on an existing session.

        ``scope_profiles=None`` clears the override and falls back to
        config default. ``focus_profile`` is the auto-detected
        conversation focus (computed in tool_agent); the SPA stores it
        for read-only display.
        """
        scope_json = json.dumps(list(scope_profiles)) if scope_profiles is not None else None
        with self._history._lock, self._history._connect() as conn:
            conn.execute(
                "UPDATE chat_sessions SET scope_profiles_json = ?, "
                "focus_profile = ?, last_active_at = ? WHERE id = ?",
                (scope_json, focus_profile, time.time(), int(session_id)),
            )

    def get_scope(self, session_id: int) -> list[str] | None:
        """Return the sticky scope for *session_id* or ``None`` when
        unset (caller should fall back to config default)."""
        with self._history._connect() as conn:
            row = conn.execute(
                "SELECT scope_profiles_json FROM chat_sessions WHERE id = ?",
                (int(session_id),),
            ).fetchone()
        if not row:
            return None
        raw = row["scope_profiles_json"]
        if not raw:
            return None
        try:
            value = json.loads(raw)
        except (TypeError, ValueError):
            return None
        if not isinstance(value, list):
            return None
        return [str(name).strip() for name in value if str(name).strip()]

    def list_sessions(
        self,
        *,
        db_profile: str | None = None,
        llm_profile: str | None = None,
        limit: int = 20,
        include_ended: bool = True,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if db_profile is not None:
            clauses.append("db_profile = ?")
            params.append(str(db_profile))
        if llm_profile is not None:
            clauses.append("llm_profile = ?")
            params.append(str(llm_profile))
        if not include_ended:
            clauses.append("ended_at IS NULL")
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(max(1, int(limit)))
        with self._history._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT s.*,
                       (SELECT question FROM chat_turns t
                        WHERE t.session_id = s.id AND t.role = 'user'
                        ORDER BY t.turn_index ASC LIMIT 1) AS first_question
                FROM chat_sessions s
                {where}
                ORDER BY s.last_active_at DESC
                LIMIT ?
                """,
                tuple(params),
            ).fetchall()
        return [dict(row) for row in rows]

    # ── turn I/O ─────────────────────────────────────────────────────────

    def _next_turn_index(self, conn: Any, session_id: int) -> int:
        row = conn.execute(
            "SELECT COALESCE(MAX(turn_index), -1) + 1 AS next FROM chat_turns WHERE session_id = ?",
            (int(session_id),),
        ).fetchone()
        return int(row["next"] if row else 0)

    def append_user_turn(
        self,
        session_id: int,
        *,
        question: str,
        estimated_tokens: int | None = None,
    ) -> int:
        est = (
            estimated_tokens
            if estimated_tokens is not None
            else _estimate_turn_tokens(question, None)
        )
        now = time.time()
        with self._history._lock, self._history._connect() as conn:
            idx = self._next_turn_index(conn, session_id)
            cur = conn.execute(
                """
                INSERT INTO chat_turns
                    (session_id, run_id, turn_index, role, question, answer_summary,
                     intent, topic, confidence, tables_json, columns_json,
                     plan_json, tokens_json, request_id, created_at, estimated_tokens)
                VALUES (?, NULL, ?, 'user', ?, NULL, NULL, NULL, NULL,
                        '[]', '[]', NULL, NULL, NULL, ?, ?)
                """,
                (int(session_id), idx, str(question), now, int(est)),
            )
            conn.execute(
                "UPDATE chat_sessions SET turn_count = turn_count + 1, "
                "total_tokens = total_tokens + ?, last_active_at = ? WHERE id = ?",
                (int(est), now, int(session_id)),
            )
            return int(cur.lastrowid or 0)

    def append_assistant_turn(
        self,
        session_id: int,
        *,
        run_id: int | None,
        answer_summary: str,
        intent: str = "",
        topic: str = "",
        confidence: str = "",
        tables: list[str] | None = None,
        columns: list[str] | None = None,
        plan: dict[str, Any] | None = None,
        tokens: dict[str, Any] | None = None,
        request_id: str | None = None,
        estimated_tokens: int | None = None,
    ) -> int:
        est = (
            estimated_tokens
            if estimated_tokens is not None
            else _estimate_turn_tokens(None, answer_summary)
        )
        now = time.time()
        tables_json = json.dumps(list(tables or []), ensure_ascii=True)
        columns_json = json.dumps(list(columns or []), ensure_ascii=True)
        plan_json = json.dumps(plan or {}, ensure_ascii=True) if plan is not None else None
        tokens_json = json.dumps(tokens or {}, ensure_ascii=True) if tokens is not None else None
        with self._history._lock, self._history._connect() as conn:
            idx = self._next_turn_index(conn, session_id)
            cur = conn.execute(
                """
                INSERT INTO chat_turns
                    (session_id, run_id, turn_index, role, question, answer_summary,
                     intent, topic, confidence, tables_json, columns_json,
                     plan_json, tokens_json, request_id, created_at, estimated_tokens)
                VALUES (?, ?, ?, 'assistant', NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(session_id),
                    int(run_id) if run_id is not None else None,
                    idx,
                    str(answer_summary or ""),
                    str(intent or ""),
                    str(topic or ""),
                    str(confidence or ""),
                    tables_json,
                    columns_json,
                    plan_json,
                    tokens_json,
                    str(request_id) if request_id else None,
                    now,
                    int(est),
                ),
            )
            conn.execute(
                "UPDATE chat_sessions SET turn_count = turn_count + 1, "
                "total_tokens = total_tokens + ?, last_active_at = ? WHERE id = ?",
                (int(est), now, int(session_id)),
            )
            return int(cur.lastrowid or 0)

    def recent_turns(
        self,
        session_id: int,
        *,
        limit: int | None = None,
        include_summary: bool = True,
        include_compacted: bool = False,
    ) -> list[dict[str, Any]]:
        """Return ordered turns (oldest first) for the active session.

        ``limit`` is interpreted as the most-recent ``limit`` *Q/A turn pairs*
        (where one pair = one user + one assistant turn). When the slice would
        cut a pair, we keep the assistant turn — follow-up planners need the
        prior answer summary, not just the prior question.
        """
        if not session_id:
            return []
        clauses = ["session_id = ?"]
        params: list[Any] = [int(session_id)]
        if not include_compacted:
            clauses.append("compacted_at IS NULL")
        if not include_summary:
            clauses.append("role != 'summary'")
        where = " AND ".join(clauses)
        with self._history._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT * FROM chat_turns
                WHERE {where}
                ORDER BY turn_index ASC
                """,
                tuple(params),
            ).fetchall()
        out = [self._hydrate_turn(row) for row in rows]
        if limit is not None and limit > 0:
            # `limit` Q/A pairs ≈ `limit * 2` rows. Keep the most recent slice
            # but never drop a 'summary' row — its whole purpose is to carry
            # compacted history forward. Compaction appends the summary as the
            # newest row (highest turn_index), so a naive "last N" slice would
            # already include it; we still pull summaries out and put them
            # first so the planner sees prior context before the recent tail.
            target = max(2, int(limit) * 2)
            if len(out) > target:
                summaries = [t for t in out if t.get("role") == "summary"]
                non_summary = [t for t in out if t.get("role") != "summary"]
                tail = non_summary[-target:]
                out = summaries + tail
        return out

    @staticmethod
    def _hydrate_turn(row: Any) -> dict[str, Any]:
        d = dict(row)
        for key in ("tables_json", "columns_json", "plan_json", "tokens_json"):
            raw = d.get(key)
            if isinstance(raw, str) and raw:
                try:
                    d[key.removesuffix("_json")] = json.loads(raw)
                except Exception:
                    d[key.removesuffix("_json")] = (
                        []
                        if key.endswith("_json") and key.startswith(("tables", "columns"))
                        else {}
                    )
            else:
                d[key.removesuffix("_json")] = [] if key.startswith(("tables", "columns")) else {}
        return d

    # ── token accounting ────────────────────────────────────────────────

    def total_turn_tokens(self, session_id: int) -> int:
        if not session_id:
            return 0
        with self._history._connect() as conn:
            row = conn.execute(
                "SELECT COALESCE(SUM(estimated_tokens), 0) AS t FROM chat_turns "
                "WHERE session_id = ? AND compacted_at IS NULL",
                (int(session_id),),
            ).fetchone()
        return int(row["t"] if row else 0)

    # ── compaction ──────────────────────────────────────────────────────

    def maybe_compact(
        self,
        session_id: int,
        *,
        model: str | None,
        llm_provider: Any | None,
        ratio: float = _COMPACTION_RATIO,
    ) -> dict[str, Any] | None:
        """If the session's live turns exceed ~``ratio * model_budget``,
        collapse the oldest slice into a single summary turn.

        Returns a dict describing what happened, or ``None`` if no
        compaction was needed.
        """
        if not session_id:
            return None
        threshold = max(1024, int(_input_budget_for(model) * float(ratio)))
        live_turns = self.recent_turns(session_id, include_summary=True, include_compacted=False)
        total = sum(int(t.get("estimated_tokens") or 0) for t in live_turns)
        if total <= threshold:
            return None

        target_tail = int(threshold * _KEEP_TAIL_RATIO)
        # Walk from the newest turn backwards; keep accumulating until we'd
        # blow the target_tail. The cut point is the oldest turn we'll keep.
        cumulative = 0
        cut_index = 0
        for turn in reversed(live_turns):
            cumulative += int(turn.get("estimated_tokens") or 0)
            if cumulative > target_tail:
                cut_index = int(turn.get("turn_index") or 0)
                break
        # Build the slice to summarise (turns with turn_index < cut_index).
        old_slice = [t for t in live_turns if int(t.get("turn_index") or 0) < cut_index]
        if not old_slice:
            return None

        through_turn_id = int(old_slice[-1]["id"])
        summary_text, summary_tables, summary_columns, summary_tokens = self._summarise_slice(
            old_slice, llm_provider=llm_provider, model=model
        )
        self._replace_turns_with_summary(
            session_id,
            through_turn_id=through_turn_id,
            summary_text=summary_text,
            summary_tables=summary_tables,
            summary_columns=summary_columns,
            summary_tokens=summary_tokens,
        )
        return {
            "compacted_through_turn_id": through_turn_id,
            "compacted_count": len(old_slice),
            "summary_tokens": summary_tokens,
            "kept_tokens": cumulative,
            "threshold": threshold,
            "summary_text": summary_text,
        }

    def _summarise_slice(
        self,
        old_slice: list[dict[str, Any]],
        *,
        llm_provider: Any | None,
        model: str | None,
    ) -> tuple[str, list[str], list[str], int]:
        """Summarise the old slice; falls back to a stub if no LLM is available."""
        merged_tables: list[str] = []
        merged_columns: list[str] = []
        for t in old_slice:
            for tname in t.get("tables") or []:
                if tname and tname not in merged_tables:
                    merged_tables.append(str(tname))
            for cname in t.get("columns") or []:
                if cname and cname not in merged_columns:
                    merged_columns.append(str(cname))

        if llm_provider is None:
            stub = f"(history truncated, {len(old_slice)} earlier turns dropped)"
            return stub, merged_tables, merged_columns, _estimate_turn_tokens(None, stub)

        compact_payload = [
            {
                "turn_index": t.get("turn_index"),
                "role": t.get("role"),
                "question": t.get("question") or "",
                "answer_summary": t.get("answer_summary") or "",
                "intent": t.get("intent") or "",
                "topic": t.get("topic") or "",
                "tables": t.get("tables") or [],
                "columns": t.get("columns") or [],
            }
            for t in old_slice
        ]
        system = (
            "You are summarising a metadata Q&A history. Produce one short paragraph "
            "covering what the user investigated, then three bullet lines listing: "
            "(a) tables referenced, (b) columns referenced, (c) intents pursued. "
            "Use plain text. No prose intro, no JSON, no greetings."
        )
        user_msg = json.dumps({"turns": compact_payload}, ensure_ascii=True)
        try:
            result = llm_provider.chat(
                [
                    {"role": "system", "content": system},
                    {"role": "user", "content": user_msg},
                ],
                temperature=0.0,
                max_tokens=_SUMMARY_MAX_TOKENS,
                use_logprobs=False,
            )
            text = (getattr(result, "content", "") or "").strip()
            if not text:
                raise RuntimeError("empty summary from LLM")
        except Exception as exc:
            log.warning("Compaction LLM call failed: %s; using stub.", exc)
            stub = f"(history truncated, {len(old_slice)} earlier turns dropped)"
            return stub, merged_tables, merged_columns, _estimate_turn_tokens(None, stub)
        return text, merged_tables, merged_columns, _estimate_turn_tokens(None, text)

    def _replace_turns_with_summary(
        self,
        session_id: int,
        *,
        through_turn_id: int,
        summary_text: str,
        summary_tables: list[str],
        summary_columns: list[str],
        summary_tokens: int,
    ) -> None:
        now = time.time()
        tables_json = json.dumps(list(summary_tables or []), ensure_ascii=True)
        columns_json = json.dumps(list(summary_columns or []), ensure_ascii=True)
        with self._history._lock, self._history._connect() as conn:
            # Concurrency guard: re-read compaction_state and abort if another
            # process already advanced past `through_turn_id`.
            row = conn.execute(
                "SELECT compaction_state_json FROM chat_sessions WHERE id = ?",
                (int(session_id),),
            ).fetchone()
            existing = {}
            if row and row["compaction_state_json"]:
                try:
                    existing = json.loads(row["compaction_state_json"]) or {}
                except Exception:
                    existing = {}
            already = int(existing.get("compacted_through_turn_id") or 0)
            if already >= int(through_turn_id):
                return
            # Soft-delete the slice. We do NOT drop the rows so the audit trail survives.
            conn.execute(
                "UPDATE chat_turns SET compacted_at = ? "
                "WHERE session_id = ? AND id <= ? AND compacted_at IS NULL",
                (now, int(session_id), int(through_turn_id)),
            )
            idx = self._next_turn_index(conn, session_id)
            conn.execute(
                """
                INSERT INTO chat_turns
                    (session_id, run_id, turn_index, role, question, answer_summary,
                     intent, topic, confidence, tables_json, columns_json,
                     plan_json, tokens_json, request_id, created_at, estimated_tokens)
                VALUES (?, NULL, ?, 'summary', NULL, ?, 'compaction', '', '',
                        ?, ?, NULL, NULL, NULL, ?, ?)
                """,
                (
                    int(session_id),
                    idx,
                    str(summary_text),
                    tables_json,
                    columns_json,
                    now,
                    int(summary_tokens),
                ),
            )
            conn.execute(
                "UPDATE chat_sessions SET compaction_state_json = ?, last_active_at = ? "
                "WHERE id = ?",
                (
                    json.dumps(
                        {
                            "compacted_through_turn_id": int(through_turn_id),
                            "summary_tokens": int(summary_tokens),
                            "compacted_at": now,
                        },
                        ensure_ascii=True,
                    ),
                    now,
                    int(session_id),
                ),
            )

    # ── helpers ─────────────────────────────────────────────────────────

    def reset_for_test(self) -> None:
        """Wipe both tables. Test-only."""
        with self._history._lock, self._history._connect() as conn:
            conn.execute("DELETE FROM chat_turns")
            conn.execute("DELETE FROM chat_sessions")
