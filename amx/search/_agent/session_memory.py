"""Session / memory / connection-state helpers for ``SearchAgent``.

Lightweight accessors that wrap the LLM provider, the chat-session
store, and the catalog readiness check. These methods are tiny but
shared by every other mixin (``self._llm_provider()``,
``self._memory_turns()``, ``self._catalog_ready()``); pulling them out
makes the dependency direction explicit and prevents the larger mixins
from accidentally drifting their definition.
"""

from __future__ import annotations

import contextlib
from typing import Any

from amx.llm.provider import LLMProvider
from amx.search.session_store import ChatSessionStore
from amx.storage.sqlite_store import history_store
from amx.utils.logging import get_logger

log = get_logger("search.agent.session_memory")


class SessionMemoryMixin:
    """Session / memory / connection-state methods for ``SearchAgent``."""

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

    def _ensure_session_store(self) -> ChatSessionStore | None:
        if self._session_store is not None:
            return self._session_store
        store = history_store()
        if store is None:
            return None
        self._session_store = ChatSessionStore(store)
        return self._session_store

    def _ensure_session_id(self) -> int | None:
        """Resolve the active chat session id.

        Each REPL boot starts fresh: ``cfg.active_chat_session_id`` is None
        until a `/ask` runs (or the user explicitly `/session resume`-d).
        We lazily call ``start_session`` so users who never run `/ask` don't
        accumulate empty session rows.
        """
        store = self._ensure_session_store()
        if store is None:
            return None
        existing = getattr(self.cfg, "active_chat_session_id", None)
        if existing:
            self._session_id = int(existing)
            return self._session_id
        if self._session_id is not None:
            return self._session_id
        # Persist the multi-profile scope on the new session record so
        # the Studio "/api/ask/sessions/{id}" GET (and the CLI's own
        # follow-up turns) honour the same sticky scope without
        # rebuilding it from cfg each time. ``self.db_profiles`` is the
        # SearchAgent-collected scope (caller kwarg > config default).
        scope_profiles: list[str] | None = None
        if getattr(self, "db_profiles", None):
            scope_profiles = list(self.db_profiles)
        sid = store.start_session(
            db_profile=self.db_profile,
            llm_profile=self._llm_profile,
            scope_profiles=scope_profiles,
        )
        self._session_id = sid
        with contextlib.suppress(Exception):
            self.cfg.active_chat_session_id = sid
        # Mirror to env so subsequent ``main_command.main()`` invocations from
        # the interactive REPL re-pick the same session via ``AMXConfig.load``.
        # Without this, each ``/ask <q>`` line creates a brand-new session and
        # follow-up questions lose all prior context.
        try:
            import os as _os

            _os.environ["AMX_CHAT_SESSION_ID"] = str(int(sid))
        except Exception:
            pass
        return sid

    def _memory(self) -> list[dict[str, Any]]:
        store = self._ensure_session_store()
        sid = getattr(self.cfg, "active_chat_session_id", None) or self._session_id
        if store is None or not sid:
            return list(self._fallback_memory)
        turns = store.recent_turns(int(sid), limit=self._memory_turns(), include_summary=True)
        # Project to the legacy turn-shape used by callers
        # (_last_tables, _memory_summary, planner payloads).
        out: list[dict[str, Any]] = []
        for t in turns:
            role = str(t.get("role") or "")
            if role == "summary":
                out.append(
                    {
                        "question": "",
                        "intent": "compaction",
                        "topic": "previous_context_summary",
                        "tables": list(t.get("tables") or []),
                        "columns": list(t.get("columns") or []),
                        "answer_summary": str(t.get("answer_summary") or ""),
                    }
                )
                continue
            if role == "user":
                # Pair the user turn with the next assistant turn; we'll fill
                # answer_summary from there in a second pass below.
                out.append(
                    {
                        "question": str(t.get("question") or ""),
                        "intent": "",
                        "topic": "",
                        "tables": [],
                        "columns": [],
                        "answer_summary": "",
                    }
                )
                continue
            # assistant
            plan = t.get("plan") or {}
            payload = {
                "question": "",
                "intent": str(t.get("intent") or ""),
                "topic": str(t.get("topic") or plan.get("normalized_question") or ""),
                "tables": list(t.get("tables") or []),
                "columns": list(t.get("columns") or []),
                "answer_summary": str(t.get("answer_summary") or ""),
            }
            # Backfill question onto the most recent user-only entry if any.
            if out and out[-1].get("question") and not out[-1].get("intent"):
                out[-1]["intent"] = payload["intent"]
                out[-1]["topic"] = payload["topic"]
                out[-1]["tables"] = payload["tables"]
                out[-1]["columns"] = payload["columns"]
                out[-1]["answer_summary"] = payload["answer_summary"]
            else:
                out.append(payload)
        return out

    def _remember(self, turn: dict[str, Any]) -> None:
        """Persist an assistant turn (back-compat shape).

        ``turn`` carries: question, intent, topic, tables, columns, and
        optionally answer_summary, confidence, plan, tokens, request_id,
        run_id. The user-side row was already inserted at the top of
        ``ask()`` via ``append_user_turn``; this writes the matching
        assistant row.
        """
        store = self._ensure_session_store()
        sid = self._ensure_session_id()
        if store is None or not sid:
            self._fallback_memory.append(dict(turn))
            max_turns = self._memory_turns()
            if max_turns > 0:
                self._fallback_memory = self._fallback_memory[-max_turns:]
            return
        store.append_assistant_turn(
            int(sid),
            run_id=turn.get("run_id"),
            answer_summary=str(turn.get("answer_summary") or "")[:1000],
            intent=str(turn.get("intent") or ""),
            topic=str(turn.get("topic") or ""),
            confidence=str(turn.get("confidence") or ""),
            tables=list(turn.get("tables") or []),
            columns=list(turn.get("columns") or []),
            plan=turn.get("plan"),
            tokens=turn.get("tokens"),
            request_id=turn.get("request_id"),
        )

    def _memory_summary(self) -> list[dict[str, Any]]:
        summary: list[dict[str, Any]] = []
        for turn in self._memory():
            # 200 chars used to be enough for the JSON planner payload, but
            # the tool agent feeds these straight into a chat history — long
            # answers (e.g. "12 tables have boolean columns: ...") were being
            # cut off and the LLM failed to resolve "Only those?" follow-ups.
            # 1000 chars is comfortably under the 24K-input budget even with
            # 6+ pairs in scope.
            summary.append(
                {
                    "question": turn.get("question", ""),
                    "intent": turn.get("intent", ""),
                    "topic": turn.get("topic", ""),
                    "tables": turn.get("tables", []),
                    "columns": turn.get("columns", []),
                    "answer_summary": str(turn.get("answer_summary") or "")[:1000],
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


__all__ = ["SessionMemoryMixin"]
