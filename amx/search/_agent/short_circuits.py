"""Short-circuit answer handlers for ``SearchAgent``.

These methods recognise question patterns where the full
plan/retrieve/synthesize loop is overkill, and produce a direct
answer instead:

* ``_handle_chitchat`` — non-database conversational filler.
* ``_handle_meta_query`` — questions about AMX itself ("what can you
  do?", "list connectors").
* ``_handle_followup_reaffirmation`` — short follow-up turns where the
  prior table scope is reused.
* ``_answer_via_tool_agent`` — falls through to the tool-calling agent
  when retrieval would be too clumsy.
* ``_should_remember_table_scope`` + ``_record_short_circuit_assistant``
  — bookkeeping helpers for memory recording.
"""

from __future__ import annotations

import re
import threading
import time
from typing import Any

from amx.agents.orchestrator import RunCancelled
from amx.search._agent._types import SearchPlan
from amx.search.catalog import SearchAnswer
from amx.utils.console import step_spinner
from amx.utils.logging import get_logger

log = get_logger("search.agent.short_circuits")


class ShortCircuitsMixin:
    """Short-circuit answer handlers for ``SearchAgent``."""

    def _handle_chitchat(self, question: str, question_language: str) -> SearchAnswer | None:
        """Recognise greetings / "how are you" / thanks and reply directly.

        Without this short-circuit the LLM planner sometimes flags the input
        as ``needs_clarification=True``, yielding "Could you clarify the
        exact scope (database/schema/table)?" — a confusing reply when the
        user just typed "hi".

        Detection is delegated to the pure
        :func:`amx.search.pipeline.short_circuits.is_chitchat`; the
        mixin keeps ownership of the side-effect glue (session-store
        recording, ``SearchAnswer`` construction).
        """
        from amx.search.pipeline.short_circuits import chitchat_summary, is_chitchat

        if not is_chitchat(question, self._CHITCHAT_TOKENS):
            return None
        summary = chitchat_summary()
        self._record_short_circuit_assistant(summary=summary, intent="chitchat")
        return SearchAnswer(
            intent="chitchat",
            question=question,
            rows=[],
            confidence="high",
            summary=summary,
            provenance=["client_side_short_circuit"],
            details={
                "reason": "chitchat_short_circuit",
                "answer_language": question_language or "english",
                "answer_shape": "single_fact",
                "stage_metrics": [],
            },
        )

    def _record_short_circuit_assistant(self, *, summary: str, intent: str) -> None:
        """Persist a synthetic assistant turn for chitchat / meta / reaffirm.

        Without this, ``ask()`` writes the user-side row at the top of the
        call but no matching assistant row gets written for the deterministic
        short-circuits — leaving the session memory unbalanced and confusing
        the next planner pass. We record a small assistant turn carrying just
        the answer text + the short-circuit kind so memory stays paired.
        """
        store = self._ensure_session_store()
        sid = self.cfg.active_chat_session_id
        if store is None or not sid:
            return
        try:
            store.append_assistant_turn(
                int(sid),
                run_id=None,
                answer_summary=str(summary or "")[:480],
                intent=intent,
                plan={"agent": "short_circuit", "kind": intent},
                confidence="high",
            )
        except Exception as exc:
            log.warning("Failed to record %s assistant turn: %s", intent, exc)

    def _handle_meta_query(self, question: str, question_language: str) -> SearchAnswer | None:
        """Answer questions ABOUT the conversation itself (no LLM call).

        Patterns: "what was my previous question?", "what did I ask?".
        Resolves against ``ChatSessionStore.recent_turns`` so the user gets
        the literal prior question text rather than a clarification prompt.

        Detection delegated to the pure
        :func:`amx.search.pipeline.short_circuits.is_meta_query`;
        reply composition uses
        :func:`amx.search.pipeline.short_circuits.meta_query_summary`.
        The mixin retains the side-effect glue (session-store lookup,
        assistant-turn recording, ``SearchAnswer`` construction).
        """
        from amx.search.pipeline.short_circuits import (
            is_meta_query,
            meta_query_summary,
        )

        if not is_meta_query(question):
            return None
        prior_question = ""
        store = self._ensure_session_store()
        sid = self.cfg.active_chat_session_id
        if store is not None and sid:
            try:
                turns = store.recent_turns(int(sid), include_summary=False, limit=8)
            except Exception:
                turns = []
            user_turns = [t for t in turns if str(t.get("role") or "") == "user"]
            # The latest user turn IS this very question (just appended);
            # we want the one BEFORE it.
            if len(user_turns) >= 2:
                prior_question = str(user_turns[-2].get("question") or "").strip()
        summary = meta_query_summary(prior_question)
        self._record_short_circuit_assistant(summary=summary, intent="meta_query")
        return SearchAnswer(
            intent="meta_query",
            question=question,
            rows=[],
            confidence="high",
            summary=summary,
            provenance=["chat_session_store"],
            details={
                "reason": "meta_query_short_circuit",
                "answer_language": question_language or "english",
                "answer_shape": "single_fact",
                "prior_question": prior_question,
                "stage_metrics": [],
            },
        )

    def _handle_followup_reaffirmation(
        self, question: str, question_language: str
    ) -> SearchAnswer | None:
        """Restate the prior assistant turn when the user pushes back briefly.

        The user types "Are you sure?" / "really?" — these are too short for
        the planner to map to anything meaningful and we don't want to fall
        through to "Could you clarify the exact scope?". Pull the last
        assistant turn out of the session store and re-confirm it verbatim.

        Detection + summary composition delegated to pure helpers in
        :mod:`amx.search.pipeline.short_circuits`; the mixin retains
        ownership of the session-store lookup and ``SearchAnswer``
        construction.
        """
        from amx.search.pipeline.short_circuits import (
            is_followup_reaffirmation,
            reaffirmation_summary,
        )

        if not is_followup_reaffirmation(question):
            return None
        store = self._ensure_session_store()
        sid = self.cfg.active_chat_session_id
        if store is None or not sid:
            return None
        try:
            turns = store.recent_turns(int(sid), include_summary=False, limit=8)
        except Exception:
            return None
        # Find the most recent assistant turn (the one we want to confirm).
        prior_assistant = ""
        for turn in reversed(turns):
            if str(turn.get("role") or "") == "assistant":
                prior_assistant = str(
                    turn.get("answer_summary") or turn.get("answer") or ""
                ).strip()
                if prior_assistant:
                    break
        if not prior_assistant:
            return None
        summary = reaffirmation_summary(prior_assistant)
        self._record_short_circuit_assistant(summary=summary, intent="reaffirmation")
        return SearchAnswer(
            intent="reaffirmation",
            question=question,
            rows=[],
            confidence="high",
            summary=summary,
            provenance=["chat_session_store", "reaffirm_short_circuit"],
            details={
                "reason": "followup_reaffirmation",
                "answer_language": question_language or "english",
                "answer_shape": "prose",
                "prior_assistant": prior_assistant,
                "stage_metrics": [],
            },
        )

    def _answer_via_tool_agent(
        self,
        *,
        question: str,
        clean_question: str,
        question_language: str,
        cancel_token: threading.Event | None = None,
    ) -> SearchAnswer | None:
        """Run the tool-calling loop and return a SearchAnswer.

        Returns ``None`` on any unexpected failure so the caller can fall
        back to the legacy LLM-Pass-1 path. The legacy path stays in place
        as a deliberate safety net during this rollout.
        """
        try:
            # Lazy import keeps a circular path between agent.py / tool_agent.py
            # impossible — tool_agent imports from agent_tools and catalog only.
            from amx.search.tool_agent import run_tool_agent
        except Exception as exc:
            log.warning("tool_agent unavailable, falling back to legacy router: %s", exc)
            return None
        # Convert the existing memory summary into the {role, content} pairs
        # the tool agent expects for context. ``_memory_summary`` returns
        # the most recent turns in chronological order; we keep both user
        # questions and assistant answer summaries so follow-ups resolve.
        # IMPORTANT: ``ask()`` already wrote the *current* user question to
        # the session store at the top of the call, so the latest entry in
        # ``_memory_summary()`` IS the question we're about to ask the LLM.
        # If we forward it here, ``run_tool_agent`` would then append it a
        # second time as the live user message — duplication confuses the
        # model ("Only those?" became unrecognisable). Drop the trailing
        # entry whose ``question`` matches the current one and which has no
        # paired assistant answer yet.
        memory_turns = list(self._memory_summary())
        if memory_turns:
            tail = memory_turns[-1]
            tail_q = str(tail.get("question") or "").strip()
            tail_ans = str(tail.get("answer_summary") or "").strip()
            if tail_q == clean_question and not tail_ans:
                memory_turns = memory_turns[:-1]
        prior_turns: list[dict[str, str]] = []
        for turn in memory_turns:
            user_q = str(turn.get("question") or "").strip()
            if user_q:
                prior_turns.append({"role": "user", "content": user_q})
            assistant_summary = str(turn.get("answer_summary") or "").strip()
            if assistant_summary:
                prior_turns.append({"role": "assistant", "content": assistant_summary})
        try:
            from amx.utils.live_display import get_display

            display = get_display()
            t0 = time.monotonic()
            # ``step_spinner`` opens the thinking panel; the tool agent then
            # streams the model's reasoning text into it via ``display`` so
            # the user sees real thinking content rather than a blank
            # spinner. The panel clears the moment the loop returns.
            with step_spinner("Search Agent: thinking with tools"):
                # Multi-profile scope (0.13+): SearchAgent collects the
                # active profile list at construction time
                # (cfg.effective_db_profiles or the explicit kwarg), and
                # forwards it through to the tool agent so catalog
                # search/find tools span every profile in scope.
                result = run_tool_agent(
                    cfg=self.cfg,
                    catalog=self.catalog,
                    llm=self._llm_provider(),
                    question=clean_question,
                    answer_language=question_language,
                    session_memory=prior_turns,
                    display=display if display.is_active else None,
                    db_profiles=list(self.db_profiles) if self.db_profiles else None,
                    cancel_token=cancel_token,
                )
            elapsed = round(time.monotonic() - t0, 4)
        except RunCancelled:
            # User pressed Ctrl-C; let the cancellation propagate so
            # SearchAgent.ask can render the "Cancelled by user"
            # answer instead of bailing into the legacy router (which
            # would try to re-ask the LLM and ignore the user's
            # interrupt).
            raise
        except Exception as exc:
            log.warning("Tool agent failed (%s); falling back to legacy router.", exc)
            return None

        # Persist the assistant turn so follow-up turns can read the recap.
        sid = self.cfg.active_chat_session_id
        store = self._ensure_session_store()
        if store is not None and sid:
            try:
                store.append_assistant_turn(
                    int(sid),
                    run_id=None,
                    answer_summary=result.answer[:480],
                    intent="tool_agent",
                    plan={"agent": "tool_agent", "iterations": result.iterations},
                    tokens=result.usage,
                    confidence="high",
                )
            except Exception as exc:
                log.warning("Failed to record tool-agent assistant turn: %s", exc)

        return SearchAnswer(
            intent="tool_agent",
            question=question,
            rows=[],
            confidence="high",
            summary=result.answer,
            provenance=["tool_calling_agent"],
            details={
                "answer_shape": "prose",
                "answer_language": question_language or "english",
                "agent": "tool_calling",
                "iterations": result.iterations,
                "tool_calls": result.tool_calls,
                "tokens": result.usage,
                "stage_metrics": [{"stage": "tool_agent", "duration_sec": elapsed}],
                "evidence_sources": [
                    f"tool:{call.get('name', '')}" for call in result.tool_calls if call.get("name")
                ],
            },
        )

    def _should_remember_table_scope(
        self, plan: SearchPlan, retrieval_details: dict[str, Any], question: str
    ) -> bool:
        if retrieval_details.get("resolved_tables"):
            return True
        if plan.search_mode in {"table_explain", "join_candidates", "joinable_tables"}:
            return True
        if self._explicit_table_paths_for_question(question):
            return True
        row_tables = {
            f"{row.get('schema_name')}.{row.get('table_name')}"
            for row in retrieval_details.get("visible_rows", [])
            if row.get("schema_name") and row.get("table_name")
        }
        return len(row_tables) == 1 and bool(row_tables)


__all__ = ["ShortCircuitsMixin"]
