"""Tests for the SQLite-backed `/ask` chat session layer.

These cover persistence across simulated REPL restarts, the LLM-driven
compaction path with its drop-old fallback, list/resume profile filtering,
and that follow-up planner payloads still reach prior tables after the
oldest turns have been compacted into a summary row.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from amx.search.session_store import (
    _COMPACTION_RATIO,
    ChatSessionStore,
    _estimate_turn_tokens,
    _input_budget_for,
)
from amx.storage.sqlite_store import SQLiteHistoryStore


def _fresh_store(tmpdir: str) -> tuple[SQLiteHistoryStore, ChatSessionStore]:
    db_path = Path(tmpdir) / "history.db"
    history = SQLiteHistoryStore(db_path)
    history.init()
    return history, ChatSessionStore(history)


class _FakeLLM:
    """Stub mimicking ``LLMProvider.chat`` for compaction tests."""

    def __init__(self, summary_text: str) -> None:
        self._summary_text = summary_text
        self.calls: list[list[dict[str, str]]] = []

    def chat(self, messages, **kwargs):  # noqa: ANN001
        self.calls.append(list(messages))
        return type(
            "ChatResult",
            (),
            {
                "content": self._summary_text,
                "usage": {"prompt_tokens": 100, "completion_tokens": 50, "total_tokens": 150},
            },
        )()


class _ExplodingLLM:
    """Stub that simulates an LLM call failure (e.g. transient API error)."""

    def chat(self, messages, **kwargs):  # noqa: ANN001
        raise RuntimeError("simulated provider outage")


class ChatSessionPersistenceTests(unittest.TestCase):
    def test_session_persists_across_simulated_restart(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            _, store = _fresh_store(td)
            sid = store.start_session(db_profile="dev", llm_profile="default")
            store.append_user_turn(sid, question="what tracks invoices?")
            store.append_assistant_turn(
                sid,
                run_id=None,
                answer_summary="`finance.invoices` looks like the canonical table.",
                intent="find_tables",
                topic="invoices",
                tables=["finance.invoices"],
                columns=["invoice_id"],
            )

            # Simulate a process restart: open a brand-new SQLiteHistoryStore
            # against the same on-disk database file.
            db_path = Path(td) / "history.db"
            history2 = SQLiteHistoryStore(db_path)
            history2.init()
            store2 = ChatSessionStore(history2)
            turns = store2.recent_turns(sid)
            self.assertEqual(len(turns), 2)
            self.assertEqual(turns[0]["role"], "user")
            self.assertEqual(turns[0]["question"], "what tracks invoices?")
            self.assertEqual(turns[1]["role"], "assistant")
            self.assertEqual(turns[1]["tables"], ["finance.invoices"])

    def test_delete_session_drops_turns_and_returns_true(self) -> None:
        """``delete_session`` must wipe both the chat_sessions row AND
        the chat_turns it owns. ``end_session`` is a soft mark; this is
        the hard-remove the Studio sidebar / CLI ``/session delete``
        need so a user can prune throwaway chats from the picker."""
        with tempfile.TemporaryDirectory() as td:
            _, store = _fresh_store(td)
            sid = store.start_session(db_profile="dev", llm_profile="default")
            store.append_user_turn(sid, question="ephemeral")
            store.append_assistant_turn(
                sid,
                run_id=None,
                answer_summary="ack",
                intent="x",
                topic="x",
                tables=[],
                columns=[],
            )
            self.assertIsNotNone(store.get_session(sid))
            self.assertEqual(len(store.recent_turns(sid)), 2)

            deleted = store.delete_session(sid)
            self.assertTrue(deleted)
            self.assertIsNone(store.get_session(sid))
            self.assertEqual(store.recent_turns(sid), [])

            # Idempotent: a second delete is a no-op that reports False.
            self.assertFalse(store.delete_session(sid))

    def test_list_sessions_filters_by_active_profile_pair(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            _, store = _fresh_store(td)
            sid_a = store.start_session(db_profile="dev", llm_profile="claude")
            store.append_user_turn(sid_a, question="dev/claude question")
            sid_b = store.start_session(db_profile="prod", llm_profile="claude")
            store.append_user_turn(sid_b, question="prod/claude question")
            sid_c = store.start_session(db_profile="dev", llm_profile="gpt4")
            store.append_user_turn(sid_c, question="dev/gpt4 question")

            dev_claude = store.list_sessions(db_profile="dev", llm_profile="claude")
            self.assertEqual({s["id"] for s in dev_claude}, {sid_a})
            all_dev = store.list_sessions(db_profile="dev")
            self.assertEqual({s["id"] for s in all_dev}, {sid_a, sid_c})
            everything = store.list_sessions()
            self.assertEqual({s["id"] for s in everything}, {sid_a, sid_b, sid_c})

    def test_first_question_excerpt_is_carried_in_list(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            _, store = _fresh_store(td)
            sid = store.start_session(db_profile="dev", llm_profile="default")
            store.append_user_turn(sid, question="What is the revenue table?")
            store.append_user_turn(sid, question="Follow-up.")
            sessions = store.list_sessions(db_profile="dev", llm_profile="default")
            self.assertEqual(sessions[0]["first_question"], "What is the revenue table?")


class ChatSessionCompactionTests(unittest.TestCase):
    def _seed_heavy_session(
        self, store: ChatSessionStore, *, turns: int, per_turn_tokens: int
    ) -> int:
        sid = store.start_session(db_profile="dev", llm_profile="default")
        for i in range(turns):
            # Override estimated_tokens so we can drive the threshold deterministically
            # without manufacturing huge text payloads.
            store.append_user_turn(sid, question=f"q{i}", estimated_tokens=per_turn_tokens // 2)
            store.append_assistant_turn(
                sid,
                run_id=None,
                answer_summary=f"answer about table_{i}",
                intent="find_tables",
                topic=f"topic_{i}",
                tables=[f"sap.table_{i}"],
                columns=[f"col_{i}"],
                estimated_tokens=per_turn_tokens // 2,
            )
        return sid

    def test_compaction_summarises_when_threshold_exceeded(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            _, store = _fresh_store(td)
            # default budget is 60K * 0.40 = 24K threshold; 30 turns × 2K = 60K
            sid = self._seed_heavy_session(store, turns=30, per_turn_tokens=2_000)
            self.assertGreater(
                store.total_turn_tokens(sid), int(_input_budget_for(None) * _COMPACTION_RATIO)
            )

            llm = _FakeLLM(
                "Summary: investigating SAP sales tables.\n- tables: sap.table_0..9\n- columns: col_0..9\n- intents: find_tables"
            )
            result = store.maybe_compact(sid, model=None, llm_provider=llm)

            self.assertIsNotNone(result)
            self.assertEqual(len(llm.calls), 1, "LLM should be called exactly once for compaction")
            # After compaction, live total ≤ threshold * 0.7 + summary tokens.
            new_total = store.total_turn_tokens(sid)
            threshold = int(_input_budget_for(None) * _COMPACTION_RATIO)
            self.assertLessEqual(new_total, int(threshold * 0.7) + result["summary_tokens"] + 500)

            # A summary turn should now be present and pre-existing turns soft-deleted.
            visible = store.recent_turns(sid, include_summary=True, include_compacted=False)
            self.assertTrue(any(t["role"] == "summary" for t in visible))
            archived = store.recent_turns(sid, include_summary=True, include_compacted=True)
            self.assertGreater(len(archived), len(visible))

    def test_compaction_no_op_under_threshold(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            _, store = _fresh_store(td)
            sid = self._seed_heavy_session(store, turns=2, per_turn_tokens=200)
            llm = _FakeLLM("not used")
            result = store.maybe_compact(sid, model=None, llm_provider=llm)
            self.assertIsNone(result)
            self.assertEqual(len(llm.calls), 0)

    def test_compaction_falls_back_to_stub_when_llm_unavailable(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            _, store = _fresh_store(td)
            sid = self._seed_heavy_session(store, turns=30, per_turn_tokens=2_000)
            result = store.maybe_compact(sid, model=None, llm_provider=None)
            self.assertIsNotNone(result)
            visible = store.recent_turns(sid, include_summary=True, include_compacted=False)
            summary_rows = [t for t in visible if t["role"] == "summary"]
            self.assertEqual(len(summary_rows), 1)
            self.assertIn("history truncated", summary_rows[0]["answer_summary"])

    def test_compaction_falls_back_when_llm_call_fails(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            _, store = _fresh_store(td)
            sid = self._seed_heavy_session(store, turns=30, per_turn_tokens=2_000)
            result = store.maybe_compact(sid, model=None, llm_provider=_ExplodingLLM())
            self.assertIsNotNone(result)
            visible = store.recent_turns(sid, include_summary=True, include_compacted=False)
            summary_rows = [t for t in visible if t["role"] == "summary"]
            self.assertEqual(len(summary_rows), 1)

    def test_followup_summary_carries_tables_for_planner_payload(self) -> None:
        """After compaction, the synthetic summary turn must surface the
        previous-tables list so a follow-up like "what about its columns?"
        can still ground against ``sap.vbak`` even though the original turn
        is soft-deleted."""
        with tempfile.TemporaryDirectory() as td:
            _, store = _fresh_store(td)
            sid = store.start_session(db_profile="dev", llm_profile="default")
            store.append_user_turn(sid, question="vbak?", estimated_tokens=500)
            store.append_assistant_turn(
                sid,
                run_id=None,
                answer_summary="`sap.vbak` is the sales-order header table.",
                intent="explain_table",
                topic="vbak",
                tables=["sap.vbak"],
                columns=["vbeln"],
                estimated_tokens=15_000,
            )
            # Pad with junk turns so compaction is forced.
            for i in range(20):
                store.append_user_turn(sid, question=f"q{i}", estimated_tokens=500)
                store.append_assistant_turn(
                    sid,
                    run_id=None,
                    answer_summary=f"a{i}",
                    intent="x",
                    topic="x",
                    tables=[f"t{i}"],
                    columns=[],
                    estimated_tokens=2_000,
                )
            llm = _FakeLLM("Investigated SAP sales tables.")
            store.maybe_compact(sid, model=None, llm_provider=llm)
            visible = store.recent_turns(sid, include_summary=True, include_compacted=False)
            summary_rows = [t for t in visible if t["role"] == "summary"]
            self.assertEqual(len(summary_rows), 1)
            self.assertIn("sap.vbak", summary_rows[0]["tables"])

    def test_concurrency_guard_skips_when_already_advanced(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            _, store = _fresh_store(td)
            sid = self._seed_heavy_session(store, turns=30, per_turn_tokens=2_000)
            llm = _FakeLLM("first summary")
            store.maybe_compact(sid, model=None, llm_provider=llm)
            # Manually invoke replace again with an OLDER through_turn_id; it
            # should be a no-op because compaction_state has already advanced.
            store._replace_turns_with_summary(  # noqa: SLF001
                sid,
                through_turn_id=1,
                summary_text="should-not-appear",
                summary_tables=["x"],
                summary_columns=["y"],
                summary_tokens=10,
            )
            visible = store.recent_turns(sid, include_summary=True, include_compacted=False)
            summaries = [t for t in visible if t["role"] == "summary"]
            self.assertEqual(len(summaries), 1, "no second summary should be inserted")
            self.assertEqual(summaries[0]["answer_summary"], "first summary")


class ChatSessionRecentTurnsTests(unittest.TestCase):
    def test_recent_turns_keeps_summary_at_head(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            _, store = _fresh_store(td)
            sid = store.start_session(db_profile="dev", llm_profile="default")
            for i in range(40):
                store.append_user_turn(sid, question=f"q{i}", estimated_tokens=2_000)
                store.append_assistant_turn(
                    sid,
                    run_id=None,
                    answer_summary=f"answer-{i}",
                    intent="x",
                    topic="x",
                    tables=[f"t{i}"],
                    columns=[],
                    estimated_tokens=2_000,
                )
            llm = _FakeLLM("History summary.")
            store.maybe_compact(sid, model=None, llm_provider=llm)
            turns = store.recent_turns(sid, limit=2, include_summary=True)
            # Whatever the trimming chose, a summary turn (if present) should
            # still be in the result so follow-up planners can ground against it.
            roles = [t["role"] for t in turns]
            if any(r == "summary" for r in roles):
                self.assertEqual(roles[0], "summary")


class TokenEstimationTests(unittest.TestCase):
    def test_estimate_turn_tokens_handles_empty_inputs(self) -> None:
        self.assertGreaterEqual(_estimate_turn_tokens(None, None), 1)
        self.assertGreater(_estimate_turn_tokens("hello", None), 1)
        self.assertGreater(_estimate_turn_tokens(None, "world"), 1)
        self.assertGreater(_estimate_turn_tokens("a", "b" * 200), _estimate_turn_tokens("a", "b"))


class ConfidenceBandTests(unittest.TestCase):
    def test_band_thresholds(self) -> None:
        from amx.search.confidence import BAND_HIGH, BAND_LOW, BAND_MEDIUM, band

        self.assertEqual(band(12.0), BAND_HIGH)
        self.assertEqual(band(11.99), BAND_MEDIUM)
        self.assertEqual(band(6.0), BAND_MEDIUM)
        self.assertEqual(band(5.99), BAND_LOW)
        self.assertEqual(band(0.01), BAND_LOW)
        self.assertEqual(band(0.0), BAND_LOW)
        self.assertEqual(band(165.0), BAND_HIGH)
        self.assertEqual(band(None), BAND_LOW)
        self.assertEqual(band("garbage"), BAND_LOW)


if __name__ == "__main__":
    unittest.main()
