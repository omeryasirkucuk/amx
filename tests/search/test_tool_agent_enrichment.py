"""Studio ASK enriches its synthesis context with lineage and pages.

The legacy CLI path (``SearchAgent.ask``) folds anchor-based lineage and
published-pages evidence into ``retrieval_details`` before the synthesis
prompt is built. Studio routes through ``run_tool_agent`` instead, and
prior to Task 7b the new evidence stopped at the HTTP boundary because
the tool loop never consumed the forwarded ``lineage_profiles`` /
``pages_enabled`` kwargs. This module locks in the fix: when the tool
loop resolves at least one catalog entity, the enricher fires with the
forwarded kwargs and its output reaches the LLM's working context as
an appendix on the messages list.

The tests stub the LLM provider and the ``ToolBox`` constructor so the
agent loop runs without any real catalog query, and they monkeypatch
``enrich_retrieval_details_with_lineage_and_pages`` so the assertion
target is the exact dict the production enricher would emit.
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

from amx.storage.sqlite_store import SQLiteHistoryStore


def _stub_llm_response(content: str, tool_calls: list[Any] | None = None) -> SimpleNamespace:
    """Mirror the ``LLMProvider.chat`` return shape the tool loop reads."""
    return SimpleNamespace(
        content=content,
        tool_calls=tool_calls or [],
        finish_reason="stop",
        usage={"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
        thinking_content="",
    )


class _FakeToolbox:
    """Stand-in for :class:`amx.search.agent_tools.ToolBox`.

    Returns a JSON payload from :meth:`invoke` that carries the
    ``db_profile`` / ``schema`` / ``table`` fields the anchor-id
    harvester reads — so the test exercises the real harvesting
    path rather than mocking it out.
    """

    @staticmethod
    def schemas() -> list[Any]:
        return []

    def available_schemas(self) -> list[Any]:
        return self.schemas()

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.db_profiles: list[str] = ["p1"]
        self.db_profile: str = "p1"

    def __enter__(self) -> "_FakeToolbox":
        return self

    def __exit__(self, *exc: Any) -> bool:
        return False

    def _live_db(self) -> MagicMock:
        return MagicMock(list_schemas=lambda: [])

    def invoke(self, name: str, args: str) -> str:
        # Truncated-JSON-safe payload: kept short enough to survive
        # the 280-char ``result_preview`` truncation in
        # :func:`_summarise_tool_call`.
        return json.dumps(
            {
                "matches": [
                    {
                        "db_profile": "p1",
                        "schema": "s",
                        "table": "customers",
                    }
                ],
                "source": "catalog",
            }
        )


def _build_cfg() -> MagicMock:
    return MagicMock(
        db=SimpleNamespace(catalog="", backend="postgresql", database="", project=""),
        llm=SimpleNamespace(language="english", model="x"),
        active_db_profile="p1",
        active_llm_profile="default",
        current_schema=None,
        current_table=None,
        db_profiles={},
    )


def _seed_anchor_entity(store: SQLiteHistoryStore) -> None:
    """Insert one ``catalog_entities`` row the resolver can find."""
    with store._connect() as conn:  # noqa: SLF001
        conn.execute(
            """
            INSERT INTO catalog_entities
                (id, db_profile, db_backend, database_name, schema_name,
                 table_name, entity_kind, asset_kind)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (42, "p1", "postgresql", "db", "s", "customers", "table", "table"),
        )


def test_run_tool_agent_invokes_enrichment_with_forwarded_kwargs(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The enricher fires once per turn, sees the forwarded
    ``lineage_profiles`` / ``pages_enabled`` overrides, and the
    resolved anchor id list mirrors the catalog rows the tool loop
    surfaced. The synthesis messages list then contains the
    formatted appendix so the LLM can ground its answer."""
    import amx.search.tool_agent as ta

    monkeypatch.setattr(ta, "ToolBox", _FakeToolbox)

    store = SQLiteHistoryStore(tmp_path / "history.db")
    store.init()
    _seed_anchor_entity(store)
    monkeypatch.setattr(
        "amx.storage.sqlite_store.history_store", lambda: store
    )

    captured: dict[str, Any] = {}

    def _fake_enrich(
        *,
        store: Any,
        rows: list[dict[str, Any]],
        retrieval_details: dict[str, Any],
        question: str,
        plan: Any,
        lineage_profiles: list[str] | None,
        pages_enabled: bool | None,
    ) -> dict[str, Any]:
        captured["entity_ids"] = [r["id"] for r in rows]
        captured["question"] = question
        captured["lineage_profiles"] = lineage_profiles
        captured["pages_enabled"] = pages_enabled
        retrieval_details.setdefault("evidence_sources", [])
        retrieval_details["evidence_sources"].extend(["lineage", "pages"])
        retrieval_details["lineage"] = {
            "kind": "lineage",
            "artifact_names": ["customers-canvas"],
            "upstream_entity_ids": [20],
            "downstream_entity_ids": [30],
            "external_systems": [],
            "comments": [],
        }
        retrieval_details["pages"] = {
            "kind": "pages",
            "items": [
                {
                    "title": "Customers notes",
                    "slug": "customers",
                    "excerpt": "Daily refresh of the customers table.",
                }
            ],
        }
        return retrieval_details

    monkeypatch.setattr(
        "amx.search._agent.retrieval.enrich_retrieval_details_with_lineage_and_pages",
        _fake_enrich,
    )

    # Two LLM rounds:
    #   1. emit one tool_call → tool loop invokes the fake catalog
    #      tool, harvests refs, and injects the appendix into the
    #      messages list.
    #   2. no tool_calls → loop breaks and the second-round messages
    #      list is the one we assert on (it already contains the
    #      appendix from round 1).
    tool_call = SimpleNamespace(id="c1", name="search_tables_by_concept", arguments="{}")
    chat_calls: list[list[dict[str, Any]]] = []
    responses = [
        _stub_llm_response("", tool_calls=[tool_call]),
        _stub_llm_response("final answer"),
    ]

    fake_llm = MagicMock()

    def _fake_chat(messages: list[dict[str, Any]], **kwargs: Any) -> SimpleNamespace:
        # Capture a deep-ish copy of each round's messages so we
        # can verify the appendix is in scope by the time the
        # synthesis round runs.
        chat_calls.append([dict(m) for m in messages])
        return responses[len(chat_calls) - 1]

    fake_llm.chat.side_effect = _fake_chat

    result = ta.run_tool_agent(
        cfg=_build_cfg(),
        catalog=MagicMock(),
        llm=fake_llm,
        question="what is the customers table?",
        answer_language="english",
        session_memory=None,
        lineage_profiles=["my-canvas"],
        pages_enabled=False,
    )

    # 1. The enricher was called exactly once and saw the forwarded
    #    overrides — Studio's lineage_profiles + pages_enabled flow
    #    end-to-end through the tool agent.
    assert captured["lineage_profiles"] == ["my-canvas"]
    assert captured["pages_enabled"] is False
    # 2. The anchor id resolver pulled the catalog_entities row id
    #    for the (profile, schema, table) the fake tool returned.
    assert captured["entity_ids"] == [42]
    assert captured["question"] == "what is the customers table?"

    # 3. The appendix actually reaches the synthesis context — the
    #    second LLM round's messages list contains a user-role
    #    system note with both the lineage and pages blocks.
    assert len(chat_calls) == 2
    second_round_text = "\n".join(
        str(m.get("content") or "") for m in chat_calls[1]
    )
    assert "Lineage evidence" in second_round_text
    assert "customers-canvas" in second_round_text
    assert "Documentation pages anchored" in second_round_text
    assert "Customers notes" in second_round_text

    # 4. The agent still returns the final answer the LLM produced
    #    on the synthesis round.
    assert result.answer == "final answer"


def test_run_tool_agent_skips_enrichment_when_no_catalog_refs(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """When the tool results never surface a ``(profile, schema,
    table)`` triple, the enricher is not called — keeps the loop
    cheap on chitchat / no-evidence questions."""
    import amx.search.tool_agent as ta

    class _EmptyToolbox(_FakeToolbox):
        def invoke(self, name: str, args: str) -> str:
            return json.dumps({"matches": [], "source": "catalog"})

    monkeypatch.setattr(ta, "ToolBox", _EmptyToolbox)

    store = SQLiteHistoryStore(tmp_path / "history.db")
    store.init()
    monkeypatch.setattr(
        "amx.storage.sqlite_store.history_store", lambda: store
    )

    called = {"count": 0}

    def _fake_enrich(**kwargs: Any) -> dict[str, Any]:
        called["count"] += 1
        return kwargs.get("retrieval_details") or {}

    monkeypatch.setattr(
        "amx.search._agent.retrieval.enrich_retrieval_details_with_lineage_and_pages",
        _fake_enrich,
    )

    tool_call = SimpleNamespace(id="c1", name="search_tables_by_concept", arguments="{}")
    fake_llm = MagicMock()
    fake_llm.chat.side_effect = [
        _stub_llm_response("", tool_calls=[tool_call]),
        _stub_llm_response("done"),
    ]

    result = ta.run_tool_agent(
        cfg=_build_cfg(),
        catalog=MagicMock(),
        llm=fake_llm,
        question="hi",
        answer_language="english",
        session_memory=None,
        lineage_profiles=None,
        pages_enabled=None,
    )

    assert result.answer == "done"
    # The enricher must not have been called — no anchors means no
    # lineage / pages retrieval. The tool agent stays as cheap as it
    # was before the enrichment hook.
    assert called["count"] == 0


def test_run_tool_agent_swallows_enricher_exceptions(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A failure inside the enricher must never break the agent
    loop — Studio answers always come back, evidence or no
    evidence."""
    import amx.search.tool_agent as ta

    monkeypatch.setattr(ta, "ToolBox", _FakeToolbox)

    store = SQLiteHistoryStore(tmp_path / "history.db")
    store.init()
    _seed_anchor_entity(store)
    monkeypatch.setattr(
        "amx.storage.sqlite_store.history_store", lambda: store
    )

    def _boom(**kwargs: Any) -> None:
        raise RuntimeError("simulated enrichment failure")

    monkeypatch.setattr(
        "amx.search._agent.retrieval.enrich_retrieval_details_with_lineage_and_pages",
        _boom,
    )

    tool_call = SimpleNamespace(id="c1", name="search_tables_by_concept", arguments="{}")
    fake_llm = MagicMock()
    fake_llm.chat.side_effect = [
        _stub_llm_response("", tool_calls=[tool_call]),
        _stub_llm_response("survived"),
    ]

    result = ta.run_tool_agent(
        cfg=_build_cfg(),
        catalog=MagicMock(),
        llm=fake_llm,
        question="hi",
        answer_language="english",
        session_memory=None,
        lineage_profiles=["x"],
        pages_enabled=True,
    )

    assert result.answer == "survived"
