"""Tests for the pages LLM composer."""

from __future__ import annotations

from typing import Any

from amx.pages.composer import compose
from amx.pages.types import PageContext


class StubLLM:
    def __init__(self) -> None:
        self.calls: list[list[dict[str, str]]] = []

    def chat(self, messages: list[dict[str, str]], **kw: Any) -> object:
        self.calls.append(messages)

        class R:
            content = "# Overview\n\nGenerated body."

        return R()


def test_compose_uses_system_and_user_messages() -> None:
    llm = StubLLM()
    ctx = PageContext(intent="explain orders", db_blocks=["DDL"])
    body, model = compose(ctx, llm=llm, model_name="claude-haiku-4-5")
    assert body.startswith("# Overview")
    assert model == "claude-haiku-4-5"
    msgs = llm.calls[0]
    assert msgs[0]["role"] == "system"
    assert "technical writer" in msgs[0]["content"]
    assert "explain orders" in msgs[1]["content"]
    assert "DDL" in msgs[1]["content"]


def test_compose_handles_missing_content() -> None:
    class EmptyLLM:
        def chat(self, messages: list[dict[str, str]], **kw: Any) -> object:
            class R:
                content = None

            return R()

    body, _ = compose(PageContext(intent="x"), llm=EmptyLLM(), model_name="m")
    assert body == ""
