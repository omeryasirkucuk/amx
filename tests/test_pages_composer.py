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


def test_system_prompt_is_intent_shaped() -> None:
    """The prompt must not force a fixed section template; it must
    delegate section choice to the LLM based on the user's intent, and
    must instruct lineage images to be preserved verbatim."""
    from amx.pages.composer import SYSTEM_PROMPT

    lower = SYSTEM_PROMPT.lower()
    assert "intent" in lower
    assert "lineage" in lower
    assert "image" in lower
    # The old rigid 5-section list must be gone.
    assert "1. overview" not in lower
    assert "2. data assets" not in lower
    assert "3. business logic" not in lower
    # Forced placeholder sections must be discouraged.
    collapsed = " ".join(lower.split())
    assert "do not append generic placeholder sections" in collapsed
    assert "open questions" in collapsed  # cited as an example to avoid


def test_compose_handles_missing_content() -> None:
    class EmptyLLM:
        def chat(self, messages: list[dict[str, str]], **kw: Any) -> object:
            class R:
                content = None

            return R()

    body, _ = compose(PageContext(intent="x"), llm=EmptyLLM(), model_name="m")
    assert body == ""


def test_compose_records_usage_into_token_tracker() -> None:
    """Every pages composition consumes provider tokens; the global
    token tracker must learn about the call so ``/usage`` and Studio's
    total-cost banner reflect page generation."""
    from amx.utils.token_tracker import tracker

    class _Cfg:
        provider = "anthropic"
        model = "claude-haiku-4-5"

    class _UsageLLM:
        cfg = _Cfg()

        def chat(self, messages: list[dict[str, str]], **kw: Any) -> object:
            # The token tracker accepts either a dict OR an object with
            # both ``prompt_tokens`` attrs and a ``.get`` method (which
            # is what real LiteLLM/Anthropic Usage objects expose). A
            # dict is the simplest stand-in for the test.
            class R:
                content = "# body"
                usage = {
                    "prompt_tokens": 1234,
                    "completion_tokens": 567,
                    "total_tokens": 1801,
                }

            return R()

    tracker.reset()
    try:
        compose(PageContext(intent="explain"), llm=_UsageLLM(), model_name="m")
        records = tracker.records()
        assert len(records) == 1
        rec = records[0]
        assert rec["step"] == "pages_compose"
        assert rec["prompt_tokens"] == 1234
        assert rec["completion_tokens"] == 567
        assert rec["total_tokens"] == 1801
        assert rec["provider"] == "anthropic"
        assert rec["model"] == "claude-haiku-4-5"
    finally:
        tracker.reset()
