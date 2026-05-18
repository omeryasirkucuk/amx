"""Builds the LLM prompt for a documentation page and parses the response.

The model identity is read from the active AMX LLM profile by the
caller; this module is pure - same input, same prompt, same output -
so it is easy to test.
"""

from __future__ import annotations

from typing import Any, Protocol

from amx.pages.types import PageContext

SYSTEM_PROMPT = """You are a senior technical writer. Produce a Markdown documentation
page from the assets and sources below. Use these sections:
  1. Overview
  2. Data Assets
  3. Business Logic
  4. Pipelines & Lineage
  5. Open Questions
Keep each section short and concrete. Cite asset names inline when
referencing them. Do not invent fields or relationships that are
not in the provided context."""


class LLMClient(Protocol):
    def chat(self, messages: list[dict[str, str]], **kw: Any) -> object: ...


def compose(ctx: PageContext, *, llm: LLMClient, model_name: str) -> tuple[str, str]:
    user = f"INTENT: {ctx.intent}\n\nCONTEXT:\n{ctx.serialise()}"
    result = llm.chat(
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user},
        ]
    )
    body = getattr(result, "content", "") or ""
    return str(body), model_name
