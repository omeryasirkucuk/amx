"""Builds the LLM prompt for a documentation page and parses the response.

The model identity is read from the active AMX LLM profile by the
caller; this module is pure - same input, same prompt, same output -
so it is easy to test.
"""

from __future__ import annotations

import re
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
not in the provided context.

OUTPUT FORMAT: Reply with raw Markdown only. Do NOT wrap the answer
in a fenced code block (no ```markdown ... ``` and no ``` ... ```).
Start directly with the first heading."""


_FENCE_RE = re.compile(
    r"^\s*```(?:markdown|md)?\s*\n(.*?)\n```\s*$",
    re.DOTALL | re.IGNORECASE,
)


def strip_outer_markdown_fence(body: str) -> str:
    """Strip a leading ``` / ```markdown fence if the LLM wrapped the
    whole response in one. Some models (including reasoning variants of
    Kimi and Claude) ignore the "raw markdown only" prompt directive
    and emit ``` ```markdown ... ``` `` around the entire body, which
    causes downstream Markdown renderers to display the response as a
    single inert code block."""
    if not body:
        return body
    match = _FENCE_RE.match(body.strip())
    if match:
        return match.group(1)
    return body


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
    return strip_outer_markdown_fence(str(body)), model_name
