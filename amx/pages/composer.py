"""Builds the LLM prompt for a documentation page and parses the response.

The model identity is read from the active AMX LLM profile by the
caller; this module is pure - same input, same prompt, same output -
so it is easy to test.
"""

from __future__ import annotations

import re
from typing import Any, Protocol

from amx.pages.types import PageContext

SYSTEM_PROMPT = """You are a senior technical writer. Produce a Markdown
documentation page that fulfils the user's INTENT, using ONLY facts
from the provided CONTEXT.

Choose the section structure that best serves the intent. Examples:
  * single-table page: Purpose / Schema / Values / Lineage
  * single-column page: Origin / Transformations / Downstream usage
  * DB profile overview: Domain / Table groups / Key relationships
  * project overview: Systems / Domains / Entities / Data Flows
  * lineage narrative: Sources / Pipeline / Targets / Operational notes
Do not impose a fixed template. Do not append generic placeholder
sections (e.g. "Open Questions", "TBD", "Notes") unless the intent
specifically asks for them.

Do not invent fields, relationships, owners, or numbers that are not
in the CONTEXT. Cite asset names inline using backticks.

When the CONTEXT contains lineage artifacts, embed each referenced
image using the markdown image link exactly as provided — do not
rewrite the path, do not omit the image.

OUTPUT FORMAT: raw Markdown only. Do NOT wrap the answer in a fenced
code block (no ```markdown ... ``` and no ``` ... ```). Start
directly with the first heading."""


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
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user},
    ]
    result = llm.chat(messages=messages)
    body = getattr(result, "content", "") or ""

    # Record into the global token tracker so /usage, Studio's total
    # cost banner, and per-run cost breakdowns include page generation.
    # Failures here must not break the page composition — the LLM call
    # already succeeded by the time we get here.
    try:
        from amx.utils.token_tracker import estimate_tokens, tracker

        est = estimate_tokens(messages)
        tracker.record_for(
            "pages_compose",
            est,
            llm,
            usage=getattr(result, "usage", None),
        )
    except Exception:  # noqa: BLE001 — cost tracking is opportunistic
        pass

    return strip_outer_markdown_fence(str(body)), model_name
