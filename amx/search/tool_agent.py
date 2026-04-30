"""Tool-calling ``/ask`` agent.

Replaces the regex-driven LLM-Pass1/Pass2 router in ``amx/search/agent.py``
with a thin loop: hand the LLM a fixed set of metadata tools, let it pick
which one(s) to call, then synthesize a final answer from the gathered
results. The deterministic short-circuits (chitchat, meta-query,
reaffirmation) and the catalog-grounded target resolver remain the
responsibility of the caller (``SearchService``) — this module only owns
the tool-loop step.

Why this design exists (see CHANGELOG.md): the previous router classified
the whole question through a JSON schema and then we patched LLM mistakes
with regex overrides. Every new phrasing required a new regex, and the
regex overrides kept causing collateral bugs ("under" being captured from
"tables under sap_test"). The tool-calling loop pushes the routing
decision back into the model, but unlike the original prompt-only design,
the model now has actual catalog/live-DB tools to ground its answer
against — so it doesn't have to hallucinate.
"""

from __future__ import annotations

import json
import time
from dataclasses import asdict
from typing import Any

from amx.config import AMXConfig
from amx.llm.provider import LLMProvider
from amx.search.agent_tools import ToolBox
from amx.search.catalog import SearchCatalog


# Maximum number of tool-call iterations before we force the LLM to answer.
# A typical question takes 1–3 tool calls; 6 gives headroom for chained
# discovery (list_schemas → list_tables_in_schema → describe_table) without
# letting a confused model spin forever.
_MAX_ITERATIONS = 6

# Cap on tokens per LLM round in the agent loop. Tool answers are JSON
# blobs; we trim them in agent_tools._safe_json so the prompt stays bounded.
_AGENT_MAX_TOKENS = 1500


class ToolAgentResult:
    """Container for what the agent loop produced."""

    def __init__(
        self,
        *,
        answer: str,
        tool_calls: list[dict[str, Any]],
        iterations: int,
        usage: dict[str, Any],
        finish_reason: str | None,
    ) -> None:
        self.answer = answer
        self.tool_calls = tool_calls
        self.iterations = iterations
        self.usage = usage
        self.finish_reason = finish_reason

    def as_dict(self) -> dict[str, Any]:
        return {
            "answer": self.answer,
            "tool_calls": self.tool_calls,
            "iterations": self.iterations,
            "usage": dict(self.usage),
            "finish_reason": self.finish_reason,
        }


def _agent_system_prompt(cfg: AMXConfig, schema_hint: list[str]) -> str:
    """The single system prompt the LLM sees throughout the loop.

    Includes live ground-truth (databases, schemas) so the model can route
    without us having to regex-classify the question.
    """
    db_name = cfg.db.database or cfg.db.catalog or cfg.db.project or "(active database)"
    schema_line = (
        ", ".join(schema_hint) if schema_hint else "(none indexed yet — use list_schemas to discover)"
    )
    current_schema = cfg.current_schema or "(none — user has not pinned a schema)"
    current_table = cfg.current_table or "(none — user has not pinned a table)"
    metadata_lang = cfg.llm.language or "english"

    return (
        "You are AMX's metadata-search assistant. Answer the user's question by calling the "
        "tools available to you. NEVER guess; ALWAYS ground every claim in a tool result.\n\n"
        f"Active database: {db_name}\n"
        f"Schemas in this DB: {schema_line}\n"
        f"User's pinned schema: {current_schema}\n"
        f"User's pinned table: {current_table}\n"
        f"User's language preference: {metadata_lang}\n\n"
        "Routing guidance — choose the smallest correct path:\n"
        "* User names an exact identifier ('vbrk', 'adrc') → call find_table_by_name first; if it\n"
        "  returns one match, call describe_table on it. If multiple, ask the user to disambiguate.\n"
        "* User asks 'tables in <schema>' / 'tables under <schema>' / 'list tables of <schema>' → \n"
        "  call list_tables_in_schema with that exact schema. The user said 'tables', not\n"
        "  'a table named X'.\n"
        "* User asks 'which schemas / what schemas / how many schemas' → call list_schemas.\n"
        "* User asks 'which databases' → call list_databases.\n"
        "* User asks about a concept ('pricing tables', 'address columns', 'müşteri bilgisi') → \n"
        "  call search_tables_by_concept or search_columns_by_concept.\n"
        "* User asks how X and Y join → call get_join_candidates.\n"
        "* When the question is a follow-up (pronoun like 'it', 'this table', 'o tablo', 'bu'),\n"
        "  resolve it from the prior assistant turn(s) before calling a tool.\n\n"
        "When you have enough information, STOP calling tools and return a short, direct answer.\n"
        "Style:\n"
        "  - One natural-language paragraph.\n"
        "  - Quote schema.table identifiers in backticks.\n"
        f"  - Match the user's language; default to {metadata_lang}.\n"
        "  - If a tool reports 'found: false' or empty matches, say so plainly. NEVER invent a\n"
        "    table name. NEVER substitute a similar-sounding one without flagging it.\n"
        "  - If a tool is unavailable, explain what's missing and suggest the user run\n"
        "    `/search sync` (catalog refresh) or check their DB connection."
    )


def _convert_message_for_litellm(message: dict[str, Any]) -> dict[str, Any]:
    """LiteLLM expects the OpenAI message shape verbatim — nothing extra."""
    msg = {k: v for k, v in message.items() if v is not None}
    return msg


def _summarise_tool_call(tool_call: Any, result: str) -> dict[str, Any]:
    return {
        "name": tool_call.name,
        "arguments": tool_call.arguments,
        "result_preview": result[:280] + ("…" if len(result) > 280 else ""),
    }


def run_tool_agent(
    *,
    cfg: AMXConfig,
    catalog: SearchCatalog,
    llm: LLMProvider,
    question: str,
    answer_language: str,
    session_memory: list[dict[str, Any]] | None = None,
) -> ToolAgentResult:
    """Run the tool-calling loop and return the final synthesised answer.

    ``session_memory`` carries the recap of prior turns the planner already
    builds in ``SearchAgent._memory_summary``. We forward it as a prelude
    user/assistant exchange so the model can resolve "it" / "that table" /
    "bu tablo" without re-asking the user.
    """
    toolbox = ToolBox(cfg, catalog)
    # Pre-fetch the schema list once; if it succeeds we put it into the
    # system prompt so the LLM doesn't have to spend a tool call discovering
    # what schemas exist before answering simple "list tables in X" queries.
    schemas_hint: list[str] = []
    try:
        schemas_hint = [str(s) for s in toolbox._live_db().list_schemas()]  # noqa: SLF001
    except Exception:
        schemas_hint = []

    messages: list[dict[str, Any]] = [
        {"role": "system", "content": _agent_system_prompt(cfg, schemas_hint)}
    ]
    # Inject prior conversation context so follow-ups resolve.
    for turn in session_memory or []:
        role = str(turn.get("role") or "")
        content = str(turn.get("content") or "")
        if role in {"user", "assistant"} and content:
            messages.append({"role": role, "content": content})
    messages.append({"role": "user", "content": question})

    aggregated_usage = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
    tool_call_log: list[dict[str, Any]] = []
    final_answer = ""
    finish_reason: str | None = None
    iterations = 0
    tools_schema = ToolBox.schemas()

    for iteration in range(_MAX_ITERATIONS):
        iterations = iteration + 1
        result = llm.chat(
            [_convert_message_for_litellm(m) for m in messages],
            temperature=0.0,
            max_tokens=_AGENT_MAX_TOKENS,
            use_logprobs=False,
            tools=tools_schema,
            tool_choice="auto",
        )
        # Aggregate usage across iterations.
        for key in ("prompt_tokens", "completion_tokens", "total_tokens"):
            aggregated_usage[key] += int((result.usage or {}).get(key, 0) or 0)
        finish_reason = result.finish_reason or finish_reason

        if not result.tool_calls:
            final_answer = (result.content or "").strip()
            break

        # Append the assistant's tool-call request, then the tool results.
        messages.append(
            {
                "role": "assistant",
                "content": result.content or None,
                "tool_calls": [
                    {
                        "id": tc.id or f"tool_{iteration}_{idx}",
                        "type": "function",
                        "function": {"name": tc.name, "arguments": tc.arguments or "{}"},
                    }
                    for idx, tc in enumerate(result.tool_calls)
                ],
            }
        )
        for tc in result.tool_calls:
            tool_result = toolbox.invoke(tc.name, tc.arguments or "{}")
            tool_call_log.append(_summarise_tool_call(tc, tool_result))
            messages.append(
                {
                    "role": "tool",
                    "tool_call_id": tc.id or f"tool_{iteration}_{tc.name}",
                    "content": tool_result,
                }
            )
    else:
        # Hit the iteration cap without a final answer — force a closing call
        # without ``tools`` so the LLM returns plain text from whatever it
        # gathered.
        result = llm.chat(
            [_convert_message_for_litellm(m) for m in messages]
            + [
                {
                    "role": "user",
                    "content": (
                        "You've reached the tool-call budget. Compose your final answer now, in "
                        f"{answer_language or 'english'}, based on the tool results above."
                    ),
                }
            ],
            temperature=0.0,
            max_tokens=_AGENT_MAX_TOKENS,
            use_logprobs=False,
        )
        for key in ("prompt_tokens", "completion_tokens", "total_tokens"):
            aggregated_usage[key] += int((result.usage or {}).get(key, 0) or 0)
        final_answer = (result.content or "").strip()
        finish_reason = result.finish_reason or finish_reason

    return ToolAgentResult(
        answer=final_answer or "(empty response)",
        tool_calls=tool_call_log,
        iterations=iterations,
        usage=aggregated_usage,
        finish_reason=finish_reason,
    )
