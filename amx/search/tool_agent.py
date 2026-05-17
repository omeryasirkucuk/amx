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

import threading
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from amx.config import AMXConfig
from amx.llm.provider import LLMProvider
from amx.search._tool_agent_prompts import agent_system_prompt as _agent_system_prompt
from amx.search.agent_tools import ToolBox
from amx.search.catalog import SearchCatalog
from amx.utils.token_tracker import estimate_tokens
from amx.utils.token_tracker import tracker as token_tracker

if TYPE_CHECKING:
    from amx.utils.live_display import LiveDisplay

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
        scope_profiles: list[str] | None = None,
        focus_profile: str | None = None,
        total_latency_ms: int | None = None,
        per_tool_latency_ms: dict[str, int] | None = None,
    ) -> None:
        self.answer = answer
        self.tool_calls = tool_calls
        self.iterations = iterations
        self.usage = usage
        self.finish_reason = finish_reason
        # Multi-profile observability (PR-D): each /ask reports the
        # resolved scope, the auto-detected focus, the wall-clock
        # latency, and the per-tool latency breakdown so users (and
        # the SPA's footer badge) can spot slow profiles + measure
        # the cost of multi-profile retrieval.
        self.scope_profiles = scope_profiles
        self.focus_profile = focus_profile
        self.total_latency_ms = total_latency_ms
        self.per_tool_latency_ms = per_tool_latency_ms or {}

    def as_dict(self) -> dict[str, Any]:
        return {
            "answer": self.answer,
            "tool_calls": self.tool_calls,
            "iterations": self.iterations,
            "usage": dict(self.usage),
            "finish_reason": self.finish_reason,
            "scope_profiles": list(self.scope_profiles or []),
            "focus_profile": self.focus_profile,
            "total_latency_ms": self.total_latency_ms,
            "per_tool_latency_ms": dict(self.per_tool_latency_ms),
        }


def _convert_message_for_litellm(message: dict[str, Any]) -> dict[str, Any]:
    """LiteLLM expects the OpenAI message shape verbatim — nothing extra."""
    msg = {k: v for k, v in message.items() if v is not None}
    return msg


def _looks_partial(tool_result: str) -> bool:
    """Cheap textual check for the ``"partial": true`` marker on a
    tool result. The result is the JSON string the catalog tools
    return; parsing every result would be wasteful when only a
    minority carry the flag. ``"partial": true`` (with single or
    double quotes) is unambiguous enough."""
    if not tool_result:
        return False
    if '"partial": true' in tool_result or '"partial":true' in tool_result:
        return True
    return "'partial': True" in tool_result or "'partial':True" in tool_result


def _summarise_tool_call(tool_call: Any, result: str) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "name": tool_call.name,
        "arguments": tool_call.arguments,
        "result_preview": result[:280] + ("…" if len(result) > 280 else ""),
    }
    # PR E: surface structured citations for ``search_docs`` calls so the
    # /ask SSE stream can render a Sources block under the answer and a
    # per-hit table inside the tool-call expander. We parse the JSON
    # tool-result here (toolbox.invoke returns a ``_safe_json`` string)
    # and pull each hit's source / chunk_idx / score / snippet. The
    # extraction is best-effort — any parse failure falls back to an
    # empty list so existing callers keep working unchanged.
    # PR γ: ``search_code`` joins ``search_docs`` here so the /ask SSE
    # stream renders the same Sources block + per-call hit table for
    # code retrievals as it does for docs. The hit shape differs
    # (code hits carry ``rel_path`` + ``start_line`` / ``end_line``
    # instead of ``source`` + plain ``chunk_idx``); the citation dict
    # is the unified shape both surfaces emit.
    if tool_call.name in ("search_docs", "search_code"):
        citations: list[dict[str, Any]] = []
        try:
            import json as _json

            parsed = _json.loads(result) if isinstance(result, str) else result
            hits = parsed.get("hits") if isinstance(parsed, dict) else None
            if isinstance(hits, list):
                for hit in hits:
                    if not isinstance(hit, dict):
                        continue
                    raw_meta = hit.get("metadata")
                    meta: dict[str, Any] = raw_meta if isinstance(raw_meta, dict) else {}
                    # ``rel_path`` is the user-friendly key for code
                    # hits (``src/foo.py``); fall back to the absolute
                    # ``source`` only when ``rel_path`` is absent.
                    source = str(
                        hit.get("rel_path")
                        or hit.get("source")
                        or meta.get("rel_path")
                        or meta.get("source")
                        or ""
                    )
                    chunk_idx = hit.get("chunk_idx")
                    if chunk_idx is None:
                        chunk_idx = meta.get("chunk_idx")
                    try:
                        chunk_idx_int = int(chunk_idx) if chunk_idx is not None else 0
                    except (TypeError, ValueError):
                        chunk_idx_int = 0
                    distance = hit.get("distance")
                    score_val = hit.get("score")
                    if score_val is None and isinstance(distance, (int, float)):
                        # Chroma reports distance (lower = better); flip
                        # to a normalised 0..1 similarity score so the
                        # frontend can render a single "score 0.84" line
                        # consistent with PR C's RunDetail citations.
                        score_val = max(0.0, 1.0 - float(distance))
                    try:
                        score_float = float(score_val) if score_val is not None else 0.0
                    except (TypeError, ValueError):
                        score_float = 0.0
                    snippet = str(hit.get("snippet") or hit.get("text") or "")
                    if len(snippet) > 200:
                        snippet = snippet[:200]
                    # PR γ: pull line range from the hit (code-RAG)
                    # falling back to metadata. ``None`` when the
                    # chunk pre-dates the line-bound rollout so the
                    # frontend can fall back to ``path:chunk_idx``.
                    start_line = hit.get("start_line")
                    if start_line is None:
                        start_line = meta.get("start_line")
                    end_line = hit.get("end_line")
                    if end_line is None:
                        end_line = meta.get("end_line")
                    line_range: list[int] | None = None
                    if start_line is not None:
                        try:
                            sl = int(start_line)
                            el = int(end_line) if end_line is not None else sl
                            if sl > 0:
                                line_range = [sl, el]
                        except (TypeError, ValueError):
                            line_range = None
                    citations.append(
                        {
                            "source": source,
                            "chunk_idx": chunk_idx_int,
                            "score": score_float,
                            "snippet": snippet,
                            "line_range": line_range,
                        }
                    )
        except Exception:
            citations = []
        summary["citations"] = citations
    return summary


def run_tool_agent(
    *,
    cfg: AMXConfig,
    catalog: SearchCatalog,
    llm: LLMProvider,
    question: str,
    answer_language: str,
    session_memory: list[dict[str, Any]] | None = None,
    display: LiveDisplay | None = None,
    on_thinking_delta: Callable[[str], None] | None = None,
    on_tool_call: Callable[[dict[str, Any]], None] | None = None,
    on_content_delta: Callable[[str], None] | None = None,
    cancel_token: threading.Event | None = None,
    db_profiles: list[str] | None = None,
    doc_profiles: list[str] | None = None,
    code_profiles: list[str] | None = None,
    allow_live_refresh: bool = False,
) -> ToolAgentResult:
    """Run the tool-calling loop and return the final synthesised answer.

    ``session_memory`` carries the recap of prior turns the planner already
    builds in ``SearchAgent._memory_summary``. We forward it as a prelude
    user/assistant exchange so the model can resolve "it" / "that table" /
    "bu tablo" without re-asking the user.

    ``display`` is forwarded to the LLM call so reasoning models can stream
    their thinking text into the live thinking panel; for models that don't
    expose reasoning, it's a no-op.

    ``db_profiles`` is the multi-profile retrieval scope — every catalog
    tool call (``search_tables_by_concept``, ``find_joinable_tables``, …)
    expands its WHERE clause to ``db_profile IN (?, ?, …)`` so a single
    question can union evidence from N profiles. Empty / ``None`` falls
    back to ``cfg.active_db_profile`` (legacy single-profile behaviour) so
    pre-multi-profile callers keep working unchanged.

    The remaining kwargs are additive hooks AMX Studio's
    ``/api/ask`` SSE endpoint plugs into:

    * ``on_thinking_delta(text)`` — invoked for every reasoning chunk
      a thinking-model streams. Non-thinking models simply never call
      it. The CLI's ``LiveDisplay`` already receives the same chunks
      via ``display.update_thinking``; this kwarg gives the web layer
      a parallel hook so the SPA can render a streaming spinner.
    * ``on_tool_call({"name", "arguments", "result_preview"})`` —
      fired right after each tool returns. Lets the SPA render a
      "ran ``list_tables_in_schema``" log row in real time.
    * ``cancel_token`` — checked at the top of every iteration of the
      tool loop. When set, the loop bails before the next LLM call,
      raising :class:`amx.agents.orchestrator.RunCancelled` so the
      JobRegistry can flip the job to ``cancelled``.

    All three are optional and default to ``None`` so existing
    callers (CLI ``/ask``, batch scripts) keep working unchanged.

    ``allow_live_refresh`` (default ``False``) is the per-question
    cache-only gate. When ``False``, every tool that takes a
    ``force_fresh`` argument ignores it — the agent only sees cached
    catalog metadata for the duration of the question. When ``True``,
    ``force_fresh=true`` is honoured as before, letting the LLM pull
    fresh data from the live DB. The Ask UI exposes a "Live refresh"
    toggle that flips this bit; CLI ``/ask`` keeps the legacy
    cache-only-default contract.
    """
    # Use ``with`` so the live DB connector (SQLAlchemy engine + connection
    # pool) is disposed at the end of every question. Without this, each
    # ``/ask`` turn leaks a few file descriptors; after enough turns the
    # process hits ``OSError: Too many open files`` (the user-reported case).
    with ToolBox(
        cfg,
        catalog,
        db_profiles=db_profiles,
        doc_profiles=doc_profiles,
        code_profiles=code_profiles,
        allow_live_refresh=allow_live_refresh,
    ) as toolbox:
        return _run_tool_loop(
            toolbox=toolbox,
            cfg=cfg,
            llm=llm,
            question=question,
            answer_language=answer_language,
            session_memory=session_memory,
            display=display,
            on_thinking_delta=on_thinking_delta,
            on_tool_call=on_tool_call,
            on_content_delta=on_content_delta,
            cancel_token=cancel_token,
        )


def _compute_focus_profile(
    session_memory: list[dict[str, Any]] | None,
    scope: list[str],
) -> str | None:
    """Detect the conversation's focus profile from prior turns.

    Heuristic: scan the last ~3 assistant turns' answer text for
    ``db_profile=NAME`` or ``profile NAME`` mentions. If one profile
    accounts for ≥60% of the mentions, return it. Otherwise return
    ``None`` and let the LLM pick. Lightweight on purpose — we don't
    re-parse tool_call traces (those aren't carried in session_memory)
    so the heuristic operates on what the LLM has already said.

    Skipped entirely when scope is single-profile (focus is implicit).
    """
    if not scope or len(scope) < 2 or not session_memory:
        return None
    last_turns = [t for t in session_memory if t.get("role") == "assistant"][-3:]
    if not last_turns:
        return None
    counts: dict[str, int] = dict.fromkeys(scope, 0)
    text_blob = " ".join(str(t.get("content") or "") for t in last_turns).lower()
    for name in scope:
        # Word-boundary-ish: name surrounded by whitespace, punctuation, or quotes.
        # Cheap substring count is correct enough for short profile names.
        counts[name] = text_blob.count(name.lower())
    total = sum(counts.values())
    if total < 2:  # too few mentions → don't bias
        return None
    top_name, top_count = max(counts.items(), key=lambda kv: kv[1])
    if top_count == 0 or top_count / total < 0.60:
        return None
    return top_name


def _run_tool_loop(
    *,
    toolbox: ToolBox,
    cfg: AMXConfig,
    llm: LLMProvider,
    question: str,
    answer_language: str,
    session_memory: list[dict[str, Any]] | None,
    display: LiveDisplay | None = None,
    on_thinking_delta: Callable[[str], None] | None = None,
    on_tool_call: Callable[[dict[str, Any]], None] | None = None,
    on_content_delta: Callable[[str], None] | None = None,
    cancel_token: threading.Event | None = None,
) -> ToolAgentResult:
    # Pre-fetch the schema list once; if it succeeds we put it into the
    # system prompt so the LLM doesn't have to spend a tool call discovering
    # what schemas exist before answering simple "list tables in X" queries.
    schemas_hint: list[str] = []
    try:
        schemas_hint = [str(s) for s in toolbox._live_db().list_schemas()]  # noqa: SLF001
    except Exception:
        schemas_hint = []

    # Multi-profile system-prompt enrichment: scope (which profiles the
    # LLM may surface) + focus (which the conversation has gravitated
    # toward over recent turns). Both are advisory — tool_box catalog
    # calls already enforce scope; focus only nudges the LLM's framing.
    scope_profiles: list[str] = list(toolbox.db_profiles)
    focus_profile = _compute_focus_profile(session_memory, scope_profiles)

    messages: list[dict[str, Any]] = [
        {
            "role": "system",
            "content": _agent_system_prompt(
                cfg,
                schemas_hint,
                scope_profiles=scope_profiles,
                focus_profile=focus_profile,
            ),
        }
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
    # Per-tool latency accumulator: name → total ms across all calls
    # in this question (a single tool may be called multiple times).
    # Surfaced in the response payload so the SPA's footer can show
    # which tool dominated wall-clock — invaluable for debugging
    # "why did this take 9 seconds" without having to attach a
    # profiler.
    per_tool_latency_ms: dict[str, int] = {}
    import time as _time

    loop_start = _time.monotonic()
    final_answer = ""
    finish_reason: str | None = None
    last_thinking_content: str = ""
    iterations = 0
    tools_schema = ToolBox.schemas()

    # Single closure shared across iterations so the thinking panel keeps
    # streaming continuously even as we hop between LLM calls + tool calls.
    # The web layer ``on_thinking_delta`` runs alongside the CLI
    # ``display.update_thinking`` — both fire on the same chunk so /ask
    # in the terminal and /studio in the browser stay in sync.
    def _forward_thinking(text: str) -> None:
        if display is not None:
            display.update_thinking(text)
        if on_thinking_delta is not None:
            try:
                on_thinking_delta(text)
            except Exception:
                # Never let a UI hook crash the agent loop.
                pass

    on_thinking = (
        _forward_thinking if (display is not None or on_thinking_delta is not None) else None
    )

    for iteration in range(_MAX_ITERATIONS):
        iterations = iteration + 1
        if cancel_token is not None and cancel_token.is_set():
            from amx.agents.orchestrator import RunCancelled

            raise RunCancelled(f"Cancelled before iteration {iteration}")
        chat_messages = [_convert_message_for_litellm(m) for m in messages]
        # Pre-call token estimate so the tracker can label this step's
        # input cost. Falls back to 0 silently when tiktoken can't
        # tokenise the message shape (some structured tool messages).
        est = estimate_tokens(chat_messages)

        # Visible-content streaming. The web layer's on_content_delta
        # forwards per-token deltas as ``answer.delta`` SSE events live.
        # Interim narration ("Let me search the catalog…") that precedes
        # a tool call is also streamed — the SPA resets its answer buffer
        # whenever a ``tool.call`` event arrives, so by the time the
        # final iteration streams its real answer the user sees only
        # that. This keeps the streaming feel without buffering inside
        # the agent loop.
        def _forward_content(chunk: str) -> None:
            if on_content_delta is None:
                return
            try:
                on_content_delta(chunk)
            except Exception:
                # Never let a UI hook crash the agent loop.
                pass

        result = llm.chat(
            chat_messages,
            temperature=0.0,
            max_tokens=_AGENT_MAX_TOKENS,
            use_logprobs=False,
            tools=tools_schema,
            tool_choice="auto",
            on_thinking=on_thinking,
            on_content=_forward_content if on_content_delta is not None else None,
            # Threading the cancel token through ``llm.chat`` means the
            # stream consumer can bail between chunks, not just at the
            # next iteration boundary. Without this a Cancel click during
            # a long streamed answer waits for the whole answer to drain.
            cancel_token=cancel_token,
        )
        # Per-step record so the Run detail Metrics card can render
        # an honest tool_agent.iter row -- the previous wiring summed
        # ``aggregated_usage`` for the SSE answer.final event but never
        # wrote into ``analysis_runs.tokens_json``.
        token_tracker.record_for(
            f"tool_agent.iter{iterations}",
            est,
            llm,
            getattr(result, "usage", None),
        )
        # Aggregate usage across iterations.
        for key in ("prompt_tokens", "completion_tokens", "total_tokens"):
            aggregated_usage[key] += int((result.usage or {}).get(key, 0) or 0)
        finish_reason = result.finish_reason or finish_reason

        # Some thinking-channel models (gpt-oss family on Ollama, etc.)
        # emit their visible answer into ``reasoning_content`` and leave
        # ``content`` empty when the prompt is short or the harmony
        # ``final`` channel never fires. Capture the last seen reasoning
        # so the empty-content branch below can surface it instead of a
        # bare "(empty response)".
        if getattr(result, "thinking_content", "") or "":
            last_thinking_content = str(result.thinking_content)

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
            tool_t0 = _time.monotonic()
            tool_result = toolbox.invoke(tc.name, tc.arguments or "{}")
            tool_elapsed_ms = int((_time.monotonic() - tool_t0) * 1000)
            per_tool_latency_ms[tc.name] = per_tool_latency_ms.get(tc.name, 0) + tool_elapsed_ms
            summary = _summarise_tool_call(tc, tool_result)
            summary["latency_ms"] = tool_elapsed_ms
            tool_call_log.append(summary)
            if on_tool_call is not None:
                try:
                    on_tool_call(summary)
                except Exception:
                    pass
            messages.append(
                {
                    "role": "tool",
                    "tool_call_id": tc.id or f"tool_{iteration}_{tc.name}",
                    "content": tool_result,
                }
            )
            # Partial-catalog honesty injection. When a tool returns
            # ``"partial": true`` the catalog skeleton hasn't finished
            # syncing this profile yet, the result came from a live
            # DB query and may not enumerate every table the user
            # actually owns. Prepend a system-flavoured user note so
            # the LLM can't confidently report "9 tables total" when
            # the real count is unknown. The note is injected as a
            # ``user`` role (system-style messages aren't accepted
            # mid-conversation on every provider) and is dropped from
            # the session memory by the recap logic that already
            # filters synthetic messages.
            if _looks_partial(tool_result):
                messages.append(
                    {
                        "role": "user",
                        "content": (
                            "(system note) AMX's catalog skeleton sync for the "
                            "active DB profile is still running, so the tool "
                            "result above came from a live database query. "
                            "Treat the listed rows as a snapshot — not the "
                            "complete picture. Mention this explicitly in "
                            'your answer ("the catalog is still syncing, so '
                            'this list may be incomplete") instead of '
                            "implying the rows are the full set."
                        ),
                    }
                )
    else:
        # Hit the iteration cap without a final answer — force a closing call
        # without ``tools`` so the LLM returns plain text from whatever it
        # gathered.
        closing_messages = [_convert_message_for_litellm(m) for m in messages] + [
            {
                "role": "user",
                "content": (
                    "You've reached the tool-call budget. Compose your final answer now, in "
                    f"{answer_language or 'english'}, based on the tool results above."
                ),
            }
        ]
        est = estimate_tokens(closing_messages)
        result = llm.chat(
            closing_messages,
            temperature=0.0,
            max_tokens=_AGENT_MAX_TOKENS,
            use_logprobs=False,
            on_thinking=on_thinking,
        )
        token_tracker.record_for(
            "tool_agent.final",
            est,
            llm,
            getattr(result, "usage", None),
        )
        for key in ("prompt_tokens", "completion_tokens", "total_tokens"):
            aggregated_usage[key] += int((result.usage or {}).get(key, 0) or 0)
        final_answer = (result.content or "").strip()
        finish_reason = result.finish_reason or finish_reason
        if getattr(result, "thinking_content", "") or "":
            last_thinking_content = str(result.thinking_content)

    total_latency_ms = int((_time.monotonic() - loop_start) * 1000)
    # When the model produced no visible content, surface *why* instead
    # of a bare "(empty response)". Three observed shapes:
    #   1. Reasoning-channel models (gpt-oss on Ollama, Kimi K2.x
    #      thinking, …) emit their entire answer into the thinking
    #      channel — show the last thinking block (truncated) so the
    #      user has something to react to and points at the model as
    #      the root cause.
    #   2. finish_reason == "length" means the budget was exhausted
    #      mid-generation — say so explicitly with the configured
    #      max_tokens in the message.
    #   3. Otherwise: just "(empty response)" with a hint to try a
    #      different model.
    if not final_answer:
        if last_thinking_content.strip():
            thinking_preview = last_thinking_content.strip()
            if len(thinking_preview) > 1200:
                thinking_preview = thinking_preview[:1200].rstrip() + "…"
            final_answer = (
                "_The model returned reasoning text but no visible answer "
                "— likely a thinking-channel model whose final channel "
                "never fired. Showing the reasoning summary below; try a "
                "different model for a clean answer._\n\n"
                f"{thinking_preview}"
            )
        elif finish_reason == "length":
            final_answer = (
                "_The model hit its output-token budget before producing "
                "a visible answer. Raise ``max_tokens`` under Settings → "
                "LLM (or set ``AMX_LLM_MIN_MAX_TOKENS``) and retry._"
            )
        else:
            final_answer = (
                "_Empty response from the model. The provider returned no "
                "tool calls and no content — try rephrasing the question "
                "or switching to a different model._"
            )

    return ToolAgentResult(
        answer=final_answer,
        tool_calls=tool_call_log,
        iterations=iterations,
        usage=aggregated_usage,
        finish_reason=finish_reason,
        scope_profiles=list(toolbox.db_profiles),
        focus_profile=focus_profile,
        total_latency_ms=total_latency_ms,
        per_tool_latency_ms=per_tool_latency_ms,
    )
