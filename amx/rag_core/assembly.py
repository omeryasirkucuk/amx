"""Prompt-assembly helpers for the RAG Agent (PR-H).

Three concerns live here:

1. **Edges-first chunk reorder** — combats the "Lost in the Middle"
   attention failure documented in Liu et al. (2023). LLMs pay
   strong attention to the beginning and end of the prompt and
   weak attention to the middle; placing the highest-scoring
   chunks at *both* edges and the lower-scoring chunks in the
   middle reduces the rate at which the model overlooks important
   evidence buried in mid-prompt context.

   The algorithm: take the top-k chunks (already in
   descending-relevance order from rerank), then split into
   odd-indexed and even-indexed positions, and concatenate
   ``odd + reversed(even)``. The top-scorers anchor both ends;
   mid-scorers settle into the attention-dead zone.

2. **Per-chunk citation header** — every chunk's body gets a
   short prefix line that names the source file, the Markdown
   section heading (if known — produced by PR-D's Markdown-aware
   splitter), and the chunk's relevance score. This gives the LLM
   a scannable summary it can latch onto even when attention is
   weakest in the middle of the prompt, and gives downstream
   citation extraction a stable channel to mine.

3. **Per-model input budget** — replaces the previous
   ``max_tokens * 3`` heuristic with a real per-model lookup via
   LiteLLM. Stops AMX from over-stuffing a small-context Mistral
   or under-using a 200k-token Claude.

All three are exposed as small pure-ish functions so PR-J can
extract them into the shared retrieval core without touching the
RAG agent's call sites again.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from amx.utils.logging import get_logger

log = get_logger("rag_core.assembly")


def assemble_chunks(chunks: Sequence[Mapping[str, Any]], k: int) -> list[Mapping[str, Any]]:
    """Take the top-``k`` chunks from ``chunks`` and reorder them so
    the highest-relevance chunks anchor both ends of the prompt.

    ``chunks`` is expected in descending-relevance order (the
    rerank output). The reorder works on the truncated top-``k`` —
    chunks beyond index ``k`` are dropped, which is **by construction**
    the "drop from middle" behaviour the old budget-compactor
    used to perform with a second pass: after edges-first reorder,
    the middle of the list is the lowest-relevance band.

    Algorithm:
        Given ``[c1, c2, c3, c4, c5, c6]`` (descending score)
        Top-k slice (k=6) → ``[c1, c2, c3, c4, c5, c6]``
        Odd positions (0, 2, 4) → ``[c1, c3, c5]``
        Even positions (1, 3, 5) → ``[c2, c4, c6]`` → reversed → ``[c6, c4, c2]``
        Result → ``[c1, c3, c5, c6, c4, c2]``

    Top scorers anchor both ends; mid-scorers sit in the middle
    where attention is weakest.

    The same operation as LangChain's ``LongContextReorder`` — no
    dependency on it here so PR-J can reuse this from outside the
    rag_agent without dragging in LangChain.

    Edge cases:
    - ``k <= 0`` → return ``[]``.
    - ``len(chunks) <= 1`` → return chunks unchanged.
    """
    if k <= 0:
        return []
    top = list(chunks[:k])
    if len(top) <= 1:
        return top
    odd = top[0::2]
    even = top[1::2]
    return odd + list(reversed(even))


def format_chunk_header(hit: Mapping[str, Any]) -> str:
    """One-line citation header rendered above each chunk body.

    Reads ``metadata.source``, ``metadata.h2`` / ``metadata.h3``
    (produced by PR-D's Markdown-aware splitter), and ``score``
    (from the rerank step). Falls back to a minimal header when
    fields are missing — the prompt never breaks on incomplete
    metadata, it just degrades from
    "orders.md | section=total_amount (rel=0.84)" to "orders.md".

    Examples:
        >>> format_chunk_header({
        ...     "metadata": {"source": "/path/orders.md",
        ...                  "h2": "total_amount"},
        ...     "score": 1.34})
        '[orders.md | section=total_amount] (rel=1.34)'

        >>> format_chunk_header({"metadata": {"source": "/x/y.txt"}})
        '[y.txt]'
    """
    meta = hit.get("metadata") or {}
    raw_source = meta.get("source") or "unknown"
    # Project to file basename so the header stays readable when
    # the absolute path is deep. Citation lookup still happens on
    # the full path via ``hit['metadata']['source']``.
    source = Path(str(raw_source)).name or str(raw_source)

    # Pick the most specific heading available. h3 is more specific
    # than h2; h2 than h1. None of them is fine.
    section = meta.get("h3") or meta.get("h2") or meta.get("h1") or ""

    score = hit.get("score")

    parts = [source]
    if section:
        parts.append(f"section={section}")
    header = "[" + " | ".join(parts) + "]"
    if isinstance(score, (int, float)):
        header = f"{header} (rel={float(score):.2f})"
    return header


def compute_input_budget(model_name: str, max_output_tokens: int) -> int:
    """Return the input-token budget for ``model_name``.

    Looks up the model's input window via LiteLLM
    (``litellm.get_model_info``) and subtracts the planned output
    budget plus a 256-token safety margin (for the system prompt,
    tool calls, etc.). Falls back to the previous ``max_tokens * 3``
    heuristic when the lookup fails (custom models, proxy
    deployments, LiteLLM stale on the resolver side), with a floor
    of 1000 tokens so the prompt is never starved.

    The lookup is cached by LiteLLM internally, so this is cheap to
    call on every retrieval.
    """
    floor = max(1_000, int(max_output_tokens) * 3)
    if not model_name:
        return floor
    try:
        import litellm  # noqa: E402

        info = litellm.get_model_info(model_name)
    except Exception as exc:  # noqa: BLE001 — broad on purpose
        # Custom model id LiteLLM doesn't know about → keep the
        # legacy heuristic. Log once-per-process via the logger's
        # built-in dedup (no extra book-keeping needed; AMX runs
        # exactly one _build_messages per table).
        log.debug("litellm.get_model_info(%r) failed: %s; using heuristic", model_name, exc)
        return floor
    max_input = int(info.get("max_input_tokens") or 0)
    if max_input <= 0:
        return floor
    safety_margin = 256
    usable = max(floor, max_input - int(max_output_tokens) - safety_margin)
    return usable


__all__ = [
    "assemble_chunks",
    "compute_input_budget",
    "format_chunk_header",
]
