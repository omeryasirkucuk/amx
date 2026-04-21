"""OpenAI Batch API provider for AMX.

The Batch API lets AMX submit all LLM requests for a run as a single
asynchronous job, which OpenAI processes within 24 hours at 50 % cost.

Typical flow
------------
1. Collect ``BatchRequest`` objects from all agents (no LLM calls yet).
2. Call ``run_batch(requests, llm_cfg)`` — uploads JSONL, submits the job,
   polls with a live spinner, then returns ``{custom_id: ChatResult}``.
3. The orchestrator feeds results to agent parsers.

Only OpenAI is supported; other providers fall back to chat completions.
"""

from __future__ import annotations

import io
import json
import time
from dataclasses import dataclass, field
from typing import Any

from rich.console import Console

from amx.config import LLMConfig
from amx.llm.provider import ChatResult
from amx.utils.logging import get_logger

log = get_logger("llm.batch")
_console = Console()

_POLL_INITIAL_SLEEP = 10   # seconds before first status check
_POLL_INTERVAL = 15        # seconds between subsequent checks


@dataclass
class BatchRequest:
    """A single deferred LLM request to be included in an OpenAI Batch job."""

    custom_id: str
    messages: list[dict[str, str]]
    max_tokens: int = 4096
    temperature: float = 0.2
    metadata: dict[str, Any] = field(default_factory=dict)


def _build_jsonl(requests: list[BatchRequest], model: str) -> bytes:
    lines: list[str] = []
    for req in requests:
        body: dict[str, Any] = {
            "model": model,
            "messages": req.messages,
            "max_tokens": req.max_tokens,
            "temperature": req.temperature,
        }
        lines.append(
            json.dumps(
                {
                    "custom_id": req.custom_id,
                    "method": "POST",
                    "url": "/v1/chat/completions",
                    "body": body,
                }
            )
        )
    return ("\n".join(lines) + "\n").encode()


def run_batch(
    requests: list[BatchRequest],
    llm_cfg: LLMConfig,
) -> dict[str, ChatResult]:
    """Submit *requests* as an OpenAI Batch job and return results keyed by custom_id.

    Polls every ``_POLL_INTERVAL`` seconds while displaying a live status line.
    Raises ``RuntimeError`` if the batch fails or expires.
    """
    try:
        import openai
    except ImportError as exc:
        raise ImportError(
            "The 'openai' package is required for Batch mode. "
            "Install it with: pip install openai"
        ) from exc

    if not requests:
        return {}

    api_key = llm_cfg.api_key or None
    client = openai.OpenAI(api_key=api_key)

    # Resolve the bare model name (strip provider prefix).
    model = llm_cfg.model.split("/")[-1].strip() if "/" in (llm_cfg.model or "") else (llm_cfg.model or "gpt-4o-mini")

    _console.print(
        f"[bold cyan]Batch[/bold cyan] Preparing {len(requests)} request(s) "
        f"→ model [yellow]{model}[/yellow]"
    )

    jsonl_bytes = _build_jsonl(requests, model)

    # 1. Upload the JSONL file.
    _console.print("[dim]  ↳ Uploading request file to OpenAI Files API…[/dim]")
    file_obj = client.files.create(
        file=("amx_batch.jsonl", io.BytesIO(jsonl_bytes), "application/jsonl"),
        purpose="batch",
    )
    log.debug("Uploaded batch file id=%s", file_obj.id)

    # 2. Create the batch job.
    batch = client.batches.create(
        input_file_id=file_obj.id,
        endpoint="/v1/chat/completions",
        completion_window="24h",
    )
    log.debug("Created batch id=%s status=%s", batch.id, batch.status)
    _console.print(
        f"[dim]  ↳ Batch job created: [bold]{batch.id}[/bold] (status=[green]{batch.status}[/green])[/dim]"
    )

    # 3. Poll until terminal status.
    _TERMINAL = {"completed", "failed", "expired", "cancelled"}
    elapsed = 0
    time.sleep(_POLL_INITIAL_SLEEP)
    elapsed += _POLL_INITIAL_SLEEP

    while batch.status not in _TERMINAL:
        cnt_done = (batch.request_counts.completed or 0) if batch.request_counts else 0
        cnt_total = (batch.request_counts.total or 0) if batch.request_counts else len(requests)
        _console.print(
            f"[dim]  ↳ [{elapsed:>4}s] status=[bold]{batch.status}[/bold] "
            f"{cnt_done}/{cnt_total} completed…[/dim]"
        )
        time.sleep(_POLL_INTERVAL)
        elapsed += _POLL_INTERVAL
        batch = client.batches.retrieve(batch.id)

    if batch.status != "completed":
        raise RuntimeError(
            f"OpenAI Batch job {batch.id} ended with status '{batch.status}'. "
            "Check the OpenAI dashboard for error details."
        )

    _console.print(
        f"[green]  ✓ Batch completed in {elapsed}s "
        f"({batch.request_counts.completed}/{batch.request_counts.total} succeeded)[/green]"
    )

    # 4. Download and parse results.
    if not batch.output_file_id:
        raise RuntimeError(f"Batch {batch.id} completed but output_file_id is None.")

    output_bytes = client.files.content(batch.output_file_id)
    results: dict[str, ChatResult] = {}

    for line in output_bytes.text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            log.warning("Could not parse batch output line: %s", line[:120])
            continue

        cid = obj.get("custom_id", "")
        resp_body = (obj.get("response") or {}).get("body") or {}
        choices = resp_body.get("choices") or []
        content = ""
        if choices:
            content = ((choices[0].get("message") or {}).get("content") or "")

        usage_raw = resp_body.get("usage") or {}
        usage_dict: dict | None = None
        if usage_raw:
            usage_dict = {
                "prompt_tokens": usage_raw.get("prompt_tokens", 0),
                "completion_tokens": usage_raw.get("completion_tokens", 0),
                "total_tokens": usage_raw.get("total_tokens", 0),
            }

        results[cid] = ChatResult(content=content, usage=usage_dict)

    log.debug("Parsed %d results from batch output", len(results))
    return results
