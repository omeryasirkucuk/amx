"""Provider-agnostic Batch API for AMX.

Supports any provider that offers an asynchronous batch endpoint.
Each provider implements the ``BatchProvider`` interface; the public
``run_batch()`` function auto-selects the right one based on the
active ``LLMConfig``.

Currently supported providers
-----------------------------
* **OpenAI**     — ``/v1/batches`` (Files API upload, JSONL, 50 % cost)
* **Anthropic**  — ``/v1/messages/batches`` (inline requests, 50 % cost)

Adding a new provider
---------------------
1. Subclass ``BatchProvider``.
2. Register it in ``_PROVIDER_MAP``.
"""

from __future__ import annotations

import json
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

from amx.config import LLMConfig
from amx.llm.provider import ChatResult
from amx.utils.console import console
from amx.utils.logging import get_logger

log = get_logger("llm.batch")

_POLL_INITIAL_SLEEP = 10
_POLL_INTERVAL = 15


@dataclass
class BatchRequest:
    """A single deferred LLM request."""

    custom_id: str
    messages: list[dict[str, str]]
    max_tokens: int = 4096
    temperature: float = 0.2
    metadata: dict[str, Any] = field(default_factory=dict)


# ── Abstract base ────────────────────────────────────────────────────────────


class BatchProvider(ABC):
    """Strategy interface for provider-specific batch implementations."""

    def __init__(self, cfg: LLMConfig) -> None:
        self.cfg = cfg

    @abstractmethod
    def submit(self, requests: list[BatchRequest]) -> dict[str, ChatResult]:
        """Submit *requests*, poll until done, return ``{custom_id: ChatResult}``."""

    def _resolve_model(self) -> str:
        raw = (self.cfg.model or "").strip()
        return raw.split("/")[-1] if "/" in raw else raw


# ── OpenAI ───────────────────────────────────────────────────────────────────


class OpenAIBatchProvider(BatchProvider):

    def submit(self, requests: list[BatchRequest]) -> dict[str, ChatResult]:
        import io

        try:
            import openai
        except ImportError as exc:
            raise ImportError(
                "The 'openai' package is required for OpenAI Batch mode. "
                "Install it with: pip install openai"
            ) from exc

        client = openai.OpenAI(api_key=self.cfg.api_key or None)
        model = self._resolve_model() or "gpt-4o-mini"

        console.print(
            f"[bold cyan]Batch[/bold cyan] Preparing {len(requests)} request(s) "
            f"→ model [yellow]{model}[/yellow]"
        )

        jsonl = self._build_jsonl(requests, model)

        console.print("[dim]  ↳ Uploading request file…[/dim]")
        file_obj = client.files.create(
            file=("amx_batch.jsonl", io.BytesIO(jsonl), "application/jsonl"),
            purpose="batch",
        )

        batch = client.batches.create(
            input_file_id=file_obj.id,
            endpoint="/v1/chat/completions",
            completion_window="24h",
        )
        console.print(
            f"[dim]  ↳ Batch job [bold]{batch.id}[/bold] "
            f"(status=[green]{batch.status}[/green])[/dim]"
        )

        batch, elapsed = self._poll(client, batch, len(requests))

        if batch.status != "completed":
            raise RuntimeError(
                f"Batch job {batch.id} ended with status '{batch.status}'."
            )

        console.print(
            f"[green]  ✓ Completed in {elapsed}s "
            f"({batch.request_counts.completed}/{batch.request_counts.total})[/green]"
        )

        return self._download_results(client, batch)

    @staticmethod
    def _build_jsonl(requests: list[BatchRequest], model: str) -> bytes:
        lines: list[str] = []
        for req in requests:
            lines.append(json.dumps({
                "custom_id": req.custom_id,
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": {
                    "model": model,
                    "messages": req.messages,
                    "max_tokens": req.max_tokens,
                    "temperature": req.temperature,
                },
            }))
        return ("\n".join(lines) + "\n").encode()

    @staticmethod
    def _poll(client: Any, batch: Any, total: int) -> tuple[Any, int]:
        terminal = {"completed", "failed", "expired", "cancelled"}
        elapsed = 0
        time.sleep(_POLL_INITIAL_SLEEP)
        elapsed += _POLL_INITIAL_SLEEP
        while batch.status not in terminal:
            done = (batch.request_counts.completed or 0) if batch.request_counts else 0
            cnt = (batch.request_counts.total or 0) if batch.request_counts else total
            console.print(
                f"[dim]  ↳ [{elapsed:>4}s] {batch.status} — {done}/{cnt}[/dim]"
            )
            time.sleep(_POLL_INTERVAL)
            elapsed += _POLL_INTERVAL
            batch = client.batches.retrieve(batch.id)
        return batch, elapsed

    @staticmethod
    def _download_results(client: Any, batch: Any) -> dict[str, ChatResult]:
        if not batch.output_file_id:
            raise RuntimeError("Batch completed but output_file_id is missing.")

        output = client.files.content(batch.output_file_id)
        results: dict[str, ChatResult] = {}
        for line in output.text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                log.warning("Unparseable batch output line: %.120s", line)
                continue

            cid = obj.get("custom_id", "")
            body = (obj.get("response") or {}).get("body") or {}
            choices = body.get("choices") or []
            content = ((choices[0].get("message") or {}).get("content") or "") if choices else ""
            usage = body.get("usage")
            usage_dict = _normalize_usage(usage) if usage else None
            results[cid] = ChatResult(content=content, usage=usage_dict)

        log.debug("Parsed %d results from batch output", len(results))
        return results


# ── Anthropic ────────────────────────────────────────────────────────────────


class AnthropicBatchProvider(BatchProvider):

    def submit(self, requests: list[BatchRequest]) -> dict[str, ChatResult]:
        try:
            import anthropic
        except ImportError as exc:
            raise ImportError(
                "The 'anthropic' package is required for Anthropic Batch mode. "
                "Install it with: pip install anthropic"
            ) from exc

        client = anthropic.Anthropic(api_key=self.cfg.api_key or None)
        model = self._resolve_model() or "claude-sonnet-4-20250514"

        console.print(
            f"[bold cyan]Batch[/bold cyan] Preparing {len(requests)} request(s) "
            f"→ model [yellow]{model}[/yellow]"
        )

        batch_requests = self._build_requests(requests, model)

        batch = client.messages.batches.create(requests=batch_requests)
        console.print(
            f"[dim]  ↳ Batch [bold]{batch.id}[/bold] "
            f"(status=[green]{batch.processing_status}[/green])[/dim]"
        )

        batch, elapsed = self._poll(client, batch, len(requests))

        if batch.processing_status != "ended":
            raise RuntimeError(
                f"Anthropic batch {batch.id} ended with status "
                f"'{batch.processing_status}'."
            )

        succeeded = batch.request_counts.succeeded or 0
        total = (
            (batch.request_counts.succeeded or 0)
            + (batch.request_counts.errored or 0)
            + (batch.request_counts.canceled or 0)
            + (batch.request_counts.expired or 0)
        )
        console.print(
            f"[green]  ✓ Completed in {elapsed}s ({succeeded}/{total})[/green]"
        )

        return self._collect_results(client, batch)

    @staticmethod
    def _build_requests(
        requests: list[BatchRequest], model: str
    ) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for req in requests:
            system_parts = [m["content"] for m in req.messages if m["role"] == "system"]
            user_msgs = [
                {"role": m["role"], "content": m["content"]}
                for m in req.messages
                if m["role"] != "system"
            ]
            params: dict[str, Any] = {
                "model": model,
                "max_tokens": req.max_tokens,
                "messages": user_msgs,
            }
            if system_parts:
                params["system"] = "\n\n".join(system_parts)
            if req.temperature:
                params["temperature"] = req.temperature
            items.append({"custom_id": req.custom_id, "params": params})
        return items

    @staticmethod
    def _poll(client: Any, batch: Any, total: int) -> tuple[Any, int]:
        terminal = {"ended", "expired", "canceled", "canceling"}
        elapsed = 0
        time.sleep(_POLL_INITIAL_SLEEP)
        elapsed += _POLL_INITIAL_SLEEP
        while batch.processing_status not in terminal:
            done = (batch.request_counts.succeeded or 0) if batch.request_counts else 0
            console.print(
                f"[dim]  ↳ [{elapsed:>4}s] {batch.processing_status} — {done}/{total}[/dim]"
            )
            time.sleep(_POLL_INTERVAL)
            elapsed += _POLL_INTERVAL
            batch = client.messages.batches.retrieve(batch.id)
        return batch, elapsed

    @staticmethod
    def _collect_results(client: Any, batch: Any) -> dict[str, ChatResult]:
        results: dict[str, ChatResult] = {}
        for entry in client.messages.batches.results(batch.id):
            cid = entry.custom_id
            if entry.result.type != "succeeded":
                log.warning("Batch entry %s failed: %s", cid, entry.result.type)
                continue
            msg = entry.result.message
            content = ""
            for block in (msg.content or []):
                if getattr(block, "type", None) == "text":
                    content += block.text
            usage = msg.usage
            usage_dict = {
                "prompt_tokens": getattr(usage, "input_tokens", 0) or 0,
                "completion_tokens": getattr(usage, "output_tokens", 0) or 0,
                "total_tokens": (
                    (getattr(usage, "input_tokens", 0) or 0)
                    + (getattr(usage, "output_tokens", 0) or 0)
                ),
            } if usage else None
            results[cid] = ChatResult(content=content, usage=usage_dict)

        log.debug("Parsed %d results from Anthropic batch", len(results))
        return results


# ── Registry ─────────────────────────────────────────────────────────────────

_PROVIDER_MAP: dict[str, type[BatchProvider]] = {
    "openai": OpenAIBatchProvider,
    "anthropic": AnthropicBatchProvider,
}


def get_batch_provider(cfg: LLMConfig) -> BatchProvider | None:
    """Return the batch provider for *cfg*, or ``None`` if unsupported."""
    cls = _PROVIDER_MAP.get(cfg.provider)
    if cls is None:
        return None
    return cls(cfg)


def supported_providers() -> list[str]:
    """Return the list of provider names that support batch mode."""
    return list(_PROVIDER_MAP.keys())


# ── Public entry point ───────────────────────────────────────────────────────

def _normalize_usage(raw: dict | None) -> dict | None:
    if not raw:
        return None
    return {
        "prompt_tokens": raw.get("prompt_tokens", 0),
        "completion_tokens": raw.get("completion_tokens", 0),
        "total_tokens": raw.get("total_tokens", 0),
    }


def run_batch(
    requests: list[BatchRequest],
    llm_cfg: LLMConfig,
) -> dict[str, ChatResult]:
    """Submit *requests* via the active provider's batch API.

    Automatically selects the right ``BatchProvider`` based on
    ``llm_cfg.provider``.  Raises ``RuntimeError`` if the provider
    does not support batch mode.
    """
    if not requests:
        return {}

    provider = get_batch_provider(llm_cfg)
    if provider is None:
        raise RuntimeError(
            f"Provider '{llm_cfg.provider}' does not support batch mode. "
            f"Supported: {', '.join(supported_providers())}."
        )

    return provider.submit(requests)
