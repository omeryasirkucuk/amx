"""Unified LLM interface via LiteLLM — supports OpenAI, Anthropic, Gemini, local models, etc."""

from __future__ import annotations

import os
from typing import Any

import litellm

from amx.config import LLMConfig
from amx.utils.logging import get_logger

log = get_logger("llm.provider")

# LiteLLM expects a provider prefix for many models (e.g. openai/gpt-4o).
# If the user already passes provider/model, we leave it unchanged.
PROVIDER_MODEL_PREFIX = {
    "openai": "openai/",
    "anthropic": "anthropic/",
    "gemini": "gemini/",
    "deepseek": "deepseek/",
    "local": "openai/",
    "kimi": "openai/",
    "ollama": "ollama/",
}

PROVIDER_ENV_KEY = {
    "openai": "OPENAI_API_KEY",
    "anthropic": "ANTHROPIC_API_KEY",
    "gemini": "GEMINI_API_KEY",
    "deepseek": "DEEPSEEK_API_KEY",
}

# OpenAI "reasoning" models (gpt-5*, o-series) may spend the whole max_tokens budget on
# internal reasoning, leaving message.content empty with finish_reason=length.
# Floor output budget + optional reasoning_effort (LiteLLM passes through to the API).
_DEFAULT_REASONING_FLOOR = 16_384


def _openai_model_id(model: str) -> str:
    return model.split("/")[-1].strip().lower()


def _is_openai_reasoning_style_model(model: str) -> bool:
    mid = _openai_model_id(model)
    return (
        mid.startswith("gpt-5")
        or mid.startswith("o1")
        or mid.startswith("o3")
        or mid.startswith("o4")
    )


class LLMProvider:
    """Thin wrapper around LiteLLM so every agent uses the same calling convention."""

    def __init__(self, cfg: LLMConfig):
        self.cfg = cfg
        self._configure_env()

    def _configure_env(self) -> None:
        env_key = PROVIDER_ENV_KEY.get(self.cfg.provider)
        if env_key and self.cfg.api_key:
            os.environ[env_key] = self.cfg.api_key
        if self.cfg.api_base and self.cfg.provider in ("local", "kimi"):
            os.environ["OPENAI_API_BASE"] = self.cfg.api_base
            os.environ.setdefault("OPENAI_API_KEY", self.cfg.api_key or "local")
        litellm.drop_params = True

    @property
    def model_name(self) -> str:
        raw = (self.cfg.model or "").strip()
        if not raw:
            return raw
        if "/" in raw:
            return raw
        prefix = PROVIDER_MODEL_PREFIX.get(self.cfg.provider, "")
        return f"{prefix}{raw}" if prefix else raw

    def chat(
        self,
        messages: list[dict[str, str]],
        temperature: float | None = None,
        max_tokens: int | None = None,
        **kwargs: Any,
    ) -> str:
        model = self.model_name
        mt = max_tokens or self.cfg.max_tokens
        extra: dict[str, Any] = dict(kwargs)

        # Reasoning models: raise floor so visible content can appear after thinking tokens.
        if self.cfg.provider == "openai" and _is_openai_reasoning_style_model(model):
            floor = int(os.getenv("AMX_LLM_MIN_MAX_TOKENS", str(_DEFAULT_REASONING_FLOOR)))
            if mt < floor:
                log.debug(
                    "Raising max_tokens %d → %d for reasoning model %s",
                    mt,
                    floor,
                    model,
                )
                mt = floor
            effort = os.getenv("AMX_REASONING_EFFORT", "low").strip().lower()
            if effort in ("none", "minimal", "low", "medium", "high"):
                extra.setdefault("reasoning_effort", effort)

        log.debug("LLM call → model=%s, max_tokens=%d", model, mt)
        try:
            resp = litellm.completion(
                model=model,
                messages=messages,
                temperature=temperature or self.cfg.temperature,
                max_tokens=mt,
                api_base=self.cfg.api_base if self.cfg.provider in ("local", "kimi") else None,
                **extra,
            )
        except Exception as exc:
            log.error("LLM call failed: %s", exc)
            raise

        content = resp.choices[0].message.content or ""
        finish = getattr(resp.choices[0], "finish_reason", None)
        usage = getattr(resp, "usage", None)
        log.debug(
            "LLM response: %d chars, finish_reason=%s, usage=%s",
            len(content),
            finish,
            usage,
        )

        if not content:
            if finish == "length":
                log.warning(
                    "LLM returned EMPTY content (finish_reason=length, model=%s). "
                    "For gpt-5 / o-series, output budget may be spent on reasoning only — "
                    "increase max_tokens in ~/.amx/config.yml (e.g. 32000), set env "
                    "AMX_LLM_MIN_MAX_TOKENS, and/or AMX_REASONING_EFFORT=minimal. "
                    "Or use gpt-4o for non-reasoning completions.",
                    model,
                )
            else:
                log.warning(
                    "LLM returned EMPTY content (finish_reason=%s, model=%s). "
                    "Check model name, API key, and provider dashboard.",
                    finish,
                    model,
                )
        return content

    def test(self) -> bool:
        try:
            reply = self.chat([{"role": "user", "content": "Reply with OK"}])
            return "ok" in reply.lower()
        except Exception as exc:
            log.error("LLM test failed: %s", exc)
            return False
