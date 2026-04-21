"""Unified LLM interface via LiteLLM — supports OpenAI, Anthropic, Gemini, local models, etc."""

from __future__ import annotations

import math
import os
from dataclasses import dataclass, field
from typing import Any

import litellm

from amx.config import LLMConfig
from amx.utils.logging import get_logger

log = get_logger("llm.provider")

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


@dataclass
class ChatResult:
    """Return value of ``LLMProvider.chat`` — wraps the content string with usage metadata."""

    content: str
    usage: dict | None = None
    logprobs: list | None = None  # raw per-token logprob objects from the API

    def __str__(self) -> str:  # noqa: D105
        return self.content


# ── Logprob-based confidence calibration ────────────────────────────────────

_CONF_TOKEN_UPPER: frozenset[str] = frozenset({"HIGH", "MEDIUM", "LOW"})


def confidence_from_logprobs(logprobs_content: list | None) -> "str | None":
    """Scan token-level logprobs for the CONFIDENCE label and return calibrated level.

    Scans the completion token list for a HIGH / MEDIUM / LOW token that appears
    after a ``CONFIDENCE`` context window.  Returns the *logprob-calibrated* level:

    * p > 0.85  → ``"HIGH"``
    * p > 0.50  → ``"MEDIUM"``
    * p ≤ 0.50  → ``"LOW"``

    Returns ``None`` when logprobs are unavailable or the token is not found.
    """
    if not logprobs_content:
        return None

    for i, token_obj in enumerate(logprobs_content):
        token_str = (getattr(token_obj, "token", None) or "").strip().upper()
        if token_str not in _CONF_TOKEN_UPPER:
            continue

        prev_window = "".join(
            (getattr(t, "token", None) or "")
            for t in logprobs_content[max(0, i - 8) : i]
        ).upper()
        if "CONFIDENCE" not in prev_window:
            continue

        logprob = getattr(token_obj, "logprob", None)
        if logprob is None:
            continue

        prob = math.exp(float(logprob))
        log.debug("Logprob confidence token=%s prob=%.3f", token_str, prob)
        if prob > 0.85:
            return "HIGH"
        elif prob > 0.50:
            return "MEDIUM"
        else:
            return "LOW"

    return None


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

    @property
    def supports_logprobs(self) -> bool:
        """True when the configured provider can return per-token logprobs."""
        try:
            return litellm.supports_logprobs(model=self.model_name)
        except Exception:
            return False

    @property
    def supports_batch(self) -> bool:
        """True when the configured provider has a registered batch implementation."""
        from amx.llm.batch import get_batch_provider
        return get_batch_provider(self.cfg) is not None

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
        use_logprobs: bool = True,
        **kwargs: Any,
    ) -> ChatResult:
        model = self.model_name
        mt = max_tokens or self.cfg.max_tokens
        extra: dict[str, Any] = dict(kwargs)

        if use_logprobs and self.supports_logprobs:
            extra.setdefault("logprobs", True)
            extra.setdefault("top_logprobs", 5)

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

        choice = resp.choices[0]
        content = choice.message.content or ""
        finish = getattr(choice, "finish_reason", None)

        raw_usage = getattr(resp, "usage", None)
        usage_dict: dict | None = None
        if raw_usage:
            usage_dict = {
                "prompt_tokens": getattr(raw_usage, "prompt_tokens", 0) or 0,
                "completion_tokens": getattr(raw_usage, "completion_tokens", 0) or 0,
                "total_tokens": getattr(raw_usage, "total_tokens", 0) or 0,
            }

        raw_lp = getattr(choice, "logprobs", None)
        logprobs_content: list | None = None
        if raw_lp is not None:
            logprobs_content = getattr(raw_lp, "content", None) or None

        log.debug(
            "LLM response: %d chars, finish_reason=%s, usage=%s, logprobs=%s",
            len(content),
            finish,
            usage_dict,
            "yes" if logprobs_content else "no",
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
        return ChatResult(content=content, usage=usage_dict, logprobs=logprobs_content)

    def test(self) -> bool:
        try:
            result = self.chat([{"role": "user", "content": "Reply with OK"}])
            return "ok" in result.content.lower()
        except Exception as exc:
            log.error("LLM test failed: %s", exc)
            return False
