"""LLM adapter that satisfies :class:`amx.pages.composer.LLMClient`.

Wraps :class:`amx.llm.provider.LLMProvider` so the pages composer can
stay provider-agnostic. The :class:`amx.llm.provider.ChatResult` already
exposes a ``.content`` attribute, so :meth:`chat` simply delegates.
"""

from __future__ import annotations

from typing import Any

from amx.config import AMXConfig
from amx.llm.provider import LLMProvider


class AMXLLMClient:
    """Adapter exposing the active AMX LLM profile as the pages ``LLMClient``."""

    def __init__(self, cfg: AMXConfig) -> None:
        self._cfg = cfg
        self._provider = LLMProvider(cfg.llm)

    @property
    def model_name(self) -> str:
        return self._provider.model_name

    def chat(self, messages: list[dict[str, str]], **kw: Any) -> Any:
        return self._provider.chat(messages, **kw)
