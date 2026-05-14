"""Pin the LLM capability table against future model additions.

The Studio gating UI and the CLI picker both call into
:mod:`amx.llm.capabilities` to grade whether a knob applies to the
selected (provider, model) pair. A regression here would silently let
users set ``thinking_budget`` on a non-reasoning model (no-op) or
request ``logprobs`` from a provider that 400s on them. Lock the table.
"""

from amx.llm.capabilities import supports_logprobs, supports_thinking


class TestSupportsThinking:
    def test_anthropic_sonnet_4(self) -> None:
        assert supports_thinking("anthropic", "claude-sonnet-4-5") is True

    def test_anthropic_opus_4(self) -> None:
        assert supports_thinking("anthropic", "claude-opus-4-20250514") is True

    def test_anthropic_legacy_3_5_no_thinking(self) -> None:
        assert supports_thinking("anthropic", "claude-3-5-sonnet-20241022") is False

    def test_openai_o3(self) -> None:
        assert supports_thinking("openai", "o3-mini") is True

    def test_openai_gpt_4o_no_thinking(self) -> None:
        assert supports_thinking("openai", "gpt-4o-mini") is False

    def test_deepseek_reasoner(self) -> None:
        assert supports_thinking("deepseek", "deepseek-reasoner") is True

    def test_deepseek_chat_no_thinking(self) -> None:
        assert supports_thinking("deepseek", "deepseek-chat") is False

    def test_openrouter_kimi_thinking(self) -> None:
        assert supports_thinking("openrouter", "moonshotai/kimi-k2-thinking") is True

    def test_unknown_provider(self) -> None:
        assert supports_thinking("nonexistent", "any-model") is False


class TestSupportsLogprobs:
    def test_anthropic_native_never_supports(self) -> None:
        assert supports_logprobs("anthropic", "claude-sonnet-4-5") is False

    def test_gemini_flash_unsupported(self) -> None:
        assert supports_logprobs("gemini", "gemini-1.5-flash") is False

    def test_gemini_pro_supported(self) -> None:
        # Non-flash Gemini accepts logprobs.
        assert supports_logprobs("gemini", "gemini-1.5-pro") is True

    def test_openai_o_series_unsupported(self) -> None:
        assert supports_logprobs("openai", "o3-mini") is False
        assert supports_logprobs("openai", "o1-preview") is False

    def test_openai_gpt_4o_supported(self) -> None:
        assert supports_logprobs("openai", "gpt-4o-mini") is True

    def test_openai_gpt_5_unsupported(self) -> None:
        assert supports_logprobs("openai", "gpt-5") is False

    def test_openrouter_anthropic_route_unsupported(self) -> None:
        assert supports_logprobs("openrouter", "anthropic/claude-sonnet-4") is False

    def test_openrouter_openai_route_supported(self) -> None:
        assert supports_logprobs("openrouter", "openai/gpt-4o-mini") is True

    def test_databricks_serving_unsupported(self) -> None:
        assert supports_logprobs("databricks_serving", "any-model") is False

    def test_unknown_provider_defaults_true(self) -> None:
        # Unknown provider: trust runtime fallback to catch rejections.
        assert supports_logprobs("nonexistent", "any-model") is True
