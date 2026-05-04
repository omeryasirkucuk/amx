"""Tests for the first-class ``databricks_serving`` LLM provider.

User report 2026-05-04: Databricks Foundation Model Serving used to be
reachable only via the ``local`` provider with a manual api_base override.
The new ``databricks_serving`` provider gives it a proper place in the
``/llm`` wizard with workspace-host + endpoint-name + PAT prompts, an
auto-built ``api_base`` (``https://<host>/serving-endpoints``), and a
dedicated env var (``DATABRICKS_TOKEN``).
"""

from __future__ import annotations

import unittest
from unittest.mock import patch

from amx.cli_support.commands.profiles import default_model, interactive_llm_block
from amx.config import LLMConfig, normalize_llm_model
from amx.llm.provider import (
    PROVIDER_ENV_KEY,
    PROVIDER_MODEL_PREFIX,
    _normalized_api_base,
)


class ProviderRegistrationTests(unittest.TestCase):
    def test_databricks_serving_is_registered_in_provider_prefix_table(self) -> None:
        # LiteLLM routes ``databricks_serving`` via the OpenAI-compatible
        # client (Databricks Foundation Models exposes the same shape), so
        # the prefix is ``openai/`` — same as ``local`` / ``kimi``.
        self.assertEqual(PROVIDER_MODEL_PREFIX["databricks_serving"], "openai/")

    def test_databricks_serving_uses_databricks_token_env_var(self) -> None:
        # The PAT lands in DATABRICKS_TOKEN so it doesn't collide with any
        # OpenAI/OpenRouter key the user has set in the same shell.
        self.assertEqual(PROVIDER_ENV_KEY["databricks_serving"], "DATABRICKS_TOKEN")

    def test_default_model_returns_a_concrete_serving_endpoint_name(self) -> None:
        model = default_model("databricks_serving")
        self.assertTrue(model)
        self.assertNotIn("/", model, "Default should be the bare endpoint name, not prefixed")

    def test_normalize_llm_model_strips_a_duplicate_openai_prefix(self) -> None:
        # Users routinely paste ``openai/<endpoint>`` into the prompt. The
        # provider-prefix table already adds ``openai/``, so leaving the
        # duplicate would produce ``openai/openai/<endpoint>`` and LiteLLM
        # would reject it as an unknown provider.
        self.assertEqual(
            normalize_llm_model("databricks_serving", "openai/databricks-dbrx-instruct"),
            "databricks-dbrx-instruct",
        )


class ApiBaseNormalizationTests(unittest.TestCase):
    def test_workspace_host_alone_gets_serving_endpoints_suffix(self) -> None:
        # User pastes the bare host — we must build the chat-completions root.
        result = _normalized_api_base(
            "databricks_serving", "adb-xxxxxxxxxxxxxxxx.0.azuredatabricks.net"
        )
        self.assertEqual(
            result,
            "https://adb-xxxxxxxxxxxxxxxx.0.azuredatabricks.net/serving-endpoints",
        )

    def test_full_workspace_url_is_normalised(self) -> None:
        # User pastes the workspace URL — strip trailing slash, keep scheme.
        result = _normalized_api_base(
            "databricks_serving", "https://my-workspace.cloud.databricks.com/"
        )
        self.assertEqual(
            result,
            "https://my-workspace.cloud.databricks.com/serving-endpoints",
        )

    def test_already_normalised_url_passes_through(self) -> None:
        # Wizard-built api_base already carries the suffix — don't double it.
        result = _normalized_api_base(
            "databricks_serving",
            "https://my-workspace.cloud.databricks.com/serving-endpoints",
        )
        self.assertEqual(
            result,
            "https://my-workspace.cloud.databricks.com/serving-endpoints",
        )

    def test_other_providers_are_untouched(self) -> None:
        # The Databricks-specific normalisation only kicks in for
        # ``databricks_serving``. ``local`` / ``openai`` URLs are left as-is.
        self.assertEqual(
            _normalized_api_base("local", "http://localhost:11434/v1"),
            "http://localhost:11434/v1",
        )

    def test_empty_api_base_passes_through(self) -> None:
        self.assertIsNone(_normalized_api_base("databricks_serving", None))
        self.assertEqual(_normalized_api_base("databricks_serving", ""), "")


class WizardTests(unittest.TestCase):
    def test_wizard_builds_api_base_from_workspace_host_and_stores_pat(self) -> None:
        """Walking through the new prompts: select databricks_serving →
        type the endpoint name → type the workspace host (with scheme
        and trailing slash, to prove the normaliser strips them) → type
        the PAT → accept defaults for the generation/threshold prompts.
        The resulting LLMConfig must carry the auto-built api_base and
        the PAT in api_key (NOT the bare host or a leftover OpenAI key)."""
        ask_values = iter(
            [
                # endpoint name
                "databricks-meta-llama-3-1-70b-instruct",
                # workspace host (with scheme + trailing slash + path noise)
                "https://adb-xxxxxxxxxxxxxxxx.0.azuredatabricks.net/extra/path",
                # generation settings (n_alt / batch / temperature)
                "3",
                "10",
                "0.2",
                # logprob thresholds (high / medium)
                "0.85",
                "0.50",
            ]
        )
        choice_values = iter(["databricks_serving"])

        with (
            patch(
                "amx.cli_support.commands.profiles.ask_choice",
                side_effect=lambda *args, **kwargs: next(choice_values),
            ),
            patch(
                "amx.cli_support.commands.profiles.ask",
                side_effect=lambda *args, **kwargs: next(ask_values),
            ),
            patch(
                "amx.cli_support.commands.profiles.ask_password",
                side_effect=lambda *args, **kwargs: "dapi-test-token",
            ),
            patch("amx.cli_support.commands.profiles.info"),
        ):
            built = interactive_llm_block(LLMConfig())

        self.assertEqual(built.provider, "databricks_serving")
        self.assertEqual(built.model, "databricks-meta-llama-3-1-70b-instruct")
        self.assertEqual(
            built.api_base,
            "https://adb-xxxxxxxxxxxxxxxx.0.azuredatabricks.net/serving-endpoints",
        )
        self.assertEqual(built.api_key, "dapi-test-token")


if __name__ == "__main__":
    unittest.main()
