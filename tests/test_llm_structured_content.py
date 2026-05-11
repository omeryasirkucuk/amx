"""Tests for the structured-content flatten shim in amx/llm/provider.py.

User report 2026-05-04: Databricks Foundation Model Serving's gpt-oss-120b
endpoint returns ``message.content`` as an OpenAI-Responses-style list of
``{"type": "reasoning", ...}`` + ``{"type": "text", "text": "OK"}`` items.
LiteLLM's ``Message`` pydantic model declares ``content: str`` and rejects
the payload with a confusing
``InternalServerError: Invalid response object`` despite the upstream API
having returned a valid answer. The provider now installs a shim that
flattens structured content lists in place before LiteLLM's normalizer
runs.
"""

from __future__ import annotations

import unittest

from amx.llm.provider import (
    _flatten_structured_content,
    _normalize_response_dict_in_place,
)


class FlattenStructuredContentTests(unittest.TestCase):
    def test_plain_string_returns_none_so_caller_keeps_it(self) -> None:
        # The shim must not rewrite a content field that's already a string —
        # returning ``None`` signals "leave it alone".
        self.assertIsNone(_flatten_structured_content("hello"))
        self.assertIsNone(_flatten_structured_content(""))
        self.assertIsNone(_flatten_structured_content(None))

    def test_databricks_gpt_oss_shape_returns_text_content(self) -> None:
        """The user-reported Databricks gpt-oss-120b shape: a reasoning
        summary followed by the actual text answer. The reasoning summary
        must be dropped (it's surfaced via ``on_thinking`` separately) and
        only the user-facing text retained."""
        content = [
            {
                "type": "reasoning",
                "summary": [
                    {"type": "summary_text", "text": "The user says reply OK"},
                ],
            },
            {"type": "text", "text": "OK"},
        ]
        self.assertEqual(_flatten_structured_content(content), "OK")

    def test_multiple_text_chunks_concatenate(self) -> None:
        content = [
            {"type": "text", "text": "Hello, "},
            {"type": "text", "text": "world!"},
        ]
        self.assertEqual(_flatten_structured_content(content), "Hello, world!")

    def test_responses_api_output_text_alias_recognised(self) -> None:
        """OpenAI's Responses API uses ``output_text`` as the type key.
        The shim must accept both spellings so o3 / gpt-5 reasoning models
        over the Responses path work too."""
        content = [
            {"type": "output_text", "text": "Answer"},
        ]
        self.assertEqual(_flatten_structured_content(content), "Answer")

    def test_unknown_chunk_types_are_silently_ignored(self) -> None:
        """An unfamiliar chunk type (image / tool / ...) must NOT crash the
        shim — we drop it and keep the text we recognise."""
        content = [
            {"type": "tool_use", "id": "call_1"},
            {"type": "text", "text": "result"},
        ]
        self.assertEqual(_flatten_structured_content(content), "result")

    def test_reasoning_only_response_returns_empty_string(self) -> None:
        """Some endpoints emit only a reasoning summary with no text body
        when the model produced thinking but nothing user-facing. We
        return an empty string so LiteLLM's pydantic Message accepts the
        payload and the caller can inspect finish_reason / usage."""
        content = [
            {"type": "reasoning", "summary": [{"type": "summary_text", "text": "..."}]},
        ]
        self.assertEqual(_flatten_structured_content(content), "")


class NormalizeResponseDictInPlaceTests(unittest.TestCase):
    def test_message_content_list_flattened_in_place(self) -> None:
        payload = {
            "choices": [
                {
                    "message": {
                        "content": [
                            {"type": "reasoning"},
                            {"type": "text", "text": "OK"},
                        ],
                        "role": "assistant",
                    }
                }
            ]
        }
        _normalize_response_dict_in_place(payload)
        self.assertEqual(payload["choices"][0]["message"]["content"], "OK")

    def test_streaming_delta_content_list_also_flattened(self) -> None:
        """Streaming chunks land in ``choices[i].delta.content`` — the
        shim must cover both shapes so Server-Sent-Events payloads from
        reasoning models work too."""
        payload = {
            "choices": [
                {
                    "delta": {
                        "content": [{"type": "text", "text": "hello"}],
                    }
                }
            ]
        }
        _normalize_response_dict_in_place(payload)
        self.assertEqual(payload["choices"][0]["delta"]["content"], "hello")

    def test_string_content_left_untouched(self) -> None:
        payload = {"choices": [{"message": {"content": "already a string"}}]}
        _normalize_response_dict_in_place(payload)
        self.assertEqual(payload["choices"][0]["message"]["content"], "already a string")

    def test_non_dict_input_is_a_noop(self) -> None:
        # Defensive: the shim must not raise on weird inputs.
        _normalize_response_dict_in_place(None)
        _normalize_response_dict_in_place("not a dict")
        _normalize_response_dict_in_place([])

    def test_missing_choices_field_is_a_noop(self) -> None:
        payload = {"id": "x", "usage": {}}
        _normalize_response_dict_in_place(payload)
        self.assertEqual(payload, {"id": "x", "usage": {}})


class LiteLLMShimInstallationTests(unittest.TestCase):
    def test_litellm_import_installs_idempotent_shim(self) -> None:
        """Importing ``_litellm()`` twice must register the shim once and
        not double-wrap the LiteLLM normalizer (otherwise every call
        re-flattens content twice with no harm but with extra work)."""
        from amx.llm.provider import _litellm

        _litellm()
        from litellm.litellm_core_utils.llm_response_utils import (
            convert_dict_to_response as conv,
        )

        self.assertTrue(getattr(conv, "_amx_structured_content_shim", False))
        first_fn = conv.convert_to_model_response_object
        _litellm()
        self.assertIs(conv.convert_to_model_response_object, first_fn)

    def test_litellm_normalizer_returns_a_string_for_databricks_gpt_oss_payload(self) -> None:
        """Integration test: feed the LiteLLM normalizer a synthetic copy
        of the user's reported Databricks gpt-oss-120b response shape and
        confirm we get back a ModelResponse with ``content`` as a plain
        string (the same flow that previously raised a
        ``ValidationError`` on every call). Pinned to the user-reported
        failure so a future LiteLLM upgrade that changes the call
        signature surfaces here instead of in the field."""
        from amx.llm.provider import _litellm

        _litellm()
        from litellm.litellm_core_utils.llm_response_utils.convert_dict_to_response import (
            convert_to_model_response_object,
        )
        from litellm.types.utils import Choices, Message, ModelResponse, Usage

        payload = {
            "id": "chatcmpl_test",
            "choices": [
                {
                    "finish_reason": "stop",
                    "index": 0,
                    "logprobs": None,
                    "message": {
                        "content": [
                            {
                                "type": "reasoning",
                                "summary": [
                                    {"type": "summary_text", "text": "say OK"},
                                ],
                            },
                            {"type": "text", "text": "OK"},
                        ],
                        "role": "assistant",
                    },
                }
            ],
            "created": 0,
            "model": "gpt-oss-120b",
            "object": "chat.completion",
            "usage": {"completion_tokens": 1, "prompt_tokens": 10, "total_tokens": 11},
        }
        scaffold = ModelResponse(
            model=None,
            choices=[
                Choices(
                    finish_reason="stop",
                    index=0,
                    message=Message(content=None, role="assistant"),
                )
            ],
            usage=Usage(),
        )
        result = convert_to_model_response_object(
            response_object=payload,
            model_response_object=scaffold,
            response_type="completion",
            stream=False,
            start_time=None,
            end_time=None,
            hidden_params={},
            convert_tool_call_to_json_mode=None,
        )
        self.assertEqual(result.choices[0].message.content, "OK")
        self.assertEqual(int(result.usage.completion_tokens or 0), 1)


class MessageConstructorShimTests(unittest.TestCase):
    """Layer-2 of the structured-content fix.

    The Layer-1 patch on ``convert_to_model_response_object`` only
    intercepts calls that go through the module-level symbol AFTER our
    shim installed. LiteLLM has internal modules that did ``from …
    import convert_to_model_response_object`` at THEIR module load
    time, before our shim ran — those callers hold a frozen reference
    to the original function and bypass the rebind. The bug surfaced
    as a real ``ValidationError`` on Databricks gpt-oss-120b in the
    field even though the Layer-1 tests above were green.

    Patching ``Message.__init__`` directly closes the gap because
    every code path eventually instantiates the same ``Message``
    class. These tests pin that the patched constructor accepts a
    list-content payload without raising.
    """

    def test_message_with_list_content_accepted_after_shim(self) -> None:
        """``Message(content=[...])`` would raise a pydantic
        ``ValidationError`` upstream without our patch. After the
        shim installs the layer-2 wrapper, the same construction
        must succeed and ``content`` becomes the flattened string."""
        from amx.llm.provider import _litellm

        _litellm()  # install the shim
        from litellm.types.utils import Message

        message = Message(
            content=[
                {"type": "reasoning", "summary": [{"type": "summary_text", "text": "say OK"}]},
                {"type": "text", "text": "OK"},
            ],
            role="assistant",
        )
        self.assertEqual(message.content, "OK")

    def test_message_with_string_content_passes_through(self) -> None:
        """The shim must NOT touch string content — that would be a
        regression for the 99% case where the upstream API already
        returned a clean string."""
        from amx.llm.provider import _litellm

        _litellm()
        from litellm.types.utils import Message

        message = Message(content="already a string", role="assistant")
        self.assertEqual(message.content, "already a string")

    def test_message_with_none_content_passes_through(self) -> None:
        """``content=None`` is also valid (tool-only responses); the
        shim must not coerce it to an empty string."""
        from amx.llm.provider import _litellm

        _litellm()
        from litellm.types.utils import Message

        message = Message(content=None, role="assistant")
        self.assertIsNone(message.content)

    def test_double_litellm_call_keeps_shim_idempotent(self) -> None:
        """Re-importing ``_litellm`` must not stack the wrapper twice
        — otherwise every Message construction would flatten the
        same content two times with no extra benefit but visible cost."""
        from amx.llm.provider import _litellm

        _litellm()
        from litellm.types.utils import Message

        first_init = Message.__init__
        _litellm()
        self.assertIs(Message.__init__, first_init)


if __name__ == "__main__":
    unittest.main()
