"""Top-up retry helper — guarantees that every LLM call lands an
asset with exactly ``n_alternatives`` entries.

Covers:

* ``top_up_alternatives`` shared helper at
  :mod:`amx.agents._top_up` — the single LLM call path.
* The ProfileAgent + Variations integration via mocks (top-up
  recovers under-production; fallback pads when the retry also
  fails; no top-up when the model returned a full set).

The user contract is hard: ``n_alternatives=3`` → persisted row
must carry 3 entries. Period. The retry recovers ~most cases; the
fallback string pad is the safety net that guarantees the count
regardless of model behaviour.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from amx.agents._top_up import (
    _build_top_up_prompt,
    _parse_top_up_response,
    top_up_alternatives,
)


class TestParseTopUpResponse:
    def test_numbered_descriptions(self) -> None:
        text = "DESCRIPTION_1: First alternative text.\nDESCRIPTION_2: Second alternative text.\n"
        out = _parse_top_up_response(text, n_needed=3)
        assert out == ["First alternative text.", "Second alternative text."]

    def test_strips_surrounding_quotes(self) -> None:
        text = 'DESCRIPTION_1: "Quoted alternative text."'
        out = _parse_top_up_response(text, n_needed=1)
        assert out == ["Quoted alternative text."]

    def test_caps_at_n_needed(self) -> None:
        text = "\n".join(f"DESCRIPTION_{i}: alt {i}" for i in range(1, 6))
        out = _parse_top_up_response(text, n_needed=2)
        assert out == ["alt 1", "alt 2"]

    def test_empty_response(self) -> None:
        assert _parse_top_up_response("", n_needed=3) == []

    def test_no_description_lines(self) -> None:
        assert _parse_top_up_response("Sorry, no idea.", n_needed=3) == []


class TestBuildTopUpPrompt:
    def test_includes_existing_alternatives(self) -> None:
        prompt = _build_top_up_prompt(
            existing_alts=["existing one", "existing two"],
            n_needed=1,
            asset_label="public.orders.status",
            mode="semantic",
            seed_text=None,
        )
        assert "existing one" in prompt
        assert "existing two" in prompt
        assert "do NOT repeat or paraphrase these" in prompt
        assert "1 NEW description" in prompt

    def test_includes_seed_when_present(self) -> None:
        prompt = _build_top_up_prompt(
            existing_alts=["a"],
            n_needed=1,
            asset_label="x",
            mode="lexical",
            seed_text="the seed",
        )
        assert "SEED_DESCRIPTION" in prompt
        assert "the seed" in prompt

    def test_lexical_mode_uses_distinct_meaning_directive(self) -> None:
        prompt = _build_top_up_prompt(
            existing_alts=[],
            n_needed=2,
            asset_label="x",
            mode="lexical",
            seed_text=None,
        )
        assert "DISTINCT CANDIDATE MEANING" in prompt

    def test_semantic_mode_uses_paraphrase_directive(self) -> None:
        prompt = _build_top_up_prompt(
            existing_alts=[],
            n_needed=2,
            asset_label="x",
            mode="semantic",
            seed_text=None,
        )
        assert "PARAPHRASE" in prompt


class TestTopUpAlternatives:
    def _mock_llm(self, response_text: str = "") -> MagicMock:
        """Build a stub LLM whose ``chat`` returns one ``ChatResult``-
        shaped object carrying ``response_text``."""
        llm = MagicMock()
        llm.chat.return_value = SimpleNamespace(content=response_text, usage=None, logprobs=None)
        return llm

    def test_returns_parsed_alternatives_when_llm_responds(self) -> None:
        llm = self._mock_llm("DESCRIPTION_1: Alpha\nDESCRIPTION_2: Beta\nDESCRIPTION_3: Gamma\n")
        out = top_up_alternatives(
            llm=llm,
            existing_alts=[],
            n_needed=3,
            asset_label="public.orders.status",
            mode="semantic",
        )
        assert out == ["Alpha", "Beta", "Gamma"]
        assert llm.chat.call_count == 1

    def test_drops_entries_matching_existing(self) -> None:
        """The model occasionally echoes an existing alt despite the
        explicit ``do not repeat`` instruction. The helper filters
        them out so the caller doesn't end up with duplicates."""
        llm = self._mock_llm(
            "DESCRIPTION_1: Existing One\nDESCRIPTION_2: Genuinely new alternative\n"
        )
        out = top_up_alternatives(
            llm=llm,
            existing_alts=["existing one"],
            n_needed=2,
            asset_label="x",
            mode=None,
        )
        # Echo dropped; new entry kept.
        assert out == ["Genuinely new alternative"]

    def test_drops_seed_echo(self) -> None:
        """When seed_text is passed, the helper additionally filters
        an entry that matches the seed verbatim."""
        llm = self._mock_llm("DESCRIPTION_1: The seed text\nDESCRIPTION_2: A real alternative\n")
        out = top_up_alternatives(
            llm=llm,
            existing_alts=[],
            n_needed=2,
            asset_label="x",
            mode="lexical",
            seed_text="The seed text",
        )
        assert out == ["A real alternative"]

    def test_empty_on_zero_needed(self) -> None:
        llm = self._mock_llm("DESCRIPTION_1: should not be called")
        out = top_up_alternatives(
            llm=llm,
            existing_alts=["x"],
            n_needed=0,
            asset_label="x",
            mode=None,
        )
        assert out == []
        assert llm.chat.call_count == 0

    def test_empty_on_llm_failure(self) -> None:
        llm = MagicMock()
        llm.chat.side_effect = RuntimeError("API unreachable")
        out = top_up_alternatives(
            llm=llm,
            existing_alts=[],
            n_needed=2,
            asset_label="x",
            mode=None,
        )
        # Caller pads with fallback strings — the helper just
        # surfaces an empty list on failure.
        assert out == []

    def test_caps_at_n_needed_even_when_model_returns_more(self) -> None:
        """Defends against a chatty model emitting more lines than
        requested — the caller's slot budget is preserved."""
        text = "\n".join(f"DESCRIPTION_{i}: alt {i}" for i in range(1, 6))
        llm = self._mock_llm(text)
        out = top_up_alternatives(
            llm=llm,
            existing_alts=[],
            n_needed=2,
            asset_label="x",
            mode=None,
        )
        assert len(out) == 2
