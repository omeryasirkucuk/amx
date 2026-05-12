from amx.llm.style.guard import contains_leakage, scrub_placeholders
from amx.llm.style.profile import StyleProfile


def _profile(examples):
    return StyleProfile(
        language="en-US",
        tone="formal",
        avg_length_words=8,
        length_range=(4, 12),
        person="impersonal",
        capitalization="sentence-case",
        ends_with_period=True,
        structural_patterns=["noun + role"],
        vocabulary_register="business",
        redacted_examples=examples,
    )


def test_scrub_removes_known_placeholder():
    assert scrub_placeholders("Holds the <ENTITY> name.") == "Holds the name."


def test_scrub_collapses_whitespace():
    assert scrub_placeholders("<ENTITY> wraps <METRIC> values") == "wraps values"


def test_scrub_preserves_clean_text():
    assert scrub_placeholders("Order identifier issued at checkout.") == (
        "Order identifier issued at checkout."
    )


def test_scrub_handles_empty_and_none_like():
    assert scrub_placeholders("") == ""
    assert scrub_placeholders("   ") == ""


def test_guard_still_flags_placeholder_literal():
    p = _profile(["Unique id of the <ENTITY>."])
    assert contains_leakage("This row holds the <ENTITY> name.", p) is True


def test_sentinel_does_not_propagate_after_scrub():
    """Red-team: even if the model emits a placeholder, scrub removes it."""
    raw = "Captures the <ENTITY> created at <DATE_FIELD>."
    cleaned = scrub_placeholders(raw)
    assert "<ENTITY>" not in cleaned
    assert "<DATE_FIELD>" not in cleaned
    assert "Captures the created at" in cleaned


def test_profile_agent_scrubs_suggestions_when_style_active(tmp_path, monkeypatch):
    from unittest.mock import patch

    from amx.agents.base import Confidence, MetadataSuggestion
    from amx.agents.profile_agent import ProfileAgent, _scrub_suggestions
    from amx.config import AMXConfig
    from amx.llm.style.profile import StyleProfile
    from amx.storage.sqlite_store import SQLiteHistoryStore
    from amx.storage.style_store import StyleStore

    SQLiteHistoryStore(tmp_path / "history.db").init()
    StyleStore(tmp_path / "history.db").upsert(
        "default",
        "a.b.c",
        "duckdb",
        StyleProfile(
            language="en-US",
            tone="x",
            avg_length_words=1,
            length_range=(1, 1),
            person="x",
            capitalization="x",
            ends_with_period=True,
            structural_patterns=[],
            vocabulary_register="x",
            redacted_examples=["Unique id of the <ENTITY>."],
        ),
        sample_count=3,
    )

    class _FakeLLMCfg:
        column_batch_size = 10
        n_alternatives = 3
        description_verbosity = "brief"
        prompt_detail_cfg = None

    class _StubLLM:
        cfg = _FakeLLMCfg()

    class _FakeCfg:
        def __init__(self, config_dir):
            self.CONFIG_DIR = str(config_dir)
            self.active_llm_profile = "default"

    with patch.object(AMXConfig, "load", return_value=_FakeCfg(tmp_path)):
        agent = ProfileAgent(_StubLLM())

    assert agent._style_profile is not None

    dirty = [
        MetadataSuggestion(
            schema="s",
            table="t",
            column="c",
            suggestions=["Captures the <ENTITY> name."],
            confidence=Confidence.LOW,
            reasoning="",
            source="profile",
        )
    ]
    cleaned = _scrub_suggestions(dirty, agent._style_profile)
    assert cleaned[0].suggestions[0] == "Captures the name."
