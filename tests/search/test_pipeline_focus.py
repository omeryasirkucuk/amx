"""Pure detection tests for the conversation-focus heuristic.

``compute_focus_profile`` was inlined inside
``amx/search/tool_agent.py`` before PR 8 moved it to its own
pipeline module. The heuristic is purely textual so it deserves
isolated coverage — the legacy location was tested only indirectly
through the tool-loop integration tests.

Important note on the heuristic: it uses ``text_blob.count(name)``,
a *literal substring count*, not a word-level match. Tests use
distinctive multi-character names (``prod``, ``stage``) so the
substring artifact doesn't pollute assertions.
"""

from __future__ import annotations

from amx.search.pipeline.focus import compute_focus_profile


def _msg(role: str, content: str) -> dict[str, object]:
    return {"role": role, "content": content}


def test_single_profile_scope_returns_none() -> None:
    """Focus is undefined in single-profile mode — the LLM cannot
    pick between alternatives that don't exist."""
    assert (
        compute_focus_profile(
            [_msg("assistant", "talking about profile prod")],
            scope=["prod"],
        )
        is None
    )


def test_no_session_memory_returns_none() -> None:
    """No prior turns → nothing to bias on."""
    assert compute_focus_profile(None, scope=["prod", "stage"]) is None
    assert compute_focus_profile([], scope=["prod", "stage"]) is None


def test_dominant_profile_wins_with_60_percent_threshold() -> None:
    """3 prod mentions vs 1 stage (75%) clears the 60% threshold."""
    turns = [
        _msg("assistant", "prod has the table customers."),
        _msg("assistant", "Working on prod tables again."),
        _msg("assistant", "Verifying prod data; stage is separate."),
    ]
    assert compute_focus_profile(turns, scope=["prod", "stage"]) == "prod"


def test_balanced_mentions_below_threshold_returns_none() -> None:
    """2 mentions each (50%) is below the 60% threshold → no focus,
    let the LLM pick. The function returns ``None``."""
    turns = [
        _msg("assistant", "prod customers."),
        _msg("assistant", "stage customers."),
        _msg("assistant", "prod and stage both have data."),
    ]
    # prod count = 2, stage count = 2, total = 4 → max share = 0.5 < 0.60
    assert compute_focus_profile(turns, scope=["prod", "stage"]) is None


def test_only_recent_three_assistant_turns_considered() -> None:
    """The heuristic looks at the LAST 3 assistant turns; older
    history is ignored so the focus follows the recent thread."""
    turns = [
        # Old turns (dropped) heavily mention stage
        _msg("assistant", "stage stage stage stage"),
        _msg("assistant", "stage stage stage"),
        # Recent 3 heavily mention prod
        _msg("assistant", "prod prod prod prod"),
        _msg("assistant", "prod is the focus."),
        _msg("assistant", "Still prod here."),
    ]
    assert compute_focus_profile(turns, scope=["prod", "stage"]) == "prod"


def test_user_turns_excluded() -> None:
    """Only assistant turns count toward focus — what the user said
    is not signal for what AMX has been emphasising."""
    turns = [
        _msg("user", "tell me about prod prod prod prod prod prod prod"),
        _msg("assistant", "Here is stage for you."),
        _msg("assistant", "Also stage stage."),
    ]
    # prod appears only in user turn → ignored. stage dominates assistant turns.
    assert compute_focus_profile(turns, scope=["prod", "stage"]) == "stage"


def test_too_few_mentions_returns_none() -> None:
    """The heuristic requires at least 2 total mentions to bias."""
    turns = [_msg("assistant", "prod mentioned once.")]
    assert compute_focus_profile(turns, scope=["prod", "stage"]) is None


def test_returns_profile_name_with_original_case() -> None:
    """The returned profile name preserves the case from ``scope``
    even though counting is case-insensitive — callers that compare
    profile names to scope entries can use ``==``."""
    turns = [
        _msg("assistant", "PROD has customers."),
        _msg("assistant", "still on PROD."),
    ]
    # text_blob is lowercased; scope name is "Prod"; counts.lower() = "prod"
    # appears 2 times; total = 2 → 100% share. Returns the original
    # case from scope.
    assert compute_focus_profile(turns, scope=["Prod", "Stage"]) == "Prod"


def test_tool_agent_backcompat_reexport_still_works() -> None:
    """`tool_agent._compute_focus_profile` is still importable and
    points at the canonical location — callers that imported through
    the old path keep working until the alias is removed."""
    import amx.search.tool_agent as ta

    assert ta._compute_focus_profile is compute_focus_profile
