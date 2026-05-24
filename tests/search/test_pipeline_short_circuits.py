"""Pure detection tests for /ask short circuits.

The chitchat detector lives in
``amx/search/pipeline/short_circuits.py`` as a pure function so it
can be exercised without instantiating ``SearchAgent``. This suite
locks in the behaviour the legacy mixin used to inline; it also
guards against accidental drift between the two until the mixin's
side-effect shell is migrated in a later PR.
"""

from __future__ import annotations

from amx.search.pipeline.short_circuits import (
    chitchat_summary,
    is_chitchat,
)

# Subset of ``SearchAgent._CHITCHAT_TOKENS`` — kept small and
# explicit so the test reads as a spec for the rule.
TOKENS = frozenset({"hi", "hello", "hey", "thanks", "ok", "good", "morning"})


def test_short_pure_chitchat_recognised() -> None:
    for q in ("hi", "Hello", "Hey!", "thanks", "ok"):
        assert is_chitchat(q, TOKENS) is True, q


def test_two_word_chitchat_recognised() -> None:
    """Up to 4 words counts as chitchat when every token matches —
    'good morning', 'hi there' style salutations."""
    assert is_chitchat("good morning", TOKENS) is True
    assert is_chitchat("hello!", TOKENS) is True


def test_too_many_words_rejected() -> None:
    """Long greetings are NOT short-circuited; the planner gets
    them so a real follow-up question after a greeting still
    flows through normally."""
    assert is_chitchat("hi how are you doing today", TOKENS) is False


def test_non_chitchat_word_rejected() -> None:
    """A single off-token word in an otherwise short input means
    the user is asking something — must fall through to planning."""
    assert is_chitchat("hi customers", TOKENS) is False
    assert is_chitchat("show me tables", TOKENS) is False


def test_empty_question_rejected() -> None:
    assert is_chitchat("", TOKENS) is False
    assert is_chitchat("   ", TOKENS) is False


def test_punctuation_only_rejected() -> None:
    """After stripping ``?!.,;:`` the input has no words at all —
    nothing to classify."""
    assert is_chitchat("?", TOKENS) is False
    assert is_chitchat("!!", TOKENS) is False


def test_token_set_can_be_iterable_not_frozenset() -> None:
    """SearchAgent currently passes a ``frozenset`` but the
    contract should accept any iterable — converting once inside
    the function keeps callers honest."""
    assert is_chitchat("hi", ["hi", "hello"]) is True
    assert is_chitchat("hi", ("hi", "hello")) is True


def test_chitchat_summary_is_stable() -> None:
    """The canned reply is a constant; tests pin the wording so
    accidental edits surface as test failures."""
    s = chitchat_summary()
    assert "AMX's metadata search assistant" in s
    assert "vbrk" in s


# --------------------------------------------------------------------- meta_query

from amx.search.pipeline.short_circuits import (  # noqa: E402
    is_meta_query,
    meta_query_summary,
)


def test_meta_query_recognises_previous_question_phrasings() -> None:
    """The three legacy regex patterns each match at least one
    natural phrasing of "what was my last question?"."""
    for q in (
        "what was my previous question",
        "what was my last question?",
        "what is my prior question",
        "what did I ask?",
        "what have I asked so far",
        "what have I been asking",
    ):
        assert is_meta_query(q) is True, q


def test_meta_query_rejects_normal_questions() -> None:
    """Questions about data, not about the conversation itself,
    must fall through to planning."""
    for q in (
        "what is the customers table",
        "list all schemas",
        "describe the vbrk table",
        "",
        "   ",
    ):
        assert is_meta_query(q) is False, q


def test_meta_query_summary_uses_canonical_first_turn_wording_when_empty() -> None:
    s = meta_query_summary("")
    assert "first question in this session" in s


def test_meta_query_summary_quotes_prior_question_verbatim() -> None:
    s = meta_query_summary("what is the customers table?")
    assert 'Your previous question was: "what is the customers table?"' == s


def test_meta_query_summary_strips_whitespace_around_prior() -> None:
    """Leading/trailing whitespace on the stored prior question is
    cleaned before quoting."""
    s = meta_query_summary("   list schemas   ")
    assert 'Your previous question was: "list schemas"' == s


# --------------------------------------------------------------------- followup_reaffirmation

from amx.search.pipeline.short_circuits import (  # noqa: E402
    is_followup_reaffirmation,
    reaffirmation_summary,
)


def test_reaffirmation_recognises_short_pushbacks() -> None:
    """Each legacy pattern matches at least one natural pushback."""
    for q in (
        "are you sure",
        "are you sure?",
        "you sure?",
        "really",
        "seriously",
        "sure?",
        "is that right",
        "is that correct?",
        "is that true",
        "you positive",
        "positive?",
        "why",
        "why?",
        "how come",
        "how",
    ):
        assert is_followup_reaffirmation(q) is True, q


def test_reaffirmation_rejects_fresh_questions_containing_trigger_words() -> None:
    """Patterns are anchored with ^/$ — a fresh question that
    happens to contain 'sure' or 'why' must NOT short-circuit."""
    for q in (
        "are you sure the customers table exists",
        "why does this table have no rows",
        "is that correct in the schema",
        "what was the reason",
        "",
    ):
        assert is_followup_reaffirmation(q) is False, q


def test_reaffirmation_summary_quotes_prior_assistant_verbatim() -> None:
    s = reaffirmation_summary("The customers table has 42 rows.")
    assert "Yes, I'm sure" in s
    assert "live database metadata" in s
    assert "The customers table has 42 rows." in s


def test_reaffirmation_summary_strips_whitespace_around_prior() -> None:
    s = reaffirmation_summary("   42 rows.   ")
    assert s.endswith("To restate: 42 rows.")
