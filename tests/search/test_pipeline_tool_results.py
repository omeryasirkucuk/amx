"""Coverage for the tool-result inspection helpers.

`looks_partial` was inlined in ``amx/search/tool_agent.py`` before
PR 9 moved it to the pipeline package. The textual heuristic gets
direct coverage here so the future removal of the back-compat
re-export does not lose its test.
"""

from __future__ import annotations

from amx.search.pipeline.tool_results import looks_partial


def test_returns_false_for_none_and_empty() -> None:
    assert looks_partial(None) is False
    assert looks_partial("") is False


def test_detects_canonical_double_quoted_form_with_space() -> None:
    assert looks_partial('{"partial": true, "rows": []}') is True


def test_detects_canonical_double_quoted_form_no_space() -> None:
    """Some JSON serializers compact the colon — both shapes are
    canonical so the heuristic must match either."""
    assert looks_partial('{"partial":true,"rows":[]}') is True


def test_detects_python_repr_single_quoted_form_with_space() -> None:
    """Some logging paths log a `dict` repr instead of JSON — both
    quote styles need to match for the loop's partial-catalog
    detection to fire."""
    assert looks_partial("{'partial': True, 'rows': []}") is True


def test_detects_python_repr_single_quoted_form_no_space() -> None:
    assert looks_partial("{'partial':True}") is True


def test_returns_false_when_flag_absent() -> None:
    assert looks_partial('{"partial": false, "rows": []}') is False
    assert looks_partial('{"rows": []}') is False


def test_returns_false_for_unrelated_text_containing_word_partial() -> None:
    """Mentioning 'partial' in prose must not trigger — the marker
    is the JSON KEY, not the literal word."""
    assert looks_partial("the rows are partial but complete") is False


def test_tool_agent_backcompat_reexport_still_works() -> None:
    """The legacy `_looks_partial` name on `tool_agent` still
    routes to the new implementation."""
    import amx.search.tool_agent as ta

    assert ta._looks_partial('{"partial": true}') is True
    assert ta._looks_partial(None) is False
