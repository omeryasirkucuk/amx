"""Pure helpers extracted from ``amx.search.agent_tools``.

Pinning the contracts here so a future ``ToolBox`` mixin split can't
silently change a scoring heuristic. Both the new direct module and
the legacy re-export through ``agent_tools`` are tested so historical
imports keep working.
"""

from __future__ import annotations

import pytest


def test_helpers_are_re_exported_from_agent_tools_module() -> None:
    """The legacy import path must keep working for any external
    caller that grabbed a private helper before this PR landed."""
    from amx.search import _agent_tools_helpers as helpers
    from amx.search import agent_tools

    for name in (
        "_ToolError",
        "_name_overlap_score",
        "_dtype_compat_score",
        "_description_proximity",
        "_safe_json",
    ):
        assert getattr(agent_tools, name) is getattr(helpers, name), (
            f"{name} re-export drifted from the canonical helpers module"
        )


def test_name_overlap_identical_strings_score_one() -> None:
    from amx.search._agent_tools_helpers import _name_overlap_score

    assert _name_overlap_score("customer_id", "customer_id") == 1.0


def test_name_overlap_token_match_scores_high() -> None:
    from amx.search._agent_tools_helpers import _name_overlap_score

    score = _name_overlap_score("customer_id", "cust_id")
    assert 0.3 < score <= 1.0  # token "id" matches; jaccard or char ratio carries it


def test_name_overlap_unrelated_scores_zero() -> None:
    from amx.search._agent_tools_helpers import _name_overlap_score

    assert _name_overlap_score("customer_id", "payment_status") == 0.0


def test_name_overlap_empty_input_returns_zero() -> None:
    from amx.search._agent_tools_helpers import _name_overlap_score

    assert _name_overlap_score("", "anything") == 0.0
    assert _name_overlap_score("anything", "") == 0.0


def test_dtype_compat_same_family_returns_one() -> None:
    from amx.search._agent_tools_helpers import _dtype_compat_score

    assert _dtype_compat_score("INT", "BIGINT") == 1.0
    assert _dtype_compat_score("VARCHAR(64)", "TEXT") == 1.0


def test_dtype_compat_int_vs_numeric_is_weak() -> None:
    from amx.search._agent_tools_helpers import _dtype_compat_score

    assert _dtype_compat_score("INT", "NUMERIC") == 0.5


def test_dtype_compat_incompatible_returns_zero() -> None:
    from amx.search._agent_tools_helpers import _dtype_compat_score

    assert _dtype_compat_score("VARCHAR", "INT") == 0.0
    assert _dtype_compat_score("UUID", "INT") == 0.0


def test_description_proximity_zero_when_either_side_blank() -> None:
    from amx.search._agent_tools_helpers import _description_proximity

    assert _description_proximity("", "Order header.") == 0.0
    assert _description_proximity("Order header.", "") == 0.0


def test_safe_json_truncates_long_payloads() -> None:
    from amx.search._agent_tools_helpers import _safe_json

    big = {"rows": ["x" * 100 for _ in range(200)]}
    out = _safe_json(big, max_len=200)
    assert len(out) <= 200
    assert out.endswith("...<truncated>")


def test_safe_json_returns_str_for_unserialisable() -> None:
    from amx.search._agent_tools_helpers import _safe_json

    class _Funny:
        def __repr__(self) -> str:
            return "<funny>"

    out = _safe_json(_Funny())
    assert "<funny>" in out


def test_tool_error_is_runtime_error_subclass() -> None:
    """``ToolBox.invoke`` distinguishes ``_ToolError`` from arbitrary
    exceptions; the inheritance chain must not regress."""
    from amx.search._agent_tools_helpers import _ToolError

    assert issubclass(_ToolError, RuntimeError)
    with pytest.raises(_ToolError):
        raise _ToolError("smoke")
