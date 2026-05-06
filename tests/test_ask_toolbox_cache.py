"""Phase 3 of the perf plan: ToolBox memoizes tool calls within one
``/ask`` question.

The 6-iteration LLM loop in `tool_agent.py` lets the model re-ask for
the same data ("describe table foo", then a few iterations later
"describe table foo" again). Every duplicate invocation hits a real
``profile_table()`` underneath, which on Databricks/Snowflake/BigQuery
maps to a full warehouse scan. We cache by (tool_name, args) inside
the ToolBox so the second and later identical calls return a free
memory lookup.

Errors are deliberately not cached so a transient network blip can
be retried in the next iteration.
"""

from __future__ import annotations

import json

from amx.search.agent_tools import ToolBox


def _make_toolbox_with_counters() -> tuple[ToolBox, dict[str, int]]:
    """Build a ToolBox where each tool handler increments a call counter.

    Lets the test assert how many times the underlying handler ran
    (the cache should turn 3 calls with identical args into 1 handler
    invocation).
    """
    tb = ToolBox.__new__(ToolBox)  # bypass real __init__ wiring
    tb._tool_cache = {}
    tb._tool_cache_hits = 0
    tb._UNCACHED_TOOLS = frozenset()
    # Multi-profile cache key uses self.db_profiles tuple — set to a
    # single-profile scope so the bypass-init test fixture doesn't
    # crash on AttributeError when invoke() builds the key.
    tb.db_profiles = ["test"]
    tb.db_profile = "test"

    counters = {"echo": 0, "fail_echo": 0, "non_json": 0}

    def _tool_echo(self, value: str = "") -> dict:
        counters["echo"] += 1
        return {"result": value, "calls": counters["echo"]}

    def _tool_fail_echo(self, value: str = "") -> dict:
        counters["fail_echo"] += 1
        # Return error payload — must NOT be cached.
        return {"error": "synthetic failure"}

    def _tool_non_json(self, blob) -> dict:
        counters["non_json"] += 1
        return {"ok": True}

    # Bind the handlers as bound methods.
    tb._tool_echo = lambda **kw: _tool_echo(tb, **kw)  # type: ignore[attr-defined]
    tb._tool_fail_echo = lambda **kw: _tool_fail_echo(tb, **kw)  # type: ignore[attr-defined]
    tb._tool_non_json = lambda **kw: _tool_non_json(tb, **kw)  # type: ignore[attr-defined]
    return tb, counters


def test_identical_calls_hit_cache_after_first() -> None:
    tb, counters = _make_toolbox_with_counters()

    args = json.dumps({"value": "hello"})
    out1 = tb.invoke("echo", args)
    out2 = tb.invoke("echo", args)
    out3 = tb.invoke("echo", args)

    # Handler ran exactly once even though we invoked 3 times.
    assert counters["echo"] == 1
    # All three responses identical — same JSON snapshot.
    assert out1 == out2 == out3
    # Two of the three were cache hits.
    assert tb._tool_cache_hits == 2


def test_different_args_each_run_handler() -> None:
    tb, counters = _make_toolbox_with_counters()

    tb.invoke("echo", json.dumps({"value": "a"}))
    tb.invoke("echo", json.dumps({"value": "b"}))
    tb.invoke("echo", json.dumps({"value": "a"}))  # cache hit

    assert counters["echo"] == 2
    assert tb._tool_cache_hits == 1


def test_argument_ordering_doesnt_break_cache() -> None:
    """``{"a":1,"b":2}`` and ``{"b":2,"a":1}`` are the same call — same cache slot."""
    tb, counters = _make_toolbox_with_counters()

    # Add a handler that accepts two args
    def _tool_two_args(self, a, b):
        counters["echo"] += 1
        return {"a": a, "b": b}

    tb._tool_two_args = lambda **kw: _tool_two_args(tb, **kw)  # type: ignore[attr-defined]

    tb.invoke("two_args", '{"a": 1, "b": 2}')
    tb.invoke("two_args", '{"b": 2, "a": 1}')  # same call, different key order

    assert counters["echo"] == 1
    assert tb._tool_cache_hits == 1


def test_errors_are_not_cached() -> None:
    """A tool that returned ``{"error": ...}`` must be retried next call."""
    tb, counters = _make_toolbox_with_counters()

    args = json.dumps({"value": "x"})
    tb.invoke("fail_echo", args)
    tb.invoke("fail_echo", args)
    tb.invoke("fail_echo", args)

    # Error payload re-runs the handler every time.
    assert counters["fail_echo"] == 3
    assert tb._tool_cache_hits == 0


def test_uncached_tools_set_skips_cache() -> None:
    """Tools listed in ``_UNCACHED_TOOLS`` always re-run."""
    tb, counters = _make_toolbox_with_counters()
    # Mark "echo" as uncacheable post-hoc
    tb._UNCACHED_TOOLS = frozenset({"echo"})

    args = json.dumps({"value": "hi"})
    tb.invoke("echo", args)
    tb.invoke("echo", args)

    assert counters["echo"] == 2
    assert tb._tool_cache_hits == 0


def test_unknown_tool_is_not_cached() -> None:
    tb, _ = _make_toolbox_with_counters()
    out1 = tb.invoke("does_not_exist", "{}")
    tb.invoke("does_not_exist", "{}")
    # Unknown-tool response is an error payload → not cached → the
    # second call still ran the dispatch path but no cache was populated.
    assert "Unknown tool" in out1
    assert tb._tool_cache_hits == 0


def test_invalid_json_returns_error_no_crash() -> None:
    tb, _ = _make_toolbox_with_counters()
    out = tb.invoke("echo", "{not json}")
    assert "Invalid arguments JSON" in out
