"""Phase helpers extracted from ``execute_analyze_run`` (v0.9.4 refactor).

The historical ``execute_analyze_run`` was a 600-line procedural script
with three top-level exception handlers (``FatalLLMError``,
``KeyboardInterrupt``, ``Exception``) and four runtime prompts (dedup
choice, scope finalization, coverage filter, review strategy) all
sharing local state. v0.9.4 extracts the two largest contiguous
phases — the per-schema orchestration loop and the post-loop summary
plus apply branch — into standalone functions, plus a small
``KeyboardInterrupt`` body helper. ``execute_analyze_run`` becomes a
~340-line orchestrator that wires the pieces together.

The extracted functions take the run state explicitly (no shared
class) so each is independently testable: feed in a stub
``Orchestrator``, a synthetic ``scope`` dict, and assert the returned
counters / pending-review state.
"""

from amx.cli_support.commands._analyze.interrupt import (
    handle_keyboard_interrupt,
)
from amx.cli_support.commands._analyze.run_loop import (
    PerSchemaLoopResult,
    run_per_schema_loop,
)
from amx.cli_support.commands._analyze.run_summary import (
    render_summary_and_apply,
)

__all__ = [
    "PerSchemaLoopResult",
    "handle_keyboard_interrupt",
    "render_summary_and_apply",
    "run_per_schema_loop",
]
