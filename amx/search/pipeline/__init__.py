"""Pure-function pipeline stages for the /ask agent.

The /ask refactor (see ``moonlit-snacking-quokka.md`` plan) replaces
the mixin-based ``SearchAgent`` with a pipeline:

    ask(q) = synthesize(retrieve(plan(interpret(q))))

Each stage owns one concern and takes its dependencies explicitly so
it can be unit-tested without instantiating the whole SearchAgent.
This module is the incremental landing point — stages migrate here
one at a time, then ``SearchAgent`` mixins become thin shims that
delegate to the pure functions.
"""

from amx.search.pipeline.budget import (
    TRUNCATED_TOOL_PAYLOAD,
    enforce_input_token_budget,
)

__all__ = [
    "TRUNCATED_TOOL_PAYLOAD",
    "enforce_input_token_budget",
]
