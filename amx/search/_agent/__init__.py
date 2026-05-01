"""Internal mixin modules for ``SearchAgent`` (v0.9 refactor).

The historical ``amx/search/agent.py`` was a 3700-line god-class with 70
methods spanning 6+ logical responsibilities (session memory, planning,
target resolution, short-circuit handlers, retrieval, answer
synthesis). v0.9 splits those clusters into mixin modules under this
package so each file is a manageable size, each cluster is testable
in isolation, and ``SearchAgent`` itself becomes a thin facade that
just composes the mixins and runs ``ask()``.

The mixins form a strict hierarchy: each only references attributes
set in ``SearchAgent.__init__`` plus methods exposed by mixins it
explicitly depends on. Public API is preserved — ``SearchAgent.ask()``
is the only call site outside this package.
"""

from amx.search._agent.answering import AnsweringMixin
from amx.search._agent.deterministic import DeterministicAnswersMixin
from amx.search._agent.planning import PlanningMixin
from amx.search._agent.resolution import ResolutionMixin
from amx.search._agent.retrieval import RetrievalMixin
from amx.search._agent.session_memory import SessionMemoryMixin
from amx.search._agent.short_circuits import ShortCircuitsMixin

__all__ = [
    "AnsweringMixin",
    "DeterministicAnswersMixin",
    "PlanningMixin",
    "ResolutionMixin",
    "RetrievalMixin",
    "SessionMemoryMixin",
    "ShortCircuitsMixin",
]
