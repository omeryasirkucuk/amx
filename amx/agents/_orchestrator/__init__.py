"""Internal helpers extracted from ``Orchestrator`` (v0.9.2 refactor).

The historical ``Orchestrator.process_table`` was a 281-line method
that grew to four overlapping filter chains plus an agent loop plus
three apply-branches. v0.9.2 extracts that logic into
:class:`TableProcessor` so each phase is its own short method.
``Orchestrator.process_table`` becomes a 4-line delegator.
"""

from amx.agents._orchestrator.rerun import RerunOutcome, rerun_items
from amx.agents._orchestrator.table_processor import TableProcessor

__all__ = ["RerunOutcome", "TableProcessor", "rerun_items"]
