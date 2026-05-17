"""Run resolution helpers for ``amx compare``.

Extracted from :mod:`amx.cli_support.commands.compare` so the
``_resolve_runs`` waterfall (single run id / comma-list / latest-N /
scope-match) plus the dimension auto-detection (``_detect_by``) live
in their own focused module. They form the entry point to every
``amx compare`` invocation — the rest of the command operates on the
resolved run list.

``compare.py`` re-exports the public names so existing test imports
(tests/test_compare.py) keep working unchanged.
"""

from __future__ import annotations

from typing import Any

from amx.config import AMXConfig
from amx.utils.console import error, warn

_BY_DIMENSIONS: tuple[str, ...] = (
    "llm_profile",
    "doc_profile",
    "code_profile",
    "llm_model",
    "db_profile",
    "run",
)


_BY_TO_RUN_KEY: dict[str, str] = {
    "model": "llm_model",
    "llm_model": "llm_model",
    "llm_profile": "llm_profile",
    "doc_profile": "doc_profile",
    "code_profile": "code_profile",
    "db_profile": "db_profile",
    "run": "id",
}


def _detect_by(runs: list[dict[str, Any]]) -> str:
    """Pick the first dimension that varies across the resolved runs.

    Falls back to ``"run"`` (so each run column gets a header but no
    cell-level highlighting) when every dimension is uniform.
    """
    for dim in _BY_DIMENSIONS:
        if dim == "run":
            continue
        values = {(r.get(dim) or "") for r in runs}
        if len(values) > 1:
            return dim
    return "run"


def _resolve_runs(
    *,
    cfg: AMXConfig,
    run_ids: tuple[str, ...],
    schema: str,
    table: str,
    last_n: int,
    command_filter: str,
) -> list[dict[str, Any]]:
    """Resolve which runs to compare — positional IDs > scope+last_n > current scope."""
    # Read ``history_store`` off the compare module so existing tests
    # that ``patch("amx.cli_support.commands.compare.history_store", ...)``
    # affect this call site too.
    from amx.cli_support.commands import compare as compare_module

    hs = compare_module.history_store()
    if hs is None:
        error("History store is not initialized.")
        return []

    if run_ids:
        out: list[dict[str, Any]] = []
        for raw in run_ids:
            try:
                rid = int(str(raw).lstrip("#"))
            except ValueError:
                warn(f"Skipping non-integer run id '{raw}'.")
                continue
            row = hs.get_run(rid)
            if row is None:
                warn(f"Run #{rid} not found — skipping.")
                continue
            out.append(row)
        # Newest-first to match the --last path.
        out.sort(key=lambda r: float(r.get("started_at") or 0.0), reverse=True)
        return out

    eff_schema = (schema or cfg.current_schema or "").strip()
    eff_table = (table or cfg.current_table or "").strip()
    cmd = command_filter if command_filter and command_filter != "all" else None

    if not eff_schema and not eff_table:
        error(
            "No scope to compare — pass run IDs or use --schema/--table. "
            "Example: /compare --schema sales --last 3."
        )
        return []

    return hs.find_runs_for_scope(
        schema=eff_schema or None,
        table=eff_table or None,
        command_filter=cmd,
        limit=max(1, int(last_n)),
    )
