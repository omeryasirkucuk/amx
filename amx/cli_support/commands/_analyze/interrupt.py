"""``KeyboardInterrupt`` handler body extracted from ``execute_analyze_run``.

Captures whatever partial state is on hand when the user hits Ctrl+C
mid-run, decides between ``ready_for_review`` and ``cancelled`` for
the final history status, and emits the structured log event.

The function returns ``(final_status, final_error_text)`` so the
caller's ``finally`` block can pass them to
``_finalize_history_run``.
"""

from __future__ import annotations

import contextlib
from typing import Any

from amx.utils.console import warn
from amx.utils.logging import get_logger
from amx.utils.token_tracker import tracker as token_tracker

log = get_logger("cli.analyze_flow.interrupt")


def handle_keyboard_interrupt(
    *,
    all_results: list[Any],
    review_strategy: str,
    use_batch: bool,
    run_id: int | None,
    history_store_fn: Any,
    log_event: Any,
) -> tuple[str, str]:
    """Build the final status + error text for a Ctrl+C during the run.

    Mutates ``all_results`` only insofar as it picks ``approved`` /
    ``skipped`` slices to save partial work. Returns
    ``(final_status, final_error_text)``.
    """
    approved = [r for r in all_results if getattr(r, "applied", False)]
    if approved:
        try:
            from amx.pending_review import save_pending

            save_pending(approved)
        except Exception:
            pass

    has_reviewable_results = bool(all_results)
    hs = history_store_fn()
    if not has_reviewable_results and run_id is not None and hs is not None:
        with contextlib.suppress(Exception):
            has_reviewable_results = bool(hs.get_run_results(run_id))
    if not has_reviewable_results:
        has_reviewable_results = bool(token_tracker.total_tokens)

    # auto-apply runs explicitly skip human review; landing them in
    # ``ready_for_review`` would tell the user to "go review what you've
    # done" — exactly the step they opted out of. The accepted partial
    # results are already in the catalog (and on /run-apply they're in
    # the live DB via apply_review_results_to_db), so 'cancelled' is the
    # correct terminal state.
    if review_strategy == "auto-apply":
        final_status = "cancelled"
    else:
        final_status = "ready_for_review" if has_reviewable_results else "cancelled"
    final_error_text = "Interrupted by user"

    log_event(
        event_type="analyze_run",
        status=final_status,
        command="analyze.run",
        details={
            "mode": ("batch" if use_batch else "chat"),
            "error": "KeyboardInterrupt",
            "results_ready": has_reviewable_results,
        },
    )
    warn("User interrupted process.")
    return final_status, final_error_text


__all__ = ["handle_keyboard_interrupt"]
