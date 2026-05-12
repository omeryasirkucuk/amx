"""Confirmation-flow tests for the /review bulk-filtered CLI flags (PR B).

The actual file writes / DB writes are covered indirectly by the existing
pending-review and orchestrator tests; here we pin the *prompt* contract so a
misclick can't silently mutate a review queue:

* ``--accept-filtered`` prompts once with the count
* ``--skip-filtered`` prompts once with the count
* ``--apply-filtered`` prompts twice — once for the count, once for the
  live-DB warning
"""

from __future__ import annotations

from amx.cli_support.review_picker import bulk_confirm


def test_accept_filtered_confirms_count_before_acting() -> None:
    captured: list[str] = []
    decided = bulk_confirm(
        action="accept",
        count=7,
        sample=["sales.orders", "sales.customers"],
        input_fn=lambda _: "yes",
        print_fn=captured.append,
    )
    assert decided is True
    assert any("Will accept 7 rows" in line for line in captured)


def test_skip_filtered_dry_run_when_user_declines() -> None:
    """User types ``no`` → returns False, caller should not mutate anything."""
    captured: list[str] = []
    decided = bulk_confirm(
        action="skip",
        count=12,
        sample=[],
        input_fn=lambda _: "no",
        print_fn=captured.append,
    )
    assert decided is False
    assert any("Will skip 12 rows" in line for line in captured)


def test_apply_filtered_requires_extra_confirmation() -> None:
    """Apply renders the count prompt + a live-DB warning prompt separately.

    The CLI command site calls ``bulk_confirm`` twice. Here we simulate that
    two-step pattern and assert that the warning text is surfaced AND that
    declining on either step short-circuits the action.
    """
    # Step 1 only — user accepts the count, but step 2 will pin the live-DB risk.
    step1 = bulk_confirm(
        action="apply",
        count=4,
        sample=[],
        input_fn=lambda _: "yes",
        print_fn=lambda _: None,
    )
    assert step1 is True

    captured: list[str] = []
    step2 = bulk_confirm(
        action="apply",
        count=4,
        sample=[],
        extra_warning="Apply writes COMMENT statements to the live database; this is permanent.",
        input_fn=lambda _: "no",
        print_fn=captured.append,
    )
    assert step2 is False
    assert any("permanent" in line for line in captured)


def test_apply_filtered_step2_yes_proceeds() -> None:
    decided = bulk_confirm(
        action="apply",
        count=2,
        sample=[],
        extra_warning="Type 'yes' again to proceed.",
        input_fn=lambda _: "yes",
        print_fn=lambda _: None,
    )
    assert decided is True


def test_singular_row_phrasing() -> None:
    captured: list[str] = []
    bulk_confirm(
        action="accept",
        count=1,
        sample=["sales.orders"],
        input_fn=lambda _: "no",
        print_fn=captured.append,
    )
    assert any("Will accept 1 row" in line and "rows" not in line for line in captured)
