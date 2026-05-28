"""CLI `schedule add` validates the cron expression at creation time.

The tick engine treats an invalid cron as one-shot (no re-arm), so a
typo'd "recurring" schedule used to fire once and silently stop, with the
error only in a log file the user never saw. The CLI now rejects it at
creation, matching the Studio route's behaviour.
"""

from __future__ import annotations

import click
import pytest

from amx.cli_support.commands.schedule import _validate_cli_cron


def test_valid_cron_returns_cleaned() -> None:
    assert _validate_cli_cron("0 */6 * * *") == "0 */6 * * *"
    assert _validate_cli_cron("  0 0 * * *  ") == "0 0 * * *"


def test_none_and_empty_normalise_to_none() -> None:
    assert _validate_cli_cron(None) is None
    assert _validate_cli_cron("   ") is None


def test_invalid_cron_raises_badparameter() -> None:
    with pytest.raises(click.BadParameter):
        _validate_cli_cron("every 6 hours")  # natural language, not cron
    with pytest.raises(click.BadParameter):
        _validate_cli_cron("0 0 0")  # too few fields
