"""Regression tests for the Rich-markup bug-class fix in 0.12.3.

The hard-to-spot bug: ``warn("pip install 'amx-cli[databricks]'")`` used
to render as ``⚠ pip install 'amx-cli'`` because ``warn`` wraps its arg
in ``[warning]…[/warning]`` and Rich then parses ``[databricks]`` as
another (unknown) style tag and silently drops it. Users copy-pasted
the wrong command and concluded the package was broken.

These tests pin the fix: every console helper that wraps user-supplied
text in Rich markup must escape that text so substrings shaped like
``[<word>]`` survive as literal characters in the rendered output.
"""

from __future__ import annotations

import io

import pytest
from rich.console import Console

from amx.utils import console as amx_console


@pytest.fixture
def captured_console(monkeypatch: pytest.MonkeyPatch) -> io.StringIO:
    """Replace the module-level Rich Console with one that writes to a buffer.

    Lets each test inspect the rendered output verbatim. The replacement
    uses ``record=True`` and ``force_terminal=False`` so style codes are
    stripped — we want to assert on the user-visible characters, not the
    ANSI escape sequences.
    """
    buf = io.StringIO()
    fake = Console(file=buf, force_terminal=False, color_system=None, theme=amx_console._theme)
    monkeypatch.setattr(amx_console, "console", fake)
    return buf


@pytest.mark.parametrize(
    "helper, payload",
    [
        (amx_console.warn, "pip install 'amx-cli[databricks]'"),
        (amx_console.info, "pip install 'amx-cli[bigquery]'"),
        (amx_console.success, "Installed amx-cli[snowflake]."),
        (amx_console.error, "Could not install amx-cli[mysql]."),
    ],
)
def test_brackets_in_helpers_are_preserved(
    captured_console: io.StringIO,
    helper,
    payload: str,
) -> None:
    helper(payload)
    out = captured_console.getvalue()
    # The literal substring must survive — no Rich-eaten brackets.
    assert payload in out, f"helper={helper.__name__!r} dropped brackets: {out!r}"


def test_pre_fix_failure_mode_is_now_safe(captured_console: io.StringIO) -> None:
    """The exact warn() text from amx/db/adapters/databricks.py:78 must
    render with [databricks] intact — that's the bug the user reported."""
    amx_console.warn(
        "databricks-sqlalchemy is required for the Databricks backend. "
        "Install the extra: pip install 'amx-cli[databricks]'"
    )
    out = captured_console.getvalue()
    assert "[databricks]" in out


def test_caller_supplied_markup_no_longer_styles_text(captured_console: io.StringIO) -> None:
    """Defensive: even tags that LOOK like real Rich styles (``[red]``,
    ``[bold]``) get escaped. We don't want a stray ``[bold]`` in a
    user-facing string to silently change the formatting."""
    amx_console.info("[bold]not actually bold[/bold]")
    out = captured_console.getvalue()
    assert "[bold]not actually bold[/bold]" in out


def test_heading_escapes_user_text(captured_console: io.StringIO) -> None:
    """`heading()` wraps text in a Panel with `[heading]` markup; the
    same escape contract applies."""
    amx_console.heading("Section [extra]")
    out = captured_console.getvalue()
    assert "[extra]" in out
