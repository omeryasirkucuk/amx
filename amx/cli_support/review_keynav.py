"""Keyboard-navigation state machine for the CLI individual review loop.

PR B adds non-linear navigation to the row-by-row review: previously the loop
only walked forward (``n``); now the user can step backward (``p``), jump to
any position (``g N``), jump to the last (``G``), or narrow the remaining
queue with a sub-filter (``/ pattern``).

To keep the change tractable across Windows (no termios) and CI (no TTY) we
treat each navigation token as a full command-then-Enter string rather than
single-keypress raw mode. The shape mirrors what readline supports anywhere:

    Prompt:        ``[3/47 · sales.orders.customer_id] (a)ccept/(s)kip/(n)ext/(p)rev/(g)oto/(G)last/(/)filter/(?)help: ``
    Recognised:    ``n`` ``p`` ``j`` ``k`` ``g 5`` ``G`` ``/sales`` ``?``

This module owns the pure-state half of the dance — given a position, queue
length, and a command string it returns the next position OR a sentinel for
"open filter sub-prompt" / "show help". The CLI command site supplies the
actual ``input()`` + printing.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal

NavAction = Literal[
    "next",
    "prev",
    "goto",
    "last",
    "filter",
    "help",
    "unknown",
]


@dataclass(frozen=True)
class NavResult:
    """Outcome of interpreting a single nav command.

    ``position`` — zero-indexed position the loop should walk to next, OR the
    current position when the command is non-positional (filter, help).
    ``action`` — what the loop should do next (advance, prompt for filter, …).
    ``payload`` — for ``goto`` it's the int target; for ``filter`` the regex
    string after the leading ``/``; otherwise empty.
    """

    position: int
    action: NavAction
    payload: str = ""


_GOTO_RE = re.compile(r"^g\s+(\d+)$", re.IGNORECASE)


def parse_nav_command(
    command: str,
    *,
    position: int,
    queue_len: int,
) -> NavResult:
    """Interpret a navigation command at ``position`` against ``queue_len``.

    ``command`` is the user's raw input minus the trailing newline. Returns a
    :class:`NavResult`. Positions are clamped into ``[0, queue_len - 1]``.

    Recognised commands:

    * ``""`` / ``"n"`` / ``"j"`` → next (advance one; clamps at end)
    * ``"p"`` / ``"k"`` → previous (clamps at 0)
    * ``"g 5"`` → goto 1-indexed row 5 (clamped); ``"g"`` alone returns
      ``goto`` with empty payload so the CLI can sub-prompt
    * ``"G"`` → last row
    * ``"/pattern"`` → filter the remaining queue; payload is ``pattern``
    * ``"?"`` → help
    * anything else → ``unknown`` (position unchanged)
    """
    cmd = (command or "").strip()
    end = max(0, queue_len - 1)

    def clamp(i: int) -> int:
        return max(0, min(end, i))

    if cmd == "" or cmd.lower() in {"n", "j", "next"}:
        return NavResult(position=clamp(position + 1), action="next")
    if cmd.lower() in {"p", "k", "prev", "previous"}:
        return NavResult(position=clamp(position - 1), action="prev")
    if cmd == "G" or cmd.lower() == "last":
        return NavResult(position=end, action="last")
    if cmd == "?" or cmd.lower() == "help":
        return NavResult(position=position, action="help")
    if cmd.startswith("/"):
        return NavResult(position=position, action="filter", payload=cmd[1:].strip())
    if cmd.lower() == "g":
        # User typed ``g`` alone — caller should sub-prompt for the index.
        return NavResult(position=position, action="goto", payload="")
    match = _GOTO_RE.match(cmd)
    if match:
        target = int(match.group(1))
        return NavResult(position=clamp(target - 1), action="goto", payload=str(target))
    return NavResult(position=position, action="unknown")


KEYNAV_HELP_LINES: tuple[str, ...] = (
    "n / j / <enter>   — next row",
    "p / k             — previous row",
    "g N               — jump to 1-indexed row N (e.g. ``g 12``)",
    "G                 — jump to the last row",
    "/PATTERN          — filter the remaining queue by regex",
    "?                 — show this help",
)


def format_help() -> str:
    """Return the printable cheatsheet for the individual-review prompt."""
    return "\n".join(f"  {line}" for line in KEYNAV_HELP_LINES)


__all__ = ["NavAction", "NavResult", "parse_nav_command", "format_help", "KEYNAV_HELP_LINES"]
