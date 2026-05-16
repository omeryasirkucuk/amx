"""Small bounded-input prompt helpers used by ``amx analyze``.

Extracted from :mod:`amx.cli_support.commands.analyze_flow` so the
three ``_ask_optional_*`` helpers and their input-validation rules
live in one focused module — they were originally the only piece of
``analyze_flow`` with no dependency on the run lifecycle and are a
natural home for any future shared CLI prompt helper.

``analyze_flow.py`` re-exports each name so any internal call site
that imported the underscore form continues to work unchanged.
"""

from __future__ import annotations

from amx.utils.console import ask, ask_choice, warn


def _ask_optional_float(
    prompt: str,
    *,
    current: float | None,
    lo: float,
    hi: float,
) -> tuple[bool, float | None]:
    """Prompt for a bounded float, returning ``(changed, value)``.

    ``current`` shows up as the default in the prompt. The user can:
    * press Enter to keep the current value (changed=False)
    * type ``-`` (a single dash) to clear the field (changed=True, value=None)
    * type a number in [lo, hi] (changed=True, value=number)

    Out-of-range or unparseable input is treated as "keep current"
    with a warning so a typo never silently lands a bad override.
    """
    raw = ask(prompt, default="" if current is None else str(current)).strip()
    if not raw:
        return False, current
    if raw == "-":
        return current is not None, None
    try:
        value = float(raw)
    except ValueError:
        warn(f"Could not parse {raw!r} as a number; keeping {current}.")
        return False, current
    if value < lo or value > hi:
        warn(f"Value {value} out of range [{lo}, {hi}]; keeping {current}.")
        return False, current
    return value != current, value


def _ask_optional_int(
    prompt: str,
    *,
    current: int,
    lo: int,
    hi: int,
) -> tuple[bool, int]:
    """Bounded integer prompt; same conventions as ``_ask_optional_float``."""
    raw = ask(prompt, default=str(current)).strip()
    if not raw:
        return False, current
    try:
        value = int(raw)
    except ValueError:
        warn(f"Could not parse {raw!r} as an integer; keeping {current}.")
        return False, current
    if value < lo or value > hi:
        warn(f"Value {value} out of range [{lo}, {hi}]; keeping {current}.")
        return False, current
    return value != current, value


def _ask_optional_choice(
    prompt: str,
    *,
    current: str,
    choices: list[str],
) -> tuple[bool, str]:
    """Pick-from-list prompt with ``current`` as the default."""
    selected = ask_choice(prompt, choices, default=current if current in choices else choices[0])
    return selected != current, selected
