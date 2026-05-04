"""Pin every bare-logger format string to ASCII-only characters.

User report 2026-05-04: a Windows user running Python 3.13 on
``cp1252`` saw a traceback the moment the LLM call kicked off:

    UnicodeEncodeError: 'charmap' codec can't encode character
    '\\u2192' (→) in position 129: character maps to <undefined>

Rich-routed prompts (``info``/``warn``/``success``/``error`` from
``amx.utils.console``) survive cp1252 because Rich forces a UTF-8
console. ``logging.Logger`` does not — it writes to the raw
``sys.stderr`` which inherits the locale codec on Windows.

This test grep-walks every bare ``log.{debug,info,warning,
warn,error,critical}`` call site under ``amx/`` and asserts the
literal format string is plain ASCII. It catches the exact class
of regression the user reported (a contributor sneaking a
``→`` into a debug line that fires on every LLM call) before it
ships to PyPI.
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / "amx"

# Capture the FORMAT STRING argument of every bare-logger call.
# We deliberately accept double-quoted, single-quoted, and triple-
# quoted strings so a future contributor's stylistic choice can't
# tunnel an unsafe character past us.
_LOG_CALL = re.compile(
    r"""
    log\.(?:debug|info|warning|warn|error|critical|exception)
    \s*\(\s*
    (?P<quote>["']{1,3})
    (?P<text>.*?)
    (?P=quote)
    """,
    re.VERBOSE | re.DOTALL,
)

# Characters that are safe enough on every codec we ship to:
# regular ASCII plus newlines and tabs. Anything else
# (smart quotes, em-dashes, arrows, accented letters baked
# into a log line) risks the Windows cp1252 traceback.
_ALLOWED = set(range(0x20, 0x7F)) | {ord("\n"), ord("\t")}


def _is_ascii_safe(text: str) -> tuple[bool, str | None]:
    for ch in text:
        if ord(ch) not in _ALLOWED:
            return False, ch
    return True, None


def test_no_non_ascii_in_bare_logger_format_strings() -> None:
    offenders: list[tuple[str, str, str]] = []
    for path in ROOT.rglob("*.py"):
        if "__pycache__" in path.parts:
            continue
        text = path.read_text(encoding="utf-8")
        for match in _LOG_CALL.finditer(text):
            ok, bad = _is_ascii_safe(match.group("text"))
            if not ok:
                offenders.append((str(path), match.group("text")[:80], bad or "?"))
    assert not offenders, (
        "Bare logger format strings carry non-ASCII characters that crash on "
        "Windows cp1252:\n"
        + "\n".join(f"  {p}: {sample!r}  (char={ch!r})" for p, sample, ch in offenders)
    )
