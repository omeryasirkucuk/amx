"""Cross-platform terminal background detection.

The startup banner uses a single brand-orange accent (``#fb923c``). On a
black terminal that hits ~5.7:1 contrast — fine. On a white terminal it
falls to ~2.3:1, well below WCAG AA (4.5:1), and the banner is unreadable
on Terminal.app default profile, GNOME Terminal "Light", solarized-light,
Windows Terminal "Light", VS Code Light theme, etc.

This module asks the terminal what its background is and lets callers
pick a darker accent (``#9a3412``) when the foreground is on white. The
ASCII art and panel layout do not change — only the hex color does.

Detection runs in tiers, ordered by reliability + cost:

    1. ``AMX_THEME=light|dark|auto`` — manual override, highest priority.
    2. ``NO_COLOR`` set or stdout/stdin not a TTY → "unknown" (caller
       should not apply any styling).
    3. ``COLORFGBG`` env var (rxvt/urxvt/Konsole/Terminator family).
    4. Windows Console API (legacy cmd / conhost PowerShell). Only used
       when ``WT_SESSION`` is unset; Windows Terminal supports OSC 11
       directly.
    5. OSC 11 escape query (xterm/iTerm2/Kitty/Alacritty/WezTerm/foot/
       VS Code/Windows Terminal). Skipped on Apple_Terminal — the OS
       Terminal.app does not respond and the timeout is wasted.
    6. Fallback: "dark" (preserves the pre-detection behavior).

The result is memoized for the process lifetime; banner rendering hits
this once. Theme switches mid-session require restarting the CLI.
"""

from __future__ import annotations

import os
import re
import sys
import time
from typing import Literal

# Brand-orange accent for dark/unknown backgrounds (Tailwind orange-400).
ACCENT_DARK_BG = "#fb923c"
# Burnt-orange accent for light backgrounds (Tailwind orange-800).
# Same hue family — preserves brand identity, but lifts contrast on
# white from ~2.3:1 to ~6.4:1 (WCAG AA pass).
ACCENT_LIGHT_BG = "#9a3412"

# Info / secondary cyan for dark backgrounds (Tailwind cyan-400). The
# pre-existing brand teal — unchanged on dark terminals.
INFO_DARK_BG = "#22d3ee"
# Deep teal for light backgrounds (Tailwind cyan-700). Cyan-400 on
# white is ~2.0:1 contrast (unreadable); cyan-700 lands at ~6.0:1
# while staying inside the same hue family.
INFO_LIGHT_BG = "#0e7490"

Background = Literal["light", "dark", "unknown"]

_OSC11_TIMEOUT_S = 0.15

_cached_background: Background | None = None


def detect_background() -> Background:
    """Return the detected terminal background, memoized for the process.

    ``"unknown"`` means we should not apply theme-conditional styling
    (NO_COLOR, non-TTY, piped output).
    """
    global _cached_background
    if _cached_background is None:
        _cached_background = _detect_uncached()
    return _cached_background


def accent_color() -> str:
    """Resolve the WCAG-passing brand accent for the current terminal."""
    bg = detect_background()
    return ACCENT_LIGHT_BG if bg == "light" else ACCENT_DARK_BG


def info_color() -> str:
    """Resolve the WCAG-passing info / secondary cyan for the current terminal."""
    bg = detect_background()
    return INFO_LIGHT_BG if bg == "light" else INFO_DARK_BG


def themed_palette() -> dict[str, str]:
    """Build the Rich theme map with background-aware brand colors.

    Three keys cover everything that touches the brand palette:

    - ``info`` — secondary cyan; used by ``info()`` / ``info_styled()`` /
      ``info_markdown()`` and any inline ``[info]…[/info]`` markup.
    - ``accent`` — non-bold brand orange.
    - ``heading`` — bold brand orange; used by ``heading()`` and panel
      titles.

    The ``success`` / ``warning`` / ``error`` styles intentionally still
    use ANSI palette names so the user's terminal palette controls them
    — the brand-orange / brand-cyan are the only colors AMX hardcodes.
    """
    accent = accent_color()
    return {
        "info": info_color(),
        "accent": accent,
        "heading": f"bold {accent}",
        "success": "bold green",
        "warning": "bold yellow",
        "error": "bold red",
    }


def reset_cache() -> None:
    """Clear the memoized background. Test-only entry point."""
    global _cached_background
    _cached_background = None


def _detect_uncached() -> Background:
    explicit = os.getenv("AMX_THEME", "").strip().lower()
    if explicit in {"light", "dark"}:
        return explicit  # type: ignore[return-value]

    if os.getenv("NO_COLOR") is not None:
        return "unknown"
    try:
        if not (sys.stdout.isatty() and sys.stdin.isatty()):
            return "unknown"
    except (ValueError, OSError):
        return "unknown"

    cfg = _parse_colorfgbg(os.getenv("COLORFGBG"))
    if cfg is not None:
        return cfg

    if os.name == "nt" and not os.getenv("WT_SESSION"):
        win = _query_windows_console()
        if win is not None:
            return win
        return "dark"

    rgb = _query_osc11()
    if rgb is not None:
        return "light" if _relative_luminance(*rgb) > 0.5 else "dark"

    if os.environ.get("TERM_PROGRAM") == "Apple_Terminal":
        rgb = _query_apple_terminal()
        if rgb is not None:
            return "light" if _relative_luminance(*rgb) > 0.5 else "dark"

    return "dark"


def _parse_colorfgbg(raw: str | None) -> Background | None:
    """Map the ``COLORFGBG=fg;bg`` (or ``fg;default;bg``) env var to a theme.

    The trailing field is the background palette index. Indices 7
    (light gray) and 15 (white) indicate a light terminal; 0–6 and 8–14
    indicate dark. Unparseable values return ``None`` so the next tier
    can take over.
    """
    if not raw:
        return None
    parts = [p.strip() for p in raw.split(";") if p.strip()]
    if len(parts) < 2:
        return None
    try:
        bg = int(parts[-1])
    except ValueError:
        return None
    return "light" if bg in {7, 15} else "dark"


def _relative_luminance(r: float, g: float, b: float) -> float:
    """WCAG 2.x relative luminance for sRGB inputs in [0, 1]."""

    def channel(c: float) -> float:
        c = max(0.0, min(1.0, c))
        return c / 12.92 if c <= 0.03928 else ((c + 0.055) / 1.055) ** 2.4

    return 0.2126 * channel(r) + 0.7152 * channel(g) + 0.0722 * channel(b)


_OSC11_RGB_RE = re.compile(r"rgb:([0-9a-fA-F]+)/([0-9a-fA-F]+)/([0-9a-fA-F]+)")


def _query_osc11() -> tuple[float, float, float] | None:
    """Send OSC 11 to the terminal and parse the background-color reply.

    Returns RGB in [0, 1] or ``None`` if the terminal didn't reply
    inside the timeout window. Restores the tty state in a ``finally``
    so a partial reply can't leave the user's shell in raw mode.

    POSIX-only — Windows Terminal also supports OSC 11 but the input
    side requires ``ENABLE_VIRTUAL_TERMINAL_INPUT`` plumbing; the
    Console API tier covers the conhost case and ``AMX_THEME`` covers
    the rest.
    """
    if os.name != "posix":
        return None
    try:
        import select
        import termios
        import tty
    except ImportError:
        return None

    try:
        fd = sys.stdin.fileno()
    except (ValueError, OSError, AttributeError):
        return None

    try:
        old_attrs = termios.tcgetattr(fd)
    except (termios.error, OSError):
        return None

    buf = b""
    try:
        tty.setraw(fd)
        try:
            sys.stdout.write("\x1b]11;?\x07")
            sys.stdout.flush()
        except (OSError, ValueError):
            return None
        deadline = time.monotonic() + _OSC11_TIMEOUT_S
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                ready, _, _ = select.select([fd], [], [], remaining)
            except (OSError, ValueError):
                break
            if not ready:
                break
            try:
                chunk = os.read(fd, 64)
            except OSError:
                break
            if not chunk:
                break
            buf += chunk
            if b"\x07" in buf or b"\x1b\\" in buf:
                break
    finally:
        try:
            termios.tcsetattr(fd, termios.TCSADRAIN, old_attrs)
        except (termios.error, OSError):
            pass

    text = buf.decode("ascii", errors="ignore")
    match = _OSC11_RGB_RE.search(text)
    if not match:
        return None
    return _normalize_rgb_hex(*match.groups())


def _normalize_rgb_hex(r_hex: str, g_hex: str, b_hex: str) -> tuple[float, float, float] | None:
    """Convert xterm-style ``rgb:RRRR/GGGG/BBBB`` triplets to [0, 1].

    Each channel may be 1, 2, 4, 8, 12, or 16 hex digits. Truncate or
    left-pad to 4 digits before scaling so the math is predictable.
    """
    out: list[float] = []
    for hex_str in (r_hex, g_hex, b_hex):
        if not hex_str:
            return None
        s = hex_str.ljust(4, "0")[:4]
        try:
            value = int(s, 16) / 0xFFFF
        except ValueError:
            return None
        out.append(value)
    return out[0], out[1], out[2]


_APPLESCRIPT_BG_QUERY = (
    'tell application "Terminal" to get background color '
    "of selected tab of window 1"
)
_APPLESCRIPT_TIMEOUT_S = 0.5


def _query_apple_terminal() -> tuple[float, float, float] | None:
    """Read Terminal.app's active-tab background via AppleScript.

    Terminal.app is the one mainstream macOS terminal that does not
    answer OSC 11. AppleScript is its supported public API for the
    current background color and works reliably on every macOS that
    ships Terminal.app — at the cost of one ``osascript`` subprocess
    (~50–200 ms) and a one-time Automation permission prompt the
    first time AMX runs in a given Terminal.app session.

    Returns RGB in [0, 1] or ``None`` when the user denied automation,
    no Terminal.app window is open, or osascript isn't available.
    """
    try:
        import subprocess
    except ImportError:
        return None
    try:
        result = subprocess.run(
            ["osascript", "-e", _APPLESCRIPT_BG_QUERY],
            capture_output=True,
            text=True,
            timeout=_APPLESCRIPT_TIMEOUT_S,
            check=False,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        return None
    if result.returncode != 0:
        return None
    parts = [p.strip() for p in result.stdout.strip().split(",")]
    if len(parts) != 3:
        return None
    try:
        # AppleScript returns 16-bit unsigned color components (0–65535).
        return tuple(int(p) / 65535 for p in parts)  # type: ignore[return-value]
    except ValueError:
        return None


_WIN_LIGHT_BG_INDICES = {7, 11, 14, 15}


def _query_windows_console() -> Background | None:
    """Read the legacy console buffer's background attribute via ctypes.

    Only meaningful on Windows when the user is on classic conhost
    (cmd, legacy PowerShell). Windows Terminal renders its own theme
    on top of these attributes, so we skip this path when
    ``WT_SESSION`` is set.
    """
    if os.name != "nt":
        return None
    try:
        import ctypes
        from ctypes import wintypes
    except Exception:
        return None

    STD_OUTPUT_HANDLE = -11

    class COORD(ctypes.Structure):
        _fields_ = [("X", wintypes.SHORT), ("Y", wintypes.SHORT)]

    class SMALL_RECT(ctypes.Structure):
        _fields_ = [
            ("Left", wintypes.SHORT),
            ("Top", wintypes.SHORT),
            ("Right", wintypes.SHORT),
            ("Bottom", wintypes.SHORT),
        ]

    class CONSOLE_SCREEN_BUFFER_INFO(ctypes.Structure):
        _fields_ = [
            ("dwSize", COORD),
            ("dwCursorPosition", COORD),
            ("wAttributes", wintypes.WORD),
            ("srWindow", SMALL_RECT),
            ("dwMaximumWindowSize", COORD),
        ]

    try:
        kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
        handle = kernel32.GetStdHandle(STD_OUTPUT_HANDLE)
        if handle in (None, 0, ctypes.c_void_p(-1).value):
            return None
        info = CONSOLE_SCREEN_BUFFER_INFO()
        ok = kernel32.GetConsoleScreenBufferInfo(handle, ctypes.byref(info))
        if not ok:
            return None
        bg_index = (info.wAttributes >> 4) & 0x0F
    except Exception:
        return None

    return "light" if bg_index in _WIN_LIGHT_BG_INDICES else "dark"
