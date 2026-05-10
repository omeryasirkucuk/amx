"""Tests for terminal background detection.

The detection runs once per process and caches; every test resets the
cache via the ``_reset`` autouse fixture so ordering can't make a stale
result leak from one test into the next. Real terminal I/O (OSC 11
escape, Windows Console API ctypes call) is monkeypatched to keep the
suite hermetic — touching ``/dev/tty`` or kernel32 from inside pytest
would either flake on CI or open a permission dialog on macOS.
"""

from __future__ import annotations

import pytest

from amx.utils import terminal_theme as tt


@pytest.fixture(autouse=True)
def _reset() -> None:
    tt.reset_cache()


@pytest.fixture
def _isolate_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Strip every env var that the pipeline consults, so individual
    tests opt back in to exactly the signal they want to assert on."""
    for name in (
        "AMX_THEME",
        "NO_COLOR",
        "COLORFGBG",
        "WT_SESSION",
        "TERM_PROGRAM",
    ):
        monkeypatch.delenv(name, raising=False)


@pytest.fixture
def _force_tty(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make stdout / stdin look like a tty so the non-TTY short-circuit
    doesn't fire when the test is itself running under pytest's capture."""
    monkeypatch.setattr("sys.stdout.isatty", lambda: True)
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)


# ── _parse_colorfgbg ─────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("15;0", "dark"),
        ("0;15", "light"),
        ("7;0", "dark"),
        ("0;7", "light"),
        ("15;default;0", "dark"),
        ("0;default;15", "light"),
        ("0;1", "dark"),
        ("0;8", "dark"),
    ],
)
def test_parse_colorfgbg_known_values(raw: str, expected: str) -> None:
    assert tt._parse_colorfgbg(raw) == expected


@pytest.mark.parametrize("raw", [None, "", "garbage", "15", "15;default", "15;abc"])
def test_parse_colorfgbg_unparseable_returns_none(raw: str | None) -> None:
    assert tt._parse_colorfgbg(raw) is None


# ── _relative_luminance ──────────────────────────────────────────────────────


def test_relative_luminance_endpoints() -> None:
    assert tt._relative_luminance(0.0, 0.0, 0.0) == pytest.approx(0.0)
    assert tt._relative_luminance(1.0, 1.0, 1.0) == pytest.approx(1.0)


def test_relative_luminance_white_is_light_black_is_dark() -> None:
    assert tt._relative_luminance(1.0, 1.0, 1.0) > 0.5
    assert tt._relative_luminance(0.0, 0.0, 0.0) < 0.5


def test_relative_luminance_clamps_out_of_range() -> None:
    # Should not throw and should treat negatives as 0, > 1 as 1.
    assert tt._relative_luminance(-0.5, -0.5, -0.5) == pytest.approx(0.0)
    assert tt._relative_luminance(2.0, 2.0, 2.0) == pytest.approx(1.0)


# ── _normalize_rgb_hex ───────────────────────────────────────────────────────


def test_normalize_rgb_hex_4digit() -> None:
    r, g, b = tt._normalize_rgb_hex("ffff", "ffff", "ffff")
    assert (r, g, b) == (1.0, 1.0, 1.0)


def test_normalize_rgb_hex_2digit_padded() -> None:
    # Terminal.app-style "rgb:ff/ff/ff" should still resolve to white.
    r, g, b = tt._normalize_rgb_hex("ff", "ff", "ff")
    # "ff" left-pads to "ff00" → 0xff00/0xffff ≈ 0.996 (close to white,
    # well above the 0.5 light/dark threshold). The whole point of the
    # test is that the function tolerates the short form without crashing.
    assert b > 0.99
    assert g > 0.99
    assert r > 0.99


def test_normalize_rgb_hex_empty_returns_none() -> None:
    assert tt._normalize_rgb_hex("", "ffff", "ffff") is None


# ── _query_apple_terminal ────────────────────────────────────────────────────


class _FakeProcess:
    def __init__(self, *, stdout: str = "", returncode: int = 0) -> None:
        self.stdout = stdout
        self.returncode = returncode


def test_apple_terminal_parses_white_profile(monkeypatch: pytest.MonkeyPatch) -> None:
    """``Novel`` / ``Basic`` profiles return ``65535, 65535, 65535``-ish."""

    def fake_run(*args: object, **kwargs: object) -> _FakeProcess:
        return _FakeProcess(stdout="65535, 64751, 56541\n", returncode=0)

    monkeypatch.setattr("subprocess.run", fake_run)
    rgb = tt._query_apple_terminal()
    assert rgb is not None
    assert all(v > 0.85 for v in rgb)
    assert tt._relative_luminance(*rgb) > 0.5


def test_apple_terminal_parses_dark_profile(monkeypatch: pytest.MonkeyPatch) -> None:
    """``Pro`` profile returns small RGB values."""

    def fake_run(*args: object, **kwargs: object) -> _FakeProcess:
        return _FakeProcess(stdout="5866, 5866, 5866\n", returncode=0)

    monkeypatch.setattr("subprocess.run", fake_run)
    rgb = tt._query_apple_terminal()
    assert rgb is not None
    assert tt._relative_luminance(*rgb) < 0.5


def test_apple_terminal_returns_none_on_nonzero_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Permission denied / Terminal.app closed surface as a non-zero exit."""

    def fake_run(*args: object, **kwargs: object) -> _FakeProcess:
        return _FakeProcess(stdout="", returncode=1)

    monkeypatch.setattr("subprocess.run", fake_run)
    assert tt._query_apple_terminal() is None


def test_apple_terminal_returns_none_on_malformed_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run(*args: object, **kwargs: object) -> _FakeProcess:
        return _FakeProcess(stdout="not a triple\n", returncode=0)

    monkeypatch.setattr("subprocess.run", fake_run)
    assert tt._query_apple_terminal() is None


def test_apple_terminal_returns_none_when_osascript_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run(*args: object, **kwargs: object) -> _FakeProcess:
        raise FileNotFoundError("osascript not on PATH")

    monkeypatch.setattr("subprocess.run", fake_run)
    assert tt._query_apple_terminal() is None


# ── detect_background priority order ─────────────────────────────────────────


def test_amx_theme_explicit_light_wins_over_everything(
    monkeypatch: pytest.MonkeyPatch,
    _isolate_env: None,
) -> None:
    monkeypatch.setenv("AMX_THEME", "light")
    monkeypatch.setenv("COLORFGBG", "15;0")  # would say dark
    monkeypatch.setenv("NO_COLOR", "1")  # would say unknown
    assert tt.detect_background() == "light"


def test_amx_theme_explicit_dark_wins(
    monkeypatch: pytest.MonkeyPatch,
    _isolate_env: None,
) -> None:
    monkeypatch.setenv("AMX_THEME", "dark")
    monkeypatch.setenv("COLORFGBG", "0;15")  # would say light
    assert tt.detect_background() == "dark"


def test_amx_theme_auto_falls_through(
    monkeypatch: pytest.MonkeyPatch,
    _isolate_env: None,
    _force_tty: None,
) -> None:
    monkeypatch.setenv("AMX_THEME", "auto")
    monkeypatch.setenv("COLORFGBG", "0;15")
    assert tt.detect_background() == "light"


def test_no_color_returns_unknown_even_on_tty(
    monkeypatch: pytest.MonkeyPatch,
    _isolate_env: None,
    _force_tty: None,
) -> None:
    monkeypatch.setenv("NO_COLOR", "1")
    monkeypatch.setenv("COLORFGBG", "0;15")
    assert tt.detect_background() == "unknown"


def test_non_tty_returns_unknown(
    monkeypatch: pytest.MonkeyPatch,
    _isolate_env: None,
) -> None:
    monkeypatch.setattr("sys.stdout.isatty", lambda: False)
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)
    monkeypatch.setenv("COLORFGBG", "0;15")  # would say light if we got that far
    assert tt.detect_background() == "unknown"


def test_colorfgbg_used_when_set_and_tty(
    monkeypatch: pytest.MonkeyPatch,
    _isolate_env: None,
    _force_tty: None,
) -> None:
    monkeypatch.setenv("COLORFGBG", "0;15")
    # Ensure OSC 11 path isn't reached — if it were, this assertion
    # would still pass by coincidence, but the test would be brittle.
    monkeypatch.setattr(tt, "_query_osc11", lambda: pytest.fail("OSC 11 reached"))
    assert tt.detect_background() == "light"


def test_apple_terminal_falls_back_to_applescript_when_osc11_silent(
    monkeypatch: pytest.MonkeyPatch,
    _isolate_env: None,
    _force_tty: None,
) -> None:
    monkeypatch.setenv("TERM_PROGRAM", "Apple_Terminal")
    monkeypatch.setattr(tt, "_query_osc11", lambda: None)
    monkeypatch.setattr(tt, "_query_apple_terminal", lambda: (1.0, 1.0, 1.0))
    monkeypatch.setattr("os.name", "posix", raising=False)
    assert tt.detect_background() == "light"


def test_apple_terminal_uses_osc11_when_it_responds(
    monkeypatch: pytest.MonkeyPatch,
    _isolate_env: None,
    _force_tty: None,
) -> None:
    """If a future Terminal.app build starts answering OSC 11, the
    AppleScript subprocess should be skipped — the cheap path wins."""
    monkeypatch.setenv("TERM_PROGRAM", "Apple_Terminal")
    monkeypatch.setattr(tt, "_query_osc11", lambda: (1.0, 1.0, 1.0))
    monkeypatch.setattr(
        tt,
        "_query_apple_terminal",
        lambda: pytest.fail("AppleScript reached even though OSC 11 succeeded"),
    )
    monkeypatch.setattr("os.name", "posix", raising=False)
    assert tt.detect_background() == "light"


def test_non_apple_terminal_does_not_call_applescript(
    monkeypatch: pytest.MonkeyPatch,
    _isolate_env: None,
    _force_tty: None,
) -> None:
    monkeypatch.setenv("TERM_PROGRAM", "iTerm.app")
    monkeypatch.setattr(tt, "_query_osc11", lambda: None)
    monkeypatch.setattr(
        tt,
        "_query_apple_terminal",
        lambda: pytest.fail("AppleScript reached for non-Apple_Terminal"),
    )
    monkeypatch.setattr("os.name", "posix", raising=False)
    assert tt.detect_background() == "dark"


def test_apple_terminal_applescript_returns_none_falls_back_to_dark(
    monkeypatch: pytest.MonkeyPatch,
    _isolate_env: None,
    _force_tty: None,
) -> None:
    """Permission denied / Terminal.app closed → dark fallback so
    AMX_THEME=light remains the documented escape hatch."""
    monkeypatch.setenv("TERM_PROGRAM", "Apple_Terminal")
    monkeypatch.setattr(tt, "_query_osc11", lambda: None)
    monkeypatch.setattr(tt, "_query_apple_terminal", lambda: None)
    monkeypatch.setattr("os.name", "posix", raising=False)
    assert tt.detect_background() == "dark"


def test_osc11_white_response_resolves_to_light(
    monkeypatch: pytest.MonkeyPatch,
    _isolate_env: None,
    _force_tty: None,
) -> None:
    monkeypatch.setattr(tt, "_query_osc11", lambda: (1.0, 1.0, 1.0))
    monkeypatch.setattr("os.name", "posix", raising=False)
    assert tt.detect_background() == "light"


def test_osc11_black_response_resolves_to_dark(
    monkeypatch: pytest.MonkeyPatch,
    _isolate_env: None,
    _force_tty: None,
) -> None:
    monkeypatch.setattr(tt, "_query_osc11", lambda: (0.0, 0.0, 0.0))
    monkeypatch.setattr("os.name", "posix", raising=False)
    assert tt.detect_background() == "dark"


def test_osc11_no_reply_falls_back_to_dark(
    monkeypatch: pytest.MonkeyPatch,
    _isolate_env: None,
    _force_tty: None,
) -> None:
    monkeypatch.setattr(tt, "_query_osc11", lambda: None)
    monkeypatch.setattr("os.name", "posix", raising=False)
    assert tt.detect_background() == "dark"


# ── Cross-platform Windows path ──────────────────────────────────────────────


def test_windows_legacy_calls_console_api(
    monkeypatch: pytest.MonkeyPatch,
    _isolate_env: None,
    _force_tty: None,
) -> None:
    monkeypatch.setattr("os.name", "nt", raising=False)
    monkeypatch.setattr(tt, "_query_windows_console", lambda: "light")
    # OSC 11 path must not be reached on legacy Windows conhost.
    monkeypatch.setattr(tt, "_query_osc11", lambda: pytest.fail("OSC 11 reached"))
    assert tt.detect_background() == "light"


def test_windows_terminal_skips_console_api(
    monkeypatch: pytest.MonkeyPatch,
    _isolate_env: None,
    _force_tty: None,
) -> None:
    monkeypatch.setattr("os.name", "nt", raising=False)
    monkeypatch.setenv("WT_SESSION", "fake-session-guid")
    monkeypatch.setattr(tt, "_query_windows_console", lambda: pytest.fail("Console API reached"))
    monkeypatch.setattr(tt, "_query_osc11", lambda: (1.0, 1.0, 1.0))
    assert tt.detect_background() == "light"


# ── accent_color resolver ────────────────────────────────────────────────────


def test_accent_color_dark_returns_orange_400(
    monkeypatch: pytest.MonkeyPatch,
    _isolate_env: None,
) -> None:
    monkeypatch.setenv("AMX_THEME", "dark")
    assert tt.accent_color() == tt.ACCENT_DARK_BG == "#fb923c"


def test_accent_color_light_returns_orange_800(
    monkeypatch: pytest.MonkeyPatch,
    _isolate_env: None,
) -> None:
    monkeypatch.setenv("AMX_THEME", "light")
    assert tt.accent_color() == tt.ACCENT_LIGHT_BG == "#9a3412"


def test_accent_color_unknown_falls_back_to_dark_accent(
    monkeypatch: pytest.MonkeyPatch,
    _isolate_env: None,
    _force_tty: None,
) -> None:
    monkeypatch.setenv("NO_COLOR", "1")
    # NO_COLOR yields "unknown"; the resolver picks the dark accent so
    # the brand color stays consistent rather than disappearing.
    assert tt.accent_color() == tt.ACCENT_DARK_BG


# ── Cache behavior ───────────────────────────────────────────────────────────


def test_detect_is_memoized_across_calls(
    monkeypatch: pytest.MonkeyPatch,
    _isolate_env: None,
) -> None:
    monkeypatch.setenv("AMX_THEME", "light")
    first = tt.detect_background()
    monkeypatch.setenv("AMX_THEME", "dark")
    second = tt.detect_background()
    assert first == second == "light"


def test_reset_cache_re_detects(
    monkeypatch: pytest.MonkeyPatch,
    _isolate_env: None,
) -> None:
    monkeypatch.setenv("AMX_THEME", "light")
    assert tt.detect_background() == "light"
    tt.reset_cache()
    monkeypatch.setenv("AMX_THEME", "dark")
    assert tt.detect_background() == "dark"
