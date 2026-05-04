"""Shared Rich console and interaction helpers."""

from __future__ import annotations

import os
import threading
import time
from collections.abc import Generator
from contextlib import contextmanager, suppress
from typing import Any

from prompt_toolkit import prompt as pt_prompt
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit.key_binding import KeyBindings, merge_key_bindings
from rich import box
from rich.align import Align
from rich.console import Console
from rich.markup import escape as _markup_escape
from rich.panel import Panel
from rich.progress import BarColumn, MofNCompleteColumn, Progress, TextColumn, TimeElapsedColumn
from rich.table import Table
from rich.text import Text
from rich.theme import Theme

_theme = Theme(
    {
        "info": "cyan",
        "success": "bold green",
        "warning": "bold yellow",
        "error": "bold red",
        "heading": "bold magenta",
    }
)

_real_console = Console(theme=_theme)
# Long-lived /dev/null sink the proxy routes to while a thread is in
# ``quiet_console()``. Process-lifetime resource — closing it via a
# ``with`` block would defeat the proxy, so SIM115 is suppressed.
_devnull_handle = open(os.devnull, "w")  # noqa: SIM115
_null_console = Console(file=_devnull_handle, theme=_theme, force_terminal=False)

_BANNER_SHOWN = False

# Per-thread "quiet" flag. The visualizer's run worker turns this on
# before invoking the orchestrator so the rich CLI banners + table
# previews don't bleed into the user's terminal — they belong on the
# SSE stream the browser is subscribed to. ``info`` / ``success`` /
# ``warn`` / ``error`` short-circuit when the flag is set; the
# original CLI REPL thread leaves it unset and keeps full output.
_thread_quiet = threading.local()


def is_quiet() -> bool:
    return bool(getattr(_thread_quiet, "value", False))


class _ConsoleProxy:
    """Forward attribute access to the real console, except when the
    current thread is in quiet mode — then route everything to a
    null sink so Rich panels / tables / plain prints disappear.

    Dunders that Python looks up on the class (not via ``__getattr__``)
    have to be forwarded explicitly. Rich's ``Live`` uses
    ``with self.console:`` so context-manager support is mandatory.
    """

    def _target(self) -> Console:
        return _null_console if is_quiet() else _real_console

    def __getattr__(self, name: str) -> Any:
        return getattr(self._target(), name)

    def __enter__(self) -> Any:
        return self._target().__enter__()

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> Any:
        return self._target().__exit__(exc_type, exc, tb)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self._target()(*args, **kwargs)

    def __repr__(self) -> str:  # pragma: no cover - cosmetic
        return f"<ConsoleProxy quiet={is_quiet()}>"


# Module-level alias preserves every existing ``from amx.utils.console
# import console`` call. Existing CLI threads see the real console;
# the visualizer worker sees the null one for the duration of
# ``quiet_console()``.
console: Any = _ConsoleProxy()


@contextmanager
def quiet_console() -> Generator[None, None, None]:
    """Suppress info/success/warn/error printing + Rich console output
    for the current thread.

    Used by the visualizer's headless run worker to keep the parent
    CLI's terminal clean while the browser streams the same events
    over SSE.
    """
    previous = getattr(_thread_quiet, "value", False)
    _thread_quiet.value = True
    try:
        yield
    finally:
        _thread_quiet.value = previous


def show_banner(force: bool = False) -> None:
    """Render AMX startup banner once per process.

    Two tiers — tagline above the ASCII art, one-liner footer below.
    Box-drawing brackets ``┃ ... ┃`` substitute for the previous
    asterisks so the framing matches the Unicode block of the ASCII
    art instead of mixing vector glyphs with grid art.

    The version is intentionally NOT shown here. The "AMX Interactive
    Session" info block (rendered by the session module) already lists
    ``Version`` alongside the runtime Config / Database / LLM context;
    duplicating it in the banner produces visible noise without adding
    information. Banner = identity; session info = runtime state.
    """
    global _BANNER_SHOWN
    if _BANNER_SHOWN and not force:
        return
    if os.getenv("AMX_NO_BANNER", "").lower() in {"1", "true", "yes"}:
        return

    tagline = Text("┃  Agentic Metadata Extractor  ┃", style="bold cyan")
    art = Text(
        "\n".join(
            [
                " █████╗ ███╗   ███╗██╗  ██╗",
                "██╔══██╗████╗ ████║╚██╗██╔╝",
                "███████║██╔████╔██║ ╚███╔╝ ",
                "██╔══██║██║╚██╔╝██║ ██╔██╗ ",
                "██║  ██║██║ ╚═╝ ██║██╔╝ ██╗",
                "╚═╝  ╚═╝╚═╝     ╚═╝╚═╝  ╚═╝",
            ]
        ),
        style="bold bright_cyan",
    )
    footer = Text("AI-inferred database descriptions", style="cyan")

    content = Text.assemble(
        tagline,
        "\n\n",
        art,
        "\n\n",
        footer,
        justify="center",
    )
    console.print(
        Panel(
            Align.center(content),
            border_style="bright_cyan",
            box=box.DOUBLE_EDGE,
            padding=(1, 2),
        )
    )
    _BANNER_SHOWN = True


# All five helpers escape *text* before splicing it into a Rich markup
# string. Without escape() a substring like ``[databricks]`` (the kind
# that appears inside ``pip install 'amx-cli[databricks]'`` install hints)
# is interpreted by Rich as another style tag and silently dropped, so
# the user sees ``pip install 'amx-cli'`` and concludes the package is
# broken. Every existing callsite passes plain text intended for users,
# so escaping is always the right behaviour.
def heading(text: str) -> None:
    console.print(Panel(f"[heading]{_markup_escape(text)}[/heading]", expand=False))


def info(text: str) -> None:
    if is_quiet():
        return
    console.print(f"[info]ℹ  {_markup_escape(text)}[/info]")


def success(text: str) -> None:
    if is_quiet():
        return
    console.print(f"[success]✓  {_markup_escape(text)}[/success]")


def warn(text: str) -> None:
    if is_quiet():
        return
    console.print(f"[warning]⚠  {_markup_escape(text)}[/warning]")


def error(text: str) -> None:
    if is_quiet():
        return
    console.print(f"[error]✗  {_markup_escape(text)}[/error]")


@contextmanager
def _live_paused_for_input() -> Generator[None, None, None]:
    """Pause an active ``LiveDisplay`` so prompt_toolkit can echo keystrokes.

    The Rich ``Live`` region runs at 10 Hz and overwrites whatever the user
    types between frames — so the user can press '2', press Enter, and the
    selection works, but they never SEE their keystroke. We pause the live
    region while waiting for input and resume it after.

    No-op when there's no active display, so non-interactive callers (and
    code paths outside ``command_display``) don't pay any cost.
    """
    paused = False
    display = None
    try:
        # Lazy import — ``utils/console`` must not import ``utils/live_display``
        # at module load time (would create a cycle through ``step_spinner``).
        from amx.utils.live_display import get_display

        display = get_display()
        if display.is_active:
            display.pause()
            paused = True
    except Exception:
        display = None
    try:
        yield
    finally:
        if paused and display is not None:
            with suppress(Exception):
                display.resume()


class PromptCancelled(Exception):
    """Raised when the user soft-cancels a prompt with Esc.

    Distinguishes "user explicitly cancelled" (Esc) from "user
    submitted an empty answer" (Enter on an empty buffer) and from
    "user killed the whole session" (Ctrl-C → KeyboardInterrupt).

    The prompt helpers below catch this internally, print a one-line
    "Cancelled." note so the user sees the keystroke landed, and
    return the appropriate "no answer" sentinel for that helper
    (empty string / empty list / False). Callers that need to tell
    cancel apart from empty-Enter can wrap a call in their own
    try/except — but in practice the same outcome works for both.
    """


# Esc binding shared by every prompt this module dispatches via
# ``_safe_pt_prompt``. Bound without ``eager=True`` so prompt_toolkit's
# input parser can still distinguish a bare Esc from Esc-prefixed
# multi-key sequences (arrow keys, Alt-shortcuts, function keys all
# start with Esc on most terminals). After the parser's small
# disambiguation window elapses the bare-Esc branch fires and the
# prompt closes via the ``PromptCancelled`` exception.
_ESC_CANCEL_BINDINGS = KeyBindings()


@_ESC_CANCEL_BINDINGS.add("escape")
def _on_escape_cancel(event: Any) -> None:
    event.app.exit(exception=PromptCancelled())


def _safe_pt_prompt(*args: Any, **kwargs: Any) -> str:
    """``prompt_toolkit.prompt`` wrapper that pauses the live display first.

    Also installs a global Esc-to-cancel binding so users can soft-
    cancel any prompt without having to fall back to Ctrl-C (which
    kills the whole session). Callers that pass their own
    ``key_bindings`` are merged with the cancel binding rather than
    replaced.
    """
    user_bindings = kwargs.pop("key_bindings", None)
    if user_bindings is None:
        kwargs["key_bindings"] = _ESC_CANCEL_BINDINGS
    else:
        kwargs["key_bindings"] = merge_key_bindings([user_bindings, _ESC_CANCEL_BINDINGS])
    with _live_paused_for_input():
        return pt_prompt(*args, **kwargs)


def ask(question: str, default: str = "") -> str:
    """Free-text prompt. Esc soft-cancels and returns ''."""
    try:
        return _safe_pt_prompt(f"  {question}: ", default=default).strip()
    except PromptCancelled:
        info("Cancelled.")
        return ""


def ask_password(question: str) -> str:
    """Hidden prompt for secrets. Esc soft-cancels and returns ''."""
    try:
        return _safe_pt_prompt(f"  {question}: ", is_password=True).strip()
    except PromptCancelled:
        info("Cancelled.")
        return ""


def ask_choice(
    question: str,
    choices: list[str],
    default: str = "",
    descriptions: dict[str, str] | None = None,
) -> str:
    """Prompt for a single choice. Type 1–N or a matching label. Enter accepts the default.

    *descriptions* is an optional ``{choice: one-line-description}`` mapping shown next to each option.
    The input line is never pre-filled with the default text (so you can type ``2`` immediately).
    """
    if not choices:
        return default
    completer = WordCompleter(choices, ignore_case=True)
    console.print(f"  [info]{question}[/info]")
    for i, c in enumerate(choices, 1):
        mark = " — default (Enter)" if default and c == default else ""
        desc = f"  [dim]{descriptions[c]}[/dim]" if descriptions and c in descriptions else ""
        console.print(f"    {i}. [bold]{c}[/bold]{desc}[dim]{mark}[/dim]")
    # Keep the prompt minimal: users can press Enter for default without extra hint text.
    # Do not pass default= to pt_prompt — it pre-fills the whole string and forces delete-before-2.
    try:
        answer = _safe_pt_prompt("  > ", completer=completer).strip()
    except PromptCancelled:
        # Esc → return the empty string so callers detect "no choice
        # made" the same way they handle invalid input. Print a note
        # so the keystroke does not feel like a no-op.
        info("Cancelled.")
        return ""
    if not answer:
        return default if default in choices else ""
    if answer.isdigit() and 1 <= int(answer) <= len(choices):
        return choices[int(answer) - 1]
    if answer in choices:
        return answer
    return default if default in choices else ""


def ask_multi_choice(question: str, choices: list[str]) -> list[str]:
    console.print(f"  [info]{question}[/info]")
    console.print(
        "  (comma-separated numbers or names; `all` = everything; "
        "Enter alone cancels — no accidental 'run on every table'; Esc cancels too)"
    )
    for i, c in enumerate(choices, 1):
        console.print(f"    {i}. {c}")
    try:
        raw = _safe_pt_prompt("  > ").strip()
    except PromptCancelled:
        info("Cancelled.")
        return []
    if not raw:
        return []
    if raw.lower() == "all":
        return choices
    selected: list[str] = []
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        if token.isdigit() and 1 <= int(token) <= len(choices):
            selected.append(choices[int(token) - 1])
            continue
        if token in choices:
            selected.append(token)
            continue
        lower_matches = [c for c in choices if c.lower() == token.lower()]
        if len(lower_matches) == 1:
            selected.append(lower_matches[0])
            continue
        pref = [c for c in choices if c.lower().startswith(token.lower())]
        if len(pref) == 1:
            selected.append(pref[0])
            continue
        sub = [c for c in choices if token.lower() in c.lower()]
        if len(sub) == 1:
            selected.append(sub[0])
            continue
    if not selected:
        warn(f"No option matched {raw!r}. Use numbers from the list, exact names, or `all`.")
    return selected


def confirm(question: str, default: bool = True) -> bool:
    """Yes/no prompt. Esc soft-cancels and returns False (treated as 'no').

    The Esc-as-False convention is deliberate: every confirm() call in
    AMX is a destructive or scoping decision (disable shared mode?
    apply comments? proceed despite warnings?). False — "do not
    proceed" — is the safe default for an explicit cancel.
    """
    suffix = " [Y/n]" if default else " [y/N]"
    try:
        answer = _safe_pt_prompt(f"  {question}{suffix}: ").strip().lower()
    except PromptCancelled:
        info("Cancelled.")
        return False
    if not answer:
        return default
    return answer in ("y", "yes")


def render_table(title: str, columns: list[str], rows: list[list[Any]]) -> None:
    table = Table(title=title, show_lines=True)
    for col in columns:
        table.add_column(col, style="cyan")
    for row in rows:
        table.add_row(*[str(v) for v in row])
    console.print(table)


# ── Progress / spinner helpers ──────────────────────────────────────────────


@contextmanager
def step_spinner(
    label: str,
    *,
    token_estimate: int | None = None,
    done_message: str | None = None,
) -> Generator[None, None, None]:
    from amx.utils.live_display import get_display

    display = get_display()

    if display.is_active:
        idx = display.add_activity(label, token_estimate=token_estimate or 0)
        display.begin_activity(idx)
        display.set_thinking(label)
        try:
            yield
        except Exception:
            display.stop_thinking()
            display.fail_activity(idx)
            raise
        else:
            display.stop_thinking()
            msg = done_message or label
            display.complete_activity(idx, detail=msg)
    else:
        tok = f" (~{token_estimate:,} input tokens)" if token_estimate else ""
        t0 = time.monotonic()
        with console.status(f"[info]{label}{tok}[/info]", spinner="dots") as status:
            stop_evt = threading.Event()

            def _refresh_elapsed() -> None:
                while not stop_evt.is_set():
                    elapsed_now = time.monotonic() - t0
                    status.update(f"[info]{label}{tok} ({elapsed_now:.1f}s)[/info]")
                    stop_evt.wait(0.1)

            tick = threading.Thread(target=_refresh_elapsed, daemon=True)
            tick.start()
            try:
                yield
            finally:
                stop_evt.set()
                tick.join(timeout=0.2)
                elapsed = time.monotonic() - t0
                msg = done_message or label
                status.update(f"[success]✓ {msg} ({elapsed:.1f}s)[/success]")
        success(f"{msg} ({elapsed:.1f}s)")


@contextmanager
def file_progress(total: int, label: str = "Scanning files") -> Generator[Progress, None, None]:
    """Rich progress bar for iterating over files."""
    progress = Progress(
        TextColumn("[info]{task.description}[/info]"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        console=console,
        transient=True,
    )
    with progress:
        task = progress.add_task(label, total=total)
        progress._amx_task_id = task  # type: ignore[attr-defined]
        yield progress


def advance_file_progress(progress: Progress, filename: str = "") -> None:
    """Advance the file progress bar by one step."""
    task_id = getattr(progress, "_amx_task_id", None)
    if task_id is not None:
        if filename:
            progress.update(task_id, description=f"Scanning: {filename}")
        progress.advance(task_id)


def render_token_summary(tracker: object) -> None:
    """Render a Rich table summarising per-step token usage."""
    from amx.utils.token_tracker import TokenTracker

    if not isinstance(tracker, TokenTracker) or not tracker.has_records:
        return
    rows = tracker.summary()
    table = Table(title="Token usage", show_lines=True, box=box.SIMPLE_HEAVY)
    table.add_column("Step", style="cyan")
    table.add_column("Input", justify="right")
    table.add_column("Output", justify="right")
    table.add_column("Total", justify="right", style="bold")
    tot_in = tot_out = tot_all = 0
    for step, inp, out, total in rows:
        table.add_row(step, f"{inp:,}", f"{out:,}", f"{total:,}")
        tot_in += inp
        tot_out += out
        tot_all += total
    table.add_section()
    table.add_row(
        "[bold]TOTAL[/bold]",
        f"[bold]{tot_in:,}[/bold]",
        f"[bold]{tot_out:,}[/bold]",
        f"[bold]{tot_all:,}[/bold]",
    )
    console.print(table)
