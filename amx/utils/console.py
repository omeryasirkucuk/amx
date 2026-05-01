"""Shared Rich console and interaction helpers."""

from __future__ import annotations

import os
import threading
import time
from contextlib import contextmanager
from typing import Any, Generator

from prompt_toolkit import prompt as pt_prompt
from rich import box
from rich.align import Align
from prompt_toolkit.completion import WordCompleter
from rich.console import Console
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

console = Console(theme=_theme)

_BANNER_SHOWN = False


def show_banner(force: bool = False) -> None:
    """Render AMX startup banner once per process.

    The banner has three tiers — tagline → ASCII art → version+url —
    each on its own line and rendered in a slightly different cyan
    tint so the eye can pick out the hierarchy. The tagline replaces
    the previous redundant "Metadata Extraction System" subtitle (the
    title was already saying the same thing); the version line gives
    open-source users a quick "what version am I on" + project URL.

    Box-drawing brackets ``┃ ... ┃`` substitute for the previous
    asterisks so the framing matches the terminal-native font of the
    rest of the banner instead of mixing vector glyphs with grid art.
    """
    global _BANNER_SHOWN
    if _BANNER_SHOWN and not force:
        return
    if os.getenv("AMX_NO_BANNER", "").lower() in {"1", "true", "yes"}:
        return

    # Lazy import keeps utils.console free of a hard dependency on the
    # top-level package — important because show_banner is called from
    # cli.py during early startup.
    try:
        from amx import __version__ as _amx_version
    except Exception:
        _amx_version = ""

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
    version_label = f"v{_amx_version}" if _amx_version else "development build"
    footer = Text.assemble(
        (version_label, "bold cyan"),
        ("  •  ", "dim cyan"),
        ("AI-inferred database descriptions", "cyan"),
    )

    content = Text.assemble(
        tagline, "\n\n",
        art, "\n\n",
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


def heading(text: str) -> None:
    console.print(Panel(f"[heading]{text}[/heading]", expand=False))


def info(text: str) -> None:
    console.print(f"[info]ℹ  {text}[/info]")


def success(text: str) -> None:
    console.print(f"[success]✓  {text}[/success]")


def warn(text: str) -> None:
    console.print(f"[warning]⚠  {text}[/warning]")


def error(text: str) -> None:
    console.print(f"[error]✗  {text}[/error]")


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
            try:
                display.resume()
            except Exception:
                pass


def _safe_pt_prompt(*args: Any, **kwargs: Any) -> str:
    """``prompt_toolkit.prompt`` wrapper that pauses the live display first."""
    with _live_paused_for_input():
        return pt_prompt(*args, **kwargs)


def ask(question: str, default: str = "") -> str:
    return _safe_pt_prompt(f"  {question}: ", default=default).strip()


def ask_password(question: str) -> str:
    return _safe_pt_prompt(f"  {question}: ", is_password=True).strip()


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
    answer = _safe_pt_prompt("  > ", completer=completer).strip()
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
        "Enter alone cancels — no accidental 'run on every table')"
    )
    for i, c in enumerate(choices, 1):
        console.print(f"    {i}. {c}")
    raw = _safe_pt_prompt("  > ").strip()
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
    suffix = " [Y/n]" if default else " [y/N]"
    answer = _safe_pt_prompt(f"  {question}{suffix}: ").strip().lower()
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
    table.add_row("[bold]TOTAL[/bold]", f"[bold]{tot_in:,}[/bold]", f"[bold]{tot_out:,}[/bold]", f"[bold]{tot_all:,}[/bold]")
    console.print(table)
