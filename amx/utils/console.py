"""Shared Rich console and interaction helpers."""

from __future__ import annotations

import os
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
    """Render AMX startup banner once per process."""
    global _BANNER_SHOWN
    if _BANNER_SHOWN and not force:
        return
    if os.getenv("AMX_NO_BANNER", "").lower() in {"1", "true", "yes"}:
        return

    title = Text("* AMX (Agentic Metadata Extractor) — Initialization & Setup *", style="bold cyan")
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
    subtitle = Text(
        "Metadata Extraction System | Multi-Source Inference Pipeline",
        style="bold #66ffff",
    )

    content = Text.assemble(title, "\n\n", art, "\n", subtitle)
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


def ask(question: str, default: str = "") -> str:
    return pt_prompt(f"  {question}: ", default=default).strip()


def ask_password(question: str) -> str:
    return pt_prompt(f"  {question}: ", is_password=True).strip()


def ask_choice(question: str, choices: list[str], default: str = "") -> str:
    """Prompt for a single choice. Type 1–N or a matching label. Enter accepts the default.

    The input line is never pre-filled with the default text (so you can type ``2`` immediately).
    """
    if not choices:
        return default
    completer = WordCompleter(choices, ignore_case=True)
    console.print(f"  [info]{question}[/info]")
    for i, c in enumerate(choices, 1):
        mark = " — default (Enter)" if default and c == default else ""
        console.print(f"    {i}. {c}[dim]{mark}[/dim]")
    if default and default in choices:
        console.print("  [dim]Enter = default · or type a number 1–%d[/dim]" % len(choices))
    # Do not pass default= to pt_prompt — it pre-fills the whole string and forces delete-before-2.
    answer = pt_prompt("  > ", completer=completer).strip()
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
    raw = pt_prompt("  > ").strip()
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
    answer = pt_prompt(f"  {question}{suffix}: ").strip().lower()
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
    """Rich spinner that shows elapsed time and optional token estimate.

    Usage::

        with step_spinner("Profile Agent batch 1/3", token_estimate=1240):
            result = llm.chat(messages)
    """
    tok = f" (~{token_estimate:,} input tokens)" if token_estimate else ""
    t0 = time.monotonic()
    with console.status(f"[info]{label}{tok}[/info]", spinner="dots") as status:
        try:
            yield
        finally:
            elapsed = time.monotonic() - t0
            msg = done_message or label
            status.update(f"[success]✓ {msg} ({elapsed:.1f}s)[/success]")
    if done_message:
        success(f"{done_message} ({elapsed:.1f}s)")
    else:
        success(f"{label} ({elapsed:.1f}s)")


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
