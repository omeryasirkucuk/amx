"""Shared Rich console and interaction helpers."""

from __future__ import annotations

from typing import Any

from prompt_toolkit import prompt as pt_prompt
from prompt_toolkit.completion import WordCompleter
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
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
    completer = WordCompleter(choices, ignore_case=True)
    console.print(f"  [info]{question}[/info]")
    for i, c in enumerate(choices, 1):
        console.print(f"    {i}. {c}")
    answer = pt_prompt("  > ", completer=completer, default=default).strip()
    if answer.isdigit() and 1 <= int(answer) <= len(choices):
        return choices[int(answer) - 1]
    return answer if answer in choices else default


def ask_multi_choice(question: str, choices: list[str]) -> list[str]:
    console.print(f"  [info]{question}[/info]")
    console.print("  (enter comma-separated numbers or names, 'all' for everything)")
    for i, c in enumerate(choices, 1):
        console.print(f"    {i}. {c}")
    raw = pt_prompt("  > ").strip()
    if raw.lower() == "all":
        return choices
    selected: list[str] = []
    for token in raw.split(","):
        token = token.strip()
        if token.isdigit() and 1 <= int(token) <= len(choices):
            selected.append(choices[int(token) - 1])
        elif token in choices:
            selected.append(token)
    return selected or choices


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
