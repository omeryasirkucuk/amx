"""Interactive conflict resolution prompt for OCC-protected resources.

When a save operation on a shared-store resource fails with
:class:`~amx.storage.conflicts.StaleVersionError`, the CLI can invoke
:func:`resolve_conflict_interactive` to show a side-by-side diff and
let the user choose how to proceed.

Non-interactive usage: pass ``on_conflict`` to skip the prompt.
Accepted values: ``"cancel"``, ``"overwrite"``, ``"fail"``.

Typical call site (e.g. a page body update)::

    from amx.cli_support.conflict_prompt import resolve_conflict_interactive
    from amx.storage.conflicts import ConflictResolution, StaleVersionError

    try:
        store.update_documentation_page_body(
            page_id, markdown_body=new_body, expected_version=my_version
        )
    except StaleVersionError as exc:
        resolution = resolve_conflict_interactive(
            exc,
            my_value=new_body,
            on_conflict=on_conflict_flag,
        )
        if resolution == ConflictResolution.OVERWRITE:
            store.update_documentation_page_body(
                page_id, markdown_body=new_body,
                expected_version=exc.actual.version,
                force_overwrite=True,
            )
        elif resolution == ConflictResolution.CANCEL:
            console.print("[dim]Edit cancelled.[/dim]")
        # MERGE: caller handles providing a merged value

Cross-platform: uses only stdlib / rich for terminal I/O, no ANSI
escape codes that break on Windows cmd.exe or PowerShell.
"""

from __future__ import annotations

from typing import Literal

from amx.storage.conflicts import ConflictResolution, StaleVersionError

# On-conflict policy type used by CLI flag parsers
OnConflictPolicy = Literal["cancel", "overwrite", "fail"]


def resolve_conflict_interactive(
    exc: StaleVersionError,
    *,
    my_value: str,
    on_conflict: OnConflictPolicy | None = None,
) -> ConflictResolution:
    """Resolve a :class:`StaleVersionError`, interactively or via policy.

    When ``on_conflict`` is provided, skips the interactive prompt:
      - ``"cancel"``    → return CANCEL
      - ``"overwrite"`` → return OVERWRITE
      - ``"fail"``      → re-raise the original exception

    When ``on_conflict`` is None, shows a terminal diff and prompts the
    user with ``[k]eep theirs / [o]verwrite / [e]dit / [c]ancel``.

    Returns a :class:`~amx.storage.conflicts.ConflictResolution` value.
    The caller is responsible for applying the chosen resolution.
    """
    if on_conflict == "cancel":
        return ConflictResolution.CANCEL
    if on_conflict == "overwrite":
        return ConflictResolution.OVERWRITE
    if on_conflict == "fail":
        raise exc

    # Interactive path
    try:
        from rich.columns import Columns
        from rich.console import Console
        from rich.panel import Panel
        from rich.text import Text

        console = Console()
        their_text = _extract_their_text(exc.actual.current_value)
        updated_by = exc.actual.updated_by or "another user"
        updated_at = (
            exc.actual.updated_at.isoformat()
            if hasattr(exc.actual.updated_at, "isoformat")
            else str(exc.actual.updated_at)
        )
        console.print(
            f"\n[bold yellow]Edit conflict[/bold yellow] on "
            f"[bold]{exc.resource}[/bold]\n"
            f"Changed by [bold]{updated_by}[/bold] at {updated_at}"
        )
        their_panel = Panel(
            Text(their_text[:2000], overflow="fold"),
            title="[dim]Current (theirs)[/dim]",
            border_style="dim",
            expand=True,
        )
        my_panel = Panel(
            Text(my_value[:2000], overflow="fold"),
            title="[cyan]Your version[/cyan]",
            border_style="cyan",
            expand=True,
        )
        console.print(Columns([their_panel, my_panel]))
        console.print(
            "\n[dim]Choose:[/dim] "
            "[bold]k[/bold]=keep theirs  "
            "[bold]o[/bold]=overwrite with mine  "
            "[bold]e[/bold]=edit my version  "
            "[bold]c[/bold]=cancel"
        )
        while True:
            try:
                key = input("> ").strip().lower()
            except (EOFError, KeyboardInterrupt):
                console.print("[dim]Cancelled.[/dim]")
                return ConflictResolution.CANCEL
            if key in ("k", "keep"):
                return ConflictResolution.CANCEL
            if key in ("o", "overwrite"):
                return ConflictResolution.OVERWRITE
            if key in ("e", "edit"):
                return ConflictResolution.MERGE
            if key in ("c", "cancel", ""):
                return ConflictResolution.CANCEL
            console.print("[dim]Type k, o, e, or c.[/dim]")
    except ImportError:
        # Rich not available — fall back to print/input
        print(f"\nEdit conflict on {exc.resource}")
        print(f"Changed by {exc.actual.updated_by} at {exc.actual.updated_at}")
        print(f"Expected version {exc.expected_version}, got {exc.actual.version}")
        print("\n[k]eep theirs  [o]verwrite  [e]dit  [c]ancel")
        while True:
            try:
                key = input("> ").strip().lower()
            except (EOFError, KeyboardInterrupt):
                return ConflictResolution.CANCEL
            if key in ("k",):
                return ConflictResolution.CANCEL
            if key in ("o",):
                return ConflictResolution.OVERWRITE
            if key in ("e",):
                return ConflictResolution.MERGE
            if key in ("c", ""):
                return ConflictResolution.CANCEL
            print("Type k, o, e, or c.")

    return ConflictResolution.CANCEL


def _extract_their_text(current_value: dict) -> str:
    """Pull a human-readable string out of the conflict snapshot."""
    if isinstance(current_value, dict):
        for key in ("markdown_body", "text", "body", "value"):
            if isinstance(current_value.get(key), str):
                return current_value[key]
    try:
        import json

        return json.dumps(current_value, indent=2, default=str)
    except Exception:
        return str(current_value)


__all__ = [
    "OnConflictPolicy",
    "resolve_conflict_interactive",
]
