"""CLI hints that point at richer Studio UIs.

Centralising the strings makes it cheap to retune copy without hunting
through every command file, and lets us toggle them off via a single
config knob if the noise becomes unwelcome.
"""

from __future__ import annotations

from amx.utils.console import console

#: Per-feature hint copy. Keys are stable so docs can reference them.
_STUDIO_HINTS: dict[str, str] = {
    "doc-add": ("In Studio, Settings → Docs lets you drag-and-drop files to add them."),
    "code-search": (
        "In Studio, the 'Search code' box under Settings → Code shows the "
        "same results as interactive cards."
    ),
    "code-analyze": (
        "In Studio, the Code → Analyze page lets you run this job "
        "interactively with table selection."
    ),
    "ask-context": ("In Studio, the AskChat top banner shows this scope as a small badge."),
}


def studio_hint(name: str) -> None:
    """Print a one-line tip for ``name`` if registered, else no-op.

    Output goes through the AMX console so it picks up the same theme
    as the rest of the CLI (dim colour, no extra padding).
    """
    text = _STUDIO_HINTS.get(name)
    if not text:
        return
    console.print(f"[dim]💡 Tip: {text}[/dim]")
