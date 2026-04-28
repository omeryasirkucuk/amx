"""Helpers for showing the shared live command display outside `/run`."""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager

from amx.utils.live_display import LiveDisplay, get_display


@contextmanager
def command_display(
    *,
    schema: str = "",
    table: str = "",
    mode: str = "",
    provider: str = "",
    model: str = "",
) -> Generator[LiveDisplay, None, None]:
    """Start the shared live display when a command needs visible progress."""
    display = get_display()
    started = False
    if not display.is_active:
        display.start(
            schema=schema,
            table=table,
            mode=mode,
            provider=provider,
            model=model,
        )
        started = True
    else:
        context: dict[str, str] = {}
        if schema:
            context["schema"] = schema
        if table:
            context["table"] = table
        if mode:
            context["mode"] = mode
        if provider:
            context["provider"] = provider
        if model:
            context["model"] = model
        if context:
            display.set_context(**context)
    try:
        yield display
    finally:
        if started:
            display.stop()
