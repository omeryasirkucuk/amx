"""Live terminal display for AMX agent runs."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.tree import Tree
from rich import box

import amx


class ActivityState(Enum):
    PENDING = "pending"
    ACTIVE = "active"
    DONE = "done"
    FAILED = "failed"


_STATE_GLYPH = {
    ActivityState.PENDING: "[dim]○[/dim]",
    ActivityState.ACTIVE: "[bold yellow]✦[/bold yellow]",
    ActivityState.DONE: "[green]●[/green]",
    ActivityState.FAILED: "[red]✗[/red]",
}


@dataclass
class Activity:
    label: str
    state: ActivityState = ActivityState.PENDING
    start_time: float | None = None
    end_time: float | None = None
    details: list[str] = field(default_factory=list)
    token_estimate: int = 0
    tokens_used: int = 0

    @property
    def elapsed(self) -> float:
        if self.start_time is None:
            return 0.0
        end = self.end_time or time.monotonic()
        return end - self.start_time

    @property
    def elapsed_str(self) -> str:
        s = self.elapsed
        if s < 60:
            return f"{s:.1f}s"
        return f"{int(s // 60)}m{int(s % 60)}s"


class LiveDisplay:
    """Manages the full terminal UI during agent runs.

    Call ``start()`` before agent work, ``stop()`` after.
    Between those, use ``add_activity`` / ``begin`` / ``complete``
    to drive the activity tree, and ``set_thinking`` for the LLM
    thinking indicator.
    """

    def __init__(self, console: Console | None = None) -> None:
        self._console = console or Console()
        self._live: Live | None = None
        self._lock = threading.Lock()

        self._context_schema: str = ""
        self._context_table: str = ""
        self._context_mode: str = ""
        self._context_provider: str = ""
        self._context_model: str = ""

        self._activities: list[Activity] = []
        self._thinking: bool = False
        self._thinking_label: str = ""
        self._thinking_start: float = 0.0
        self._collapsed: bool = False
        self._session_start: float = 0.0

        self._total_tokens_in: int = 0
        self._total_tokens_out: int = 0

    # ── Lifecycle ─────────────────────────────────────────────────────────

    def start(
        self,
        schema: str = "",
        table: str = "",
        mode: str = "",
        provider: str = "",
        model: str = "",
    ) -> None:
        self._context_schema = schema
        self._context_table = table
        self._context_mode = mode
        self._context_provider = provider
        self._context_model = model
        self._activities.clear()
        self._thinking = False
        self._collapsed = False
        self._session_start = time.monotonic()
        self._total_tokens_in = 0
        self._total_tokens_out = 0
        self._live = Live(
            self._render(),
            console=self._console,
            refresh_per_second=4,
            transient=False,
        )
        self._live.start()

    def stop(self) -> None:
        if self._live:
            self._live.update(self._render())
            self._live.stop()
            self._live = None

    def pause(self) -> None:
        if self._live:
            self._live.stop()

    def resume(self) -> None:
        if self._live:
            self._live = Live(
                self._render(),
                console=self._console,
                refresh_per_second=4,
                transient=False,
            )
            self._live.start()

    @property
    def is_active(self) -> bool:
        return self._live is not None

    # ── Context ───────────────────────────────────────────────────────────

    def set_context(self, **kwargs: str) -> None:
        for key in ("schema", "table", "mode", "provider", "model"):
            if key in kwargs:
                setattr(self, f"_context_{key}", kwargs[key])
        self._refresh()

    # ── Activity management ───────────────────────────────────────────────

    def add_activity(self, label: str, token_estimate: int = 0) -> int:
        act = Activity(label=label, token_estimate=token_estimate)
        with self._lock:
            self._activities.append(act)
            idx = len(self._activities) - 1
        self._refresh()
        return idx

    def begin_activity(self, idx: int) -> None:
        with self._lock:
            if 0 <= idx < len(self._activities):
                self._activities[idx].state = ActivityState.ACTIVE
                self._activities[idx].start_time = time.monotonic()
        self._refresh()

    def complete_activity(self, idx: int, detail: str = "") -> None:
        with self._lock:
            if 0 <= idx < len(self._activities):
                act = self._activities[idx]
                act.state = ActivityState.DONE
                act.end_time = time.monotonic()
                if detail:
                    act.details.append(detail)
        self._refresh()

    def fail_activity(self, idx: int, detail: str = "") -> None:
        with self._lock:
            if 0 <= idx < len(self._activities):
                act = self._activities[idx]
                act.state = ActivityState.FAILED
                act.end_time = time.monotonic()
                if detail:
                    act.details.append(detail)
        self._refresh()

    def add_detail(self, idx: int, detail: str) -> None:
        with self._lock:
            if 0 <= idx < len(self._activities):
                self._activities[idx].details.append(detail)
        self._refresh()

    def update_tokens(self, idx: int, tokens_used: int) -> None:
        with self._lock:
            if 0 <= idx < len(self._activities):
                self._activities[idx].tokens_used = tokens_used
        self._refresh()

    def add_session_tokens(self, input_tokens: int = 0, output_tokens: int = 0) -> None:
        with self._lock:
            self._total_tokens_in += input_tokens
            self._total_tokens_out += output_tokens
        self._refresh()

    # ── Thinking state ────────────────────────────────────────────────────

    def set_thinking(self, label: str = "Thinking") -> None:
        self._thinking = True
        self._thinking_label = label
        self._thinking_start = time.monotonic()
        self._refresh()

    def stop_thinking(self) -> None:
        self._thinking = False
        self._refresh()

    def toggle_collapse(self) -> None:
        self._collapsed = not self._collapsed
        self._refresh()

    # ── Rendering ─────────────────────────────────────────────────────────

    def _refresh(self) -> None:
        if self._live:
            try:
                self._live.update(self._render())
            except Exception:
                pass

    def _render(self) -> Group:
        parts: list[Any] = []
        parts.append(self._render_header())

        if self._thinking:
            parts.append(self._render_thinking())

        if self._activities:
            parts.append(self._render_activity_tree())

        parts.append(self._render_footer())
        return Group(*parts)

    def _render_header(self) -> Panel:
        ver = getattr(amx, "__version__", "?")
        left = f"[bold cyan]AMX[/bold cyan] [dim]v{ver}[/dim]"

        ctx_parts: list[str] = []
        if self._context_provider and self._context_model:
            ctx_parts.append(f"[yellow]{self._context_provider}/{self._context_model}[/yellow]")
        if self._context_schema:
            schema_str = self._context_schema
            if self._context_table:
                schema_str += f".{self._context_table}"
            ctx_parts.append(f"[cyan]{schema_str}[/cyan]")
        if self._context_mode:
            mode_color = "green" if self._context_mode == "batch" else "blue"
            ctx_parts.append(f"[{mode_color}]{self._context_mode.upper()}[/{mode_color}]")

        elapsed_total = time.monotonic() - self._session_start if self._session_start else 0
        time_str = f"[dim]{elapsed_total:.0f}s[/dim]"

        right = " │ ".join(ctx_parts) if ctx_parts else ""

        tokens_total = self._total_tokens_in + self._total_tokens_out
        tok_str = f"[dim]↓ {tokens_total:,} tokens[/dim]" if tokens_total else ""

        header_text = Text.from_markup(
            f"  {left}  {right}  {time_str}  {tok_str}"
        )
        return Panel(header_text, box=box.HEAVY, style="dim", height=3)

    def _render_thinking(self) -> Text:
        elapsed = time.monotonic() - self._thinking_start
        dots = "." * (int(elapsed * 2) % 4)
        collapsed_hint = "[dim](Tab to expand)[/dim]" if self._collapsed else ""
        return Text.from_markup(
            f"  [bold yellow]⟳[/bold yellow] {self._thinking_label}{dots} "
            f"[dim]({elapsed:.0f}s)[/dim] {collapsed_hint}"
        )

    def _render_activity_tree(self) -> Tree:
        tree = Tree("[bold]Pipeline[/bold]", guide_style="dim")

        for act in self._activities:
            glyph = _STATE_GLYPH[act.state]
            elapsed_str = f" [dim]({act.elapsed_str})[/dim]" if act.start_time else ""
            tok_str = ""
            if act.state == ActivityState.ACTIVE and act.token_estimate:
                tok_str = f" [dim]~{act.token_estimate:,} tokens[/dim]"
            elif act.state == ActivityState.DONE and act.tokens_used:
                tok_str = f" [dim]↓ {act.tokens_used:,} tokens[/dim]"

            node_label = f"{glyph} {act.label}{elapsed_str}{tok_str}"
            node = tree.add(node_label)

            if not self._collapsed:
                for detail in act.details:
                    node.add(f"[dim]{detail}[/dim]")

        return tree

    def _render_footer(self) -> Text:
        active = next(
            (a for a in self._activities if a.state == ActivityState.ACTIVE),
            None,
        )
        if active:
            return Text.from_markup(
                f"  [dim]Active:[/dim] [bold]{active.label}[/bold] "
                f"[dim]{active.elapsed_str}[/dim]  │  "
                f"[dim]Tab[/dim] toggle details  "
                f"[dim]Ctrl+C[/dim] interrupt"
            )
        return Text.from_markup(
            "  [dim]Tab[/dim] toggle details  [dim]Ctrl+C[/dim] interrupt"
        )


# ── Module singleton ──────────────────────────────────────────────────────

_display: LiveDisplay | None = None


def get_display() -> LiveDisplay:
    global _display
    if _display is None:
        from amx.utils.console import console
        _display = LiveDisplay(console=console)
    return _display
