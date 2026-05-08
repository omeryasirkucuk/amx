"""Live terminal display for AMX agent runs."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from rich import box
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.text import Text
from rich.tree import Tree

import amx


# ── Thread-local subscriber bus ───────────────────────────────────────────
#
# AMX Studio's run-detail page relies on the same LiveDisplay calls the
# CLI's Rich UI does ("Profiling address.state…", "Calling LLM (batch
# 1/3)…", "Saved 12 suggestions"), but it fan-outs over SSE instead of a
# Rich Live region. Without this bus, the Studio "Live progress" card
# only saw the per-table ``activity.added/begin/complete`` events the
# web worker emitted before / after each table — the multi-minute work
# inside ``orchestrator.process_table()`` rendered as
# "Waiting for the worker to begin…" until the table finished.
#
# Pattern: per-thread subscriber list. Each web worker installs a
# subscriber when it starts a job (via :func:`push_subscriber`), the
# subscriber forwards every LiveDisplay state change into the job's
# SSE queue, and the worker pops the subscriber on exit. Threading-
# local keeps concurrent jobs from leaking each other's events.

_DisplaySubscriber = Callable[[str, dict[str, Any]], None]
_local = threading.local()


def push_subscriber(callback: _DisplaySubscriber) -> None:
    """Install a subscriber on the current thread's display event bus.

    The subscriber receives every ``(event_type, payload)`` produced by
    LiveDisplay state-mutating methods on this thread. Stack-friendly:
    nested ``push`` / ``pop`` pairs work without confusing siblings.
    """
    stack = getattr(_local, "stack", None)
    if stack is None:
        stack = []
        _local.stack = stack
    stack.append(callback)


def pop_subscriber(callback: _DisplaySubscriber) -> None:
    """Remove the most-recent matching subscriber from this thread."""
    stack = getattr(_local, "stack", None) or []
    try:
        # ``list.remove`` removes the FIRST match; we want the LAST so
        # nested context managers unwind in LIFO order.
        for idx in range(len(stack) - 1, -1, -1):
            if stack[idx] is callback:
                del stack[idx]
                break
    except ValueError:
        pass


def _notify_subscribers(event_type: str, payload: dict[str, Any]) -> None:
    """Best-effort fan-out to every subscriber on the current thread.

    A subscriber that raises is logged-and-skipped so a buggy bridge
    can't poison the rest of the run — the LiveDisplay UI / SSE stream
    keeps moving.
    """
    stack = getattr(_local, "stack", None) or []
    for callback in stack:
        try:
            callback(event_type, payload)
        except Exception:  # pragma: no cover — bridge failures stay silent
            pass


class ActivityState(Enum):
    PENDING = "pending"
    ACTIVE = "active"
    DONE = "done"
    FAILED = "failed"


_STATE_GLYPH = {
    ActivityState.PENDING: "[dim]○[/dim]",
    ActivityState.ACTIVE: "[bold #fb923c]✦[/bold #fb923c]",
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
        # Headless activation flag: true while a non-CLI consumer
        # (currently AMX Studio's run worker) wants the display's
        # state machine + subscriber bus running, but does NOT want
        # a Rich ``Live`` painted on the parent terminal. With this
        # set, ``is_active`` returns True and ``step_spinner`` emits
        # ``step.*`` events through ``_notify_subscribers``, which
        # the SSE bridge in ``amx/web/routers/runs.py`` forwards to
        # the browser — without doubling up on output in the CLI
        # terminal where Studio was launched from.
        self._headless: bool = False
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
        # Streaming reasoning text from the model. Populated by
        # ``update_thinking`` while a chat call streams reasoning deltas;
        # cleared on ``stop_thinking`` and ``start`` so it never leaks
        # between turns. Capped to the most recent slice so the panel
        # height stays bounded.
        self._thinking_text: str = ""
        self._collapsed: bool = False
        self._session_start: float = 0.0

        self._total_tokens_in: int = 0
        self._total_tokens_out: int = 0
        # Running USD cost across this session, summed from per-call
        # ``ModelPrice``-resolved figures by ``add_session_tokens``.
        # Rendered in the header next to the token total so users see
        # both pressure dimensions in the same eye-line.
        self._total_cost_usd: float = 0.0

        # Reentrancy counter for ``pause()`` / ``resume()``. Nested
        # contexts (e.g. TableProcessor pauses for human review which
        # internally invokes ``ask_choice`` which itself pauses via
        # ``_live_paused_for_input``) need balanced pairs without
        # double-starting Rich's underlying ``Live``. The outer pause
        # actually stops the Live; inner pauses just bump the depth;
        # the matching resumes decrement; only the outermost resume
        # restarts the Live. Without this, the inner resume restarted
        # the Live, and the outer resume hit
        # ``Only one live display may be active at once``.
        self._pause_depth: int = 0

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
        self._thinking_text = ""
        self._collapsed = False
        self._session_start = time.monotonic()
        self._total_tokens_in = 0
        self._total_tokens_out = 0
        self._total_cost_usd = 0.0
        # ``transient=True`` clears the WHOLE live region (header + thinking
        # spinner + pipeline tree) when ``stop()`` runs. Without this, every
        # height change in the renderable left a frame behind in the scroll
        # buffer, producing the "stacked AMX v0.4.2 SEARCH 2s / 3s / 9s"
        # header bars users were complaining about. To preserve the pipeline
        # tree as a useful summary, we re-print it once after stop() — see
        # ``stop()`` below.
        self._live = Live(
            self,
            console=self._console,
            refresh_per_second=10,
            transient=True,
        )
        self._live.start()
        # Reset pause depth on a fresh start so a leftover counter from
        # a previous start/stop cycle doesn't make the next pause/resume
        # asymmetric.
        self._pause_depth = 0

    def stop(self) -> None:
        if self._live:
            self._live.update(self._render())
            self._live.stop()
            self._live = None
        self._pause_depth = 0
        # After the transient live region clears, leave a quiet, single-line
        # summary of the pipeline so the user can still tell what AMX did.
        # Skip when there were no real activities (e.g. a deterministic
        # short-circuit answered without invoking any agent step).
        try:
            tree = self._summary_tree_after_stop()
            if tree is not None:
                self._console.print(tree)
        except Exception:
            # Never let post-stop summary rendering break the call site.
            pass

    def _summary_tree_after_stop(self) -> Tree | None:
        """Render a quiet pipeline summary to print AFTER ``stop()``.

        Only includes activities that actually ran (excludes the live header
        and the running spinner). Returns ``None`` when there's nothing
        worth showing — short-circuit answers don't deserve a Pipeline
        block underneath them.
        """
        if not self._activities:
            return None
        tree = Tree("[dim]Pipeline[/dim]", guide_style="dim")
        for act in self._activities:
            elapsed = (act.end_time or time.monotonic()) - (act.start_time or self._session_start)
            mark = {
                ActivityState.DONE: "[green]✓[/green]",
                ActivityState.FAILED: "[red]✗[/red]",
                ActivityState.ACTIVE: "[#fb923c]…[/#fb923c]",
            }.get(act.state, "[dim]·[/dim]")
            tree.add(f"{mark} [dim]{act.label}[/dim] [dim]({elapsed:.1f}s)[/dim]")
        return tree

    def pause(self) -> None:
        """Stop the Rich ``Live`` so prompt_toolkit can echo keystrokes.

        Reentrant — only the **first** pause actually stops the live
        region; nested pauses just bump the depth counter. This
        matches the natural call pattern where TableProcessor pauses
        for human review and the prompt helpers inside that review
        (``ask_choice`` via ``_live_paused_for_input``) pause again
        per-keystroke.
        """
        if self._live is None:
            return
        if self._pause_depth == 0:
            try:
                self._live.stop()
            except Exception:
                # Defensive — if Rich's internal state is already off,
                # don't break the user-facing flow.
                pass
        self._pause_depth += 1

    def resume(self) -> None:
        """Restart the paused ``Live`` region.

        Reentrant counterpart to :meth:`pause`. The matching outer
        ``resume`` is the one that restarts Rich's ``Live``; inner
        resumes just decrement the depth. Without this, the inner
        ``resume`` started the Live early, and the outer ``resume``
        hit ``Only one live display may be active at once`` because
        Rich rejects starting an already-started Live.

        No-op when the display was never paused (or when pause was
        called on a None ``_live``) — keeps callers from having to
        track whether they actually paused.
        """
        if self._live is None or self._pause_depth == 0:
            return
        self._pause_depth -= 1
        if self._pause_depth == 0:
            try:
                self._live.start()
            except Exception:
                # If Rich refuses (e.g. another Live snuck in), surface
                # the issue via debug logging rather than crashing the
                # outer flow. The display may end up out of sync but
                # the user's run continues.
                pass

    @property
    def is_active(self) -> bool:
        return self._live is not None or self._headless

    # ── Headless lifecycle (Studio worker) ────────────────────────────────

    def start_headless(
        self,
        schema: str = "",
        table: str = "",
        mode: str = "",
        provider: str = "",
        model: str = "",
    ) -> None:
        """Activate the display's state + subscriber bus WITHOUT painting.

        Mirrors :meth:`start` for state initialisation but skips the
        Rich ``Live`` widget — the parent CLI terminal stays clean
        (the Studio launcher's ``quiet_console()`` is in effect, but
        Rich's ``Live`` would still write).

        ``step_spinner`` checks ``is_active`` to decide whether to
        emit ``step.*`` events; with headless on, the SSE bridge in
        :mod:`amx.web.routers.runs` sees the same per-batch / per-
        agent narration the CLI's terminal Live would have shown.
        """
        # Reset the same fields ``start()`` resets so a worker is
        # never poisoned by a previous run's leftover state.
        self._context_schema = schema
        self._context_table = table
        self._context_mode = mode
        self._context_provider = provider
        self._context_model = model
        self._activities.clear()
        self._thinking = False
        self._thinking_text = ""
        self._collapsed = False
        self._session_start = time.monotonic()
        self._total_tokens_in = 0
        self._total_tokens_out = 0
        self._total_cost_usd = 0.0
        self._pause_depth = 0
        self._headless = True

    def stop_headless(self) -> None:
        """Deactivate the headless state. Idempotent."""
        self._headless = False

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
        _notify_subscribers(
            "step.added",
            {"idx": idx, "label": label, "token_estimate": token_estimate},
        )
        return idx

    def begin_activity(self, idx: int) -> None:
        with self._lock:
            if 0 <= idx < len(self._activities):
                self._activities[idx].state = ActivityState.ACTIVE
                self._activities[idx].start_time = time.monotonic()
                label = self._activities[idx].label
            else:
                label = ""
        self._refresh()
        _notify_subscribers("step.begin", {"idx": idx, "label": label})

    def complete_activity(self, idx: int, detail: str = "") -> None:
        with self._lock:
            if 0 <= idx < len(self._activities):
                act = self._activities[idx]
                act.state = ActivityState.DONE
                act.end_time = time.monotonic()
                if detail:
                    act.details.append(detail)
                label = act.label
            else:
                label = ""
        self._refresh()
        _notify_subscribers(
            "step.complete",
            {"idx": idx, "label": label, "detail": detail},
        )

    def fail_activity(self, idx: int, detail: str = "") -> None:
        with self._lock:
            if 0 <= idx < len(self._activities):
                act = self._activities[idx]
                act.state = ActivityState.FAILED
                act.end_time = time.monotonic()
                if detail:
                    act.details.append(detail)
                label = act.label
            else:
                label = ""
        self._refresh()
        _notify_subscribers(
            "step.fail",
            {"idx": idx, "label": label, "detail": detail},
        )

    def add_detail(self, idx: int, detail: str) -> None:
        with self._lock:
            if 0 <= idx < len(self._activities):
                self._activities[idx].details.append(detail)
        self._refresh()
        _notify_subscribers("step.detail", {"idx": idx, "detail": detail})

    def update_activity(
        self,
        idx: int,
        *,
        label: str | None = None,
        reset_details: bool = False,
    ) -> None:
        with self._lock:
            if 0 <= idx < len(self._activities):
                act = self._activities[idx]
                if label is not None:
                    act.label = label
                if reset_details:
                    act.details.clear()
                resolved_label = act.label
            else:
                resolved_label = label or ""
        self._refresh()
        if label is not None:
            _notify_subscribers(
                "step.update", {"idx": idx, "label": resolved_label}
            )

    def update_tokens(self, idx: int, tokens_used: int) -> None:
        with self._lock:
            if 0 <= idx < len(self._activities):
                self._activities[idx].tokens_used = tokens_used
        self._refresh()

    def add_session_tokens(
        self,
        input_tokens: int = 0,
        output_tokens: int = 0,
        cost_delta_usd: float = 0.0,
    ) -> None:
        """Accumulate one LLM call's tokens + cost into the live header.

        ``cost_delta_usd`` is the freshly-computed USD figure from
        :func:`amx.llm.pricing.compute_cost` (zero when no price is
        known for the model). Called by
        :meth:`amx.utils.token_tracker.TokenTracker.record` right after
        every LLM round-trip.
        """
        with self._lock:
            self._total_tokens_in += input_tokens
            self._total_tokens_out += output_tokens
            try:
                self._total_cost_usd += float(cost_delta_usd or 0.0)
            except (TypeError, ValueError):
                pass
            tokens_in_total = self._total_tokens_in
            tokens_out_total = self._total_tokens_out
            cost_total = self._total_cost_usd
        _notify_subscribers(
            "tokens.delta",
            {
                "in": int(input_tokens or 0),
                "out": int(output_tokens or 0),
                "cost_usd": float(cost_delta_usd or 0.0),
                "total_in": tokens_in_total,
                "total_out": tokens_out_total,
                "total_cost_usd": cost_total,
            },
        )
        self._refresh()

    # ── Thinking state ────────────────────────────────────────────────────

    def set_thinking(self, label: str = "Thinking") -> None:
        self._thinking = True
        self._thinking_label = label
        self._thinking_start = time.monotonic()
        self._thinking_text = ""
        self._refresh()
        _notify_subscribers("step.thinking", {"label": label})

    def update_thinking(self, text: str) -> None:
        """Replace the streaming reasoning snippet shown under the spinner.

        Callers pass the full accumulated reasoning so far; we keep only the
        tail (~600 chars) so the panel never grows unbounded as the model
        continues to think.
        """
        if not text:
            return
        with self._lock:
            tail = text[-600:] if len(text) > 600 else text
            if tail == self._thinking_text:
                return
            self._thinking_text = tail
        self._refresh()

    def stop_thinking(self) -> None:
        self._thinking = False
        self._thinking_text = ""
        self._refresh()
        _notify_subscribers("step.thinking_done", {})

    def toggle_collapse(self) -> None:
        self._collapsed = not self._collapsed
        self._refresh()

    # ── Rendering ─────────────────────────────────────────────────────────

    def _refresh(self) -> None:
        pass

    def __rich_console__(self, console, options):
        yield self._render()

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
        left = f"[bold #fb923c]AMX[/bold #fb923c] [dim]v{ver}[/dim]"

        ctx_parts: list[str] = []
        if self._context_provider and self._context_model:
            ctx_parts.append(f"[#fb923c]{self._context_provider}/{self._context_model}[/#fb923c]")
        if self._context_schema:
            schema_str = self._context_schema
            if self._context_table:
                schema_str += f".{self._context_table}"
            ctx_parts.append(f"[#22d3ee]{schema_str}[/#22d3ee]")
        if self._context_mode:
            mode_color = "green" if self._context_mode == "batch" else "#fb923c"
            ctx_parts.append(f"[{mode_color}]{self._context_mode.upper()}[/{mode_color}]")

        elapsed_total = time.monotonic() - self._session_start if self._session_start else 0
        time_str = f"[dim]{elapsed_total:.0f}s[/dim]"

        right = " │ ".join(ctx_parts) if ctx_parts else ""

        tokens_total = self._total_tokens_in + self._total_tokens_out
        if tokens_total:
            cost = self._total_cost_usd
            if cost > 0:
                tok_str = f"[dim]↓ {tokens_total:,} tokens · ${cost:,.4f}[/dim]"
            else:
                tok_str = f"[dim]↓ {tokens_total:,} tokens[/dim]"
        else:
            tok_str = ""

        header_text = Text.from_markup(f"  {left}  {right}  {time_str}  {tok_str}")
        return Panel(header_text, box=box.HEAVY, style="dim", height=3)

    def _render_thinking(self) -> Group | Text:
        elapsed = time.monotonic() - self._thinking_start
        dots = "." * (int(elapsed * 2) % 4)
        collapsed_hint = "[dim](Tab to expand)[/dim]" if self._collapsed else ""
        header = Text.from_markup(
            f"  [bold #fb923c]⟳[/bold #fb923c] {self._thinking_label}{dots} "
            f"[dim]({elapsed:.0f}s)[/dim] {collapsed_hint}"
        )
        if not self._thinking_text:
            return header
        # Show the last few lines of streamed reasoning, dim-italic and
        # indented so it reads as ambient model chatter rather than answer
        # text. ``transient=True`` on the Live wipes it on stop().
        snippet = self._thinking_text.replace("\r", "")
        lines = [ln for ln in snippet.split("\n") if ln.strip()]
        tail_lines = lines[-3:] if len(lines) > 3 else lines
        body = Text("\n".join(f"    {ln}" for ln in tail_lines), style="dim italic")
        body.overflow = "fold"
        return Group(header, body)

    def _render_activity_tree(self) -> Tree:
        tree = Tree("[bold]Pipeline[/bold]", guide_style="dim")

        # Windowing logic to prevent terminal overflow
        max_visible = 15 if self._collapsed else 25
        total_acts = len(self._activities)

        display_acts = self._activities
        hidden_count = 0

        if total_acts > max_visible:
            hidden_count = total_acts - max_visible
            display_acts = self._activities[-max_visible:]
            tree.add(
                f"[dim]... {hidden_count} older activities hidden (Press Tab to toggle details) ...[/dim]"
            )

        for act in display_acts:
            elapsed_str = f" [dim]({act.elapsed_str})[/dim]" if act.start_time else ""
            tok_str = ""
            if act.state == ActivityState.ACTIVE and act.token_estimate:
                tok_str = f" [dim]~{act.token_estimate:,} tokens[/dim]"
            elif act.state == ActivityState.DONE and act.tokens_used:
                tok_str = f" [dim]↓ {act.tokens_used:,} tokens[/dim]"

            if act.state == ActivityState.ACTIVE:
                idx = int(time.monotonic() * 10) % 8
                spinner = ["⢹", "⢺", "⢼", "⣸", "⣇", "⡧", "⡏", "⡟"][idx]
                glyph = f"[bold #fb923c]{spinner}[/bold #fb923c]"
            else:
                glyph = _STATE_GLYPH[act.state]

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
        return Text.from_markup("  [dim]Tab[/dim] toggle details  [dim]Ctrl+C[/dim] interrupt")


# ── Module singleton ──────────────────────────────────────────────────────

_display: LiveDisplay | None = None


def get_display() -> LiveDisplay:
    global _display
    if _display is None:
        from amx.utils.console import console

        _display = LiveDisplay(console=console)
    return _display
