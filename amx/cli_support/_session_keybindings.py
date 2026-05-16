"""Prompt-toolkit key bindings for the ``amx`` REPL session.

Extracted from :mod:`amx.cli_support.session` so the key-binding rules
(Escape / Ctrl-C / Left / Right tab nav) live in their own focused
module. The namespace state ``_NS_STATE`` lives here too because it is
the single source of truth the keybindings read and write — keeping
state and behaviour in the same module avoids a circular import
between the session module and the keybindings module.

``session.py`` re-exports ``_NS_STATE`` and ``_kb_escape_namespace``
for any caller that imported them directly.
"""

from __future__ import annotations

from prompt_toolkit.filters import Condition
from prompt_toolkit.key_binding import KeyBindings

_NS_STATE: dict[str, str] = {"namespace": ""}


def _kb_escape_namespace() -> KeyBindings:
    kb = KeyBindings()

    @Condition
    def _is_buffer_empty() -> bool:
        from prompt_toolkit.application.current import get_app

        return len(get_app().current_buffer.text) == 0

    tabs = ["", "db", "metadata", "docs", "llm", "code", "analyze", "search", "history"]

    @kb.add("escape")
    def _(event) -> None:  # type: ignore[no-untyped-def]
        buf = event.app.current_buffer
        if buf.text:
            buf.reset()
            return
        namespace = _NS_STATE.get("namespace", "")
        if namespace:
            _NS_STATE["namespace"] = ""
            event.app.exit(result="__amx_esc_back__")
        else:
            event.app.exit(result="__amx_esc_root__")

    @kb.add("c-c")
    def _(event) -> None:  # type: ignore[no-untyped-def]
        # Ctrl-C with text in the buffer clears the line (standard shell
        # convention). On an empty prompt — i.e. the user is just sitting
        # on a tab, not mid-input — leave the session entirely so Ctrl-C
        # behaves as a quick exit instead of a no-op.
        buf = event.app.current_buffer
        if buf.text:
            buf.reset()
        else:
            event.app.exit(result="__amx_exit__")

    @kb.add("right", filter=_is_buffer_empty)
    def _(event) -> None:  # type: ignore[no-untyped-def]
        curr = _NS_STATE.get("namespace", "")
        idx = tabs.index(curr) if curr in tabs else 0
        event.app.exit(result=f"__amx_switch_ns__:{tabs[(idx + 1) % len(tabs)]}")

    @kb.add("left", filter=_is_buffer_empty)
    def _(event) -> None:  # type: ignore[no-untyped-def]
        curr = _NS_STATE.get("namespace", "")
        idx = tabs.index(curr) if curr in tabs else 0
        event.app.exit(result=f"__amx_switch_ns__:{tabs[(idx - 1) % len(tabs)]}")

    return kb
