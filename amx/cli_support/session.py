"""Interactive session helpers for the AMX CLI."""

from __future__ import annotations

import os
import shlex
import signal
from collections.abc import Callable

import click
from prompt_toolkit.completion import Completer, Completion
from prompt_toolkit.formatted_text import HTML
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.shortcuts import CompleteStyle, PromptSession
from prompt_toolkit.styles import Style

from amx.cli_support._session_keybindings import (  # noqa: PLC0414
    _NS_STATE as _NS_STATE,
)
from amx.cli_support._session_keybindings import (
    _kb_escape_namespace as _kb_escape_namespace,
)
from amx.cli_support._session_ui import (  # noqa: PLC0414
    _TAB_ORDER as _TAB_ORDER,
)
from amx.cli_support._session_ui import (
    _print_namespace_hint as _print_namespace_hint,
)
from amx.cli_support._session_ui import (
    _print_session_help as _print_session_help,
)
from amx.cli_support._session_ui import (
    _print_tab_bar as _print_tab_bar,
)
from amx.cli_support.commands.db import (
    cmd_add_profile as _cmd_add_profile,
)
from amx.cli_support.commands.db import (
    cmd_cache_clear as _cmd_cache_clear,
)
from amx.cli_support.commands.db import (
    cmd_cache_show as _cmd_cache_show,
)
from amx.cli_support.commands.db import (
    cmd_cache_stats as _cmd_cache_stats,
)
from amx.cli_support.commands.db import (
    cmd_edit_profile as _cmd_edit_profile,
)
from amx.cli_support.commands.db import (
    cmd_inspect as _cmd_inspect,
)
from amx.cli_support.commands.db import (
    cmd_profiles as _cmd_profiles,
)
from amx.cli_support.commands.db import (
    cmd_profiling as _cmd_profiling,
)
from amx.cli_support.commands.db import (
    cmd_remove_profile as _cmd_remove_profile,
)
from amx.cli_support.commands.db import (
    cmd_use as _cmd_use,
)
from amx.cli_support.commands.embeddings import cmd_embeddings as _cmd_embeddings
from amx.cli_support.commands.mcp import cmd_mcp as _cmd_mcp
from amx.cli_support.commands.profiles import (
    cmd_add_code_profile as _cmd_add_code_profile,
)
from amx.cli_support.commands.profiles import (
    cmd_add_doc_profile as _cmd_add_doc_profile,
)
from amx.cli_support.commands.profiles import (
    cmd_add_llm_profile as _cmd_add_llm_profile,
)
from amx.cli_support.commands.profiles import (
    cmd_alternatives_mode as _cmd_alternatives_mode,
)
from amx.cli_support.commands.profiles import (
    cmd_ask_context as _cmd_ask_context,
)
from amx.cli_support.commands.profiles import (
    cmd_batch_context_columns as _cmd_batch_context_columns,
)
from amx.cli_support.commands.profiles import (
    cmd_code_link as _cmd_code_link,
)
from amx.cli_support.commands.profiles import (
    cmd_code_profiles as _cmd_code_profiles,
)
from amx.cli_support.commands.profiles import (
    cmd_confidence_signal as _cmd_confidence_signal,
)
from amx.cli_support.commands.profiles import (
    cmd_cost as _cmd_cost,
)
from amx.cli_support.commands.profiles import (
    cmd_description_verbosity as _cmd_description_verbosity,
)
from amx.cli_support.commands.profiles import (
    cmd_doc_files as _cmd_doc_files,
)
from amx.cli_support.commands.profiles import (
    cmd_doc_link as _cmd_doc_link,
)
from amx.cli_support.commands.profiles import (
    cmd_doc_profiles as _cmd_doc_profiles,
)
from amx.cli_support.commands.profiles import (
    cmd_llm_batch_size as _cmd_llm_batch_size,
)
from amx.cli_support.commands.profiles import (
    cmd_llm_profiles as _cmd_llm_profiles,
)
from amx.cli_support.commands.profiles import (
    cmd_logprob_thresholds as _cmd_logprob_thresholds,
)
from amx.cli_support.commands.profiles import (
    cmd_max_tokens as _cmd_max_tokens,
)
from amx.cli_support.commands.profiles import (
    cmd_n_alternatives as _cmd_n_alternatives,
)
from amx.cli_support.commands.profiles import (
    cmd_prompt_detail as _cmd_prompt_detail,
)
from amx.cli_support.commands.profiles import (
    cmd_refresh_prices as _cmd_refresh_prices,
)
from amx.cli_support.commands.profiles import (
    cmd_remove_code_profile as _cmd_remove_code_profile,
)
from amx.cli_support.commands.profiles import (
    cmd_remove_doc_profile as _cmd_remove_doc_profile,
)
from amx.cli_support.commands.profiles import (
    cmd_remove_llm_profile as _cmd_remove_llm_profile,
)
from amx.cli_support.commands.profiles import (
    cmd_temperature as _cmd_temperature,
)
from amx.cli_support.commands.profiles import (
    cmd_use_code as _cmd_use_code,
)
from amx.cli_support.commands.profiles import (
    cmd_use_doc as _cmd_use_doc,
)
from amx.cli_support.commands.profiles import (
    cmd_use_llm as _cmd_use_llm,
)
from amx.cli_support.commands.profiles import (
    cmd_use_rag_llm as _cmd_use_rag_llm,
)
from amx.cli_support.commands.style import cmd_style as _cmd_style
from amx.cli_support.commands.usage import cmd_usage as _cmd_usage
from amx.cli_support.slash_commands import (
    cmd_heads_for_namespace as _registry_cmd_heads,
)
from amx.cli_support.slash_commands import (
    commands_for_namespace as _registry_commands_for_namespace,
)
from amx.config import AMXConfig
from amx.utils.console import PromptCancelled, console, error, heading, info, success, warn

LogEvent = Callable[..., None]
WarnNoPaths = Callable[..., None]
NormalizeArgs = Callable[[list[str], AMXConfig], list[str]]
PrintDbHint = Callable[[], None]


def _rebuild_prompt_input() -> None:
    """Force prompt_toolkit to drop its cached ``Vt100Input``.

    Background: ``PromptSession`` constructs an ``Input`` once at
    startup and reuses it for every ``prompt()`` call (the instance
    caches ``_fileno``, a ``PosixStdinReader``, and a ``Vt100Parser``).
    When an external subprocess such as ``/studio`` runs between
    iterations and leaves stdin in a weird state, the next call to
    ``Vt100Input.raw_mode().__enter__`` hits ``termios.tcgetattr`` —
    which on error **silently no-ops** — and the terminal stays in
    cooked mode. ICANON then buffers arrow-key escapes and the user
    sees ``^[[C`` echo until the CLI is restarted.

    The fix is to invalidate the AppSession's cached input so the
    next ``prompt()`` rebuilds it via ``create_input()``, which
    snapshots a fresh fileno from ``sys.stdin`` and constructs new
    reader/parser objects. We also do a defensive ``tcflush`` to
    drain any stale bytes the subprocess may have left in the
    kernel-side input queue.

    Cheap (one syscall + one attribute set); safe to call every
    iteration of the prompt loop.
    """
    # Flush kernel-side stale input bytes. Platform/tty guarded so
    # it's a no-op on Windows / piped input / detached tty.
    try:
        import os as _os
        import sys as _sys
        import termios as _termios

        fd = _sys.stdin.fileno()
        if _os.isatty(fd):
            _termios.tcflush(fd, _termios.TCIFLUSH)
    except (ImportError, AttributeError, ValueError, OSError):
        pass
    except Exception:  # pragma: no cover - termios.error on some platforms
        pass

    # Drop the AppSession's cached Vt100Input. ``_input`` is the
    # documented (via source) cache slot; setting it to ``None``
    # triggers a fresh ``create_input()`` on next access.
    try:
        from prompt_toolkit.application.current import get_app_session

        app_session = get_app_session()
        # Lazy property — setting to None invalidates the cache so the
        # next read recreates a Vt100Input from sys.stdin.
        app_session._input = None  # type: ignore[assignment]
    except Exception:  # pragma: no cover - prompt_toolkit internals shift
        pass


def _canonical_namespace(namespace: str) -> str:
    return "metadata" if namespace == "manual" else namespace


def _handle_manual_usage_shortcuts(namespace: str, parts: list[str]) -> bool:
    """Show guided metadata-edit workflow for incomplete edit commands."""
    return False


def _did_you_mean(token: str) -> str:
    """Return a ' Did you mean /X?' suffix for a mistyped command, else ''.

    Matches the offending token against every known command head + namespace
    so a typo (e.g. /dbb, /db tabels) gets a pointer instead of a flat
    'Unknown command'. The candidate set is the single-source slash-command
    registry, so it stays correct as commands are added/renamed.
    """
    import difflib

    cleaned = (token or "").lstrip("/").strip()
    if not cleaned:
        return ""
    head = cleaned.split()[0]
    from amx.cli_support.slash_commands import ALL_COMMANDS, all_namespaces

    candidates = {c.head for c in ALL_COMMANDS}
    candidates.update(all_namespaces())
    matches = difflib.get_close_matches(head, sorted(candidates), n=1, cutoff=0.6)
    return f" Did you mean /{matches[0]}?" if matches else ""


def _format_session_click_error(cmdline: str, exc: click.ClickException) -> str:
    """Render slash-session-friendly Click errors."""
    if isinstance(exc, click.UsageError) and "No such command" in str(exc):
        import re as _re

        m = _re.search(r"No such command '([^']+)'", str(exc))
        bad = m.group(1) if m else cmdline
        return f"Unknown command: /{cmdline}.{_did_you_mean(bad)} Type /help."
    return str(exc)


class _SlashCompleter(Completer):
    def __init__(self, namespace_cb: Callable[[], str], cfg: AMXConfig):
        self._namespace_cb = namespace_cb
        self._cfg = cfg

    def get_completions(self, document, complete_event):
        text = document.text_before_cursor
        if not text.startswith("/"):
            return

        namespace = self._namespace_cb()
        partial = text[1:]
        for cmd, meta in _slash_command_catalog(namespace, self._cfg):
            if cmd[1:].startswith(partial):
                yield Completion(cmd, start_position=-len(text), display_meta=meta)


def _slash_command_catalog(namespace: str, cfg: AMXConfig) -> list[tuple[str, str]]:
    """Return ``(slash_command, short_description)`` pairs for autocomplete.

    Pre-v0.9.3 this function carried hand-maintained lists for every
    namespace — the same data was also duplicated in the dispatch
    chain's ``*_cmd_heads`` frozensets, in ``_print_session_help``
    blocks, and in ``run_interactive_session``. The
    ``amx.cli_support.slash_commands`` registry is now the single
    source of truth; this function is just an adapter that converts
    :class:`SlashCommand` records into the ``(slash, desc)`` tuples
    the autocompleter expects.
    """
    canonical = _canonical_namespace(namespace) if namespace else ""
    cmds = _registry_commands_for_namespace(canonical or namespace)
    return [(c.command, c.short_desc) for c in cmds]


def _require_namespace(cmd: str, namespace: str, expected: str, replacement: str) -> bool:
    """Allow cross-namespace slash commands.

    Slash commands carry their own namespace in the name (e.g. ``/llm-profiles``,
    ``/db-profiles``). Refusing to execute them just because the user happens to
    be in a different tab is friction without value — every handler operates on
    ``cfg`` and doesn't care about the current namespace. We still emit a one-
    line note when the command is dispatched cross-namespace, so the user can
    learn the canonical home if they didn't already know it.
    """
    if namespace and namespace != expected:
        info(f"Running /{cmd} from /{namespace} (canonical home: /{expected}).")
    return True


def _run_ask_repl(
    cfg: AMXConfig,
    *,
    main_command: click.Group,
    log_event: LogEvent,
) -> None:
    """Drop into a sticky ``ask>`` REPL when ``/ask`` is typed alone.

    Each non-empty line is dispatched as a ``/search ask <line>`` invocation,
    re-using the same conversational session pointer (``cfg.active_chat_session_id``)
    so follow-up turns ("any others?", "what about its columns?") are linked.

    Exits on ``/exit``, ``/quit``, ``/back``, an empty line + Ctrl-D, or Ctrl-C.
    Any other line that begins with ``/`` is rejected with a hint — REPL mode is
    deliberately question-only so users don't accidentally run unrelated CLI
    commands while mid-conversation.
    """
    sid = cfg.active_chat_session_id
    sid_label = f"#{sid}" if sid else "new"
    heading(f"Ask mode (session {sid_label})")
    info("Type a question, press Enter. /exit (or Ctrl-D on an empty line) to leave.")
    # Mirror the active session id into the environment BEFORE the first
    # ``main_command.main()`` so ``AMXConfig.load`` picks it up. Without this
    # bridge each invocation re-opens a fresh chat session and follow-ups
    # lose prior context. We clear the variable when the user explicitly
    # /resume's a different session — handled inside the agent on next set.
    if sid:
        os.environ["AMX_CHAT_SESSION_ID"] = str(int(sid))
    else:
        # Brand-new REPL — drop any stale id so a fresh session is created
        # on the first question.
        os.environ.pop("AMX_CHAT_SESSION_ID", None)

    ask_kb = KeyBindings()

    @ask_kb.add("escape", eager=True)
    def _(event) -> None:  # type: ignore[no-untyped-def]
        # Esc with text in the buffer clears the line; on an empty
        # prompt it leaves ask mode (mirrors the main session's Esc-back
        # convention so users don't have to remember /exit).
        buf = event.app.current_buffer
        if buf.text:
            buf.reset()
        else:
            event.app.exit(result="__amx_ask_exit__")

    @ask_kb.add("c-c")
    def _(event) -> None:  # type: ignore[no-untyped-def]
        # Ctrl-C with text clears the line; on an empty prompt it
        # leaves ask mode rather than re-drawing a fresh ask> forever.
        buf = event.app.current_buffer
        if buf.text:
            buf.reset()
        else:
            event.app.exit(result="__amx_ask_exit__")

    inner = PromptSession(
        message=HTML("<ansicyan><b>ask&gt;</b></ansicyan> "),
        mouse_support=False,
        key_bindings=ask_kb,
    )
    while True:
        try:
            raw = inner.prompt()
        except EOFError:
            # Ctrl-D on an empty ask prompt drops back to the main session.
            console.print()
            success("Left ask mode.")
            return
        except KeyboardInterrupt:
            # Defensive: the c-c binding above handles Ctrl-C, but if a
            # nested context re-raises we still want to exit cleanly.
            console.print()
            success("Left ask mode.")
            return

        if raw == "__amx_ask_exit__":
            success("Left ask mode.")
            return

        line = (raw or "").strip()
        if not line:
            continue
        # Allow the user to escape the REPL with familiar slash verbs without
        # needing to remember "press Ctrl-D on an empty line".
        if line in {"/exit", "/quit", "/q", "/back", "exit", "quit", "q", "back"}:
            success("Left ask mode.")
            return
        if line.startswith("/"):
            warn(
                "Inside /ask only questions are accepted. /exit to leave, "
                "then run any slash command from the main prompt."
            )
            continue

        previous = os.environ.get("AMX_SESSION_CHILD")
        os.environ["AMX_SESSION_CHILD"] = "1"
        try:
            main_command.main(args=["search", "ask", line], prog_name="amx", standalone_mode=False)
        except click.ClickException as exc:
            error(_format_session_click_error(f"ask {line}", exc))
        except KeyboardInterrupt:
            # Ctrl-C cancels the running ask question but keeps the
            # ask REPL alive — see the matching block in ``run_session``.
            console.print()
            info("Interrupted. Back at the ask prompt.")
        except SystemExit:
            pass
        except Exception as exc:  # pragma: no cover
            error(f"Ask failed: {exc}")
        finally:
            if previous is None:
                os.environ.pop("AMX_SESSION_CHILD", None)
            else:
                os.environ["AMX_SESSION_CHILD"] = previous


def _handle_session_builtin(
    cfg: AMXConfig,
    namespace: str,
    parts: list[str],
    *,
    log_event: LogEvent,
) -> bool | str:
    head = parts[0]
    if head in {"profiles", "use", "add-profile", "remove-profile"}:
        error(
            f"/{head} was renamed — use /db (then /db-profiles, /use-db, /add-db-profile, /remove-db-profile)."
        )
        return True
    if head == "mcp":
        if not _require_namespace(head, namespace, "llm", "mcp"):
            return True
        _cmd_mcp(cfg, parts[1:])
        return True
    if head == "llm-profiles":
        if not _require_namespace(head, namespace, "llm", "llm-profiles"):
            return True
        _cmd_llm_profiles(cfg)
        return True
    if head == "use-llm":
        if not _require_namespace(head, namespace, "llm", "use-llm"):
            return True
        _cmd_use_llm(cfg, parts[1:])
        return True
    if head == "use-rag-llm":
        if not _require_namespace(head, namespace, "llm", "use-rag-llm"):
            return True
        _cmd_use_rag_llm(cfg, parts[1:])
        return True
    if head == "add-llm-profile":
        if not _require_namespace(head, namespace, "llm", "add-llm-profile"):
            return True
        _cmd_add_llm_profile(cfg, parts[1:])
        return True
    if head == "remove-llm-profile":
        if not _require_namespace(head, namespace, "llm", "remove-llm-profile"):
            return True
        _cmd_remove_llm_profile(cfg, parts[1:])
        return True
    if head == "prompt-detail":
        if not _require_namespace(head, namespace, "llm", "prompt-detail"):
            return True
        _cmd_prompt_detail(cfg, parts[1:])
        return True
    if head == "description-verbosity":
        if not _require_namespace(head, namespace, "llm", "description-verbosity"):
            return True
        _cmd_description_verbosity(cfg, parts[1:])
        return True
    if head == "n-alternatives":
        if not _require_namespace(head, namespace, "llm", "n-alternatives"):
            return True
        _cmd_n_alternatives(cfg, parts[1:])
        return True
    if head == "confidence-signal":
        if not _require_namespace(head, namespace, "llm", "confidence-signal"):
            return True
        _cmd_confidence_signal(cfg, parts[1:])
        return True
    if head == "alternatives-mode":
        if not _require_namespace(head, namespace, "llm", "alternatives-mode"):
            return True
        _cmd_alternatives_mode(cfg, parts[1:])
        return True
    if head == "style":
        if not _require_namespace(head, namespace, "llm", "style"):
            return True
        _cmd_style(cfg, parts[1:])
        return True
    if head == "llm-batch-size":
        if not _require_namespace(head, namespace, "llm", "llm-batch-size"):
            return True
        _cmd_llm_batch_size(cfg, parts[1:])
        return True
    if head == "batch-context-columns":
        if not _require_namespace(head, namespace, "llm", "batch-context-columns"):
            return True
        _cmd_batch_context_columns(cfg, parts[1:])
        return True
    if head == "logprob-thresholds":
        if not _require_namespace(head, namespace, "llm", "logprob-thresholds"):
            return True
        _cmd_logprob_thresholds(cfg, parts[1:])
        return True
    if head == "temperature":
        if not _require_namespace(head, namespace, "llm", "temperature"):
            return True
        _cmd_temperature(cfg, parts[1:])
        return True
    if head in ("max-tokens", "max_tokens"):
        if not _require_namespace(head, namespace, "llm", "max-tokens"):
            return True
        _cmd_max_tokens(cfg, parts[1:])
        return True
    if head == "cost":
        if not _require_namespace(head, namespace, "llm", "cost"):
            return True
        _cmd_cost(cfg, parts[1:])
        return True
    if head in ("refresh-prices", "refresh_prices"):
        if not _require_namespace(head, namespace, "llm", "refresh-prices"):
            return True
        _cmd_refresh_prices(cfg, parts[1:])
        return True
    if head == "doc-profiles":
        if not _require_namespace(head, namespace, "docs", "doc-profiles"):
            return True
        _cmd_doc_profiles(cfg)
        return True
    if head == "doc-files":
        if not _require_namespace(head, namespace, "docs", "doc-files"):
            return True
        _cmd_doc_files(cfg, parts[1:])
        return True
    if head == "use-doc":
        if not _require_namespace(head, namespace, "docs", "use-doc"):
            return True
        _cmd_use_doc(cfg, parts[1:])
        return True
    if head == "add-doc-profile":
        if not _require_namespace(head, namespace, "docs", "add-doc-profile"):
            return True
        _cmd_add_doc_profile(cfg, parts[1:])
        return True
    if head == "remove-doc-profile":
        if not _require_namespace(head, namespace, "docs", "remove-doc-profile"):
            return True
        _cmd_remove_doc_profile(cfg, parts[1:])
        return True
    if head == "doc-link":
        if not _require_namespace(head, namespace, "docs", "doc-link"):
            return True
        _cmd_doc_link(cfg, parts[1:])
        return True
    if head == "code-profiles":
        if not _require_namespace(head, namespace, "code", "code-profiles"):
            return True
        _cmd_code_profiles(cfg)
        return True
    if head == "use-code":
        if not _require_namespace(head, namespace, "code", "use-code"):
            return True
        _cmd_use_code(cfg, parts[1:])
        return True
    if head == "add-code-profile":
        if not _require_namespace(head, namespace, "code", "add-code-profile"):
            return True
        _cmd_add_code_profile(cfg, parts[1:])
        return True
    if head == "remove-code-profile":
        if not _require_namespace(head, namespace, "code", "remove-code-profile"):
            return True
        _cmd_remove_code_profile(cfg, parts[1:])
        return True
    if head == "code-link":
        if not _require_namespace(head, namespace, "code", "code-link"):
            return True
        _cmd_code_link(cfg, parts[1:])
        return True
    if head == "ask-context":
        # /ask-context lives under /search (the same namespace as /ask)
        # so the user can run it mid-session without switching contexts.
        if not _require_namespace(head, namespace, "search", "ask-context"):
            return True
        _cmd_ask_context(cfg)
        return True
    if head == "db-profiles":
        if not _require_namespace(head, namespace, "db", "db-profiles"):
            return True
        _cmd_profiles(cfg)
        return True
    if head == "use-db":
        if not _require_namespace(head, namespace, "db", "use-db"):
            return True
        _cmd_use(cfg, parts[1:], log_event=log_event)
        return True
    if head == "add-db-profile":
        if not _require_namespace(head, namespace, "db", "add-db-profile"):
            return True
        _cmd_add_profile(cfg, parts[1:], log_event=log_event)
        return True
    if head == "edit-db-profile":
        if not _require_namespace(head, namespace, "db", "edit-db-profile"):
            return True
        _cmd_edit_profile(cfg, parts[1:], log_event=log_event)
        return True
    if head == "remove-db-profile":
        if not _require_namespace(head, namespace, "db", "remove-db-profile"):
            return True
        _cmd_remove_profile(cfg, parts[1:])
        return True
    if head == "profiling":
        if not _require_namespace(head, namespace, "db", "profiling"):
            return True
        _cmd_profiling(cfg, parts[1:])
        return True
    if head == "tls":
        if not _require_namespace(head, namespace, "db", "tls"):
            return True
        from amx.cli_support.commands.db import cmd_tls as _cmd_tls

        _cmd_tls(cfg, parts[1:])
        return True
    if head == "inspect":
        # Lives under /db so it shares the namespace of the other DB
        # commands. /inspect [profile] dumps backend, connection summary,
        # capabilities, connection-test result, and per-schema table counts
        # so users can self-diagnose connector and permission problems.
        if not _require_namespace(head, namespace, "db", "inspect"):
            return True
        _cmd_inspect(cfg, parts[1:])
        return True
    if head == "cache-show":
        if not _require_namespace(head, namespace, "db", "cache-show"):
            return True
        _cmd_cache_show(cfg, parts[1:])
        return True
    if head == "cache-stats":
        if not _require_namespace(head, namespace, "db", "cache-stats"):
            return True
        _cmd_cache_stats(cfg, parts[1:])
        return True
    if head == "cache-clear":
        if not _require_namespace(head, namespace, "db", "cache-clear"):
            return True
        _cmd_cache_clear(cfg, parts[1:])
        return True
    if head == "sync-stop":
        if not _require_namespace(head, namespace, "db", "sync-stop"):
            return True
        from amx.cli_support.commands.db import cmd_sync_stop as _cmd_sync_stop

        _cmd_sync_stop(cfg, parts[1:])
        return True
    if head == "comment-local":
        if not _require_namespace(head, namespace, "db", "comment-local"):
            return True
        from amx.cli_support.commands.db import cmd_comment_local as _cmd_comment_local

        _cmd_comment_local(cfg, parts[1:])
        return True
    if head == "save":
        path = cfg.save()
        success(f"Saved configuration to {path}")
        return True
    if head == "restore-config":
        from amx.cli_support.commands.restore_config import cmd_restore_config

        cmd_restore_config(cfg, parts[1:])
        return True
    if head == "usage":
        # Top-level: /usage summarises LLM cost from local history without
        # requiring the user to enter any namespace first. No network call.
        _cmd_usage(cfg, parts[1:])
        return True
    if head in {"embeddings", "embedding"}:
        # Lives under /search since switching the embedding provider only
        # affects the search index. ``embedding`` (singular) is accepted as
        # a typo-friendly alias. When typed at the root tab, the auto-
        # namespace logic (search_cmd_heads) shifts the user into /search
        # and prints "Assumed /search namespace for this command." — the
        # same UX as /add-db-profile.
        if not _require_namespace(head, namespace, "search", head):
            return True
        _cmd_embeddings(cfg, parts[1:])
        return True
    return False


#: Top-level Click namespaces (the ``/db``, ``/llm``, … tabs) plus
#: every command registered as a sibling Click subcommand at the same
#: level. ``head`` matches against this set so a typed
#: ``/<head> <subcmd>`` from any tab dispatches directly to the right
#: namespace's Click group — no per-namespace whitelist drift. Tabs
#: GROUP commands for discovery; this set keeps every command
#: dispatchable from any tab.
_CROSS_NAMESPACE_HEADS: frozenset[str] = frozenset(
    {
        "db",
        "metadata",
        "manual",  # alias → metadata
        "docs",
        "llm",
        "code",
        "analyze",
        "search",
        "history",
        "session",
        "setup",
        "config",
        "studio",
        "lineage",
        "pages",
        # /admin is now wired (register_admin_commands is called in cli.py);
        # without this it stayed a dead tab — advertised everywhere but
        # "Unknown command" when entered.
        "admin",
        # Real top-level Click commands that were reachable via `amx rerun`
        # / `amx variations` but returned None here (→ "Unknown command")
        # when typed in the REPL, because they were missing from this set.
        "rerun",
        "variations",
    }
)


def session_to_click_args(namespace: str, parts: list[str]) -> list[str] | None:
    head = parts[0]
    shortcut_map = {
        "connect": ["db", "connect"],
        "schemas": ["db", "schemas"],
        "tables": ["db", "tables"],
        "profile": ["db", "profile"],
        "index": ["docs", "index"],
        "search-docs": ["docs", "search-docs"],
        "doc-add": ["docs", "add"],
        "doc-analyze": ["docs", "analyze"],
        "export-doc-report": ["docs", "export-report"],
        "inspect": ["metadata", "inspect"],
        "edit": ["metadata", "edit"],
        "monitor": ["metadata", "monitor"],
        "run": ["analyze", "run"],
        "run-apply": ["analyze", "run", "--apply"],
        "apply": ["analyze", "apply"],
        # PR A — bulk-review UX. /review delegates to `analyze review`
        # (filter / sort / group viewer over a completed run's
        # suggestions). Routed here so it's reachable from any tab.
        "review": ["analyze", "review"],
        "code-index": ["code", "index"],
        "code-search": ["code", "search"],
        "code-results": ["code", "results"],
        "export-code-report": ["code", "export-report"],
        "ask": ["search", "ask"],
        "status": ["search", "status"],
        "sources": ["search", "sources"],
        "sync": ["search", "sync"],
        "rebuild": ["search", "rebuild"],
        "setup": ["setup"],
        "config": ["config"],
        "help": ["--help"],
        # /doctor is cross-namespace: registered as a top-level Click
        # subcommand and listed in _ROOT_BUILTINS with cross_namespace=True.
        # Without this entry, typing `/doctor` from /llm, /db, etc. would
        # fall through to `[namespace, "doctor"]` which Click rejects, and
        # from /search it would fall through to `["search", "ask",
        # "doctor"]` — sending the literal string "doctor" to the search
        # agent as a question, which silently "looks like it worked".
        "doctor": ["doctor"],
        # /compare lives under /history (audit operation, not search).
        # Same dispatch story as /doctor: from /search it'd otherwise
        # be swallowed as a question, from /db etc. it'd hit Click as
        # an unknown subcommand. The shortcut maps it to the correct
        # namespace from anywhere.
        "compare": ["history", "compare"],
        # /history-store lives under /db (it manages a database
        # resource — a saved DB profile that hosts the AMX schema).
        # When typed from outside /db this shortcut routes it; when
        # typed from inside /db the namespace+head fallthrough below
        # already produces ["db", "history-store", ...].
        "history-store": ["db", "history-store"],
    }
    if head == "search" and len(parts) > 1:
        if parts[1] in {
            "ask",
            "status",
            "sources",
            "config",
            "sync",
            "rebuild",
            "find-columns",
            "join-candidates",
            "explain",
            "explain-table",
        }:
            return parts
        return ["search", "ask"] + parts[1:]
    if namespace == "search":
        if head in {"ask", "status", "sources", "config", "sync", "rebuild"}:
            return ["search"] + parts
        if head in {"find-columns", "join-candidates", "explain", "explain-table"}:
            return ["search"] + parts
        # Before swallowing the line as `/search ask <head>`, see if the
        # command is a known cross-namespace shortcut (e.g. /run, /apply,
        # /llm-profiles). If so, route it to the correct namespace instead
        # of asking the LLM to "interpret" it as a question.
        if head in shortcut_map:
            return shortcut_map[head] + parts[1:]
        # Single source of truth for cross-namespace dispatch — same
        # set the non-search branch below uses, so every command stays
        # reachable regardless of which tab the user is on. Tabs are
        # for grouping/discovery; routing is global.
        if head in _CROSS_NAMESPACE_HEADS:
            if head == "manual":
                return ["metadata"] + parts[1:]
            return parts
        # Unknown slash command typed inside /search. Bare-text questions are
        # already rewritten to ``/ask <text>`` upstream (see the input loop's
        # ``not raw.startswith("/")`` branch), so anything still landing here
        # has an explicit leading slash from the user — i.e. they meant a
        # command, not a question. Return ``None`` so the caller surfaces
        # "Unknown command: /<x>" instead of silently routing the typo into
        # the search agent (e.g. ``/asl`` → ``search ask asl``).
        return None
    if head in _CROSS_NAMESPACE_HEADS:
        if head == "manual":
            return ["metadata"] + parts[1:]
        return parts
    if namespace and head in shortcut_map:
        return shortcut_map[head] + parts[1:]
    if head in shortcut_map:
        return shortcut_map[head] + parts[1:]
    if namespace:
        return [namespace] + parts
    return None


def inject_session_defaults(cfg: AMXConfig, namespace: str, args: list[str]) -> list[str]:
    if not args:
        return args
    if args[:2] == ["db", "tables"] and len(args) == 2 and cfg.current_schema:
        return ["db", "tables", cfg.current_schema]
    if args[:2] == ["db", "profile"]:
        if len(args) == 2 and cfg.current_schema and cfg.current_table:
            return ["db", "profile", cfg.current_schema, cfg.current_table]
        if len(args) == 3 and cfg.current_table:
            return ["db", "profile", args[2], cfg.current_table]
    if (
        len(args) >= 2
        and args[0] == "code"
        and args[1] == "index"
        and "--schema" not in args
        and "-s" not in args
        and cfg.current_schema
    ):
        return args + ["--schema", cfg.current_schema]
    return args


def run_interactive_session(
    cfg: AMXConfig,
    *,
    version: str,
    main_command: click.Group,
    normalize_click_argv: NormalizeArgs,
    warn_no_doc_paths_for_scan_or_ingest: WarnNoPaths,
    print_interactive_startup_summary: Callable[[AMXConfig], None],
    print_db_namespace_hint: PrintDbHint,
    log_event: LogEvent,
    show_banner: Callable[..., None],
) -> None:
    _print_namespace_hint(
        "",
        cfg,
        version=version,
        print_interactive_startup_summary=print_interactive_startup_summary,
        print_db_namespace_hint=print_db_namespace_hint,
    )
    namespace = ""

    # Pre-v0.9.3 each cmd_heads frozenset was hand-maintained here AND
    # in ``_slash_command_catalog`` AND in ``_print_session_help`` —
    # drift between those caused the v0.6.1/0.6.2 description-verbosity
    # regressions. The ``amx.cli_support.slash_commands`` registry is
    # now the single source of truth; we just look up the head set per
    # namespace. The ``embeddings``/``embedding`` heads are not in the
    # registry as primary commands but still routed through /search.
    db_cmd_heads = _registry_cmd_heads("db")
    metadata_cmd_heads = _registry_cmd_heads("metadata")
    docs_cmd_heads = _registry_cmd_heads("docs")
    llm_cmd_heads = _registry_cmd_heads("llm")
    code_cmd_heads = _registry_cmd_heads("code")
    analyze_cmd_heads = _registry_cmd_heads("analyze")
    search_cmd_heads = _registry_cmd_heads("search") | frozenset({"embeddings", "embedding"})
    history_cmd_heads = _registry_cmd_heads("history")
    lineage_cmd_heads = _registry_cmd_heads("lineage")
    pages_cmd_heads = _registry_cmd_heads("pages")

    # SIGWINCH (terminal resize) is POSIX-only — Windows raises
    # AttributeError on signal.SIGWINCH. Guard so the interactive session
    # starts on Windows; the save/restore is purely defensive against
    # prompt_toolkit installing its own handler.
    _sigwinch = getattr(signal, "SIGWINCH", None)
    prev_sigwinch = signal.getsignal(_sigwinch) if _sigwinch is not None else None

    def _toolbar() -> HTML:
        ns = namespace or "root"
        schema_ctx = cfg.current_schema or "—"
        table_ctx = cfg.current_table or "—"
        llm_short = f"{cfg.llm.provider}/{cfg.llm.model}" if cfg.llm.model else "—"
        return HTML(
            f"<b>AMX v{version}</b> │ "
            f"ns:<b>{ns}</b> │ "
            f"schema:<b>{schema_ctx}</b> table:<b>{table_ctx}</b> │ "
            f"llm:<b>{llm_short}</b> │ "
            "<b>↑↓</b> navigate · <b>Esc</b> back · <b>Ctrl+C</b> exit"
        )

    session = PromptSession(
        completer=_SlashCompleter(lambda: namespace, cfg),
        key_bindings=_kb_escape_namespace(),
        mouse_support=False,
        bottom_toolbar=_toolbar,
        complete_while_typing=True,
        complete_style=CompleteStyle.COLUMN,
        style=Style.from_dict(
            {
                "completion-menu": "bg:#1f1f1f",
                "completion-menu.completion": "fg:#ffffff bg:#2b2b2b",
                "completion-menu.completion.current": "fg:#ffffff bold bg:#0b5fff",
                "completion-menu.meta.completion": "fg:#e6e6e6 bg:#2b2b2b",
                "completion-menu.meta.completion.current": "fg:#ffffff bold bg:#0b5fff",
            }
        ),
    )

    def _build_prompt_message(ns: str) -> HTML:
        # Tabs are rendered separately via ``_print_tab_bar`` immediately
        # under the banner so they stay at a fixed line as the user moves
        # between namespaces. The prompt itself is just the input chevron
        # — keeping it short means prompt_toolkit's redraw on every
        # keystroke doesn't repaint a wide tab strip below the hint.
        del ns
        return HTML("<b>&gt;</b> ")

    try:
        while True:
            _NS_STATE["namespace"] = namespace
            try:
                raw = session.prompt(_build_prompt_message(namespace)).strip()
            except EOFError:
                # Ctrl-D on an empty prompt closes the session (standard
                # shell convention).
                console.print()
                success("Session closed.")
                return
            except KeyboardInterrupt:
                # Reached only if the c-c key binding didn't fire (e.g. a
                # nested prompt raised). Treat as a line reset so the user
                # is never stuck.
                console.print()
                continue

            # Pick up any changes Studio (or another CLI session) wrote
            # to ~/.amx/config.yml since the last prompt. Without this
            # the CLI's in-memory cfg stays stale until the user
            # restarts the session — see issue surfaced after PR #350
            # where a doc profile added in Studio was invisible in CLI.
            # Single stat() per prompt; load happens only on mtime
            # change so the steady-state cost is one syscall per turn.
            # Defensive try/except: a transient disk failure or a YAML
            # midway through being written must NEVER take down the
            # interactive prompt loop. The next call retries.
            try:
                cfg.reload_if_stale()
            except Exception as exc:  # pragma: no cover - defensive
                log_event(
                    "cli.cfg_reload_failed",
                    error=str(exc),
                )

            # Recover from any external subprocess (notably ``/studio``)
            # that may have left stdin in a weird state. prompt_toolkit
            # caches a single ``Vt100Input`` instance per AppSession,
            # whose ``raw_mode.__enter__`` silently no-ops on
            # ``termios.error`` — leaving the terminal in cooked mode
            # and ICANON-buffering arrow-key escapes as literal
            # ``^[[C``. Forcing the AppSession to rebuild its input
            # gives the next ``session.prompt()`` a fresh fd snapshot,
            # PosixStdinReader, and Vt100Parser. One TCIFLUSH + one
            # object replacement; harmless when nothing was wrong.
            _rebuild_prompt_input()

            if raw == "__amx_exit__":
                console.print()
                success("Session closed.")
                return
            if raw == "__amx_esc_back__":
                namespace = ""
                console.clear()
                show_banner(force=True)
                _print_namespace_hint(
                    namespace,
                    cfg,
                    version=version,
                    print_interactive_startup_summary=print_interactive_startup_summary,
                    print_db_namespace_hint=print_db_namespace_hint,
                )
                continue
            if raw == "__amx_esc_root__":
                continue
            if raw.startswith("__amx_switch_ns__:"):
                namespace = raw.split(":", 1)[1]
                console.clear()
                show_banner(force=True)
                _print_namespace_hint(
                    namespace,
                    cfg,
                    version=version,
                    print_interactive_startup_summary=print_interactive_startup_summary,
                    print_db_namespace_hint=print_db_namespace_hint,
                )
                continue
            if not raw:
                continue
            if not raw.startswith("/"):
                if namespace == "search":
                    raw = f"/ask {raw}"
                else:
                    warn("Use slash commands (example: /db, /connect, /run --schema sap_s6p)")
                    continue

            cmdline = raw[1:].strip()
            if not cmdline:
                continue
            if cmdline in {"exit", "quit", "q"}:
                success("Session closed.")
                return
            if cmdline == "clear":
                console.clear()
                show_banner(force=True)
                _print_namespace_hint(
                    namespace,
                    cfg,
                    version=version,
                    print_interactive_startup_summary=print_interactive_startup_summary,
                    print_db_namespace_hint=print_db_namespace_hint,
                )
                continue
            if cmdline in {"help", "?"}:
                _print_session_help(namespace=namespace, cfg=cfg)
                continue
            if cmdline == "back":
                namespace = ""
                console.clear()
                show_banner(force=True)
                _print_namespace_hint(
                    namespace,
                    cfg,
                    version=version,
                    print_interactive_startup_summary=print_interactive_startup_summary,
                    print_db_namespace_hint=print_db_namespace_hint,
                )
                continue
            if cmdline in {
                "db",
                "metadata",
                "manual",
                "docs",
                "llm",
                "code",
                "analyze",
                "search",
                "history",
                "lineage",
                "pages",
            }:
                namespace = "metadata" if cmdline == "manual" else cmdline
                console.clear()
                show_banner(force=True)
                _print_namespace_hint(
                    namespace,
                    cfg,
                    version=version,
                    print_interactive_startup_summary=print_interactive_startup_summary,
                    print_db_namespace_hint=print_db_namespace_hint,
                )
                continue

            try:
                parts = shlex.split(cmdline)
            except ValueError as exc:
                error(f"Invalid command syntax: {exc}")
                continue
            if not parts:
                continue

            if not namespace:
                head = parts[0]
                if head in db_cmd_heads:
                    namespace = "db"
                    info("Assumed /db namespace for this command.")
                elif head in metadata_cmd_heads:
                    namespace = "metadata"
                    info("Assumed /metadata namespace for this command.")
                elif head in docs_cmd_heads:
                    namespace = "docs"
                    info("Assumed /docs namespace for this command.")
                elif head in llm_cmd_heads:
                    namespace = "llm"
                    info("Assumed /llm namespace for this command.")
                elif head in code_cmd_heads:
                    namespace = "code"
                    info("Assumed /code namespace for this command.")
                elif head in analyze_cmd_heads:
                    namespace = "analyze"
                    info("Assumed /analyze namespace for this command.")
                elif head in search_cmd_heads or head in {
                    "find-columns",
                    "join-candidates",
                    "explain",
                    "explain-table",
                }:
                    namespace = "search"
                    info("Assumed /search namespace for this command.")
                elif head in history_cmd_heads:
                    namespace = "history"
                    info("Assumed /history namespace for this command.")
                elif head in lineage_cmd_heads:
                    namespace = "lineage"
                    info("Assumed /lineage namespace for this command.")
                # Pages dispatched last so its generic heads (/list,
                # /show, /edit, /delete) don't shadow the same heads in
                # /history, /lineage, /metadata. Pages-unique heads
                # (/new, /export) still resolve here from root.
                elif head in pages_cmd_heads:
                    namespace = "pages"
                    info("Assumed /pages namespace for this command.")

            if namespace == "docs":
                if parts[0] == "search-docs" and len(parts) == 1:
                    error("Usage: /search-docs <text>")
                    info('Example: /search-docs What does field "BUKRS" mean in our docs?')
                    continue
                if parts[0] == "index" and len(parts) == 1 and not cfg.effective_doc_paths():
                    warn_no_doc_paths_for_scan_or_ingest(cfg, cmd="index")
                    continue
            if _handle_manual_usage_shortcuts(namespace, parts):
                continue

            # Special-case: bare "/ask" (no question) drops the user into a
            # sticky ask>-prompt REPL. We want this BEFORE the builtin/click
            # routing because Click would error out with
            # "Usage: /search ask <question>" otherwise.
            if parts == ["ask"]:
                _run_ask_repl(cfg, main_command=main_command, log_event=log_event)
                continue

            try:
                handled = _handle_session_builtin(cfg, namespace, parts, log_event=log_event)
            except PromptCancelled:
                # A prompt helper (ask/ask_choice/confirm/...) re-raised
                # because the user pressed Esc. Wizards no longer
                # silently absorb that with a "Cancelled." then keep
                # walking — bubble it out, print a single note, and
                # drop back to the namespace prompt with no partial
                # state saved.
                info("Cancelled — no changes were saved.")
                continue
            if handled == "exit":
                success("Session closed.")
                return
            if handled:
                continue

            args = session_to_click_args(namespace, parts)
            if args is None:
                error(f"Unknown command: /{cmdline}.{_did_you_mean(cmdline)} Type /help.")
                continue

            args = normalize_click_argv(args, cfg)
            args = inject_session_defaults(cfg, namespace, args)

            previous = os.environ.get("AMX_SESSION_CHILD")
            os.environ["AMX_SESSION_CHILD"] = "1"
            try:
                main_command.main(args=args, prog_name="amx", standalone_mode=False)
            except click.ClickException as exc:
                error(_format_session_click_error(cmdline, exc))
            except PromptCancelled:
                # Same Esc-aborts-the-wizard pathway as the builtin
                # dispatcher above, for Click-routed commands.
                info("Cancelled — no changes were saved.")
            except KeyboardInterrupt:
                # Ctrl-C interrupts a running command but must NOT exit
                # the session. KeyboardInterrupt is a BaseException
                # (not Exception), so the broad ``except Exception``
                # below would let it bubble up to Click which then
                # prints "Aborted!" and quits. Catch it explicitly,
                # drop a one-line note, and loop back to the namespace
                # prompt.
                console.print()
                info("Interrupted. Back at the prompt — /exit to leave the session.")
            except SystemExit:
                pass
            except Exception as exc:  # pragma: no cover
                # Surface the exception type alongside the message so a
                # bare ``[Errno 2] No such file or directory`` actually
                # tells the user it's a FileNotFoundError. Set
                # ``AMX_DEBUG=1`` for the full traceback when triaging.
                error(f"Command failed: {type(exc).__name__}: {exc}")
                if os.environ.get("AMX_DEBUG"):
                    import traceback as _tb

                    _tb.print_exc()
            finally:
                if previous is None:
                    os.environ.pop("AMX_SESSION_CHILD", None)
                else:
                    os.environ["AMX_SESSION_CHILD"] = previous
    finally:
        if _sigwinch is not None and prev_sigwinch is not None:
            signal.signal(_sigwinch, prev_sigwinch)
