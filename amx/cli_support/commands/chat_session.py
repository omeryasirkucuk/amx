"""`/session` namespace — manage `/ask` conversational sessions.

Each `/ask` invocation appends to whichever session ``cfg.active_chat_session_id``
points to (lazily started on first ask). These commands let users explicitly
list past sessions, switch the active pointer, or close out the current one.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

import click

from amx.config import AMXConfig
from amx.search.session_store import ChatSessionStore
from amx.storage.sqlite_store import history_store
from amx.utils.console import error, info, render_table, success, warn

LogEvent = Callable[..., None]


def _store() -> ChatSessionStore | None:
    hs = history_store()
    if hs is None:
        return None
    return ChatSessionStore(hs)


def _fmt_ts(value: Any) -> str:
    try:
        ts = float(value or 0)
    except (TypeError, ValueError):
        return "—"
    if ts <= 0:
        return "—"
    return datetime.fromtimestamp(ts, tz=timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M")


def register_chat_session_commands(
    main: click.Group,
    *,
    pass_config: Callable[[Callable[..., Any]], Callable[..., Any]],
    log_event: LogEvent,
) -> None:
    """Attach `/session` namespace commands to the main Click group."""

    @main.group()
    def session() -> None:
        """Manage `/ask` conversation sessions (persistent, SQLite-backed)."""

    @session.command("new")
    @click.option(
        "--title", "title", default=None, help="Optional human-friendly label for this session."
    )
    @pass_config
    def session_new(cfg: AMXConfig, title: str | None) -> None:
        """Start a fresh chat session and make it the active one."""
        store = _store()
        if store is None:
            error("History store is not initialized; cannot start a session.")
            return
        # Multi-profile scope: seed the new session with whatever
        # cfg.effective_db_profiles() resolves to (cfg.active_db_profiles
        # set by /use-db, or the legacy single-active fallback). The
        # /ask-scope command can override this for the chat without
        # touching the persisted config-level scope.
        try:
            scope_profiles = list(cfg.effective_db_profiles()) or None
        except Exception:
            scope_profiles = None
        sid = store.start_session(
            db_profile=cfg.active_db_profile or "default",
            llm_profile=cfg.active_llm_profile or "default",
            title=title,
            scope_profiles=scope_profiles,
        )
        cfg.active_chat_session_id = sid
        success(f"Started chat session #{sid}." + (f" Title: {title!r}." if title else ""))

    @session.command("scope")
    @click.argument("profiles", nargs=-1)
    @pass_config
    def session_scope(cfg: AMXConfig, profiles: tuple[str, ...]) -> None:
        """Show or set the sticky scope for the active chat session.

        ``/session scope`` (no args) prints the current scope. ``/session
        scope clear`` resets to the config default. ``/session scope
        prod_pg analytics_bq`` pins multi-profile scope only for THIS
        chat (separate from the persisted ``/use-db`` scope).
        """
        store = _store()
        if store is None:
            error("History store is not initialized; cannot manage scope.")
            return
        sid = getattr(cfg, "active_chat_session_id", None) or 0
        if not sid:
            error("No active chat session. Run `/session new` first.")
            return
        if not profiles:
            current = store.get_scope(int(sid))
            if current:
                info(f"Sticky scope for session #{sid}: {', '.join(current)}")
            else:
                info(
                    f"Session #{sid} uses the config default ({len(cfg.db_profiles)} "
                    f"profile{'s' if len(cfg.db_profiles) != 1 else ''})."
                )
            return
        if profiles == ("clear",):
            store.update_scope(int(sid), scope_profiles=None)
            success(f"Cleared sticky scope on session #{sid}; back to config default.")
            return
        unknown = [p for p in profiles if p not in cfg.db_profiles]
        if unknown:
            error(
                f"Unknown DB profile(s): {', '.join(unknown)}. "
                f"Available: {', '.join(sorted(cfg.db_profiles)) or '(none)'}."
            )
            return
        store.update_scope(int(sid), scope_profiles=list(profiles))
        success(
            f"Sticky scope on session #{sid}: {', '.join(profiles)}. "
            "Subsequent /ask questions in this chat use this scope."
        )

    @session.command("list")
    @click.option(
        "-n",
        "--limit",
        default=20,
        show_default=True,
        help="How many sessions to list (most recent first).",
    )
    @click.option(
        "--all-profiles",
        "all_profiles",
        is_flag=True,
        help="Include sessions from other DB/LLM profile pairs.",
    )
    @click.option(
        "--include-ended",
        "include_ended",
        is_flag=True,
        default=True,
        help="Include closed sessions.",
    )
    @pass_config
    def session_list(cfg: AMXConfig, limit: int, all_profiles: bool, include_ended: bool) -> None:
        """List recent chat sessions for the active profile pair."""
        store = _store()
        if store is None:
            error("History store is not initialized.")
            return
        if all_profiles:
            sessions = store.list_sessions(limit=limit, include_ended=include_ended)
        else:
            sessions = store.list_sessions(
                db_profile=cfg.active_db_profile or "default",
                llm_profile=cfg.active_llm_profile or "default",
                limit=limit,
                include_ended=include_ended,
            )
        if not sessions:
            info("No chat sessions yet. Run `/ask <question>` to start one.")
            return
        active = cfg.active_chat_session_id
        rows: list[list[str]] = []
        for s in sessions:
            sid = int(s.get("id") or 0)
            first_q = str(s.get("first_question") or "")
            if len(first_q) > 60:
                first_q = first_q[:57] + "…"
            ended = s.get("ended_at")
            state = "active" if not ended else "closed"
            marker = "→" if sid == active else " "
            rows.append(
                [
                    f"{marker} {sid}",
                    _fmt_ts(s.get("started_at")),
                    _fmt_ts(s.get("last_active_at")),
                    state,
                    str(int(s.get("turn_count") or 0)),
                    str(s.get("title") or ""),
                    first_q or "(no questions yet)",
                ]
            )
        render_table(
            "Chat sessions",
            ["ID", "Started", "Last active", "State", "Turns", "Title", "First question"],
            rows,
        )

    @session.command("resume")
    @click.argument("session_id", type=int)
    @pass_config
    def session_resume(cfg: AMXConfig, session_id: int) -> None:
        """Make ``session_id`` the active session for follow-up `/ask`s."""
        store = _store()
        if store is None:
            error("History store is not initialized.")
            return
        meta = store.get_session(session_id)
        if not meta:
            error(f"No session #{session_id} found.")
            return
        # Refuse cross-profile resume to prevent stitching unrelated histories.
        active_db = cfg.active_db_profile or "default"
        active_llm = cfg.active_llm_profile or "default"
        if (
            str(meta.get("db_profile") or "") != active_db
            or str(meta.get("llm_profile") or "") != active_llm
        ):
            warn(
                f"Session #{session_id} belongs to profile "
                f"{meta.get('db_profile')!r}/{meta.get('llm_profile')!r}; "
                f"current is {active_db!r}/{active_llm!r}. Switch profiles first or pass --all-profiles to /session list."
            )
            return
        cfg.active_chat_session_id = int(session_id)
        success(f"Resumed chat session #{session_id} ({meta.get('turn_count', 0)} turns).")

    @session.command("end")
    @pass_config
    def session_end(cfg: AMXConfig) -> None:
        """Close the active session. Future `/ask` invocations start a new one."""
        store = _store()
        if store is None:
            error("History store is not initialized.")
            return
        sid = cfg.active_chat_session_id
        if not sid:
            info("No active chat session.")
            return
        store.end_session(int(sid))
        cfg.active_chat_session_id = None
        success(f"Closed chat session #{sid}.")

    @session.command("delete")
    @click.argument("session_id", type=int)
    @pass_config
    def session_delete(cfg: AMXConfig, session_id: int) -> None:
        """Drop a chat session and every turn it owns. Mirrors the
        Studio sidebar trash icon — use it to prune throwaway chats
        that clutter the picker. ``/session end`` only marks the
        session inactive; this hard-removes it.
        """
        store = _store()
        if store is None:
            error("History store is not initialized.")
            return
        meta = store.get_session(int(session_id))
        if not meta:
            error(f"No session #{session_id} found.")
            return
        store.delete_session(int(session_id))
        if int(cfg.active_chat_session_id or 0) == int(session_id):
            cfg.active_chat_session_id = None
        success(f"Deleted chat session #{session_id}.")

    @session.command("show")
    @click.option(
        "--id",
        "session_id",
        type=int,
        default=None,
        help="Session id; defaults to the active session.",
    )
    @click.option(
        "--include-compacted",
        "include_compacted",
        is_flag=True,
        help="Also display soft-deleted (compacted) turns.",
    )
    @pass_config
    def session_show(cfg: AMXConfig, session_id: int | None, include_compacted: bool) -> None:
        """Dump the conversation turns of a session for inspection."""
        store = _store()
        if store is None:
            error("History store is not initialized.")
            return
        sid = session_id or cfg.active_chat_session_id
        if not sid:
            info("No active chat session. Run `/session list` to pick one.")
            return
        meta = store.get_session(int(sid))
        if not meta:
            error(f"No session #{sid} found.")
            return
        turns = store.recent_turns(
            int(sid), include_summary=True, include_compacted=include_compacted
        )
        if not turns:
            info(f"Session #{sid} has no turns yet.")
            return
        rows: list[list[str]] = []
        for t in turns:
            role = str(t.get("role") or "")
            preview = str(t.get("question") or t.get("answer_summary") or "")
            if len(preview) > 80:
                preview = preview[:77] + "…"
            tables = ", ".join(str(x) for x in (t.get("tables") or []))
            rows.append(
                [
                    str(int(t.get("turn_index") or 0)),
                    role,
                    _fmt_ts(t.get("created_at")),
                    str(int(t.get("estimated_tokens") or 0)),
                    tables[:40],
                    preview,
                ]
            )
        title = (
            f"Session #{sid} — {meta.get('turn_count', 0)} turns"
            f", {meta.get('total_tokens', 0)} est. tokens"
        )
        render_table(title, ["#", "Role", "When", "Tokens", "Tables", "Content"], rows)
