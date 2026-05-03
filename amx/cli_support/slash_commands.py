"""Single-source-of-truth registry for AMX slash commands.

Before v0.9.3, the same slash command had to be listed in **four**
places inside ``amx/cli_support/session.py``:

1. ``_slash_command_catalog`` — autocomplete pairs (slash, short
   description).
2. ``*_cmd_heads`` frozensets in ``run_interactive_session`` — bare
   command names used by the dispatch chain.
3. ``_print_session_help`` — multi-line help text with numbered
   commands per namespace.
4. The dispatch ``if head in db_cmd_heads: namespace = "db"``
   ladder.

Drift between those four sources caused multiple regressions (the
``/description-verbosity`` command was added in v0.5.x but missed
appearing in autocomplete + help text until v0.6.1, etc.). The
registry collapses them into one Python data module — declarative,
type-checked, no YAML load at startup.

Adding a new slash command now means adding one entry here; the
session module derives autocomplete pairs, dispatch heads, and help
metadata automatically.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SlashCommand:
    """One AMX slash command + its metadata.

    ``command`` is the canonical form WITH leading slash (``"/db"``,
    ``"/run-apply"``). ``namespace`` is the dotted namespace this
    command lives under; commands available from every namespace
    (``/help``, ``/exit``, ``/clear``, ``/back``, ``/save``) use the
    ``cross_namespace`` flag instead.

    ``short_desc`` powers the autocomplete dropdown; if the command
    needs more space (the ``/help`` view), ``long_desc`` is used and
    falls back to ``short_desc`` when not set.
    """

    command: str
    namespace: str
    short_desc: str
    long_desc: str = ""
    aliases: tuple[str, ...] = ()
    cross_namespace: bool = False

    @property
    def head(self) -> str:
        """Bare command name without leading slash, e.g. ``"db-profiles"``."""
        return self.command.lstrip("/")

    @property
    def description(self) -> str:
        """Best description text — ``long_desc`` if set, else ``short_desc``."""
        return self.long_desc or self.short_desc


# ──────────────────────────────────────────────────────────────────────────
# Registry — order matters for autocomplete + help display.
# ──────────────────────────────────────────────────────────────────────────

_ROOT_BUILTINS: tuple[SlashCommand, ...] = (
    SlashCommand("/help", "", "Contextual help", cross_namespace=True),
    SlashCommand("/exit", "", "Exit session", cross_namespace=True),
    SlashCommand("/clear", "", "Clear terminal output", cross_namespace=True),
    SlashCommand("/back", "", "Return to root namespace", cross_namespace=True),
    SlashCommand("/save", "", "Save config to disk", cross_namespace=True),
    SlashCommand(
        "/doctor",
        "",
        "Diagnose install / config / connectivity",
        long_desc=(
            "Run the AMX doctor: PATH conflict detection (the ghost-profile "
            "bug class), config schema version, optional-dep imports, active "
            "DB + LLM connectivity. Use --skip-network for an offline quick check."
        ),
        cross_namespace=True,
    ),
)

_ROOT_ENTRYPOINTS: tuple[SlashCommand, ...] = (
    SlashCommand("/setup", "root", "Run setup wizard"),
    SlashCommand("/config", "root", "Show configuration"),
    SlashCommand("/db", "root", "Enter /db namespace"),
    SlashCommand("/docs", "root", "Enter /docs namespace"),
    SlashCommand(
        "/metadata",
        "root",
        "Enter /metadata namespace",
        aliases=("/manual",),
    ),
    SlashCommand("/manual", "root", "Alias for /metadata"),
    SlashCommand("/llm", "root", "Enter /llm namespace"),
    SlashCommand("/code", "root", "Enter /code namespace"),
    SlashCommand("/analyze", "root", "Enter /analyze namespace"),
    SlashCommand("/search", "root", "Enter /search namespace"),
    SlashCommand("/history", "root", "Enter /history namespace"),
    SlashCommand("/session", "root", "Manage /ask conversation sessions"),
)

_DB_COMMANDS: tuple[SlashCommand, ...] = (
    SlashCommand("/db-profiles", "db", "List DB profiles"),
    SlashCommand(
        "/use-db",
        "db",
        "Switch DB scope. Single: /use-db prod_pg. Multi (0.11+): /use-db prod_pg analytics_bq → persisted multi-profile scope for /ask /run /sync.",
    ),
    SlashCommand("/add-db-profile", "db", "Add profile — choose engine then connection details"),
    SlashCommand("/remove-db-profile", "db", "Remove DB profile (/remove-db-profile <name>)"),
    SlashCommand(
        "/profiling",
        "db",
        "Show/set profiling guardrails (/profiling [full|sampled|metadata] [max_rows|off] [sample_size])",
    ),
    SlashCommand("/tls", "db", "Show/set Databricks TLS settings (/tls [on|off] [ca_path|clear])"),
    SlashCommand("/schema", "db", "Set current schema (/schema <name>)"),
    SlashCommand("/table", "db", "Set current table (/table <name>)"),
    SlashCommand("/connect", "db", "Test DB connectivity"),
    SlashCommand("/schemas", "db", "List schemas"),
    SlashCommand("/tables", "db", "List tables (/tables [schema])"),
    SlashCommand("/profile", "db", "Profile table (/profile [schema] [table])"),
    SlashCommand(
        "/cleanup-placeholders",
        "db",
        "Remove auto-inference placeholder comments from live DB (/cleanup-placeholders [schema])",
    ),
    SlashCommand(
        "/history-store",
        "db",
        "Configure shared run-history (/history-store opens picker; /history-store status|enable|disable|migrate-from-local|flush-pending|dump-ddl)",
        long_desc=(
            "Configure shared run-history for team collaboration. Bare "
            "/history-store opens an interactive picker that prints the "
            "current shared-mode status and offers Status / Enable / "
            "Disable / Migrate from local / Flush pending / Dump DDL "
            "based on whether shared mode is on. Power-user shortcuts "
            "accept a subcommand directly."
        ),
    ),
)

_METADATA_COMMANDS: tuple[SlashCommand, ...] = (
    SlashCommand("/inspect", "metadata", "Inspect current metadata (/inspect [schema] [table])"),
    SlashCommand("/edit", "metadata", "Edit wizard or /edit <db>[.<schema>[.<table>[.<column>]]]"),
    SlashCommand("/monitor", "metadata", "Show metadata coverage (/monitor [schema])"),
)

_DOCS_COMMANDS: tuple[SlashCommand, ...] = (
    SlashCommand("/doc-profiles", "docs", "List document profiles"),
    SlashCommand("/use-doc", "docs", "Switch document profile (/use-doc <name>)"),
    SlashCommand("/add-doc-profile", "docs", "Add/update document profile"),
    SlashCommand(
        "/remove-doc-profile", "docs", "Remove document profile (/remove-doc-profile <name>)"
    ),
    SlashCommand("/scan", "docs", "Scan documents (/scan [--doc-profile NAME] [paths...])"),
    SlashCommand("/ingest", "docs", "Ingest (/ingest [--doc-profile NAME] [--refresh] [paths...])"),
    SlashCommand("/search-docs", "docs", "Similarity search (/search-docs <text>, no LLM)"),
    SlashCommand("/doc-analyze", "docs", "Run RAG Agent standalone (/doc-analyze [TABLE …])"),
    SlashCommand(
        "/export-doc-report", "docs", "Export doc RAG summary (/export-doc-report [FILE])"
    ),
)

_LLM_COMMANDS: tuple[SlashCommand, ...] = (
    SlashCommand("/llm-profiles", "llm", "List LLM profiles"),
    SlashCommand("/use-llm", "llm", "Switch LLM profile (/use-llm <name>)"),
    SlashCommand("/add-llm-profile", "llm", "Add/update LLM profile"),
    SlashCommand("/remove-llm-profile", "llm", "Remove LLM profile (/remove-llm-profile <name>)"),
    SlashCommand(
        "/prompt-detail",
        "llm",
        "Show/set prompt detail level (/prompt-detail [minimal|standard|detailed|full])",
    ),
    SlashCommand(
        "/description-verbosity",
        "llm",
        "Show/set output description length (/description-verbosity [brief|detailed])",
    ),
    SlashCommand(
        "/n-alternatives",
        "llm",
        "Show/set number of alternatives per column (/n-alternatives [1-5])",
    ),
    SlashCommand(
        "/llm-batch-size", "llm", "Show/set number of columns per LLM call (/llm-batch-size [N])"
    ),
    SlashCommand(
        "/batch-context-columns",
        "llm",
        "Show/set extra non-batch column names in each batch (/batch-context-columns [off|all|N])",
    ),
    SlashCommand(
        "/logprob-thresholds",
        "llm",
        "Show/set confidence thresholds (/logprob-thresholds [high] [med])",
    ),
    SlashCommand(
        "/temperature",
        "llm",
        "Show/set LLM sampling temperature (/temperature [0.0-2.0])",
    ),
)

_CODE_COMMANDS: tuple[SlashCommand, ...] = (
    SlashCommand("/code-profiles", "code", "List codebase profiles"),
    SlashCommand("/use-code", "code", "Switch codebase profile (/use-code <name>)"),
    SlashCommand("/add-code-profile", "code", "Add/update codebase profile"),
    SlashCommand(
        "/remove-code-profile", "code", "Remove codebase profile (/remove-code-profile <name>)"
    ),
    SlashCommand(
        "/code-scan", "code", "Scan codebase + save (/code-scan [path] [--code-profile NAME])"
    ),
    SlashCommand("/code-refresh", "code", "Clear cache + semantic code index"),
    SlashCommand("/code-results", "code", "Show last cached scan results"),
    SlashCommand("/code-analyze", "code", "Run Code Agent standalone (/code-analyze [TABLE …])"),
    SlashCommand(
        "/export-code-report", "code", "Export scan to markdown (/export-code-report [FILE])"
    ),
)

_ANALYZE_COMMANDS: tuple[SlashCommand, ...] = (
    SlashCommand(
        "/run",
        "analyze",
        "Run all agents — scope: database / schema / asset / column. Add --db-profile NAME (multi) for cross-DB execution (/run [ASSET …] [--schema …] [--apply] [--db-profile NAME …])",
    ),
    SlashCommand(
        "/run-apply", "analyze", "Run + apply (/run-apply [ASSET …] [--schema …] [--table …])"
    ),
    SlashCommand("/apply", "analyze", "Write pending comments to the database"),
)

_SEARCH_COMMANDS: tuple[SlashCommand, ...] = (
    SlashCommand(
        "/ask",
        "search",
        "Ask a metadata question; add --db-profile NAME (multi) for cross-DB scope; --actions for approved follow-up execution",
    ),
    SlashCommand("/status", "search", "Show catalog/index status"),
    SlashCommand("/sources", "search", "Show evidence sources and settings"),
    SlashCommand("/config", "search", "Show/set search config (/config [key] [value])"),
    SlashCommand(
        "/sync",
        "search",
        "Sync DB structure/comments and code evidence. Add --db-profile NAME (multi) for cross-DB sync (/sync [--db-profile NAME …])",
    ),
    SlashCommand("/rebuild", "search", "Rebuild effective search state and vector index"),
)

_HISTORY_COMMANDS: tuple[SlashCommand, ...] = (
    SlashCommand("/list", "history", "Show recent runs (/list -n 20)"),
    SlashCommand("/show", "history", "Show one run payload (/show <run_id>)"),
    SlashCommand("/stats", "history", "Aggregate run/event metrics"),
    SlashCommand("/events", "history", "Recent app events (/events -n 30)"),
    SlashCommand("/results", "history", "Show saved LLM alternatives (/results <run_id>)"),
    SlashCommand(
        "/review",
        "history",
        "Re-evaluate alternatives (/review <run_id> [--unevaluated-only] [--apply])",
    ),
    SlashCommand(
        "/compare",
        "history",
        "Compare past runs side-by-side (settings, descriptions, timing, tokens)",
        long_desc=(
            "Pivot past runs of the same assets so you can see how different LLM "
            "models, doc profiles, code profiles, prompt-detail levels, or batch "
            "sizes changed the descriptions, confidence, logprob_score, model "
            "processing time, and token usage. Lives under /history because "
            "comparing past runs is fundamentally an audit operation, not a "
            "search one. Examples: /compare --last 3, /compare 42 41 39, "
            "/compare --schema sales --table orders --by doc_profile."
        ),
    ),
)

# Search-namespace cross-cuts: extra commands callable from /search even
# though their handlers live elsewhere. Used by the dispatch chain to
# accept these without bouncing the user back to root.
_SEARCH_NAMESPACE_EXTRA_HEADS: frozenset[str] = frozenset(
    {
        "find-columns",
        "join-candidates",
        "explain",
        "explain-table",
    }
)


# Master tuple — every slash command in AMX. The order here is the
# order autocomplete shows for root.
ALL_COMMANDS: tuple[SlashCommand, ...] = (
    *_ROOT_BUILTINS,
    *_ROOT_ENTRYPOINTS,
    *_DB_COMMANDS,
    *_METADATA_COMMANDS,
    *_DOCS_COMMANDS,
    *_LLM_COMMANDS,
    *_CODE_COMMANDS,
    *_ANALYZE_COMMANDS,
    *_SEARCH_COMMANDS,
    *_HISTORY_COMMANDS,
)


# ──────────────────────────────────────────────────────────────────────────
# Public derivations — these replace the four hand-maintained sources in
# session.py.
# ──────────────────────────────────────────────────────────────────────────


def commands_for_namespace(namespace: str) -> tuple[SlashCommand, ...]:
    """Return the slash commands a user in ``namespace`` can use.

    Cross-namespace builtins (``/help``, ``/exit``, ``/clear``,
    ``/back``, ``/save``) are inserted at the top of every namespace
    listing. The root namespace gets the entry-point commands plus
    builtins; sub-namespaces get builtins + their own commands.
    """
    ns = (namespace or "").strip().lower()
    if ns in {"", "root"}:
        # Root: builtins (minus /back which is meaningless at root) +
        # entry-point commands.
        return (
            *(c for c in _ROOT_BUILTINS if c.command != "/back"),
            *_ROOT_ENTRYPOINTS,
        )
    if ns == "metadata" or ns == "manual":
        return (*_ROOT_BUILTINS, *_METADATA_COMMANDS)
    if ns == "db":
        return (*_ROOT_BUILTINS, *_DB_COMMANDS)
    if ns == "docs":
        return (*_ROOT_BUILTINS, *_DOCS_COMMANDS)
    if ns == "llm":
        return (*_ROOT_BUILTINS, *_LLM_COMMANDS)
    if ns == "code":
        return (*_ROOT_BUILTINS, *_CODE_COMMANDS)
    if ns == "analyze":
        return (*_ROOT_BUILTINS, *_ANALYZE_COMMANDS)
    if ns == "search":
        return (*_ROOT_BUILTINS, *_SEARCH_COMMANDS)
    if ns == "history":
        return (*_ROOT_BUILTINS, *_HISTORY_COMMANDS)
    return _ROOT_BUILTINS


def cmd_heads_for_namespace(namespace: str) -> frozenset[str]:
    """Return bare command heads (no leading ``/``) used by the dispatch chain.

    The dispatch ladder in ``run_interactive_session`` checks
    ``head in db_cmd_heads`` to decide which namespace a user just
    entered. With this helper it's a single registry lookup.
    """
    ns = (namespace or "").strip().lower()
    table = {
        "db": _DB_COMMANDS,
        "metadata": _METADATA_COMMANDS,
        "docs": _DOCS_COMMANDS,
        "llm": _LLM_COMMANDS,
        "code": _CODE_COMMANDS,
        "analyze": _ANALYZE_COMMANDS,
        "search": _SEARCH_COMMANDS,
        "history": _HISTORY_COMMANDS,
    }
    if ns not in table:
        return frozenset()
    heads = {c.head for c in table[ns]}
    if ns == "search":
        # Search has a few extra heads (find-columns, etc.) routed
        # through but not in the registry as primary commands.
        heads = heads | _SEARCH_NAMESPACE_EXTRA_HEADS
    return frozenset(heads)


def find_command(slash_or_head: str) -> SlashCommand | None:
    """Resolve a user-typed token to a registered :class:`SlashCommand`.

    Accepts both ``"/run"`` and ``"run"`` forms. Returns ``None`` when
    the token is unknown (so the caller can fall through to the legacy
    free-text handler).
    """
    token = (slash_or_head or "").strip().lower()
    if not token:
        return None
    candidate = token if token.startswith("/") else f"/{token}"
    for cmd in ALL_COMMANDS:
        if cmd.command == candidate or candidate in cmd.aliases:
            return cmd
    return None


def all_namespaces() -> tuple[str, ...]:
    """Distinct namespaces that have at least one command."""
    return ("db", "metadata", "docs", "llm", "code", "analyze", "search", "history")


__all__ = [
    "ALL_COMMANDS",
    "SlashCommand",
    "all_namespaces",
    "cmd_heads_for_namespace",
    "commands_for_namespace",
    "find_command",
]
