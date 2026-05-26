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
    SlashCommand(
        "/restore-config",
        "",
        "Restore config.yml from a rotated backup",
        long_desc=(
            "Recovery tool when a recent save corrupted config.yml. Lists "
            "available `config.yml.bak.1..N` backups with timestamps; pick "
            "one and AMX rotates the current live file into a backup slot "
            "before restoring. Always reversible: the pre-restore state "
            "becomes the new .bak.1."
        ),
        cross_namespace=True,
    ),
)

_ROOT_ENTRYPOINTS: tuple[SlashCommand, ...] = (
    SlashCommand("/setup", "root", "Run setup wizard"),
    SlashCommand("/config", "root", "Show configuration"),
    SlashCommand("/db", "root", "Enter /db namespace"),
    SlashCommand("/llm", "root", "Enter /llm namespace"),
    SlashCommand("/docs", "root", "Enter /docs namespace"),
    SlashCommand("/code", "root", "Enter /code namespace"),
    SlashCommand(
        "/metadata",
        "root",
        "Enter /metadata namespace",
        aliases=("/manual",),
    ),
    SlashCommand("/manual", "root", "Alias for /metadata"),
    SlashCommand("/analyze", "root", "Enter /analyze namespace"),
    SlashCommand("/search", "root", "Enter /search namespace"),
    SlashCommand("/history", "root", "Enter /history namespace"),
    SlashCommand(
        "/lineage",
        "root",
        "Enter /lineage namespace",
        long_desc=(
            "Render and manage column-level lineage diagrams. AMX derives "
            "edges from cached schema introspection (FK constraints), parsed "
            "view DDL, and a name-match heuristic — no live DB call unless "
            "you explicitly confirm. Subcommands: create, list, open, "
            "refresh, delete, show."
        ),
    ),
    SlashCommand(
        "/pages",
        "root",
        "Enter /pages namespace",
        long_desc=(
            "Compose, edit, and export documentation pages backed by the "
            "active LLM. Attach DB / doc / lineage assets as context and "
            "ship the result as Markdown or PDF. Subcommands: new, list, "
            "show, edit, export, delete."
        ),
    ),
    SlashCommand(
        "/admin",
        "root",
        "Enter /admin namespace",
        long_desc=(
            "Workspace administration: member registry, role management, "
            "audit log, and session history. Subcommands: members, promote, "
            "demote, revoke, unrevoke, audit, sessions. Write commands "
            "require the admin role."
        ),
    ),
    SlashCommand(
        "/studio",
        "root",
        "Open AMX Studio in your browser",
        long_desc=(
            "Boot AMX Studio at http://127.0.0.1:<port> and open it in your "
            "default browser. Browse every database / catalog / schema / table / column, "
            "trigger /run + /apply jobs, and chat with /ask from one place. Ctrl-C in this "
            "terminal stops the server."
        ),
    ),
)

_DB_COMMANDS: tuple[SlashCommand, ...] = (
    # Profile management
    SlashCommand("/db-profiles", "db", "List DB profiles"),
    SlashCommand(
        "/use-db",
        "db",
        "Switch DB scope. Single: /use-db prod_pg. Multi (0.11+): /use-db prod_pg analytics_bq → persisted multi-profile scope for /ask /run /sync.",
    ),
    SlashCommand("/add-db-profile", "db", "Add profile — choose engine then connection details"),
    SlashCommand(
        "/edit-db-profile",
        "db",
        "Edit existing DB profile — current values prefilled, validates catalog/database against the live backend (/edit-db-profile [<name>])",
    ),
    SlashCommand("/remove-db-profile", "db", "Remove DB profile (/remove-db-profile <name>)"),
    # Profile settings
    SlashCommand(
        "/profiling",
        "db",
        "Show/set profiling guardrails (/profiling [full|sampled|metadata] [max_rows|off] [sample_size])",
    ),
    SlashCommand("/tls", "db", "Show/set Databricks TLS settings (/tls [on|off] [ca_path|clear])"),
    # Connection & inspection
    SlashCommand("/connect", "db", "Test DB connectivity"),
    SlashCommand("/schemas", "db", "List schemas"),
    SlashCommand("/tables", "db", "List tables (/tables [schema])"),
    SlashCommand("/profile", "db", "Profile table (/profile [schema] [table])"),
    SlashCommand(
        "/inspect",
        "db",
        "Diagnose a DB profile — backend, capabilities, connection test, visible schemas, table counts (/inspect [profile])",
    ),
    # Team collaboration
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
    # Cache management — explicit handles over the SQLite catalog
    # caches (schemas_cache, column_comments_cache, catalog_entities).
    SlashCommand(
        "/cache-show",
        "db",
        "Show DB cache contents (/cache-show [--profile=X] [--database=Y])",
        long_desc=(
            "Render per-(profile, database) row counts across the "
            "schemas_cache, column_comments_cache, and catalog_entities "
            "tables so the user can see exactly what AMX has indexed "
            "for each scope plus the most recent fetch timestamp."
        ),
    ),
    SlashCommand(
        "/cache-stats",
        "db",
        "Aggregate DB cache metrics (rows, oldest fetch, %% expired, TTL window)",
        long_desc=(
            "Per-cache totals: total rows, distinct profiles / databases, "
            "oldest and newest fetch timestamps, expired-row count under "
            "the active TTL window. The catalog_entities row block has "
            "no TTL (rewritten by /sync) so its expired-row count is "
            "always zero."
        ),
    ),
    SlashCommand(
        "/cache-clear",
        "db",
        "Flush DB caches (/cache-clear [--profile=X] [--database=Y] [--type=schemas|columns|catalog|all] [--force])",
        long_desc=(
            "DELETE rows from the requested cache tables. Without "
            "explicit scope flags AND without --force the command "
            "double-confirms before nuking every profile's cache. "
            "Default --type=all clears all three cache tables for the "
            "scope. Cache loss is reversible — the next live-DB read "
            "or /sync rebuilds the rows."
        ),
    ),
)

_METADATA_COMMANDS: tuple[SlashCommand, ...] = (
    SlashCommand("/inspect", "metadata", "Inspect current metadata (/inspect [schema] [table])"),
    SlashCommand("/monitor", "metadata", "Show metadata coverage (/monitor [schema])"),
    SlashCommand("/edit", "metadata", "Edit wizard or /edit <db>[.<schema>[.<table>[.<column>]]]"),
)

_DOCS_COMMANDS: tuple[SlashCommand, ...] = (
    SlashCommand("/doc-profiles", "docs", "List document profiles"),
    SlashCommand(
        "/doc-files",
        "docs",
        "Show files staged under a doc profile (/doc-files [<name>])",
        long_desc=(
            "Walks each local path in the profile and prints a table of "
            "files with sizes and last-modified times so you can confirm "
            "what's actually attached. Remote paths (http/s3/gs) are "
            "listed as '(remote)' — use /scan for their full inventory."
        ),
    ),
    SlashCommand("/use-doc", "docs", "Switch document profile (/use-doc <name>)"),
    SlashCommand("/add-doc-profile", "docs", "Add/update document profile"),
    SlashCommand(
        "/remove-doc-profile", "docs", "Remove document profile (/remove-doc-profile <name>)"
    ),
    SlashCommand(
        "/doc-link",
        "docs",
        "Link doc profile → DB profile(s) (/doc-link <doc-profile> [--db NAME …] [--clear])",
    ),
    SlashCommand(
        "/doc-add",
        "docs",
        "Drag-drop equivalent: /doc-add <profile> <file>... [--no-ingest]",
    ),
    SlashCommand(
        "/index",
        "docs",
        "Index documents: incremental ingest, rebuild on embedding change "
        "(/index [--doc-profile NAME] [paths...])",
    ),
    SlashCommand("/search-docs", "docs", "Similarity search (/search-docs <text>, no LLM)"),
    SlashCommand("/doc-analyze", "docs", "Run RAG Agent standalone (/doc-analyze [TABLE …])"),
    SlashCommand(
        "/export-doc-report", "docs", "Export doc RAG summary (/export-doc-report [FILE])"
    ),
)

_LLM_COMMANDS: tuple[SlashCommand, ...] = (
    # IDE integration
    SlashCommand(
        "/mcp",
        "llm",
        "Connect AMX's catalog to IDE code agents via MCP",
        long_desc=(
            "Expose AMX's read-only catalog (schemas, descriptions, join "
            "keys, lineage, docs/code) to an IDE code agent over the Model "
            "Context Protocol. Bare /mcp runs a wizard; subcommands: "
            "/mcp connect [ide] · /mcp status · /mcp snippet [ide] · "
            "/mcp disconnect [ide]. Supported IDEs: Cursor, Claude Desktop, "
            "VS Code."
        ),
    ),
    # Profile management
    SlashCommand("/llm-profiles", "llm", "List LLM profiles"),
    SlashCommand("/use-llm", "llm", "Switch LLM profile (/use-llm <name>)"),
    SlashCommand(
        "/use-rag-llm",
        "llm",
        "Pin a different LLM profile to the RAG agent (/use-rag-llm [<name>|none])",
    ),
    SlashCommand("/add-llm-profile", "llm", "Add/update LLM profile"),
    SlashCommand("/remove-llm-profile", "llm", "Remove LLM profile (/remove-llm-profile <name>)"),
    # Prompt control (input shape)
    SlashCommand(
        "/prompt-detail",
        "llm",
        "Show/set prompt detail level (/prompt-detail [minimal|standard|detailed|full])",
    ),
    SlashCommand(
        "/description-verbosity",
        "llm",
        "Show/set output description length (/description-verbosity [brief|detailed|comprehensive|exhaustive])",
    ),
    SlashCommand(
        "/n-alternatives",
        "llm",
        "Show/set number of alternatives per column (/n-alternatives [1-5])",
    ),
    SlashCommand(
        "/confidence-signal",
        "llm",
        "Show/set active per-alternative confidence scorer "
        "(/confidence-signal [none|logprob|self_consistency|self_decl|judge])",
    ),
    SlashCommand(
        "/alternatives-mode",
        "llm",
        "Show/set alternatives diversity mode (/alternatives-mode [semantic|lexical])",
    ),
    SlashCommand(
        "/style",
        "llm",
        "Reference table for description style",
        long_desc=(
            "Attach a reference table to the active LLM profile so AMX "
            "matches your description style on /run. Reads metadata only; "
            "never copies entity names. "
            "Subcommands: /style [wizard] · /style set <db>.<schema>.<table> · "
            "/style show · /style clear · /style on · /style off."
        ),
        cross_namespace=True,
    ),
    SlashCommand(
        "/temperature",
        "llm",
        "Show/set LLM sampling temperature (/temperature [0.0-2.0])",
    ),
    SlashCommand(
        "/max-tokens",
        "llm",
        "Show/set LLM output token budget (/max-tokens [N]) — reasoning models keep their floor",
    ),
    SlashCommand(
        "/cost",
        "llm",
        "Show/set per-1M-token cost override for the active profile "
        "(/cost [<input> <output> | reset])",
    ),
    SlashCommand(
        "/refresh-prices",
        "llm",
        "Re-fetch LLM prices from LiteLLM + OpenRouter (24h cache)",
    ),
    # Batching (throughput)
    SlashCommand(
        "/llm-batch-size", "llm", "Show/set number of columns per LLM call (/llm-batch-size [N])"
    ),
    SlashCommand(
        "/batch-context-columns",
        "llm",
        "Show/set extra non-batch column names in each batch (/batch-context-columns [off|all|N])",
    ),
    # Confidence
    SlashCommand(
        "/logprob-thresholds",
        "llm",
        "Show/set confidence thresholds (/logprob-thresholds [high] [med])",
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
        "/code-link",
        "code",
        "Link code profile → DB profile(s) (/code-link <code-profile> [--db NAME …] [--clear])",
    ),
    SlashCommand(
        "/code-index",
        "code",
        "Index codebase: scan table+column refs + build semantic index "
        "(/code-index [path] [--code-profile NAME])",
    ),
    SlashCommand(
        "/code-search",
        "code",
        "Similarity search over amx_code (/code-search <text> [--code-profile NAME], no LLM)",
    ),
    SlashCommand("/code-results", "code", "Show last cached scan results"),
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
    # PR A — bulk-review UX: a focused entry point for reviewing a
    # previously-completed run's suggestions with filter / sort /
    # status / group flags. Reads the pending-review queue + the
    # ``run_results`` rows for ``<run_id>`` (defaults to the most
    # recent completed run when omitted) and renders the same
    # Rich-table view the post-run summary uses.
    SlashCommand(
        "/review",
        "analyze",
        (
            "Review a run's suggestions (/review [<run_id>] "
            "[--filter REGEX] [--sort KEY] [--group-by schema|table] "
            "[--only-unreviewed] [--only-low-conf])"
        ),
        long_desc=(
            "Surfaces the suggestions of a completed analyze run with "
            "the same filter / sort / group vocabulary the Studio "
            "ResultsFilterBar uses. Without ``--filter`` and friends, "
            "the command renders every approved row in natural order.\n"
            "Flags:\n"
            "  --filter REGEX        case-insensitive regex against "
            "schema.table.column\n"
            "  --sort KEY            conf-asc | conf-desc | "
            "logprob-asc | logprob-desc | name-asc | status\n"
            "  --group-by KIND       none | schema | table\n"
            "  --only-unreviewed     drop rows already accepted / "
            "skipped / applied\n"
            "  --only-low-conf       keep rows with confidence < 0.7"
        ),
    ),
)

_SEARCH_COMMANDS: tuple[SlashCommand, ...] = (
    # Ask
    SlashCommand(
        "/ask",
        "search",
        "Ask a metadata question; add --db-profile NAME (multi) for cross-DB scope; --actions for approved follow-up execution",
    ),
    # Sessions — manage `/ask` conversational threads. Dispatch is
    # cross-namespace (works from any tab) like /doctor and /compare;
    # listing here groups it next to /ask where it conceptually belongs.
    SlashCommand(
        "/session",
        "search",
        "Manage /ask conversation sessions (/session list|resume <id>|new|end|scope [profiles])",
        long_desc=(
            "Manage chat sessions for /ask. Subcommands:\n"
            "  /session list                   — recent sessions (filter "
            "with -n N or --all-profiles)\n"
            "  /session resume <id>            — make ID the active session "
            "for follow-up /ask turns\n"
            "  /session new [--title TEXT]     — start a fresh session\n"
            "  /session end                    — close the active session\n"
            "  /session scope [profiles|clear] — sticky multi-profile scope "
            "for the active session"
        ),
    ),
    # Status
    SlashCommand("/status", "search", "Show catalog/index status"),
    SlashCommand("/sources", "search", "Show evidence sources and settings"),
    SlashCommand(
        "/ask-context",
        "search",
        "Show which doc/code profiles /ask will pull in for the current DB scope",
    ),
    # Configure
    SlashCommand("/config", "search", "Show/set search config (/config [key] [value])"),
    SlashCommand(
        "/embeddings",
        "search",
        "Embedding provider + health (/embeddings status | rebuild [side|all] | [minilm|openai|local] [model])",
    ),
    # Sync & rebuild
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

_PAGES_COMMANDS: tuple[SlashCommand, ...] = (
    SlashCommand(
        "/new",
        "pages",
        "Create a new documentation page (/new [--title T] [--intent X] [--asset KIND:REF …] [--source PATH …] [--no-generate])",
        long_desc=(
            "Bare /new walks a wizard: title prompt → DB / doc / lineage "
            "asset pickers → free-text intent → optional local source "
            "files → LLM composition. Power-user flags map 1:1 to the "
            "wizard steps so existing scripts keep working."
        ),
    ),
    SlashCommand("/list", "pages", "List active documentation pages"),
    SlashCommand(
        "/show",
        "pages",
        "Print the markdown body of a page (/show <page_id>)",
    ),
    SlashCommand(
        "/edit",
        "pages",
        "Open the page body in $EDITOR and save the result as a new revision (/edit <page_id> [--note TEXT])",
    ),
    SlashCommand(
        "/export",
        "pages",
        "Export a page as md or pdf (/export <page_id> --format md|pdf [--out PATH])",
    ),
    SlashCommand(
        "/delete",
        "pages",
        "Soft-delete a page (/delete <page_id> [--purge])",
        long_desc=(
            "Default soft-delete hides the page from /pages list but keeps "
            "every revision and source row. --purge hard-deletes the page "
            "and every related row from the history store; not reversible."
        ),
    ),
    SlashCommand(
        "/assign-profile",
        "pages",
        "Associate a db_profile with a page (/assign-profile [<slug>] [--profile <name>])",
        long_desc=(
            "Bare /assign-profile runs a wizard: page picker → profile picker "
            "→ confirm. Power-user shortcut: /assign-profile <slug> --profile <name>. "
            "Pass an empty --profile or omit it in the wizard to clear the field "
            "(marks the page as unscoped / cross-profile)."
        ),
    ),
)


_ADMIN_COMMANDS: tuple[SlashCommand, ...] = (
    SlashCommand(
        "/members",
        "admin",
        "List workspace members and their roles",
    ),
    SlashCommand(
        "/promote",
        "admin",
        "Promote a user to admin (/promote [username])",
        long_desc=(
            "Bare /promote opens a user picker then confirmation. "
            "Power-user shortcut: /promote <username>."
        ),
    ),
    SlashCommand(
        "/demote",
        "admin",
        "Demote an admin to viewer (/demote [username])",
        long_desc=(
            "Bare /demote opens a user picker then confirmation. "
            "Power-user shortcut: /demote <username>."
        ),
    ),
    SlashCommand(
        "/revoke",
        "admin",
        "Revoke a user, blocking future connections (/revoke [username])",
    ),
    SlashCommand(
        "/unrevoke",
        "admin",
        "Reinstate a revoked user (/unrevoke [username])",
    ),
    SlashCommand(
        "/audit",
        "admin",
        "Recent admin audit log (/audit [-n N] [--actor X] [--action Y])",
    ),
    SlashCommand(
        "/sessions",
        "admin",
        "Recent session connection events (/sessions [--since ISO] [-n N])",
    ),
)


_LINEAGE_COMMANDS: tuple[SlashCommand, ...] = (
    SlashCommand(
        "/create",
        "lineage",
        "Render a lineage diagram (/create <table> [--out PATH --format svg|png|jpg])",
        long_desc=(
            "Run extractors in cache-only mode by default. If anything would "
            "require a DB round-trip, AMX prints the cost and asks before "
            "fetching. --no-cache forces fresh; --cache-only refuses any DB "
            "hit; --prefetch auto-confirms the cache fill."
        ),
    ),
    SlashCommand("/list", "lineage", "List rendered lineage artifacts"),
    SlashCommand(
        "/open",
        "lineage",
        "Open a saved lineage diagram (/open <name_or_id>)",
        long_desc=(
            "Reads the cached image file from disk and opens it in the "
            "platform default viewer. No DB call. Warns when the edge-set "
            "hash differs from the current catalog state."
        ),
    ),
    SlashCommand(
        "/refresh",
        "lineage",
        "Re-render an existing lineage artifact (/refresh <name_or_id>)",
        long_desc=(
            "Re-runs the extractors with the artifact's stored scope and "
            "writes a new image at the same path. Cache-first by default; "
            "--no-cache invalidates the view-cache for the anchor's schema "
            "and forces a fresh DB fetch."
        ),
    ),
    SlashCommand(
        "/delete",
        "lineage",
        "Delete a lineage artifact row and its file (/delete <name_or_id>)",
    ),
    SlashCommand(
        "/show",
        "lineage",
        "Text-mode upstream/downstream tree for a table or column",
    ),
    SlashCommand(
        "/suggest",
        "lineage",
        "Ask the active LLM to propose lineage edges (opt-in, spends tokens)",
        long_desc=(
            "Sends one chat call to your active LLM profile with the anchor's "
            "columns + 30 candidate tables from the same database. Edges are "
            "persisted into catalog_relationships with source='llm' so future "
            "reads pick them up without another round-trip. Use /lineage show "
            "to inspect the reasoning afterwards."
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
    *_LLM_COMMANDS,
    *_DOCS_COMMANDS,
    *_CODE_COMMANDS,
    *_METADATA_COMMANDS,
    *_ANALYZE_COMMANDS,
    *_SEARCH_COMMANDS,
    *_HISTORY_COMMANDS,
    *_LINEAGE_COMMANDS,
    *_PAGES_COMMANDS,
    *_ADMIN_COMMANDS,
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
    if ns == "lineage":
        return (*_ROOT_BUILTINS, *_LINEAGE_COMMANDS)
    if ns == "pages":
        return (*_ROOT_BUILTINS, *_PAGES_COMMANDS)
    if ns == "admin":
        return (*_ROOT_BUILTINS, *_ADMIN_COMMANDS)
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
        "lineage": _LINEAGE_COMMANDS,
        "pages": _PAGES_COMMANDS,
        "admin": _ADMIN_COMMANDS,
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
    """Distinct namespaces that have at least one command.

    This is the single source of truth for the tab order. Both the REPL
    tab bar (:mod:`amx.cli_support._session_ui`) and the Left/Right
    arrow navigation in :mod:`amx.cli_support._session_keybindings`
    derive from this tuple, so adding a new tab here automatically
    surfaces it everywhere.
    """
    return (
        "db",
        "metadata",
        "docs",
        "llm",
        "code",
        "analyze",
        "search",
        "history",
        "lineage",
        "pages",
        "admin",
    )


__all__ = [
    "ALL_COMMANDS",
    "SlashCommand",
    "all_namespaces",
    "cmd_heads_for_namespace",
    "commands_for_namespace",
    "find_command",
]
