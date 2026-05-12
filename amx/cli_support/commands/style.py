"""Slash-command handlers for /style.

Entry point: cmd_style(cfg, rest) — dispatched from session.py.

Subcommands
-----------
set <db>.<schema>.<table>
    Extract a StyleProfile from the given table's column comments and
    persist it to the local history DB, keyed by the active LLM profile.
show
    Print the stored StyleProfile for the active LLM profile.
clear
    Delete the stored StyleProfile for the active LLM profile.
on / off
    Enable or disable style-reference injection for the active LLM profile.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from amx.config import AMXConfig

_USAGE = (
    "Usage:\n"
    "  /style set                        -- pick reference DB profile / catalog / schema / table interactively\n"
    "  /style set <db>.<schema>.<table>  -- extract style from a reference table\n"
    "  /style show                       -- show the current style reference\n"
    "  /style clear                      -- remove the current style reference\n"
    "  /style on                         -- enable style injection\n"
    "  /style off                        -- disable style injection\n"
)


# ---------------------------------------------------------------------------
# Private seams — patched in tests to avoid real DB / LLM I/O
# ---------------------------------------------------------------------------


def _open_connector(cfg: AMXConfig, profile_name: str):
    """Return a DatabaseConnector for the given profile."""
    from amx.db.connector import DatabaseConnector

    db_cfg = cfg.db_profiles[profile_name]
    return DatabaseConnector(db_cfg, profile_name=profile_name)


def _make_llm_caller(cfg: AMXConfig, profile_name: str) -> Callable[[str, str], str]:
    """Return a callable that sends a system+user message to the active LLM."""
    from amx.llm.provider import LLMProvider

    llm_cfg = cfg.llm_profiles[profile_name]

    def _call(system: str, user: str) -> str:
        provider = LLMProvider(llm_cfg)
        result = provider.chat(
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            temperature=0.0,
        )
        return result.content

    return _call


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _db_path(cfg: AMXConfig) -> Path:
    config_dir = Path(getattr(cfg, "CONFIG_DIR", str(Path.home() / ".amx")))
    return config_dir / "history.db"


def _resolve_active_db(cfg: AMXConfig) -> str | None:
    """Return the active DB profile name, or None if not configured."""
    from amx.utils.console import error

    active = cfg.active_db_profile or ""
    if not active or active not in cfg.db_profiles:
        error(
            f"No active DB profile configured (active_db_profile={active!r}). "
            "Set one with /use-db <profile>."
        )
        return None
    return active


def _resolve_active_llm(cfg: AMXConfig) -> str | None:
    """Return the active LLM profile name, or None if not configured."""
    from amx.utils.console import error

    active = cfg.active_llm_profile or ""
    if not active or active not in cfg.llm_profiles:
        error(
            f"No active LLM profile configured (active_llm_profile={active!r}). "
            "Set one with /use-llm <profile>."
        )
        return None
    return active


# ---------------------------------------------------------------------------
# Subcommand implementations
# ---------------------------------------------------------------------------


def _pick_one(label: str, options: list[str]) -> str | None:
    """Single-pick prompt shared by every /style set picker step.

    ``review_picker.pick_rows`` already handles fzf + numbered fallback
    + ESC/Ctrl-C, so reuse it and just collapse the selection back to
    one entry. Returns ``None`` when the user cancels (empty pick) so
    callers can bail out cleanly instead of carrying a placeholder.
    """
    from amx.cli_support.review_picker import pick_rows
    from amx.utils.console import info

    if not options:
        return None
    info(f"Pick a {label}:")
    picked = pick_rows(options)
    if not picked:
        return None
    return options[picked[0]]


def _interactive_pick_reference(cfg: AMXConfig) -> tuple[str, str] | None:
    """Walk DB profile → catalog/database → schema → table.

    Returns ``(db_profile, "<catalog_or_db>.<schema>.<table>")`` on a
    full selection, or ``None`` on cancel. The DB profile may differ
    from ``cfg.active_db_profile`` — Studio surfaces the same picker
    and lets the user pick any defined profile, the CLI mirrors that
    so the two flows stay in lockstep.
    """
    from amx.utils.console import error, info

    profiles = list(cfg.db_profiles.keys())
    if not profiles:
        error(
            "No DB profiles defined. Add one with /add-db-profile before "
            "attaching a style reference."
        )
        return None
    db_profile = _pick_one("DB profile", profiles)
    if db_profile is None:
        info("Cancelled.")
        return None

    try:
        conn = _open_connector(cfg, db_profile)
    except Exception as exc:
        error(f"Failed to open connector for {db_profile!r}: {exc}")
        return None

    try:
        if conn.supports_catalogs():
            level_label = "catalog"
            options = conn.list_catalogs()
        else:
            level_label = "database"
            options = conn.list_databases()
    except Exception as exc:
        error(f"Failed to list {level_label}s on {db_profile!r}: {exc}")
        return None

    if not options:
        error(
            f"No {level_label}s visible to profile {db_profile!r}. "
            "Check the profile's credentials and try again."
        )
        return None
    cat_or_db = _pick_one(level_label, options)
    if cat_or_db is None:
        info("Cancelled.")
        return None

    try:
        conn.use(cat_or_db)
        schemas = conn.list_schemas()
    except Exception as exc:
        error(f"Failed to list schemas under {cat_or_db!r}: {exc}")
        return None
    if not schemas:
        error(f"No schemas visible under {cat_or_db!r}.")
        return None
    schema = _pick_one("schema", schemas)
    if schema is None:
        info("Cancelled.")
        return None

    try:
        tables = conn.list_tables(schema)
    except Exception as exc:
        error(f"Failed to list tables in {schema!r}: {exc}")
        return None
    if not tables:
        error(f"No tables visible in {schema!r}.")
        return None
    table = _pick_one("table", tables)
    if table is None:
        info("Cancelled.")
        return None

    return db_profile, f"{cat_or_db}.{schema}.{table}"


def _cmd_set(cfg: AMXConfig, args: list[str]) -> None:
    from amx.llm.style.extractor import NoSamplesError, extract_style
    from amx.storage.style_store import StyleStore
    from amx.utils.console import error, info

    active_llm = _resolve_active_llm(cfg)
    if active_llm is None:
        return

    if not args:
        # Interactive picker mode: DB profile → catalog/database →
        # schema → table. Mirrors the Studio Settings → LLM → Writing
        # style reference picker so the two surfaces stay in lockstep.
        picked = _interactive_pick_reference(cfg)
        if picked is None:
            return
        active_db, ref = picked
        parts = ref.split(".")
        db_name, schema, table = parts
    else:
        ref = args[0].strip()
        parts = ref.split(".")
        if len(parts) != 3:
            error(
                f"Expected <db>.<schema>.<table>, got {ref!r}. "
                "Example: /style set my_warehouse.sales.orders. "
                "Or run `/style set` with no args to pick interactively."
            )
            return
        db_name, schema, table = parts

        active_db = _resolve_active_db(cfg)
        if active_db is None:
            return

    # Step 1 — fetch column comments
    try:
        conn = _open_connector(cfg, active_db)
        conn.use(db_name)
        comments = conn.get_column_comments(schema, table)
    except Exception as exc:
        error(f"Failed to read column comments from {ref}: {exc}")
        return

    # Step 2 — build LLM caller
    llm_call = _make_llm_caller(cfg, active_llm)

    # Step 3 — extract style
    try:
        profile, n = extract_style(comments, llm_call=llm_call)
    except NoSamplesError as exc:
        error(f"Not enough samples to extract style: {exc}")
        return
    except ValueError as exc:
        error(f"Style extraction failed: {exc}")
        return

    # Step 4 — determine backend kind
    kind = conn.backend

    # Step 5 — persist
    StyleStore(_db_path(cfg)).upsert(
        llm_profile=active_llm,
        source_ref=ref,
        source_db_kind=kind,
        profile=profile,
        sample_count=n,
    )

    info(
        f"Saved style profile for LLM '{active_llm}' "
        f"(language={profile.language}, samples={n}, "
        f"examples={len(profile.redacted_examples)})."
    )


def _cmd_show(cfg: AMXConfig) -> None:
    from amx.storage.style_store import StyleStore
    from amx.utils.console import info

    active_llm = _resolve_active_llm(cfg)
    if active_llm is None:
        return

    row = StyleStore(_db_path(cfg)).get(active_llm)
    if row is None:
        info(
            f"No style reference attached to LLM profile '{active_llm}'. "
            "Use /style set <db>.<schema>.<table>."
        )
        return

    p = row.profile
    info(f"LLM profile   : {row.llm_profile}")
    info(f"Source        : {row.source_ref}  ({row.source_db_kind})")
    info(f"Enabled       : {row.enabled}")
    info(f"Samples used  : {row.sample_count}")
    info(f"Language      : {p.language}")
    info(f"Tone          : {p.tone}")
    info(f"Avg length    : {p.avg_length_words} words")
    info(f"Length range  : {p.length_range[0]}–{p.length_range[1]} words")
    info(f"Person        : {p.person}")
    info(f"Capitalization: {p.capitalization}")
    info(f"Ends w/ period: {p.ends_with_period}")
    info(f"Vocab register: {p.vocabulary_register}")
    info(f"Patterns      : {', '.join(p.structural_patterns) or '(none)'}")
    info(f"Examples      : {', '.join(repr(e) for e in p.redacted_examples)}")
    info("")
    info("Raw JSON:")
    info(json.dumps(json.loads(p.to_json()), indent=2))


def _cmd_clear(cfg: AMXConfig) -> None:
    from amx.storage.style_store import StyleStore
    from amx.utils.console import info

    active_llm = _resolve_active_llm(cfg)
    if active_llm is None:
        return

    StyleStore(_db_path(cfg)).clear(active_llm)
    info(f"Cleared style reference for LLM profile '{active_llm}'.")


def _cmd_toggle(cfg: AMXConfig, enabled: bool) -> None:
    from amx.storage.style_store import StyleStore
    from amx.utils.console import info

    active_llm = _resolve_active_llm(cfg)
    if active_llm is None:
        return

    StyleStore(_db_path(cfg)).set_enabled(active_llm, enabled)
    state = "enabled" if enabled else "disabled"
    info(f"Style injection {state} for LLM profile '{active_llm}'.")


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def cmd_style(cfg: AMXConfig, rest: list[str]) -> None:
    """Dispatch /style subcommands."""
    from amx.utils.console import info

    if not rest:
        info(_USAGE)
        return

    sub = rest[0].lower()

    if sub == "set":
        _cmd_set(cfg, rest[1:])
    elif sub == "show":
        _cmd_show(cfg)
    elif sub == "clear":
        _cmd_clear(cfg)
    elif sub == "on":
        _cmd_toggle(cfg, True)
    elif sub == "off":
        _cmd_toggle(cfg, False)
    else:
        info(f"Unknown /style subcommand: {sub!r}\n\n{_USAGE}")
