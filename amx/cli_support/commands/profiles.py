"""Profile and prompt-setting helpers for the AMX interactive CLI."""

from __future__ import annotations

from dataclasses import replace

from amx.config import (
    DISABLED_PROFILE,
    AMXConfig,
    LLMConfig,
    _normalize_db_host,
    normalize_llm_model,
)
from amx.utils.console import (
    ask,
    ask_choice,
    ask_password,
    confirm,
    error,
    heading,
    info,
    render_table,
    success,
    warn,
)


def default_model(provider: str) -> str:
    return {
        "openai": "gpt-4o",
        "openrouter": "openai/gpt-4o-mini",
        "anthropic": "claude-sonnet-4-20250514",
        "gemini": "gemini-2.0-flash",
        "deepseek": "deepseek-chat",
        "local": "llama3",
        "kimi": "kimi",
        "ollama": "llama3",
        # Databricks Foundation Model Serving — common managed endpoints. The
        # actual catalog is workspace-specific; users override at the prompt.
        "databricks_serving": "databricks-meta-llama-3-1-70b-instruct",
    }.get(provider, "gpt-4o")


def interactive_llm_block(defaults: LLMConfig | None = None) -> LLMConfig:
    """Interactive prompts to build an LLMConfig.

    When ``defaults`` is ``None``, every prompt starts blank and the
    user explicitly types every value — this is the path used by
    ``/add-llm-profile`` for a new profile name, so the active
    profile's API key, model, etc. never silently fill into the new
    profile.

    When editing an existing profile, the caller passes that profile
    so the user can press Enter to keep current values.
    """
    new_profile = defaults is None
    if defaults is None:
        defaults = LLMConfig()
    provider = ask_choice(
        "Select AI provider",
        [
            "openai",
            "openrouter",
            "anthropic",
            "gemini",
            "deepseek",
            "local",
            "kimi",
            "ollama",
            "databricks_serving",
        ],
        default=defaults.provider or "openai",
    )

    # If the user picked a different provider than the existing
    # profile's, drop those defaults — model name + api_base + key
    # belong to the previous provider and would otherwise leak.
    if defaults.provider and provider != defaults.provider:
        defaults = LLMConfig(provider=provider)

    # Brand-new profile? Start every prompt empty so values cannot
    # silently leak from the active profile via Enter-to-keep.
    if new_profile:
        defaults = LLMConfig(provider=provider)
    info(
        "Model: use the provider's natural model id. AMX will add any required provider prefix internally."
    )
    if provider == "openrouter":
        info(
            "OpenRouter model examples: openai/gpt-4o-mini, anthropic/claude-3.5-sonnet, qwen/qwen3.6-plus"
        )
    elif provider == "openai":
        info("OpenAI model example: gpt-4o")
    elif provider == "anthropic":
        info("Anthropic model example: claude-sonnet-4-20250514")
    elif provider == "gemini":
        info("Gemini model example: gemini-2.0-flash")
    elif provider == "deepseek":
        info("DeepSeek model example: deepseek-chat")
    elif provider == "ollama":
        info("Ollama model example: llama3")
    elif provider == "databricks_serving":
        info(
            "Databricks Serving model = the SERVING ENDPOINT NAME from your "
            "workspace's 'Serving' tab (Foundation Models or your custom endpoint)."
        )
        info(
            "  Examples: databricks-meta-llama-3-1-70b-instruct, "
            "databricks-dbrx-instruct, my-custom-mistral-endpoint"
        )
    model = ask(
        "Model name" if provider != "databricks_serving" else "Serving endpoint name",
        normalize_llm_model(provider, defaults.model) or default_model(provider),
    )

    api_base = defaults.api_base
    if provider == "databricks_serving":
        # Ask for the workspace host (the same hostname the DB profile uses for
        # SQL warehouse connections). The api_base is constructed from it so
        # users don't have to remember the ``/serving-endpoints`` suffix.
        existing_host = ""
        if api_base:
            stripped = api_base.strip().rstrip("/")
            for scheme in ("https://", "http://"):
                if stripped.lower().startswith(scheme):
                    stripped = stripped[len(scheme) :]
                    break
            existing_host = stripped.split("/", 1)[0]
        host = ask(
            "Databricks workspace host (e.g. adb-xxxxxxxxxxxxxxxx.0.azuredatabricks.net)",
            existing_host,
        )
        host = _normalize_db_host(host)
        api_base = f"https://{host}/serving-endpoints" if host else api_base
    elif provider in ("local", "ollama", "kimi", "openrouter"):
        default_api_base = (
            "http://localhost:11434" if provider == "ollama" else "http://localhost:11434/v1"
        )
        if provider == "openrouter":
            default_api_base = "https://openrouter.ai/api/v1"
        api_base = ask("API base URL", api_base or default_api_base)

    if provider in ("local", "ollama"):
        api_key = ask("API key (optional)", defaults.api_key or "")
    elif provider == "databricks_serving":
        api_key = ask_password("Databricks personal access token (PAT)") or defaults.api_key
    else:
        api_key = ask_password("API key") or defaults.api_key

    info("Generation settings:")
    n_alt = ask("  Alternatives (1-5)", default=str(getattr(defaults, "n_alternatives", 3)))
    batch = ask("  Column batch size", default=str(getattr(defaults, "column_batch_size", 10)))
    temp_raw = ask("  Temperature (0.0-2.0)", default=str(getattr(defaults, "temperature", 0.2)))
    try:
        temperature = float(temp_raw)
    except ValueError:
        temperature = defaults.temperature
    temperature = max(0.0, min(2.0, temperature))

    info("Confidence thresholds (token probability 0.0-1.0):")
    high = ask("  High threshold", default=str(getattr(defaults, "logprob_high", 0.85)))
    med = ask("  Medium threshold", default=str(getattr(defaults, "logprob_medium", 0.50)))

    return LLMConfig(
        provider=provider,
        model=normalize_llm_model(provider, model),
        api_key=api_key,
        api_base=api_base,
        temperature=temperature,
        max_tokens=defaults.max_tokens,
        n_alternatives=int(n_alt),
        column_batch_size=int(batch),
        batch_context_column_names=int(getattr(defaults, "batch_context_column_names", 0)),
        logprob_high=float(high),
        logprob_medium=float(med),
    )


def cmd_logprob_thresholds(cfg: AMXConfig, rest: list[str]) -> None:
    """Show or set logprob confidence thresholds for the active LLM profile."""
    if not rest:
        high = getattr(cfg.llm, "logprob_high", 0.85)
        med = getattr(cfg.llm, "logprob_medium", 0.50)
        info(
            f"Current logprob thresholds: [bold]HIGH[/] >= {high:.2f} | [bold]MEDIUM[/] >= {med:.2f}"
        )
        info("Run [cyan]/logprob-thresholds <high> <med>[/cyan] to change (e.g. 0.9 0.6).")
        return

    try:
        if len(rest) < 2:
            error("Usage: /logprob-thresholds <high> <medium>")
            return
        high = float(rest[0])
        medium = float(rest[1])
    except ValueError:
        error(f"Expected numeric values, got: {rest}")
        return

    if not (0.0 <= medium < high <= 1.0):
        error("Thresholds must be 0.0-1.0, and high must be greater than medium.")
        return

    cfg.llm.logprob_high = high
    cfg.llm.logprob_medium = medium
    if cfg.active_llm_profile and cfg.active_llm_profile in cfg.llm_profiles:
        profile = cfg.llm_profiles[cfg.active_llm_profile]
        profile.logprob_high = high
        profile.logprob_medium = medium
    cfg.save()
    success(
        f"Logprob thresholds saved for LLM profile '{cfg.active_llm_profile}': "
        f"HIGH >= {high:.2f}, MEDIUM >= {medium:.2f}"
    )


def cmd_temperature(cfg: AMXConfig, rest: list[str]) -> None:
    """Show or set the LLM sampling temperature for the active profile."""
    if not rest:
        current = getattr(cfg.llm, "temperature", 0.2)
        info(f"Current LLM temperature: [bold]{current:.2f}[/]")
        info("Run [cyan]/temperature <value>[/cyan] to change (e.g. 0.7). Range: 0.0-2.0.")
        return

    try:
        value = float(rest[0])
    except ValueError:
        error(f"Expected a numeric value, got: {rest[0]}")
        return

    clamped = max(0.0, min(2.0, value))
    if clamped != value:
        warn(f"Temperature {value} clamped to {clamped:.2f} (allowed range 0.0-2.0).")

    cfg.llm.temperature = clamped
    if cfg.active_llm_profile and cfg.active_llm_profile in cfg.llm_profiles:
        cfg.llm_profiles[cfg.active_llm_profile].temperature = clamped
    cfg.save()
    success(f"Temperature saved for LLM profile '{cfg.active_llm_profile}': {clamped:.2f}")


def cmd_llm_profiles(cfg: AMXConfig) -> None:
    rag_override = (cfg.rag_llm_profile or "").strip()
    rows = []
    for name, llm in sorted(cfg.llm_profiles.items(), key=lambda x: x[0]):
        marks = ""
        marks += "*" if name == cfg.active_llm_profile else " "
        marks += "R" if rag_override and name == rag_override else " "
        rows.append([f"{marks} {name}", llm.provider, llm.model])
    render_table(
        "LLM profiles (* = active, R = RAG override)",
        ["Profile", "Provider", "Model"],
        rows,
    )
    if rag_override and rag_override in cfg.llm_profiles:
        if rag_override == cfg.active_llm_profile:
            info(
                f"RAG override set to {rag_override!r} — same as the active profile, "
                "so it has no effect. Run /use-rag-llm to pick a different profile."
            )
        else:
            info(f"RAG agent uses {rag_override!r} (override). Run `/use-rag-llm none` to clear.")
    else:
        info("RAG agent uses the active profile. Run `/use-rag-llm` to override.")


def cmd_use_llm(cfg: AMXConfig, rest: list[str]) -> None:
    if len(rest) >= 1:
        name = rest[0]
    else:
        names = sorted(cfg.llm_profiles.keys())
        if not names:
            error("No LLM profiles configured.")
            return
        name = ask_choice("Select LLM profile", names, default=cfg.active_llm_profile)
    try:
        cfg.set_active_llm_profile(name)
        cfg.save()
        success(f"Switched active LLM profile to: {name}")
    except Exception as exc:
        error(str(exc))


_RAG_LLM_CLEAR_TOKENS = {"none", "(none)", "off", "clear", "default", "-"}


def cmd_use_rag_llm(cfg: AMXConfig, rest: list[str]) -> None:
    """Pin a different LLM profile to the RAG agent, or clear that pin.

    Without args, opens an interactive picker that lists every LLM
    profile plus a leading ``(none)`` entry meaning "RAG falls back to
    the active profile". With a name, switches directly. Pass any of
    ``none / off / clear / default / -`` to clear the override
    non-interactively.

    The override is stored in ``cfg.rag_llm_profile``; it has no effect
    when empty.
    """
    names = sorted(cfg.llm_profiles.keys())
    if not names:
        error("No LLM profiles configured. Use /add-llm-profile first.")
        return

    if len(rest) >= 1:
        raw = rest[0].strip()
        if raw.lower() in _RAG_LLM_CLEAR_TOKENS:
            cfg.rag_llm_profile = ""
            cfg.save()
            success(
                f"RAG override cleared. RAG agent now uses the active LLM "
                f"profile {cfg.active_llm_profile!r}."
            )
            return
        if raw not in cfg.llm_profiles:
            error(f"Unknown LLM profile: {raw}. Available: {', '.join(names)}")
            return
        name = raw
    else:
        choices = ["(none)"] + names
        default_choice = (
            cfg.rag_llm_profile if cfg.rag_llm_profile in cfg.llm_profiles else "(none)"
        )
        picked = ask_choice(
            "Select LLM profile for the RAG agent",
            choices,
            default=default_choice,
        )
        if picked == "(none)" or not picked:
            cfg.rag_llm_profile = ""
            cfg.save()
            success(
                f"RAG override cleared. RAG agent now uses the active LLM "
                f"profile {cfg.active_llm_profile!r}."
            )
            return
        name = picked

    cfg.rag_llm_profile = name
    cfg.save()
    if name == cfg.active_llm_profile:
        info(
            f"RAG override set to {name!r} — same as the active profile, "
            "so behaviour is unchanged. Pick a different profile to actually "
            "split RAG from the rest."
        )
    else:
        success(
            f"RAG agent now uses LLM profile {name!r} "
            f"(active profile for everything else: {cfg.active_llm_profile!r})."
        )


def cmd_add_llm_profile(cfg: AMXConfig, rest: list[str]) -> None:
    name = rest[0] if len(rest) >= 1 else ask("LLM profile name", default="work")
    existing = cfg.llm_profiles.get(name)
    if existing is not None:
        info(f"Editing LLM profile: {name}")
        llm = interactive_llm_block(replace(existing))
    else:
        info(f"Creating new LLM profile: {name}")
        # New profile — every prompt starts blank. We deliberately do
        # NOT use ``cfg.llm`` (the active profile) as defaults: doing
        # so would silently pre-fill /add-llm-profile with the active
        # profile's model name, API key, base URL, and language.
        llm = interactive_llm_block(None)
    with cfg.transaction():
        cfg.upsert_llm_profile(name, llm)
        if confirm(f"Activate profile {name} now?", default=True):
            cfg.set_active_llm_profile(name)
    success(f"LLM profile saved: {name} (active: {cfg.active_llm_profile})")


def cmd_remove_llm_profile(cfg: AMXConfig, rest: list[str]) -> None:
    if len(rest) < 1:
        error("Usage: /remove-llm-profile <name>")
        return
    name = rest[0]
    try:
        cfg.remove_llm_profile(name)
        cfg.save()
        success(f"Removed LLM profile: {name} (active: {cfg.active_llm_profile})")
    except Exception as exc:
        error(str(exc))


def cmd_prompt_detail(cfg: AMXConfig, rest: list[str]) -> None:
    """Show or set the prompt detail level for the active LLM profile."""
    from amx.config import PROMPT_DETAIL_LEVELS, prompt_detail_for

    if not rest:
        current = cfg.llm.prompt_detail or "standard"
        heading(f"Prompt detail level: {current}")
        rows = []
        flags = [
            ("samples", "include_samples"),
            ("null counts", "include_null_counts"),
            ("min / max", "include_min_max"),
            ("cardinality ratio", "include_cardinality"),
            ("col. comment", "include_existing_col_comment"),
            ("PK / FK keys", "include_pk_fk"),
            ("unique+check constraints", "include_unique_check"),
            ("usage stats (pg_stat)", "include_usage_stats"),
            ("schema+db comments", "include_schema_db_comments"),
            ("FK neighbour comments", "include_related_comments"),
            ("query-log analysis", "include_query_log_analysis"),
            ("RAG table hits", "rag_table_hits"),
            ("RAG col hits", "rag_col_hits"),
            ("RAG max chunks", "rag_max_chunks"),
        ]
        for label, attr in flags:
            row = [label]
            for level in PROMPT_DETAIL_LEVELS:
                prompt_detail = prompt_detail_for(level)
                value = getattr(prompt_detail, attr)
                mark = ("✓" if value else "-") if isinstance(value, bool) else str(value)
                row.append(
                    f"[{'success' if value else 'dim'}]{mark}[/]"
                    if isinstance(value, bool)
                    else mark
                )
            rows.append(row)
        render_table("Preset comparison", ["Field", *PROMPT_DETAIL_LEVELS], rows)
        info(
            f"Current level: [cyan]{current}[/cyan]  "
            f"(n_alternatives={cfg.llm.n_alternatives})  "
            "- run [cyan]/prompt-detail <level>[/cyan] to change."
        )
        return

    level = rest[0].lower().strip()
    if level not in PROMPT_DETAIL_LEVELS:
        error(f"Unknown level: {level!r}. Valid levels: {', '.join(PROMPT_DETAIL_LEVELS)}")
        return

    cfg.llm.prompt_detail = level
    if cfg.active_llm_profile and cfg.active_llm_profile in cfg.llm_profiles:
        cfg.llm_profiles[cfg.active_llm_profile].prompt_detail = level
    cfg.save()
    success(
        f"Prompt detail set to [cyan]{level}[/cyan] and saved "
        f"for LLM profile '{cfg.active_llm_profile}'."
    )
    prompt_detail = prompt_detail_for(level)
    info(
        f"  samples={prompt_detail.include_samples}(max={prompt_detail.max_samples})  "
        f"null_counts={prompt_detail.include_null_counts}  "
        f"min_max={prompt_detail.include_min_max}  "
        f"cardinality={prompt_detail.include_cardinality}  "
        f"pk_fk={prompt_detail.include_pk_fk}  "
        f"usage_stats={prompt_detail.include_usage_stats}  "
        f"query_log_analysis={prompt_detail.include_query_log_analysis}  "
        f"rag_chunks={prompt_detail.rag_max_chunks}"
    )


_DESCRIPTION_VERBOSITY_LEVELS = ("brief", "detailed")


def cmd_description_verbosity(cfg: AMXConfig, rest: list[str]) -> None:
    """Show or set the description-verbosity level for the active LLM profile.

    Brief (default) = 1 short sentence per column. Detailed = 2-4 sentences
    covering purpose + typical values + relationships, when supported by
    evidence. Detailed roughly doubles per-column output token cost.
    """
    if not rest:
        current = cfg.llm.description_verbosity or "brief"
        heading(f"Description verbosity: {current}")
        info(
            "  brief    — 1 sentence per column (e.g. 'Sales document number.')\n"
            "  detailed — 2-4 sentences covering purpose, typical values, "
            "and relationships when supported by evidence.\n"
            f"\nCurrent: [cyan]{current}[/cyan] for LLM profile "
            f"'{cfg.active_llm_profile or 'default'}'.\n"
            "Run [cyan]/description-verbosity brief|detailed[/cyan] to change."
        )
        return

    level = rest[0].lower().strip()
    if level not in _DESCRIPTION_VERBOSITY_LEVELS:
        error(f"Unknown verbosity: {level!r}. Valid: {', '.join(_DESCRIPTION_VERBOSITY_LEVELS)}")
        return

    cfg.llm.description_verbosity = level
    if cfg.active_llm_profile and cfg.active_llm_profile in cfg.llm_profiles:
        cfg.llm_profiles[cfg.active_llm_profile].description_verbosity = level
    cfg.save()
    success(
        f"Description verbosity set to [cyan]{level}[/cyan] and saved "
        f"for LLM profile '{cfg.active_llm_profile or 'default'}'."
    )
    if level == "detailed":
        info(
            "Detailed mode: per-column output tokens roughly double. "
            "Tune AMX_LLM_TIMEOUT_SEC / column_batch_size if you hit timeouts."
        )


def cmd_n_alternatives(cfg: AMXConfig, rest: list[str]) -> None:
    """Show or set number of description alternatives per column."""
    if not rest:
        current = getattr(cfg.llm, "n_alternatives", 3)
        info(
            f"Current n_alternatives: [cyan]{current}[/cyan]  "
            "(1 = cheapest, 5 = maximum alternatives)  "
            "- run [cyan]/n-alternatives <N>[/cyan] to change."
        )
        return

    try:
        value = int(rest[0])
    except ValueError:
        error(f"Expected an integer 1-5, got: {rest[0]!r}")
        return

    if not 1 <= value <= 5:
        error("n_alternatives must be between 1 and 5.")
        return

    cfg.llm.n_alternatives = value
    if cfg.active_llm_profile and cfg.active_llm_profile in cfg.llm_profiles:
        cfg.llm_profiles[cfg.active_llm_profile].n_alternatives = value
    cfg.save()
    cost_note = {
        1: "cheapest - 1 option shown at review",
        2: "lean",
        3: "balanced (default)",
        4: "rich",
        5: "maximum context, highest cost",
    }.get(value, "")
    success(
        f"n_alternatives set to [cyan]{value}[/cyan] ({cost_note}) and saved "
        f"for LLM profile '{cfg.active_llm_profile}'."
    )


def cmd_llm_batch_size(cfg: AMXConfig, rest: list[str]) -> None:
    """Show or set the number of columns processed in a single LLM call."""
    if not rest:
        current = getattr(cfg.llm, "column_batch_size", 10)
        info(
            f"Current LLM batch size: [cyan]{current}[/cyan] columns  "
            "(Small = safer/more precise, Large = faster/cheaper)  "
            "- run [cyan]/llm-batch-size <N>[/cyan] to change."
        )
        return

    try:
        value = int(rest[0])
    except ValueError:
        error(f"Expected an integer, got: {rest[0]!r}")
        return

    if value < 1:
        error("Batch size must be at least 1.")
        return

    cfg.llm.column_batch_size = value
    if cfg.active_llm_profile and cfg.active_llm_profile in cfg.llm_profiles:
        cfg.llm_profiles[cfg.active_llm_profile].column_batch_size = value
    cfg.save()
    success(
        f"LLM batch size set to {value} columns and saved for LLM profile '{cfg.active_llm_profile}'."
    )


def cmd_batch_context_columns(cfg: AMXConfig, rest: list[str]) -> None:
    """Show or set how many remaining column names are sent with each batch."""
    if not rest:
        current = int(getattr(cfg.llm, "batch_context_column_names", 0))
        if current == -1:
            current_label = "all remaining names"
        elif current == 0:
            current_label = "off"
        else:
            current_label = f"{current} names"
        info(
            f"Current batch context columns: [cyan]{current_label}[/cyan]. "
            "Use [cyan]/batch-context-columns off[/cyan], [cyan]all[/cyan], "
            "or [cyan]/batch-context-columns <N>[/cyan]."
        )
        return

    raw = (rest[0] or "").strip().lower()
    if raw in {"off", "none", "0"}:
        value = 0
    elif raw == "all":
        value = -1
    else:
        try:
            value = int(raw)
        except ValueError:
            error(f"Expected off, all, or an integer; got: {rest[0]!r}")
            return
        if value < 0:
            error("Value must be >= 0, or use 'all'.")
            return

    cfg.llm.batch_context_column_names = value
    if cfg.active_llm_profile and cfg.active_llm_profile in cfg.llm_profiles:
        cfg.llm_profiles[cfg.active_llm_profile].batch_context_column_names = value
    cfg.save()
    if value == -1:
        success(
            f"Batch context columns set to all remaining names and saved for LLM profile '{cfg.active_llm_profile}'."
        )
    elif value == 0:
        success(
            f"Batch context columns disabled and saved for LLM profile '{cfg.active_llm_profile}'."
        )
    else:
        success(
            f"Batch context columns set to {value} and saved for LLM profile '{cfg.active_llm_profile}'."
        )


def cmd_doc_profiles(cfg: AMXConfig) -> None:
    if not cfg.doc_profiles:
        info("No document profiles. Use /add-doc-profile <name>")
        return
    rows = []
    if cfg.active_doc_profile == DISABLED_PROFILE:
        rows.append(["* (none)", "0", "profiles disabled"])
    for name, paths in sorted(cfg.doc_profiles.items(), key=lambda x: x[0]):
        mark = "*" if name == cfg.active_doc_profile else " "
        preview = "; ".join(paths[:2]) + (" ..." if len(paths) > 2 else "")
        rows.append([f"{mark} {name}", str(len(paths)), preview])
    render_table("Document profiles (* = active)", ["Profile", "# paths", "Preview"], rows)


def cmd_use_doc(cfg: AMXConfig, rest: list[str]) -> None:
    if len(rest) >= 1:
        raw = rest[0].strip().lower()
        name = DISABLED_PROFILE if raw in {"none", "(none)", "off", "disable"} else rest[0]
    else:
        names = sorted(cfg.doc_profiles.keys())
        if not names:
            error("No document profiles.")
            return
        choices = ["(none)"] + names
        default_choice = (
            "(none)" if cfg.active_doc_profile == DISABLED_PROFILE else cfg.active_doc_profile
        )
        picked = ask_choice("Select document profile", choices, default=default_choice)
        name = DISABLED_PROFILE if picked == "(none)" else picked
    if name != DISABLED_PROFILE and name not in cfg.doc_profiles:
        error(f"Unknown document profile: {name}")
        return
    cfg.active_doc_profile = name
    cfg.save()
    if name == DISABLED_PROFILE:
        success("Document profiles disabled for this session/config.")
    else:
        success(f"Active document profile: {name}")


def cmd_add_doc_profile(cfg: AMXConfig, rest: list[str]) -> None:
    name = rest[0] if len(rest) >= 1 else ask("Document profile name", default="default")
    from amx.docs.scanner import test_source_reachable

    existing = list(cfg.doc_profiles.get(name, []))
    new_paths: list[str] = []
    info(
        "Enter document roots (local dir, s3://, GitHub URL, Google Drive, SharePoint/OneDrive). "
        "Each path is checked for reachability only (no full scan)."
    )
    while True:
        path = ask("Path (empty to finish)" if new_paths else "Path", default="")
        if not path:
            if new_paths:
                break
            error("No paths added.")
            return
        if path in existing or path in new_paths:
            if not confirm(
                f"This path is already in profile {name!r}: {path}. Add duplicate anyway?",
                default=False,
            ):
                continue
        try:
            test_source_reachable(path)
            success(f"Source reachable: {path}")
            new_paths.append(path)
        except Exception as exc:
            error(f"Source not reachable: {path}")
            warn(str(exc))
        if not confirm("Add another path?", default=False):
            break
    if not new_paths:
        error("No valid document sources to save.")
        return
    merged = existing + new_paths
    cfg.upsert_doc_profile(name, merged)
    if not cfg.active_doc_profile or confirm(
        f"Switch active document profile to {name}?", default=True
    ):
        cfg.active_doc_profile = name
    cfg.save()
    success(f"Document profile saved: {name} ({len(merged)} path(s))")


def warn_no_doc_paths_for_scan_or_ingest(cfg: AMXConfig, *, cmd: str) -> None:
    """User-friendly hint when /scan or /ingest has no paths and no configured profile."""
    error(f"No document paths to {cmd}.")
    if not cfg.doc_profiles and not cfg.doc_paths:
        info("Add a document profile first: /add-doc-profile (or run /setup).")
    elif cfg.doc_profiles and not cfg.effective_doc_paths():
        info("Your document profiles look empty. Run /add-doc-profile to add paths.")
    else:
        info(
            "Pass paths on the command (e.g. /ingest /path/to/docs) or set an active profile with /use-doc."
        )


def cmd_remove_doc_profile(cfg: AMXConfig, rest: list[str]) -> None:
    if len(rest) < 1:
        error("Usage: /remove-doc-profile <name>")
        return
    try:
        cfg.remove_doc_profile(rest[0])
        cfg.save()
        success(f"Removed document profile: {rest[0]}")
    except Exception as exc:
        error(str(exc))


def cmd_code_profiles(cfg: AMXConfig) -> None:
    if not cfg.code_profiles:
        info("No codebase profiles. Use /add-code-profile <name>")
        return
    rows = []
    if cfg.active_code_profile == DISABLED_PROFILE:
        rows.append(["* (none)", "disabled"])
    for name, path in sorted(cfg.code_profiles.items(), key=lambda x: x[0]):
        mark = "*" if name == cfg.active_code_profile else " "
        rows.append([f"{mark} {name}", path])
    render_table("Codebase profiles (* = active)", ["Profile", "Path / URL"], rows)


def cmd_use_code(cfg: AMXConfig, rest: list[str]) -> None:
    if len(rest) >= 1:
        raw = rest[0].strip().lower()
        name = DISABLED_PROFILE if raw in {"none", "(none)", "off", "disable"} else rest[0]
    else:
        names = sorted(cfg.code_profiles.keys())
        if not names:
            error("No codebase profiles.")
            return
        choices = ["(none)"] + names
        default_choice = (
            "(none)" if cfg.active_code_profile == DISABLED_PROFILE else cfg.active_code_profile
        )
        picked = ask_choice("Select codebase profile", choices, default=default_choice)
        name = DISABLED_PROFILE if picked == "(none)" else picked
    if name != DISABLED_PROFILE and name not in cfg.code_profiles:
        error(f"Unknown codebase profile: {name}")
        return
    cfg.active_code_profile = name
    cfg.save()
    if name == DISABLED_PROFILE:
        success("Codebase profiles disabled for this session/config.")
    else:
        success(f"Active codebase profile: {name}")


def cmd_add_code_profile(cfg: AMXConfig, rest: list[str]) -> None:
    if len(rest) >= 1:
        name = rest[0]
        path = " ".join(rest[1:]).strip() if len(rest) > 1 else ""
    else:
        name = ask("Codebase profile name", default="default")
        path = ""
    if not path:
        path = ask("Codebase path (local dir or Git URL)", default="")
    if not path:
        error("Path required.")
        return
    from amx.codebase.analyzer import test_codebase_path_reachable

    previous = cfg.code_profiles.get(name)
    if previous == path:
        success(f"Codebase profile {name!r} already points to this path - nothing to change.")
        return
    others = [
        name_
        for name_, other_path in cfg.code_profiles.items()
        if other_path == path and name_ != name
    ]
    if others:
        other_list = ", ".join(sorted(others))
        if not confirm(
            f"This path is already used by codebase profile(s): {other_list}. Point {name!r} here too?",
            default=True,
        ):
            return
    try:
        test_codebase_path_reachable(path)
        success(f"Codebase reachable: {path}")
    except Exception as exc:
        error(f"Codebase not reachable: {path}")
        warn(str(exc))
        return
    cfg.upsert_code_profile(name, path)
    if not cfg.active_code_profile or confirm(
        f"Switch active codebase profile to {name}?", default=True
    ):
        cfg.active_code_profile = name
    cfg.save()
    success(f"Codebase profile saved: {name}")


def cmd_remove_code_profile(cfg: AMXConfig, rest: list[str]) -> None:
    if len(rest) < 1:
        error("Usage: /remove-code-profile <name>")
        return
    try:
        cfg.remove_code_profile(rest[0])
        cfg.save()
        success(f"Removed codebase profile: {rest[0]}")
    except Exception as exc:
        error(str(exc))
