"""Backend-conditional injection in the /ask tool-agent system prompt.

Routing-rule blocks that name a specific backend's tooling (Databricks
Volumes, ``list_catalogs`` on 3-level backends, ``list_server_databases``
on 2-level backends, the ``needs_catalog=true`` recovery flow) now render
only when at least one profile the LLM may retrieve from this turn
actually uses that backend. Users on other backends get a tighter prompt;
none of the load-bearing rules (false-positive filtering, push-back
handling, profile-boundary discipline, STATS-EXAMPLE-DRILL, partial-
catalog signal, fast-path coverage routing, academic metric citations,
lexical-mode distinct-meaning rule, LOAD MECHANISM routing) move.

The tests render the prompt against representative configs and assert
both the conditional removals and the byte-level presence of every rule
that must survive every render.
"""

from __future__ import annotations

import pytest

from amx.config import AMXConfig, DBConfig, LLMConfig
from amx.search._tool_agent_prompts import agent_system_prompt


def _render(backend: str, *, schemas: list[str] | None = None) -> str:
    cfg = AMXConfig(
        db=DBConfig(backend=backend, database="testdb"),
        llm=LLMConfig(),
    )
    return agent_system_prompt(cfg, schema_hint=schemas or ["public", "sales"])


def _render_multi_profile(profiles: dict[str, str]) -> str:
    """Render with several profiles in scope; ``profiles`` is name → backend."""
    db_profiles = {name: DBConfig(backend=backend) for name, backend in profiles.items()}
    first_name = next(iter(profiles))
    cfg = AMXConfig(
        db=DBConfig(backend=profiles[first_name], database="testdb"),
        llm=LLMConfig(),
        db_profiles=db_profiles,
        active_db_profile=first_name,
    )
    return agent_system_prompt(
        cfg,
        schema_hint=["public"],
        scope_profiles=list(profiles.keys()),
    )


# ── Backend-keyed blocks render only for the matching backend family ────────


def test_volumes_block_only_for_databricks() -> None:
    assert "Databricks Volumes" in _render("databricks")
    assert "Databricks Volumes" not in _render("postgresql")
    assert "Databricks Volumes" not in _render("bigquery")
    assert "Databricks Volumes" not in _render("snowflake")


def test_list_catalogs_block_only_for_three_level() -> None:
    assert "list_catalogs" in _render("databricks")
    assert "list_catalogs" in _render("bigquery")
    assert "list_catalogs" not in _render("postgresql")
    assert "list_catalogs" not in _render("snowflake")
    assert "list_catalogs" not in _render("mysql")


def test_list_server_databases_block_only_for_two_level() -> None:
    assert "list_server_databases" in _render("postgresql")
    assert "list_server_databases" in _render("snowflake")
    assert "list_server_databases" in _render("mysql")
    assert "list_server_databases" not in _render("databricks")
    assert "list_server_databases" not in _render("bigquery")


def test_needs_catalog_recovery_only_for_three_level() -> None:
    assert "needs_catalog=true" in _render("databricks")
    assert "needs_catalog=true" in _render("bigquery")
    assert "needs_catalog=true" not in _render("postgresql")
    assert "needs_catalog=true" not in _render("mysql")


# ── Multi-profile mode: blocks render when ANY in-scope profile matches ─────


def test_multi_profile_includes_blocks_for_each_backend() -> None:
    prompt = _render_multi_profile({"pg": "postgresql", "dbr": "databricks"})
    # Databricks profile in scope → Volumes block present
    assert "Databricks Volumes" in prompt
    # 3-level profile in scope → list_catalogs + needs_catalog present
    assert "list_catalogs" in prompt
    assert "needs_catalog=true" in prompt
    # 2-level profile in scope → list_server_databases present
    assert "list_server_databases" in prompt


def test_multi_profile_only_two_level_omits_three_level_blocks() -> None:
    prompt = _render_multi_profile({"pg": "postgresql", "snow": "snowflake"})
    assert "Databricks Volumes" not in prompt
    assert "list_catalogs" not in prompt
    assert "needs_catalog=true" not in prompt
    assert "list_server_databases" in prompt


# ── Unknown / empty backend falls back to full prompt (safe default) ────────


def test_empty_backend_includes_every_block() -> None:
    """Misconfigured profiles must never lose routing hints."""
    prompt = _render("")
    assert "Databricks Volumes" in prompt
    assert "list_catalogs" in prompt
    assert "list_server_databases" in prompt
    assert "needs_catalog=true" in prompt


# ── Load-bearing rules must appear in every render, byte-identical ──────────

LOAD_BEARING_SNIPPETS = [
    # find_table_by_name #1 regression rule — must run before concept search
    "ALWAYS call find_table_by_name",
    "#1 cause of",
    # Result-validation phone-number canonical example
    "tel_number",
    "fax_number",
    "FALSE POSITIVES",
    # Push-back handling
    "Thank you for your patience",
    "previous tool result was",
    # Profile-boundary discipline (multi-profile anti-hallucination)
    "Profile-boundary discipline",
    "NEVER copy one profile's pinned name onto",
    # Academic citations for compare_runs quality tiers
    "chrF",
    "Popović 2015",
    "ROUGE-L Lin 2004",
    "G-Eval Liu",
    # Lexical-mode distinct meaning rule
    "DISTINCT CANDIDATE MEANING",
    "distinct candidate meaning",
    # STATS-EXAMPLE-DRILL pattern + UX rules
    "STATS-EXAMPLE-DRILL",
    "Stats line",
    "Drill-in invitation",
    # Partial-catalog signal
    "partial_reason",
    # Fast-path cache routing (avoid live-only tool refusal)
    "catalog_coverage_summary",
    "needs_live_refresh",
    # Cache vs live provenance attribution
    "source=catalog",
    "source=live",
    # LOAD MECHANISM block + v0.11 disclaimer
    "LOAD MECHANISM",
    "v0.11",
    # Interpretive answering — never reply flat "no"
    "Interpretive answering",
    "fuzzy_matches",
    # Anti-onboarding rule for vague input
    "fabricate a welcome",
]


@pytest.mark.parametrize(
    "backend",
    ["postgresql", "databricks", "bigquery", "snowflake", "mysql", "mssql", ""],
)
def test_load_bearing_rules_survive_every_render(backend: str) -> None:
    prompt = _render(backend)
    missing = [s for s in LOAD_BEARING_SNIPPETS if s not in prompt]
    assert not missing, f"Backend {backend!r} dropped load-bearing snippets: {missing}"


# ── Token-budget contract — conditional injection actually saves tokens ─────


def test_single_backend_prompt_is_shorter_than_safe_default() -> None:
    pg = len(_render("postgresql"))
    unknown = len(_render(""))
    # PG omits Databricks Volumes + list_catalogs + needs_catalog handling.
    # Expect a meaningful win even after the compression in Change 4.
    assert pg < unknown, "PG prompt should be shorter than safe-default fallback"
    assert (unknown - pg) > 1000, "Conditional injection saved < 1000 chars; regression?"


def test_schema_hint_capped_at_twenty() -> None:
    many = [f"schema_{i}" for i in range(200)]
    prompt = _render("postgresql", schemas=many)
    # Only the first 20 names render verbatim; the rest collapse into a hint.
    assert "schema_19" in prompt
    assert "schema_20" not in prompt
    assert "+180; call list_schemas" in prompt
