"""Unit tests for the pages intent template registry."""

from __future__ import annotations

from amx.pages.intent_templates import (
    INTENT_TEMPLATES,
    render,
    template_by_slug,
)


def test_registry_has_expected_slugs() -> None:
    slugs = {t.slug for t in INTENT_TEMPLATES}
    expected = {
        "single-table",
        "single-column",
        "db-profile-overview",
        "cross-db",
        "lineage-narrative",
        "project-overview",
    }
    assert expected.issubset(slugs)


def test_template_by_slug_returns_match() -> None:
    t = template_by_slug("single-table")
    assert t is not None
    assert t.required_assets == "one_db_table"


def test_template_by_slug_returns_none_for_unknown() -> None:
    assert template_by_slug("does-not-exist") is None


def test_render_substitutes_placeholders() -> None:
    t = template_by_slug("single-column")
    assert t is not None
    rendered = render(t, column="email", table="users", db_profile="prod")
    assert "`email`" in rendered
    assert "`users`" in rendered
    assert "`prod`" in rendered
    assert "{column}" not in rendered


def test_render_leaves_missing_placeholders_intact() -> None:
    """Missing placeholders should not raise; they stay literal so the
    LLM still receives a coherent (if slightly generic) intent."""
    t = template_by_slug("single-table")
    assert t is not None
    rendered = render(t, db_profile="prod")
    assert "`prod`" in rendered
    # {table} placeholder is left as-is when not provided.
    assert "{table}" in rendered
