"""Preset intent templates for the ``/pages new`` wizard.

The pages composer (`amx/pages/composer.py`) takes a free-form INTENT
string. The template registry exposes a small set of common shapes
("single table summary", "project overview", ...) so a wizard user can
pick from a list instead of inventing the phrasing each time, while
power users can still pass arbitrary text.

Each template owns three pieces of data:

* ``slug`` — stable identifier used by ``--intent-template``
* ``label`` — human-readable picker line
* ``required_assets`` — asset kinds the page MUST collect; consumed by
  the wizard to skip unrelated asset prompts and validate the result
* ``prompt_skeleton`` — fmt-string with optional ``{table}``,
  ``{column}``, ``{db_profile}`` etc. placeholders; rendered into the
  final intent string after the wizard collects the parameters

The registry is intentionally small and explicit. Adding a new
template is a 6-line tuple plus, if needed, a wizard branch that
collects its placeholders.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

AssetRequirement = Literal[
    "one_db_table",
    "one_db_column",
    "one_db_profile",
    "many_db_profiles",
    "one_lineage",
    "any",
]


@dataclass(frozen=True)
class IntentTemplate:
    slug: str
    label: str
    required_assets: AssetRequirement
    prompt_skeleton: str


INTENT_TEMPLATES: tuple[IntentTemplate, ...] = (
    IntentTemplate(
        slug="single-table",
        label="Single table — detailed summary",
        required_assets="one_db_table",
        prompt_skeleton=(
            "Produce a focused documentation page for table `{table}` in "
            "DB profile `{db_profile}`. Cover its business purpose, the "
            "meaning of each column, value distributions where available, "
            "and any upstream/downstream lineage present in the context."
        ),
    ),
    IntentTemplate(
        slug="single-column",
        label="Single column — origin, transforms, downstream usage",
        required_assets="one_db_column",
        prompt_skeleton=(
            "Document column `{column}` of table `{table}` in DB profile "
            "`{db_profile}`. Explain its meaning, its origin (source "
            "tables/columns from lineage), any transformations along the "
            "way, and which downstream tables or reports consume it."
        ),
    ),
    IntentTemplate(
        slug="db-profile-overview",
        label="DB profile overview — schema, table groups, key relationships",
        required_assets="one_db_profile",
        prompt_skeleton=(
            "Produce a documentation page for DB profile `{db_profile}`. "
            "Describe the overall purpose of the database, group tables "
            "by domain, summarise the most important relationships, and "
            "highlight central entities."
        ),
    ),
    IntentTemplate(
        slug="cross-db",
        label="Cross-DB connections — joins, shared keys across profiles",
        required_assets="many_db_profiles",
        prompt_skeleton=(
            "Produce a documentation page that explains how DB profiles "
            "{db_profiles} interact. Focus on shared keys, replicated "
            "entities, and any documented data flow between them."
        ),
    ),
    IntentTemplate(
        slug="lineage-narrative",
        label="Pipeline / lineage narrative",
        required_assets="one_lineage",
        prompt_skeleton=(
            "Produce a narrative documentation page for the data pipeline "
            "captured by lineage artifact `{lineage}`. Describe the "
            "source systems, the transformations along the way, and the "
            "target tables / reports."
        ),
    ),
    IntentTemplate(
        slug="project-overview",
        label="Project overview — multi-asset documentation",
        required_assets="any",
        prompt_skeleton=(
            "Produce a project-level documentation page that synthesises "
            "the provided assets into a coherent overview. Cover the "
            "systems in scope, the main domains, the key entities, and "
            "the data flows between them."
        ),
    ),
)


def template_by_slug(slug: str) -> IntentTemplate | None:
    for t in INTENT_TEMPLATES:
        if t.slug == slug:
            return t
    return None


def render(template: IntentTemplate, **params: str) -> str:
    """Render the prompt skeleton with the collected placeholder values.

    Missing placeholders are tolerated (left as ``{name}`` in the output)
    so a wizard that skips an optional field still produces a valid
    intent string.
    """
    text = template.prompt_skeleton
    for key, value in params.items():
        text = text.replace("{" + key + "}", value)
    return text


__all__ = [
    "AssetRequirement",
    "INTENT_TEMPLATES",
    "IntentTemplate",
    "render",
    "template_by_slug",
]
