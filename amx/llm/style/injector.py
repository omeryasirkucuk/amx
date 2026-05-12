"""Renders the `## Writing style` system-prompt fragment from a
StyleProfile. The guard sentence is non-negotiable and ships every
time a profile is injected.
"""

from __future__ import annotations

from amx.llm.style.profile import PLACEHOLDERS, StyleProfile

_GUARD = (
    "Match this style profile for tone, language, length, and sentence "
    "structure. The placeholders {placeholders} mark domain-specific "
    "terms from another schema. Never copy these placeholders into "
    "your output, and never invent entity names from them. Always "
    "derive domain terms from the target column you are describing."
).format(placeholders=", ".join(sorted(PLACEHOLDERS)))


def render_style_section(profile: StyleProfile | None) -> str:
    if profile is None:
        return ""
    lo, hi = profile.length_range
    patterns = "\n".join(f"- {p}" for p in profile.structural_patterns)
    examples = "\n".join(f"- {ex}" for ex in profile.redacted_examples)
    return (
        "\n\n## Writing style\n"
        f"- Language: {profile.language}\n"
        f"- Tone: {profile.tone}\n"
        f"- Person: {profile.person}\n"
        f"- Capitalization: {profile.capitalization}\n"
        f"- Ends with period: {'yes' if profile.ends_with_period else 'no'}\n"
        f"- Length (words): {lo}-{hi}, target ~{profile.avg_length_words}\n"
        f"- Vocabulary register: {profile.vocabulary_register}\n"
        f"### Structural patterns\n{patterns}\n"
        f"### Examples (placeholders, do NOT copy)\n{examples}\n\n"
        f"{_GUARD}\n"
    )
