"""Studio bundle smoke assertions.

The frontend has no JS test runner installed, so pinning a UI
contract that we just removed has to happen at the bundle layer.
This module greps the built Studio bundle for strings we want to
keep gone (or strings we want present), and fails when the
contract drifts.

When adding new bundle assertions: keep the string short, anchor it
on UX-meaningful copy (not a CSS class or a hashed variable), and
add a comment explaining what regression it pins.
"""

from __future__ import annotations

from pathlib import Path

import pytest

_BUNDLE_ROOT = Path(__file__).resolve().parents[1] / "amx" / "web" / "static" / "assets"


def _all_bundle_text() -> str:
    """Return the concatenated text of every .js file in the Studio
    bundle. Cheap enough to run as a fixture — the build is ~500 KB
    total — and a substring search across the whole bundle catches
    code that landed under a different chunk than expected (Vite
    splits chunks by import graph)."""
    if not _BUNDLE_ROOT.exists():
        pytest.skip("Studio bundle not present (run `cd frontend && npm run build`).")
    parts: list[str] = []
    for path in sorted(_BUNDLE_ROOT.glob("*.js")):
        parts.append(path.read_text(encoding="utf-8", errors="ignore"))
    text = "\n".join(parts)
    if not text.strip():
        pytest.skip("Studio bundle is empty.")
    return text


class TestAdvancedLLMOverridesBundle:
    """Pin the AdvancedLLMOverrides UX contract at the bundle layer."""

    def test_redundant_header_removed(self) -> None:
        """The original "Source profile X. Edit a field…" header was
        removed because it went stale the moment the user picked a
        different profile from the in-panel dropdown (saying the
        active profile is the source while the dropdown showed
        something else). The profile name now lives in the
        ``default <value>`` chip on the LLM profile row, so the same
        information surfaces with override status — no separate
        header line."""
        text = _all_bundle_text()
        assert "Source profile" not in text, (
            "AdvancedLLMOverrides header 'Source profile X.' must stay "
            "removed across every mount (RunNew, RerunDialog, "
            "VariationsDialog). See PR notes on the override-chip "
            "consistency refactor."
        )
        assert "Edit a field to override for this run only" not in text, (
            "Tail of the removed header line is back — likely a "
            "partial revert. The chip-based UX makes this redundant."
        )

    def test_override_chip_marker_present(self) -> None:
        """The chip pattern is ``default <value>`` always, with
        ``· override`` appended when the field is changed. Confirm
        both halves of the contract ship in the bundle."""
        text = _all_bundle_text()
        # Vite may emit unicode middle-dot as the literal character
        # or as an escape sequence; accept either.
        assert "default " in text
        assert ("· override" in text) or ("\\u00b7 override" in text), (
            "Expected the ``· override`` chip suffix in the bundle. "
            "Did the override indicator regress to the old "
            "''override' replaces default'' pattern?"
        )

    def test_llm_profile_picker_present(self) -> None:
        """The Advanced section's first row is the LLM profile
        picker. The placeholder ``use active profile`` is the
        cleanest anchor — it only appears in the picker's empty
        option."""
        text = _all_bundle_text()
        assert "use active profile" in text, (
            "The LLM profile dropdown's empty-option label is gone. "
            "The profile picker must be the first row of the "
            "Generation block in AdvancedLLMOverrides."
        )

    def test_reset_button_label_unchanged(self) -> None:
        """``Reset to profile defaults`` is the MATCH PROFILE button.
        Pin the label so the test elsewhere that talks about the
        button doesn't drift."""
        text = _all_bundle_text()
        assert "Reset to profile defaults" in text
