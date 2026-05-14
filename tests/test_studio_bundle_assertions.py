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

    def test_llm_profile_row_renders_before_temperature(self) -> None:
        """The LLM profile picker must be the first row of the
        Generation block — i.e. its label appears earlier in the
        bundle than ``Temperature``'s label.

        The Vite chunk that owns ``AdvancedLLMOverrides`` renders the
        rows top-to-bottom in source order, so the label-string offsets
        in the compiled chunk match render order. A regression that
        moves the profile picker below Temperature (or drops it
        entirely from the Generation block) flips this ordering.
        """
        adv = sorted(_BUNDLE_ROOT.glob("AdvancedLLMOverrides-*.js"))
        if not adv:
            pytest.skip("AdvancedLLMOverrides chunk not present in bundle.")
        chunk = adv[-1].read_text(encoding="utf-8", errors="ignore")
        profile_idx = chunk.find("LLM profile")
        temperature_idx = chunk.find("Temperature")
        assert profile_idx != -1, (
            "The ``LLM profile`` row label is missing from the "
            "AdvancedLLMOverrides chunk. The picker must render "
            "regardless of caller; check the row is no longer "
            "gated behind a hide-prop."
        )
        assert temperature_idx != -1
        assert profile_idx < temperature_idx, (
            f"``LLM profile`` (offset {profile_idx}) renders AFTER "
            f"``Temperature`` (offset {temperature_idx}) in the "
            "compiled chunk. The picker must be the first row of "
            "the Generation block."
        )


class TestRunNewMountsProfilePicker:
    """RunNew must pass the profiles + capability props to
    AdvancedLLMOverrides — the shared component's profile row is
    gated on ``profiles.length > 0`` so omitting the prop silently
    hides the picker on the New run page."""

    def test_runnew_chunk_fetches_llm_profiles(self) -> None:
        runnew = sorted(_BUNDLE_ROOT.glob("RunNew-*.js"))
        if not runnew:
            pytest.skip("RunNew chunk not present in bundle.")
        chunk = runnew[-1].read_text(encoding="utf-8", errors="ignore")
        # The TanStack Query key for the LLM profiles list.
        assert "/api/profiles/llm" in chunk, (
            "RunNew must fetch ``/api/profiles/llm`` so the shared "
            "AdvancedLLMOverrides component can render its profile "
            "picker row. The picker is gated on a non-empty profiles "
            "array."
        )

    def test_runnew_chunk_uses_capabilities_hook(self) -> None:
        """RunNew must wire ``useLLMCapabilities`` so capability gates
        re-resolve when the user picks a different profile in the
        in-panel dropdown. The capabilities endpoint is the most
        reliable anchor — it appears in the chunk that imports
        ``useLLMCapabilities``. Searching the whole bundle (rather
        than the RunNew chunk in isolation) is intentional: Vite
        may split the hook into a shared chunk."""
        runnew = sorted(_BUNDLE_ROOT.glob("RunNew-*.js"))
        if not runnew:
            pytest.skip("RunNew chunk not present in bundle.")
        all_text = _all_bundle_text()
        assert "/api/llm/capabilities" in all_text, (
            "Capabilities endpoint missing from the bundle. The "
            "``useLLMCapabilities`` hook must drive the per-profile "
            "knob gating on every Advanced LLM settings surface."
        )


class TestRunDetailLineageAndDescendants:
    """RunDetail must show a lineage chip on Variations / Re-Run
    children and an inline ``Other versions`` panel on parents."""

    def test_lineage_chip_strings_present(self) -> None:
        """Anchor on the three labels the LineageChip emits so a
        regression that drops the chip lands on this test."""
        text = _all_bundle_text()
        assert "From run" in text, "Variations header chip 'From run #N' is missing."
        assert "Re-run of run" in text, "Re-Run header chip is missing."
        assert "Navigate to the parent run that produced the seed alternative" in text, (
            "LineageChip tooltip copy is missing."
        )

    def test_descendants_render_as_sibling_result_row_items(self) -> None:
        """Descendants now render as their OWN ``ResultRowItem``
        instances spliced into the main rows flow — not as a
        read-only group inside the v1 card. The user spec required
        v1 and v2 to have equal interaction surface (click-to-apply,
        SC badge, ✨ trigger). Anchor on the provenance banner copy
        that ResultRowItem now renders for descendant rows."""
        text = _all_bundle_text()
        # The old run-wide panel must stay gone.
        assert "Variations + Re-Runs derived from this run" not in text
        # The interim VersionGroupsSection (which rendered descendants
        # as read-only li elements inside v1) must also stay gone.
        assert "no alternatives saved" not in text, (
            "The interim VersionGroupsSection's read-only li renderer "
            "is back. Descendants must render as full ResultRowItem "
            "instances so v2 alternatives have click-to-apply / SC "
            "badge / ✨ trigger parity with v1."
        )
        # The provenance banner copy lives on the descendant
        # ResultRowItem's header — anchor on it.
        assert "variations of" in text or "re-run" in text
        assert "Open this version on its own detail page" in text

    def test_results_secondary_count_line_present(self) -> None:
        """The 'Showing N original · M variations' secondary line
        under the tab strip — Issue 4 of the lineage UI spec."""
        text = _all_bundle_text()
        assert "original" in text  # part of "Showing N original"

    def test_include_descendants_query_param_used(self) -> None:
        """RunDetail must fetch with include_descendants=true so the
        descendants panel actually has data to render."""
        text = _all_bundle_text()
        assert "include_descendants=true" in text
