"""CLI hints that point at richer Studio UIs.

Centralising the strings makes it cheap to retune copy without hunting
through every command file, and lets us toggle them off via a single
config knob if the noise becomes unwelcome.
"""

from __future__ import annotations

from amx.utils.console import console

#: Per-feature hint copy. Keys are stable so docs can reference them.
_STUDIO_HINTS: dict[str, str] = {
    "doc-add": (
        "Studio'da Settings → Docs altında dosyaları sürükle-bırak ile de ekleyebilirsiniz."
    ),
    "code-search": (
        "Studio'da Settings → Code altında 'Search code' kutusu aynı "
        "sonuçları interaktif kart olarak gösterir."
    ),
    "code-analyze": (
        "Studio'da Code → Analyze sayfasından bu işi tablo seçimiyle interaktif yürütebilirsiniz."
    ),
    "ask-context": ("Studio'da AskChat üst banner'ı bu kapsamı küçük rozetle gösterir."),
}


def studio_hint(name: str) -> None:
    """Print a one-line tip for ``name`` if registered, else no-op.

    Output goes through the AMX console so it picks up the same theme
    as the rest of the CLI (dim colour, no extra padding).
    """
    text = _STUDIO_HINTS.get(name)
    if not text:
        return
    console.print(f"[dim]💡 Tip: {text}[/dim]")
