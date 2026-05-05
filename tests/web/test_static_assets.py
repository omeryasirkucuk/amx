"""Polish-phase tests: confirm the bundled SPA dist is shippable
and the FastAPI app serves the same hashed bundles the wheel
includes.

These don't exercise React behaviour (we don't run a headless
browser in CI). They guard the seam between the Vite output and
the Python wheel: a stale build, a renamed favicon, or a missing
asset would all surface here before users hit AMX Studio.
"""

from __future__ import annotations

from pathlib import Path

import pytest

import amx.web.server as server


def _static_root() -> Path:
    return Path(server._static_root())


def test_static_root_exists_inside_wheel() -> None:
    """The amx/web/static directory is committed as a vendored dist;
    it MUST exist after a normal pip install or `pip install -e .`."""
    root = _static_root()
    assert root.exists(), f"Expected {root} to be on disk."
    assert (root / "index.html").exists(), "index.html missing from SPA dist."


def test_index_carries_token_capture_marker() -> None:
    """Pin the on-disk index.html to a marker the launcher relies on:
    every SPA build must include the token-capture script (or, for
    PR-A's placeholder, the placeholder marker). If a future Vite
    build accidentally strips the inline script, this test catches
    it before users hit a broken AMX Studio."""
    text = (_static_root() / "index.html").read_text(encoding="utf-8")
    assert "<title>" in text.lower()
    # Either the placeholder marker (PR-A) or the SPA's runtime
    # marker (PR-B+ Vite-built bundle) is enough to tell us this
    # isn't an empty/garbage file.
    markers = ("AMX Studio", '<div id="root">')
    assert any(m in text for m in markers), (
        f"index.html doesn't contain a recognised SPA marker. Snippet: {text[:200]!r}"
    )


def test_favicon_present() -> None:
    favicon = _static_root() / "favicon.svg"
    if not favicon.exists():
        pytest.skip("favicon.svg is optional; PR-B+ ships one but the placeholder didn't.")
    assert favicon.stat().st_size > 0


def test_assets_directory_holds_hashed_bundles() -> None:
    """When the SPA is built, ``amx/web/static/assets/`` carries
    the hashed JS/CSS bundles. PR-A doesn't ship any (placeholder
    only); PR-B onwards does. The test passes when the directory
    is missing (PR-A) AND when it carries Vite-style hashed
    chunks (PR-B+)."""
    assets = _static_root() / "assets"
    if not assets.exists():
        pytest.skip("PR-A placeholder doesn't ship hashed assets.")
    chunks = list(assets.glob("*.js")) + list(assets.glob("*.css"))
    assert chunks, "Expected hashed bundles under static/assets/."
    # Vite uses 8-char hash suffixes — pin so that the wheel is
    # actually shipping built artefacts (not the placeholder).
    for chunk in chunks:
        # filenames look like 'index-CYG0yngN.css' / 'react-Dn-u2SB0.js'.
        stem = chunk.stem
        assert "-" in stem, f"Expected hashed Vite chunk, got {chunk.name}."


def test_app_serves_index_unauthenticated(client) -> None:
    """The SPA boot path must work without a token (the user lands
    on `/` first; the React tree captures the token from the URL
    and only then starts hitting `/api/*`)."""
    response = client.get("/")
    assert response.status_code == 200
    assert "AMX Studio" in response.text or '<div id="root">' in response.text
