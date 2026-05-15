"""Pin the canonical supported-extension list.

The upload UI and the local scanner both used to keep their own
whitelists; they drifted (``.markdown`` and ``.tsv`` were uploadable
but not scannable; ``.rtf`` scanned but had no real loader). One
canonical set is now exposed by :mod:`amx.docs.extensions` and both
callers re-export it.

These tests pin three properties:

* the canonical set is the **same object** from every consumer
  (no accidental copying or filtering on import),
* every supported extension has a loader entry in
  ``amx.docs.rag.LOADER_MAP`` (so ingest can never silently drop a
  file that scan accepted), and
* the historically broken extensions (``.markdown``, ``.tsv``) are
  in the set; the deliberately-dropped one (``.rtf``) is not.
"""

from __future__ import annotations


def test_canonical_supported_extensions_shared_by_uploads_and_scanner() -> None:
    from amx.docs.extensions import SUPPORTED_EXTENSIONS as CANONICAL
    from amx.docs.scanner import SUPPORTED_EXTENSIONS as SCAN
    from amx.docs.uploads import ACCEPTED_EXTENSIONS as UPLOAD

    assert SCAN is CANONICAL
    assert UPLOAD is CANONICAL


def test_canonical_extensions_include_markdown_and_tsv() -> None:
    """Fixes the upload-but-not-scan drift."""
    from amx.docs.extensions import SUPPORTED_EXTENSIONS

    assert ".markdown" in SUPPORTED_EXTENSIONS
    assert ".tsv" in SUPPORTED_EXTENSIONS


def test_canonical_extensions_drop_rtf() -> None:
    """``.rtf`` is removed: langchain has no deterministic loader without
    extra optional dependencies, so accepting it was misleading."""
    from amx.docs.extensions import SUPPORTED_EXTENSIONS

    assert ".rtf" not in SUPPORTED_EXTENSIONS


def test_every_supported_extension_has_a_loader() -> None:
    """If scan accepts a file, ingest must be able to parse it. This
    guard previously broke for ``.tsv`` and ``.markdown``."""
    from amx.docs.extensions import SUPPORTED_EXTENSIONS
    from amx.docs.rag import _build_loader_map

    loader_map = _build_loader_map()
    missing = sorted(ext for ext in SUPPORTED_EXTENSIONS if ext not in loader_map)
    assert missing == [], f"loaders missing for: {missing}"
