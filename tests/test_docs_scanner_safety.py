"""Local doc scanner safety: ``.gitignore`` respect + binary detection.

The scanner used to ``rglob("*")`` and trust the extension whitelist
alone. Two gaps we close here:

1. ``.gitignore`` was ignored, so a user pointing AMX at a working
   git repo could accidentally feed ``node_modules/**`` or
   ``.venv/**`` into the embedding store.
2. Extensionless files and ``.txt`` files that hold raw bytes still
   slipped through the whitelist and produced garbage embeddings.

Both gaps are best-effort: ``pathspec`` is optional (falls back to
"no filter" when missing) and the binary sniff is the simplest
possible heuristic (``NUL`` byte in the first 4 KB).
"""

from __future__ import annotations

from pathlib import Path

from amx.docs.scanner import _resolve_local


def _scan(path: Path) -> list[str]:
    """Helper — return scanned paths as plain strings, sorted for assert."""
    return sorted(d.path for d in _resolve_local(str(path)))


def test_resolve_local_picks_up_supported_extensions(tmp_path: Path) -> None:
    (tmp_path / "good.md").write_text("docs", encoding="utf-8")
    (tmp_path / "good.txt").write_text("plain", encoding="utf-8")
    # Unsupported extension — must be filtered out.
    (tmp_path / "skip.bin").write_bytes(b"raw")
    paths = _scan(tmp_path)
    names = [Path(p).name for p in paths]
    assert "good.md" in names
    assert "good.txt" in names
    assert "skip.bin" not in names


def test_resolve_local_skips_binary_files_with_supported_extension(tmp_path: Path) -> None:
    """A ``.txt`` carrying raw bytes (NUL in first 4 KB) is rejected
    so the embedding store doesn't end up indexing garbage."""
    bad = tmp_path / "binary.txt"
    bad.write_bytes(b"hello\x00world")
    good = tmp_path / "real.txt"
    good.write_text("hello world", encoding="utf-8")
    paths = _scan(tmp_path)
    names = [Path(p).name for p in paths]
    assert "real.txt" in names
    assert "binary.txt" not in names


def test_resolve_local_respects_gitignore_when_pathspec_available(tmp_path: Path) -> None:
    """When ``pathspec`` is installed (and a ``.gitignore`` is present),
    matching files are skipped. We use a deliberately-broad pattern
    (``ignored/``) so the test runs whether or not ``pathspec`` is
    present — when it isn't, the file is still picked up; when it is,
    it isn't. Both paths are valid behaviour for the scanner."""
    (tmp_path / ".gitignore").write_text("ignored/\n", encoding="utf-8")
    keep = tmp_path / "keep.md"
    keep.write_text("kept", encoding="utf-8")
    nested = tmp_path / "ignored"
    nested.mkdir()
    (nested / "drop.md").write_text("dropped", encoding="utf-8")

    try:
        import pathspec  # noqa: F401  # only used to detect availability
    except Exception:
        pathspec_available = False
    else:
        pathspec_available = True

    paths = _scan(tmp_path)
    names = [Path(p).name for p in paths]
    assert "keep.md" in names
    if pathspec_available:
        assert "drop.md" not in names
    # If pathspec isn't installed, drop.md is still seen — that's the
    # documented graceful-degradation behaviour.


def test_resolve_local_single_file_path(tmp_path: Path) -> None:
    """Pointing the scanner at a single file (not a directory) works
    too — historic CLI behaviour the new branching must preserve."""
    f = tmp_path / "doc.md"
    f.write_text("hi", encoding="utf-8")
    paths = _scan(f)
    assert paths == [str(f)]


def test_resolve_local_single_binary_file_is_skipped(tmp_path: Path) -> None:
    f = tmp_path / "weird.txt"
    f.write_bytes(b"\x00binary")
    paths = _scan(f)
    assert paths == []


def test_resolve_local_keeps_pdf_with_nul_bytes(tmp_path: Path) -> None:
    """Regression: PDFs are a binary format and almost always carry NUL
    bytes in their stream. The NUL-byte heuristic used to filter them
    out, so users who drag-dropped a resume PDF onto a doc profile
    ended up with 0 ingested chunks and a search index that only knew
    about the ``.amx-manifest.json`` sidecar. Binary-loader extensions
    (``.pdf``, ``.docx``, ``.xlsx``, ...) must bypass the heuristic."""
    f = tmp_path / "resume.pdf"
    f.write_bytes(b"%PDF-1.4\n\x00\x00binary stream data with \x00 nul bytes")
    paths = _scan(f)
    assert paths == [str(f)]


def test_resolve_local_skips_amx_manifest_sidecar(tmp_path: Path) -> None:
    """The ``~/.amx/uploads/<profile>/.amx-manifest.json`` sidecar tracks
    original filenames for hashed uploads. Pre-fix the scanner ingested
    it as if it were user content, so every "Search docs" query
    surfaced the manifest as the top hit — there was nothing else to
    return on a fresh profile."""
    keep = tmp_path / "notes.md"
    keep.write_text("real content", encoding="utf-8")
    manifest = tmp_path / ".amx-manifest.json"
    manifest.write_text('{"files": {}}', encoding="utf-8")
    paths = _scan(tmp_path)
    names = [Path(p).name for p in paths]
    assert "notes.md" in names
    assert ".amx-manifest.json" not in names


def test_resolve_local_unknown_path_returns_nothing(tmp_path: Path) -> None:
    """Non-existent paths short-circuit cleanly; no exception."""
    missing = tmp_path / "does-not-exist"
    paths = _scan(missing)
    assert paths == []
